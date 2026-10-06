import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Exercises the real processor. Adapter hooks intentionally hold late replies;
/// no network request, Realm write or durable page acknowledgement is performed.
final class ChangeRequestProcessorCancellationTests: XCTestCase, @unchecked Sendable {
    @BigSyncBackgroundActor
    private func addLive(_ processor: ChangeRequestProcessor, _ adapter: ProcessorCancellationAdapter,
                         run: UUID, name: String = "Item.one") {
        processor.addFetchedChangeRequest(.init(
            downloadedRecord: CKRecord(recordType: String(name.prefix { $0 != "." }),
                                       recordID: .init(recordName: name, zoneID: adapter.recordZoneID)),
            deletedRecordID: nil, adapter: adapter, runID: run))
    }

    @BigSyncBackgroundActor
    private func addDeletion(_ processor: ChangeRequestProcessor, _ adapter: ProcessorCancellationAdapter,
                             run: UUID, name: String = "Item.deleted") {
        processor.addFetchedChangeRequest(.init(downloadedRecord: nil,
            deletedRecordID: .init(recordName: name, zoneID: adapter.recordZoneID), adapter: adapter, runID: run))
    }

    private func assertCancelled(_ result: Result<InboundProcessingOutcomes, Error>,
                                 file: StaticString = #filePath, line: UInt = #line) {
        guard case .failure(let error) = result else {
            return XCTFail("Cancelled processing delivered success", file: file, line: line)
        }
        XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)", file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testResetDoesNotRetainLateLiveFailure() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "live apply held")
        let gate = ProcessorCancellationGate()
        let error = NSError(domain: "RetiredLiveApply", code: 1)
        let adapter = ProcessorCancellationAdapter(save: { _ in
            entered.fulfill(); await gate.wait(); throw error
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        processor.reset()
        XCTAssertTrue(processor.getErrors().isEmpty)
        await gate.open()
        assertCancelled(await request.result)
        XCTAssertTrue(processor.getErrors().isEmpty, "Retired apply repopulated cleared error state")
    }

    @BigSyncBackgroundActor
    func testResetDoesNotRetainLateDeletionFailure() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "delete apply held")
        let gate = ProcessorCancellationGate()
        let adapter = ProcessorCancellationAdapter(delete: { _ in
            entered.fulfill(); await gate.wait(); throw NSError(domain: "RetiredDelete", code: 2)
        })
        let run = await processor.beginRun()
        addDeletion(processor, adapter, run: run)
        let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        processor.reset()
        await gate.open()
        assertCancelled(await request.result)
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testBeginRunDoesNotInheritRetiredApplyFailure() async throws {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "old apply held")
        let cancelled = expectation(description: "old child cancelled by replacement")
        let gate = ProcessorCancellationGate()
        let adapter = ProcessorCancellationAdapter(save: { _ in
            try await withTaskCancellationHandler {
                entered.fulfill(); await gate.wait()
                throw NSError(domain: "OldApplyDuringReplacement", code: 3)
            } onCancel: { cancelled.fulfill() }
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        let replacement = Task { @BigSyncBackgroundActor in await processor.beginRun() }
        addTeardownBlock { await gate.open(); _ = await replacement.value }
        await fulfillment(of: [cancelled], timeout: 2)
        XCTAssertTrue(processor.cancelSync, "Replacement waits for old processing before reopening")
        await gate.open()
        let nextRun = await replacement.value
        assertCancelled(await request.result)
        XCTAssertNotEqual(run, nextRun)
        XCTAssertFalse(processor.cancelSync)
        XCTAssertTrue(processor.getErrors().isEmpty, "Old error leaked across the run boundary")
        let current = ProcessorCancellationAdapter()
        addLive(processor, current, run: nextRun, name: "Item.current")
        let result = try await processor.finishProcessing(for: current)
        XCTAssertEqual(result.liveResults.map(\.event.recordName), ["Item.current"])
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testCallerCancellationStopsBeforeDeletionAfterSuccessfulLiveApply() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "live apply held before deletion")
        let gate = ProcessorCancellationGate()
        let seen = ProcessorCancellationObservation()
        let adapter = ProcessorCancellationAdapter(save: { records in
            entered.fulfill(); await gate.wait()
            seen.liveApplied += records.count // Represents a completed adapter effect, not native persistence.
            seen.childSawCancellation = Task.isCancelled
            return ProcessorCancellationAdapter.liveResults(records)
        }, delete: { ids in
            seen.deletions += ids.count
            return ProcessorCancellationAdapter.deletionResults(ids)
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        addDeletion(processor, adapter, run: run)
        let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        await gate.open()
        assertCancelled(await request.result)
        XCTAssertEqual(seen.liveApplied, 1, "Cancellation does not undo an already-applied adapter result")
        XCTAssertTrue(seen.childSawCancellation)
        XCTAssertEqual(seen.deletions, 0, "Cancelled caller started the next mutation phase")
        XCTAssertTrue(processor.getErrors().isEmpty)
        XCTAssertFalse(processor.cancelSync, "Caller cancellation must not reset the shared processor")
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledEmptyAggregateDoesNotReturnSuccess() async {
        let processor = ChangeRequestProcessor()
        _ = await processor.beginRun()
        let request = Task { @BigSyncBackgroundActor in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await processor.finishProcessing()
        }
        assertCancelled(await request.result)
        XCTAssertFalse(processor.cancelSync)
    }

    @BigSyncBackgroundActor
    func testResetEmptyAggregateDoesNotReturnSuccess() async {
        let processor = ChangeRequestProcessor()
        processor.reset()
        do { _ = try await processor.finishProcessing(); XCTFail("Stopped empty aggregate succeeded") }
        catch { XCTAssertTrue(error is CancellationError) }
    }

    @BigSyncBackgroundActor
    func testCurrentOperationFailureKeepsOriginalErrorIdentity() async throws {
        let processor = ChangeRequestProcessor()
        let original = NSError(domain: "CurrentAdapterFailure", code: 4)
        let adapter = ProcessorCancellationAdapter(save: { _ in throw original })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let outcome = try await processor.finishProcessing(for: adapter)
        XCTAssertTrue(outcome.liveResults.isEmpty)
        XCTAssertEqual(processor.getErrors().count, 1)
        XCTAssertTrue((processor.getErrors().first as NSError?) === original)
        processor.clearErrors()
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testCurrentMixedBatchPreservesLiveOutcomeWhenDeletionFails() async throws {
        let processor = ChangeRequestProcessor()
        let original = NSError(domain: "CurrentDeletionFailure", code: 5)
        let adapter = ProcessorCancellationAdapter(delete: { _ in throw original })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        addDeletion(processor, adapter, run: run)
        let outcome = try await processor.finishProcessing(for: adapter)
        XCTAssertEqual(outcome.liveResults.count, 1)
        XCTAssertTrue(outcome.deletionResults.isEmpty)
        XCTAssertEqual(processor.getErrors().count, 1)
        XCTAssertTrue((processor.getErrors().first as NSError?) === original)
    }
}

@BigSyncBackgroundActor
private final class ProcessorCancellationObservation {
    var liveApplied = 0
    var deletions = 0
    var childSawCancellation = false
    var returned = false
    var validationAllowed = true
    nonisolated let cancellationSignal = ProcessorCancellationSignal()
}

/// Noncooperative reply gate, always released and drained by each test.
private actor ProcessorCancellationGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func open() {
        isOpen = true
        let current = waiters; waiters.removeAll()
        current.forEach { $0.resume() }
    }
}

final class ProcessorCancellationAdapter: NSObject, ModelAdapter, @unchecked Sendable {
    typealias Save = @Sendable @BigSyncBackgroundActor ([CKRecord]) async throws -> [InboundLiveResult]
    typealias Delete = @Sendable @BigSyncBackgroundActor ([CKRecord.ID]) async throws -> [InboundDeletionResult]
    let recordZoneID = CKRecordZone.ID(zoneName: "processor-" + UUID().uuidString, ownerName: CKCurrentUserDefaultName)
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    private let save: Save
    private let delete: Delete
    private let activation: @Sendable @BigSyncBackgroundActor () -> Void
    init(save: @escaping Save = { liveResults($0) }, delete: @escaping Delete = { deletionResults($0) },
         activation: @escaping @Sendable @BigSyncBackgroundActor () -> Void = {}) {
        self.save = save; self.delete = delete; self.activation = activation
    }
    @BigSyncBackgroundActor
    func activateTransportNamespace(containerIdentifier: String, databaseScope: CKDatabase.Scope) async throws {
        activation()
    }
    static func liveResults(_ records: [CKRecord]) -> [InboundLiveResult] {
        records.enumerated().map { .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
            recordID: $0.element.recordID), disposition: .applied) }
    }
    static func deletionResults(_ ids: [CKRecord.ID]) -> [InboundDeletionResult] {
        ids.enumerated().map { .init(event: .init(ordinal: $0.offset, entityType: "Item",
            recordID: $0.element), disposition: .appliedTombstone) }
    }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] { try await save(records) }
    func deleteRecords(with ids: [CKRecord.ID]) async throws -> [InboundDeletionResult] { try await delete(ids) }
    var hasChanges: Bool { false }
    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func persistImportedChanges() async throws {}
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] { [] }
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {}
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    func requeueMissingServerRecords(_ ids: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func didFinishImport() async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

extension ChangeRequestProcessorCancellationTests {
    @BigSyncBackgroundActor
    func testRetiredMalformedRepliesDoNotBecomeCurrentValidationErrors() async {
        for deletion in [false, true] {
            for wrongIdentity in [false, true] {
                let processor = ChangeRequestProcessor()
                let entered = expectation(description: "malformed old result held")
                let gate = ProcessorCancellationGate()
                let adapter = ProcessorCancellationAdapter(save: { records in
                    entered.fulfill(); await gate.wait()
                    guard wrongIdentity else { return [] }
                    return [.init(event: .init(ordinal: 99, entityType: records[0].recordType,
                        recordID: records[0].recordID), disposition: .applied)]
                }, delete: { ids in
                    entered.fulfill(); await gate.wait()
                    guard wrongIdentity else { return [] }
                    return [.init(event: .init(ordinal: 99, entityType: "Item",
                        recordID: ids[0]), disposition: .appliedTombstone)]
                })
                let run = await processor.beginRun()
                if deletion { addDeletion(processor, adapter, run: run) }
                else { addLive(processor, adapter, run: run) }
                let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
                addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
                await fulfillment(of: [entered], timeout: 2)
                processor.reset()
                await gate.open()
                assertCancelled(await request.result)
                XCTAssertTrue(processor.getErrors().isEmpty, "Retired validation contaminated current state")
            }
        }
    }

    @BigSyncBackgroundActor
    func testCurrentMalformedResultsStillRecordExactValidationFailure() async throws {
        for deletion in [false, true] {
            for wrongIdentity in [false, true] {
                let processor = ChangeRequestProcessor()
                let adapter = ProcessorCancellationAdapter(save: { records in
                    guard wrongIdentity else { return [] }
                    return [.init(event: .init(ordinal: 99, entityType: records[0].recordType,
                        recordID: records[0].recordID), disposition: .applied)]
                }, delete: { ids in
                    guard wrongIdentity else { return [] }
                    return [.init(event: .init(ordinal: 99, entityType: "Item",
                        recordID: ids[0]), disposition: .appliedTombstone)]
                })
                let run = await processor.beginRun()
                if deletion { addDeletion(processor, adapter, run: run) }
                else { addLive(processor, adapter, run: run) }
                let result = try await processor.finishProcessing(for: adapter)
                XCTAssertTrue(result.liveResults.isEmpty)
                XCTAssertTrue(result.deletionResults.isEmpty)
                let errors = processor.getErrors()
                XCTAssertEqual(errors.count, 1)
                let expected: InboundDispositionValidationError = wrongIdentity
                    ? .identityMismatch(ordinal: 0, expectedRecordName: deletion ? "Item.deleted" : "Item.one")
                    : .cardinality(expected: 1, actual: 0)
                XCTAssertEqual(errors.first as? InboundDispositionValidationError, expected)
            }
        }
    }

    @BigSyncBackgroundActor
    func testCallerCancellationRejectsLateFailureWithoutResettingProcessor() async {
        for deletion in [false, true] {
            let processor = ChangeRequestProcessor()
            let entered = expectation(description: "caller cancellation before late failure")
            let gate = ProcessorCancellationGate()
            let fail: @Sendable @BigSyncBackgroundActor () async throws -> Void = {
                entered.fulfill(); await gate.wait()
                throw NSError(domain: "LateUnstructuredChild", code: 6)
            }
            let adapter = ProcessorCancellationAdapter(save: { _ in try await fail(); return [] },
                                                        delete: { _ in try await fail(); return [] })
            let run = await processor.beginRun()
            if deletion { addDeletion(processor, adapter, run: run) }
            else { addLive(processor, adapter, run: run) }
            let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
            addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
            await fulfillment(of: [entered], timeout: 2)
            request.cancel()
            await gate.open()
            assertCancelled(await request.result)
            XCTAssertFalse(processor.cancelSync, "A caller must not globally reset another owner's processor")
            XCTAssertTrue(processor.getErrors().isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testAggregateCancellationLeavesUnstartedOtherAdapterAvailable() async throws {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "aggregate first adapter held")
        let gate = ProcessorCancellationGate()
        let observed = ProcessorCancellationObservation()
        let first = ProcessorCancellationAdapter(save: { records in
            entered.fulfill(); await gate.wait()
            return ProcessorCancellationAdapter.liveResults(records)
        })
        let second = ProcessorCancellationAdapter(save: { records in
            observed.liveApplied += records.count
            return ProcessorCancellationAdapter.liveResults(records)
        })
        let run = await processor.beginRun()
        addLive(processor, first, run: run, name: "Item.first")
        addLive(processor, second, run: run, name: "Item.second")
        let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing() }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        await gate.open()
        assertCancelled(await request.result)
        XCTAssertEqual(observed.liveApplied, 0, "Cancelled aggregate entered another adapter")
        XCTAssertTrue(processor.hasPendingChangeRequests(for: second))
        let result = try await processor.finishProcessing(for: second)
        XCTAssertEqual(result.liveResults.map(\.event.recordName), ["Item.second"])
        XCTAssertEqual(observed.liveApplied, 1)
        XCTAssertFalse(processor.cancelSync)
    }

    @BigSyncBackgroundActor
    func testCancellationOnlyReachesCapturedChildAndKeepsReplacementTaskOwned() async {
        let processor = ChangeRequestProcessor()
        let firstEntered = expectation(description: "first processor child held")
        let secondEntered = expectation(description: "independent second child held")
        let secondCancelled = expectation(description: "only subsequent reset cancels second child")
        let firstGate = ProcessorCancellationGate()
        let secondGate = ProcessorCancellationGate()
        let observation = ProcessorCancellationObservation()
        let first = ProcessorCancellationAdapter(save: { records in
            firstEntered.fulfill(); await firstGate.wait()
            return ProcessorCancellationAdapter.liveResults(records)
        })
        let second = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                secondEntered.fulfill(); await secondGate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: {
                // A synchronous signal distinguishes actual child cancellation
                // from success observed only after the gate is released.
                observation.cancellationSignal.record()
                secondCancelled.fulfill()
            }
        })
        let run = await processor.beginRun()
        addLive(processor, first, run: run)
        let firstRequest = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: first) }
        addTeardownBlock { firstRequest.cancel(); await firstGate.open(); _ = await firstRequest.result }
        await fulfillment(of: [firstEntered], timeout: 2)
        addLive(processor, second, run: run)
        let secondRequest = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: second) }
        addTeardownBlock { secondRequest.cancel(); await secondGate.open(); _ = await secondRequest.result }
        await fulfillment(of: [secondEntered], timeout: 2)
        firstRequest.cancel()
        await firstGate.open()
        assertCancelled(await firstRequest.result)
        XCTAssertEqual(observation.cancellationSignal.count, 0, "Old caller cancelled the installed successor child")
        XCTAssertFalse(processor.cancelSync)
        // The first caller's defer must not retire the second task handle.
        processor.reset()
        await fulfillment(of: [secondCancelled], timeout: 2)
        await secondGate.open()
        assertCancelled(await secondRequest.result)
        XCTAssertEqual(observation.cancellationSignal.count, 1)
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testResetCancelsAndJoinsEveryOverlappingProcessingChild() async {
        let processor = ChangeRequestProcessor()
        let firstEntered = expectation(description: "first overlapping child held")
        let secondEntered = expectation(description: "second overlapping child held")
        let firstCancelled = expectation(description: "reset cancels first child")
        let secondCancelled = expectation(description: "reset cancels second child")
        let firstGate = ProcessorCancellationGate()
        let secondGate = ProcessorCancellationGate()
        let restartReturned = ProcessorCancellationSignal()
        let first = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                firstEntered.fulfill(); await firstGate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: { firstCancelled.fulfill() }
        })
        let second = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                secondEntered.fulfill(); await secondGate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: { secondCancelled.fulfill() }
        })
        let run = await processor.beginRun()
        addLive(processor, first, run: run, name: "Item.first-overlap")
        let firstRequest = Task { @BigSyncBackgroundActor in
            try await processor.finishProcessing(for: first)
        }
        addTeardownBlock {
            firstRequest.cancel(); await firstGate.open(); _ = await firstRequest.result
        }
        await fulfillment(of: [firstEntered], timeout: 2)
        addLive(processor, second, run: run, name: "Item.second-overlap")
        let secondRequest = Task { @BigSyncBackgroundActor in
            try await processor.finishProcessing(for: second)
        }
        addTeardownBlock {
            secondRequest.cancel(); await secondGate.open(); _ = await secondRequest.result
        }
        await fulfillment(of: [secondEntered], timeout: 2)

        let restart = Task { @BigSyncBackgroundActor in
            let next = await processor.beginRun()
            restartReturned.record()
            return next
        }
        addTeardownBlock { restart.cancel(); _ = await restart.result }
        await fulfillment(of: [firstCancelled, secondCancelled], timeout: 2)
        await secondGate.open()
        assertCancelled(await secondRequest.result)
        XCTAssertEqual(restartReturned.count, 0,
                       "beginRun returned while an older overlapping child was still running")
        await firstGate.open()
        assertCancelled(await firstRequest.result)
        let nextRun = await restart.value
        XCTAssertNotEqual(nextRun, run)
        XCTAssertEqual(restartReturned.count, 1)
        XCTAssertFalse(processor.cancelSync)
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testUncancelledBatchingRestrictionAndAdapterOrderRemainIntact() async throws {
        let processor = ChangeRequestProcessor()
        processor.fetchedChangeBatchSize = 1
        let first = ProcessorCancellationAdapter()
        let second = ProcessorCancellationAdapter()
        let run = await processor.beginRun()
        addLive(processor, first, run: run, name: "Other.keep")
        addLive(processor, first, run: run, name: "Item.one")
        addDeletion(processor, first, run: run, name: "Item.deleted")
        addLive(processor, second, run: run, name: "Item.second-adapter")
        addLive(processor, first, run: run, name: "Item.two")
        let selected = try await processor.finishProcessing(for: first, restrictedToEntityType: "Item")
        XCTAssertEqual(selected.liveResults.map(\.event.recordName), ["Item.one", "Item.two"])
        XCTAssertEqual(selected.deletionResults.map(\.event.recordName), ["Item.deleted"])
        XCTAssertTrue(processor.hasPendingChangeRequests(for: first, restrictedToEntityType: "Other"))
        XCTAssertTrue(processor.hasPendingChangeRequests(for: second))
        let remaining = try await processor.finishProcessing()
        XCTAssertEqual(remaining.liveResults.map(\.event.recordName), ["Other.keep", "Item.second-adapter"])
        XCTAssertTrue(remaining.deletionResults.isEmpty)
        XCTAssertFalse(processor.hasPendingChangeRequests(for: first))
        XCTAssertFalse(processor.hasPendingChangeRequests(for: second))
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testLiveEmptyDrainsAndRestartStillReturnSuccessfulEmptyOutcomes() async throws {
        let processor = ChangeRequestProcessor()
        let adapter = ProcessorCancellationAdapter()
        for _ in 0..<2 {
            _ = await processor.beginRun()
            let single = try await processor.finishProcessing(for: adapter)
            let aggregate = try await processor.finishProcessing()
            XCTAssertTrue(single.liveResults.isEmpty && single.deletionResults.isEmpty)
            XCTAssertTrue(aggregate.liveResults.isEmpty && aggregate.deletionResults.isEmpty)
            XCTAssertTrue(processor.getErrors().isEmpty)
            processor.reset()
        }
    }

    @BigSyncBackgroundActor
    func testAdapterCancellationIsTerminalWithoutBecomingStoredFailure() async {
        for deletion in [false, true] {
            let processor = ChangeRequestProcessor()
            let adapter = ProcessorCancellationAdapter(save: { _ in throw CancellationError() },
                                                        delete: { _ in throw CancellationError() })
            let run = await processor.beginRun()
            if deletion { addDeletion(processor, adapter, run: run) }
            else { addLive(processor, adapter, run: run) }
            let request = Task { @BigSyncBackgroundActor in try await processor.finishProcessing(for: adapter) }
            assertCancelled(await request.result)
            XCTAssertTrue(processor.getErrors().isEmpty)
            XCTAssertFalse(processor.cancelSync)
        }
    }

    @BigSyncBackgroundActor
    func testResetDiscardsOldQueuedRequestsAndRejectsOldRunReentry() async throws {
        let processor = ChangeRequestProcessor()
        let adapter = ProcessorCancellationAdapter()
        let originalRun = await processor.beginRun()
        addLive(processor, adapter, run: originalRun, name: "Item.queued-old")
        processor.reset()
        let newRun = await processor.beginRun()
        addLive(processor, adapter, run: originalRun, name: "Item.late-old")
        addLive(processor, adapter, run: newRun, name: "Item.current")
        let result = try await processor.finishProcessing()
        XCTAssertEqual(result.liveResults.map(\.event.recordName), ["Item.current"])
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testCancellationStillJoinsNoncooperativeAdapterBeforeReturning() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "noncooperative adapter held")
        let cancelled = expectation(description: "captured child cancellation signalled")
        let gate = ProcessorCancellationGate()
        let observed = ProcessorCancellationObservation()
        let adapter = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                entered.fulfill(); await gate.wait()
                observed.liveApplied += records.count
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: { cancelled.fulfill() }
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let request = Task { @BigSyncBackgroundActor in
            defer { observed.returned = true }
            return try await processor.finishProcessing(for: adapter)
        }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        // Failure of this signal is a real failed test, but releasing the gate
        // below still drains the original implementation's uncooperative child.
        await fulfillment(of: [cancelled], timeout: 2)
        XCTAssertFalse(observed.returned, "Cancellation must not orphan still-running adapter work")
        await gate.open()
        assertCancelled(await request.result)
        XCTAssertEqual(observed.liveApplied, 1)
        XCTAssertTrue(observed.returned)
    }
}

/// Test-only synchronous cancellation signal; never holds its lock over a callout.
private final class ProcessorCancellationSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    var count: Int { lock.withLock { value } }
    func record() { lock.withLock { value += 1 } }
}


extension ChangeRequestProcessorCancellationTests {
    @BigSyncBackgroundActor
    func testValidatedBeginRunPrecancelledDoesNotResetCurrentRun() async throws {
        let processor = ChangeRequestProcessor()
        let adapter = ProcessorCancellationAdapter()
        let originalRun = await processor.beginRun()
        addLive(processor, adapter, run: originalRun, name: "Item.keep")

        let rejected = Task { @BigSyncBackgroundActor in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await processor.beginRun {
                try Task.checkCancellation()
            }
        }
        assertCancelled(await rejected.result)
        XCTAssertFalse(processor.cancelSync)
        XCTAssertTrue(processor.hasPendingChangeRequests(for: adapter))

        let result = try await processor.finishProcessing(for: adapter)
        XCTAssertEqual(result.liveResults.map(\.event.recordName), ["Item.keep"])
        XCTAssertTrue(processor.getErrors().isEmpty)
    }

    @BigSyncBackgroundActor
    func testValidatedBeginRunCancellationDuringJoinLeavesProcessorStopped() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "old processor child held")
        let cancelled = expectation(description: "replacement reset cancels old child")
        let gate = ProcessorCancellationGate()
        let adapter = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                entered.fulfill()
                await gate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: {
                cancelled.fulfill()
            }
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let oldRequest = Task { @BigSyncBackgroundActor in
            try await processor.finishProcessing(for: adapter)
        }
        addTeardownBlock {
            oldRequest.cancel()
            await gate.open()
            _ = await oldRequest.result
        }
        await fulfillment(of: [entered], timeout: 2)

        let replacement = Task { @BigSyncBackgroundActor in
            try await processor.beginRun {
                try Task.checkCancellation()
            }
        }
        addTeardownBlock {
            replacement.cancel()
            await gate.open()
            _ = await replacement.result
        }
        await fulfillment(of: [cancelled], timeout: 2)
        replacement.cancel()
        await gate.open()
        assertCancelled(await oldRequest.result)
        switch await replacement.result {
        case .failure(let error):
            XCTAssertTrue(error is CancellationError)
        case .success:
            XCTFail("Cancelled replacement reopened the processor")
        }
        XCTAssertTrue(processor.cancelSync)

        _ = await processor.beginRun()
        XCTAssertFalse(processor.cancelSync, "A later live owner can reopen the processor")
    }

    @BigSyncBackgroundActor
    func testValidatedBeginRunAuthorityFailureAfterJoinLeavesProcessorStopped() async {
        let processor = ChangeRequestProcessor()
        let entered = expectation(description: "old processor child held")
        let cancelled = expectation(description: "replacement reset cancels old child")
        let gate = ProcessorCancellationGate()
        let observation = ProcessorCancellationObservation()
        observation.validationAllowed = true
        let adapter = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                entered.fulfill()
                await gate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: {
                cancelled.fulfill()
            }
        })
        let run = await processor.beginRun()
        addLive(processor, adapter, run: run)
        let oldRequest = Task { @BigSyncBackgroundActor in
            try await processor.finishProcessing(for: adapter)
        }
        addTeardownBlock {
            oldRequest.cancel()
            await gate.open()
            _ = await oldRequest.result
        }
        await fulfillment(of: [entered], timeout: 2)

        let replacement = Task { @BigSyncBackgroundActor in
            try await processor.beginRun {
                guard observation.validationAllowed else {
                    throw CancellationError()
                }
            }
        }
        addTeardownBlock {
            replacement.cancel()
            await gate.open()
            _ = await replacement.result
        }
        await fulfillment(of: [cancelled], timeout: 2)
        observation.validationAllowed = false
        await gate.open()
        assertCancelled(await oldRequest.result)
        switch await replacement.result {
        case .failure(let error):
            XCTAssertTrue(error is CancellationError)
        case .success:
            XCTFail("Invalidated replacement reopened the processor")
        }
        XCTAssertTrue(processor.cancelSync)
    }
}
