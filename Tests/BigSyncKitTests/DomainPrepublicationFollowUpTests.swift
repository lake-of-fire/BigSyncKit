import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Exercises the actual synchronizer tail, not a copy of its follow-up flag.
/// The empty transport is deliberately not signed CloudKit/app qualification.
final class DomainPrepublicationFollowUpTests: XCTestCase {
    @BigSyncBackgroundActor
    func testLocalOnlyDeclarationRequestsOneFreshPassBeforeReceipt() async throws {
        let fixture = try makeFixture()
        let synchronizer = fixture.synchronizer
        var consumedRuns = [UUID]()
        var preparedRuns = [UUID]()
        synchronizer.synchronizationWillConsumeServerChangesHandler = {
            consumedRuns.append($0.runID)
        }
        synchronizer.domainPrepublicationHandler = { context in
            preparedRuns.append(context.runID)
            if preparedRuns.count == 1 {
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
                return [.init(code: "test-local-declaration-changed")]
            }
            return []
        }
        let result = try await synchronizer.synchronize()
        XCTAssertEqual(consumedRuns.count, 2)
        XCTAssertEqual(preparedRuns, consumedRuns)
        XCTAssertNotEqual(preparedRuns.first, preparedRuns.last)
        XCTAssertEqual(result.receipt?.runID, preparedRuns.last)
        XCTAssertEqual(result.publicationState, .complete)
        XCTAssertEqual(result.completionScope, .fullSynchronization)
        XCTAssertFalse(synchronizer.synchronizationRequestedWhileRunning)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0,
                       "A local declaration must not manufacture upload work")
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testPreviousPassCannotRequestAnotherRun() async throws {
        let fixture = try makeFixture()
        var previous: CloudKitSynchronizer.PrepublicationBoundaryContext?
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            if let previous {
                XCTAssertThrowsError(try fixture.worker.requestFollowUpSynchronization(after: previous))
                XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            } else {
                previous = context
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
            }
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 2)
        let completedContext = try XCTUnwrap(previous)
        XCTAssertThrowsError(try fixture.worker.requestFollowUpSynchronization(after: completedContext))
        XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testDownloadOnlyDoesNotAcquireUploadOrFollowUpAuthority() async throws {
        let fixture = try makeFixture()
        fixture.synchronizer.syncMode = .downloadOnly
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            // Changing configuration does not upgrade this already-running
            // download-only drain's immutable completion scope.
            fixture.synchronizer.syncMode = .sync
            XCTAssertFalse(try fixture.worker.requestFollowUpSynchronization(after: context))
            let readiness = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(readiness, .downloadOnly)
            XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        XCTAssertEqual(result.completionScope, .downloadOnly)
        XCTAssertNil(result.receipt)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testReplacedWorkerRejectsPreviousWorkerBoundary() async throws {
        let old = try makeFixture()
        let replacement = try makeFixture(account: "replacement-account")
        var oldContext: CloudKitSynchronizer.PrepublicationBoundaryContext?
        old.synchronizer.domainPrepublicationHandler = { context in
            oldContext = context
            return []
        }
        _ = try await old.synchronizer.synchronize()
        let captured = try XCTUnwrap(oldContext)
        old.worker._test_installSynchronizer(
            replacement.synchronizer, performsAccountAvailabilityPreflight: false
        )
        var passes = 0
        replacement.synchronizer.domainPrepublicationHandler = { current in
            passes += 1
            XCTAssertNotEqual(captured.replicaBindingGenerationIdentifier,
                              current.replicaBindingGenerationIdentifier)
            XCTAssertThrowsError(try old.worker.requestFollowUpSynchronization(after: captured))
            XCTAssertFalse(replacement.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        _ = try await replacement.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        await old.stop()
        await replacement.stop()
    }

    @BigSyncBackgroundActor
    func testCancelledCallerCannotRequestFollowUpFromCurrentBoundary() async throws {
        let fixture = try makeFixture()
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            let task = Task { @BigSyncBackgroundActor in
                try fixture.worker.requestFollowUpSynchronization(after: context)
            }
            task.cancel()
            do {
                _ = try await task.value
                XCTFail("A cancelled callback must not request another drain")
            } catch is CancellationError {
            }
            XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testTransitionReadinessRechecksPendingWorkAndSemanticBlockers() async throws {
        let fixture = try makeFixture()
        var inspected = false
        fixture.synchronizer.domainPrepublicationHandler = { context in
            inspected = true
            let ready = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(ready, .ready)
            fixture.adapter.pending = true
            let pending = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(pending, .pendingWork)
            fixture.adapter.blockers = [.init(code: "test-semantic-recovery")]
            let pendingAndBlocked = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(pendingAndBlocked, .pendingWork)
            fixture.adapter.pending = false
            let blocked = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(blocked, .blocked)
            XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning,
                           "Read-only inspection must not create a retry loop for semantic debt")
            fixture.adapter.blockers = []
            let recovered = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(recovered, .ready)
            return []
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertTrue(inspected)
        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testTransitionReadinessRejectsEarlierPassWithoutBlockingFreshWork() async throws {
        let fixture = try makeFixture()
        var previous: CloudKitSynchronizer.PrepublicationBoundaryContext?
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            if let previous {
                do {
                    _ = try await fixture.worker.domainTransitionReadiness(after: previous)
                    XCTFail("An earlier pass cannot authorize a transition")
                } catch is CancellationError {}
            } else {
                previous = context
                _ = try fixture.worker.requestFollowUpSynchronization(after: context)
            }
            let ready = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(ready, .ready)
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 2)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testTransitionReadinessRejectsWorkerReplacementDuringInspection() async throws {
        let fixture = try makeFixture()
        let replacement = try makeFixture(account: "new-account")
        var inspected = false
        fixture.synchronizer.domainPrepublicationHandler = { context in
            fixture.adapter.onInspection = {
                fixture.worker._test_installSynchronizer(
                    replacement.synchronizer, performsAccountAvailabilityPreflight: false
                )
            }
            defer { fixture.adapter.onInspection = nil }
            do {
                _ = try await fixture.worker.domainTransitionReadiness(after: context)
                XCTFail("A readiness result must still belong to the current worker")
            } catch is CancellationError { inspected = true }
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertTrue(inspected)
        XCTAssertFalse(replacement.synchronizer.synchronizationRequestedWhileRunning)
        await fixture.stop()
        await replacement.stop()
    }

    @BigSyncBackgroundActor
    func testCancelledReadinessInspectionCannotAcquireCurrentPass() async throws {
        let fixture = try makeFixture()
        var rejected = false
        fixture.synchronizer.domainPrepublicationHandler = { context in
            let inspection = Task { @BigSyncBackgroundActor in
                try await fixture.worker.domainTransitionReadiness(after: context)
            }
            inspection.cancel()
            do {
                _ = try await inspection.value
                XCTFail("Cancellation must reject before readiness is returned")
            } catch is CancellationError { rejected = true }
            let fresh = try await fixture.worker.domainTransitionReadiness(after: context)
            XCTAssertEqual(fresh, .ready)
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertTrue(rejected)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testPendingWorkDiscoveredAfterReconciliationDrainsBeforeBlockedPublication() async throws {
        let fixture = try makeFixture()
        let blocker = CloudKitSynchronizer.DomainBlocker(code: "reconciliation-debt")
        var runs = [UUID]()
        fixture.adapter.onImport = { fixture.adapter.pending = false }
        fixture.synchronizer.domainPrepublicationHandler = { context in
            runs.append(context.runID)
            if runs.count == 1 {
                // The semantic inspection follows journal forwarding. This
                // generation must still force a fresh drain before .blocked.
                fixture.adapter.onInspection = {
                    fixture.adapter.pending = true
                    fixture.adapter.onInspection = nil
                }
            }
            return [blocker]
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(runs.count, 2)
        XCTAssertNotEqual(runs.first, runs.last)
        XCTAssertEqual(result.terminalBoundary?.runID, runs.last)
        XCTAssertEqual(result.publicationState, .blocked([blocker]))
        XCTAssertNil(result.receipt)
        XCTAssertFalse(fixture.adapter.pending)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testDownloadOnlyAcknowledgesCapturedDeliveryBeforeForwardingAndPreservesNewerBatch() async throws {
        let fixture = try makeFixture()
        fixture.synchronizer.syncMode = .downloadOnly
        let oldIdentity = CommittedInboundIdentity(
            entityType: "DomainFollowUpObject", recordName: "old", disposition: .upsert
        )
        let newerIdentity = CommittedInboundIdentity(
            entityType: "DomainFollowUpObject", recordName: "new", disposition: .delete
        )
        fixture.adapter.committedBatch = .init(deliveryID: "old-delivery", identities: [oldIdentity])
        var events = [String]()
        let blocker = CloudKitSynchronizer.DomainBlocker(code: "download-reconciliation-debt")
        fixture.synchronizer.domainPrepublicationHandler = { context in
            XCTAssertEqual(context.committedInboundIdentities, [oldIdentity])
            events.append("reconcile")
            fixture.adapter.committedBatch = .init(deliveryID: "new-delivery", identities: [newerIdentity])
            fixture.adapter.pending = true
            fixture.adapter.onAcknowledgement = { events.append("ack:" + $0) }
            fixture.adapter.onImport = { events.append("forward") }
            fixture.adapter.onInspection = { events.append("inspect") }
            return [blocker]
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(events, ["reconcile", "ack:old-delivery", "forward", "inspect"])
        XCTAssertEqual(fixture.adapter.committedBatch?.deliveryID, "new-delivery")
        XCTAssertTrue(fixture.adapter.pending)
        XCTAssertEqual(result.completionScope, .downloadOnly)
        XCTAssertEqual(result.publicationState, .blocked([blocker]))
        XCTAssertNil(result.receipt)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testDownloadOnlyRejectsCursorChangedAfterDomainReconciliation() async throws {
        let fixture = try makeFixture()
        fixture.synchronizer.syncMode = .downloadOnly
        fixture.adapter.boundaryIdentifier = "captured-boundary"
        var completions = 0
        fixture.synchronizer.synchronizationCompletionHandler = { _ in completions += 1 }
        fixture.synchronizer.domainPrepublicationHandler = { context in
            XCTAssertEqual(context.consumedServerBoundaryIdentifier, "captured-boundary")
            fixture.adapter.boundaryIdentifier = "newer-boundary"
            return []
        }
        do {
            _ = try await fixture.synchronizer.synchronize()
            XCTFail("A changed inbound cursor cannot publish the captured boundary")
        } catch CloudKitSynchronizer.SyncError.inboundBoundaryChanged {}
        XCTAssertEqual(completions, 0)
        XCTAssertNil(fixture.synchronizer.activeReceiptAuthorizationID)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testRetiredTerminalErrorCannotFailReplacementDrainOrItsWaiters() async throws {
        let fixture = try makeFixture()
        let synchronizer = fixture.synchronizer
        let enteredRetiredCallback = expectation(description: "Retired terminal callback entered")
        let releaseRetiredCallback = DomainFollowUpGate()
        let enteredReplacementConsume = expectation(description: "Replacement consume boundary entered")
        let releaseReplacementConsume = DomainFollowUpGate()
        let enteredReplacementCallback = expectation(description: "Replacement terminal callback entered")
        let releaseReplacementCallback = DomainFollowUpGate()
        var consumedRuns = [UUID]()
        var terminalRuns = [UUID]()
        var publishedRuns = [UUID]()
        synchronizer.synchronizationWillConsumeServerChangesHandler = { context in
            consumedRuns.append(context.runID)
            if consumedRuns.count == 2 {
                enteredReplacementConsume.fulfill()
                await releaseReplacementConsume.wait()
            }
        }
        synchronizer.synchronizationCompletionHandler = {
            if let runID = $0.terminalBoundary?.runID {
                publishedRuns.append(runID)
            }
        }
        synchronizer.domainPrepublicationHandler = { context in
            terminalRuns.append(context.runID)
            if terminalRuns.count == 1 {
                enteredRetiredCallback.fulfill()
                // Deliberately ignore cooperative cancellation, as a domain
                // dependency may deliver an ordinary error after retirement.
                await releaseRetiredCallback.wait()
                throw DomainFollowUpError.retiredTerminalCallback
            }
            if terminalRuns.count == 2 {
                enteredReplacementCallback.fulfill()
                await releaseReplacementCallback.wait()
            }
            return []
        }
        // Release held boundaries before the fixture's cancellation teardown,
        // including when an assertion or request unexpectedly fails.
        addTeardownBlock { @BigSyncBackgroundActor in
            releaseRetiredCallback.open()
            releaseReplacementConsume.open()
            releaseReplacementCallback.open()
        }
        let retiredRequest = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        await fulfillment(of: [enteredRetiredCallback], timeout: 5)
        guard terminalRuns.count == 1 else { return }
        let retiredAttemptID = synchronizer.synchronizationAttemptID
        XCTAssertEqual(synchronizer._testActiveRunCallbackCount, 1)

        synchronizer.cancelSynchronization()
        do {
            _ = try await retiredRequest.value
            XCTFail("Cancellation must release the retired request's waiter")
        } catch is CancellationError {}
        let cancelledAttemptID = synchronizer.synchronizationAttemptID
        let replacementRequest = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        // Observe admission, not an elapsed-time guess. The held callback
        // prevents B's orchestration from crossing its callback barrier.
        guard await waitForCondition(description: "Replacement request admitted", {
            synchronizer.synchronizationAttemptID != cancelledAttemptID
        }) else { return }
        let replacementAttemptID = synchronizer.synchronizationAttemptID
        XCTAssertNotEqual(replacementAttemptID, retiredAttemptID)
        XCTAssertTrue(synchronizer.synchronizationDrainIsActive)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertNil(synchronizer.activeRunContext)
        XCTAssertEqual(synchronizer._testActiveRunCallbackCount, 1)

        let joinedReplacementRequest = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        guard await waitForCondition(description: "Replacement waiter joined", {
            synchronizer.synchronizationRequestedWhileRunning
        }) else { return }
        XCTAssertTrue(publishedRuns.isEmpty)
        releaseRetiredCallback.open()
        await fulfillment(of: [enteredReplacementConsume], timeout: 5)
        guard consumedRuns.count == 2 else { return }
        XCTAssertEqual(synchronizer.synchronizationAttemptID, replacementAttemptID)
        XCTAssertTrue(synchronizer.synchronizationDrainIsActive)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertTrue(publishedRuns.isEmpty)
        XCTAssertEqual(synchronizer._testActiveRunCallbackCount, 0)
        // Joining an active request asks the shared drain for another pass.
        // Both replacement waiters must survive that ordinary tail pass too.
        releaseReplacementConsume.open()
        await fulfillment(of: [enteredReplacementCallback], timeout: 5)
        guard terminalRuns.count == 2 else { return }

        XCTAssertEqual(consumedRuns.count, 3)
        XCTAssertEqual(terminalRuns.last, consumedRuns.last)
        XCTAssertEqual(terminalRuns.count, 2)
        XCTAssertNotEqual(terminalRuns.first, terminalRuns.last)
        XCTAssertTrue(synchronizer.synchronizationDrainIsActive)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertTrue(publishedRuns.isEmpty)
        let replacementRunID = try XCTUnwrap(terminalRuns.last)
        releaseReplacementCallback.open()
        let replacementResult = try await replacementRequest.value
        let joinedResult = try await joinedReplacementRequest.value
        XCTAssertEqual(replacementResult.receipt?.runID, replacementRunID)
        XCTAssertEqual(joinedResult.receipt?.runID, replacementRunID)
        XCTAssertEqual(replacementResult.publicationState, .complete)
        XCTAssertEqual(joinedResult.publicationState, .complete)
        XCTAssertFalse(publishedRuns.contains(try XCTUnwrap(terminalRuns.first)))
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    private func waitForCondition(
        description: String,
        _ condition: @escaping @BigSyncBackgroundActor () -> Bool
    ) async -> Bool {
        let reachedCondition = expectation(description: description)
        let observer = Task { @BigSyncBackgroundActor in
            while !Task.isCancelled {
                if condition() {
                    reachedCondition.fulfill()
                    return
                }
                await Task.yield()
            }
        }
        await fulfillment(of: [reachedCondition], timeout: 5)
        observer.cancel()
        return condition()
    }

    @BigSyncBackgroundActor
    private func makeFixture(account: String = "follow-up-account") throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("bigsync-domain-follow-up-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let transport = DomainFollowUpTransport()
        let zone = CKRecordZone.ID(zoneName: "follow-up-\(UUID().uuidString)",
                                   ownerName: CKCurrentUserDefaultName)
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString, containerIdentifier: "iCloud.bigsync.test",
            database: transport, recordZoneID: zone,
            keyValueStore: FileKeyValueStore(fileURL: directory.appendingPathComponent("state.json")),
            accountIdentifierProvider: { account }, accountStatusProvider: { .available },
            backupDetectionBaseURL: directory.appendingPathComponent("sentinel"),
            initialReplicaBindingAdmissionHandler: { _ in },
            accountReplacementPolicy: .localDatasetRebootstrap,
            logger: Logger(label: "DomainPrepublicationFollowUpTests")
        )
        let adapter = DomainFollowUpAdapter(zoneID: zone)
        synchronizer.addModelAdapter(adapter)
        let worker = BigSyncBackgroundActor()
        worker._test_installSynchronizer(synchronizer, performsAccountAvailabilityPreflight: false)
        let fixture = Fixture(directory: directory, worker: worker,
                              synchronizer: synchronizer, transport: transport, adapter: adapter)
        addTeardownBlock { @BigSyncBackgroundActor in
            await fixture.stop()
            fixture.removeFiles()
        }
        return fixture
    }

    @BigSyncBackgroundActor
    private struct Fixture {
        let directory: URL
        let worker: BigSyncBackgroundActor
        let synchronizer: CloudKitSynchronizer
        let transport: DomainFollowUpTransport
        let adapter: DomainFollowUpAdapter

        func stop() async {
            synchronizer.domainPrepublicationHandler = nil
            synchronizer.synchronizationWillConsumeServerChangesHandler = nil
            adapter.onInspection = nil
            adapter.onImport = nil
            adapter.onAcknowledgement = nil
            synchronizer.synchronizationCompletionHandler = nil
            await synchronizer.cancelSynchronizationAndWait()
        }

        func removeFiles() {
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

private final class DomainFollowUpTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private(set) var recordMutationCount = 0
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {}
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        CKRecordZone(zoneID: identifier)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {}
    func modifyRecords(saving recordsToSave: [CKRecord], deleting recordIDsToDelete: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool)
        async throws -> CloudKitRecordMutationResults {
        recordMutationCount += 1
        return .init(saveResults: [:], deleteResults: [:])
    }
    func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?)
        async throws -> CloudKitDatabaseChangePage {
        .init(cursor: DatabaseChangeCursor(serializedData: Data("follow-up-db".utf8)),
              changedZoneIDs: [], deletions: [], moreComing: false)
    }
    func recordZoneChanges(in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
                           desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?)
        async throws -> CloudKitRecordZoneChangePage {
        .init(cursor: RecordZoneChangeCursor(serializedData: Data("follow-up-zone".utf8)),
              records: [], deletedRecordIDs: [], moreComing: false)
    }
}

private final class DomainFollowUpAdapter: NSObject, ModelAdapter, ChangeFeedResetMigrating,
    TerminalSynchronizationStateModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    private var rebuilding = false
    @BigSyncBackgroundActor var pending = false
    @BigSyncBackgroundActor var blockers = [CloudKitSynchronizer.DomainBlocker]()
    @BigSyncBackgroundActor var onInspection: (() -> Void)?
    @BigSyncBackgroundActor var onImport: (() -> Void)?
    @BigSyncBackgroundActor var onAcknowledgement: ((String) -> Void)?
    @BigSyncBackgroundActor var committedBatch: CommittedInboundIdentityBatch?
    @BigSyncBackgroundActor var boundaryIdentifier: String?
    init(zoneID: CKRecordZone.ID) { recordZoneID = zoneID }
    var hasChanges: Bool { false }
    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func prepareChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws { rebuilding = true }
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws {}
    func isChangeFeedServerBootstrapActive() async -> Bool { rebuilding }
    func changeFeedResetCompletionIsDurable(accountScopeIdentifier: String, epoch: Int,
                                            mode: ChangeFeedResetMode) async throws -> Bool { !rebuilding }
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int,
                                                 mode: ChangeFeedResetMode) async throws {}
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws { rebuilding = false }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
                               recordID: $0.element.recordID), disposition: .applied)
        }
    }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] {
        recordIDs.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: "DomainFollowUpObject", recordID: $0.element),
                  disposition: .appliedTombstone)
        }
    }
    func persistImportedChanges() async throws {}
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] { [] }
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {}
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(accountScopeIdentifier: String, replicaBindingGenerationIdentifier: String?,
                                          containerIdentifier: String, databaseScope: CKDatabase.Scope) throws -> String? { boundaryIdentifier }
    @BigSyncBackgroundActor func changeFeedEpoch() throws -> Int? { nil }
    @BigSyncBackgroundActor func didFinishImport() async throws { onImport?() }
    @BigSyncBackgroundActor
    func pendingCommittedInboundIdentityBatch() throws -> CommittedInboundIdentityBatch? { committedBatch }
    @BigSyncBackgroundActor
    func acknowledgeCommittedInboundIdentityBatch(deliveryID: String) async throws {
        onAcknowledgement?(deliveryID)
        if committedBatch?.deliveryID == deliveryID { committedBatch = nil }
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor func hasPendingChangesAtTerminalBoundary() throws -> Bool { pending }
    @BigSyncBackgroundActor
    func semanticPublicationBlockers() async throws -> [CloudKitSynchronizer.DomainBlocker] {
        onInspection?()
        return blockers
    }
}

private enum DomainFollowUpError: Error {
    case retiredTerminalCallback
}

@BigSyncBackgroundActor
private final class DomainFollowUpGate {
    private var isOpen = false
    private var waiters = [CheckedContinuation<Void, Never>]()

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let continuations = waiters
        waiters.removeAll()
        continuations.forEach { $0.resume() }
    }
}
