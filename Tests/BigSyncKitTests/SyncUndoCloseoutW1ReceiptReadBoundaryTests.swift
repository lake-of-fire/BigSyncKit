import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private final class ReceiptReadRefreshSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var armed = false
    private var observed = false
    func arm() { lock.lock(); defer { lock.unlock() }; armed = true }
    func receive() -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard armed, !observed else { return false }
        observed = true
        return true
    }
    var didObserve: Bool { lock.lock(); defer { lock.unlock() }; return observed }
}

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testComparisonReceiptObserverRejectsProvisionalMatchingRevision() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let committedRevision = baseline.revision
        let provisionalRevision = UUID().uuidString
        try target.beginWrite()
        defer { if target.isInWriteTransaction { target.cancelWrite() } }
        baseline.revision = provisionalRevision
        XCTAssertFalse(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: provisionalRevision,
            recordName: incoming.recordID.recordName, in: target))
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: committedRevision,
            recordName: incoming.recordID.recordName, in: target))
        XCTAssertTrue(target.isInWriteTransaction)
        XCTAssertEqual(baseline.revision, provisionalRevision)
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptObserverRetainsCommittedReceiptDuringRemoval() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = baseline.revision
        try target.beginWrite()
        defer { if target.isInWriteTransaction { target.cancelWrite() } }
        target.delete(baseline)
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target))
        XCTAssertTrue(target.isInWriteTransaction)
        XCTAssertNil(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptObserverIgnoresProvisionalInvalidation() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = baseline.revision
        try target.beginWrite()
        defer { if target.isInWriteTransaction { target.cancelWrite() } }
        baseline.isComparisonInvalidated = true
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target))
        XCTAssertTrue(target.isInWriteTransaction)
        XCTAssertTrue(baseline.isComparisonInvalidated)
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptOwnedWriterUsesLiveRevision() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = UUID().uuidString
        try target.beginWrite()
        defer { if target.isInWriteTransaction { target.cancelWrite() } }
        baseline.revision = revision
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target,
            ownsTargetTransaction: true))
        XCTAssertTrue(target.isInWriteTransaction)
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptOwnedWriterRejectsLiveInvalidation() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = baseline.revision
        try target.beginWrite()
        defer { if target.isInWriteTransaction { target.cancelWrite() } }
        baseline.isComparisonInvalidated = true
        XCTAssertFalse(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target,
            ownsTargetTransaction: true))
        XCTAssertTrue(target.isInWriteTransaction)
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptFrozenViewDoesNotAdvance() async throws {
        let (adapter, target, _, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = baseline.revision
        let snapshot = target.freeze()
        try target.write { baseline.revision = UUID().uuidString }
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: snapshot))
        XCTAssertFalse(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target))
    }

    @BigSyncBackgroundActor
    private func exerciseComparisonReceiptRefresh(revokes: Bool) async throws {
        let (adapter, target, object, incoming) = try await acceptedNote()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName))
        let revision = baseline.revision
        let priorAutorefresh = target.autorefresh
        target.autorefresh = false
        let signal = ReceiptReadRefreshSignal()
        let observation = target.observe { notification, _ in
            guard case .didChange = notification, signal.receive() else { return }
            if revokes {
                adapter.cancelSynchronization()
                do { try adapter.prepareForFencedMigrationAfterCancellation() }
                catch { XCTFail("Could not restore cancellation flag: \(error)") }
            }
        }
        defer {
            observation.invalidate()
            target.autorefresh = priorAutorefresh
        }
        let queue = DispatchQueue(label: "test.receipt-read-refresh." + UUID().uuidString)
        let configuration = target.configuration
        let identifier = noteID
        let originalText = object.text
        try queue.sync {
            let writer = try Realm(configuration: configuration, queue: queue)
            try writer.write {
                let value = try XCTUnwrap(writer.object(ofType: W1ContractNote.self,
                    forPrimaryKey: identifier))
                // A local-only metadata notification, not new journal intent.
                value.modifiedAt = value.modifiedAt.addingTimeInterval(1)
            }
        }
        signal.arm()
        do {
            let valid = try adapter._test_comparisonReceiptIsCurrent(
                context: context, revision: revision,
                recordName: incoming.recordID.recordName, in: target)
            XCTAssertFalse(revokes, "Refresh revoked the original receipt observer")
            XCTAssertTrue(valid)
        } catch {
            XCTAssertTrue(revokes)
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertTrue(signal.didObserve, "Must exercise real Realm notification delivery")
        XCTAssertEqual(object.text, originalText)
        XCTAssertEqual(baseline.revision, revision)
        XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
        observation.invalidate()
        try await adapter.unsetCancellation()
        XCTAssertTrue(try adapter._test_comparisonReceiptIsCurrent(
            context: context, revision: revision,
            recordName: incoming.recordID.recordName, in: target))
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptRefreshCancellationGenerationRejectsAndRetries() async throws {
        try await exerciseComparisonReceiptRefresh(revokes: true)
    }

    @BigSyncBackgroundActor
    func testComparisonReceiptCurrentRefreshRemainsValid() async throws {
        try await exerciseComparisonReceiptRefresh(revokes: false)
    }
}
