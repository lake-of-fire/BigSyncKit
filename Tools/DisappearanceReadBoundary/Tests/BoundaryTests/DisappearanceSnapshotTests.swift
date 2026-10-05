import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private struct Fixture: Sendable {
    let target: Realm
    let tracking: Realm
    let adapter: RealmSwiftAdapter
    let record: CKRecord.ID
    var name: String { record.recordName }

    @BigSyncBackgroundActor
    init(deleted: Bool = false, journal: Bool = true,
         state: SyncedEntityState = .changed, invalidated: Bool = false) throws {
        let target = Realm(), tracking = Realm()
        self.target = target; self.tracking = tracking
        adapter = RealmSwiftAdapter(target: target, tracking: tracking)
        record = .init(recordName: HarnessNote.className() + ".note", zoneID: adapter.recordZoneID)
        let name = record.recordName
        try target.write {
            let note = HarnessNote(); note.isDeleted = deleted; target.add(note)
            let baseline = BigSyncRecordBaseline(); baseline.recordName = name
            if invalidated {
                baseline.isComparisonInvalidated = true
                baseline.serverChangeTag = nil; baseline.acceptedSystemFields = nil
            }
            target.add(baseline)
            if journal { let row = BigSyncPendingMutation(); row.recordName = name; target.add(row) }
        }
        try tracking.write {
            let row = SyncedEntity(entityType: HarnessNote.className(), identifier: name, state: state.rawValue)
            if journal || state == .deletedLocally || state == .changed {
                row.setPendingMutation(generation: "generation-1", replicaBindingGenerationIdentifier: "binding-1")
            }
            tracking.add(row)
        }
    }
    @BigSyncBackgroundActor func note(in realm: Realm? = nil) throws -> HarnessNote {
        try XCTUnwrap((realm ?? target).object(ofType: HarnessNote.self, forPrimaryKey: "note"))
    }
    @BigSyncBackgroundActor func baseline() throws -> BigSyncRecordBaseline {
        try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name))
    }
    @BigSyncBackgroundActor func mutation() -> BigSyncPendingMutation? {
        target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
    }
    @BigSyncBackgroundActor func entity() throws -> SyncedEntity {
        try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
    }
    @BigSyncBackgroundActor func putMutation(_ generation: String) {
        let row = BigSyncPendingMutation(); row.recordName = name; row.generation = generation; target.add(row)
    }
    @BigSyncBackgroundActor func reconcile() async throws -> InboundDeletionDisposition {
        try await adapter.reconcilePhysicalDeletion(recordID: record, type: HarnessNote.self, in: target)
    }
    @BigSyncBackgroundActor func prepare() async throws -> BigSyncPreparedDeletionEvidence? {
        try await adapter.preparePhysicalDeletionEvidence(recordID: record, type: HarnessNote.self,
            generation: "generation-1", in: target)
    }
    @BigSyncBackgroundActor func finish() {
        adapter._testAfterDisappearanceTargetWrite = nil
        adapter._testBeforeRemoteDeletionTargetWrite = nil
        if target.isInWriteTransaction { target.cancelWrite() }
        if tracking.isInWriteTransaction { tracking.cancelWrite() }
    }
}

@BigSyncBackgroundActor
private func exerciseSnapshots(deleted: Bool = false, journal: Bool = true,
                 state: SyncedEntityState = .changed, invalidated: Bool = false,
                 _ test: @BigSyncBackgroundActor @Sendable (Fixture) async throws -> Void) async throws {
    let f = try Fixture(deleted: deleted, journal: journal, state: state, invalidated: invalidated)
    defer { f.finish() }
    try await test(f)
}

final class DisappearanceSnapshotTests: XCTestCase {
    nonisolated func testPublicationExcludesProvisionalGeneration() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                try f.target.beginWrite(); f.putMutation("generation-provisional")
                try f.note().isDeleted = true
            }
            _ = try await f.reconcile()
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertEqual(f.mutation()?.generation, "generation-provisional")
            XCTAssertEqual(try f.entity().entityState, .new)
            XCTAssertEqual(try f.entity().pendingGeneration, "generation-1")
            XCTAssertNil(try f.entity().encodedRecord)
            f.target.cancelWrite()
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testPublicationDoesNotSkipCommittedFenceForProvisionalRevision() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                try f.target.beginWrite(); try f.baseline().revision = "provisional-revision"
            }
            _ = try await f.reconcile()
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertEqual(try f.baseline().revision, "provisional-revision")
            XCTAssertEqual(try f.entity().entityState, .new)
            XCTAssertNil(try f.entity().encodedRecord)
        }
    }

    nonisolated func testPublicationExcludesUncommittedResurrection() async throws {
        try await exerciseSnapshots(journal: false, state: .synced) { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                try f.target.beginWrite(); try f.note().isDeleted = false; f.putMutation("provisional")
            }
            let outcome = try await f.reconcile()
            XCTAssertEqual(outcome, .appliedTombstone)
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertFalse(try f.note().isDeleted)
            XCTAssertEqual(try f.entity().entityState, .deletedRemotely)
            XCTAssertNil(try f.entity().pendingGeneration)
            f.target.cancelWrite()
            XCTAssertTrue(try f.note().isDeleted)
            XCTAssertNil(f.mutation())
        }
    }

    nonisolated func testPublicationPreservesCommittedSuccessorFenceAndTracking() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                try f.target.write {
                    try f.baseline().revision = "newer-committed"
                    try f.note().text = "newer"; f.putMutation("generation-2")
                }
                try f.tracking.write {
                    let entity = try f.entity(); entity.entityState = .changed
                    entity.pendingGeneration = "generation-2"; entity.encodedRecord = Data([2])
                }
            }
            _ = try await f.reconcile()
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().pendingGeneration, "generation-2")
            XCTAssertEqual(try f.entity().encodedRecord, Data([2]))
            XCTAssertEqual(try f.note().text, "newer")
        }
    }

    nonisolated func testPreparationRejectsProvisionalTombstone() async throws {
        try await exerciseSnapshots { f in
            try f.target.beginWrite(); try f.note().isDeleted = true
            let evidence = try await f.prepare()
            XCTAssertNil(evidence, "An uncommitted tombstone is not permission to delete on the server")
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertTrue(try f.note().isDeleted)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testPreparationKeepsCommittedDeletionAcrossProvisionalResurrection() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            try f.target.beginWrite(); try f.note().isDeleted = false
            let evidence = try await f.prepare()
            XCTAssertNotNil(evidence)
            XCTAssertEqual(evidence?.revision, "revision-1")
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertFalse(try f.note().isDeleted)
        }
    }

    nonisolated func testPreparationDoesNotTreatProvisionalAcknowledgementAsDurable() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally, invalidated: true) { f in
            try f.target.beginWrite(); f.target.delete(try XCTUnwrap(f.mutation()))
            let evidence = try await f.prepare()
            XCTAssertNotNil(evidence)
            XCTAssertEqual(try f.entity().entityState, .deletedLocally)
            XCTAssertEqual(try f.entity().pendingGeneration, "generation-1")
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertNil(f.mutation())
            f.target.cancelWrite()
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testTrackingRepairResamplesAfterPreparationSuspends() async throws {
        try await exerciseSnapshots(deleted: true, journal: false, state: .deletedLocally, invalidated: true) { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                try f.target.write { try f.note().isDeleted = false; f.putMutation("generation-2") }
            }
            let evidence = try await f.prepare()
            XCTAssertNil(evidence)
            XCTAssertEqual(try f.entity().entityState, .new)
            XCTAssertEqual(try f.entity().pendingGeneration, "generation-2")
            XCTAssertEqual(f.mutation()?.generation, "generation-2")
        }
    }

    nonisolated func testReconciliationCapturesCommittedRevisionBeforeOwnerAbort() async throws {
        try await exerciseSnapshots { f in
            try f.target.beginWrite(); try f.baseline().revision = "uncommitted"
            f.adapter._testBeforeRemoteDeletionTargetWrite = { f.target.cancelWrite() }
            let outcome = try await f.reconcile()
            XCTAssertEqual(outcome, .preservedNewerLive(generation: "generation-1"))
            XCTAssertEqual(try f.entity().entityState, .new)
            XCTAssertFalse(try f.note().isDeleted)
        }
    }

    nonisolated func testReconciliationRejectsActuallyCommittedSuccessor() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testBeforeRemoteDeletionTargetWrite = {
                try f.target.write { try f.baseline().revision = "committed-successor" }
            }
            do { _ = try await f.reconcile(); XCTFail("Expected live CAS rejection") }
            catch is RealmSwiftInboundTargetChangedError {}
            XCTAssertEqual(try f.baseline().revision, "committed-successor")
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testLegacyRecoveryDoesNotPromoteProvisionalTrackingWork() async throws {
        try await exerciseSnapshots(journal: false, state: .synced) { f in
            try f.tracking.beginWrite(); try f.entity().entityState = .changed
            f.adapter._testAfterDisappearanceTargetWrite = { f.tracking.cancelWrite() }
            let outcome = try await f.reconcile()
            XCTAssertEqual(outcome, .appliedTombstone)
            XCTAssertTrue(try f.note().isDeleted)
            XCTAssertNil(f.mutation(), "Provisional tracking is not authority to create a durable journal")
            XCTAssertEqual(try f.entity().entityState, .deletedRemotely)
        }
    }

    nonisolated func testLegacyRecoveryPreservesCommittedWorkBehindProvisionalClear() async throws {
        try await exerciseSnapshots(journal: false, state: .changed) { f in
            try f.tracking.beginWrite(); try f.entity().entityState = .synced
            f.adapter._testAfterDisappearanceTargetWrite = { f.tracking.cancelWrite() }
            let outcome = try await f.reconcile()
            guard case .preservedNewerLive = outcome else { return XCTFail("Committed local work was discarded") }
            XCTAssertFalse(try f.note().isDeleted)
            XCTAssertNotNil(f.mutation())
            XCTAssertEqual(try f.entity().entityState, .new)
        }
    }

    nonisolated func testRefreshCallbackCannotSupplyProvisionalDeletionEvidence() async throws {
        try await exerciseSnapshots { f in
            f.target.onRefresh = {
                try! f.target.beginWrite()
                f.target.object(ofType: HarnessNote.self, forPrimaryKey: "note")!.isDeleted = true
            }
            let evidence = try await f.prepare()
            XCTAssertNil(evidence)
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertTrue(try f.note().isDeleted)
        }
    }

    nonisolated func testTransportReplacementStillRejectsTrackingPublication() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = { f.adapter.cancellationGeneration += 1 }
            do { _ = try await f.reconcile(); XCTFail("Expected the original transport cut to reject") }
            catch is CancellationError {}
            XCTAssertTrue(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testUnexplainedCommittedLiveObjectStillRejectsRecovery() async throws {
        try await exerciseSnapshots(journal: false, state: .deletedLocally, invalidated: true) { f in
            do { _ = try await f.prepare(); XCTFail("Invalidated evidence cannot certify unjournaled live data") }
            catch BigSyncRecordRebaseError.inconsistentReceipt(let name) { XCTAssertEqual(name, f.name) }
            XCTAssertEqual(try f.entity().entityState, .deletedLocally)
            XCTAssertFalse(try f.note().isDeleted)
        }
    }
}
