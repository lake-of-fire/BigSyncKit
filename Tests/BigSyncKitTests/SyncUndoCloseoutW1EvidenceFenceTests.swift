import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testInvalidatedFenceCannotCertifyUnjournaledLiveObject() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let values = try BigSyncRecordFingerprint.fields(of: object)
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        // Deliberate evidence corruption only: production disappearance keeps
        // the live recreation's journal in the same target transaction.
        try realm.write {
            BigSyncRecordBaseline.invalidate(recordName: incoming.recordID.recordName, in: realm)
        }
        let revision = baseline.revision
        XCTAssertTrue(baseline.isComparisonInvalidated)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: object), values)
        let issue = "invalidated-comparison-unexplained-live-target:" + incoming.recordID.recordName
        let audit = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(audit.isClean)
        XCTAssertEqual(audit.invalidatedBaselineCount, 1)
        XCTAssertTrue(audit.issues.contains(issue))
        XCTAssertThrowsError(try adapter.hasPendingChangesAtTerminalBoundary()) { error in
            XCTAssertTrue((error as? BigSyncComparisonEvidenceError)?.issues.contains(issue) == true)
        }
        let blockers = try await adapter.semanticPublicationBlockers()
        XCTAssertTrue(blockers.contains { $0.code == "comparison-evidence-inconsistent" })
        // Restart cannot reinterpret the revision fence as accepted evidence.
        let (restarted, reopened) = try await restart(adapter)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        XCTAssertThrowsError(try restarted.hasPendingChangesAtTerminalBoundary())
        // A genuine server observation repairs comparison evidence; neither
        // the audit nor the fence is authority to manufacture a local write.
        _ = try await deliver([incoming], to: restarted)
        let recovered = try XCTUnwrap(reopened.objects(W1ContractNote.self).first)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: recovered), values)
        XCTAssertFalse(try XCTUnwrap(reopened.objects(BigSyncRecordBaseline.self).first).isComparisonInvalidated)
        try await quiet(restarted, realm: reopened)
        let cleanAudit = try await restarted.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertTrue(cleanAudit.isClean, cleanAudit.issues.joined(separator: ","))
    }
}

// These methods use the actual file-backed W1 fixtures and adapter entries.
// Provisional changes deliberately remain open on the exact adapter Realm;
// tests never treat a synthetic CloudKit record as signed transport evidence.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testDisappearancePublicationExcludesHeldTargetGeneration() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "committed local", time: 30,
                                        realm: realm, adapter: adapter)
        let name = incoming.recordID.recordName
        adapter._testAfterDisappearanceTargetWrite = {
            realm.beginWrite()
            object.text = "provisional local"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
        defer {
            adapter._testAfterDisappearanceTargetWrite = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        let result = try await adapter.reconcilePhysicalDeletion(
            recordID: incoming.recordID, type: W1ContractNote.self, in: realm)
        XCTAssertEqual(result, .preservedNewerLive(generation: generation))
        XCTAssertTrue(realm.isInWriteTransaction)
        let provisionalGeneration = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
        XCTAssertNotEqual(provisionalGeneration, generation)
        let tracked = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(
            ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(tracked.entityState, .new)
        XCTAssertEqual(tracked.pendingGeneration, generation)
        XCTAssertNil(tracked.encodedRecord)
        XCTAssertEqual(object.text, "provisional local")
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertEqual(object.text, "committed local")
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 30))
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                   forPrimaryKey: name)?.generation, generation)
        XCTAssertEqual(tracked.pendingGeneration, generation)
    }

    @BigSyncBackgroundActor
    func testDisappearancePublicationIgnoresHeldTargetRevision() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "retained", time: 30,
                                        realm: realm, adapter: adapter)
        let name = incoming.recordID.recordName
        adapter._testAfterDisappearanceTargetWrite = {
            realm.beginWrite()
            // An independent provisional evidence transition is not a newer
            // committed CAS fence and cannot suppress this tracking phase.
            BigSyncRecordBaseline.invalidate(recordName: name, in: realm)
        }
        defer {
            adapter._testAfterDisappearanceTargetWrite = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        _ = try await adapter.reconcilePhysicalDeletion(
            recordID: incoming.recordID, type: W1ContractNote.self, in: realm)
        XCTAssertTrue(realm.isInWriteTransaction)
        let provisional = try XCTUnwrap(realm.object(
            ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision)
        let committed = try XCTUnwrap(realm.freeze().object(
            ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision)
        XCTAssertNotEqual(provisional, committed)
        let tracked = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(
            ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(tracked.entityState, .new)
        XCTAssertEqual(tracked.pendingGeneration, generation)
        XCTAssertNil(tracked.encodedRecord)
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertEqual(realm.object(ofType: BigSyncRecordBaseline.self,
                                   forPrimaryKey: name)?.revision, committed)
    }

    @BigSyncBackgroundActor
    func testDeletionPreparationRejectsHeldTargetTombstone() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "still live", time: 30,
                                        realm: realm, adapter: adapter)
        realm.beginWrite()
        // Exercise another owner's intermediate value before its final
        // metadata refresh. The preceding committed journal is still live.
        object.isDeleted = true
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        let proof = try await adapter.preparePhysicalDeletionEvidence(
            recordID: incoming.recordID, type: W1ContractNote.self,
            generation: generation, in: realm)
        XCTAssertNil(proof, "Uncommitted deletion must not authorize a server delete")
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(object.isDeleted)
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(object.text, "still live")
    }

    @BigSyncBackgroundActor
    func testDeletionPreparationPreservesCommittedTombstoneDuringHeldResurrection() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation)
        let revision = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: incoming.recordID.recordName)?.revision)
        realm.beginWrite()
        object.isDeleted = false
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        let proof = try await adapter.preparePhysicalDeletionEvidence(
            recordID: incoming.recordID, type: W1ContractNote.self,
            generation: generation, in: realm)
        XCTAssertNotNil(proof)
        XCTAssertEqual(proof?.revision, revision)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertFalse(object.isDeleted)
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertTrue(object.isDeleted)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testDeletionPreparationCannotPublishProvisionalAcknowledgement() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        try await adapter.didFinishImport()
        let name = incoming.recordID.recordName
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                    forPrimaryKey: name)?.generation)
        let tracked = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(
            ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(tracked.entityState, .deletedLocally)
        let originalArchive = tracked.encodedRecord
        realm.beginWrite()
        // Deliberately emulate an uncommitted target acknowledgement. It must
        // not be interpreted as the durable crash-recovery disposition.
        realm.delete(try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                               forPrimaryKey: name)))
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        let proof = try await adapter.preparePhysicalDeletionEvidence(
            recordID: incoming.recordID, type: W1ContractNote.self,
            generation: generation, in: realm)
        XCTAssertNotNil(proof)
        XCTAssertEqual(tracked.entityState, .deletedLocally)
        XCTAssertEqual(tracked.pendingGeneration, generation)
        XCTAssertEqual(tracked.encodedRecord, originalArchive)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                   forPrimaryKey: name)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testDisappearanceCapturesCommittedFenceBeforeHeldOwnerAbort() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "survives", time: 30,
                                        realm: realm, adapter: adapter)
        realm.beginWrite()
        BigSyncRecordBaseline.invalidate(recordName: incoming.recordID.recordName, in: realm)
        adapter._testBeforeRemoteDeletionTargetWrite = {
            XCTAssertTrue(realm.isInWriteTransaction)
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        defer {
            adapter._testBeforeRemoteDeletionTargetWrite = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        let result = try await adapter.reconcilePhysicalDeletion(
            recordID: incoming.recordID, type: W1ContractNote.self, in: realm)
        XCTAssertEqual(result, .preservedNewerLive(generation: generation))
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(object.text, "survives")
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testDisappearanceStillRejectsCommittedSuccessorFence() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "survives", time: 30,
                                        realm: realm, adapter: adapter)
        let name = incoming.recordID.recordName
        adapter._testBeforeRemoteDeletionTargetWrite = {
            try realm.write { BigSyncRecordBaseline.invalidate(recordName: name, in: realm) }
        }
        defer { adapter._testBeforeRemoteDeletionTargetWrite = nil }
        do {
            _ = try await adapter.reconcilePhysicalDeletion(
                recordID: incoming.recordID, type: W1ContractNote.self, in: realm)
            XCTFail("A committed successor still requires page replay")
        } catch let error as RealmSwiftInboundTargetChangedError {
            XCTAssertEqual(error.recordName, name)
        }
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                   forPrimaryKey: name)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testDisappearanceDoesNotJournalProvisionalLegacyTracking() async throws {
        try await exerciseHeldLegacyTracking(committedLocalWork: false)
    }

    @BigSyncBackgroundActor
    func testDisappearancePreservesCommittedLegacyTrackingBehindProvisionalClear() async throws {
        try await exerciseHeldLegacyTracking(committedLocalWork: true)
    }

    @BigSyncBackgroundActor
    private func exerciseHeldLegacyTracking(committedLocalWork: Bool) async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let name = incoming.recordID.recordName
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let tracked = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        let modified = object.modifiedAt, explicitlyModified = object.explicitlyModifiedAt
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        if committedLocalWork {
            // Model the supported pre-journal recovery case: committed dirty
            // tracking is the only remaining local-work evidence.
            try tracking.write {
                tracked.entityState = .changed
                tracked.setPendingMutation(generation: "legacy-committed",
                    replicaBindingGenerationIdentifier: "w1-binding")
            }
        }
        tracking.beginWrite()
        tracked.entityState = committedLocalWork ? .synced : .changed
        if committedLocalWork { tracked.clearPendingMutation() }
        else {
            tracked.setPendingMutation(generation: "legacy-provisional",
                replicaBindingGenerationIdentifier: "w1-binding")
        }
        adapter._testAfterDisappearanceTargetWrite = {
            XCTAssertTrue(tracking.isInWriteTransaction,
                          "Target reconciliation must not settle the tracking owner")
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
        }
        defer {
            adapter._testAfterDisappearanceTargetWrite = nil
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
        }
        let result = try await adapter.reconcilePhysicalDeletion(
            recordID: incoming.recordID, type: W1ContractNote.self, in: realm)
        let mutation = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
        if committedLocalWork {
            let generation = try XCTUnwrap(mutation?.generation)
            XCTAssertEqual(result, .preservedNewerLive(generation: generation))
            XCTAssertFalse(object.isDeleted)
            XCTAssertEqual(tracked.entityState, .new)
            XCTAssertEqual(tracked.pendingGeneration, generation)
        } else {
            XCTAssertEqual(result, .appliedTombstone)
            XCTAssertTrue(object.isDeleted)
            XCTAssertNil(mutation)
            XCTAssertEqual(tracked.entityState, .deletedRemotely)
            XCTAssertNil(tracked.pendingGeneration)
        }
        XCTAssertEqual(object.modifiedAt, modified)
        XCTAssertEqual(object.explicitlyModifiedAt, explicitlyModified)
        XCTAssertNil(tracked.encodedRecord)
    }
}
