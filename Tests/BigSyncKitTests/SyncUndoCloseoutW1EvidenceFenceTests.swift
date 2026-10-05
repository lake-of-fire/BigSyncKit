import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit


// These are committed-read tests, not permission to mutate another owner's
// transaction. The held writes use the exact operational W1 Realm handles;
// direct journal removal deliberately emulates an uncommitted acknowledgement.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testPendingInventoryDoesNotExposeProvisionalJournalInsertion() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        XCTAssertTrue(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]).isEmpty)
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "provisional"
        object.refreshChangeMetadata(explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40))
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
        XCTAssertTrue(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]).isEmpty)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertEqual(object.text, "provisional")
    }

    @BigSyncBackgroundActor
    func testPendingInventoryKeepsCommittedDebtDuringProvisionalRemoval() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        try realm.write {
            object.text = "committed"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let before = try adapter.pendingMutationInventory(entityTypes: [W1ContractNote.className()])
        XCTAssertEqual(before.count, 1)
        let mutation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName))
        let generation = mutation.generation
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        realm.delete(mutation)
        XCTAssertEqual(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]), before)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        realm.cancelWrite()
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testPendingInventoryKeepsCommittedFieldsDuringProvisionalEdit() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        try realm.write {
            object.text = "committed"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let before = try adapter.pendingMutationInventory(entityTypes: [W1ContractNote.className()])
        XCTAssertEqual(before.count, 1)
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "provisional"
        object.refreshChangeMetadata(explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40))
        XCTAssertEqual(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]), before)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 40))
    }

    @BigSyncBackgroundActor
    func testPendingInventoryDoesNotBorrowProvisionalTargetTombstone() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        try realm.write {
            object.text = "live"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let before = try adapter.pendingMutationInventory(entityTypes: [W1ContractNote.className()])
        XCTAssertEqual(before.map(\.isDeletion), [false])
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        // Before this owner's final refresh, even the journal is unchanged.
        object.isDeleted = true
        XCTAssertEqual(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]), before)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(object.isDeleted)
    }

    @BigSyncBackgroundActor
    func testPendingInventoryRetainsCommittedTombstoneDuringProvisionalResurrection() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let before = try adapter.pendingMutationInventory(entityTypes: [W1ContractNote.className()])
        XCTAssertEqual(before.map(\.isDeletion), [true])
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.isDeleted = false
        XCTAssertEqual(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]), before)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertFalse(object.isDeleted)
    }

    @BigSyncBackgroundActor
    func testPendingInventoryResamplesAfterIndependentCommit() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        XCTAssertTrue(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]).isEmpty)
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
        let after = try adapter.pendingMutationInventory(entityTypes: [W1ContractNote.className()])
        XCTAssertEqual(after.map(\.isDeletion), [true])
        XCTAssertEqual(after.map(\.changedAt), [Date(timeIntervalSinceReferenceDate: 40)])
        XCTAssertFalse(realm.isInWriteTransaction)
    }

    @BigSyncBackgroundActor
    func testEmptyPendingInventorySelectionsLeaveHeldOwnersUntouched() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        realm.beginWrite()
        tracking.beginWrite()
        defer {
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        object.text = "held"
        XCTAssertTrue(try adapter.pendingMutationInventory(entityTypes: []).isEmpty)
        XCTAssertTrue(try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: []).isEmpty)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertEqual(object.text, "held")
    }

    @BigSyncBackgroundActor
    func testTrackingGenerationProofIgnoresProvisionalRemoval() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "pending", time: 30,
            realm: realm, adapter: adapter)
        let name = incoming.recordID.recordName
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: [name]),
            [name: generation])
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        tracking.delete(entity)
        XCTAssertEqual(try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: [name]),
            [name: generation])
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertNil(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testTrackingGenerationProofIgnoresProvisionalReplacementThenSeesCommit() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "pending", time: 30,
            realm: realm, adapter: adapter)
        let name = incoming.recordID.recordName
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        entity.setPendingMutation(generation: "independent-successor",
            replicaBindingGenerationIdentifier: "w1-binding")
        XCTAssertEqual(try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: [name]),
            [name: generation])
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertEqual(entity.pendingGeneration, "independent-successor")
        try tracking.commitWrite()
        XCTAssertEqual(try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: [name]),
            [name: "independent-successor"])
    }

    @BigSyncBackgroundActor
    func testJournalForwardingDoesNotPublishProvisionalGeneration() async throws {
        try await assertCommittedJournalForwarding(committedDeletion: false) { realm, object, _ in
            object.text = "provisional generation"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
    }

    @BigSyncBackgroundActor
    func testJournalForwardingKeepsDebtBehindProvisionalJournalRemoval() async throws {
        try await assertCommittedJournalForwarding(committedDeletion: false) { realm, _, name in
            realm.delete(try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)))
        }
    }

    @BigSyncBackgroundActor
    func testJournalForwardingDoesNotPublishProvisionalDeletionDisposition() async throws {
        try await assertCommittedJournalForwarding(committedDeletion: false) { _, object, _ in
            object.isDeleted = true
        }
    }

    @BigSyncBackgroundActor
    func testJournalForwardingPreservesCommittedDeletionDuringProvisionalResurrection() async throws {
        try await assertCommittedJournalForwarding(committedDeletion: true) { _, object, _ in
            object.isDeleted = false
        }
    }

    @BigSyncBackgroundActor
    private func assertCommittedJournalForwarding(
        committedDeletion: Bool,
        provisionalChange: @escaping @BigSyncBackgroundActor @Sendable (Realm, W1ContractNote, String) throws -> Void
    ) async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let name = incoming.recordID.recordName
        try realm.write {
            object.text = "committed journal value"
            object.isDeleted = committedDeletion
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let probe = PendingJournalPublicationProbe()
        defer {
            adapter._testBeforePendingMutationTrackingWrite = nil
            adapter._testAfterPendingMutationTrackingWrite = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        adapter._testBeforePendingMutationTrackingWrite = {
            adapter._testBeforePendingMutationTrackingWrite = nil
            probe.beforeCount += 1
            realm.beginWrite()
            try provisionalChange(realm, object, name)
        }
        adapter._testAfterPendingMutationTrackingWrite = {
            adapter._testAfterPendingMutationTrackingWrite = nil
            defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
            probe.afterCount += 1
            XCTAssertTrue(realm.isInWriteTransaction)
            // Assert at publication, before a later drain can conceal a bad
            // intermediate generation by forwarding the restored target.
            let published = try XCTUnwrap(tracking.freeze().object(ofType: SyncedEntity.self,
                forPrimaryKey: name))
            XCTAssertEqual(published.pendingGeneration, generation)
            XCTAssertEqual(published.entityState == .deletedLocally, committedDeletion)
        }
        try await adapter.didFinishImport()
        XCTAssertEqual(probe.beforeCount, 1)
        XCTAssertEqual(probe.afterCount, 1)
        XCTAssertFalse(realm.isInWriteTransaction)
        XCTAssertEqual(object.isDeleted, committedDeletion)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
    }
}

@BigSyncBackgroundActor
private final class PendingJournalPublicationProbe {
    var beforeCount = 0
    var afterCount = 0
}

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

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testAuditRejectsProvisionalCompletionAcrossTargetAndTracking() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "committed-pending", time: 30,
                                        realm: realm, adapter: adapter)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let name = incoming.recordID.recordName
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(before.isClean)
        XCTAssertEqual(before.pendingMutationCount, 1)
        realm.beginWrite()
        defer {
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        // Simulate an incomplete acknowledgement owned by another caller.
        // None of these provisional postimages can certify durable completion.
        object.text = try XCTUnwrap(incoming["text"] as? String)
        object.modifiedAt = try XCTUnwrap(incoming["modifiedAt"] as? Date)
        object.explicitlyModifiedAt = incoming["explicitlyModifiedAt"] as? Date
        realm.delete(try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)))
        tracking.beginWrite()
        entity.entityState = .synced
        entity.clearPendingMutation()
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertFalse(during.isClean)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        XCTAssertNil(entity.pendingGeneration)
        if tracking.isInWriteTransaction { tracking.cancelWrite() }
        if realm.isInWriteTransaction { realm.cancelWrite() }
        let after = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(after, before)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation, generation)
        XCTAssertEqual(object.text, "committed-pending")
    }

    @BigSyncBackgroundActor
    func testAuditRetainsTrackingDebtBehindHeldAcknowledgement() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "pending", time: 30,
                                        realm: realm, adapter: adapter)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
                                                   forPrimaryKey: incoming.recordID.recordName))
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(entity.pendingGeneration, generation)
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        entity.entityState = .synced
        entity.clearPendingMutation()
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertNil(entity.pendingGeneration)
        if tracking.isInWriteTransaction { tracking.cancelWrite() }
        XCTAssertEqual(entity.pendingGeneration, generation)
    }

    @BigSyncBackgroundActor
    func testAuditIgnoresHeldProvisionalTargetMutation() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertTrue(before.isClean, before.issues.joined(separator: ","))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "provisional-edit"
        object.refreshChangeMetadata(explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40))
        let provisionalGeneration = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation)
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertEqual(object.text, "provisional-edit")
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName)?.generation, provisionalGeneration)
        if realm.isInWriteTransaction { realm.cancelWrite() }
        let after = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(after, before)
    }

    @BigSyncBackgroundActor
    func testAuditRetainsSubmittedCandidateBehindProvisionalRemoval() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        _ = try await edit(object, text: "submitted", time: 30, realm: realm, adapter: adapter)
        _ = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(before.unresolvedSubmissionCount, 1)
        let submission = try XCTUnwrap(realm.objects(BigSyncRecordSubmission.self).first)
        let candidateIdentity = submission.candidateIdentity
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        realm.delete(submission)
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertEqual(during.unresolvedSubmissionCount, 1)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        if realm.isInWriteTransaction { realm.cancelWrite() }
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, candidateIdentity)
    }

    @BigSyncBackgroundActor
    func testAuditRetainsRelationshipDebtBehindProvisionalRemoval() async throws {
        let (adapter, _, _, incoming) = try await acceptedNote()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
                                                   forPrimaryKey: incoming.recordID.recordName))
        let relationship = PendingRelationship()
        relationship.relationshipName = "audit-pending-relationship"
        relationship.targetIdentifier = incoming.recordID.recordName
        relationship.forSyncedEntity = entity
        try tracking.write { tracking.add(relationship) }
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(before.pendingRelationshipCount, 1)
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        tracking.delete(relationship)
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertEqual(during.pendingRelationshipCount, 1)
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertTrue(tracking.objects(PendingRelationship.self).isEmpty)
        if tracking.isInWriteTransaction { tracking.cancelWrite() }
        XCTAssertEqual(tracking.objects(PendingRelationship.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testAuditResamplesActuallyCommittedOwnerOnNextInvocation() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertTrue(before.isClean, before.issues.joined(separator: ","))
        try realm.write {
            object.text = "committed-after-audit"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
        let after = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(after.isClean)
        XCTAssertEqual(after.pendingMutationCount, 1)
        XCTAssertNotEqual(after, before)
    }

    // Bind an existing string property as the disposable adapter's account
    // scope, without adding any test-only Realm model to global discovery.
    @BigSyncBackgroundActor
    private func auditAccountScopedFixture() async throws -> (RealmSwiftAdapter, Realm, CKRecord) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("w1-audit-account-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        realmFixtureOwner.ownDirectory(directory)
        let accountProperties = [W1ContractNote.className(): "text"]
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [W1ContractNote.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: [], accountScopePropertyByClassName: accountProperties)
            .install(configurations: [target], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "w1-local", replicaBindingGenerationIdentifier: "w1-binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent("tracking.realm")
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: [],
            accountScopePropertyByClassName: accountProperties,
            recordZoneID: .init(zoneName: "w1-audit-account"),
            logger: Logger(label: "W1AuditAccount"), startSetupTask: false)
        realmFixtureOwner.own(adapter)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "server-text",
            replicaBindingGenerationIdentifier: "w1-binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.w1-closeout",
            databaseScope: .private)
        try await adapter.ensureSetup()
        adapter.invalidateTokens()
        let incoming = try tagged(note(adapter), "audit-account-tag")
        _ = try await deliver([incoming], to: adapter)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        return (adapter, realm, incoming)
    }

    @BigSyncBackgroundActor
    func testAuditAccountFilterIgnoresProvisionalDeparture() async throws {
        let (adapter, realm, incoming) = try await auditAccountScopedFixture()
        let before = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertTrue(before.isClean, before.issues.joined(separator: ","))
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "other-account"
        let during = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertEqual(during, before)
        XCTAssertEqual(during.trackingRecordCount, 1)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertEqual(object.text, "other-account")
    }

    @BigSyncBackgroundActor
    func testAuditAccountFilterDoesNotAdoptProvisionalArrival() async throws {
        let (adapter, realm, _) = try await auditAccountScopedFixture()
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "second-account",
            replicaBindingGenerationIdentifier: "w1-binding")
        let before = try await adapter.auditSynchronizationState(serverRecords: [])
        XCTAssertTrue(before.isClean, before.issues.joined(separator: ","))
        XCTAssertEqual(before.trackingRecordCount, 0)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "second-account"
        let during = try await adapter.auditSynchronizationState(serverRecords: [])
        XCTAssertEqual(during, before)
        XCTAssertEqual(during.trackingRecordCount, 0)
        XCTAssertTrue(realm.isInWriteTransaction)
    }
}

// Exercise the ID-bearing observer processor before a later lifecycle scan can
// hide the lost invalidation. The test owns only its provisional target write.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testObservedJournalDebtSurvivesProvisionalRemovalAndAbort() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let name = incoming.recordID.recordName
        try realm.write {
            object.text = "committed observed edit"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let mutation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name))
        let generation = mutation.generation
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        realm.delete(mutation)
        adapter._test_enqueueObservedJournalRecordNames([name])
        try await adapter._test_processObservedRealmChanges()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.freeze().object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.pendingGeneration, generation)
        XCTAssertTrue(realm.isInWriteTransaction)
        realm.cancelWrite()
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
    }
}
