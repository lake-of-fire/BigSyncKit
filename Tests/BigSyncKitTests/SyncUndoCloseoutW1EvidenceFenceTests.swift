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
