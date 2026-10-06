import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

// Cold restoration reads use task-owned file-backed Realms, never released
// migration inputs or the owner's application data.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    private func publicationRestorationFixture() async throws -> (
        RealmSwiftAdapter, RealmSwiftAdapter.PublicationRestorationInspection,
        Realm.Configuration, Realm.Configuration, BigSyncDurablePublicationEvidence
    ) {
        let (adapter, realm, _, _) = try await acceptedNote()
        let cursor = RecordZoneChangeCursor(serializedData: Data("restoration-committed".utf8))
        try await adapter.saveToken(cursor)
        let preparedInspection = try await adapter.preparePublicationRestorationInspection()
        let inspection = try XCTUnwrap(preparedInspection)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let boundary = try XCTUnwrap(try adapter.consumedServerBoundaryIdentifier(
            accountScopeIdentifier: "w1-account", replicaBindingGenerationIdentifier: "w1-binding",
            containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        let evidence = BigSyncDurablePublicationEvidence(
            domainScopeIdentifier: "w1-domain", accountScopeIdentifier: "w1-account",
            replicaBindingGenerationIdentifier: "w1-binding",
            zoneOwnerName: adapter.recordZoneID.ownerName, zoneName: adapter.recordZoneID.zoneName,
            changeFeedEpoch: try adapter.changeFeedEpoch() ?? 0,
            consumedServerBoundaryIdentifier: boundary, runID: UUID(), publishedAt: Date())
        return (adapter, inspection, realm.configuration, tracking.configuration, evidence)
    }

    @BigSyncBackgroundActor
    func testColdRestorationKeepsCommittedCursorBehindProvisionalRemoval() async throws {
        let (_, inspection, _, trackingConfig, evidence) = try await publicationRestorationFixture()
        // Open on this thread, like matches(), after the final await. The held
        // owner and synchronous inspection may share the native cached handle.
        let owner = try Realm(configuration: trackingConfig)
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        owner.beginWrite()
        defer { if owner.isInWriteTransaction { owner.cancelWrite() } }
        owner.delete(owner.objects(ServerToken.self))
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        XCTAssertTrue(owner.isInWriteTransaction)
        XCTAssertTrue(owner.objects(ServerToken.self).isEmpty)
        owner.cancelWrite()
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
    }

    @BigSyncBackgroundActor
    func testColdRestorationCannotCertifyProvisionalSuccessorCursor() async throws {
        let (adapter, inspection, _, trackingConfig, original) = try await publicationRestorationFixture()
        let owner = try Realm(configuration: trackingConfig)
        let cursorData = Data("restoration-provisional-successor".utf8)
        let successor = BigSyncDurablePublicationEvidence(
            domainScopeIdentifier: original.domainScopeIdentifier,
            accountScopeIdentifier: original.accountScopeIdentifier,
            replicaBindingGenerationIdentifier: original.replicaBindingGenerationIdentifier,
            zoneOwnerName: original.zoneOwnerName, zoneName: original.zoneName,
            changeFeedEpoch: original.changeFeedEpoch,
            consumedServerBoundaryIdentifier: CloudKitSynchronizer.makeConsumedServerBoundaryIdentifier(
                containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private,
                accountScopeIdentifier: original.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: original.replicaBindingGenerationIdentifier,
                recordZoneID: adapter.recordZoneID, changeFeedEpoch: original.changeFeedEpoch,
                cursorData: cursorData), runID: original.runID, publishedAt: original.publishedAt)
        XCTAssertFalse(try inspection.matches(successor, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        let token = try XCTUnwrap(owner.objects(ServerToken.self).first)
        owner.beginWrite()
        defer { if owner.isInWriteTransaction { owner.cancelWrite() } }
        token.token = cursorData
        XCTAssertFalse(try inspection.matches(successor, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        XCTAssertTrue(owner.isInWriteTransaction)
        XCTAssertEqual(token.token, cursorData)
        try owner.commitWrite()
        XCTAssertTrue(try inspection.matches(successor, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        XCTAssertFalse(try inspection.matches(original, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
    }

    @BigSyncBackgroundActor
    func testColdRestorationIgnoresProvisionalTargetJournalUntilCommit() async throws {
        let (_, inspection, targetConfig, _, evidence) = try await publicationRestorationFixture()
        let owner = try Realm(configuration: targetConfig)
        let object = try XCTUnwrap(owner.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        owner.beginWrite()
        defer { if owner.isInWriteTransaction { owner.cancelWrite() } }
        object.text = "provisional independent edit"
        object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
        XCTAssertEqual(owner.objects(BigSyncPendingMutation.self).count, 1)
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        XCTAssertTrue(owner.isInWriteTransaction)
        try owner.commitWrite()
        XCTAssertFalse(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
    }

    @BigSyncBackgroundActor
    func testColdRestorationKeepsOriginalAfterProvisionalTargetJournalRollback() async throws {
        let (_, inspection, targetConfig, _, evidence) = try await publicationRestorationFixture()
        let owner = try Realm(configuration: targetConfig)
        let object = try XCTUnwrap(owner.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let originalText = object.text
        owner.beginWrite()
        defer { if owner.isInWriteTransaction { owner.cancelWrite() } }
        object.text = "rolled-back independent edit"
        object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
        XCTAssertTrue(owner.isInWriteTransaction)
        owner.cancelWrite()
        XCTAssertEqual(object.text, originalText)
        XCTAssertTrue(owner.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(try inspection.matches(evidence, containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private))
    }
}


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
