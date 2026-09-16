import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(RebaseIntegrationBoundaryRow)
private final class RebaseIntegrationBoundaryRow: Object,
    ChangeMetadataRecordable, BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"])
    }

    @Persisted(primaryKey: true) var id = "article"
    @Persisted var epoch = "E0"
    @Persisted var title = "original"
    @Persisted var count = 0
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// These exercise the production adapter, not a second merge implementation.
/// The ordinary deletion path and the value-bearing inbound path are tested
/// separately: a CloudKit deleted-record ID does not contain a lifetime.
final class SyncRebaseIntegrationBoundaryTests: XCTestCase {
    private let lowerNonce = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
    private let higherNonce = UUID(uuidString: "ffffffff-ffff-ffff-ffff-ffffffffffff")!

    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let suffix = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "integration-boundary-target-" + suffix
        target.objectTypes = [RebaseIntegrationBoundaryRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        let policy = BigSyncMutationPolicy(excludedClassNames: [])
        policy.install(configurations: [target], mutationJournalIdentityProvider: {
            .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
        })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "integration-boundary-tracking-" + suffix
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: .init(zoneName: "integration-boundary"),
            logger: Logger(label: "SyncRebaseIntegrationBoundaryTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.integration-boundary", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(
        _ adapter: RealmSwiftAdapter, epoch: String, title: String = "original",
        count: Int = 0, deleted: Bool = false, time: TimeInterval = 10
    ) -> CKRecord {
        let result = CKRecord(
            recordType: RebaseIntegrationBoundaryRow.className(),
            recordID: .init(
                recordName: RebaseIntegrationBoundaryRow.className() + ".article",
                zoneID: adapter.recordZoneID
            )
        )
        result["epoch"] = epoch as CKRecordValue
        result["title"] = title as CKRecordValue
        result["count"] = count as CKRecordValue
        result["isDeleted"] = deleted as CKRecordValue
        result["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        result["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        result["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        return result
    }

    @BigSyncBackgroundActor
    private func deliver(_ record: CKRecord, to adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(in: [record], forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }

    private func object(in realm: Realm) throws -> RebaseIntegrationBoundaryRow {
        try XCTUnwrap(realm.object(ofType: RebaseIntegrationBoundaryRow.self, forPrimaryKey: "article"))
    }

    private func pending(in realm: Realm) throws -> BigSyncPendingMutation {
        try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                  forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
    }

    @BigSyncBackgroundActor
    func testPendingLocalDeletionDoesNotMakeAnOtherwiseValidInboundPageFail() async throws {
        let (adapter, realm) = try await fixture()
        let epoch = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: epoch), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try pending(in: realm).generation
        // This edit is older than the local deletion. Deletion invalidates the
        // field base, but that must not break the existing deletion conflict fence.
        try await deliver(record(adapter, epoch: epoch, title: "remote edit", time: 20), to: adapter)
        realm.refresh()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(try pending(in: realm).generation, generation)
        let proof = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                                               forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
        XCTAssertTrue(proof.isComparisonInvalidated)
    }

    @BigSyncBackgroundActor
    func testValueBearingOldLifetimeDeletionCannotDefeatNewerPendingResetByTimestamp() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.epoch = next
            value.count = 0
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 20))
        }
        try await deliver(record(adapter, epoch: old, count: 7, deleted: true, time: 900), to: adapter)
        realm.refresh()
        XCTAssertFalse(value.isDeleted, "A later unrelated clock is not authority over a successor lifetime")
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
        _ = try pending(in: realm)
    }

    @BigSyncBackgroundActor
    func testValueBearingNewerLifetimeDeletionWinsEvenWithOlderRecordTimestamp() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.title = "unrelated later title"
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 900))
        }
        try await deliver(record(adapter, epoch: next, count: 0, deleted: true, time: 20), to: adapter)
        realm.refresh()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
    }

    @BigSyncBackgroundActor
    func testPendingOldDeletionCannotBlockAnOrderedSuccessor() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 900))
        }
        let oldDeletion = try pending(in: realm).generation
        try await deliver(record(adapter, epoch: next, count: 0, time: 20), to: adapter)
        realm.refresh()
        XCTAssertFalse(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
        XCTAssertNotEqual(try pending(in: realm).generation, oldDeletion,
                          "An old deletion receipt must not acknowledge the replacement lifetime")
        let proof = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
        XCTAssertFalse(proof.isComparisonInvalidated)
    }

    @BigSyncBackgroundActor
    func testPendingNewerDeletionStillRejectsAnOlderLiveLifetime() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.epoch = next
            value.count = 0
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 20))
        }
        let deletion = try pending(in: realm).generation
        try await deliver(record(adapter, epoch: old, count: 7, time: 900), to: adapter)
        realm.refresh()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
        XCTAssertEqual(try pending(in: realm).generation, deletion)
    }

    @BigSyncBackgroundActor
    func testUnknownInitialAncestorStillCannotBeInventedForDifferentPendingValues() async throws {
        let (adapter, realm) = try await fixture()
        let value = RebaseIntegrationBoundaryRow()
        try realm.write {
            realm.add(value)
            value.title = "local creation"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try pending(in: realm).generation
        do {
            try await deliver(record(adapter, epoch: "E0", title: "other creation"), to: adapter)
            XCTFail("An empty cloud migration is not proof of a shared ancestor for simultaneous creations")
        } catch BigSyncRecordRebaseError.missingBaseline {
            // Explicit unresolved input is safer than discarding either value.
        }
        XCTAssertEqual(value.title, "local creation")
        XCTAssertEqual(try pending(in: realm).generation, generation)
    }

    /// Production forwarding is deliberately part of the precondition. The
    /// target journal alone does not exercise the tracking admission gate.
    @BigSyncBackgroundActor
    private func checkForwardedDeletion(incomingKind: Int) async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        let localEpoch = incomingKind == -1 ? next : old
        try await deliver(record(adapter, epoch: localEpoch, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 900))
        }
        let deletionGeneration = try pending(in: realm).generation
        try await adapter.didFinishImport()
        let deletionBatch = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletionBatch.recordIDs.count, 1,
                       "The deletion must actually reach tracking before inbound admission")
        let incomingEpoch = incomingKind == 1 ? next : old
        let received = record(adapter, epoch: incomingEpoch, count: 0, time: 20)
        try await deliver(received, to: adapter)
        realm.refresh()
        if incomingKind == 1 {
            XCTAssertFalse(value.isDeleted)
            XCTAssertEqual(value.epoch, next)
            XCTAssertEqual(value.count, 0)
            let successorGeneration = try pending(in: realm).generation
            XCTAssertNotEqual(successorGeneration, deletionGeneration)
            let proof = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: received.recordID.recordName))
            XCTAssertFalse(proof.isComparisonInvalidated)
            try await adapter.acknowledgeDeletedRecordIDs(
                deletionBatch.recordIDs, from: deletionBatch
            )
            try await adapter.cleanUp()
            realm.refresh()
            XCTAssertFalse(try object(in: realm).isDeleted)
            XCTAssertEqual(try pending(in: realm).generation, successorGeneration)
            let remainingDeletes = try await adapter.prepareDeletionBatch(limit: 10)
            XCTAssertTrue(remainingDeletes.recordIDs.isEmpty)
            let upload = try await adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(upload.records.count, 1)
            XCTAssertEqual(upload.records.first?["epoch"] as? String, next)
            XCTAssertEqual(upload.records.first?["isDeleted"] as? Bool, false)
        } else {
            XCTAssertTrue(value.isDeleted)
            XCTAssertEqual(value.epoch, localEpoch)
            XCTAssertEqual(try pending(in: realm).generation, deletionGeneration)
            let stillPending = try await adapter.prepareDeletionBatch(limit: 10)
            XCTAssertEqual(stillPending.recordIDs, deletionBatch.recordIDs)
        }
    }

    @BigSyncBackgroundActor
    func testForwardedPredecessorDeletionAdmitsSuccessorAndRejectsLateDeleteReceipt() async throws {
        try await checkForwardedDeletion(incomingKind: 1)
    }

    @BigSyncBackgroundActor
    func testForwardedNewerDeletionPreservesItsGenerationAgainstOldLiveRecord() async throws {
        try await checkForwardedDeletion(incomingKind: -1)
    }

    @BigSyncBackgroundActor
    func testForwardedSameLifetimeDeletionPreservesItsGeneration() async throws {
        try await checkForwardedDeletion(incomingKind: 0)
    }


    @BigSyncBackgroundActor
    private func acknowledgeAndRequireQuietSecondDrain(_ adapter: RealmSwiftAdapter, realm: Realm) async throws {
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        // An injected successful reply exercises local receipt accounting;
        // it is not a simulation of server-assigned CloudKit system fields.
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await adapter.didFinishImport()
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
        let baseline = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
        let revision = baseline.revision
        try await adapter.didFinishImport()
        let secondUpload = try await adapter.prepareUploadBatch(limit: 10)
        let secondDeletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertTrue(secondUpload.records.isEmpty)
        XCTAssertTrue(secondDeletion.recordIDs.isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
        XCTAssertEqual(baseline.revision, revision)
    }

    @BigSyncBackgroundActor
    func testForwardedSuccessorFinishesWithAQuietSecondDrain() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.count, 1)
        try await deliver(record(adapter, epoch: next), to: adapter)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await acknowledgeAndRequireQuietSecondDrain(adapter, realm: realm)
        XCTAssertFalse(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
    }

    @BigSyncBackgroundActor
    private func openPersistedBoundaryAdapter(
        target: Realm.Configuration, tracking: Realm.Configuration
    ) async throws -> RealmSwiftAdapter {
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking, targetRealmConfigurations: [target],
            excludedClassNames: [], recordZoneID: .init(zoneName: "integration-boundary"),
            logger: Logger(label: "PersistedBoundaryTests"), startSetupTask: false
        )
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.integration-boundary",
            databaseScope: .private)
        // Do not call resetSyncCaches: that would erase the tracking state
        // whose persistence/reopen behavior this test is meant to exercise.
        try await adapter.ensureSetup()
        adapter.invalidateTokens()
        return adapter
    }

    @BigSyncBackgroundActor
    private func persistForwardedDeletion(
        target: Realm.Configuration, tracking: Realm.Configuration, epoch: String
    ) async throws -> String {
        let adapter = try await openPersistedBoundaryAdapter(target: target, tracking: tracking)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        try await deliver(record(adapter, epoch: epoch, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(batch.recordIDs.count, 1)
        let generation = try pending(in: realm).generation
        adapter.cancelSynchronization()
        await adapter.waitForCancellation()
        adapter.invalidateTokens()
        return generation
    }

    @BigSyncBackgroundActor
    func testFileBackedTrackingReopenStillAdmitsOrderedSuccessor() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("bigsync-forwarded-reopen-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [RebaseIntegrationBoundaryRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent("tracking.realm")
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        let oldGeneration = try await persistForwardedDeletion(target: target, tracking: tracking, epoch: old)
        let reopened = try await openPersistedBoundaryAdapter(target: target, tracking: tracking)
        let realm = try XCTUnwrap(reopened.realmProvider?.targetReaderRealms?.first)
        XCTAssertEqual(try pending(in: realm).generation, oldGeneration)
        let deletion = try await reopened.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.count, 1)
        try await deliver(record(reopened, epoch: next), to: reopened)
        realm.refresh()
        XCTAssertFalse(try object(in: realm).isDeleted)
        XCTAssertEqual(try object(in: realm).epoch, next)
        XCTAssertNotEqual(try pending(in: realm).generation, oldGeneration)
        try await reopened.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await reopened.cleanUp()
        try await acknowledgeAndRequireQuietSecondDrain(reopened, realm: realm)
        reopened.cancelSynchronization()
        await reopened.waitForCancellation()
        reopened.invalidateTokens()
    }

}
