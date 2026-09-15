import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(SyncIntentMutable)
private final class SyncIntentMutable: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

private enum IntentValidationError: Error { case invalid }

@objc(SyncIntentSnapshot)
private final class SyncIntentSnapshot: Object, ChangeMetadataRecordable,
    BigSyncAuthoritativeServerSnapshotModel,
    BigSyncInboundSemanticRecordValidating,
    BigSyncInboundSemanticReplacementValidating,
    BigSyncInboundPendingSemanticReplacementValidating {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = ""
    @Persisted var catalog = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false

    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        guard record["payload"] is String, record["catalog"] is String,
              record["modifiedAt"] is Date else {
            throw IntentValidationError.invalid
        }
    }

    static func validateInboundSemanticReplacement(
        _ record: CKRecord, existingObject: Object?
    ) throws {
        guard let existing = existingObject as? Self else { return }
        guard record.recordID.recordName == className() + "." + existing.id,
              existing.catalog.isEmpty
                || record["catalog"] as? String == existing.catalog else {
            throw IntentValidationError.invalid
        }
    }

    static func validateInboundSemanticPredecessorOfPendingMutation(
        _ record: CKRecord, existingObject: Object
    ) throws {
        guard let existing = existingObject as? Self,
              record.recordID.recordName == className() + "." + existing.id,
              !existing.catalog.isEmpty,
              record["catalog"] as? String == "" else {
            throw IntentValidationError.invalid
        }
    }
}

final class SyncSemanticIntentTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "semantic-intent-target-" + nonce
        target.objectTypes = [SyncIntentMutable.self, SyncIntentSnapshot.self,
                              BigSyncPendingMutation.self]
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "current-installation",
                      replicaBindingGenerationIdentifier: "binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "semantic-intent-tracking-" + nonce
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: .init(zoneName: "semantic-intent"),
            logger: Logger(label: "SyncSemanticIntentTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.intent", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(
        _ type: Object.Type, adapter: RealmSwiftAdapter,
        payload: String, at time: Double, catalog: String = ""
    ) -> CKRecord {
        let record = CKRecord(recordType: type.className(), recordID: .init(
            recordName: type.className() + ".one", zoneID: adapter.recordZoneID
        ))
        record["payload"] = payload as CKRecordValue
        record["catalog"] = catalog as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["explicitlyModifiedAt"] = record["modifiedAt"]
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func beginRestore(_ adapter: RealmSwiftAdapter) async throws {
        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "account", epoch: 91, mode: .backupRestore
        )
        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: "account", epoch: 91, mode: .backupRestore
        )
    }

    @BigSyncBackgroundActor
    func testBackupBootstrapDoesNotReauthorRetainedFutureTimestamp() async throws {
        let (adapter, _) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "retained-backup", at: 2_000
        )], forceSave: true)
        try await beginRestore(adapter)
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "current-server", at: 1_000
        )], forceSave: true)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: SyncIntentMutable.self,
                                    forPrimaryKey: "one")?.payload, "current-server")
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty,
                      "Retained backup data must not become newly authored work")
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(batch.records.isEmpty)
    }

    @BigSyncBackgroundActor
    func testBackupBootstrapStillProtectsGenuineNewMutation() async throws {
        let (adapter, _) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "backup", at: 2_000
        )], forceSave: true)
        try await beginRestore(adapter)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        let value = try XCTUnwrap(realm.object(ofType: SyncIntentMutable.self, forPrimaryKey: "one"))
        try realm.write {
            value.payload = "post-restore-user-edit"
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 3_000))
        }
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "server", at: 4_000
        )], forceSave: true)
        realm.refresh()
        XCTAssertEqual(value.payload, "post-restore-user-edit")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testOrdinaryMutableRecordKeepsExistingTimestampPolicy() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "newer-local", at: 2_000
        )], forceSave: true)
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentMutable.self, adapter: adapter, payload: "older-remote", at: 1_000
        )], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: SyncIntentMutable.self, forPrimaryKey: "one")?.payload,
                       "newer-local")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testOwnedSnapshotAcceptsServerWithoutReauthoringOldCachedClock() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "x,y", at: 2_000
        )], forceSave: true)
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "y", at: 1_000
        )], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: SyncIntentSnapshot.self, forPrimaryKey: "one")?.payload, "y")
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testOwnedSnapshotDoesNotOverridePendingWriter() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "server-base", at: 1_000
        )], forceSave: true)
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: SyncIntentSnapshot.self, forPrimaryKey: "one"))
        try realm.write {
            value.payload = "pending-owner-value"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = realm.objects(BigSyncPendingMutation.self).first?.generation
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "other", at: 9_000
        )], forceSave: true)
        realm.refresh()
        XCTAssertEqual(value.payload, "pending-owner-value")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testValidUnboundPredecessorRebasesPendingExtensionWithoutQuarantine() async throws {
        let (adapter, realm) = try await fixture()
        let unbound = record(SyncIntentSnapshot.self, adapter: adapter, payload: "base", at: 1_000)
        _ = try await adapter.saveChanges(in: [unbound], forceSave: true)
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: SyncIntentSnapshot.self, forPrimaryKey: "one"))
        try realm.write {
            value.catalog = "bound-catalog"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        let results = try await adapter.saveChanges(in: [unbound], forceSave: true)
        realm.refresh()
        XCTAssertEqual(value.catalog, "bound-catalog")
        XCTAssertEqual(results.first?.disposition, .preservedPendingLocal(generation: generation))
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertTrue(tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        let ownResults = try await adapter.validateAuthoritativeOwnUploadRecords([unbound])
        XCTAssertEqual(ownResults.first?.disposition, .validatedAuthoritativeOwnUpload)
    }

    @BigSyncBackgroundActor
    func testDifferentNonemptyCatalogIsStillQuarantinedWhilePending() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "base", at: 1_000
        )], forceSave: true)
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: SyncIntentSnapshot.self, forPrimaryKey: "one"))
        try realm.write {
            value.catalog = "local-catalog"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let results = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "remote", at: 2_000,
            catalog: "incompatible-catalog"
        )], forceSave: true)
        guard case .quarantined? = results.first?.disposition else {
            return XCTFail("Pending work is not permission to admit a contradictory catalog")
        }
        realm.refresh()
        XCTAssertEqual(value.catalog, "local-catalog")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testUnboundRollbackWithoutPendingIntentRemainsQuarantined() async throws {
        let (adapter, _) = try await fixture()
        _ = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "bound", at: 1_000, catalog: "C"
        )], forceSave: true)
        let results = try await adapter.saveChanges(in: [record(
            SyncIntentSnapshot.self, adapter: adapter, payload: "unbound", at: 2_000
        )], forceSave: true)
        guard case .quarantined? = results.first?.disposition else {
            return XCTFail("An accepted binding must not be silently cleared")
        }
    }

    @BigSyncBackgroundActor
    func testMalformedPredecessorStillFailsStandaloneValidation() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = record(SyncIntentSnapshot.self, adapter: adapter, payload: "base", at: 1_000)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: SyncIntentSnapshot.self, forPrimaryKey: "one"))
        try realm.write {
            value.catalog = "C"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        incoming["payload"] = nil
        let results = try await adapter.saveChanges(in: [incoming], forceSave: true)
        guard case .quarantined? = results.first?.disposition else {
            return XCTFail("Predecessor handling must not bypass standalone validation")
        }
    }

    func testQuarantinedUploadConflictIsNotAHandledRebase() throws {
        let id = CKRecord.ID(recordName: "Type.record", zoneID: .init(zoneName: "zone"))
        let outcome = InboundLiveResult(
            event: .init(ordinal: 0, entityType: "Type", recordID: id),
            disposition: .quarantined(lineageID: "lineage")
        )
        XCTAssertThrowsError(try requireResolvedUploadConflictOutcomes([outcome])) { error in
            XCTAssertEqual((error as? BigSyncSemanticUploadConflictError)?.recordNames, ["Type.record"])
        }
    }

    func testPreservedPendingUploadConflictCanRetry() throws {
        let id = CKRecord.ID(recordName: "Type.record", zoneID: .init(zoneName: "zone"))
        try requireResolvedUploadConflictOutcomes([.init(
            event: .init(ordinal: 0, entityType: "Type", recordID: id),
            disposition: .preservedPendingLocal(generation: "g")
        )])
    }
}
