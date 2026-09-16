import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(SyncStaleBaseRow)
private final class SyncStaleBaseRow: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var remoteField = ""
    @Persisted var localField = ""
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class SyncStaleBaseReproductionTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "stale-base-target-" + nonce
        target.objectTypes = [SyncStaleBaseRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(
                    installationIdentifier: "replica-B",
                    replicaBindingGenerationIdentifier: "binding-B"
                )
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "stale-base-tracking-" + nonce
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: .init(zoneName: "stale-base"),
            logger: Logger(label: "SyncStaleBaseReproductionTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding-B"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.stale-base",
            databaseScope: .private
        )
        return (
            adapter,
            try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        )
    }

    private func record(
        adapter: RealmSwiftAdapter,
        remote: String,
        local: String,
        at timestamp: TimeInterval
    ) -> CKRecord {
        let record = CKRecord(
            recordType: SyncStaleBaseRow.className(),
            recordID: .init(
                recordName: SyncStaleBaseRow.className() + ".row",
                zoneID: adapter.recordZoneID
            )
        )
        let date = Date(timeIntervalSinceReferenceDate: timestamp)
        record["remoteField"] = remote as CKRecordValue
        record["localField"] = local as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func installBase(_ adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(
            in: [record(adapter: adapter, remote: "remote-v0", local: "local-v0", at: 10)],
            forceSave: true
        )
        try await adapter.persistImportedChanges()
    }

    /// Reproduction of the Steve/SQLiteData class of problem. This test
    /// deliberately describes the CURRENT broken behavior so it stays green
    /// until we choose a repair. Flip these assertions when that repair lands.
    @BigSyncBackgroundActor
    func testKnownIssueLocalPendingWholeRecordCarriesStaleRemoteField() async throws {
        let (adapter, realm) = try await fixture()
        try await installBase(adapter)
        let value = try XCTUnwrap(realm.object(
            ofType: SyncStaleBaseRow.self,
            forPrimaryKey: "row"
        ))
        try realm.write {
            value.localField = "local-v1"
            value.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 20)
            )
        }

        _ = try await adapter.saveChanges(
            in: [record(adapter: adapter, remote: "remote-v1", local: "local-v0", at: 30)],
            forceSave: false
        )
        try await adapter.persistImportedChanges()
        realm.refresh()

        // Current behavior: pending local intent protects the ENTIRE working
        // row, not just localField. remoteField therefore remains the old v0.
        XCTAssertEqual(value.remoteField, "remote-v0")
        XCTAssertEqual(value.localField, "local-v1")
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v0")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
        XCTAssertNotEqual(outgoing["remoteField"] as? String, "remote-v1")
    }

    /// Control proving the exact same two semantic edits preserve both values
    /// when the remote change is consumed before local authoring. The differing
    /// result demonstrates the ordering dependency without any long-running test.
    @BigSyncBackgroundActor
    func testControlRemoteBeforeLocalEditPreservesBothFields() async throws {
        let (adapter, realm) = try await fixture()
        try await installBase(adapter)
        _ = try await adapter.saveChanges(
            in: [record(adapter: adapter, remote: "remote-v1", local: "local-v0", at: 30)],
            forceSave: false
        )
        try await adapter.persistImportedChanges()
        let value = try XCTUnwrap(realm.object(
            ofType: SyncStaleBaseRow.self,
            forPrimaryKey: "row"
        ))
        try realm.write {
            value.localField = "local-v1"
            value.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40)
            )
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v1")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
    }
}
