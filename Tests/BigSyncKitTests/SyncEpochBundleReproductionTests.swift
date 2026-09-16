import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(SyncEpochArticle)
private final class SyncEpochArticle: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = "article"
    @Persisted var title = "server title"
    @Persisted var epoch = "E0"
    @Persisted var aggregateCount = 7
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(SyncEpochControl)
private final class SyncEpochControl: Object, ChangeMetadataRecordable,
    BigSyncAuthoritativeServerSnapshotModel {
    @Persisted(primaryKey: true) var id = "control"
    @Persisted var epoch = "E0"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class SyncEpochBundleReproductionTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "epoch-bundle-target-" + nonce
        target.objectTypes = [SyncEpochArticle.self, SyncEpochControl.self,
                              BigSyncPendingMutation.self]
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local",
                      replicaBindingGenerationIdentifier: "binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "epoch-bundle-tracking-" + nonce
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: [],
            recordZoneID: .init(zoneName: "epoch-bundle"),
            logger: Logger(label: "SyncEpochBundleReproductionTests"),
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
            containerIdentifier: "iCloud.test.epoch-bundle", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func article(_ adapter: RealmSwiftAdapter, title: String,
                         epoch: String, count: Int, at: Double) -> CKRecord {
        let record = CKRecord(recordType: SyncEpochArticle.className(), recordID: .init(
            recordName: SyncEpochArticle.className() + ".article",
            zoneID: adapter.recordZoneID
        ))
        let date = Date(timeIntervalSinceReferenceDate: at)
        record["title"] = title as CKRecordValue
        record["epoch"] = epoch as CKRecordValue
        record["aggregateCount"] = count as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    private func control(_ adapter: RealmSwiftAdapter, epoch: String, at: Double) -> CKRecord {
        let record = CKRecord(recordType: SyncEpochControl.className(), recordID: .init(
            recordName: SyncEpochControl.className() + ".control",
            zoneID: adapter.recordZoneID
        ))
        let date = Date(timeIntervalSinceReferenceDate: at)
        record["epoch"] = epoch as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func installE0(_ adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(in: [
            article(adapter, title: "server title", epoch: "E0", count: 7, at: 10),
            control(adapter, epoch: "E0", at: 10),
        ], forceSave: true)
        try await adapter.persistImportedChanges()
    }

    /// Known issue reproduction. The control is a separate authoritative
    /// snapshot and advances to E1. A pending title-only Article mutation still
    /// protects the whole E0 Article, including epoch-bound aggregate fields.
    /// This intentionally asserts current broken behavior until a repair is chosen.
    @BigSyncBackgroundActor
    func testKnownIssuePendingArticleMetadataEditSplitsEpochBundle() async throws {
        let (adapter, realm) = try await fixture()
        try await installE0(adapter)
        let localArticle = try XCTUnwrap(realm.object(
            ofType: SyncEpochArticle.self, forPrimaryKey: "article"
        ))
        try realm.write {
            localArticle.title = "local title only"
            localArticle.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 20)
            )
        }

        _ = try await adapter.saveChanges(in: [
            control(adapter, epoch: "E1", at: 30),
            article(adapter, title: "server title after reset", epoch: "E1", count: 0, at: 30),
        ], forceSave: false)
        try await adapter.persistImportedChanges()
        realm.refresh()

        let localControl = try XCTUnwrap(realm.object(
            ofType: SyncEpochControl.self, forPrimaryKey: "control"
        ))
        XCTAssertEqual(localControl.epoch, "E1")
        XCTAssertEqual(localArticle.title, "local title only")
        XCTAssertEqual(localArticle.epoch, "E0")
        XCTAssertEqual(localArticle.aggregateCount, 7)
        XCTAssertNotEqual(localArticle.epoch, localControl.epoch)

        try await adapter.didFinishImport()
        let outgoing = try await adapter.prepareUploadBatch(limit: 10)
        let row = try XCTUnwrap(outgoing.records.first {
            $0.recordType == SyncEpochArticle.className()
        })
        XCTAssertEqual(row["epoch"] as? String, "E0")
        XCTAssertEqual(row["aggregateCount"] as? Int, 7)
    }

    /// Same semantic operations, opposite order: remote E1 lands first, then
    /// title-only local authoring. This produces a coherent E1 bundle.
    @BigSyncBackgroundActor
    func testControlEpochResetBeforeMetadataEditStaysCoherent() async throws {
        let (adapter, realm) = try await fixture()
        try await installE0(adapter)
        _ = try await adapter.saveChanges(in: [
            control(adapter, epoch: "E1", at: 30),
            article(adapter, title: "server title after reset", epoch: "E1", count: 0, at: 30),
        ], forceSave: false)
        try await adapter.persistImportedChanges()
        let localArticle = try XCTUnwrap(realm.object(
            ofType: SyncEpochArticle.self, forPrimaryKey: "article"
        ))
        let localControl = try XCTUnwrap(realm.object(
            ofType: SyncEpochControl.self, forPrimaryKey: "control"
        ))
        XCTAssertEqual(localArticle.epoch, "E1")
        XCTAssertEqual(localControl.epoch, "E1")
        XCTAssertEqual(localArticle.aggregateCount, 0)
        try realm.write {
            localArticle.title = "local title only"
            localArticle.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40)
            )
        }
        try await adapter.didFinishImport()
        let outgoing = try await adapter.prepareUploadBatch(limit: 10)
        let row = try XCTUnwrap(outgoing.records.first {
            $0.recordType == SyncEpochArticle.className()
        })
        XCTAssertEqual(row["epoch"] as? String, "E1")
        XCTAssertEqual(row["aggregateCount"] as? Int, 0)
    }
}
