import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(ReceiptRebaseRow)
private final class ReceiptRebaseRow: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var text = "base"
    @Persisted var other = "base"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class SyncRecordReceiptTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "receipt-target-" + UUID().uuidString
        target.objectTypes = [ReceiptRebaseRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "receipt-tracking-" + UUID().uuidString
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking, targetRealmConfigurations: [target],
            excludedClassNames: [], recordZoneID: .init(zoneName: "receipts"),
            logger: Logger(label: "SyncRecordReceiptTests"), startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.receipts", databaseScope: .private)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        _ = try await adapter.saveChanges(in: [record(adapter)], forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
        return (adapter, realm)
    }

    private func record(_ adapter: RealmSwiftAdapter, text: String = "base", other: String = "base") -> CKRecord {
        let record = CKRecord(recordType: ReceiptRebaseRow.className(), recordID: .init(
            recordName: ReceiptRebaseRow.className() + ".row", zoneID: adapter.recordZoneID
        ))
        record["text"] = text as CKRecordValue
        record["other"] = other as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 10) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 10) as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func prepare(_ adapter: RealmSwiftAdapter, realm: Realm) async throws -> RealmSwiftPreparedUploadBatch {
        let object = try XCTUnwrap(realm.object(ofType: ReceiptRebaseRow.self, forPrimaryKey: "row"))
        try realm.write {
            object.text = "local edit"
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
        }
        try await adapter.didFinishImport()
        return try await adapter.prepareUploadBatch(limit: 10)
    }

    @BigSyncBackgroundActor
    func testNewerComparisonRevisionCannotLeakIntoLegacyAcknowledgement() async throws {
        let (adapter, realm) = try await fixture()
        let batch = try await prepare(adapter, realm: realm)
        let sent = try XCTUnwrap(batch.records.first)
        let name = sent.recordID.recordName
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
        let base = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name))
        let namespace = base.namespace, fields = base.fieldDigests
        // Advance only accepted comparison evidence, as can happen before the
        // later tracking transaction completes. Never mutate SDK system fields.
        try realm.write {
            BigSyncRecordBaseline.install(recordName: name, namespace: namespace,
                fields: fields, serverChangeTag: "newer-accepted-version", in: realm)
        }
        let revision = base.revision
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation,
                       generation, "STALE RECEIPT: rejected comparison proof still cleared durable work")
        XCTAssertEqual(base.revision, revision)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)?.pendingGeneration,
                       generation, "STALE RECEIPT: rejected comparison proof still cleared tracking work")
    }

    @BigSyncBackgroundActor
    func testChangedReceiptTypeCannotUseOriginalPreparedGeneration() async throws {
        let (adapter, realm) = try await fixture()
        let batch = try await prepare(adapter, realm: realm)
        let sent = try XCTUnwrap(batch.records.first)
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: sent.recordID.recordName)?.generation)
        let wrongType = CKRecord(recordType: "DifferentType", recordID: sent.recordID)
        do {
            try await adapter.acknowledgeUploadedRecords([wrongType], from: batch)
            XCTFail("INVALID RECEIPT: record ID alone cannot authorize a different record type")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: sent.recordID.recordName)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testDuplicatePreparedEntriesAreRejectedBeforeChangingAnyBaseline() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await prepare(adapter, realm: realm)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let item = try XCTUnwrap(prepared.first)
        let baseline = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: item.record.recordID.recordName))
        let revision = baseline.revision
        do {
            try await adapter.didUpload(savedRecords: [item.record], matchingPreparedUploads: [item, item])
            XCTFail("INVALID RECEIPT: duplicate preparation identity must be rejected")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(baseline.revision, revision)
    }

    @BigSyncBackgroundActor
    func testValidReceiptRemainsIdempotent() async throws {
        let (adapter, realm) = try await fixture()
        let batch = try await prepare(adapter, realm: realm)
        let name = try XCTUnwrap(batch.records.first).recordID.recordName
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        let revision = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        XCTAssertEqual(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision, revision)
        try await adapter.didFinishImport()
        let remaining = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(remaining.records.isEmpty)
    }

    @BigSyncBackgroundActor
    func testBaselineCommittedBeforeTrackingAcknowledgementCanResume() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await prepare(adapter, realm: realm)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let item = try XCTUnwrap(prepared.first)
        let proof = try XCTUnwrap(item.comparisonBase)
        // Replay a receipt after its baseline transaction committed but before
        // its tracking acknowledgement. The same prepared evidence must finish.
        try realm.write {
            BigSyncRecordBaseline.install(recordName: item.record.recordID.recordName,
                namespace: proof.context.namespace, fields: proof.fields,
                serverChangeTag: item.record.recordChangeTag, in: realm)
        }
        try await adapter.didUpload(savedRecords: [item.record], matchingPreparedUploads: prepared)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: item.record.recordID.recordName))
        try await adapter.didFinishImport()
        let remaining = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(remaining.records.isEmpty)
    }

    @BigSyncBackgroundActor
    func testValidOlderReceiptAdvancesAcceptedBaseButKeepsNewerEdit() async throws {
        let (adapter, realm) = try await fixture()
        let batch = try await prepare(adapter, realm: realm)
        let name = try XCTUnwrap(batch.records.first).recordID.recordName
        let object = try XCTUnwrap(realm.object(ofType: ReceiptRebaseRow.self, forPrimaryKey: "row"))
        try realm.write {
            object.text = "later typing"
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertEqual(object.text, "later typing")
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation, generation)
        try await adapter.didFinishImport()
        let remaining = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(remaining.records.first?["text"] as? String, "later typing")
    }
}
