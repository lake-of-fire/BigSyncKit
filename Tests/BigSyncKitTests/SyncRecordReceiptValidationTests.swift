import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(ReceiptValidationRow)
private final class ReceiptValidationRow: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    @Persisted(primaryKey: true) var id = ""
    @Persisted var text = "base"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class SyncRecordReceiptValidationTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm, [PreparedRecordUpload]) {
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "receipt-validation-" + UUID().uuidString
        target.objectTypes = [ReceiptValidationRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "receipt-validation-tracking-" + UUID().uuidString
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking, targetRealmConfigurations: [target],
            excludedClassNames: [], recordZoneID: .init(zoneName: "receipt-validation"),
            logger: Logger(label: "SyncRecordReceiptValidationTests"), startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.receipts", databaseScope: .private)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        let records = ["a", "b"].map { id -> CKRecord in
            let record = CKRecord(recordType: ReceiptValidationRow.className(), recordID: .init(
                recordName: ReceiptValidationRow.className() + "." + id, zoneID: adapter.recordZoneID
            ))
            record["text"] = "base" as CKRecordValue
            record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
            record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
            record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
            record["isDeleted"] = false as CKRecordValue
            return record
        }
        _ = try await adapter.saveChanges(in: records, forceSave: false)
        try await adapter.persistImportedChanges()
        try realm.write {
            for object in realm.objects(ReceiptValidationRow.self) {
                object.text = "local-" + object.id
                object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 2))
            }
        }
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        XCTAssertEqual(prepared.count, 2)
        return (adapter, realm, prepared)
    }

    private func generations(in realm: Realm) -> [String: String] {
        realm.refresh()
        return Dictionary(uniqueKeysWithValues: realm.objects(BigSyncPendingMutation.self).map {
            ($0.recordName, $0.generation)
        })
    }

    @BigSyncBackgroundActor
    func testStrippedComparisonProofCannotFallBackToLegacyAcknowledgement() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let before = generations(in: realm)
        let stripped = prepared.map { PreparedRecordUpload(record: $0.record, generation: $0.generation) }
        do {
            try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: stripped)
            XCTFail("Missing comparison evidence must not opt an enabled row out of its receipt fence")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(generations(in: realm), before)
    }

    @BigSyncBackgroundActor
    func testGenerationOnlyEntryPointCannotAcknowledgeComparisonEnabledRecords() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let before = generations(in: realm)
        do {
            try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingGenerations: before)
            XCTFail("A generation cannot replace a preparation-time comparison proof")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(generations(in: realm), before)
    }

    @BigSyncBackgroundActor
    func testDuplicateSavedIdentityRejectsWholeBatchBeforeBaselineMutation() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let first = try XCTUnwrap(prepared.first)
        let before = generations(in: realm)
        let revision = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: first.record.recordID.recordName)?.revision)
        do {
            try await adapter.didUpload(savedRecords: [first.record, first.record], matchingPreparedUploads: prepared)
            XCTFail("A duplicated response identity is not two successful receipts")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(generations(in: realm), before)
        XCTAssertEqual(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: first.record.recordID.recordName)?.revision, revision)
    }

    @BigSyncBackgroundActor
    func testDifferentZoneReceiptCannotUsePreparedName() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let sent = try XCTUnwrap(prepared.first).record
        let before = generations(in: realm)
        let wrongZone = CKRecord(recordType: sent.recordType, recordID: .init(
            recordName: sent.recordID.recordName, zoneID: .init(zoneName: "other-zone")
        ))
        do {
            try await adapter.didUpload(savedRecords: [wrongZone], matchingPreparedUploads: prepared)
            XCTFail("Record names are scoped to their exact zone")
        } catch BigSyncRecordRebaseError.inconsistentReceipt(_) {}
        XCTAssertEqual(generations(in: realm), before)
    }

    @BigSyncBackgroundActor
    func testStaleReceiptLeavesItsWorkPendingWithoutBlockingValidSibling() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let stale = try XCTUnwrap(prepared.first)
        let valid = try XCTUnwrap(prepared.dropFirst().first)
        let staleName = stale.record.recordID.recordName
        let validName = valid.record.recordID.recordName
        let before = generations(in: realm)
        let base = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: staleName))
        let fields = base.fieldDigests, namespace = base.namespace
        try realm.write {
            BigSyncRecordBaseline.install(recordName: staleName, namespace: namespace,
                fields: fields, serverChangeTag: "later-accepted-version", in: realm)
        }
        let revision = base.revision
        try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: prepared)
        XCTAssertEqual(generations(in: realm), [staleName: try XCTUnwrap(before[staleName])])
        XCTAssertEqual(base.revision, revision)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: staleName)?.pendingGeneration,
                       before[staleName])
        XCTAssertNil(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: validName)?.pendingGeneration)
    }

    @BigSyncBackgroundActor
    func testAccountReplacementRejectsOldProofWithoutLegacyFallback() async throws {
        let (adapter, realm, prepared) = try await fixture()
        let before = generations(in: realm)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "other-account", replicaBindingGenerationIdentifier: "other-binding")
        try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: prepared)
        XCTAssertEqual(generations(in: realm), before)
    }
}
