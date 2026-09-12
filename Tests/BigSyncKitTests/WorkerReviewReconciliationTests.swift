import XCTest
import CloudKit
import Logging
@testable import BigSyncKit
import RealmSwift
import RealmSwiftGaps

@objc(WorkerReviewReceiver)
private final class WorkerReviewReceiver: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = "constructor-default"
    @Persisted var categoryID: UUID?
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// These assert the desired behavior. A failed assertion is reproduction of a
/// review finding, not a passing expected-failure test or a production repair.
final class WorkerReviewReconciliationTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "worker-review-tracking-" + nonce
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "worker-review-target-" + nonce
        target.objectTypes = [WorkerReviewReceiver.self, BigSyncPendingMutation.self]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "worker-review", ownerName: CKCurrentUserDefaultName),
            logger: Logger(label: "WorkerReview"),
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory.appendingPathComponent("worker-review-assets-" + nonce)
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func remote(
        id: String, zone: CKRecordZone.ID,
        date: Date, explicit: Bool, categoryID: UUID? = nil
    ) -> CKRecord {
        let record = CKRecord(recordType: WorkerReviewReceiver.className(), recordID: .init(recordName: WorkerReviewReceiver.className() + "." + id, zoneID: zone))
        record["payload"] = "remote-payload" as CKRecordValue
        record["createdAt"] = date as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        if explicit { record["explicitlyModifiedAt"] = date as CKRecordValue }
        if let categoryID { record["categoryID"] = categoryID.uuidString as CKRecordValue }
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    func testReviewFirstImportWithoutExplicitTimestampMustNotInventLocalAuthority() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = remote(id: "first", zone: adapter.recordZoneID,
                              date: Date(timeIntervalSinceReferenceDate: 1000), explicit: false)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        try await adapter.persistImportedChanges()
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: WorkerReviewReceiver.self, forPrimaryKey: "first"))
        let pendingCount = realm.objects(BigSyncPendingMutation.self).count
        print("REVIEW_FIRST_IMPORT", value.payload, "pending", pendingCount)
        XCTAssertEqual(value.payload, "remote-payload", "Constructor defaults are not a competing local revision")
        XCTAssertEqual(pendingCount, 0, "An admitted remote creation must not fabricate a journaled local edit")
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        print("REVIEW_FIRST_IMPORT_UPLOAD", batch.records.map { $0["payload"] as? String ?? "nil" })
        XCTAssertTrue(batch.records.isEmpty, "A download must not queue constructor-default replacement data")
    }

    @BigSyncBackgroundActor
    func testReviewFirstImportWithExplicitTimestampControl() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = remote(id: "explicit", zone: adapter.recordZoneID,
                              date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: WorkerReviewReceiver.self, forPrimaryKey: "explicit")?.payload, "remote-payload")
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(batch.records.isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewOptionalUUIDClearMustConvergeAndStayClearedOnLaterUpload() async throws {
        let (adapter, realm) = try await fixture()
        let category = UUID()
        let initial = remote(id: "clear", zone: adapter.recordZoneID,
                             date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true, categoryID: category)
        _ = try await adapter.saveChanges(in: [initial], forceSave: true)
        realm.refresh()
        let value = try XCTUnwrap(realm.object(ofType: WorkerReviewReceiver.self, forPrimaryKey: "clear"))
        XCTAssertEqual(value.categoryID, category)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        let cleared = remote(id: "clear", zone: adapter.recordZoneID,
                             date: Date(timeIntervalSinceReferenceDate: 2000), explicit: true)
        XCTAssertNil(cleared["categoryID"])
        _ = try await adapter.saveChanges(in: [cleared], forceSave: true)
        realm.refresh()
        print("REVIEW_UUID_CLEAR", value.categoryID?.uuidString ?? "nil")
        XCTAssertNil(value.categoryID, "Full-record absence must clear an optional UUID")
        try await realm.asyncWrite {
            value.payload = "unrelated-local-edit"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first { $0.recordID == cleared.recordID })
        print("REVIEW_UUID_REUPLOAD", outgoing["categoryID"] as? String ?? "nil")
        XCTAssertNil(outgoing["categoryID"], "An unrelated local edit must not reintroduce the cleared remote relationship")
    }

    @BigSyncBackgroundActor
    func testReviewOptionalUUIDReplacementControl() async throws {
        let (adapter, realm) = try await fixture()
        let first = UUID(), second = UUID()
        _ = try await adapter.saveChanges(in: [remote(id: "replace", zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true, categoryID: first)], forceSave: true)
        _ = try await adapter.saveChanges(in: [remote(id: "replace", zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 2000), explicit: true, categoryID: second)], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: WorkerReviewReceiver.self, forPrimaryKey: "replace")?.categoryID, second)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewExistingNewerLocalAuthorityControl() async throws {
        let (adapter, realm) = try await fixture()
        let local = WorkerReviewReceiver()
        local.id = "existing"
        local.payload = "genuine-local"
        local.modifiedAt = Date(timeIntervalSinceReferenceDate: 3000)
        local.explicitlyModifiedAt = local.modifiedAt
        try await realm.asyncWrite { realm.add(local) }
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        _ = try await adapter.saveChanges(in: [remote(id: local.id, zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true)], forceSave: true)
        realm.refresh()
        XCTAssertEqual(local.payload, "genuine-local")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testReviewMalformedNonNilUUIDStillRejectedControl() async throws {
        let (adapter, realm) = try await fixture()
        let record = remote(id: "malformed", zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true)
        record["categoryID"] = "not-a-uuid" as CKRecordValue
        do {
            _ = try await adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Malformed non-nil UUID must be rejected")
        } catch {
            print("REVIEW_MALFORMED_UUID_REJECTED", error)
        }
        realm.refresh()
        XCTAssertNil(realm.object(ofType: WorkerReviewReceiver.self, forPrimaryKey: "malformed"))
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }
}
