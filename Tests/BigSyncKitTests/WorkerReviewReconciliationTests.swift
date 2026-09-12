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
    @Persisted var requiredUUID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
    @Persisted var relatedIDs: List<UUID>
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(WorkerReviewObjectMap)
private final class WorkerReviewObjectMap: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var children: Map<String, WorkerReviewReceiver?>
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
        target.objectTypes = [WorkerReviewReceiver.self, WorkerReviewObjectMap.self, BigSyncPendingMutation.self]
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

    @BigSyncBackgroundActor
    func testReviewNilReplayAndAbsentRequiredUUIDPreserveCompatibility() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = remote(id: "nil-replay", zone: adapter.recordZoneID,
                              date: Date(timeIntervalSinceReferenceDate: 1000), explicit: false)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        let object = try XCTUnwrap(realm.object(ofType: WorkerReviewReceiver.self,
                                                forPrimaryKey: "nil-replay"))
        let expected = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        XCTAssertEqual(object.requiredUUID, expected)
        for _ in 0..<2 {
            _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        }
        realm.refresh()
        XCTAssertNil(object.categoryID)
        XCTAssertEqual(object.requiredUUID, expected)
        XCTAssertEqual(object.payload, "remote-payload")
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewRemoteClearCannotReplacePendingLocalUUID() async throws {
        let (adapter, realm) = try await fixture()
        let local = WorkerReviewReceiver()
        local.id = "pending-local"
        local.payload = "genuine-local"
        local.categoryID = UUID()
        let uuid = local.categoryID
        try await realm.asyncWrite {
            realm.add(local)
            local.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        let incoming = remote(id: local.id, zone: adapter.recordZoneID,
                              date: Date().addingTimeInterval(1000), explicit: true)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        XCTAssertEqual(local.categoryID, uuid)
        XCTAssertEqual(local.payload, "genuine-local")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testReviewPrimitiveUUIDListRoundTripsAndClearsInOrder() async throws {
        let (sender, source) = try await fixture()
        let (receiver, target) = try await fixture()
        let object = WorkerReviewReceiver()
        object.id = "uuid-list"
        let first = UUID(), second = UUID()
        try await source.asyncWrite {
            source.add(object)
            object.relatedIDs.append(objectsIn: [second, first, second])
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await sender.didFinishImport()
        let batch = try await sender.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first)
        XCTAssertEqual(outgoing["relatedIDs"] as? [String],
                       [second.uuidString, first.uuidString, second.uuidString])
        _ = try await receiver.saveChanges(in: batch.records, forceSave: true)
        target.refresh()
        let received = try XCTUnwrap(target.object(ofType: WorkerReviewReceiver.self,
                                                    forPrimaryKey: object.id))
        XCTAssertEqual(Array(received.relatedIDs), [second, first, second])
        try await sender.acknowledgeUploadedRecords(batch.records, from: batch)
        try await source.asyncWrite {
            object.relatedIDs.removeAll()
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await sender.didFinishImport()
        let cleared = try await sender.prepareUploadBatch(limit: 10)
        XCTAssertNil(try XCTUnwrap(cleared.records.first)["relatedIDs"])
        _ = try await receiver.saveChanges(in: cleared.records, forceSave: true)
        target.refresh()
        XCTAssertTrue(received.relatedIDs.isEmpty)
        XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewUnsupportedObjectMapFailsWithoutAcknowledgingJournal() async throws {
        let (adapter, realm) = try await fixture()
        let child = WorkerReviewReceiver()
        child.id = "child"
        let parent = WorkerReviewObjectMap()
        parent.id = "parent"
        try await realm.asyncWrite {
            realm.add(child)
            realm.add(parent)
            parent.children["child"] = child
            parent.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        do {
            _ = try await adapter.preparedRecordsToUpload(limit: 10,
                restrictedToEntityType: WorkerReviewObjectMap.className())
            XCTFail("An unsupported object map cannot silently become an absent field")
        } catch let error as RealmSwiftRemoteRecordDecodingError {
            guard case .malformedField(_, let property, _) = error else {
                return XCTFail("Unexpected map error: \(error)")
            }
            XCTAssertEqual(property, "children")
        }
        realm.refresh()
        XCTAssertEqual(parent.children["child"]??.id, child.id)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
    }
}


extension WorkerReviewReconciliationTests {
    @BigSyncBackgroundActor
    func testReviewAutomaticLocalWinnerPreservesConflictClockAndLaterRemoteEditWins() async throws {
        let (adapter, realm) = try await fixture()
        let t1 = Date(timeIntervalSinceReferenceDate: 1_000)
        let t2 = Date(timeIntervalSinceReferenceDate: 2_000)
        let t3 = Date(timeIntervalSinceReferenceDate: 3_000)
        let local = WorkerReviewReceiver()
        local.id = "automatic-local-winner"
        local.payload = "local-t2"
        local.createdAt = t2
        local.modifiedAt = t2
        local.explicitlyModifiedAt = t2
        try await realm.asyncWrite {
            realm.add(local)
            local.refreshChangeMetadata(explicitlyModified: true, at: t2)
        }
        try await adapter.didFinishImport()
        let authored = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(authored.records.count, 1)
        try await adapter.acknowledgeUploadedRecords(authored.records, from: authored)
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)

        func incoming(_ payload: String, at timestamp: Date) -> CKRecord {
            let record = CKRecord(
                recordType: WorkerReviewReceiver.className(),
                recordID: .init(
                    recordName: WorkerReviewReceiver.className() + "." + local.id,
                    zoneID: adapter.recordZoneID
                )
            )
            record["payload"] = payload as CKRecordValue
            record["createdAt"] = t1 as CKRecordValue
            record["modifiedAt"] = timestamp as CKRecordValue
            record["explicitlyModifiedAt"] = timestamp as CKRecordValue
            record["isDeleted"] = false as CKRecordValue
            return record
        }

        _ = try await adapter.saveChanges(
            in: [incoming("remote-t1", at: t1)],
            forceSave: true
        )
        realm.refresh()
        XCTAssertEqual(local.payload, "local-t2")
        XCTAssertEqual(local.modifiedAt, t2,
                       "Automatic retransmission must not mint a newer modifiedAt")
        XCTAssertEqual(local.explicitlyModifiedAt, t2,
                       "Automatic retransmission must not mint a newer explicit edit clock")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)

        try await adapter.didFinishImport()
        let retransmission = try await adapter.prepareUploadBatch(limit: 10)
        let retransmitted = try XCTUnwrap(retransmission.records.first)
        XCTAssertEqual(retransmitted["payload"] as? String, "local-t2")
        XCTAssertEqual(retransmitted["modifiedAt"] as? Date, t2)
        XCTAssertEqual(retransmitted["explicitlyModifiedAt"] as? Date, t2)
        try await adapter.acknowledgeUploadedRecords(
            retransmission.records, from: retransmission
        )
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)

        _ = try await adapter.saveChanges(
            in: [incoming("remote-t3", at: t3)],
            forceSave: true
        )
        realm.refresh()
        XCTAssertEqual(local.payload, "remote-t3",
                       "A genuinely later edit must outrank the original local T2 authoring clock")
        XCTAssertEqual(local.modifiedAt, t3)
        XCTAssertEqual(local.explicitlyModifiedAt, t3)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }
}
