import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(OrderedSemanticRecord)
private final class OrderedSemanticRecord: Object, ChangeMetadataRecordable,
    BigSyncInboundSemanticRecordValidating, BigSyncInboundSemanticReplacementValidating,
    BigSyncOutboundSemanticObjectValidating, BigSyncRetainsSyncedTombstone {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var rank = 0
    @Persisted var payload = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    var retainsSyncedTombstone: Bool { true }

    enum Failure: Error { case invalid }
    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        guard let rank = record["rank"] as? Int, rank > 0,
              let payload = record["payload"] as? String, !payload.isEmpty else {
            throw Failure.invalid
        }
    }
    static func validateInboundSemanticReplacement(_ record: CKRecord, existingObject: Object?) throws {
        _ = try inboundSemanticReplacementDisposition(record, existingObject: existingObject)
    }
    static func inboundSemanticReplacementDisposition(
        _ record: CKRecord, existingObject: Object?
    ) throws -> BigSyncInboundSemanticReplacementDisposition {
        try validateInboundSemanticRecord(record)
        guard let local = existingObject as? OrderedSemanticRecord else { return .preferIncomingRecord }
        guard local.payload != "unavailable" else {
            throw BigSyncSemanticAdmissionUnavailable(entityType: className())
        }
        let incoming = record["rank"] as! Int
        if incoming != local.rank { return incoming > local.rank ? .preferIncomingRecord : .preferExistingObject }
        guard record["payload"] as? String == local.payload else { throw Failure.invalid }
        return .preserveExistingObject
    }
    func validateOutboundSemanticObject(in realm: Realm) throws {
        guard rank > 0, !payload.isEmpty else { throw Failure.invalid }
    }
}

/// Exercises target transactions, retransmission and actual upload acknowledgements.
final class OrderedSemanticRecordTests: XCTestCase {
    private struct Fixture {
        let adapter: RealmSwiftAdapter
        let realm: Realm
        let zone: CKRecordZone.ID
        var recordName: String { OrderedSemanticRecord.className() + ".selector" }
    }

    @BigSyncBackgroundActor
    private func fixture() async throws -> Fixture {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "ordered-semantic-tracking-" + nonce
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "ordered-semantic-target-" + nonce
        target.objectTypes = [OrderedSemanticRecord.self, BigSyncPendingMutation.self]
        let zone = CKRecordZone.ID(zoneName: "ordered-" + nonce, ownerName: CKCurrentUserDefaultName)
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target], excludedClassNames: [], recordZoneID: zone,
            logger: Logger(label: "OrderedSemanticRecordTests"), startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory.appendingPathComponent("ordered-" + nonce))
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealmPerSchemaName[OrderedSemanticRecord.className()])
        return Fixture(adapter: adapter, realm: realm, zone: zone)
    }

    private func record(_ f: Fixture, rank: Int, time: TimeInterval, payload: String? = nil) -> CKRecord {
        let record = CKRecord(recordType: OrderedSemanticRecord.className(),
            recordID: .init(recordName: f.recordName, zoneID: f.zone))
        record["rank"] = rank as CKRecordValue
        record["payload"] = (payload ?? "rank-\(rank)") as CKRecordValue
        let date = Date(timeIntervalSinceReferenceDate: time)
        record["createdAt"] = date as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func seed(_ f: Fixture, rank: Int = 1, time: TimeInterval = 1_000) async throws -> OrderedSemanticRecord {
        let object = OrderedSemanticRecord()
        object.id = "selector"
        object.rank = rank
        object.payload = "rank-\(rank)"
        let date = Date(timeIntervalSinceReferenceDate: time)
        object.createdAt = date
        try await f.realm.asyncWrite {
            f.realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true, at: date)
        }
        try await f.adapter.didFinishImport()
        return object
    }

    private func journal(_ f: Fixture) -> String? {
        f.realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: f.recordName)?.generation
    }

    @BigSyncBackgroundActor
    private func uploads(_ f: Fixture) async throws -> [PreparedRecordUpload] {
        // The synchronizer forwards committed journal work at the import
        // boundary before asking the adapter to prepare transport records.
        try await f.adapter.didFinishImport()
        return try await f.adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: OrderedSemanticRecord.className())
    }

    @BigSyncBackgroundActor
    private func acknowledge(_ records: [PreparedRecordUpload], _ f: Fixture) async throws {
        let generations = Dictionary(uniqueKeysWithValues: records.compactMap { item -> (String, String)? in
            item.generation.map { (item.record.recordID.recordName, $0) }
        })
        try await f.adapter.didUpload(savedRecords: records.map(\.record), matchingGenerations: generations)
        f.realm.refresh()
    }

    @BigSyncBackgroundActor
    func testIncomingOrderOverridesPendingClockAndOldAcknowledgementCannotClearWinner() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        let oldUploads = try await uploads(f)
        let oldGeneration = try XCTUnwrap(journal(f))
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 10)], forceSave: true)
        f.realm.refresh()
        XCTAssertEqual(object.rank, 2)
        XCTAssertEqual(object.payload, "rank-2")
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 10))
        let selectedGeneration = try XCTUnwrap(journal(f))
        XCTAssertNotEqual(selectedGeneration, oldGeneration)
        try await acknowledge(oldUploads, f)
        XCTAssertEqual(journal(f), selectedGeneration)
        let next = try await uploads(f)
        XCTAssertEqual(next.count, 1)
        XCTAssertEqual(next.first?.record["rank"] as? Int, 2)
        XCTAssertEqual(next.first?.generation, selectedGeneration)
        try await acknowledge(next, f)
        XCTAssertNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testIncomingOrderOverridesNewerWallClockWithoutCreatingUploadDebt() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        let pending = try await uploads(f)
        try await acknowledge(pending, f)
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 10)], forceSave: true)
        f.realm.refresh()
        XCTAssertEqual(object.rank, 2)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 10))
        XCTAssertNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testExistingWinnerIsRequeuedWithoutChangingItsClockAndReplayKeepsGeneration() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f, rank: 3, time: 10)
        let pending = try await uploads(f)
        try await acknowledge(pending, f)
        XCTAssertNil(journal(f))
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 10_000)], forceSave: true)
        f.realm.refresh()
        let selectedGeneration = try XCTUnwrap(journal(f))
        XCTAssertEqual(object.rank, 3)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 10))
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 20_000)], forceSave: true)
        f.realm.refresh()
        XCTAssertEqual(journal(f), selectedGeneration)
        let next = try await uploads(f)
        XCTAssertEqual(next.first?.record["rank"] as? Int, 3)
        XCTAssertEqual(next.first?.record["modifiedAt"] as? Date, Date(timeIntervalSinceReferenceDate: 10))
    }

    @BigSyncBackgroundActor
    func testExactReplayPreservesObjectWithoutManufacturingUploadDebt() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        let pending = try await uploads(f)
        try await acknowledge(pending, f)
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 1, time: 50_000)], forceSave: true)
        f.realm.refresh()
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 1_000))
        XCTAssertEqual(object.payload, "rank-1")
        XCTAssertNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testWinnerIsRecomputedAfterAConcurrentLocalMutation() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        f.adapter._testBeforeImportedRecordTargetWrite = {
            f.adapter._testBeforeImportedRecordTargetWrite = nil
            try await f.realm.asyncWrite {
                object.rank = 3
                object.payload = "rank-3"
                object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 5))
            }
        }
        _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 20_000)], forceSave: true)
        f.realm.refresh()
        XCTAssertEqual(object.rank, 3)
        XCTAssertEqual(object.payload, "rank-3")
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 5))
        XCTAssertNotNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testUnavailableLocalAuthorityAbortsWithoutQuarantiningValidRemoteRecord() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        try await f.realm.asyncWrite { object.payload = "unavailable" }
        do {
            _ = try await f.adapter.saveChanges(in: [record(f, rank: 2, time: 10)], forceSave: true)
            XCTFail("Unavailable local authority must abort import")
        } catch is BigSyncSemanticAdmissionUnavailable { }
        let persistence = try XCTUnwrap(f.adapter.realmProvider?.persistenceRealm)
        XCTAssertTrue(persistence.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        f.realm.refresh()
        XCTAssertEqual(object.rank, 1)
        XCTAssertNotNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testInvalidOutboundValueCannotProduceAnUpload() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        try await f.realm.asyncWrite {
            object.rank = 0
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        do {
            _ = try await uploads(f)
            XCTFail("Invalid local selector must not upload")
        } catch is OrderedSemanticRecord.Failure { }
        XCTAssertNotNil(journal(f))
    }

    @BigSyncBackgroundActor
    func testRetainedSelectorNeverBecomesPhysicalDeletionOrCleanup() async throws {
        let f = try await fixture()
        defer { f.adapter.invalidateTokens() }
        let object = try await seed(f)
        let initial = try await uploads(f)
        try await acknowledge(initial, f)
        try await f.realm.asyncWrite {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await f.adapter.didFinishImport()
        let deletions = try await f.adapter.preparedRecordDeletions(limit: 10,
            restrictedToEntityType: OrderedSemanticRecord.className())
        XCTAssertTrue(deletions.isEmpty)
        let saved = try await uploads(f)
        XCTAssertEqual(saved.count, 1)
        XCTAssertEqual(saved.first?.record["isDeleted"] as? Bool, true)
        try await acknowledge(saved, f)
        _ = try await f.adapter.deleteRecords(with: [.init(recordName: f.recordName, zoneID: f.zone)])
        try await f.adapter.cleanUp()
        f.realm.refresh()
        XCTAssertFalse(object.isInvalidated)
        XCTAssertTrue(object.isDeleted)
        let persistence = try XCTUnwrap(f.adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(persistence.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
    }
}
