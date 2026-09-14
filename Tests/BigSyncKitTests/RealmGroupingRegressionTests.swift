import CloudKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(GroupingFirstObject)
private final class GroupingFirstObject: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(GroupingSecondObject)
private final class GroupingSecondObject: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// Exercises the adapter's real grouped writes, not a replacement grouping
/// implementation. Identical primary keys in separate schemas detect cross-
/// Realm mixing; partial acknowledgements and newer generations detect loss.
final class RealmGroupingRegressionTests: XCTestCase {
    private struct Table {
        let realm: Realm
        let type: Object.Type
    }

    private struct Fixture {
        let adapter: RealmSwiftAdapter
        let tables: [Table]
        let zone: CKRecordZone.ID
    }

    @BigSyncBackgroundActor
    private func fixture() async throws -> Fixture {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "grouping-tracking-" + nonce
        let types: [Object.Type] = [GroupingFirstObject.self, GroupingSecondObject.self]
        let configurations = types.enumerated().map { index, type in
            var configuration = Realm.Configuration()
            configuration.inMemoryIdentifier = "grouping-target-\(index)-" + nonce
            configuration.objectTypes = [type, BigSyncPendingMutation.self]
            return configuration
        }
        let zone = CKRecordZone.ID(zoneName: "grouping-" + nonce, ownerName: CKCurrentUserDefaultName)
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: configurations,
            excludedClassNames: [], recordZoneID: zone,
            logger: Logger(label: "RealmGroupingRegressionTests"),
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("grouping-assets-" + nonce)
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        let tables = try types.map { type in
            Table(realm: try XCTUnwrap(adapter.realmProvider?
                .targetReaderRealmPerSchemaName[type.className()]), type: type)
        }
        return Fixture(adapter: adapter, tables: tables, zone: zone)
    }

    @BigSyncBackgroundActor
    private func seed(_ fixture: Fixture, count: Int = 40) async throws {
        for table in fixture.tables {
            try await table.realm.asyncWrite {
                for index in 0..<count {
                    let object = table.type.init()
                    object["id"] = "item-\(index)"
                    object["payload"] = table.type.className() + "-local-\(index)"
                    table.realm.add(object)
                    (object as! ChangeMetadataRecordable).refreshChangeMetadata(explicitlyModified: true)
                }
            }
        }
        try await fixture.adapter.didFinishImport()
    }

    @BigSyncBackgroundActor
    private func uploads(_ fixture: Fixture) async throws -> [PreparedRecordUpload] {
        var result = [PreparedRecordUpload]()
        for table in fixture.tables {
            result += try await fixture.adapter.preparedRecordsToUpload(
                limit: 1_000, restrictedToEntityType: table.type.className()
            )
        }
        return result
    }

    private func generations(_ records: [PreparedRecordUpload]) -> [String: String] {
        Dictionary(uniqueKeysWithValues: records.compactMap {
            item -> (String, String)? in
            item.generation.map { (item.record.recordID.recordName, $0) }
        })
    }

    private func journal(_ table: Table) -> [String: String] {
        Dictionary(uniqueKeysWithValues: table.realm.objects(BigSyncPendingMutation.self)
            .map { ($0.recordName, $0.generation) })
    }

    @BigSyncBackgroundActor
    func testGroupedUploadAcknowledgementsClearOnlyExactSavedGenerationsInBothRealms() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }
        try await seed(fixture)
        let prepared = try await uploads(fixture)
        XCTAssertEqual(prepared.count, 80)
        let sentGenerations = generations(prepared)
        var acknowledged = [CKRecord]()
        var changedNames = Set<String>()
        for table in fixture.tables {
            let records = prepared.map(\.record).filter { $0.recordType == table.type.className() }
                .sorted { $0.recordID.recordName < $1.recordID.recordName }
            acknowledged += records.prefix(20)
            let victim = try XCTUnwrap(records.first)
            let name = victim.recordID.recordName
            changedNames.insert(name)
            let id = String(name.dropFirst(table.type.className().count + 1))
            try await table.realm.asyncWrite {
                let object = try XCTUnwrap(table.realm.object(ofType: table.type, forPrimaryKey: id))
                object["payload"] = "newer-local-value"
                (object as! ChangeMetadataRecordable).refreshChangeMetadata(explicitlyModified: true)
            }
        }
        // Deliberately leave newer journals unforwarded until acknowledgement.
        let before = fixture.tables.map(journal)
        try await fixture.adapter.didUpload(
            savedRecords: acknowledged, matchingGenerations: sentGenerations
        )
        let savedNames = Set(acknowledged.map { $0.recordID.recordName })
        for (index, table) in fixture.tables.enumerated() {
            table.realm.refresh()
            let expected = before[index].filter { name, _ in
                !savedNames.contains(name) || changedNames.contains(name)
            }
            XCTAssertEqual(journal(table), expected)
            for name in changedNames where before[index][name] != nil {
                XCTAssertNotEqual(before[index][name], sentGenerations[name])
            }
        }
        XCTAssertTrue(fixture.adapter.hasChanges)
    }

    @BigSyncBackgroundActor
    func testGroupedDeletionAcknowledgementsPreserveNewerTombstonesAndUnsavedGroups() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }
        try await seed(fixture)
        let initial = try await uploads(fixture)
        try await fixture.adapter.didUpload(savedRecords: initial.map(\.record), matchingGenerations: generations(initial))
        for table in fixture.tables {
            table.realm.refresh()
            XCTAssertTrue(journal(table).isEmpty)
            try await table.realm.asyncWrite {
                for object in table.realm.objects(table.type) {
                    object["isDeleted"] = true
                    (object as! ChangeMetadataRecordable).refreshChangeMetadata(explicitlyModified: true)
                }
            }
        }
        try await fixture.adapter.didFinishImport()
        var prepared = [PreparedRecordDeletion]()
        for table in fixture.tables {
            prepared += try await fixture.adapter.preparedRecordDeletions(
                limit: 1_000, restrictedToEntityType: table.type.className()
            )
        }
        XCTAssertEqual(prepared.count, 80)
        let sentGenerations = Dictionary(uniqueKeysWithValues: prepared.compactMap {
            item -> (String, String)? in
            item.generation.map { (item.recordID.recordName, $0) }
        })
        var acknowledged = [CKRecord.ID]()
        var changedNames = Set<String>()
        for table in fixture.tables {
            let prefix = table.type.className() + "."
            let records = prepared.map(\.recordID).filter { $0.recordName.hasPrefix(prefix) }
                .sorted { $0.recordName < $1.recordName }
            acknowledged += records.prefix(20)
            let victim = try XCTUnwrap(records.first)
            changedNames.insert(victim.recordName)
            let id = String(victim.recordName.dropFirst(prefix.count))
            try await table.realm.asyncWrite {
                let object = try XCTUnwrap(table.realm.object(ofType: table.type, forPrimaryKey: id))
                object["payload"] = "newer-tombstone"
                (object as! ChangeMetadataRecordable).refreshChangeMetadata(explicitlyModified: true)
            }
        }
        let before = fixture.tables.map(journal)
        try await fixture.adapter.didDelete(recordIDs: acknowledged, matchingGenerations: sentGenerations)
        let savedNames = Set(acknowledged.map(\.recordName))
        for (index, table) in fixture.tables.enumerated() {
            table.realm.refresh()
            XCTAssertEqual(journal(table), before[index].filter { name, _ in
                !savedNames.contains(name) || changedNames.contains(name)
            })
            XCTAssertTrue(table.realm.objects(table.type).allSatisfy { $0["isDeleted"] as? Bool == true })
        }
        XCTAssertTrue(fixture.adapter.hasChanges)
    }

    @BigSyncBackgroundActor
    func testInboundGroupsRouteIdenticalPrimaryKeysToCorrectRealmsWithoutJournaling() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }
        let date = Date(timeIntervalSinceReferenceDate: 10_000)
        var records = [CKRecord]()
        // Interleave schemas in every 100-record write chunk.
        for index in 0..<120 {
            for table in fixture.tables {
                let type = table.type.className()
                let record = CKRecord(recordType: type, recordID: CKRecord.ID(
                    recordName: type + ".item-\(index)", zoneID: fixture.zone
                ))
                record["payload"] = type + "-remote-\(index)" as CKRecordValue
                record["createdAt"] = date as CKRecordValue
                record["modifiedAt"] = date as CKRecordValue
                record["explicitlyModifiedAt"] = date as CKRecordValue
                record["isDeleted"] = false as CKRecordValue
                records.append(record)
            }
        }
        let dispositions = try await fixture.adapter.saveChanges(in: records, forceSave: true)
        XCTAssertEqual(dispositions.count, records.count)
        for table in fixture.tables {
            table.realm.refresh()
            XCTAssertEqual(table.realm.objects(table.type).count, 120)
            XCTAssertTrue(journal(table).isEmpty)
            for index in 0..<120 {
                let object = try XCTUnwrap(table.realm.object(ofType: table.type, forPrimaryKey: "item-\(index)"))
                XCTAssertEqual(object["payload"] as? String, table.type.className() + "-remote-\(index)")
            }
        }
    }
}
