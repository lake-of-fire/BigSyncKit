import XCTest
import Foundation
import CloudKit
import Logging
@testable import BigSyncKit
import RealmSwift
import RealmSwiftGaps

@objc(RA1ParityRecord)
private final class RA1ParityRecord: Object, ChangeMetadataRecordable {
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

@objc(RA1ParityObjectMap)
private final class RA1ParityObjectMap: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var children: Map<String, RA1ParityRecord?>
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(RA1ParityRelationshipParent)
private final class RA1ParityRelationshipParent: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var children: List<RA1ParityRecord>
    @Persisted var relatedChildren: MutableSet<RA1ParityRecord>
    @Persisted var favoriteChild: RA1ParityRecord?
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// Real Realm/adapter regression coverage. No CloudKit service, simulated
/// persistence implementation or expected-failure assertions are involved.
final class RA1GenericAdapterParityTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "ra1-adapter-parity-tracking-" + nonce
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "ra1-adapter-parity-target-" + nonce
        target.objectTypes = [RA1ParityRecord.self, RA1ParityObjectMap.self,
                              RA1ParityRelationshipParent.self, BigSyncPendingMutation.self]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "ra1-adapter-parity", ownerName: CKCurrentUserDefaultName),
            logger: Logger(label: "RA1AdapterParity"),
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory.appendingPathComponent("ra1-adapter-parity-assets-" + nonce)
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
        let record = CKRecord(recordType: RA1ParityRecord.className(), recordID: .init(recordName: RA1ParityRecord.className() + "." + id, zoneID: zone))
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
        let value = try XCTUnwrap(realm.object(ofType: RA1ParityRecord.self, forPrimaryKey: "first"))
        let pendingCount = realm.objects(BigSyncPendingMutation.self).count
        XCTAssertEqual(value.payload, "remote-payload", "Constructor defaults are not a competing local revision")
        XCTAssertEqual(pendingCount, 0, "An admitted remote creation must not fabricate a journaled local edit")
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(batch.records.isEmpty, "A download must not queue constructor-default replacement data")
    }

    @BigSyncBackgroundActor
    func testReviewFirstImportWithExplicitTimestampControl() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = remote(id: "explicit", zone: adapter.recordZoneID,
                              date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: RA1ParityRecord.self, forPrimaryKey: "explicit")?.payload, "remote-payload")
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
        let value = try XCTUnwrap(realm.object(ofType: RA1ParityRecord.self, forPrimaryKey: "clear"))
        XCTAssertEqual(value.categoryID, category)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        let cleared = remote(id: "clear", zone: adapter.recordZoneID,
                             date: Date(timeIntervalSinceReferenceDate: 2000), explicit: true)
        XCTAssertNil(cleared["categoryID"])
        _ = try await adapter.saveChanges(in: [cleared], forceSave: true)
        realm.refresh()
        XCTAssertNil(value.categoryID, "Full-record absence must clear an optional UUID")
        try await realm.asyncWrite {
            value.payload = "unrelated-local-edit"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first { $0.recordID == cleared.recordID })
        XCTAssertNil(outgoing["categoryID"], "An unrelated local edit must not reintroduce the cleared remote relationship")
    }

    @BigSyncBackgroundActor
    func testReviewOptionalUUIDReplacementControl() async throws {
        let (adapter, realm) = try await fixture()
        let first = UUID(), second = UUID()
        _ = try await adapter.saveChanges(in: [remote(id: "replace", zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 1000), explicit: true, categoryID: first)], forceSave: true)
        _ = try await adapter.saveChanges(in: [remote(id: "replace", zone: adapter.recordZoneID, date: Date(timeIntervalSinceReferenceDate: 2000), explicit: true, categoryID: second)], forceSave: true)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: RA1ParityRecord.self, forPrimaryKey: "replace")?.categoryID, second)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewExistingNewerLocalAuthorityControl() async throws {
        let (adapter, realm) = try await fixture()
        let local = RA1ParityRecord()
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
        } catch let error as RealmSwiftRemoteRecordDecodingError {
            guard case let .malformedField(recordName, propertyName, _) = error else {
                return XCTFail("Unexpected UUID decoding error: \(error)")
            }
            XCTAssertEqual(recordName, record.recordID.recordName)
            XCTAssertEqual(propertyName, "categoryID")
        }
        realm.refresh()
        XCTAssertNil(realm.object(ofType: RA1ParityRecord.self, forPrimaryKey: "malformed"))
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testReviewNilReplayAndAbsentRequiredUUIDPreserveCompatibility() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = remote(id: "nil-replay", zone: adapter.recordZoneID,
                              date: Date(timeIntervalSinceReferenceDate: 1000), explicit: false)
        _ = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        let object = try XCTUnwrap(realm.object(ofType: RA1ParityRecord.self,
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
        let local = RA1ParityRecord()
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
        let object = RA1ParityRecord()
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
        let received = try XCTUnwrap(target.object(ofType: RA1ParityRecord.self,
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
        let child = RA1ParityRecord()
        child.id = "child"
        let parent = RA1ParityObjectMap()
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
                restrictedToEntityType: RA1ParityObjectMap.className())
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

    @BigSyncBackgroundActor
    func testNonJournaledTargetChangeRejectsOrdinaryAndForcedImportThenReplays() async throws {
        for forceSave in [false, true] {
            let (adapter, realm) = try await fixture()
            let persistence = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let base = Date(timeIntervalSinceReferenceDate: 10_000)
            let object = RA1ParityRecord()
            object.id = "derived-collision"
            object.payload = "baseline"
            object.createdAt = base
            object.modifiedAt = base
            object.explicitlyModifiedAt = base
            try await realm.asyncWrite { realm.add(object) }
            let incoming = remote(id: object.id, zone: adapter.recordZoneID,
                date: base.addingTimeInterval(120), explicit: true)
            adapter._testBeforeImportedRecordTargetWrite = {
                try await realm.asyncWrite {
                    object.refreshChangeMetadata(explicitlyModified: false,
                        at: base.addingTimeInterval(60))
                }
            }
            defer { adapter._testBeforeImportedRecordTargetWrite = nil }
            do {
                _ = try await adapter.saveChanges(in: [incoming], forceSave: forceSave)
                XCTFail("An unapplied server payload must not be acknowledged as unchanged")
            } catch let error as RealmSwiftInboundTargetChangedError {
                XCTAssertEqual(error.recordName, incoming.recordID.recordName)
            }
            realm.refresh()
            persistence.refresh()
            XCTAssertEqual(object.payload, "baseline")
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertNil(persistence.object(ofType: SyncedEntity.self,
                forPrimaryKey: incoming.recordID.recordName))
            adapter._testBeforeImportedRecordTargetWrite = nil
            let replay = try await adapter.saveChanges(in: [incoming], forceSave: forceSave)
            realm.refresh()
            XCTAssertEqual(replay.first?.disposition, .applied)
            XCTAssertEqual(object.payload, "remote-payload")
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testJournaledTargetChangePreservesItsExactGenerationInsteadOfRetrying() async throws {
        let (adapter, realm) = try await fixture()
        let base = Date(timeIntervalSinceReferenceDate: 10_000)
        let object = RA1ParityRecord()
        object.id = "journaled-collision"
        object.payload = "baseline"
        object.createdAt = base
        object.modifiedAt = base
        object.explicitlyModifiedAt = base
        try await realm.asyncWrite { realm.add(object) }
        let incoming = remote(id: object.id, zone: adapter.recordZoneID,
            date: base.addingTimeInterval(120), explicit: true)
        adapter._testBeforeImportedRecordTargetWrite = {
            try await realm.asyncWrite {
                object.payload = "local-intent"
                object.refreshChangeMetadata(explicitlyModified: true,
                    at: base.addingTimeInterval(60))
            }
        }
        defer { adapter._testBeforeImportedRecordTargetWrite = nil }
        let result = try await adapter.saveChanges(in: [incoming], forceSave: true)
        realm.refresh()
        let pending = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: incoming.recordID.recordName))
        XCTAssertEqual(result.first?.disposition, .preservedPendingLocal(generation: pending.generation))
        XCTAssertEqual(object.payload, "local-intent")
    }

    @BigSyncBackgroundActor
    func testObjectListSetAndScalarRelationshipRoundTripThenClear() async throws {
        let (sender, source) = try await fixture()
        let (receiver, target) = try await fixture()
        receiver.mergePolicy = .server
        let first = RA1ParityRecord()
        first.id = "first"
        let second = RA1ParityRecord()
        second.id = "second"
        let parent = RA1ParityRelationshipParent()
        parent.id = "parent"
        parent.children.append(objectsIn: [second, first, second])
        parent.relatedChildren.insert(objectsIn: [first, second])
        parent.favoriteChild = first
        try await source.asyncWrite {
            source.add([first, second, parent])
            first.refreshChangeMetadata(explicitlyModified: true)
            second.refreshChangeMetadata(explicitlyModified: true)
            parent.refreshChangeMetadata(explicitlyModified: true)
        }
        try await sender.didFinishImport()
        let batch = try await sender.prepareUploadBatch(limit: 100)
        let record = try XCTUnwrap(batch.records.first { $0.recordType == RA1ParityRelationshipParent.className() })
        let firstID = RA1ParityRecord.className() + ".first"
        let secondID = RA1ParityRecord.className() + ".second"
        XCTAssertEqual(record["children"] as? [String], [secondID, firstID, secondID])
        XCTAssertEqual(Set(record["relatedChildren"] as? [String] ?? []), Set([firstID, secondID]))
        XCTAssertEqual(record["favoriteChild"] as? String, firstID)
        _ = try await receiver.saveChanges(in: batch.records, forceSave: true)
        try await receiver.persistImportedChanges()
        target.refresh()
        let received = try XCTUnwrap(target.object(ofType: RA1ParityRelationshipParent.self,
            forPrimaryKey: parent.id))
        XCTAssertEqual(Array(received.children.map(\.id)), ["second", "first", "second"])
        XCTAssertEqual(Set(received.relatedChildren.map(\.id)), Set(["first", "second"]))
        XCTAssertEqual(received.favoriteChild?.id, "first")
        try await sender.acknowledgeUploadedRecords(batch.records, from: batch)
        try await source.asyncWrite {
            parent.children.removeAll()
            parent.relatedChildren.removeAll()
            parent.favoriteChild = nil
            parent.refreshChangeMetadata(explicitlyModified: true)
        }
        try await sender.didFinishImport()
        let empty = try await sender.prepareUploadBatch(limit: 100)
        let cleared = try XCTUnwrap(empty.records.first { $0.recordType == RA1ParityRelationshipParent.className() })
        XCTAssertNil(cleared["children"])
        XCTAssertNil(cleared["relatedChildren"])
        XCTAssertNil(cleared["favoriteChild"])
        _ = try await receiver.saveChanges(in: empty.records, forceSave: true)
        try await receiver.persistImportedChanges()
        target.refresh()
        XCTAssertTrue(received.children.isEmpty)
        XCTAssertTrue(received.relatedChildren.isEmpty)
        XCTAssertNil(received.favoriteChild)
        XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
    }
}
