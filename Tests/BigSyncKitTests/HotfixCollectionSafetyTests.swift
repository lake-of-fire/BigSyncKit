import CloudKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(HotfixCollectionReviewChild)
private final class HotfixCollectionReviewChild: Object,
    ChangeMetadataRecordable, SoftDeletable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted(originProperty: "children")
    var parents: LinkingObjects<HotfixCollectionReviewSupported>
}

@objc(HotfixCollectionReviewSupported)
private final class HotfixCollectionReviewSupported: Object,
    ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var names: List<String>
    @Persisted var scores: MutableSet<Int>
    @Persisted var urls: List<URL>
    @Persisted var children: List<HotfixCollectionReviewChild>
    @Persisted var relatedChildren: MutableSet<HotfixCollectionReviewChild>
}

@objc(HotfixCollectionReviewObjectMap)
private final class HotfixCollectionReviewObjectMap: Object,
    ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var children: Map<String, HotfixCollectionReviewChild?>
}

@objc(HotfixCollectionReviewUnkeyed)
private final class HotfixCollectionReviewUnkeyed: Object {
    @Persisted var payload = "retained-local-value"
}

@objc(HotfixCollectionReviewUnsupported)
private final class HotfixCollectionReviewUnsupported: Object,
    ChangeMetadataRecordable, SyncSkippablePropertiesModel {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var selectedProperty = ""
    @Persisted var nullableStrings: List<String?>
    @Persisted var nullableIntegers: MutableSet<Int?>
    @Persisted var objectIdentifiers: List<ObjectId>
    @Persisted var identifierSet: MutableSet<ObjectId>
    @Persisted var externalIdentifier = ObjectId.generate()
    @Persisted var unkeyedTarget: HotfixCollectionReviewUnkeyed?

    static let candidateProperties: Set<String> = [
        "nullableStrings", "nullableIntegers", "objectIdentifiers",
        "identifierSet", "externalIdentifier", "unkeyedTarget",
    ]

    func skipSyncingProperties() -> Set<String>? {
        // Select one real Realm field per case without changing global schema
        // or depending on which property Realm enumerates first.
        Self.candidateProperties.subtracting([selectedProperty])
            .union(["selectedProperty"])
    }
}

final class HotfixCollectionSafetyTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (
        adapter: RealmSwiftAdapter, target: Realm, tracking: Realm
    ) {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "hotfix-collection-tracking-" + nonce
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "hotfix-collection-target-" + nonce
        target.objectTypes = [
            HotfixCollectionReviewChild.self,
            HotfixCollectionReviewSupported.self,
            HotfixCollectionReviewObjectMap.self,
            HotfixCollectionReviewUnsupported.self,
            HotfixCollectionReviewUnkeyed.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [HotfixCollectionReviewUnkeyed.className()],
            recordZoneID: CKRecordZone.ID(
                zoneName: "hotfix-collection-review",
                ownerName: CKCurrentUserDefaultName
            ),
            logger: Logger(label: "HotfixCollectionSafetyTests"),
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("hotfix-collection-assets-" + nonce)
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .server
        return (
            adapter,
            try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first),
            try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        )
    }

    private func mapRecord(id: String, zone: CKRecordZone.ID) -> CKRecord {
        let record = CKRecord(
            recordType: HotfixCollectionReviewObjectMap.className(),
            recordID: CKRecord.ID(
                recordName: HotfixCollectionReviewObjectMap.className() + "." + id,
                zoneID: zone
            )
        )
        let date = Date(timeIntervalSinceReferenceDate: 20_000)
        record["createdAt"] = date as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    func testAbsentObjectMapRejectsNewReceiverWithoutDeferredToOneIntent()
        async throws {
        let fixture = try await fixture()
        let record = mapRecord(id: "new-map", zone: fixture.adapter.recordZoneID)
        do {
            _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("An absent object map must not be admitted as a to-one clear")
        } catch let error as RealmSwiftRemoteRecordDecodingError {
            guard case .malformedField(_, let property, _) = error else {
                return XCTFail("Unexpected decoding error: \(error)")
            }
            XCTAssertEqual(property, "children")
        }
        fixture.target.refresh()
        fixture.tracking.refresh()
        XCTAssertNil(fixture.target.object(
            ofType: HotfixCollectionReviewObjectMap.self, forPrimaryKey: "new-map"
        ))
        XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(fixture.tracking.objects(PendingRelationship.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testAbsentObjectMapRejectsUpdateAndRollsBackOtherFields() async throws {
        let fixture = try await fixture()
        let child = HotfixCollectionReviewChild()
        child.id = "retained-child"
        let parent = HotfixCollectionReviewObjectMap()
        parent.id = "retained-map"
        let originalDate = Date(timeIntervalSinceReferenceDate: 10_000)
        parent.createdAt = originalDate
        parent.modifiedAt = originalDate
        try await fixture.target.asyncWrite {
            fixture.target.add(child)
            fixture.target.add(parent)
            parent.children["first"] = child
        }
        let record = mapRecord(id: parent.id, zone: fixture.adapter.recordZoneID)
        do {
            _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Unsupported map update must roll back")
        } catch let error as RealmSwiftRemoteRecordDecodingError {
            guard case .malformedField(_, let property, _) = error else {
                return XCTFail("Unexpected decoding error: \(error)")
            }
            XCTAssertEqual(property, "children")
        }
        fixture.target.refresh()
        fixture.tracking.refresh()
        XCTAssertEqual(parent.children["first"]??.id, child.id)
        XCTAssertEqual(parent.modifiedAt, originalDate)
        XCTAssertNil(parent.explicitlyModifiedAt)
        XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(fixture.tracking.objects(PendingRelationship.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testPresentObjectMapStillRejectsRatherThanInventingWireFormat()
        async throws {
        let fixture = try await fixture()
        let record = mapRecord(id: "present-map", zone: fixture.adapter.recordZoneID)
        record["children"] = try PropertyListSerialization.data(
            fromPropertyList: [String: String](), format: .binary, options: 0
        ) as CKRecordValue
        do {
            _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Empty encoded object maps remain unsupported")
        } catch is RealmSwiftRemoteRecordDecodingError {
        }
        fixture.target.refresh()
        XCTAssertNil(fixture.target.object(
            ofType: HotfixCollectionReviewObjectMap.self,
            forPrimaryKey: "present-map"
        ))
    }

    @BigSyncBackgroundActor
    private func assertUnsupportedUploadRetainsJournal(_ property: String)
        async throws {
        let fixture = try await fixture()
        let object = HotfixCollectionReviewUnsupported()
        object.id = "unsupported-" + property
        object.selectedProperty = property
        object.nullableStrings.append(objectsIn: ["first", nil, "last"])
        object.nullableIntegers.insert(7)
        object.nullableIntegers.insert(nil)
        let objectID = ObjectId.generate()
        object.objectIdentifiers.append(objectID)
        object.identifierSet.insert(objectID)
        object.unkeyedTarget = HotfixCollectionReviewUnkeyed()
        let authoredAt = Date(timeIntervalSinceReferenceDate: 30_000)
        try await fixture.target.asyncWrite {
            fixture.target.add(object)
            object.refreshChangeMetadata(explicitlyModified: true, at: authoredAt)
        }
        let recordName = HotfixCollectionReviewUnsupported.className() + "." + object.id
        let generation = try XCTUnwrap(fixture.target.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName
        )?.generation)
        try await fixture.adapter.didFinishImport()
        do {
            _ = try await fixture.adapter.preparedRecordsToUpload(
                limit: 10,
                restrictedToEntityType: HotfixCollectionReviewUnsupported.className()
            )
            XCTFail("Unsupported field \(property) must not yield an acknowledgeable record")
        } catch let error as RealmSwiftAdapterError {
            guard case .unsupportedUploadProperty(let entityType, let name) = error else {
                return XCTFail("Unexpected upload error: \(error)")
            }
            XCTAssertEqual(entityType, HotfixCollectionReviewUnsupported.className())
            XCTAssertEqual(name, property)
        }
        fixture.target.refresh()
        fixture.tracking.refresh()
        XCTAssertEqual(fixture.target.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName
        )?.generation, generation)
        XCTAssertEqual(fixture.tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        )?.pendingGeneration, generation)
        XCTAssertEqual(Array(object.nullableStrings), ["first", nil, "last"])
        XCTAssertTrue(object.nullableIntegers.contains(nil))
        XCTAssertEqual(Array(object.objectIdentifiers), [objectID])
        XCTAssertTrue(object.identifierSet.contains(objectID))
        XCTAssertEqual(object.unkeyedTarget?.payload, "retained-local-value")
        XCTAssertEqual(object.explicitlyModifiedAt, authoredAt)
    }

    @BigSyncBackgroundActor
    func testNullableListCannotSilentlyBecomeAnAbsentField() async throws {
        try await assertUnsupportedUploadRetainsJournal("nullableStrings")
    }

    @BigSyncBackgroundActor
    func testNullableSetCannotSilentlyBecomeAnAbsentField() async throws {
        try await assertUnsupportedUploadRetainsJournal("nullableIntegers")
    }

    @BigSyncBackgroundActor
    func testUnsupportedListElementCannotBeAcknowledged() async throws {
        try await assertUnsupportedUploadRetainsJournal("objectIdentifiers")
    }

    @BigSyncBackgroundActor
    func testUnsupportedSetElementCannotBeAcknowledged() async throws {
        try await assertUnsupportedUploadRetainsJournal("identifierSet")
    }

    @BigSyncBackgroundActor
    func testUnsupportedScalarAndUnkeyedRelationshipCannotBeAcknowledged() async throws {
        try await assertUnsupportedUploadRetainsJournal("externalIdentifier")
        try await assertUnsupportedUploadRetainsJournal("unkeyedTarget")
    }

    @BigSyncBackgroundActor
    func testSupportedCollectionsAndBacklinksRetainExistingTransport() async throws {
        let fixture = try await fixture()
        let child = HotfixCollectionReviewChild()
        child.id = "live-child"
        let deletedChild = HotfixCollectionReviewChild()
        deletedChild.id = "deleted-child"
        deletedChild.isDeleted = true
        let parent = HotfixCollectionReviewSupported()
        parent.id = "supported-parent"
        parent.names.append(objectsIn: ["second", "first", "second"])
        parent.scores.insert(4)
        parent.scores.insert(9)
        let url = try XCTUnwrap(URL(string: "https://example.invalid/reading?a=1"))
        parent.urls.append(url)
        parent.children.append(objectsIn: [child, deletedChild, child])
        parent.relatedChildren.insert(child)
        parent.relatedChildren.insert(deletedChild)
        try await fixture.target.asyncWrite {
            fixture.target.add(parent)
            parent.refreshChangeMetadata(explicitlyModified: true)
            child.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let batch = try await fixture.adapter.prepareUploadBatch(limit: 20)
        let parentName = HotfixCollectionReviewSupported.className() + "." + parent.id
        let childName = HotfixCollectionReviewChild.className() + "." + child.id
        let uploadedParent = try XCTUnwrap(batch.records.first {
            $0.recordID.recordName == parentName
        })
        let uploadedChild = try XCTUnwrap(batch.records.first {
            $0.recordID.recordName == childName
        })
        XCTAssertEqual(uploadedParent["names"] as? [String], ["second", "first", "second"])
        XCTAssertEqual(Set(try XCTUnwrap(uploadedParent["scores"] as? [Int])), [4, 9])
        XCTAssertEqual(uploadedParent["urls"] as? [String], [url.absoluteString])
        XCTAssertEqual(uploadedParent["children"] as? [String], [childName, childName])
        XCTAssertEqual(uploadedParent["relatedChildren"] as? [String], [childName])
        XCTAssertNil(uploadedChild["parents"], "Derived backlinks must not block upload")
        try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await fixture.target.asyncWrite {
            parent.names.removeAll()
            parent.scores.removeAll()
            parent.urls.removeAll()
            parent.children.removeAll()
            parent.relatedChildren.removeAll()
            parent.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let cleared = try await fixture.adapter.prepareUploadBatch(limit: 20)
        let clearedParent = try XCTUnwrap(cleared.records.first {
            $0.recordID.recordName == parentName
        })
        for field in ["names", "scores", "urls", "children", "relatedChildren"] {
            XCTAssertNil(clearedParent[field])
        }
    }
}
