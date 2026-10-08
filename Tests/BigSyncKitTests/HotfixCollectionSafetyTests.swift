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

@objc(HotfixCollectionReviewAssets)
private final class HotfixCollectionReviewAssets: Object,
    ChangeMetadataRecordable {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var text = ""
    @Persisted var number = 0
    @Persisted var payload = Data()
    @Persisted var optionalPayload: Data?
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
    private lazy var realmFixtureOwner = RealmAdapterFixtureOwner(testCase: self)

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
            HotfixCollectionReviewAssets.self,
            HotfixCollectionReviewUnsupported.self,
            HotfixCollectionReviewUnkeyed.self,
            BigSyncPendingMutation.self,
        ]
        let assetDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("hotfix-collection-assets-" + nonce)
        realmFixtureOwner.ownDirectory(assetDirectory)
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
            assetDirectoryURL: assetDirectory
        )
        realmFixtureOwner.own(adapter)
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

    private func assetRecord(id: String, zone: CKRecordZone.ID,
                             time: TimeInterval = 20_000) -> CKRecord {
        let record = CKRecord(recordType: HotfixCollectionReviewAssets.className(),
            recordID: .init(recordName: HotfixCollectionReviewAssets.className() + "." + id,
                            zoneID: zone))
        let date = Date(timeIntervalSinceReferenceDate: time)
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 10_000) as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["text"] = "retained-text" as CKRecordValue
        record["number"] = 7 as CKRecordValue
        record["payload"] = Data([1, 2, 3]) as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func assetFile(contents: Data = Data([4, 5, 6])) throws -> URL {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("hotfix-incoming-asset-" + UUID().uuidString)
        realmFixtureOwner.ownDirectory(directory)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent("payload")
        try contents.write(to: url)
        return url
    }

    @BigSyncBackgroundActor
    func testAssetInScalarFieldRejectsNewReceiverBeforeRealmAssignment() async throws {
        let fixture = try await fixture()
        let url = try assetFile()
        for field in ["text", "number", "isDeleted", "createdAt", "modifiedAt", "explicitlyModifiedAt"] {
            let record = assetRecord(id: "new-" + field, zone: fixture.adapter.recordZoneID)
            record[field] = CKAsset(fileURL: url)
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("An asset in \(field) must throw before Realm's typed setter")
            } catch let error as RealmSwiftRemoteRecordDecodingError {
                guard case .malformedField(let name, let property, _) = error else {
                    return XCTFail("Unexpected decoding error: \(error)")
                }
                XCTAssertEqual(name, record.recordID.recordName)
                XCTAssertEqual(property, field)
            }
        }
        fixture.target.refresh()
        fixture.tracking.refresh()
        XCTAssertTrue(fixture.target.objects(HotfixCollectionReviewAssets.self).isEmpty)
        XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(fixture.tracking.objects(SyncedEntity.self).isEmpty)
        XCTAssertTrue(fixture.tracking.objects(PendingRelationship.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testAssetInScalarFieldRollsBackExistingValueAndTracking() async throws {
        let fixture = try await fixture()
        let original = assetRecord(id: "existing", zone: fixture.adapter.recordZoneID)
        _ = try await fixture.adapter.saveChanges(in: [original], forceSave: true)
        fixture.target.refresh()
        let object = try XCTUnwrap(fixture.target.object(
            ofType: HotfixCollectionReviewAssets.self, forPrimaryKey: "existing"))
        let tracking = try XCTUnwrap(fixture.tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: original.recordID.recordName))
        let originalSystemFields = tracking.encodedRecord
        let originalFields = try BigSyncRecordFingerprint.fields(of: object)
        let url = try assetFile()
        for field in ["text", "number", "isDeleted", "createdAt", "modifiedAt", "explicitlyModifiedAt"] {
            let incoming = assetRecord(id: "existing", zone: fixture.adapter.recordZoneID, time: 30_000)
            incoming["text"] = "uncommitted-text" as CKRecordValue
            incoming[field] = CKAsset(fileURL: url)
            do {
                _ = try await fixture.adapter.saveChanges(in: [incoming], forceSave: true)
                XCTFail("An invalid scalar asset must roll back the entire target write")
            } catch is RealmSwiftRemoteRecordDecodingError { }
            fixture.target.refresh()
            fixture.tracking.refresh()
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: object), originalFields)
            XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 20_000))
            XCTAssertEqual(object.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 20_000))
            XCTAssertEqual(tracking.encodedRecord, originalSystemFields)
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testComparisonDecoderRejectsAssetInScalarField() async throws {
        let fixture = try await fixture()
        let url = try assetFile()
        for field in ["text", "number", "isDeleted", "createdAt", "modifiedAt", "explicitlyModifiedAt"] {
            let incoming = assetRecord(id: "comparison", zone: fixture.adapter.recordZoneID)
            incoming[field] = CKAsset(fileURL: url)
            XCTAssertThrowsError(try fixture.adapter.decodedComparisonObject(
                incoming, type: HotfixCollectionReviewAssets.self
            )) { error in
                guard let decoding = error as? RealmSwiftRemoteRecordDecodingError,
                      case .malformedField(_, let property, _) = decoding else {
                    return XCTFail("Unexpected decoding error: \(error)")
                }
                XCTAssertEqual(property, field)
            }
        }
    }

    @BigSyncBackgroundActor
    func testReadableDataAssetsDecodeAndMissingFilesRollBack() async throws {
        let fixture = try await fixture()
        let bytes = Data([4, 5, 6])
        let url = try assetFile(contents: bytes)
        let record = assetRecord(id: "data", zone: fixture.adapter.recordZoneID)
        record["payload"] = CKAsset(fileURL: url)
        record["optionalPayload"] = CKAsset(fileURL: url)
        _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        fixture.target.refresh()
        let object = try XCTUnwrap(fixture.target.object(
            ofType: HotfixCollectionReviewAssets.self, forPrimaryKey: "data"))
        XCTAssertEqual(object.payload, bytes)
        XCTAssertEqual(object.optionalPayload, bytes)
        let comparison = try fixture.adapter.decodedComparisonObject(record,
            type: HotfixCollectionReviewAssets.self)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: object),
                       try BigSyncRecordFingerprint.fields(of: comparison))
        try FileManager.default.removeItem(at: url)
        for field in ["payload", "optionalPayload"] {
            let missing = assetRecord(id: "data", zone: fixture.adapter.recordZoneID, time: 30_000)
            missing["text"] = "uncommitted-text" as CKRecordValue
            missing[field] = CKAsset(fileURL: url)
            do {
                _ = try await fixture.adapter.saveChanges(in: [missing], forceSave: true)
                XCTFail("Missing \(field) asset must retain the prior target")
            } catch let error as RealmSwiftRemoteRecordDecodingError {
                guard case .malformedField(_, let property, _) = error else {
                    return XCTFail("Unexpected decoding error: \(error)")
                }
                XCTAssertEqual(property, field)
            }
            fixture.target.refresh()
            XCTAssertEqual(object.payload, bytes)
            XCTAssertEqual(object.optionalPayload, bytes)
            XCTAssertEqual(object.text, "retained-text")
            XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 20_000))
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testSharedTraversalSkipsUnsupportedFieldsAndBacklinksAndRetainsDeferredClears() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }
        try await assertTransportExclusionsAndDeferredClears(adapter: fixture.adapter)
    }

    @RealmBackgroundActor
    private func assertTransportExclusionsAndDeferredClears(adapter: RealmSwiftAdapter) throws {
        func record(for type: Object.Type) -> CKRecord {
            let record = CKRecord(recordType: type.className(), recordID: .init(
                recordName: type.className() + ".traversal", zoneID: adapter.recordZoneID
            ))
            let date = Date(timeIntervalSinceReferenceDate: 20_000)
            record["createdAt"] = date as CKRecordValue
            record["modifiedAt"] = date as CKRecordValue
            record["explicitlyModifiedAt"] = date as CKRecordValue
            record["isDeleted"] = false as CKRecordValue
            record["id"] = "untrusted-wire-id" as CKRecordValue
            return record
        }
        let skipped = HotfixCollectionReviewUnsupported()
        skipped.id = "admitted-id"
        let skippedRecord = record(for: HotfixCollectionReviewUnsupported.self)
        skippedRecord["selectedProperty"] = "unkeyedTarget" as CKRecordValue
        for field in HotfixCollectionReviewUnsupported.candidateProperties {
            skippedRecord[field] = "malformed-but-skipped" as CKRecordValue
        }
        let skippedRequests = try adapter.applyChanges(
            in: skippedRecord, to: skipped, syncedEntityID: skippedRecord.recordID.recordName,
            syncedEntityState: .synced, entityType: skippedRecord.recordType,
            isNewlyCreatedReceiver: false, acceptsServerSnapshot: true
        )
        let comparison = try XCTUnwrap(adapter.decodedComparisonObject(
            skippedRecord, type: HotfixCollectionReviewUnsupported.self
        ) as? HotfixCollectionReviewUnsupported)
        XCTAssertEqual(skipped.id, "admitted-id")
        XCTAssertEqual(comparison.id, "")
        XCTAssertEqual(skipped.selectedProperty, "")
        XCTAssertEqual(comparison.selectedProperty, "")
        XCTAssertTrue(skipped.nullableStrings.isEmpty)
        XCTAssertTrue(comparison.nullableStrings.isEmpty)
        XCTAssertNil(skipped.unkeyedTarget)
        XCTAssertNil(comparison.unkeyedTarget)
        XCTAssertTrue(skippedRequests.isEmpty)

        let child = HotfixCollectionReviewChild()
        let childRecord = record(for: HotfixCollectionReviewChild.self)
        childRecord["parents"] = "malformed-backlink" as CKRecordValue
        let backlinkRequests = try adapter.applyChanges(
            in: childRecord, to: child, syncedEntityID: childRecord.recordID.recordName,
            syncedEntityState: .synced, entityType: childRecord.recordType,
            isNewlyCreatedReceiver: true
        )
        _ = try adapter.decodedComparisonObject(childRecord, type: HotfixCollectionReviewChild.self)
        XCTAssertTrue(backlinkRequests.isEmpty)

        // Relationship-capable incoming apply still returns deferred intents;
        // it is deliberately outside scalar comparison capability admission.
        let parent = HotfixCollectionReviewSupported()
        parent.names.append("stale")
        parent.children.append(child)
        parent.relatedChildren.insert(child)
        let parentRecord = record(for: HotfixCollectionReviewSupported.self)
        let requests = try adapter.applyChanges(
            in: parentRecord, to: parent, syncedEntityID: parentRecord.recordID.recordName,
            syncedEntityState: .synced, entityType: parentRecord.recordType,
            isNewlyCreatedReceiver: false, acceptsServerSnapshot: true
        )
        XCTAssertTrue(parent.names.isEmpty)
        XCTAssertEqual(Set(requests.map(\.name)), ["children", "relatedChildren"])
        XCTAssertTrue(requests.allSatisfy {
            $0.targetIdentifiers.isEmpty && $0.syncedEntityID == parentRecord.recordID.recordName
        })
        XCTAssertEqual(parent.children.count, 1, "Materialization follows the existing deferred-relationship boundary")
        XCTAssertEqual(parent.relatedChildren.count, 1)
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
