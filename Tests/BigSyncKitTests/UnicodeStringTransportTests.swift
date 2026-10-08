import CloudKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(BigSyncUnicodeStringTransportFixture)
private final class UnicodeStringTransportRow: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    static let bigSyncRecordContract = BigSyncRecordContract(policy: .atomicRecord)

    @Persisted(primaryKey: true) var id = "row"
    @Persisted var scalar = "unchanged"
    @Persisted var names: List<String>
    @Persisted var tags: MutableSet<String>
    @Persisted var urls: List<URL>
    @Persisted var translations: Map<String, String>
    @Persisted var counts: Map<String, Int>
    @Persisted var flags: Map<String, Bool>
    @Persisted var floatWeights: Map<String, Float>
    @Persisted var doubleWeights: Map<String, Double>
    @Persisted var dates: Map<String, Date>
    @Persisted var blobs: Map<String, Data>
    @Persisted var identifiersByKey: Map<String, UUID>
    @Persisted var identifiers: List<UUID>
    @Persisted var identifierSet: MutableSet<UUID>
    @Persisted var uuids: Map<String, UUID>
    @Persisted var integerValues: Map<String, Int>
    @Persisted var booleanValues: Map<String, Bool>
    @Persisted var floatValues: Map<String, Float>
    @Persisted var doubleValues: Map<String, Double>
    @Persisted var dateValues: Map<String, Date>
    @Persisted var dataValues: Map<String, Data>
    @Persisted var number = 7
    @Persisted var enabled = true
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date? = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var isDeleted = false
}

@objc(BigSyncUnicodeLegacyMapTransportFixture)
private final class UnicodeLegacyMapTransportRow: Object, ChangeMetadataRecordable {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "legacy-map"
    @Persisted var translations: Map<String, String>
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date? = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var isDeleted = false
}

final class UnicodeStringTransportTests: XCTestCase {

    @BigSyncBackgroundActor
    func testPrimitiveMapKeyReplacementAndCardinalityUseStoredByteIdentity() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let (composed, decomposed) = Self.pairs[0]
            let date = Date(timeIntervalSinceReferenceDate: 1_000.25)
            let data = Data([0, 1, 2])
            row.integerValues[composed] = 1
            row.booleanValues[composed] = true
            row.floatValues[composed] = 1.25
            row.doubleValues[composed] = 3.5
            row.dateValues[composed] = date
            row.dataValues[composed] = data
            let record = try Self.record(row, adapter)
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            let entries: [(String, Any)] = [
                ("integerValues", 1), ("booleanValues", true),
                ("floatValues", Float(1.25)), ("doubleValues", 3.5),
                ("dateValues", date), ("dataValues", data),
            ]
            let names = Set(entries.map { $0.0 })
            for (name, value) in entries {
                record[name] = try PropertyListSerialization.data(
                    fromPropertyList: [decomposed: value], format: .binary, options: 0) as CKRecordValue
            }
            XCTAssertEqual(Set(adapter.serverDifferencePropertyNames(record: record, object: row)), names)
            let decoded = try adapter.decodedComparisonObject(record, type: UnicodeStringTransportRow.self)
            let remote = try BigSyncRecordFingerprint.fields(of: decoded)
            let local = try BigSyncRecordFingerprint.fields(of: row)
            for name in names {
                XCTAssertNotEqual(remote[name], local[name], name)
            }

            // Realm retains both keys. A Swift String dictionary would merge
            // these same-valued members and conceal the changed cardinality.
            row.integerValues[decomposed] = 1
            row.booleanValues[decomposed] = true
            row.floatValues[decomposed] = 1.25
            row.doubleValues[decomposed] = 3.5
            row.dateValues[decomposed] = date
            row.dataValues[decomposed] = data
            XCTAssertEqual(row.integerValues.count, 2)
            XCTAssertEqual(row.booleanValues.count, 2)
            XCTAssertEqual(row.floatValues.count, 2)
            XCTAssertEqual(row.doubleValues.count, 2)
            XCTAssertEqual(row.dateValues.count, 2)
            XCTAssertEqual(row.dataValues.count, 2)
            XCTAssertEqual(Set(adapter.serverDifferencePropertyNames(record: record, object: row)), names)
        }
    }

    @BigSyncBackgroundActor
    func testUUIDMapReplayDecodesWireStringsAndDetectsRealChanges() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let first = UUID(uuidString: "ABCDEF00-0000-0000-0000-000000000001")!
            let second = UUID(uuidString: "ABCDEF00-0000-0000-0000-000000000002")!
            row.uuids["first"] = first
            row.uuids["second"] = second
            let record = try Self.record(row, adapter)
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row).isEmpty)

            // Spelling and property-list insertion order do not change a UUID.
            record["uuids"] = try Self.encodedMap([
                "second": second.uuidString.lowercased(),
                "first": first.uuidString.lowercased(),
            ])
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            let decoded = try adapter.decodedComparisonObject(record, type: UnicodeStringTransportRow.self)
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: decoded),
                           try BigSyncRecordFingerprint.fields(of: row))

            record["uuids"] = try Self.encodedMap(["first": second.uuidString, "second": second.uuidString])
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
            record["uuids"] = try Self.encodedMap(["first": first.uuidString])
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
            record["uuids"] = try Self.encodedMap(["first": "not-a-UUID", "second": second.uuidString])
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
            XCTAssertThrowsError(try adapter.decodedComparisonObject(record, type: UnicodeStringTransportRow.self))
            XCTAssertEqual(row.uuids["first"], first)
            XCTAssertEqual(row.uuids["second"], second)
            record["uuids"] = nil
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
            row.uuids.removeAll()
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
        }
    }

    @BigSyncBackgroundActor
    func testUUIDMapKeyReplacementUsesStoredByteIdentity() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let uuid = UUID(uuidString: "ABCDEF00-0000-0000-0000-000000000001")!
            let (composed, decomposed) = Self.pairs[0]
            row.uuids[composed] = uuid
            let record = try Self.record(row, adapter)
            record["uuids"] = try Self.encodedMap([decomposed: uuid.uuidString])
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
            let decoded = try adapter.decodedComparisonObject(record, type: UnicodeStringTransportRow.self)
            XCTAssertNotEqual(try BigSyncRecordFingerprint.fields(of: decoded)["uuids"],
                              try BigSyncRecordFingerprint.fields(of: row)["uuids"])

            row.uuids[decomposed] = uuid
            XCTAssertEqual(row.uuids.count, 2)
            // A single incoming key must not conceal a second byte-distinct
            // member by collecting the Realm entries in a Swift String map.
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["uuids"])
        }
    }

    @BigSyncBackgroundActor
    func testCommittedUUIDMapHasCleanSynchronizationAudit() async throws {
        try await withFixture { adapter, _ in
            try await adapter.resetSyncCaches()
            await adapter.invalidateTokens()
            try await adapter.activateReplicaBinding(accountScopeIdentifier: "unicode-string-account",
                replicaBindingGenerationIdentifier: "unicode-string-binding")
            try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.unicode-uuid-map",
                databaseScope: .private)
            let row = UnicodeStringTransportRow()
            row.uuids["first"] = UUID(uuidString: "ABCDEF00-0000-0000-0000-000000000001")!
            row.uuids["second"] = UUID(uuidString: "ABCDEF00-0000-0000-0000-000000000002")!
            let record = try Self.record(row, adapter)
            _ = try await adapter.saveChanges(in: [record], forceSave: false)
            try await adapter.persistImportedChanges()
            try await adapter.didFinishImport()

            let audit = try await adapter.auditSynchronizationState(serverRecords: [record])
            XCTAssertEqual(audit.localObjectCount, 1)
            XCTAssertEqual(audit.acceptedBaselineCount, 1)
            XCTAssertEqual(audit.pendingMutationCount, 0)
            XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        }
    }
    private static let pairs: [(String, String)] = [
        ("\u{304C}", "\u{304B}\u{3099}"),
        ("\u{00E9}", "e\u{0301}"),
        ("\u{AC00}", "\u{1100}\u{1161}"),
        ("a\u{0323}\u{0301}", "a\u{0301}\u{0323}"),
    ]
    @BigSyncBackgroundActor
    private lazy var fixtureOwner = RealmAdapterFixtureOwner(testCase: self)

    @BigSyncBackgroundActor
    private func withFixture(
        _ body: @RealmBackgroundActor (RealmSwiftAdapter, Realm.Configuration) async throws -> Void
    ) async throws {
        let nonce = UUID().uuidString
        var configuration = Realm.Configuration()
        configuration.fileURL = nil
        configuration.inMemoryIdentifier = "unicode-string-target-" + nonce
        configuration.objectTypes = [UnicodeStringTransportRow.self, UnicodeLegacyMapTransportRow.self,
                                     BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &configuration)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [configuration], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "unicode-string-fixture",
                      replicaBindingGenerationIdentifier: "unicode-string-binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = nil
        tracking.inMemoryIdentifier = "unicode-string-tracking-" + nonce
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("unicode-string-assets-" + nonce)
        fixtureOwner.ownDirectory(directory)
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [configuration], excludedClassNames: [],
            recordZoneID: .init(zoneName: "unicode-string-" + nonce),
            logger: Logger(label: "UnicodeStringTransportTests"),
            startSetupTask: false, assetDirectoryURL: directory)
        fixtureOwner.own(adapter)
        // These tests exercise real codecs/comparison with task-owned rows.
        // The legacy writer case also opens the local adapter provider.
        // Managed fixtures explicitly open on RealmBackgroundActor. Some
        // scenarios call back into BigSyncBackgroundActor and must retain
        // actor-bound rows across that suspension rather than a thread Realm.
        try await body(adapter, configuration)
    }

    private static func bytes<S: Sequence>(_ values: S) -> [Data] where S.Element == String {
        values.map { Data($0.utf8) }
    }
    private static func record(_ row: UnicodeStringTransportRow, _ adapter: RealmSwiftAdapter) throws -> CKRecord {
        try BigSyncRecordPayload.record(from: row, recordID: .init(
            recordName: UnicodeStringTransportRow.className() + "." + row.id,
            zoneID: adapter.recordZoneID))
    }
    private static func applyTags(_ record: CKRecord, to row: UnicodeStringTransportRow,
                                  using adapter: RealmSwiftAdapter) throws {
        let property = try XCTUnwrap(row.objectSchema.properties.first { $0.name == "tags" })
        try adapter.applyChange(property: property, record: record, object: row,
                                syncedEntityIdentifier: record.recordID.recordName)
    }

    private static func encodedMap(_ entries: [String: String]) throws -> CKRecordValue {
        try PropertyListSerialization.data(fromPropertyList: entries, format: .binary, options: 0) as CKRecordValue
    }

    private static func encodedScalarMap(_ entries: [String: Any]) throws -> CKRecordValue {
        try PropertyListSerialization.data(fromPropertyList: entries, format: .binary, options: 0) as CKRecordValue
    }

    @BigSyncBackgroundActor
    func testScalarMapKeyByteReplacementIsVisibleToAuditAcrossValueTypes() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let (composed, decomposed) = Self.pairs[0]
            let date = Date(timeIntervalSinceReferenceDate: 42)
            let data = Data([0, 1, 255])
            let identifier = UUID(uuidString: "12345678-90AB-CDEF-1234-567890ABCDEF")!
            row.counts[composed] = 7
            row.flags[composed] = true
            row.floatWeights[composed] = 1.5
            row.doubleWeights[composed] = 2.5
            row.dates[composed] = date
            row.blobs[composed] = data
            row.identifiersByKey[composed] = identifier
            let values: [(String, Any)] = [
                ("counts", 7), ("flags", true), ("floatWeights", Float(1.5)),
                ("doubleWeights", 2.5), ("dates", date), ("blobs", data),
                ("identifiersByKey", identifier.uuidString),
            ]
            for (property, value) in values {
                let replay = try Self.record(row, adapter)
                XCTAssertFalse(adapter.serverDifferencePropertyNames(record: replay, object: row)
                    .contains(property), "Exact replay must agree for \(property)")
                replay[property] = try Self.encodedScalarMap([decomposed: value])
                XCTAssertTrue(adapter.serverDifferencePropertyNames(record: replay, object: row)
                    .contains(property), "The stored key bytes changed for \(property)")
            }
        }
    }

    @BigSyncBackgroundActor
    func testScalarMapComparisonRetainsEveryStoredKeyIdentity() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let (composed, decomposed) = Self.pairs[0]
            row.counts[composed] = 7
            let record = try Self.record(row, adapter)
            row.counts[decomposed] = 7
            XCTAssertEqual(row.counts.count, 2)
            XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row)
                .contains("counts"), "A Swift dictionary must not collapse the second stored key")
        }
    }

    @BigSyncBackgroundActor
    func testUUIDCollectionsCompareDecodedIdentityAndRejectMalformedValues() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let first = UUID(uuidString: "12345678-90AB-CDEF-1234-567890ABCDEF")!
            let second = UUID(uuidString: "FEDCBA09-8765-4321-FEDC-BA0987654321")!
            row.identifiers.append(objectsIn: [first, second])
            row.identifierSet.insert(objectsIn: [first, second])
            row.identifiersByKey["first"] = first
            let record = try Self.record(row, adapter)
            record["identifiers"] = [first.uuidString.lowercased(), second.uuidString.lowercased()] as CKRecordValue
            record["identifierSet"] = [second.uuidString.lowercased(), first.uuidString.lowercased()] as CKRecordValue
            record["identifiersByKey"] = try Self.encodedScalarMap(["first": first.uuidString.lowercased()])
            XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row).isEmpty,
                          "The transport decoder accepts both UUID spellings as the same stored UUID")

            record["identifiers"] = [second.uuidString, first.uuidString] as CKRecordValue
            XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row).contains("identifiers"))
            record["identifiers"] = [first.uuidString, second.uuidString, "not-a-uuid"] as CKRecordValue
            record["identifierSet"] = [first.uuidString, second.uuidString, "not-a-uuid"] as CKRecordValue
            record["identifiersByKey"] = try Self.encodedScalarMap(["first": "not-a-uuid"])
            let malformed = Set(adapter.serverDifferencePropertyNames(record: record, object: row))
            XCTAssertTrue(Set(["identifiers", "identifierSet", "identifiersByKey"]).isSubset(of: malformed),
                          "Invalid members cannot disappear through compact UUID decoding")
        }
    }

    @BigSyncBackgroundActor
    func testCanonicalEquivalentMapKeysHaveDeterministicFingerprintOrder() async throws {
        try await withFixture { _, _ in
            for (composed, decomposed) in Self.pairs {
                let forward = [composed, decomposed].sorted(by: BigSyncStringIdentity.mapKeyPrecedes)
                let reversed = [decomposed, composed].sorted(by: BigSyncStringIdentity.mapKeyPrecedes)
                XCTAssertEqual(Self.bytes(forward), Self.bytes(reversed))
                XCTAssertEqual(Self.bytes(forward), [Data(composed.utf8), Data(decomposed.utf8)]
                    .sorted { $0.lexicographicallyPrecedes($1) })
                let first = UnicodeStringTransportRow()
                let second = UnicodeStringTransportRow()
                first.counts[composed] = 1
                first.counts[decomposed] = 2
                second.counts[decomposed] = 2
                second.counts[composed] = 1
                XCTAssertEqual(first.counts.count, 2)
                XCTAssertEqual(second.counts.count, 2)
                XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: first)["counts"],
                               try BigSyncRecordFingerprint.fields(of: second)["counts"])
            }
        }
    }

    func testMapKeyOrderingPreservesExistingNonEquivalentOrder() {
        let keys = ["a", "z", "\u{00E9}", "e", "\u{03B1}", "\u{3042}"]
        XCTAssertEqual(Self.bytes(keys.sorted(by: BigSyncStringIdentity.mapKeyPrecedes)),
                       Self.bytes(keys.sorted()))
    }

    @BigSyncBackgroundActor
    private static func legacyMapRecord(_ adapter: RealmSwiftAdapter) throws -> CKRecord? {
        adapter.realmProvider?.targetReaderRealmPerSchemaName[UnicodeLegacyMapTransportRow.className()]?.refresh()
        let entity = SyncedEntity()
        entity.identifier = UnicodeLegacyMapTransportRow.className() + ".legacy-map"
        entity.entityType = UnicodeLegacyMapTransportRow.className()
        entity.entityState = .new
        return try adapter.recordToUpload(syncedEntity: entity, isDummyRecord: false)
    }

    @BigSyncBackgroundActor
    func testAmbiguousLegacyMapUploadRejectsWithoutJournalMutation() async throws {
        try await withFixture { adapter, configuration in
            try await adapter.ensureSetup()
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeLegacyMapTransportRow()
            let (a, b) = Self.pairs[0]
            row.translations[a] = "first"
            row.translations[b] = "second"
            XCTAssertEqual(row.translations.count, 2)
            try realm.write {
                realm.add(row)
                row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
            }
            let name = row.objectSchema.className + "." + row.id
            let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                       forPrimaryKey: name)?.generation)
            do {
                _ = try await Self.legacyMapRecord(adapter)
                XCTFail("Ambiguous legacy upload must fail before serialization")
            } catch {
                XCTAssertTrue(error is RealmSwiftRemoteRecordDecodingError)
            }
            XCTAssertEqual(row.translations.count, 2)
            XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                       forPrimaryKey: name)?.generation, generation)
            try realm.write {
                row.translations.removeAll()
                row.translations["word"] = b
                row.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
            }
            let upload = try await Self.legacyMapRecord(adapter)
            let ordinary = try XCTUnwrap(upload)
            let payload = try XCTUnwrap(ordinary["translations"] as? Data)
            let decoded = try XCTUnwrap(try PropertyListSerialization.propertyList(
                from: payload, options: [], format: nil) as? [String: String])
            XCTAssertEqual(Data(try XCTUnwrap(decoded["word"]).utf8), Data(b.utf8))
        }
    }

    @BigSyncBackgroundActor
    func testMapValueByteChangeIsVisibleToAuditAndFingerprint() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow()
                row.translations["word"] = a
                let record = try Self.record(row, adapter)
                record["translations"] = try Self.encodedMap(["word": b])
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["translations"])
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
                let compared = try XCTUnwrap(adapter.decodedComparisonObject(
                    record, type: UnicodeStringTransportRow.self) as? UnicodeStringTransportRow)
                XCTAssertNotEqual(try BigSyncRecordFingerprint.fields(of: row)["translations"],
                                  try BigSyncRecordFingerprint.fields(of: compared)["translations"])
            }
        }
    }

    @BigSyncBackgroundActor
    func testMapKeyByteReplacementIsVisibleToAudit() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow()
                row.translations[a] = "unchanged"
                let record = try Self.record(row, adapter)
                record["translations"] = try Self.encodedMap([b: "unchanged"])
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["translations"])
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testMapExactReplayAndEntryPermutationRemainNoOps() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            row.translations["second"] = Self.pairs[0].1
            row.translations["first"] = Self.pairs[0].0
            let record = try Self.record(row, adapter)
            record["translations"] = try Self.encodedMap([
                "first": Self.pairs[0].0, "second": Self.pairs[0].1
            ])
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row).isEmpty)
            record["translations"] = try Self.encodedMap(["first": Self.pairs[0].0])
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["translations"])
        }
    }

    @BigSyncBackgroundActor
    func testMapComparisonKeepsEquivalentByteDistinctStoredKeys() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let (a, b) = Self.pairs[0]
            let record = try Self.record(row, adapter)
            row.translations[a] = "same value"
            row.translations[b] = "same value"
            XCTAssertEqual(row.translations.count, 2, "Realm must retain both stored key identities")
            // Construct the incoming single-member map directly: the upload
            // serializer's String-keyed dictionary is a separate boundary.
            for key in [a, b] {
                record["translations"] = try Self.encodedMap([key: "same value"])
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["translations"])
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testAmbiguousOutgoingMapRejectsBeforeTemplateOrJournalMutation() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow()
            let (a, b) = Self.pairs[0]
            row.translations[a] = "first"
            row.translations[b] = "second"
            XCTAssertEqual(row.translations.count, 2)
            try realm.write {
                realm.add(row)
                row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
            }
            let name = row.objectSchema.className + "." + row.id
            let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                       forPrimaryKey: name)?.generation)
            let before = try BigSyncRecordFingerprint.fields(of: row)
            let template = CKRecord(recordType: row.objectSchema.className,
                                    recordID: .init(recordName: name, zoneID: adapter.recordZoneID))
            template["scalar"] = "original template" as CKRecordValue
            template["translations"] = try Self.encodedMap(["original": "template"])
            let originalMap = template["translations"] as? Data
            XCTAssertThrowsError(try BigSyncRecordPayload.record(
                from: row, recordID: template.recordID, template: template)) { error in
                guard let failure = error as? BigSyncRecordRebaseError,
                      case .unsupportedField("translations") = failure else {
                    return XCTFail("Expected unsupported map, got \(error)")
                }
            }
            XCTAssertEqual(template["scalar"] as? String, "original template")
            XCTAssertEqual(template["translations"] as? Data, originalMap)
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), before)
            XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                       forPrimaryKey: name)?.generation, generation)
        }
    }

    @BigSyncBackgroundActor
    func testAmbiguousIncomingPropertyListRejectsBeforeRealmOrJournalMutation() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow()
            row.translations["retained"] = "original"
            try realm.write {
                realm.add(row)
                row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
            }
            let record = try Self.record(row, adapter)
            let name = record.recordID.recordName
            let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                       forPrimaryKey: name)?.generation)
            let before = try BigSyncRecordFingerprint.fields(of: row)
            // Start with original XML bytes, bypassing Swift Dictionary's
            // canonical-key collapse during fixture construction.
            let xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
            <plist version="1.0"><dict>
            <key>が</key><string>first</string>
            <key>か\u{3099}</key><string>second</string>
            </dict></plist>
            """
            let data = Data(xml.utf8)
            let raw = try XCTUnwrap(try PropertyListSerialization.propertyList(
                from: data, options: [], format: nil) as? NSDictionary)
            XCTAssertEqual(raw.count, 2, "Native plist decoding must retain raw keys until explicit admission")
            for payload in [data, try PropertyListSerialization.data(fromPropertyList: raw, format: .binary, options: 0)] {
                record["translations"] = payload as CKRecordValue
                let property = try XCTUnwrap(row.objectSchema.properties.first { $0.name == "translations" })
                XCTAssertThrowsError(try realm.write {
                    try adapter.applyChange(property: property, record: record, object: row,
                                            syncedEntityIdentifier: name)
                }) { error in
                    XCTAssertTrue(error is RealmSwiftRemoteRecordDecodingError)
                }
                XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), before)
                XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                           forPrimaryKey: name)?.generation, generation)
                XCTAssertFalse(realm.isInWriteTransaction)
            }
        }
    }

    @BigSyncBackgroundActor
    func testManagedIncomingMapReplacementPreservesExactValuesWithoutJournaling() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow()
            row.translations["stale"] = "retained until replacement"
            try realm.write { realm.add(row) }
            let record = try Self.record(row, adapter)
            let (a, b) = Self.pairs[0]
            record["translations"] = try Self.encodedMap(["first": a, "second": b])
            let property = try XCTUnwrap(row.objectSchema.properties.first { $0.name == "translations" })
            try realm.write {
                try adapter.applyChange(property: property, record: record, object: row,
                                        syncedEntityIdentifier: record.recordID.recordName)
            }
            XCTAssertEqual(row.translations.count, 2)
            XCTAssertNil(row.translations["stale"])
            XCTAssertEqual(Data(try XCTUnwrap(row.translations["first"]).utf8), Data(a.utf8))
            XCTAssertEqual(Data(try XCTUnwrap(row.translations["second"]).utf8), Data(b.utf8))
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testScalarByteChangeIsVisibleToAudit() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow(); row.scalar = a
                let record = try Self.record(row, adapter); record["scalar"] = b as CKRecordValue
                XCTAssertEqual(a, b, "The fixture must exercise Swift canonical equivalence")
                XCTAssertNotEqual(Data(a.utf8), Data(b.utf8))
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["scalar"])
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testListReplacementAndOrderUseByteIdentity() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow(); row.names.append(objectsIn: [a, b])
                let record = try Self.record(row, adapter); record["names"] = [b, a] as CKRecordValue
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["names"])
                record["names"] = [a, a] as CKRecordValue
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testSetMembershipUsesByteIdentityNotSwiftEquivalence() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow(); row.tags.insert(a); row.tags.insert(b)
                XCTAssertEqual(row.tags.count, 2, "The real Realm fixture must retain both identities")
                let record = try Self.record(row, adapter); record["tags"] = [a] as CKRecordValue
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["tags"])
                row.tags.removeAll(); row.tags.insert(a); record["tags"] = [b] as CKRecordValue
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testExactReplayAndSetPermutationRemainNoOps() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow(); row.scalar = a
                row.names.append(objectsIn: [a, b, a]); row.tags.insert(a); row.tags.insert(b)
                let record = try Self.record(row, adapter)
                record["tags"] = [b, a, b] as CKRecordValue
                XCTAssertFalse(adapter.hasChanges(record: record, object: row))
                XCTAssertTrue(adapter.serverDifferencePropertyNames(record: record, object: row).isEmpty)
            }
        }
    }

    @BigSyncBackgroundActor
    func testIncomingSetKeepsEquivalentByteDistinctMembersUnmanaged() async throws {
        try await withFixture { adapter, _ in
            for (a, b) in Self.pairs {
                let row = UnicodeStringTransportRow()
                let record = try Self.record(row, adapter); record["tags"] = [a, b] as CKRecordValue
                try Self.applyTags(record, to: row, using: adapter)
                XCTAssertEqual(row.tags.count, 2)
                XCTAssertEqual(Set(Self.bytes(row.tags)), Set(Self.bytes([a, b])))
            }
        }
    }

    @BigSyncBackgroundActor
    func testIncomingSetKeepsEquivalentByteDistinctMembersManaged() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); try realm.write { realm.add(row) }
            for (a, b) in Self.pairs {
                let record = try Self.record(row, adapter); record["tags"] = [b, a] as CKRecordValue
                try realm.write { try Self.applyTags(record, to: row, using: adapter) }
                XCTAssertEqual(row.tags.count, 2)
                XCTAssertEqual(Set(Self.bytes(row.tags)), Set(Self.bytes([a, b])))
                XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            }
        }
    }

    @BigSyncBackgroundActor
    func testIncomingSetAssignmentReplacesInsteadOfUnion() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); row.tags.insert("stale")
            try realm.write { realm.add(row) }
            let (a, b) = Self.pairs[0]
            let record = try Self.record(row, adapter); record["tags"] = [a, b] as CKRecordValue
            try realm.write { try Self.applyTags(record, to: row, using: adapter) }
            XCTAssertEqual(Set(Self.bytes(row.tags)), Set(Self.bytes([a, b])))
            XCTAssertFalse(row.tags.contains("stale"))
        }
    }

    @BigSyncBackgroundActor
    func testExactDuplicateDeduplicatesButDistinctSpellingsDoNot() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); try realm.write { realm.add(row) }
            let (a, b) = Self.pairs[1]
            let record = try Self.record(row, adapter); record["tags"] = [a, b, a, b] as CKRecordValue
            try realm.write { try Self.applyTags(record, to: row, using: adapter) }
            XCTAssertEqual(row.tags.count, 2)
            XCTAssertEqual(Set(Self.bytes(row.tags)), Set(Self.bytes([a, b])))
        }
    }

    @BigSyncBackgroundActor
    func testAbsentSetFieldClearsManagedCollection() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); row.tags.insert("stale")
            try realm.write { realm.add(row) }
            let record = try Self.record(row, adapter); record["tags"] = nil
            try realm.write { try Self.applyTags(record, to: row, using: adapter) }
            XCTAssertTrue(row.tags.isEmpty)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testMalformedSetRejectsWithoutPartialMutation() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); row.tags.insert("retained")
            try realm.write { realm.add(row) }
            let record = try Self.record(row, adapter); record["tags"] = [1, 2] as CKRecordValue
            XCTAssertThrowsError(try realm.write { try Self.applyTags(record, to: row, using: adapter) }) { error in
                XCTAssertTrue(error is RealmSwiftRemoteRecordDecodingError)
            }
            XCTAssertEqual(Self.bytes(row.tags), Self.bytes(["retained"]))
            XCTAssertFalse(realm.isInWriteTransaction)
        }
    }

    @BigSyncBackgroundActor
    func testCancelledSetDecodeDoesNotMutateTarget() async throws {
        try await withFixture { adapter, _ in
            let task = Task { @RealmBackgroundActor in
                let row = UnicodeStringTransportRow(); row.tags.insert("retained")
                let record = try Self.record(row, adapter); record["tags"] = ["replacement"] as CKRecordValue
                withUnsafeCurrentTask { $0?.cancel() }
                XCTAssertThrowsError(try Self.applyTags(record, to: row, using: adapter)) { error in
                    XCTAssertTrue(error is CancellationError)
                }
                XCTAssertEqual(Self.bytes(row.tags), Self.bytes(["retained"]))
            }
            try await task.value
        }
    }

    @BigSyncBackgroundActor
    func testComparisonDecoderAndIncomingApplyShareExactSetMembers() async throws {
        try await withFixture { adapter, _ in
            try await Task { @BigSyncBackgroundActor in
                let row = UnicodeStringTransportRow(); let (a, b) = Self.pairs[0]
                let record = try Self.record(row, adapter); record["tags"] = [a, b, a] as CKRecordValue
                let pending = try adapter.applyChanges(in: record, to: row,
                    syncedEntityID: record.recordID.recordName, syncedEntityState: .synced,
                    entityType: record.recordType, isNewlyCreatedReceiver: true)
                let compared = try XCTUnwrap(adapter.decodedComparisonObject(record, type: UnicodeStringTransportRow.self) as? UnicodeStringTransportRow)
                XCTAssertTrue(pending.isEmpty)
                XCTAssertEqual(row.tags.count, 2); XCTAssertEqual(compared.tags.count, 2)
                XCTAssertEqual(Set(Self.bytes(row.tags)), Set(Self.bytes([a, b])))
                XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), try BigSyncRecordFingerprint.fields(of: compared))
            }.value
        }
    }

    @BigSyncBackgroundActor
    func testAdoptedPayloadRoundTripPreservesAllStringIdentities() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow(); let (a, b) = Self.pairs[2]
            row.scalar = b; row.names.append(objectsIn: [a, b, a])
            row.tags.insert(a); row.tags.insert(b)
            let record = try Self.record(row, adapter)
            let restored = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(record))
            let decoded = try XCTUnwrap(adapter.decodedComparisonObject(restored, type: UnicodeStringTransportRow.self) as? UnicodeStringTransportRow)
            XCTAssertEqual(Data(decoded.scalar.utf8), Data(b.utf8))
            XCTAssertEqual(Self.bytes(decoded.names), Self.bytes([a, b, a]))
            XCTAssertEqual(Set(Self.bytes(decoded.tags)), Set(Self.bytes([a, b])))
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: decoded), try BigSyncRecordFingerprint.fields(of: row))
        }
    }

    @BigSyncBackgroundActor
    func testManagedRollbackRetainsOriginalSetAndJournalGeneration() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); row.tags.insert("original")
            try realm.write {
                realm.add(row)
                row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
            }
            let record = try Self.record(row, adapter); let (a, b) = Self.pairs[0]
            let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: record.recordID.recordName)?.generation)
            record["tags"] = [a, b] as CKRecordValue
            realm.beginWrite()
            defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
            try Self.applyTags(record, to: row, using: adapter)
            XCTAssertEqual(row.tags.count, 2)
            realm.cancelWrite()
            XCTAssertEqual(Self.bytes(row.tags), Self.bytes(["original"]))
            XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: record.recordID.recordName)?.generation, generation)
        }
    }

    @BigSyncBackgroundActor
    func testUnchangedNonStringFieldsRetainComparisonBehavior() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow(); let record = try Self.record(row, adapter)
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            record["number"] = 8 as CKRecordValue
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["number"])
            record["number"] = 7 as CKRecordValue; record["enabled"] = false as CKRecordValue
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["enabled"])
        }
    }

    @BigSyncBackgroundActor
    func testRecordIdentityConstructionRemainsUnchanged() async throws {
        try await withFixture { _, _ in
            let row = UnicodeStringTransportRow(); row.id = "stable-ascii-id"
            XCTAssertEqual(BigSyncRecordIdentity.recordName(for: row), UnicodeStringTransportRow.className() + ".stable-ascii-id")
            row.id = "\u{304C}"
            XCTAssertNil(BigSyncRecordIdentity.recordName(for: row), "CloudKit record names still have the existing ASCII restriction")
        }
    }

    @BigSyncBackgroundActor
    func testExistingFieldFingerprintsRemainUnchangedByReplay() async throws {
        try await withFixture { adapter, configuration in
            let realm = try await Realm(configuration: configuration, actor: RealmBackgroundActor.shared)
            let row = UnicodeStringTransportRow(); let (a, b) = Self.pairs[0]
            row.tags.insert(a); row.tags.insert(b); try realm.write { realm.add(row) }
            let before = try BigSyncRecordFingerprint.fields(of: row)
            let record = try Self.record(row, adapter); record["tags"] = [b, a] as CKRecordValue
            try realm.write { try Self.applyTags(record, to: row, using: adapter) }
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), before)
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
        }
    }

    @BigSyncBackgroundActor
    func testURLListUsesExistingEncodedAbsoluteStrings() async throws {
        try await withFixture { adapter, _ in
            let row = UnicodeStringTransportRow()
            let first = try XCTUnwrap(URL(string: "https://example.test/a"))
            let second = try XCTUnwrap(URL(string: "https://example.test/b"))
            row.urls.append(objectsIn: [first, second])
            let record = try Self.record(row, adapter)
            XCTAssertFalse(adapter.hasChanges(record: record, object: row))
            record["urls"] = [second.absoluteString, first.absoluteString] as CKRecordValue
            XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["urls"])
        }
    }
}
