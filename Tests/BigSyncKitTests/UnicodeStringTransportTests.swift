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
    @Persisted var number = 7
    @Persisted var enabled = true
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date? = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var isDeleted = false
}

final class UnicodeStringTransportTests: XCTestCase {
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
        configuration.objectTypes = [UnicodeStringTransportRow.self, BigSyncPendingMutation.self]
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
        // These tests exercise the real decoders/comparer with task-owned
        // Realm rows. No transport setup, CloudKit request or reset is needed.
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
            row.translations[a] = "same value"
            row.translations[b] = "same value"
            XCTAssertEqual(row.translations.count, 2, "Realm must retain both stored key identities")
            // Construct the incoming single-member map directly: the upload
            // serializer's String-keyed dictionary is a separate boundary.
            let record = try Self.record(row, adapter)
            for key in [a, b] {
                record["translations"] = try Self.encodedMap([key: "same value"])
                XCTAssertEqual(adapter.serverDifferencePropertyNames(record: record, object: row), ["translations"])
                XCTAssertTrue(adapter.hasChanges(record: record, object: row))
            }
        }
    }

    @BigSyncBackgroundActor
    func testManagedIncomingMapReplacementPreservesExactValuesWithoutJournaling() async throws {
        try await withFixture { adapter, configuration in
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
            let realm = try Realm(configuration: configuration)
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
