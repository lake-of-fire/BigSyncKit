import CloudKit
import CryptoKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(BigSyncMapUUIDTransportIdentityFixture)
private final class MapUUIDTransportRow: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    static let bigSyncRecordContract = BigSyncRecordContract(policy: .atomicRecord)
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var integers: Map<String, Int>
    @Persisted var booleans: Map<String, Bool>
    @Persisted var floats: Map<String, Float>
    @Persisted var doubles: Map<String, Double>
    @Persisted var dates: Map<String, Date>
    @Persisted var bytes: Map<String, Data>
    @Persisted var strings: Map<String, String>
    @Persisted var uuidMap: Map<String, UUID>
    @Persisted var uuidList: List<UUID>
    @Persisted var uuidSet: MutableSet<UUID>
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1000)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1000)
    @Persisted var explicitlyModifiedAt: Date? = Date(timeIntervalSinceReferenceDate: 1000)
    @Persisted var isDeleted = false
}

/// Actual native adapter/Realm companions, separate from the portable
/// value-input suite. No real CloudKit requests or application stores.
final class MapUUIDTransportIdentityTests: XCTestCase {
    @BigSyncBackgroundActor
    private lazy var fixtureOwner = RealmAdapterFixtureOwner(testCase: self)
    private static let a = "\u{304C}", b = "\u{304B}\u{3099}"
    private static let u = UUID(uuidString: "AABBCCDD-0123-4567-89AB-CDEF01234567")!
    private static let v = UUID(uuidString: "AABBCCDD-0123-4567-89AB-CDEF01234568")!

    @BigSyncBackgroundActor
    private func fixture() throws -> (adapter: RealmSwiftAdapter, realm: Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.fileURL = nil
        target.inMemoryIdentifier = "map-uuid-target-" + nonce
        target.objectTypes = [MapUUIDTransportRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "map-uuid-fixture",
                      replicaBindingGenerationIdentifier: "map-uuid-binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = nil
        tracking.inMemoryIdentifier = "map-uuid-tracking-" + nonce
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("map-uuid-assets-" + nonce)
        fixtureOwner.ownDirectory(directory)
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: [],
            recordZoneID: .init(zoneName: "map-uuid-" + nonce),
            logger: Logger(label: "MapUUIDTransportIdentityTests"),
            startSetupTask: false, assetDirectoryURL: directory)
        fixtureOwner.own(adapter)
        // No adapter setup task: each test opens and uses its Realm on the
        // owning actor, synchronously, without suspending while it holds rows.
        return (adapter, try Realm(configuration: target))
    }
    private static func record(_ row: MapUUIDTransportRow, _ adapter: RealmSwiftAdapter) throws -> CKRecord {
        try BigSyncRecordPayload.record(from: row, recordID: .init(
            recordName: MapUUIDTransportRow.className() + "." + row.id,
            zoneID: adapter.recordZoneID))
    }
    private static func encodedMap(_ value: [String: Any]) throws -> Data {
        try PropertyListSerialization.data(fromPropertyList: value, format: .binary, options: 0)
    }
    private static func apply(_ name: String, _ record: CKRecord,
                              _ row: MapUUIDTransportRow, _ adapter: RealmSwiftAdapter) throws {
        let property = try XCTUnwrap(row.objectSchema.properties.first { $0.name == name })
        try adapter.applyChange(property: property, record: record, object: row,
                                syncedEntityIdentifier: record.recordID.recordName)
    }
    private static var values: [(String, Any)] {
        [("integers", 7), ("booleans", true), ("floats", Float(1.25)),
         ("doubles", 1.25), ("dates", Date(timeIntervalSinceReferenceDate: 1000.25)),
         ("bytes", Data([0, 255])), ("strings", "value"), ("uuidMap", u)]
    }
    private static func populate(_ row: MapUUIDTransportRow) {
        for (name, value) in values { row.setValue([a: value], forKey: name) }
        row.uuidList.append(objectsIn: [u, v]); row.uuidSet.insert(u); row.uuidSet.insert(v)
    }

    @BigSyncBackgroundActor
    func testAllPrimitiveMapKeyRenamesAreByteExact() async throws {
        let f = try fixture()
        for (name, value) in Self.values {
            for (a, b) in [(Self.a, Self.b), (Self.b, Self.a)] {
                let row = MapUUIDTransportRow(); row.setValue([a: value], forKey: name)
                let record = try Self.record(row, f.adapter)
                row.setValue([b: value], forKey: name)
                XCTAssertEqual(f.adapter.serverDifferencePropertyNames(record: record, object: row), [name])
            }
        }
    }
    @BigSyncBackgroundActor
    func testExactAllPrimitiveMapRoundTripIsNoOp() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); Self.populate(row)
        let record = try Self.record(row, f.adapter)
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
        XCTAssertTrue(f.adapter.serverDifferencePropertyNames(record: record, object: row).isEmpty)
    }
    @BigSyncBackgroundActor
    func testManagedKeyRenameIsNotHiddenBySwiftDictionaryEquality() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.integers[Self.a] = 7
        try f.realm.write { f.realm.add(row) }
        let record = try Self.record(row, f.adapter)
        try f.realm.write { row.integers.removeAll(); row.integers[Self.b] = 7 }
        XCTAssertEqual(f.adapter.serverDifferencePropertyNames(record: record, object: row), ["integers"])
        XCTAssertTrue(f.realm.objects(BigSyncPendingMutation.self).isEmpty)
    }
    @BigSyncBackgroundActor
    func testUUIDMapUsesExistingPropertyListStringEncoding() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidMap[Self.a] = Self.u
        let record = try Self.record(row, f.adapter)
        let data = try XCTUnwrap(record["uuidMap"] as? Data)
        let wire = try XCTUnwrap(PropertyListSerialization.propertyList(from: data, options: [], format: nil) as? [String: String])
        XCTAssertEqual(wire[Self.a], Self.u.uuidString)
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
    }
    @BigSyncBackgroundActor
    func testUUIDMapLowercaseWireRoundTripMatchesDecodedIdentity() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidMap[Self.a] = Self.u
        let record = try Self.record(row, f.adapter)
        record["uuidMap"] = try Self.encodedMap([Self.a: Self.u.uuidString.lowercased()]) as CKRecordValue
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
        let receiver = MapUUIDTransportRow()
        try Self.apply("uuidMap", record, receiver, f.adapter)
        XCTAssertEqual(receiver.uuidMap[Self.a], Self.u)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: receiver), try BigSyncRecordFingerprint.fields(of: row))
    }
    @BigSyncBackgroundActor
    func testMalformedUUIDMapStillRejectsWithoutCommittingPartialChange() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidMap[Self.a] = Self.u
        try f.realm.write { f.realm.add(row) }
        let before = try BigSyncRecordFingerprint.fields(of: row)
        let record = try Self.record(row, f.adapter)
        record["uuidMap"] = try Self.encodedMap([Self.a: "invalid"]) as CKRecordValue
        XCTAssertTrue(f.adapter.hasChanges(record: record, object: row))
        XCTAssertThrowsError(try f.realm.write { try Self.apply("uuidMap", record, row, f.adapter) }) { error in
            XCTAssertTrue(error is RealmSwiftRemoteRecordDecodingError)
        }
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), before)
        XCTAssertFalse(f.realm.isInWriteTransaction)
    }
    @BigSyncBackgroundActor
    func testUUIDListCaseVariationIsNoOpButReorderingIsNot() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidList.append(objectsIn: [Self.u, Self.v])
        let record = try Self.record(row, f.adapter)
        record["uuidList"] = [Self.u.uuidString.lowercased(), Self.v.uuidString] as CKRecordValue
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
        record["uuidList"] = [Self.v.uuidString.lowercased(), Self.u.uuidString] as CKRecordValue
        XCTAssertEqual(f.adapter.serverDifferencePropertyNames(record: record, object: row), ["uuidList"])
    }
    @BigSyncBackgroundActor
    func testUUIDSetCasePermutationAndDuplicateSpellingsAreNoOp() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidSet.insert(Self.u); row.uuidSet.insert(Self.v)
        let record = try Self.record(row, f.adapter)
        record["uuidSet"] = [Self.v.uuidString.lowercased(), Self.u.uuidString, Self.u.uuidString.lowercased()] as CKRecordValue
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
        let receiver = MapUUIDTransportRow(); try Self.apply("uuidSet", record, receiver, f.adapter)
        XCTAssertEqual(Set(receiver.uuidSet), [Self.u, Self.v])
    }
    @BigSyncBackgroundActor
    func testMalformedUUIDCollectionsNeverMatchStoredValues() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.uuidSet.insert(Self.u); row.uuidList.append(Self.u)
        for name in ["uuidSet", "uuidList"] {
            let record = try Self.record(row, f.adapter)
            record[name] = ["invalid"] as CKRecordValue
            XCTAssertEqual(f.adapter.serverDifferencePropertyNames(record: record, object: row), [name])
            XCTAssertThrowsError(try Self.apply(name, record, row, f.adapter))
            XCTAssertEqual(Array(row.uuidList), [Self.u]); XCTAssertEqual(Set(row.uuidSet), [Self.u])
        }
    }
    @BigSyncBackgroundActor
    func testEmptyAndAbsentUUIDCollectionsRetainNoOpAndClearSemantics() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow()
        let record = try Self.record(row, f.adapter)
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
        row.uuidMap[Self.a] = Self.u; row.uuidList.append(Self.u); row.uuidSet.insert(Self.u)
        for name in ["uuidMap", "uuidList", "uuidSet"] { try Self.apply(name, record, row, f.adapter) }
        XCTAssertTrue(row.uuidMap.isEmpty); XCTAssertTrue(row.uuidList.isEmpty); XCTAssertTrue(row.uuidSet.isEmpty)
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
    }
    @BigSyncBackgroundActor
    func testArchiveRoundTripAndComparisonAgreeForUUIDCollections() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); Self.populate(row)
        let record = try Self.record(row, f.adapter)
        let restored = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(record))
        XCTAssertFalse(f.adapter.hasChanges(record: restored, object: row))
        for name in ["uuidMap", "uuidList", "uuidSet"] {
            let receiver = MapUUIDTransportRow(); try Self.apply(name, restored, receiver, f.adapter)
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: receiver)[name], try BigSyncRecordFingerprint.fields(of: row)[name])
        }
    }
    @BigSyncBackgroundActor
    func testComparisonDoesNotConsumeJournalOrAdvanceBaseline() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); Self.populate(row)
        try f.realm.write { f.realm.add(row); row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt) }
        let record = try Self.record(row, f.adapter)
        let generation = try XCTUnwrap(f.realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: record.recordID.recordName)?.generation)
        for _ in 0..<3 { XCTAssertFalse(f.adapter.hasChanges(record: record, object: row)) }
        XCTAssertEqual(f.realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: record.recordID.recordName)?.generation, generation)
        XCTAssertTrue(f.realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }
    @BigSyncBackgroundActor
    func testByteDistinctMapKeysHaveDeterministicFingerprintWithoutTransportAdmission() async throws {
        let f = try fixture(); let one = MapUUIDTransportRow(); let two = MapUUIDTransportRow(); two.id = "second"
        one.integers[Self.a] = 1; one.integers[Self.b] = 2
        two.integers[Self.b] = 2; two.integers[Self.a] = 1
        XCTAssertEqual(one.integers.count, 2); XCTAssertEqual(two.integers.count, 2)
        let expected = try BigSyncRecordFingerprint.fields(of: one)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: two), expected)
        try f.realm.write { f.realm.add(one); f.realm.add(two) }
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: one), expected)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: two), expected)
        XCTAssertFalse(BigSyncStringIdentity.realmMapKeysAreUnambiguous(one["integers"]))
        XCTAssertThrowsError(try Self.record(one, f.adapter))
        XCTAssertThrowsError(try Self.record(two, f.adapter))
    }
    @BigSyncBackgroundActor
    func testUnambiguousMapFingerprintRetainsHistoricalGoldenBytes() async throws {
        let f = try fixture(); let row = MapUUIDTransportRow(); row.integers["b"] = 2; row.integers["a"] = 1
        func frame(_ parts: [Data]) -> Data {
            var hash = SHA256()
            for part in parts {
                var length = UInt64(part.count).bigEndian
                withUnsafeBytes(of: &length) { hash.update(data: Data($0)) }
                hash.update(data: part)
            }
            return Data(hash.finalize())
        }
        let expected = frame([Data("a".utf8), frame([Data([1]), Data("1".utf8)]),
                              Data("b".utf8), frame([Data([1]), Data("2".utf8)])])
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row)["integers"], expected)
        let record = try Self.record(row, f.adapter)
        XCTAssertFalse(f.adapter.hasChanges(record: record, object: row))
    }
}
