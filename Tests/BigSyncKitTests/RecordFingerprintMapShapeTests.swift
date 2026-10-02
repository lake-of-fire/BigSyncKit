import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private enum FingerprintMapChoice: Int, PersistableEnum {
    case first = 1
    case second = 2
}

@objc(BigSyncRecordFingerprintMapShapeFixture)
private final class FingerprintMapShapeObject: Object {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }

    @Persisted(primaryKey: true) var id = "record"
    @Persisted var ints: Map<String, Int>
    @Persisted var int8s: Map<String, Int8>
    @Persisted var int16s: Map<String, Int16>
    @Persisted var int32s: Map<String, Int32>
    @Persisted var int64s: Map<String, Int64>
    @Persisted var bools: Map<String, Bool>
    @Persisted var floats: Map<String, Float>
    @Persisted var doubles: Map<String, Double>
    @Persisted var strings: Map<String, String>
    @Persisted var dates: Map<String, Date>
    @Persisted var data: Map<String, Data>
    @Persisted var uuids: Map<String, UUID>
    @Persisted var optionalInts: Map<String, Int?>
    @Persisted var optionalInt8s: Map<String, Int8?>
    @Persisted var optionalInt16s: Map<String, Int16?>
    @Persisted var optionalInt32s: Map<String, Int32?>
    @Persisted var optionalInt64s: Map<String, Int64?>
    @Persisted var optionalBools: Map<String, Bool?>
    @Persisted var optionalFloats: Map<String, Float?>
    @Persisted var optionalDoubles: Map<String, Double?>
    @Persisted var optionalStrings: Map<String, String?>
    @Persisted var optionalDates: Map<String, Date?>
    @Persisted var optionalData: Map<String, Data?>
    @Persisted var optionalUUIDs: Map<String, UUID?>
    @Persisted var choices: Map<String, FingerprintMapChoice>
    @Persisted var optionalChoices: Map<String, FingerprintMapChoice?>
}

final class RecordFingerprintMapShapeTests: XCTestCase {
    private func realm() throws -> Realm {
        try Realm(configuration: .init(
            inMemoryIdentifier: UUID().uuidString,
            objectTypes: [FingerprintMapShapeObject.self, BigSyncRecordBaseline.self]
        ))
    }

    private func populate(_ row: FingerprintMapShapeObject) {
        let date = Date(timeIntervalSinceReferenceDate: 1_000.25)
        let uuid = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        row.ints["value"] = 1
        row.int8s["value"] = 1
        row.int16s["value"] = 1
        row.int32s["value"] = 1
        row.int64s["value"] = 1
        row.bools["value"] = true
        row.floats["value"] = 1.25
        row.doubles["value"] = 1.25
        row.strings["value"] = "text"
        row.dates["value"] = date
        row.data["value"] = Data([0, 1, 2])
        row.uuids["value"] = uuid
        row.optionalInts.updateValue(1, forKey: "value")
        row.optionalInt8s.updateValue(1, forKey: "value")
        row.optionalInt16s.updateValue(1, forKey: "value")
        row.optionalInt32s.updateValue(1, forKey: "value")
        row.optionalInt64s.updateValue(1, forKey: "value")
        row.optionalBools.updateValue(true, forKey: "value")
        row.optionalFloats.updateValue(1.25, forKey: "value")
        row.optionalDoubles.updateValue(1.25, forKey: "value")
        row.optionalStrings.updateValue("text", forKey: "value")
        row.optionalDates.updateValue(date, forKey: "value")
        row.optionalData.updateValue(Data([0, 1, 2]), forKey: "value")
        row.optionalUUIDs.updateValue(uuid, forKey: "value")
        row.choices["value"] = .first
        row.optionalChoices.updateValue(.first, forKey: "value")
    }

    private func insertNulls(_ row: FingerprintMapShapeObject) {
        row.optionalInts.updateValue(nil, forKey: "null")
        row.optionalInt8s.updateValue(nil, forKey: "null")
        row.optionalInt16s.updateValue(nil, forKey: "null")
        row.optionalInt32s.updateValue(nil, forKey: "null")
        row.optionalInt64s.updateValue(nil, forKey: "null")
        row.optionalBools.updateValue(nil, forKey: "null")
        row.optionalFloats.updateValue(nil, forKey: "null")
        row.optionalDoubles.updateValue(nil, forKey: "null")
        row.optionalStrings.updateValue(nil, forKey: "null")
        row.optionalDates.updateValue(nil, forKey: "null")
        row.optionalData.updateValue(nil, forKey: "null")
        row.optionalUUIDs.updateValue(nil, forKey: "null")
        row.optionalChoices.updateValue(nil, forKey: "null")
    }

    func testEveryAdvertisedPrimitiveMapShapeFingerprintsEmptyAndPopulated() throws {
        let row = FingerprintMapShapeObject()
        XCTAssertTrue(BigSyncRecordFingerprint.supports(row))
        let empty = try BigSyncRecordFingerprint.fields(of: row)
        XCTAssertEqual(empty.count, 26)
        XCTAssertEqual(Set(empty.values).count, 1)
        populate(row)
        let populated = try BigSyncRecordFingerprint.fields(of: row)
        for name in empty.keys { XCTAssertNotEqual(empty[name], populated[name], name) }
        insertNulls(row)
        let withNulls = try BigSyncRecordFingerprint.fields(of: row)
        for name in empty.keys where name.hasPrefix("optional") {
            XCTAssertNotEqual(populated[name], withNulls[name], name)
        }
        let realm = try realm()
        try realm.write { realm.add(row) }
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), withNulls)
    }

    func testOptionalAndFixedWidthValuesKeepExistingScalarEncoding() throws {
        let row = FingerprintMapShapeObject()
        populate(row)
        let fields = try BigSyncRecordFingerprint.fields(of: row)
        for name in ["int8s", "int16s", "int32s", "int64s", "optionalInts", "optionalInt8s",
                     "optionalInt16s", "optionalInt32s", "optionalInt64s", "choices", "optionalChoices"] {
            XCTAssertEqual(fields["ints"], fields[name], name)
        }
        for (plain, optional) in [("bools", "optionalBools"), ("floats", "optionalFloats"),
                                  ("doubles", "optionalDoubles"), ("strings", "optionalStrings"),
                                  ("dates", "optionalDates"), ("data", "optionalData"), ("uuids", "optionalUUIDs")] {
            XCTAssertEqual(fields[plain], fields[optional], optional)
        }
        XCTAssertEqual(fields["floats"], fields["doubles"])
    }

    func testStoredNullMissingKeyAndPresentEmptyValueRemainDistinct() throws {
        let row = FingerprintMapShapeObject()
        let empty = try BigSyncRecordFingerprint.fields(of: row)
        row.optionalStrings.updateValue(nil, forKey: "key")
        row.optionalData.updateValue(nil, forKey: "key")
        let null = try BigSyncRecordFingerprint.fields(of: row)
        XCTAssertNotEqual(empty["optionalStrings"], null["optionalStrings"])
        XCTAssertNotEqual(empty["optionalData"], null["optionalData"])
        row.optionalStrings.updateValue("", forKey: "key")
        row.optionalData.updateValue(Data(), forKey: "key")
        let presentEmpty = try BigSyncRecordFingerprint.fields(of: row)
        XCTAssertNotEqual(null["optionalStrings"], presentEmpty["optionalStrings"])
        XCTAssertNotEqual(null["optionalData"], presentEmpty["optionalData"])
        row.optionalStrings.removeObject(for: "key")
        row.optionalData.removeObject(for: "key")
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: row), empty)
    }

    func testFixedWidthExtremesAndInsertionOrderDoNotLoseBits() throws {
        let first = FingerprintMapShapeObject(), second = FingerprintMapShapeObject()
        first.int64s["min"] = .min
        first.int64s["max"] = .max
        second.int64s["max"] = .max
        second.int64s["min"] = .min
        first.optionalInt64s.updateValue(.min, forKey: "min")
        first.optionalInt64s.updateValue(.max, forKey: "max")
        let original = try BigSyncRecordFingerprint.fields(of: first)
        XCTAssertEqual(original["int64s"], try BigSyncRecordFingerprint.fields(of: second)["int64s"])
        XCTAssertEqual(original["int64s"], original["optionalInt64s"])
        first.int64s["max"] = Int64.max - 1
        XCTAssertNotEqual(original["int64s"], try BigSyncRecordFingerprint.fields(of: first)["int64s"])
    }

    func testNonfiniteOptionalValuesStillFailRatherThanBecomingNull() throws {
        let row = FingerprintMapShapeObject()
        row.optionalFloats.updateValue(.nan, forKey: "bad")
        XCTAssertThrowsError(try BigSyncRecordFingerprint.fields(of: row))
        row.optionalFloats.removeAll()
        row.optionalDoubles.updateValue(.infinity, forKey: "bad")
        XCTAssertThrowsError(try BigSyncRecordFingerprint.fields(of: row))
        row.optionalDoubles.updateValue(nil, forKey: "bad")
        XCTAssertNoThrow(try BigSyncRecordFingerprint.fields(of: row))
    }

    func testAcceptedBaselineIsStableAndNullClearParticipatesInThreeWaySelection() throws {
        let realm = try realm()
        let row = FingerprintMapShapeObject()
        row.optionalInts.updateValue(1, forKey: "value")
        try realm.write { realm.add(row) }
        let base = try BigSyncRecordFingerprint.fields(of: row)
        try realm.write {
            XCTAssertTrue(BigSyncRecordBaseline.install(recordName: "record", namespace: "namespace",
                fields: base, schemaSignature: "same-schema", acceptedRevision: "accepted", in: realm))
            XCTAssertFalse(BigSyncRecordBaseline.install(recordName: "record", namespace: "namespace",
                fields: base, schemaSignature: "same-schema", acceptedRevision: "accepted", in: realm))
        }
        try realm.write { row.optionalInts.updateValue(nil, forKey: "value") }
        let remote = try BigSyncRecordFingerprint.fields(of: row)
        let incoming = try BigSyncRecordRebasePlanner.incomingFields(
            base: base, local: base, remote: remote, policy: .independentFields,
            preferRemoteOnConflict: false, localLifetime: nil, remoteLifetime: nil
        )
        XCTAssertTrue(incoming.contains("optionalInts"))
        let accepted = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: "record"))
        XCTAssertEqual(accepted.fieldDigests, base)
        XCTAssertEqual(accepted.revision, "accepted")
    }
}
