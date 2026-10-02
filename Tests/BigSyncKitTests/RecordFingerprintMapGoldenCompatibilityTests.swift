import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private final class FingerprintMapGoldenObject: Object {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "record"
    @Persisted var ints: Map<String, Int>
    @Persisted var strings: Map<String, String>
    @Persisted var dates: Map<String, Date>
    @Persisted var uuids: Map<String, UUID>
}

final class RecordFingerprintMapGoldenCompatibilityTests: XCTestCase {
    func testExistingNonoptionalMapEncodingsAreUnchanged() throws {
        let row = FingerprintMapGoldenObject()
        row.ints["value"] = 1
        row.strings["value"] = "text"
        row.dates["value"] = Date(timeIntervalSinceReferenceDate: 1_000.25)
        row.uuids["value"] = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        // Fixed vectors from the existing length-framed SHA-256 representation:
        // map frame(key UTF-8, scalar frame(present tag, decoded scalar bytes)).
        // They intentionally do not call the implementation to build expected values.
        let expected = [
            "ints": "1825af553caccbc9d0e96ba3081c8620df083a30bf4d7e57636f0360ef55cfc9",
            "strings": "d622f90933792961b4833c149deed88c8261af381b382f951acc03ed9fb57f91",
            "dates": "5f6e1a5b821c73c13d59045eae15966e524046d792f1bea30d22b7e0f83f5216",
            "uuids": "b0487dd7c8b8c1294be1e13bf0ad1b863d6747a8b8fd78808bc801a6e5d04bc0",
        ]
        func encodedFields() throws -> [String: String] {
            try BigSyncRecordFingerprint.fields(of: row).mapValues {
                $0.map { String(format: "%02x", $0) }.joined()
            }
        }
        XCTAssertEqual(try encodedFields(), expected)
        let realm = try Realm(configuration: .init(
            inMemoryIdentifier: UUID().uuidString,
            objectTypes: [FingerprintMapGoldenObject.self]
        ))
        try realm.write { realm.add(row) }
        XCTAssertEqual(try encodedFields(), expected)
    }
}
