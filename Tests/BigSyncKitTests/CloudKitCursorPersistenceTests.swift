import Foundation
import XCTest
@testable import BigSyncKit

/// Tests the real opaque cursor storage boundary, not CloudKit's secure decoder.
final class CloudKitCursorPersistenceTests: XCTestCase {
    func testAbsentCursorIsTheOnlyFirstFetchValue() throws {
        XCTAssertNil(try DatabaseChangeCursor(persistedValue: nil))
        let emptyRepresentations: [Any] = [Data(), NSData(), "", NSNull(), [], [:] as [String: String]]
        for value in emptyRepresentations {
            XCTAssertThrowsError(try DatabaseChangeCursor(persistedValue: value)) { error in
                XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
            }
        }
    }

    func testWrongPersistedTypesAreNotInterpretedAsAbsence() {
        let malformed: [Any] = ["token", 0, 1, false, true, Date(timeIntervalSince1970: 1),
                                [UInt8(1)], ["token": Data([1])], NSNumber(value: 12)]
        for value in malformed {
            XCTAssertThrowsError(try DatabaseChangeCursor(persistedValue: value)) { error in
                XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
            }
        }
    }

    func testNonemptyOpaqueBytesRoundTripWithoutPretendingToDecodeSDKTokens() throws {
        for bytes in [Data([0]), Data([1, 2, 3]), Data("scripted-feed-token".utf8),
                      Data((0...255).map(UInt8.init))] {
            let cursor = try XCTUnwrap(DatabaseChangeCursor(persistedValue: bytes))
            XCTAssertEqual(cursor.serializedData, bytes)
            XCTAssertEqual(cursor, DatabaseChangeCursor(serializedData: bytes))
            XCTAssertEqual(try DatabaseChangeCursor(persistedValue: bytes as NSData), cursor)
        }
        // A slice must preserve its own bytes, not the backing buffer's prefix.
        let backing = Data([99, 1, 2, 3, 88])
        let slice = backing[1..<4]
        XCTAssertEqual(try DatabaseChangeCursor(persistedValue: slice)?.serializedData, Data([1, 2, 3]))
    }

    func testCursorCapturesAValueRatherThanMutableStorage() throws {
        let mutable = NSMutableData(data: Data([1, 2, 3]))
        let cursor = try XCTUnwrap(DatabaseChangeCursor(persistedValue: mutable))
        mutable.setData(Data([4, 5]))
        XCTAssertEqual(cursor.serializedData, Data([1, 2, 3]))
    }

    func testZoneCheckpointAbsenceAndExplicitResetAreEquivalent() throws {
        XCTAssertNil(try RecordZoneChangeCursor(persistedValues: [Data?]()))
        XCTAssertNil(try RecordZoneChangeCursor(persistedValues: [nil] as [Data?]))
        let bytes = Data([0, 255, 3])
        XCTAssertEqual(try RecordZoneChangeCursor(persistedValues: [bytes])?.serializedData, bytes)
        XCTAssertThrowsError(try RecordZoneChangeCursor(persistedValues: [Data()])) { error in
            XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
        }
    }

    func testZoneCheckpointRejectsEveryAmbiguousInventoryOrder() {
        let values: [Data?] = [nil, Data(), Data([1]), Data([2])]
        for first in values {
            for second in values {
                for rows in [[first, second], [second, first], [first, second, first]] {
                    XCTAssertThrowsError(try RecordZoneChangeCursor(persistedValues: rows)) { error in
                        XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
                    }
                }
            }
        }
    }

    func testZoneCheckpointInspectionStopsAfterTwoRows() {
        final class Visits { var count = 0 }
        let visits = Visits()
        let rows = sequence(first: 0, next: { $0 + 1 }).lazy.map { index -> Data? in
            visits.count += 1
            return Data([UInt8(truncatingIfNeeded: index)])
        }
        XCTAssertThrowsError(try RecordZoneChangeCursor(persistedValues: rows)) { error in
            XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
        }
        XCTAssertEqual(visits.count, 2, "An ambiguous cursor inventory must not scan the remaining rows")
    }

    func testZoneCheckpointPreservesOpaqueAndSlicedBytes() throws {
        let backing = Data([99, 0, 255, 4, 88])
        let slice = backing[1..<4]
        let zone = try XCTUnwrap(RecordZoneChangeCursor(persistedValues: [slice]))
        XCTAssertEqual(zone.serializedData, Data([0, 255, 4]))
        XCTAssertEqual(zone, RecordZoneChangeCursor(serializedData: Data([0, 255, 4])))
        XCTAssertNotEqual(CloudKitChangeFeedError.invalidPageCursor, .corruptCursor,
                          "A faulty page must not claim previously persisted history is corrupt")
    }

}
