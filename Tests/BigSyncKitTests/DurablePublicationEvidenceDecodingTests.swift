import CoreFoundation
import Foundation
import XCTest
@testable import BigSyncKit

final class DurablePublicationEvidenceDecodingTests: XCTestCase {
    private let requestID = UUID(uuidString: "C0210123-1111-2222-3333-444455556666")!
    private let timestamp = Date(timeIntervalSince1970: 1_234_567_890)

    private func raw(binding: String? = "binding") -> [String: Any] {
        var value: [String: Any] = [
            "version": 1,
            "domainScopeIdentifier": "dataset",
            "accountScopeIdentifier": "account",
            "zoneOwnerName": "owner",
            "zoneName": "zone",
            "changeFeedEpoch": 7,
            "consumedServerBoundaryIdentifier": "boundary",
            "runID": requestID.uuidString.lowercased(),
            "publishedAt": timestamp,
        ]
        value["replicaBindingGenerationIdentifier"] = binding
        return value
    }

    private func rejects(_ value: Any, file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertThrowsError(try BigSyncDurablePublicationEvidence(persistedValue: value),
                             file: file, line: line) { error in
            guard case DurableKeyValueStoreError.mutationNotDurable = error else {
                return XCTFail("Unexpected error: \(error)", file: file, line: line)
            }
        }
    }

    func testBoundEvidencePreservesEveryValue() throws {
        let actual = try BigSyncDurablePublicationEvidence(persistedValue: raw())
        let expected = BigSyncDurablePublicationEvidence(
            domainScopeIdentifier: "dataset", accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding", zoneOwnerName: "owner",
            zoneName: "zone", changeFeedEpoch: 7,
            consumedServerBoundaryIdentifier: "boundary", runID: requestID, publishedAt: timestamp)
        XCTAssertEqual(actual, expected)
    }

    func testAbsentBindingRetainsIntentionalUnboundFormat() throws {
        let value = try BigSyncDurablePublicationEvidence(persistedValue: raw(binding: nil))
        XCTAssertNil(value.replicaBindingGenerationIdentifier)
        XCTAssertEqual(value.changeFeedEpoch, 7)
    }

    func testPresentBindingMustBeNonemptyString() {
        for invalid: Any in [NSNull(), 12, true, Data(), ["binding"], ["id": "binding"], ""] {
            var value = raw()
            value["replicaBindingGenerationIdentifier"] = invalid
            rejects(value)
        }
    }

    func testVersionRejectsBooleanFractionAndIntegralReal() {
        for invalid: Any in [true, false, 1.5, 1.0, NSDecimalNumber(string: "1.9"), "1"] {
            var value = raw()
            value["version"] = invalid
            rejects(value)
        }
    }

    func testVersionRejectsUnknownNegativeAndOverflowValues() {
        for invalid: Any in [0, -1, 2, Int.max, NSNumber(value: UInt64.max)] {
            var value = raw()
            value["version"] = invalid
            rejects(value)
        }
    }

    func testEpochRejectsBooleanFractionAndIntegralReal() {
        for invalid: Any in [true, false, 7.5, -0.5, 7.0, -0.0,
                              NSDecimalNumber(string: "7.5"), "7"] {
            var value = raw()
            value["changeFeedEpoch"] = invalid
            rejects(value)
        }
    }

    func testEpochRejectsNegativeAndUnrepresentableValues() {
        for invalid: Any in [-1, Int.min, NSNumber(value: UInt64.max), Double.infinity,
                              -Double.infinity, Double.nan] {
            var value = raw()
            value["changeFeedEpoch"] = invalid
            rejects(value)
        }
    }

    func testExactIntegerWidthsRemainSupported() throws {
        let integers: [NSNumber] = [
            NSNumber(value: Int8(7)), NSNumber(value: UInt8(7)),
            NSNumber(value: Int16(7)), NSNumber(value: UInt16(7)),
            NSNumber(value: Int32(7)), NSNumber(value: UInt32(7)),
            NSNumber(value: Int64(7)), NSNumber(value: UInt64(7)), NSNumber(value: Int(7)),
        ]
        for integer in integers {
            var value = raw()
            value["changeFeedEpoch"] = integer
            XCTAssertEqual(try BigSyncDurablePublicationEvidence(persistedValue: value).changeFeedEpoch, 7)
        }
    }

    func testEpochZeroAndLargestSupportedIntegerRemainExact() throws {
        for epoch in [0, Int.max] {
            var value = raw()
            value["changeFeedEpoch"] = epoch
            XCTAssertEqual(try BigSyncDurablePublicationEvidence(persistedValue: value).changeFeedEpoch, epoch)
        }
    }

    func testEveryRequiredFieldRejectsAbsenceAndNull() {
        for key in raw(binding: nil).keys {
            var absent = raw()
            absent.removeValue(forKey: key)
            rejects(absent)
            var null = raw()
            null[key] = NSNull()
            rejects(null)
        }
    }

    func testRequiredTextRejectsEmptyAndNonStringValues() {
        let fields = ["domainScopeIdentifier", "accountScopeIdentifier", "zoneOwnerName", "zoneName",
                      "consumedServerBoundaryIdentifier", "runID"]
        for field in fields {
            for invalid: Any in ["", 3, false, ["value"], Data()] {
                var value = raw()
                value[field] = invalid
                rejects(value)
            }
        }
    }

    func testInvalidRunIdentifierRejectsWithoutInventingOne() {
        for run in ["not-a-uuid", "C0210123-1111-2222-3333-44445555666", " " + requestID.uuidString] {
            var value = raw()
            value["runID"] = run
            rejects(value)
        }
    }

    func testNonfinitePublicationDatesReject() {
        for instant in [Double.nan, Double.infinity, -Double.infinity] {
            var value = raw()
            value["publishedAt"] = Date(timeIntervalSinceReferenceDate: instant)
            rejects(value)
        }
    }

    func testPublicationDateRejectsWrongTypes() {
        for invalid: Any in [123, "2026-10-05", Data(), NSNull()] {
            var value = raw()
            value["publishedAt"] = invalid
            rejects(value)
        }
    }

    func testFiniteHistoricalAndFutureDatesAreNotExpiryPolicy() throws {
        for date in [Date.distantPast, timestamp, Date.distantFuture] {
            var value = raw()
            value["publishedAt"] = date
            XCTAssertEqual(try BigSyncDurablePublicationEvidence(persistedValue: value).publishedAt, date)
        }
    }

    func testOpaqueStringsRetainTheirOriginalUTF8Bytes() throws {
        for opaque in ["caf\u{E9}", "cafe\u{301}", " value "] {
            var value = raw()
            value["domainScopeIdentifier"] = opaque
            value["accountScopeIdentifier"] = opaque
            value["replicaBindingGenerationIdentifier"] = opaque
            value["zoneOwnerName"] = opaque
            value["zoneName"] = opaque
            value["consumedServerBoundaryIdentifier"] = opaque
            let evidence = try BigSyncDurablePublicationEvidence(persistedValue: value)
            for actual in [evidence.domainScopeIdentifier, evidence.accountScopeIdentifier,
                           try XCTUnwrap(evidence.replicaBindingGenerationIdentifier), evidence.zoneOwnerName,
                           evidence.zoneName, evidence.consumedServerBoundaryIdentifier] {
                XCTAssertEqual(Data(actual.utf8), Data(opaque.utf8))
            }
        }
    }

    func testExtraFieldsRemainForwardCompatibleWithinKnownVersion() throws {
        var value = raw()
        value["additionalDiagnostic"] = ["reason": "future additive field"]
        XCTAssertEqual(try BigSyncDurablePublicationEvidence(persistedValue: value),
                       try BigSyncDurablePublicationEvidence(persistedValue: raw()))
    }

    func testBinaryAndXMLPropertyListsPreserveValidBoundAndUnboundRecords() throws {
        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
            for binding: String? in [nil, "binding"] {
                for epoch in [0, 7, Int.max] {
                    var value = raw(binding: binding)
                    value["changeFeedEpoch"] = epoch
                    let bytes = try PropertyListSerialization.data(fromPropertyList: value, format: format, options: 0)
                    let restored = try PropertyListSerialization.propertyList(from: bytes, options: [], format: nil)
                    let evidence = try BigSyncDurablePublicationEvidence(persistedValue: restored)
                    XCTAssertEqual(evidence, try BigSyncDurablePublicationEvidence(persistedValue: value))
                    XCTAssertEqual(evidence.changeFeedEpoch, epoch)
                }
            }
        }
    }

    func testSerializedMalformedNumbersAndBindingStillReject() throws {
        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
            for (key, invalid): (String, Any) in [("version", true), ("version", 1.5),
                                                ("changeFeedEpoch", 7.5),
                                                ("replicaBindingGenerationIdentifier", 12)] {
                var value = raw()
                value[key] = invalid
                let bytes = try PropertyListSerialization.data(fromPropertyList: value, format: format, options: 0)
                let restored = try PropertyListSerialization.propertyList(from: bytes, options: [], format: nil)
                rejects(restored)
            }
        }
    }

    func testWrongTopLevelShapesReject() {
        for value: Any in [NSNull(), 1, "evidence", ["a", "b"], Data()] {
            rejects(value)
        }
    }
}
