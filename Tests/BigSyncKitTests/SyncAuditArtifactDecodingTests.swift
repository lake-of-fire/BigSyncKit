import Foundation
import XCTest
@testable import BigSyncKit

/// Tests the actual Codable artifact, not Realm or CloudKit execution.
final class SyncAuditArtifactDecodingTests: XCTestCase {
    private let baseCounts = [
        "serverRecordCount", "ownedServerRecordCount", "unknownServerRecordCount",
        "localObjectCount", "trackingRecordCount", "pendingMutationCount",
        "pendingRelationshipCount",
    ]
    private let comparisonCounts = [
        "unresolvedSubmissionCount", "acceptedBaselineCount", "invalidatedBaselineCount",
        "resolvedPreservationReceiptCount", "retainedTombstoneCount",
    ]

    private func artifact(version: Int? = 1) -> [String: Any] {
        var value: [String: Any] = ["issues": [String]()]
        for key in baseCounts { value[key] = 0 }
        if let version {
            value["comparisonEvidenceVersion"] = version
            for key in comparisonCounts { value[key] = 0 }
        }
        return value
    }

    private func decode(_ value: [String: Any]) throws -> BigSyncSynchronizationAudit {
        try JSONDecoder().decode(BigSyncSynchronizationAudit.self,
            from: JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]))
    }

    private func rejects(_ value: [String: Any], at key: String,
                         file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertThrowsError(try decode(value), "Malformed field: \(key)", file: file, line: line) { error in
            let path: [any CodingKey]
            switch error {
            case let DecodingError.keyNotFound(missing, context): path = context.codingPath + [missing]
            case let DecodingError.valueNotFound(_, context): path = context.codingPath
            case let DecodingError.typeMismatch(_, context): path = context.codingPath
            case let DecodingError.dataCorrupted(context): path = context.codingPath
            default:
                XCTFail("Unexpected error: \(error)", file: file, line: line)
                return
            }
            // Foundation may reject a fractional JSON integer at its parser
            // boundary without a key path. The rejection is the contract here;
            // the dedicated nested-missing-field test requires its exact path.
            if let last = path.last {
                XCTAssertEqual(last.stringValue, key, file: file, line: line)
            }
        }
    }

    func testLegacyArtifactRemainsVersionZeroWithoutInventingInspection() throws {
        let old = try decode(artifact(version: nil))
        XCTAssertEqual(old.comparisonEvidenceVersion, 0)
        XCTAssertEqual(old.unresolvedSubmissionCount, 0)
        XCTAssertEqual(old.acceptedBaselineCount, 0)
        XCTAssertEqual(old.invalidatedBaselineCount, 0)
        XCTAssertEqual(old.resolvedPreservationReceiptCount, 0)
        XCTAssertEqual(old.retainedTombstoneCount, 0)
        XCTAssertTrue(old.isClean)
        XCTAssertFalse(old.comparisonEvidenceVersion == 1 && old.isClean)
        XCTAssertEqual(try JSONDecoder().decode(BigSyncSynchronizationAudit.self,
            from: JSONEncoder().encode(old)), old)
    }

    func testExplicitLegacyVersionPreservesAvailableCounts() throws {
        var value = artifact(version: 0)
        value["unresolvedSubmissionCount"] = 2
        value["acceptedBaselineCount"] = 7
        value.removeValue(forKey: "retainedTombstoneCount")
        let old = try decode(value)
        XCTAssertEqual(old.comparisonEvidenceVersion, 0)
        XCTAssertEqual(old.unresolvedSubmissionCount, 2)
        XCTAssertEqual(old.acceptedBaselineCount, 7)
        XCTAssertEqual(old.retainedTombstoneCount, 0)
        XCTAssertFalse(old.isClean)
    }

    func testCompleteCurrentArtifactRoundTripsNonzeroEvidence() throws {
        var value = artifact()
        value["serverRecordCount"] = 3
        value["ownedServerRecordCount"] = 2
        value["unknownServerRecordCount"] = 1
        value["localObjectCount"] = 2
        value["trackingRecordCount"] = 2
        value["acceptedBaselineCount"] = 1
        value["invalidatedBaselineCount"] = 1
        value["resolvedPreservationReceiptCount"] = 3
        value["retainedTombstoneCount"] = 1
        let current = try decode(value)
        XCTAssertTrue(current.isClean)
        XCTAssertEqual(current.comparisonEvidenceVersion, 1)
        XCTAssertEqual(current.acceptedBaselineCount, 1)
        XCTAssertEqual(current.invalidatedBaselineCount, 1)
        XCTAssertEqual(current.resolvedPreservationReceiptCount, 3)
        XCTAssertEqual(current.retainedTombstoneCount, 1)
        XCTAssertEqual(try JSONDecoder().decode(BigSyncSynchronizationAudit.self,
            from: JSONEncoder().encode(current)), current)
    }

    func testCurrentVersionRequiresEveryComparisonCount() {
        for key in comparisonCounts {
            var value = artifact()
            value.removeValue(forKey: key)
            rejects(value, at: key)
        }
    }

    func testCurrentVersionRejectsNullComparisonCounts() {
        for key in comparisonCounts {
            var value = artifact()
            value[key] = NSNull()
            rejects(value, at: key)
        }
    }

    func testNullVersionCannotDowngradeCurrentArtifactToLegacy() {
        var value = artifact(version: nil)
        value["comparisonEvidenceVersion"] = NSNull()
        rejects(value, at: "comparisonEvidenceVersion")
    }

    func testUnsupportedAndMalformedVersionsReject() {
        for invalid: Any in [-1, 2, true, "1", 1.5] {
            var value = artifact()
            value["comparisonEvidenceVersion"] = invalid
            rejects(value, at: "comparisonEvidenceVersion")
        }
    }

    func testAllCountsRejectNegativeValues() {
        for key in baseCounts + comparisonCounts {
            var value = artifact()
            value[key] = -1
            rejects(value, at: key)
        }
    }

    func testCountsRejectFractionBooleanAndString() {
        for key in baseCounts + comparisonCounts {
            for invalid: Any in [0.5, true, "0"] {
                var value = artifact()
                value[key] = invalid
                rejects(value, at: key)
            }
        }
    }

    func testPendingCountsCannotBeCleanWithoutIssueStrings() throws {
        for key in ["pendingMutationCount", "pendingRelationshipCount", "unresolvedSubmissionCount"] {
            var value = artifact()
            value[key] = 1
            let current = try decode(value)
            XCTAssertTrue(current.issues.isEmpty)
            XCTAssertFalse(current.isClean, "Debt must be decisive independently: \(key)")
        }
    }

    func testIssuesRemainUncleanWithoutPendingCounts() throws {
        var value = artifact()
        value["issues"] = ["server-field-mismatch:example"]
        XCTAssertFalse(try decode(value).isClean)
    }

    func testUnknownServerRecordsDoNotBecomeNewDebtPolicy() throws {
        var value = artifact()
        value["serverRecordCount"] = 3
        value["unknownServerRecordCount"] = 3
        XCTAssertTrue(try decode(value).isClean)
    }

    func testRequiredOriginalFieldsCannotBeMissing() {
        for key in baseCounts + ["issues"] {
            var value = artifact()
            value.removeValue(forKey: key)
            rejects(value, at: key)
        }
    }

    func testLegacyOptionalFieldsRejectMalformedPresentValues() {
        for key in comparisonCounts {
            for invalid: Any in [NSNull(), -1, "0"] {
                var value = artifact(version: nil)
                value[key] = invalid
                rejects(value, at: key)
            }
        }
    }

    func testBinaryAndXMLPropertyListsPreserveArtifactValues() throws {
        let value = try decode(artifact())
        for format: PropertyListSerialization.PropertyListFormat in [.binary, .xml] {
            let encoder = PropertyListEncoder()
            encoder.outputFormat = format
            XCTAssertEqual(try PropertyListDecoder().decode(BigSyncSynchronizationAudit.self,
                from: encoder.encode(value)), value)
        }
    }

    func testNestedDecodeReportsMissingComparisonFieldPath() throws {
        struct Report: Decodable { let audit: BigSyncSynchronizationAudit }
        var value = artifact()
        value.removeValue(forKey: "unresolvedSubmissionCount")
        let data = try JSONSerialization.data(withJSONObject: ["audit": value])
        XCTAssertThrowsError(try JSONDecoder().decode(Report.self, from: data)) { error in
            guard case let DecodingError.keyNotFound(key, context) = error else {
                return XCTFail("Unexpected error: \(error)")
            }
            XCTAssertEqual(key.stringValue, "unresolvedSubmissionCount")
            XCTAssertEqual(context.codingPath.map(\.stringValue), ["audit"])
        }
    }
}
