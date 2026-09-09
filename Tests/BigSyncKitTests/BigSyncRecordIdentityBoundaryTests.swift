import Foundation
import XCTest
@testable import BigSyncKit

final class BigSyncRecordIdentityBoundaryTests: XCTestCase {
    func testIntegerNamesHaveExactlyTheOutboundDecimalRepresentation() {
        for value in Array(-1024...1024) + [Int.min, Int.max] {
            let canonical = String(value)
            XCTAssertEqual(BigSyncRecordIdentifier.canonicalValue(from: canonical) { Int($0) }, value)
            let aliases = ["+" + canonical, "0" + canonical, " " + canonical,
                           canonical + " ", "\t" + canonical,
                           value < 0 ? "-0" + canonical.dropFirst() : "00" + canonical]
            for alias in aliases {
                // The grammar oracle is independent of the production helper.
                let ascii = Array(alias.utf8)
                var digits = ascii[...]
                if digits.first == 45 { digits = digits.dropFirst() }
                let canonicalGrammar = !digits.isEmpty
                    && digits.allSatisfy { (48...57).contains($0) }
                    && (digits.count == 1 || digits.first != 48)
                    && !(ascii.first == 45 && digits.count == 1 && digits.first == 48)
                let expected = canonicalGrammar ? Int(alias) : nil
                XCTAssertEqual(BigSyncRecordIdentifier.canonicalValue(from: alias) { Int($0) }, expected)
            }
        }
        for invalid in ["", "-0", "+0", "00", "01", "-01", "1.0", "1e0", "١", "１",
                        "9223372036854775808", "-9223372036854775809"] {
            XCTAssertNil(BigSyncRecordIdentifier.canonicalValue(from: invalid) { Int($0) })
        }
    }

    func testUUIDNamesRoundTripThroughTheSameWriterRepresentation() {
        let text = "ABCDEF01-2345-6789-ABCD-EF0123456789"
        let uuid = UUID(uuidString: text)!
        XCTAssertEqual(BigSyncRecordIdentifier.canonicalValue(from: text) { UUID(uuidString: $0) }, uuid)
        XCTAssertNotNil(UUID(uuidString: text.lowercased()), "The parser alone accepts the alias")
        XCTAssertNil(BigSyncRecordIdentifier.canonicalValue(from: text.lowercased()) { UUID(uuidString: $0) })
        XCTAssertNil(BigSyncRecordIdentifier.canonicalValue(from: "{" + text + "}") { UUID(uuidString: $0) })
    }

    func testOpaqueStringSuffixesKeepExactBytesIncludingLeadingCombiningMarks() {
        let identifiers = ["0", "01", "+1", "-0", ".", "a.b", "日本語", "\u{0301}id",
                           "\u{200D}id", "\0x", "é", "e\u{0301}", "K", "K"]
        for entity in ["Entity", "Type.Name", "é", "e\u{0301}"] {
            for identifier in identifiers {
                let parsed = BigSyncRecordIdentifier.objectIdentifier(from: entity + "." + identifier, entityType: entity)
                XCTAssertEqual(parsed.map { Array($0.utf8) }, Array(identifier.utf8))
            }
            XCTAssertNil(BigSyncRecordIdentifier.objectIdentifier(from: entity + ".", entityType: entity))
            XCTAssertNil(BigSyncRecordIdentifier.objectIdentifier(from: entity, entityType: entity))
            XCTAssertNil(BigSyncRecordIdentifier.objectIdentifier(from: "Wrong." + entity, entityType: entity))
        }
        for identifier in identifiers {
            XCTAssertEqual(BigSyncRecordIdentifier.entityType(from: "Entity." + identifier), "Entity")
        }
        XCTAssertNil(BigSyncRecordIdentifier.entityType(from: ".id"))
        XCTAssertNil(BigSyncRecordIdentifier.entityType(from: "Entity"))
        XCTAssertNil(BigSyncRecordIdentifier.entityType(from: ""))
        XCTAssertNil(BigSyncRecordIdentifier.objectIdentifier(from: "é.x", entityType: "e\u{0301}"))
        XCTAssertNil(BigSyncRecordIdentifier.objectIdentifier(from: ".x", entityType: ""))
    }

    func testOperationalValidationErrorsPreserveTheirExactErrorValue() throws {
        let admission = BigSyncSemanticAdmissionUnavailable(entityType: "Source")
        XCTAssertThrowsError(try BigSyncInboundValidationErrors.rethrowNonSemantic(admission)) {
            XCTAssertEqual($0 as? BigSyncSemanticAdmissionUnavailable, admission)
        }
        let resource = BigSyncInboundResourceUnavailable(entityType: "Source", fieldName: "payload")
        XCTAssertThrowsError(try BigSyncInboundValidationErrors.rethrowNonSemantic(resource)) {
            XCTAssertEqual($0 as? BigSyncInboundResourceUnavailable, resource)
        }
        XCTAssertThrowsError(try BigSyncInboundValidationErrors.rethrowNonSemantic(CancellationError())) {
            XCTAssertTrue($0 is CancellationError)
        }
        // Unrecognized validator errors preserve the existing quarantine path.
        enum SemanticFailure: Error { case rejected }
        XCTAssertNoThrow(try BigSyncInboundValidationErrors.rethrowNonSemantic(SemanticFailure.rejected))
        XCTAssertNoThrow(try BigSyncInboundValidationErrors.rethrowNonSemantic(NSError(domain: "DomainValidation", code: 1)))
    }

    func testCancelledValidationCannotBecomeAQuarantineVerdict() async {
        let cancelled = await Task.detached { () -> Bool in
            withUnsafeCurrentTask { $0?.cancel() }
            do {
                try BigSyncInboundValidationErrors.rethrowNonSemantic(NSError(domain: "DomainValidation", code: 1))
                return false
            } catch is CancellationError {
                return true
            } catch {
                return false
            }
        }.value
        XCTAssertTrue(cancelled)
    }
}
