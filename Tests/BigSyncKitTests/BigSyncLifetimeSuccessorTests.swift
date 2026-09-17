import Foundation
import XCTest
@testable import BigSyncKit

final class BigSyncLifetimeSuccessorTests: XCTestCase {
    private let low = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
    private let high = UUID(uuidString: "ffffffff-ffff-ffff-ffff-ffffffffffff")!

    func testInitialAndObservedSuccessorsAreAccepted() throws {
        let versioned = try BigSyncLifetimeID.next(after: nil, nonce: high)
        for predecessor: String? in [nil, "initial", low.uuidString, versioned] {
            for nonce in [low, high] {
                let next = try BigSyncLifetimeID.next(after: predecessor, nonce: nonce)
                XCTAssertTrue(try BigSyncLifetimeID.isImmediateSuccessor(next, of: predecessor))
            }
        }
    }

    func testSiblingsAndExactRepeatsAreNotSuccessorCommands() throws {
        let left = try BigSyncLifetimeID.next(after: "initial", nonce: low)
        let right = try BigSyncLifetimeID.next(after: "initial", nonce: high)
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(left, of: left))
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(right, of: left))
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(left, of: right))
    }

    func testSkippedAndReversedGenerationsAreRejected() throws {
        let first = try BigSyncLifetimeID.next(after: nil, nonce: low)
        let second = try BigSyncLifetimeID.next(after: first, nonce: low)
        let third = try BigSyncLifetimeID.next(after: second, nonce: high)
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(second, of: nil))
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(third, of: first))
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(first, of: second))
    }

    func testOpaqueCandidatesCannotAuthorizeAReset() throws {
        let first = try BigSyncLifetimeID.next(after: nil, nonce: low)
        for candidate in ["", "initial", low.uuidString, "another-epoch"] {
            XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(candidate, of: nil))
            XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(candidate, of: "initial"))
            XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(candidate, of: first))
        }
    }

    func testMalformedEitherSideFailsClosed() throws {
        let first = try BigSyncLifetimeID.next(after: nil, nonce: low)
        for malformed in ["bsk2:unknown", "bsk1:1:" + low.uuidString,
                          "bsk1:0000000000000000:" + low.uuidString,
                          "bsk1:0000000000000001:invalid",
                          "bsk1:000000000000000A:" + low.uuidString] {
            XCTAssertThrowsError(try BigSyncLifetimeID.isImmediateSuccessor(malformed, of: first))
            XCTAssertThrowsError(try BigSyncLifetimeID.isImmediateSuccessor(first, of: malformed))
        }
    }

    func testCounterExhaustionDoesNotOverflowValidation() throws {
        let last = "bsk1:ffffffffffffffff:" + low.uuidString.lowercased()
        let first = try BigSyncLifetimeID.next(after: nil, nonce: low)
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(last, of: last))
        XCTAssertFalse(try BigSyncLifetimeID.isImmediateSuccessor(first, of: last))
    }

    func testGenerationSuccessionDoesNotDependOnNonceOrder() throws {
        let first = try BigSyncLifetimeID.next(after: nil, nonce: high)
        let second = try BigSyncLifetimeID.next(after: first, nonce: low)
        XCTAssertTrue(try BigSyncLifetimeID.isImmediateSuccessor(second, of: first))
    }
}
