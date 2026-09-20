import BigSyncKit
import Foundation
import XCTest

/// Domain intake uses the real reconciliation order, never a parallel parser
/// or wall-clock approximation. Deliberately no @testable import.
final class PublicLifetimeOrderingTests: XCTestCase {
    func testUnversionedLifetimesDoNotManufactureCausality() throws {
        XCTAssertNil(try BigSyncLifetimeID.prefersIncoming(local: nil, incoming: nil))
        XCTAssertNil(try BigSyncLifetimeID.prefersIncoming(local: "legacy-z", incoming: "legacy-a"))
        XCTAssertNil(try BigSyncLifetimeID.prefersIncoming(local: "legacy-a", incoming: "legacy-z"))
    }

    func testKnownRetainedSuccessorOutranksOnlyItsPredecessors() throws {
        let first = try BigSyncLifetimeID.next(after: nil)
        let second = try BigSyncLifetimeID.next(after: first)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: "initial", incoming: first), true)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: first, incoming: second), true)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: second, incoming: first), false)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: first, incoming: first), false)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: first, incoming: nil), false)
    }

    func testConcurrentSuccessorsUseExistingDeterministicNonceOrder() throws {
        let lower = try BigSyncLifetimeID.next(after: nil, nonce: UUID(uuidString: "00000000-0000-0000-0000-000000000001")!)
        let higher = try BigSyncLifetimeID.next(after: nil, nonce: UUID(uuidString: "00000000-0000-0000-0000-000000000002")!)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: lower, incoming: higher), true)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: higher, incoming: lower), false)
        XCTAssertThrowsError(try BigSyncLifetimeID.prefersIncoming(local: "bsk1:invalid", incoming: higher))
        XCTAssertThrowsError(try BigSyncLifetimeID.prefersIncoming(local: lower, incoming: "bsk2:invalid"))
    }
}
