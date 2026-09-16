import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

final class BigSyncLifetimeIDTests: XCTestCase {
    private let low = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
    private let high = UUID(uuidString: "ffffffff-ffff-ffff-ffff-ffffffffffff")!

    func testLegacySuccessorStartsVersionedLineage() throws {
        let next = try BigSyncLifetimeID.next(after: "legacy-epoch", nonce: low)
        XCTAssertEqual(next, "bsk1:0000000000000001:00000000-0000-0000-0000-000000000001")
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: "legacy-epoch", incoming: next), true)
    }

    func testCausalSuccessorOutranksAnyNonceInPreviousGeneration() throws {
        let first = try BigSyncLifetimeID.next(after: nil, nonce: high)
        let next = try BigSyncLifetimeID.next(after: first, nonce: low)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: first, incoming: next), true)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: next, incoming: first), false)
    }

    func testConcurrentSuccessorsHaveSymmetricStableWinner() throws {
        let left = try BigSyncLifetimeID.next(after: "E0", nonce: low)
        let right = try BigSyncLifetimeID.next(after: "E0", nonce: high)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: left, incoming: right), true)
        XCTAssertEqual(try BigSyncLifetimeID.prefersIncoming(local: right, incoming: left), false)
    }

    func testLegacyOrderingIsNotInventedFromUUIDLexicography() throws {
        XCTAssertNil(try BigSyncLifetimeID.prefersIncoming(local: low.uuidString, incoming: high.uuidString))
    }

    func testMalformedReservedLifetimesFailClosed() {
        for value in ["bsk1:1:" + low.uuidString.lowercased(),
                      "bsk1:0000000000000000:" + low.uuidString.lowercased(),
                      "bsk1:0000000000000001:bad", "bsk2:unknown",
                      "bsk1:000000000000000A:" + low.uuidString.lowercased()] {
            XCTAssertThrowsError(try BigSyncLifetimeID.validate(value), value)
            XCTAssertThrowsError(try BigSyncLifetimeID.next(after: value), value)
        }
    }

    func testCounterOverflowDoesNotWrapToAnOlderLifetime() {
        XCTAssertThrowsError(try BigSyncLifetimeID.next(after: "bsk1:ffffffffffffffff:" + low.uuidString.lowercased())) {
            guard case BigSyncRecordRebaseError.lifetimeOverflow = $0 else {
                return XCTFail("Expected explicit overflow, got \($0)")
            }
        }
    }

    func testComparisonRevisionChangesForNewServerVersionWithIdenticalValues() throws {
        var config = Realm.Configuration()
        config.inMemoryIdentifier = UUID().uuidString
        config.objectTypes = [BigSyncRecordBaseline.self]
        let realm = try Realm(configuration: config)
        let fields = ["text": Data([1])]
        try realm.write {
            XCTAssertTrue(BigSyncRecordBaseline.install(recordName: "row", namespace: "account",
                fields: fields, serverChangeTag: "version-1", in: realm))
        }
        let baseline = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: "row"))
        let revision = baseline.revision
        try realm.write {
            XCTAssertFalse(BigSyncRecordBaseline.install(recordName: "row", namespace: "account",
                fields: fields, serverChangeTag: "version-1", in: realm))
        }
        XCTAssertEqual(baseline.revision, revision)
        try realm.write {
            XCTAssertTrue(BigSyncRecordBaseline.install(recordName: "row", namespace: "account",
                fields: fields, serverChangeTag: "version-2", in: realm))
        }
        XCTAssertNotEqual(baseline.revision, revision)
        XCTAssertEqual(baseline.fieldDigests, fields)
    }

    func testInvalidationBeforeFirstAcceptanceHasDurableNonNilRevision() throws {
        var config = Realm.Configuration()
        config.inMemoryIdentifier = UUID().uuidString
        config.objectTypes = [BigSyncRecordBaseline.self]
        let realm = try Realm(configuration: config)
        try realm.write { BigSyncRecordBaseline.invalidate(recordName: "row", in: realm) }
        let baseline = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: "row"))
        XCTAssertTrue(baseline.invalidated)
        XCTAssertFalse(baseline.revision.isEmpty)
        XCTAssertEqual(baseline.fields.count, 0)
        let revision = baseline.revision
        try realm.write {
            BigSyncRecordBaseline.install(recordName: "row", namespace: "new-account", fields: ["text": Data([2])], in: realm)
        }
        XCTAssertFalse(baseline.invalidated)
        XCTAssertNotEqual(baseline.revision, revision)
        XCTAssertEqual(baseline.namespace, "new-account")
    }
}
