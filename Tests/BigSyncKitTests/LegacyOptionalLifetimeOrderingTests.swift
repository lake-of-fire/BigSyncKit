import Foundation
import XCTest
@testable import BigSyncKit

/// Opaque field digests simulate equality, not Realm IO. Native qualification remains required.
final class LegacyOptionalLifetimeOrderingTests: XCTestCase {
    private struct State: Equatable {
        let epoch: String?
        let body: String
        let deleted: Bool
        let title: String
        var fields: [String: Data] {
            ["epoch": epoch.map { Data([1]) + Data($0.utf8) } ?? Data([0]),
             "body": Data(body.utf8), "isDeleted": Data([deleted ? 1 : 0]),
             "title": Data(title.utf8)]
        }
        static func == (a: Self, b: Self) -> Bool { a.fields == b.fields }
    }
    private let base = State(epoch: "original", body: "original", deleted: false, title: "original")
    private let absent = State(epoch: nil, body: "absent-body", deleted: false, title: "original")
    private let empty = State(epoch: "", body: "empty-body", deleted: true, title: "original")
    private let bundle: Set<String> = ["epoch", "body", "isDeleted"]

    private func selected(base: State? = nil, local: State, remote: State,
                          preferRemote: Bool = false,
                          policy: BigSyncRecordRebasePolicy = .lifetimeBundle(
                            lifetimeField: "epoch", independentFields: ["title"])) throws -> Set<String> {
        try BigSyncRecordRebasePlanner.incomingFields(
            base: (base ?? self.base).fields, local: local.fields, remote: remote.fields,
            policy: policy, preferRemoteOnConflict: preferRemote,
            localLifetime: local.epoch, remoteLifetime: remote.epoch)
    }
    private func merge(base: State? = nil, local: State, remote: State,
                       preferRemote: Bool = false) throws -> State {
        let incoming = try selected(base: base, local: local, remote: remote, preferRemote: preferRemote)
        return State(epoch: incoming.contains("epoch") ? remote.epoch : local.epoch,
            body: incoming.contains("body") ? remote.body : local.body,
            deleted: incoming.contains("isDeleted") ? remote.deleted : local.deleted,
            title: incoming.contains("title") ? remote.title : local.title)
    }
    func testAbsentAndEmptyConcurrentLifetimesChooseTheSameBundle() throws {
        XCTAssertNoThrow(try BigSyncLifetimeID.validate(nil))
        XCTAssertNoThrow(try BigSyncLifetimeID.validate(""))
        XCTAssertNil(try BigSyncLifetimeID.prefersIncoming(local: nil, incoming: ""))
        XCTAssertEqual(try merge(local: absent, remote: empty), empty)
        XCTAssertEqual(try merge(local: empty, remote: absent), empty)
    }
    func testLegacyPresenceArbitrationDoesNotUseRecordClockPreference() throws {
        for preference in [false, true] {
            XCTAssertEqual(try merge(local: absent, remote: empty, preferRemote: preference), empty)
            XCTAssertEqual(try merge(local: empty, remote: absent, preferRemote: preference), empty)
        }
    }
    func testPresenceArbitrationKeepsDeletionAndPayloadInOneBundle() throws {
        XCTAssertEqual(try selected(local: absent, remote: empty).intersection(bundle), bundle)
        XCTAssertTrue(try selected(local: empty, remote: absent).isDisjoint(with: bundle))
    }
    func testIndependentTitleStillKeepsUncontestedLocalEdit() throws {
        let local = State(epoch: nil, body: absent.body, deleted: false, title: "edited-title")
        let result = try merge(local: local, remote: empty)
        XCTAssertEqual(result.epoch, empty.epoch)
        XCTAssertEqual(result.body, empty.body)
        XCTAssertEqual(result.deleted, empty.deleted)
        XCTAssertEqual(result.title, "edited-title")
    }
    func testPresentLocalWithAbsentUnchangedRemoteStillKeepsLocal() throws {
        XCTAssertEqual(try merge(base: absent, local: empty, remote: absent, preferRemote: true), empty)
    }
    func testChangedAbsenceStillWinsOverUnchangedEmptyBase() throws {
        XCTAssertEqual(try merge(base: empty, local: empty, remote: absent), absent)
        XCTAssertEqual(try merge(base: empty, local: absent, remote: empty, preferRemote: true), absent)
    }
    func testChangedEmptyStillWinsOverUnchangedAbsentBase() throws {
        XCTAssertEqual(try merge(base: absent, local: absent, remote: empty), empty)
        XCTAssertEqual(try merge(base: absent, local: empty, remote: absent), empty)
    }
    func testUnchangedNilAndEmptyEpochsRetainSameLifetimeConflictPolicy() throws {
        for epoch: String? in [nil, ""] {
            let initial = State(epoch: epoch, body: "base", deleted: false, title: "original")
            let lhs = State(epoch: epoch, body: "left", deleted: false, title: "original")
            let rhs = State(epoch: epoch, body: "right", deleted: true, title: "original")
            XCTAssertEqual(try merge(base: initial, local: lhs, remote: rhs), lhs)
            XCTAssertEqual(try merge(base: initial, local: lhs, remote: rhs, preferRemote: true), rhs)
        }
    }
    func testLegacyByteOrderingAndASCIIOrderingRemainUnchanged() throws {
        for (lower, higher) in [("a", "z"), ("e\u{0301}", "\u{00E9}"),
                               ("\u{304B}\u{3099}", "\u{304C}")] {
            let lhs = State(epoch: lower, body: "lower", deleted: false, title: "original")
            let rhs = State(epoch: higher, body: "higher", deleted: true, title: "original")
            XCTAssertEqual(try merge(local: lhs, remote: rhs), rhs)
            XCTAssertEqual(try merge(local: rhs, remote: lhs, preferRemote: true), rhs)
        }
    }
    func testNonemptyLegacyLifetimesStillDominateAbsentOrEmpty() throws {
        let value = State(epoch: "A", body: "nonempty", deleted: true, title: "original")
        for lower in [absent, empty] {
            XCTAssertEqual(try merge(local: lower, remote: value), value)
            XCTAssertEqual(try merge(local: value, remote: lower), value)
        }
    }
    func testVersionedGenerationStillDominatesEveryLegacyValue() throws {
        let epoch = try BigSyncLifetimeID.next(after: nil,
            nonce: UUID(uuidString: "00000000-0000-0000-0000-000000000001")!)
        let versioned = State(epoch: epoch, body: "versioned", deleted: false, title: "original")
        for legacy in [absent, empty, State(epoch: "z", body: "z", deleted: true, title: "original")] {
            XCTAssertEqual(try merge(local: legacy, remote: versioned), versioned)
            XCTAssertEqual(try merge(local: versioned, remote: legacy, preferRemote: true), versioned)
        }
    }
    func testVersionedOrderingStillUsesGenerationThenNonce() throws {
        let low = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let high = UUID(uuidString: "ffffffff-ffff-ffff-ffff-ffffffffffff")!
        let oneLow = try BigSyncLifetimeID.next(after: nil, nonce: low)
        let oneHigh = try BigSyncLifetimeID.next(after: nil, nonce: high)
        let twoLow = try BigSyncLifetimeID.next(after: oneHigh, nonce: low)
        for (left, right) in [(oneLow, oneHigh), (oneHigh, twoLow)] {
            let lhs = State(epoch: left, body: "left", deleted: true, title: "original")
            let rhs = State(epoch: right, body: "right", deleted: false, title: "original")
            XCTAssertEqual(try merge(local: lhs, remote: rhs), rhs)
            XCTAssertEqual(try merge(local: rhs, remote: lhs, preferRemote: true), rhs)
        }
    }
    func testMalformedVersionedLifetimesStillThrow() throws {
        for invalid in ["bsk", "bsk2:1:x", "bsk1:0000000000000000:00000000-0000-0000-0000-000000000001"] {
            let other = State(epoch: invalid, body: "bad", deleted: false, title: "original")
            XCTAssertThrowsError(try merge(local: absent, remote: other))
            XCTAssertThrowsError(try merge(local: other, remote: empty))
        }
    }
    func testUnrelatedPoliciesRetainTheirSelectionRules() throws {
        XCTAssertEqual(try selected(local: absent, remote: empty, policy: .disabled), [])
        XCTAssertEqual(try selected(local: absent, remote: empty, policy: .atomicRecord), [])
        XCTAssertEqual(try selected(local: absent, remote: empty, preferRemote: true, policy: .atomicRecord), Set(empty.fields.keys))
        XCTAssertEqual(try selected(local: absent, remote: empty, policy: .independentFields), ["title", "isDeleted"])
    }
    func testPolicyShapeValidationRemainsFailClosed() throws {
        XCTAssertThrowsError(try selected(local: absent, remote: empty,
            policy: .lifetimeBundle(lifetimeField: "missing", independentFields: [])))
        XCTAssertThrowsError(try selected(local: absent, remote: empty,
            policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: ["epoch"])))
        var missing = empty.fields; missing.removeValue(forKey: "body")
        XCTAssertThrowsError(try BigSyncRecordRebasePlanner.incomingFields(
            base: base.fields, local: absent.fields, remote: missing,
            policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: []),
            preferRemoteOnConflict: true, localLifetime: nil, remoteLifetime: ""))
    }
    func testPairwiseLegacyExchangeIsSymmetricAndIdempotent() throws {
        let epochs: [String?] = [nil, "", "a", "z", "e\u{0301}", "\u{00E9}", "\u{304B}\u{3099}", "\u{304C}"]
        let states = epochs.enumerated().map { index, epoch in
            State(epoch: epoch, body: String(index), deleted: index % 2 == 0, title: "original")
        }
        for lhs in states {
            for rhs in states {
                let first = try merge(local: lhs, remote: rhs)
                let second = try merge(local: rhs, remote: lhs)
                XCTAssertEqual(first, second)
                XCTAssertEqual(try merge(local: first, remote: second), first)
            }
        }
    }
}
