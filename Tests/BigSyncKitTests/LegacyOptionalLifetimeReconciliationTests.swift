import Foundation
import XCTest
@testable import BigSyncKit

/// Executes the unchanged outer planner with opaque digest-equality inputs.
final class LegacyOptionalLifetimeReconciliationTests: XCTestCase {
    private struct State {
        let epoch: String?
        let text: String
        let deleted: Bool
        var fields: [String: Data] {
            ["epoch": epoch.map { Data([1]) + Data($0.utf8) } ?? Data([0]),
             "body": Data(text.utf8), "isDeleted": Data([deleted ? 1 : 0]),
             "title": Data("title".utf8)]
        }
    }
    private let base = State(epoch: "base", text: "base", deleted: false)
    private let absent = State(epoch: nil, text: "absent", deleted: false)
    private let empty = State(epoch: "", text: "empty", deleted: true)
    private let bundle: Set<String> = ["epoch", "body", "isDeleted"]
    private func plan(base: State?, local: State, remote: State,
                      pending: Bool = true, retained: Bool = true,
                      preferRemote: Bool = false) throws -> BigSyncRecordReconciliationResult {
        let policy = BigSyncRecordRebasePolicy.lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"])
        let contract = BigSyncRecordContract(policy: policy, deletion: retained ? .retained : .physical)
        return try BigSyncRecordReconciliationPlanner.plan(base: base?.fields,
            local: local.fields, remote: remote.fields, policy: policy,
            contract: contract, pending: pending, existing: true,
            localDeleted: local.deleted, remoteDeleted: remote.deleted,
            localLifetime: local.epoch, remoteLifetime: remote.epoch,
            preferRemote: preferRemote)
    }
    private func transition(_ result: BigSyncRecordReconciliationResult) throws -> BigSyncRecordTransition {
        guard case let .commit(value) = result else {
            XCTFail("Expected an ordinary adopted-record transition")
            throw CocoaError(.coderInvalidValue)
        }
        return value
    }
    func testRetainedPendingPlannerChoosesOneCompleteOptionalLifetimeBundle() throws {
        for preferRemote in [false, true] {
            let left = try transition(plan(base: base, local: absent, remote: empty, preferRemote: preferRemote))
            let right = try transition(plan(base: base, local: empty, remote: absent, preferRemote: preferRemote))
            XCTAssertEqual(left.incomingFields.intersection(bundle), bundle)
            XCTAssertTrue(right.incomingFields.isDisjoint(with: bundle))
            XCTAssertTrue(left.acceptsIncomingBaseline)
            XCTAssertTrue(right.acceptsIncomingBaseline)
        }
    }
    func testMissingComparisonBaseStillRequiresExplicitResolution() throws {
        for (local, remote) in [(absent, empty), (empty, absent)] {
            guard case .needsResolution = try plan(base: nil, local: local, remote: remote) else {
                return XCTFail("Legacy ordering must not invent missing comparison evidence")
            }
        }
    }
    func testPhysicalPendingDeletionKeepsItsExistingFence() throws {
        guard case .preservePhysicalDeletion = try plan(base: base, local: empty,
            remote: absent, retained: false, preferRemote: true) else {
            return XCTFail("This fix must not replace physical deletion policy")
        }
    }
    func testWithoutPendingLocalIntentIncomingLegacyStateStillApplies() throws {
        let selected = try transition(plan(base: base, local: empty, remote: absent, pending: false))
        XCTAssertEqual(selected.incomingFields, Set(absent.fields.keys))
        XCTAssertTrue(selected.acceptsIncomingBaseline)
    }
    func testBaseProvenChangedAbsenceBeatsUnchangedPresentEmptyEpoch() throws {
        let selected = try transition(plan(base: empty, local: empty, remote: absent))
        XCTAssertEqual(selected.incomingFields.intersection(bundle), bundle)
    }
    func testOrderedResetStillBeatsLegacyStateWithAndWithoutPendingIntent() throws {
        let next = try BigSyncLifetimeID.next(after: nil,
            nonce: UUID(uuidString: "00000000-0000-0000-0000-000000000001")!)
        let versioned = State(epoch: next, text: "newer", deleted: false)
        for pending in [false, true] {
            for legacy in [absent, empty] {
                let selected = try transition(plan(base: base, local: versioned, remote: legacy,
                    pending: pending, preferRemote: true))
                XCTAssertTrue(selected.incomingFields.isDisjoint(with: bundle))
            }
        }
    }
    func testKeepingLocalBundleStillAcceptsObservedServerBaseline() throws {
        let selected = try transition(plan(base: base, local: empty, remote: absent))
        XCTAssertTrue(selected.acceptsIncomingBaseline)
        XCTAssertTrue(selected.incomingFields.isDisjoint(with: bundle))
    }
}
