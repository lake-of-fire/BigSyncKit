import Foundation
import XCTest
@testable import BigSyncKit

final class BigSyncOutboundQuiescenceTests: XCTestCase {
    private func fixture() -> (URL, BigSyncOutboundQuiescenceCoordinator, BigSyncOutboundQuiescenceCoordinator, BigSyncOutboundPrincipal) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        addTeardownBlock { try? FileManager.default.removeItem(at: directory) }
        let principal = BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "installation",
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 1)
        return (directory,
                BigSyncOutboundQuiescenceCoordinator(sharedStateBaseURL: directory, durableStateNamespace: "client"),
                BigSyncOutboundQuiescenceCoordinator(sharedStateBaseURL: directory, durableStateNamespace: "client"), principal)
    }

    func testPeerScopeMustFinishBeforeExclusiveQuiescence() async throws {
        let (_, peer, candidate, principal) = fixture()
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        XCTAssertThrowsError(try owner.armFinalDrain())
        XCTAssertThrowsError(try peer.admit(principal: principal))
        let waiting = Task { try await candidate.waitUntilDrained(owner) }
        try await Task.sleep(nanoseconds: 25_000_000)
        // A waiting owner is itself not recoverable, even before it has gained
        // the exclusive batch lock. This must also hold in the same process.
        XCTAssertThrowsError(try peer.takeRecoveryOwnership(expected: peer.snapshot()))
        XCTAssertNotNil(batch)
        batch = nil
        try await waiting.value
        try owner.armFinalDrain()
        XCTAssertThrowsError(try peer.admit(principal: principal))
        var final: BigSyncOutboundBatchLease? = try candidate.admit(principal: principal, owner: owner)
        try final?.willSubmit()
        XCTAssertThrowsError(try candidate.validateDrained(owner, principal: principal))
        try final?.didSettle()
        final = nil
        try candidate.validateDrained(owner, principal: principal)
        owner.sealFinalDrain()
        XCTAssertThrowsError(try owner.armFinalDrain())
        XCTAssertThrowsError(try candidate.admit(principal: principal, owner: owner))
        try candidate.abort(owner)
        _ = try peer.admit(principal: principal)
    }

    func testLeaseIncludesAcknowledgementAfterTransportSettles() async throws {
        let (_, peer, candidate, principal) = fixture()
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal)
        try batch?.willSubmit()
        try batch?.didSettle()
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        let waiting = Task { try await candidate.waitUntilDrained(owner) }
        try await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertThrowsError(try candidate.validateDrained(owner, principal: principal))
        batch = nil // Only now did the response/Realm acknowledgement scope exit.
        try await waiting.value
        try candidate.abort(owner)
    }

    func testCancellationLeavesExactFenceAndDoesNotAbortSuccessor() async throws {
        let (_, peer, candidate, principal) = fixture()
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "old")
        let waiting = Task { try await candidate.waitUntilDrained(owner) }
        waiting.cancel()
        do { try await waiting.value; XCTFail("Cancelled acquisition succeeded") }
        catch is CancellationError { }
        XCTAssertThrowsError(try peer.admit(principal: principal))
        try candidate.abort(owner)
        let next = try candidate.begin(principal: principal, writerBarrierEvidenceID: "next")
        XCTAssertThrowsError(try candidate.abort(owner))
        XCTAssertEqual(try peer.snapshot().barrier?.identifier, next.barrier.identifier)
        XCTAssertNotNil(batch)
        batch = nil
        try await candidate.waitUntilDrained(next)
        try candidate.abort(next)
    }

    func testReservationFenceCannotBeAbortedOrRearmed() async throws {
        let (_, _, candidate, principal) = fixture()
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(owner)
        try owner.armFinalDrain()
        owner.sealFinalDrain()
        try candidate.requireRecovery(owner)
        XCTAssertEqual(try candidate.snapshot().barrier?.phase, .recoveryRequired)
        XCTAssertThrowsError(try candidate.abort(owner))
        XCTAssertThrowsError(try owner.armFinalDrain())
        let snapshot = try candidate.snapshot()
        XCTAssertThrowsError(try candidate.resolveOwned(owner, expected: snapshot, evidenceID: ""))
        try candidate.resolveOwned(owner, expected: snapshot, evidenceID: "committed-domain-transition")
        XCTAssertNil(try candidate.snapshot().barrier)
        XCTAssertEqual(try candidate.snapshot().lastRecoveryEvidenceID, "committed-domain-transition")
    }

    func testIdentityReplacementCannotBorrowOwnerCapability() async throws {
        let (_, peer, candidate, principal) = fixture()
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(owner)
        try owner.armFinalDrain()
        let replacements = [
            BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "new-installation", accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 1),
            BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "installation", accountScopeIdentifier: "other", replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 1),
            BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "installation", accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "new-binding", accountInvalidationGeneration: 1),
            BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "installation", accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 2)
        ]
        for replacement in replacements {
            XCTAssertThrowsError(try candidate.admit(principal: replacement, owner: owner))
            XCTAssertThrowsError(try peer.admit(principal: replacement))
        }
        try candidate.abort(owner)
    }

    func testOwnerDropNeverClearsDurableFence() async throws {
        let (_, peer, candidate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(XCTUnwrap(owner))
        let snapshot = try peer.snapshot()
        owner = nil
        XCTAssertThrowsError(try peer.admit(principal: principal))
        let recovery = try peer.takeRecoveryOwnership(expected: snapshot)
        XCTAssertThrowsError(try candidate.admit(principal: principal))
        try peer.resolveRecovery(recovery, evidenceID: "pre-reservation-abort-proof")
        _ = try candidate.admit(principal: principal)
    }

    func testIndeterminateSubmissionSurvivesLeaseDropAndBlocksCertificate() async throws {
        let (_, peer, candidate, principal) = fixture()
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal)
        try batch?.willSubmit()
        batch = nil // Model process death / an indeterminate error. NOT settlement.
        let ids = try peer.snapshot().outstandingSubmissions.map(\.identifier)
        XCTAssertEqual(ids.count, 1)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        do { try await candidate.waitUntilDrained(owner); XCTFail("Unknown server outcome certified") }
        catch let error as BigSyncOutboundQuiescenceError { XCTAssertEqual(error, .unresolvedSubmissions(ids)) }
        XCTAssertThrowsError(try owner.armFinalDrain())
        try candidate.abort(owner)
        // Abort cannot erase an older transport's unknown outcome.
        XCTAssertEqual(try peer.snapshot().outstandingSubmissions.map(\.identifier), ids)
        let recovery = try peer.takeRecoveryOwnership(expected: peer.snapshot())
        try peer.resolveRecovery(recovery, evidenceID: "authoritative-remote-settlement")
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
    }

    func testOwnerAbortCannotReleaseAnActiveFinalBatch() async throws {
        let (_, peer, candidate, principal) = fixture()
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(owner)
        try owner.armFinalDrain()
        var final: BigSyncOutboundBatchLease? = try candidate.admit(principal: principal, owner: owner)
        try final?.willSubmit()
        XCTAssertThrowsError(try candidate.abort(owner))
        XCTAssertThrowsError(try candidate.requireRecovery(owner))
        owner.sealFinalDrain()
        try final?.didSettle()
        XCTAssertThrowsError(try candidate.abort(owner))
        final = nil
        try candidate.abort(owner)
        _ = try peer.admit(principal: principal)
    }

    func testActiveBatchRetainsAbandonedOwnersOSLease() async throws {
        let (_, peer, candidate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(XCTUnwrap(owner))
        try owner?.armFinalDrain()
        var final: BigSyncOutboundBatchLease? = try candidate.admit(principal: principal, owner: owner)
        owner = nil
        XCTAssertThrowsError(try peer.takeRecoveryOwnership(expected: peer.snapshot()))
        XCTAssertNotNil(final)
        final = nil
        let recovery = try peer.takeRecoveryOwnership(expected: peer.snapshot())
        try peer.resolveRecovery(recovery, evidenceID: "exact-domain-abort")
    }

    func testRecoveryRejectsStaleSnapshotAndNeverClearsNewerFence() throws {
        let (_, peer, candidate, principal) = fixture()
        let old = try peer.snapshot()
        var owner: BigSyncOutboundQuiescenceLease? = try candidate.begin(principal: principal, writerBarrierEvidenceID: "new")
        XCTAssertNotNil(owner)
        owner = nil
        XCTAssertThrowsError(try peer.takeRecoveryOwnership(expected: old))
        XCTAssertEqual(try peer.snapshot().barrier?.writerBarrierEvidenceID, "new")
    }

    func testCorruptOrMissingInitializedStateFailsClosed() throws {
        let (_, peer, candidate, principal) = fixture()
        _ = try peer.snapshot()
        let stateURL = peer.directory.appendingPathComponent("state.json")
        let original = try Data(contentsOf: stateURL)
        try Data("not-json".utf8).write(to: stateURL)
        XCTAssertThrowsError(try candidate.admit(principal: principal))
        try original.write(to: stateURL)
        try FileManager.default.removeItem(at: stateURL)
        XCTAssertThrowsError(try candidate.admit(principal: principal))
    }

    func testDifferentClientNamespacesRemainIndependent() async throws {
        let (directory, _, candidate, principal) = fixture()
        let other = BigSyncOutboundQuiescenceCoordinator(sharedStateBaseURL: directory, durableStateNamespace: "other")
        let otherPrincipal = BigSyncOutboundPrincipal(durableStateNamespace: "other", installationIdentifier: "installation",
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 1)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(owner)
        _ = try other.admit(principal: otherPrincipal)
        XCTAssertThrowsError(try other.admit(principal: principal, owner: owner))
        try candidate.abort(owner)
    }

    func testSettlementRemovesOnlyItsOwnTicket() throws {
        let (_, peer, candidate, principal) = fixture()
        let first = try peer.admit(principal: principal)
        let second = try candidate.admit(principal: principal)
        try first.willSubmit()
        let firstID = try XCTUnwrap(peer.snapshot().outstandingSubmissions.first?.identifier)
        try second.willSubmit()
        try first.didSettle()
        let pending = try peer.snapshot().outstandingSubmissions
        XCTAssertEqual(pending.count, 1)
        XCTAssertNotEqual(pending.first?.identifier, firstID)
        try second.didSettle()
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
    }
}

extension BigSyncOutboundQuiescenceTests {
    func testExternalInvalidationInterruptsWaitWhilePeerStillOwnsAdmission() async throws {
        actor Validity {
            var valid = true
            func invalidate() { valid = false }
            func check() throws { if !valid { throw BigSyncOutboundQuiescenceError.staleAuthority } }
        }
        let (_, peer, candidate, principal) = fixture()
        let batch = try peer.admit(principal: principal)
        defer { withExtendedLifetime(batch) {} }
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        let validity = Validity()
        let waiting = Task { try await candidate.waitUntilDrained(owner) { try await validity.check() } }
        await validity.invalidate()
        do { try await waiting.value; XCTFail("Revoked waiter completed") }
        catch let error as BigSyncOutboundQuiescenceError { XCTAssertEqual(error, .staleAuthority) }
        try candidate.abort(owner)
    }

    func testDefinitiveSettlementSurvivesOrchestrationCancellationAndContention() async throws {
        let (_, peer, _, principal) = fixture()
        let batch = try peer.admit(principal: principal)
        try batch.willSubmit()
        var metadata: BigSyncFileLease? = try BigSyncFileLease(at: peer.directory.appendingPathComponent("admission.lock"))
        XCTAssertTrue(try XCTUnwrap(metadata).tryLock(exclusive: true))
        let settlement = Task { try await batch.didSettleCooperatively() }
        settlement.cancel()
        try await Task.sleep(nanoseconds: 10_000_000)
        metadata = nil
        try await settlement.value
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
    }

    func testRecoveryLeaseCannotBeUsedByAnotherNamespace() throws {
        let (directory, peer, _, _) = fixture()
        let other = BigSyncOutboundQuiescenceCoordinator(sharedStateBaseURL: directory, durableStateNamespace: "other")
        let recovery = try peer.takeRecoveryOwnership(expected: peer.snapshot())
        XCTAssertThrowsError(try other.resolveRecovery(recovery, evidenceID: "wrong-namespace"))
        try peer.resolveRecovery(recovery, evidenceID: "correct-namespace")
    }
}
