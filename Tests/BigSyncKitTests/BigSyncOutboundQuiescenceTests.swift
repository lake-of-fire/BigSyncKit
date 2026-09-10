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

    func testSubmissionPersistsExactRecoveryIdentity() throws {
        let (_, peer, _, principal) = fixture()
        let batch = try peer.admit(principal: principal)
        let descriptor = BigSyncOutboundSubmissionRecoveryDescriptor(items: [
            BigSyncOutboundSubmissionItem(
                mutation: .save,
                recordName: "record-1",
                zoneName: "zone",
                zoneOwnerName: "owner",
                recordType: "Article",
                preparedGeneration: "generation-1",
                priorRecordChangeTag: "change-tag-1"
            )
        ])
        try batch.willSubmit(recoveryDescriptor: descriptor)
        let submission = try XCTUnwrap(
            peer.snapshot().outstandingSubmissions.first
        )
        XCTAssertEqual(submission.recoveryDescriptor, descriptor)
        try batch.didSettle()
    }

    func testSubmissionRejectsDuplicateRecoveryRecordIdentity() throws {
        let (_, peer, _, principal) = fixture()
        let batch = try peer.admit(principal: principal)
        let item = BigSyncOutboundSubmissionItem(
            mutation: .save,
            recordName: "record-1",
            zoneName: "zone",
            zoneOwnerName: "owner",
            recordType: "Article",
            preparedGeneration: "generation-1",
            priorRecordChangeTag: nil
        )
        let duplicate = BigSyncOutboundSubmissionRecoveryDescriptor(
            items: [item, item]
        )
        XCTAssertThrowsError(
            try batch.willSubmit(recoveryDescriptor: duplicate)
        ) {
            XCTAssertEqual(
                $0 as? BigSyncOutboundQuiescenceError,
                .invalidState
            )
        }
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
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
        try batch?.noteDefinitiveTransportOutcome()
        // A definitive server result alone is deliberately not settlement.
        // The durable marker protects cancellation/process death before the
        // generation-matched local acknowledgement finishes.
        XCTAssertEqual(try peer.snapshot().outstandingSubmissions.count, 1)
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        let waiting = Task { try await candidate.waitUntilDrained(owner) }
        try await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertThrowsError(try candidate.validateDrained(owner, principal: principal))
        try await batch?.completeLocalResponseProcessingCooperatively()
        XCTAssertTrue(try peer.snapshot().outstandingSubmissions.isEmpty)
        // Clearing the marker after acknowledgement still does not shorten the
        // physical batch scope: the OS admission lives until this lease exits.
        XCTAssertThrowsError(try candidate.validateDrained(owner, principal: principal))
        batch = nil
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
        let preparing = try candidate.snapshot()
        XCTAssertThrowsError(try candidate.resolveOwned(
            owner, expected: preparing, evidenceID: "premature-transition"
        )) {
            XCTAssertEqual($0 as? BigSyncOutboundQuiescenceError, .recoveryRequired)
        }
        XCTAssertEqual(try candidate.snapshot(), preparing)
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

    func testSourcePublicationKeepsPeersFencedAndAllowsOwnerBatches() async throws {
        let (_, peer, candidate, principal) = fixture()
        let owner = try candidate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await candidate.waitUntilDrained(owner)
        try owner.armFinalDrain()
        owner.sealFinalDrain()
        try candidate.requireRecovery(owner)
        let recoveryRequired = try candidate.snapshot()
        let source = try candidate.authorizeSourcePublication(
            owner,
            expected: recoveryRequired,
            evidenceID: "committed-v2-head"
        )
        XCTAssertEqual(source.barrier?.phase, .sourcePublication)
        XCTAssertEqual(source.barrier?.sourcePublicationEvidenceID, "committed-v2-head")
        XCTAssertThrowsError(try peer.admit(principal: principal))
        XCTAssertThrowsError(try candidate.requireRecovery(owner))

        var batch: BigSyncOutboundBatchLease? = try candidate.admit(
            principal: principal,
            owner: owner
        )
        try batch?.willSubmit()
        try batch?.noteDefinitiveTransportOutcome()
        XCTAssertEqual(try candidate.snapshot().outstandingSubmissions.count, 1)
        try await batch?.completeLocalResponseProcessingCooperatively()
        batch = nil
        let published = try candidate.snapshot()
        XCTAssertTrue(published.outstandingSubmissions.isEmpty)
        try candidate.resolveOwned(
            owner,
            expected: published,
            evidenceID: "durable-source-completion"
        )
        XCTAssertNil(try candidate.snapshot().barrier)
        _ = try peer.admit(principal: principal)
    }

    func testSourcePublicationResumeRecoversExactUnknownSubmissionWithoutOpeningPeers() async throws {
        let (_, peer, candidate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try candidate.begin(
            principal: principal,
            writerBarrierEvidenceID: "barrier"
        )
        try await candidate.waitUntilDrained(XCTUnwrap(owner))
        try owner?.armFinalDrain()
        owner?.sealFinalDrain()
        try candidate.requireRecovery(XCTUnwrap(owner))
        let recoveryRequired = try candidate.snapshot()
        _ = try candidate.authorizeSourcePublication(
            XCTUnwrap(owner),
            expected: recoveryRequired,
            evidenceID: "committed-v2-head"
        )
        var sourceBatch: BigSyncOutboundBatchLease? = try candidate.admit(
            principal: principal,
            owner: XCTUnwrap(owner)
        )
        try sourceBatch?.willSubmit()
        sourceBatch = nil
        let ambiguous = try candidate.snapshot()
        let ids = ambiguous.outstandingSubmissions.map(\.identifier)
        XCTAssertEqual(ids.count, 1)
        XCTAssertThrowsError(try candidate.resolveOwned(
            XCTUnwrap(owner),
            expected: ambiguous,
            evidenceID: "must-not-clear-unknown"
        )) {
            XCTAssertEqual(
                $0 as? BigSyncOutboundQuiescenceError,
                .unresolvedSubmissions(ids)
            )
        }
        owner = nil
        XCTAssertThrowsError(try peer.admit(principal: principal))

        let recovery = try peer.takeRecoveryOwnership(expected: ambiguous)
        let resumed = try peer.resumeSourcePublication(
            recovery,
            principal: principal,
            recoveryEvidenceID: "authoritative-source-settlement"
        )
        let resumedState = try peer.snapshot()
        XCTAssertEqual(resumedState.barrier?.phase, .sourcePublication)
        XCTAssertTrue(resumedState.outstandingSubmissions.isEmpty)
        XCTAssertEqual(
            resumedState.lastRecoveryEvidenceID,
            "authoritative-source-settlement"
        )
        XCTAssertThrowsError(try candidate.admit(principal: principal))
        var retry: BigSyncOutboundBatchLease? = try peer.admit(
            principal: principal,
            owner: resumed
        )
        try retry?.willSubmit()
        try retry?.noteDefinitiveTransportOutcome()
        try await retry?.completeLocalResponseProcessingCooperatively()
        retry = nil
        let completed = try peer.snapshot()
        try peer.resolveOwned(
            resumed,
            expected: completed,
            evidenceID: "durable-source-completion"
        )
        _ = try candidate.admit(principal: principal)
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


extension BigSyncOutboundQuiescenceTests {
    func testRestartPromotesCommittedRecoveryCheckpointWithoutOpeningPeers() async throws {
        let (_, peer, gate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try gate.begin(
            principal: principal, writerBarrierEvidenceID: "committed-domain")
        try await gate.waitUntilDrained(XCTUnwrap(owner))
        try gate.requireRecovery(XCTUnwrap(owner))
        let checkpoint = try gate.snapshot()
        owner = nil // Domain commit won; the transport phase write did not run.
        let recovery = try peer.takeRecoveryOwnership(expected: checkpoint)
        let resumed = try peer.resumeSourcePublication(recovery, principal: principal,
            recoveryEvidenceID: "exact-committed-domain-proof")
        XCTAssertEqual(try peer.snapshot().barrier?.phase, .sourcePublication)
        XCTAssertEqual(try peer.snapshot().barrier?.identifier, checkpoint.barrier?.identifier)
        XCTAssertThrowsError(try gate.admit(principal: principal))
        XCTAssertThrowsError(try resumed.armFinalDrain())
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal, owner: resumed)
        try batch?.willSubmit()
        try batch?.noteDefinitiveTransportOutcome()
        try await batch?.completeLocalResponseProcessingCooperatively()
        batch = nil
        try peer.resolveOwned(resumed, expected: peer.snapshot(), evidenceID: "durable-completion")
        _ = try gate.admit(principal: principal)
    }

    func testRestartCannotPromotePreparingOrBorrowChangedPrincipal() async throws {
        let (_, peer, gate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try gate.begin(
            principal: principal, writerBarrierEvidenceID: "not-reserved")
        XCTAssertNotNil(owner)
        owner = nil
        let checkpoint = try gate.snapshot()
        let recovery = try peer.takeRecoveryOwnership(expected: checkpoint)
        XCTAssertThrowsError(try peer.resumeSourcePublication(recovery, principal: principal,
            recoveryEvidenceID: "not-proof-of-a-reservation"))
        XCTAssertEqual(try peer.snapshot(), checkpoint)
        try peer.resolveRecovery(recovery, evidenceID: "exact-preparing-abort")

        owner = try gate.begin(principal: principal, writerBarrierEvidenceID: "committed")
        try await gate.waitUntilDrained(XCTUnwrap(owner))
        try gate.requireRecovery(XCTUnwrap(owner))
        let committed = try gate.snapshot()
        owner = nil
        let held = try peer.takeRecoveryOwnership(expected: committed)
        let changed = BigSyncOutboundPrincipal(durableStateNamespace: "client",
            installationIdentifier: "installation", accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding", accountInvalidationGeneration: 2)
        XCTAssertThrowsError(try peer.resumeSourcePublication(held, principal: changed,
            recoveryEvidenceID: "stale-account-proof"))
        XCTAssertEqual(try peer.snapshot(), committed)
        XCTAssertThrowsError(try gate.admit(principal: principal))
    }

    func testSealedOwnerCannotSubmitPreviouslyPreparedBatch() async throws {
        for sourcePublication in [false, true] {
            let (_, _, gate, principal) = fixture()
            let owner = try gate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
            try await gate.waitUntilDrained(owner)
            if sourcePublication {
                try gate.requireRecovery(owner)
                _ = try gate.authorizeSourcePublication(owner, expected: gate.snapshot(), evidenceID: "committed")
            } else {
                try owner.armFinalDrain()
            }
            let batch = try gate.admit(principal: principal, owner: owner)
            owner.sealOutboundAdmission() // Revoked while adapter preparation was suspended.
            XCTAssertThrowsError(try batch.willSubmit())
            XCTAssertTrue(try gate.snapshot().outstandingSubmissions.isEmpty)
            XCTAssertThrowsError(try gate.admit(principal: principal, owner: owner))
        }
    }

    func testSealedSubmittedOwnerCanFinishPhysicalBookkeepingButCannotResubmit() async throws {
        let (_, peer, gate, principal) = fixture()
        let owner = try gate.begin(principal: principal, writerBarrierEvidenceID: "barrier")
        try await gate.waitUntilDrained(owner)
        try gate.requireRecovery(owner)
        _ = try gate.authorizeSourcePublication(owner, expected: gate.snapshot(), evidenceID: "committed")
        var batch: BigSyncOutboundBatchLease? = try gate.admit(principal: principal, owner: owner)
        try batch?.willSubmit()
        owner.sealOutboundAdmission()
        try batch?.noteDefinitiveTransportOutcome()
        try await batch?.completeLocalResponseProcessingCooperatively()
        XCTAssertTrue(try gate.snapshot().outstandingSubmissions.isEmpty)
        XCTAssertThrowsError(try batch?.willSubmit())
        XCTAssertThrowsError(try peer.takeRecoveryOwnership(expected: peer.snapshot()))
        batch = nil
        XCTAssertThrowsError(try gate.admit(principal: principal, owner: owner))
        try gate.resolveOwned(owner, expected: gate.snapshot(), evidenceID: "durable-completion")
    }

    func testDefinitiveOutcomeLostBeforeLocalHandlingRetainsDurableMarker() async throws {
        let (_, peer, gate, principal) = fixture()
        var batch: BigSyncOutboundBatchLease? = try peer.admit(principal: principal)
        try batch?.willSubmit()
        try batch?.noteDefinitiveTransportOutcome()
        let checkpoint = try peer.snapshot()
        batch = nil // Cancelled callback/process dies before generation-matched ack.
        let owner = try gate.begin(principal: principal, writerBarrierEvidenceID: "cutoff")
        do {
            try await gate.waitUntilDrained(owner)
            XCTFail("Definitive server return cannot stand in for durable local handling")
        } catch let error as BigSyncOutboundQuiescenceError {
            XCTAssertEqual(error, .unresolvedSubmissions(checkpoint.outstandingSubmissions.map(\.identifier)))
        }
        XCTAssertEqual(try gate.snapshot().outstandingSubmissions, checkpoint.outstandingSubmissions)
        try gate.abort(owner)
    }
}
