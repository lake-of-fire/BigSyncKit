from pathlib import Path


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one preimage, found {count}: {old[:90]!r}")
    p.write_text(text.replace(old, new, 1))


coordinator = "Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift"
synchronizer = "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+OutboundQuiescence.swift"
tests = "Tests/BigSyncKitTests/BigSyncOutboundQuiescenceTests.swift"
docs = "Documentation/OutboundQuiescence.md"

replace_one(
    coordinator,
    """        let barrier = try withState { state -> BigSyncOutboundBarrier in
            guard state == recovery.snapshot,
                  let barrier = state.barrier,
                  barrier.phase == .sourcePublication,
                  barrier.principal == principal,
                  barrier.sourcePublicationEvidenceID.map(validEvidence) == true else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            // The host proof covers the exact checkpoint, including every
            // indeterminate source request. Clear only those exact markers while
            // retaining the peer fence and recording the recovery decision.
            try write(BigSyncOutboundQuiescenceSnapshot(
                barrier: barrier,
                submissions: [],
                recoveryEvidenceID: recoveryEvidenceID
            ))
            return barrier
        }
""",
    """        let barrier = try withState { state -> BigSyncOutboundBarrier in
            guard state == recovery.snapshot,
                  var barrier = state.barrier,
                  barrier.principal == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            switch barrier.phase {
            case .recoveryRequired:
                // The domain may have committed its new authority immediately
                // before process death, before the live owner could persist the
                // source-publication phase. The exact host proof authorizes that
                // one-way promotion without ever opening peer admission.
                barrier.phase = .sourcePublication
                barrier.sourcePublicationEvidenceID = recoveryEvidenceID
            case .sourcePublication:
                guard barrier.sourcePublicationEvidenceID.map(validEvidence) == true else {
                    throw BigSyncOutboundQuiescenceError.staleAuthority
                }
            case .preparing:
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            // The host proof covers the exact checkpoint, including every
            // indeterminate source request. Clear only those exact markers while
            // retaining the peer fence and recording the recovery decision.
            try write(BigSyncOutboundQuiescenceSnapshot(
                barrier: barrier,
                submissions: [],
                recoveryEvidenceID: recoveryEvidenceID
            ))
            return barrier
        }
""",
)

replace_one(
    synchronizer,
    """        guard let persistedBarrier = expected.barrier,
              persistedBarrier.phase == .sourcePublication,
              persistedBarrier.principal == principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
""",
    """        guard let persistedBarrier = expected.barrier,
              (persistedBarrier.phase == .recoveryRequired
                || persistedBarrier.phase == .sourcePublication),
              persistedBarrier.principal == principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
""",
)

replace_one(
    synchronizer,
    """    /// Resume a previously committed source-publication phase after process or
    /// worker loss. The host proof must reconcile the exact checkpoint and every
    /// outstanding source submission; unlike generic recovery this retains the
    /// peer fence and returns a new process-local owner token.
""",
    """    /// Resume the committed domain handoff after process or worker loss. An
    /// exact `recoveryRequired` checkpoint can be promoted to source publication
    /// when the domain commit won the crash race; an existing source-publication
    /// checkpoint can be resumed after settling its outstanding submissions.
    /// Unlike generic recovery, neither path opens the peer fence.
""",
)

replace_one(
    tests,
    """    func testSettlementRemovesOnlyItsOwnTicket() throws {
""",
    """    func testSourcePublicationResumePromotesRecoveryRequiredWithoutOpeningPeers() async throws {
        let (_, peer, candidate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try candidate.begin(
            principal: principal,
            writerBarrierEvidenceID: "barrier"
        )
        try await candidate.waitUntilDrained(XCTUnwrap(owner))
        try owner?.armFinalDrain()
        owner?.sealFinalDrain()
        try candidate.requireRecovery(XCTUnwrap(owner))
        let committedDomainCheckpoint = try candidate.snapshot()
        XCTAssertEqual(committedDomainCheckpoint.barrier?.phase, .recoveryRequired)

        // Model process death after the domain has committed its new head/local
        // graph but before the live owner can persist sourcePublication.
        owner = nil
        XCTAssertThrowsError(try peer.admit(principal: principal))
        let recovery = try peer.takeRecoveryOwnership(
            expected: committedDomainCheckpoint
        )
        let resumed = try peer.resumeSourcePublication(
            recovery,
            principal: principal,
            recoveryEvidenceID: "committed-v2-after-restart"
        )
        let resumedState = try peer.snapshot()
        XCTAssertEqual(resumedState.barrier?.phase, .sourcePublication)
        XCTAssertEqual(
            resumedState.barrier?.sourcePublicationEvidenceID,
            "committed-v2-after-restart"
        )
        XCTAssertThrowsError(try candidate.admit(principal: principal))

        var source: BigSyncOutboundBatchLease? = try peer.admit(
            principal: principal,
            owner: resumed
        )
        try source?.willSubmit()
        try source?.noteDefinitiveTransportOutcome()
        try await source?.completeLocalResponseProcessingCooperatively()
        source = nil
        let completed = try peer.snapshot()
        try peer.resolveOwned(
            resumed,
            expected: completed,
            evidenceID: "durable-source-completion"
        )
        _ = try candidate.admit(principal: principal)
    }

    func testSettlementRemovesOnlyItsOwnTicket() throws {
""",
)

replace_one(
    docs,
    """7. **Owner-only source publication:** after the new head/local graph and writer authority are durably committed, take the exact `recoveryRequired` checkpoint and call `beginPostBarrierSourcePublication(token, expected: checkpoint, sourcePublicationEvidenceID:)`. The barrier moves durably to `sourcePublication`: peer/legacy aggregate batches stay blocked, while this exact original principal may run the normal synchronization pipeline to upload and generation-match acknowledge the new source journals. Do **not** arm another aggregate cutoff. Require and retain the ordinary terminal source receipt/certificate needed by the domain completion contract.
""",
    """7. **Owner-only source publication:** after the new head/local graph and writer authority are durably committed, take the exact `recoveryRequired` checkpoint and call `beginPostBarrierSourcePublication(token, expected: checkpoint, sourcePublicationEvidenceID:)`. The barrier moves durably to `sourcePublication`: peer/legacy aggregate batches stay blocked, while this exact original principal may run the normal synchronization pipeline to upload and generation-match acknowledge the new source journals. Do **not** arm another aggregate cutoff. Require and retain the ordinary terminal source receipt/certificate needed by the domain completion contract. If the process dies after the domain commit but before this phase write, `resumePostBarrierSourcePublication(expected:authorizingResume:)` may promote that exact persisted `recoveryRequired` checkpoint after re-proving the committed domain state; it never opens peers during the promotion.
""",
)

replace_one(
    docs,
    """If the persisted barrier is already `sourcePublication`, prefer `resumePostBarrierSourcePublication(expected:authorizingResume:)`: the host must prove the exact committed domain state and settle every outstanding submission in that checkpoint, then BigSyncKit reacquires owner/batch locks, clears only those proven-settled markers, and continues owner-only source publication without opening peers.
""",
    """If the persisted barrier is `recoveryRequired` after the domain commit or is already `sourcePublication`, prefer `resumePostBarrierSourcePublication(expected:authorizingResume:)`: the host must prove the exact committed domain state and settle every outstanding submission in that checkpoint, then BigSyncKit reacquires owner/batch locks, promotes or resumes the source-publication phase, clears only those proven-settled markers, and continues owner-only source publication without opening peers.
""",
)
