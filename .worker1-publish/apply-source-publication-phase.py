from pathlib import Path


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one preimage, found {count}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1))


coordinator = "Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift"
synchronizer = "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+OutboundQuiescence.swift"
worker = "Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift"
tests = "Tests/BigSyncKitTests/BigSyncOutboundQuiescenceTests.swift"
docs = "Documentation/OutboundQuiescence.md"

replace_one(
    coordinator,
    """        case recoveryRequired
    }
    public let identifier: UUID
    public let writerBarrierEvidenceID: String
    public let principal: BigSyncOutboundPrincipal
    public internal(set) var phase: Phase
""",
    """        case recoveryRequired
        /// The committed domain has switched authority, but peer aggregate
        /// writers remain fenced while this exact principal publishes source
        /// journals through the ordinary upload/acknowledgement pipeline.
        case sourcePublication
    }
    public let identifier: UUID
    public let writerBarrierEvidenceID: String
    public let principal: BigSyncOutboundPrincipal
    public internal(set) var phase: Phase
    /// Durable host evidence that authority was committed before owner-only
    /// source publication was enabled. Nil in earlier barrier phases.
    public internal(set) var sourcePublicationEvidenceID: String? = nil
""",
)

replace_one(
    coordinator,
    """                guard owner.allowsFinalDrain else { throw BigSyncOutboundQuiescenceError.blocked }
""",
    """                let mayUpload =
                    (owner.allowsFinalDrain && owner.barrier.phase == .preparing)
                    || (owner.allowsSourcePublication && owner.barrier.phase == .sourcePublication)
                guard mayUpload else { throw BigSyncOutboundQuiescenceError.blocked }
""",
)

replace_one(
    coordinator,
    """    /// Persist this before ANY domain reservation write, not after it.
    func requireRecovery(_ owner: BigSyncOutboundQuiescenceLease) throws {
        try owner.withLock {
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            try withState { state in
                var barrier = owner.barrier
                barrier.phase = .recoveryRequired
                try write(BigSyncOutboundQuiescenceSnapshot(barrier: barrier,
                    submissions: state.outstandingSubmissions, recoveryEvidenceID: state.lastRecoveryEvidenceID))
                owner.barrier = barrier
                owner.allowsFinalDrain = false
            }
        }
    }
""",
    """    /// Persist this before ANY domain reservation write, not after it.
    func requireRecovery(_ owner: BigSyncOutboundQuiescenceLease) throws {
        try owner.withLock {
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            guard owner.barrier.phase != .sourcePublication else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            if owner.barrier.phase == .recoveryRequired {
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = false
                return
            }
            try withState { state in
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\\.identifier))
                }
                var barrier = owner.barrier
                barrier.phase = .recoveryRequired
                barrier.sourcePublicationEvidenceID = nil
                try write(BigSyncOutboundQuiescenceSnapshot(barrier: barrier,
                    submissions: state.outstandingSubmissions, recoveryEvidenceID: state.lastRecoveryEvidenceID))
                owner.barrier = barrier
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = false
            }
        }
    }

    /// After the host durably commits the new authority, keep the same exclusive
    /// transport owner but permit ordinary source-journal batches. Peer/legacy
    /// batches remain fenced by the persisted barrier. This never arms another
    /// aggregate cutoff or manufactures a terminal receipt.
    func authorizeSourcePublication(
        _ owner: BigSyncOutboundQuiescenceLease,
        expected: BigSyncOutboundQuiescenceSnapshot,
        evidenceID: String
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        try owner.withLock {
            guard validEvidence(evidenceID) else { throw BigSyncOutboundQuiescenceError.invalidState }
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.barrier.phase == .recoveryRequired else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            return try withState { state in
                guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\\.identifier))
                }
                var barrier = owner.barrier
                barrier.phase = .sourcePublication
                barrier.sourcePublicationEvidenceID = evidenceID
                let updated = BigSyncOutboundQuiescenceSnapshot(
                    barrier: barrier,
                    submissions: [],
                    recoveryEvidenceID: state.lastRecoveryEvidenceID
                )
                try write(updated)
                owner.barrier = barrier
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = true
                return updated
            }
        }
    }
""",
)

replace_one(
    coordinator,
    """    func resolveRecovery(_ recovery: BigSyncOutboundRecoveryLease, evidenceID: String) throws {
        guard recovery.directory == directory, validEvidence(evidenceID), !recovery.closed else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try recovery.owner?.validateIdentity()
        try recovery.batches?.validateIdentity()
        try withState { state in
            guard state == recovery.snapshot else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
        }
        recovery.close()
    }
""",
    """    func resolveRecovery(_ recovery: BigSyncOutboundRecoveryLease, evidenceID: String) throws {
        guard recovery.directory == directory, validEvidence(evidenceID), !recovery.closed else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try recovery.owner?.validateIdentity()
        try recovery.batches?.validateIdentity()
        try withState { state in
            guard state == recovery.snapshot else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
        }
        recovery.close()
    }

    /// Convert exact crash/restart recovery ownership into owner-only source
    /// publication without ever opening peer admission. The caller's durable
    /// proof authorizes settlement of every uncertainty marker in `snapshot`;
    /// the barrier itself remains installed under the original principal.
    func resumeSourcePublication(
        _ recovery: BigSyncOutboundRecoveryLease,
        principal: BigSyncOutboundPrincipal,
        recoveryEvidenceID: String
    ) throws -> BigSyncOutboundQuiescenceLease {
        guard recovery.directory == directory, validEvidence(recoveryEvidenceID),
              validPrincipal(principal), !recovery.closed else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try recovery.owner?.validateIdentity()
        try recovery.batches?.validateIdentity()
        let barrier = try withState { state -> BigSyncOutboundBarrier in
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
        guard let ownership = recovery.owner, let batches = recovery.batches else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        recovery.owner = nil
        recovery.batches = nil
        recovery.closed = true
        let owner = BigSyncOutboundQuiescenceLease(
            coordinator: self,
            ownerLease: ownership,
            barrier: barrier
        )
        owner.batchLease = batches
        owner.allowsSourcePublication = true
        return owner
    }
""",
)

replace_one(
    coordinator,
    """            guard owner.barrier.phase == .recoveryRequired else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            try withState { state in
                guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
                try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
            }
""",
    """            guard owner.barrier.phase == .recoveryRequired
                    || owner.barrier.phase == .sourcePublication else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            try withState { state in
                guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\\.identifier))
                }
                try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
            }
""",
)

replace_one(
    coordinator,
    """                  state.barrier.map({ validEvidence($0.writerBarrierEvidenceID) && validPrincipal($0.principal) }) ?? true,
""",
    """                  state.barrier.map(validBarrier) ?? true,
""",
)

replace_one(
    coordinator,
    """    private func validPrincipal(_ principal: BigSyncOutboundPrincipal) -> Bool {
""",
    """    private func validBarrier(_ barrier: BigSyncOutboundBarrier) -> Bool {
        guard validEvidence(barrier.writerBarrierEvidenceID),
              validPrincipal(barrier.principal) else { return false }
        switch barrier.phase {
        case .preparing, .recoveryRequired:
            return barrier.sourcePublicationEvidenceID == nil
        case .sourcePublication:
            return barrier.sourcePublicationEvidenceID.map(validEvidence) == true
        }
    }

    private func validPrincipal(_ principal: BigSyncOutboundPrincipal) -> Bool {
""",
)

replace_one(
    coordinator,
    """    fileprivate var activeBatches = 0
    fileprivate var closed = false
    fileprivate var allowsFinalDrain = false
    private var hasArmedFinalDrain = false
""",
    """    fileprivate var activeBatches = 0
    fileprivate var closed = false
    fileprivate var allowsFinalDrain = false
    fileprivate var allowsSourcePublication = false
    private var hasArmedFinalDrain = false
""",
)

replace_one(
    coordinator,
    """    func sealFinalDrain() { withLock { allowsFinalDrain = false } }

    fileprivate func close() {
        closed = true; allowsFinalDrain = false
        batchLease = nil; ownerLease = nil
    }
""",
    """    func sealFinalDrain() { withLock { allowsFinalDrain = false } }

    func sealOutboundAdmission() {
        withLock {
            allowsFinalDrain = false
            allowsSourcePublication = false
        }
    }

    fileprivate func close() {
        closed = true
        allowsFinalDrain = false
        allowsSourcePublication = false
        batchLease = nil
        ownerLease = nil
    }
""",
)

replace_one(
    synchronizer,
    """    /// Relinquish only this token's pre-reservation fence. Domain cancellation
""",
    """    /// After the host has durably committed the new authority/bootstrap, allow
    /// this exact live owner to publish source journals while every peer remains
    /// fenced. The returned checkpoint is the new durable source-publication
    /// phase and should be retained for crash/restart recovery.
    @discardableResult
    public func beginPostBarrierSourcePublication(
        _ token: PostBarrierOutboundQuiescence,
        expected: BigSyncOutboundQuiescenceSnapshot,
        sourcePublicationEvidenceID: String
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierDrainAuthorization == nil, outboundRecoveryID == nil,
              let owner = matchingOutboundOwner(token),
              try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return try outboundQuiescenceCoordinator.authorizeSourcePublication(
            owner,
            expected: expected,
            evidenceID: sourcePublicationEvidenceID
        )
    }

    /// Relinquish only this token's pre-reservation fence. Domain cancellation
""",
)

replace_one(
    synchronizer,
    """        guard let owner = matchingOutboundOwner(token) else { return false }
        owner.sealFinalDrain()
        retireOutboundCapabilities(token)
""",
    """        guard let owner = matchingOutboundOwner(token) else { return false }
        owner.sealOutboundAdmission()
        retireOutboundCapabilities(token)
""",
)

replace_one(
    synchronizer,
    """    /// Crash/account/restart recovery. Ownership is held across the host's
""",
    """    /// Resume a previously committed source-publication phase after process or
    /// worker loss. The host proof must reconcile the exact checkpoint and every
    /// outstanding source submission; unlike generic recovery this retains the
    /// peer fence and returns a new process-local owner token.
    public func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        let token = try await resumePostBarrierSourcePublication(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in },
            authorizingResume: authorizingResume
        )
        do {
            try Task.checkCancellation()
        } catch {
            abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }

    internal func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, postBarrierOutboundTicket == nil,
              postBarrierDrainAuthorization == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundPrincipal()
        guard let persistedBarrier = expected.barrier,
              persistedBarrier.phase == .sourcePublication,
              persistedBarrier.principal == principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            try revalidatingExternalOwner()
            guard outboundRecoveryID == requestID,
                  synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundPrincipal() == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        try validateOwnership()
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let evidence = try await authorizingResume(expected)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        let owner = try outboundQuiescenceCoordinator.resumeSourcePublication(
            recovery,
            principal: principal,
            recoveryEvidenceID: evidence
        )
        let token = PostBarrierOutboundQuiescence(
            identifier: owner.barrier.identifier,
            writerBarrierEvidenceID: owner.barrier.writerBarrierEvidenceID,
            issuerID: synchronizationReceiptIssuerID,
            principal: principal
        )
        postBarrierOutboundLease = owner
        postBarrierOutboundTicket = token
        return token
    }

    /// Crash/account/restart recovery. Ownership is held across the host's
""",
)

replace_one(
    synchronizer,
    """            if let authorization = postBarrierDrainAuthorization,
               let identifier = authorization.outboundQuiescenceIdentifier {
                guard postBarrierOutboundTicket?.identifier == identifier else {
                    throw BigSyncOutboundQuiescenceError.staleAuthority
                }
                owner = postBarrierOutboundLease
            } else { owner = nil }
""",
    """            if let authorization = postBarrierDrainAuthorization,
               let identifier = authorization.outboundQuiescenceIdentifier {
                guard postBarrierOutboundTicket?.identifier == identifier else {
                    throw BigSyncOutboundQuiescenceError.staleAuthority
                }
                owner = postBarrierOutboundLease
            } else if let ticket = postBarrierOutboundTicket,
                      let sourceOwner = postBarrierOutboundLease,
                      sourceOwner.barrier.identifier == ticket.identifier,
                      sourceOwner.barrier.phase == .sourcePublication {
                // Post-bootstrap source publication uses the ordinary sync and
                // terminal receipt pipeline, but only this durable barrier owner
                // may enter outbound preparation while peers remain blocked.
                owner = sourceOwner
            } else {
                owner = nil
            }
""",
)

replace_one(
    worker,
    """    @BigSyncBackgroundActor
    @discardableResult
    public func abortPostBarrierOutboundQuiescence(
""",
    """    @BigSyncBackgroundActor
    @discardableResult
    public func beginPostBarrierSourcePublication(
        _ token: CloudKitSynchronizer.PostBarrierOutboundQuiescence,
        expected: BigSyncOutboundQuiescenceSnapshot,
        sourcePublicationEvidenceID: String
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        return try synchronizer.beginPostBarrierSourcePublication(
            token,
            expected: expected,
            sourcePublicationEvidenceID: sourcePublicationEvidenceID
        )
    }

    @BigSyncBackgroundActor
    public func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> CloudKitSynchronizer.PostBarrierOutboundQuiescence {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        let token = try await synchronizer.resumePostBarrierSourcePublication(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else {
                    throw CancellationError()
                }
            },
            authorizingResume: authorizingResume
        )
        guard realmSynchronizer === synchronizer else {
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw CancellationError()
        }
        do {
            try Task.checkCancellation()
        } catch {
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func abortPostBarrierOutboundQuiescence(
""",
)

replace_one(
    tests,
    """    func testSettlementRemovesOnlyItsOwnTicket() throws {
""",
    """    func testSourcePublicationKeepsPeersFencedAndAllowsOwnerBatches() async throws {
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
        let ids = ambiguous.outstandingSubmissions.map(\\.identifier)
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
""",
)

replace_one(
    docs,
    """6. **After bootstrap:** use the existing `revalidatePostBarrierDrainPrincipal` / `validatePostBarrierDrainPrincipal` for continuity only; new source journals intentionally invalidate the pre-bootstrap empty-journal witness. These methods do not authorize reservation or publication.
7. **Explicit transition/recovery:** after step 5 has durably sealed transport as `recoveryRequired` and the domain durably commits a decision that makes transport reopening safe, take an exact `outboundQuiescenceSnapshot()` and call `resolvePostBarrierOutboundQuiescence(token, expected: snapshot, recoveryEvidenceID:)`. A live owner still in `preparing` cannot use resolution as a shortcut around the reservation/recovery seal; use exact pre-reservation abort when rolling back instead. For an activated transition, all current-release writers **and outbound preparation** must now select only the new authority; legacy aggregate work must be retired or fenced before reopening. The library does not infer this from a string or flip a feature flag. It records the supplied durable evidence identifier and clears only the exact expected gate state. Alternatively leave the gate closed until recovery completes.
""",
    """6. **After bootstrap:** use the existing `revalidatePostBarrierDrainPrincipal` / `validatePostBarrierDrainPrincipal` for continuity only; new source journals intentionally invalidate the pre-bootstrap empty-journal witness. These methods do not authorize reservation or publication.
7. **Owner-only source publication:** after the new head/local graph and writer authority are durably committed, take the exact `recoveryRequired` checkpoint and call `beginPostBarrierSourcePublication(token, expected: checkpoint, sourcePublicationEvidenceID:)`. The barrier moves durably to `sourcePublication`: peer/legacy aggregate batches stay blocked, while this exact original principal may run the normal synchronization pipeline to upload and generation-match acknowledge the new source journals. Do **not** arm another aggregate cutoff. Require and retain the ordinary terminal source receipt/certificate needed by the domain completion contract.
8. **Durable completion and release:** only after source publication/acknowledgement and the domain's completion state are durably committed, take the exact clean source-publication checkpoint and call `resolvePostBarrierOutboundQuiescence(token, expected: snapshot, recoveryEvidenceID:)`. Live resolution refuses outstanding uncertainty markers. A live owner still in `preparing` cannot use resolution as a shortcut around the reservation/recovery seal; use exact pre-reservation abort when rolling back instead. Peer transport reopens only at this explicit release.
""",
)

replace_one(
    docs,
    """`abandonPostBarrierOutboundQuiescence(token)` drops local ownership without changing durable state. Actual batch scopes retain the OS lease until they unwind. After those scopes exit, a fresh worker may acquire recovery ownership. Old completed/armed capabilities cannot authorize a successor. A dropped owner, a process exit, a suspended task, or elapsed time never clears durable cutoff state.
""",
    """`abandonPostBarrierOutboundQuiescence(token)` drops local ownership without changing durable state. Actual batch scopes retain the OS lease until they unwind. After those scopes exit, a fresh worker may acquire recovery ownership. If the persisted barrier is already `sourcePublication`, prefer `resumePostBarrierSourcePublication(expected:authorizingResume:)`: the host must prove the exact committed domain state and settle every outstanding submission in that checkpoint, then BigSyncKit reacquires owner/batch locks, clears only those proven-settled markers, and continues owner-only source publication without opening peers. Old completed/armed capabilities cannot authorize a successor. A dropped owner, a process exit, a suspended task, or elapsed time never clears durable cutoff state.
""",
)
