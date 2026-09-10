import Foundation

extension CloudKitSynchronizer {
    /// Fence a late/empty installation that has observed an ALREADY accepted
    /// source head. Unlike the aggregate cutoff, this path must not upload a
    /// legacy aggregate or mint a CompletedPostBarrierDrain. The host first
    /// installs its durable local writer barrier and begins the ordinary exact
    /// outbound quiescence token. Its proof below must re-read the accepted head
    /// and its original domain operation; a head/evidence label is not proof.
    ///
    /// Existing physical batches must finish and every uncertainty marker must
    /// be settled before the proof runs. Success seals `recoveryRequired` BEFORE
    /// host archive/adoption/local-bootstrap writes. It does not grant source
    /// uploads: those still need the committed-domain proof and the existing
    /// begin/resumePostBarrierSourcePublication boundary. Failure never reopens
    /// peers or retires the host's local writer pause.
    @discardableResult
    public func sealPostBarrierQuiescenceForAcceptedHead(
        _ token: PostBarrierOutboundQuiescence,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingAcceptedHead: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> Void
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundEstablishmentID == nil,
              postBarrierDrainAuthorization == nil, completedPostBarrierDrain == nil,
              outboundRecoveryID == nil,
              let owner = matchingOutboundOwner(token) else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let requestID = UUID()
        let attemptID = synchronizationAttemptID
        postBarrierOutboundEstablishmentID = requestID
        defer {
            if postBarrierOutboundEstablishmentID == requestID {
                postBarrierOutboundEstablishmentID = nil
            }
        }

        @BigSyncBackgroundActor
        func validateOwnership() throws {
            try Task.checkCancellation()
            try revalidatingDomainOwner()
            guard self.synchronizationAttemptID == attemptID,
                  self.postBarrierOutboundEstablishmentID == requestID,
                  !self.syncing, !self.synchronizationDrainIsActive,
                  self.postBarrierDrainAuthorization == nil,
                  self.completedPostBarrierDrain == nil,
                  self.outboundRecoveryID == nil,
                  self.matchingOutboundOwner(token) === owner else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            _ = try self.validatePostBarrierOutboundQuiescence(token)
            guard owner.barrier.phase == .preparing else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
        }

        try validateOwnership()
        // A lost OS lease cannot by itself settle an old CloudKit submission.
        try await outboundQuiescenceCoordinator.waitUntilDrained(owner,
            revalidating: validateOwnership)
        try validateOwnership()
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == token.principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try outboundQuiescenceCoordinator.validateDrained(owner, principal: token.principal)
        let before = try validatePostBarrierOutboundQuiescence(token)
        try await authorizingAcceptedHead(before)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account,
              try validatePostBarrierOutboundQuiescence(token) == before else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try outboundQuiescenceCoordinator.validateDrained(owner, principal: token.principal)
        // No suspension separates exact admission from the durable seal. This
        // deliberately does NOT arm final aggregate admission or create a run,
        // receipt, source-publication proof, or acknowledgement.
        try outboundQuiescenceCoordinator.requireRecovery(owner)
        return try outboundQuiescenceCoordinator.snapshot()
    }
}

extension BigSyncBackgroundActor {
    @BigSyncBackgroundActor
    @discardableResult
    public func sealPostBarrierQuiescenceForAcceptedHead(
        _ token: CloudKitSynchronizer.PostBarrierOutboundQuiescence,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingAcceptedHead: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> Void
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        let result = try await synchronizer.sealPostBarrierQuiescenceForAcceptedHead(
            token,
            revalidatingDomainOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                try revalidatingDomainOwner()
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            },
            authorizingAcceptedHead: authorizingAcceptedHead
        )
        guard realmSynchronizer === synchronizer else {
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw CancellationError()
        }
        try Task.checkCancellation()
        try revalidatingDomainOwner()
        guard realmSynchronizer === synchronizer else {
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw CancellationError()
        }
        guard try synchronizer.validatePostBarrierOutboundQuiescence(token) == result else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return result
    }
}
