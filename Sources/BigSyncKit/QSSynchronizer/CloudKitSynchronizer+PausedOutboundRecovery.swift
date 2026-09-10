import Foundation

extension CloudKitSynchronizer {
    /// Recover physical ownership of an exact sealed reservation while leaving
    /// ALL outbound admission disabled. This is for finishing/adopting a domain
    /// graph that is not yet ready for `beginPostBarrierSourcePublication`.
    ///
    /// The required proof must validate the original operation/principal and
    /// reconcile every unresolved submission, including generation-matched local
    /// response handling. An empty journal, a fetch, or a disappeared OS lock is
    /// not settlement evidence. The proof must not reopen domain/legacy writers.
    ///
    /// The returned acquisition retains `recoveryRequired`: it cannot arm an
    /// aggregate drain or admit a source batch. After the host durably commits
    /// its source-only graph, use the existing source-publication handoff. On
    /// failure abandon this exact token without reopening the durable fence.
    public func acquirePostBarrierRecoveryOwnership(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        try Task.checkCancellation()
        try revalidatingDomainOwner()
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, postBarrierOutboundTicket == nil,
              postBarrierDrainAuthorization == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundPrincipal()
        guard let barrier = expected.barrier,
              barrier.phase == .recoveryRequired,
              barrier.principal == principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }

        @BigSyncBackgroundActor
        func validateOwnership() throws {
            try Task.checkCancellation()
            try revalidatingDomainOwner()
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
        let evidence = try await authorizingRecovery(expected)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let owner = try outboundQuiescenceCoordinator.retainPausedRecoveryOwnership(
            recovery, principal: principal, recoveryEvidenceID: evidence)
        let token = PostBarrierOutboundQuiescence(
            identifier: owner.barrier.identifier,
            writerBarrierEvidenceID: owner.barrier.writerBarrierEvidenceID,
            issuerID: synchronizationReceiptIssuerID, principal: principal)
        postBarrierOutboundLease = owner
        postBarrierOutboundTicket = token
        do {
            try validateOwnership()
            _ = try validatePostBarrierOutboundQuiescence(token)
        } catch {
            abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }
}

extension BigSyncBackgroundActor {
    /// Same paused recovery handoff, additionally fenced to the worker that
    /// was installed before the proof and account-provider suspensions.
    @BigSyncBackgroundActor
    public func acquirePostBarrierRecoveryOwnership(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> CloudKitSynchronizer.PostBarrierOutboundQuiescence {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        let token = try await synchronizer.acquirePostBarrierRecoveryOwnership(
            expected: expected,
            revalidatingDomainOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                try revalidatingDomainOwner()
                // A synchronous host callback can replace the installed worker.
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            },
            authorizingRecovery: authorizingRecovery)
        do {
            try Task.checkCancellation()
            guard realmSynchronizer === synchronizer else { throw CancellationError() }
            try revalidatingDomainOwner()
            guard realmSynchronizer === synchronizer else { throw CancellationError() }
            _ = try synchronizer.validatePostBarrierOutboundQuiescence(token)
        } catch {
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }
}
