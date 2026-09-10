import Foundation

extension CloudKitSynchronizer {
    /// Validate a live acquisition after a host-owned suspension. This checks
    /// the exact token, current principal and persisted barrier, not an empty
    /// journal or completed drain. A preparing owner may still be waiting for
    /// peer batches. The returned checkpoint is read-only transport evidence.
    @discardableResult
    public func validatePostBarrierOutboundQuiescence(
        _ token: PostBarrierOutboundQuiescence
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let owner = matchingOutboundOwner(token),
              try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return try owner.withLock {
            try owner.validateAcquisition(principal: token.principal, requiresDrained: false)
            let checkpoint = try outboundQuiescenceCoordinator.snapshot()
            guard checkpoint.barrier == owner.barrier else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            return checkpoint
        }
    }

    /// Query the real account provider as well as the durable principal. Pair
    /// with the synchronous validator after a domain read/transaction. Neither
    /// method authorizes a reservation, source publication, or gate release.
    @discardableResult
    public func revalidatePostBarrierOutboundQuiescence(
        _ token: PostBarrierOutboundQuiescence
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        _ = try validatePostBarrierOutboundQuiescence(token)
        let attemptID = synchronizationAttemptID
        let account = try await accountIdentifierProvider()
        guard synchronizationAttemptID == attemptID,
              Self.accountScopeIdentifier(for: account) == token.principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return try validatePostBarrierOutboundQuiescence(token)
    }

    /// Validate the ordinary source receipt against the exact still-held owner.
    /// A pre-bootstrap receipt, an ordinary unrelated receipt, or a receipt from
    /// an abandoned acquisition cannot authorize this publication's completion.
    /// The host must separately validate its committed head/graph and evidence.
    @discardableResult
    public func validatePostBarrierSourcePublication(
        using receipt: SynchronizationReceipt,
        ownedBy token: PostBarrierOutboundQuiescence
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        _ = try validatePostBarrierOutboundQuiescence(token)
        try validateTerminalReceipt(receipt)
        guard let context = activeRunContext,
              context.sourcePublicationOwnershipID == token.ownershipID else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try validateSourcePublicationRun(context, requiresDrained: true)
        return try validatePostBarrierOutboundQuiescence(token)
    }

    /// Account-provider revalidation for source publication. New domain writes,
    /// worker/run replacement, cancellation or a resumed owner invalidate the
    /// old receipt before it can be consumed by the host's completion write.
    @discardableResult
    public func revalidatePostBarrierSourcePublication(
        using receipt: SynchronizationReceipt,
        ownedBy token: PostBarrierOutboundQuiescence
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        _ = try validatePostBarrierSourcePublication(using: receipt, ownedBy: token)
        try await revalidateTerminalReceipt(receipt)
        return try validatePostBarrierSourcePublication(using: receipt, ownedBy: token)
    }

    /// Final non-suspending release AFTER the host's durable completion write.
    /// Requires the source receipt for this exact acquisition and an unchanged,
    /// drained transport checkpoint. No host rows or pending generations are
    /// acknowledged here. The evidence ID refers to a host-committed decision;
    /// it is not inferred from the receipt. A failed/uncertain release requires
    /// fresh inspection, not cancellation or unconditional gate clearing.
    public func completePostBarrierSourcePublication(
        _ token: PostBarrierOutboundQuiescence,
        using receipt: SynchronizationReceipt,
        expected: BigSyncOutboundQuiescenceSnapshot,
        completionEvidenceID: String
    ) throws {
        guard try validatePostBarrierSourcePublication(using: receipt, ownedBy: token) == expected else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try resolvePostBarrierOutboundQuiescence(
            token, expected: expected, recoveryEvidenceID: completionEvidenceID
        )
    }

    /// Recovery with a host-owned synchronous admission check at every async
    /// boundary, including AFTER the final account lookup and immediately before
    /// the durable change. Use an existing domain admission/generation token;
    /// this callback is read-only and must not create a second durable journal.
    /// The proof callback still owns authoritative settlement of all requests.
    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        try await recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: revalidatingDomainOwner,
            authorizingRecovery: authorizingRecovery
        )
    }

    /// Resume/promote source publication with the same final host-admission
    /// check. If this wrapper loses ownership before returning the new token,
    /// only that acquisition is abandoned; the durable peer fence is preserved.
    public func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        let token = try await resumePostBarrierSourcePublication(
            expected: expected,
            revalidatingExternalOwner: revalidatingDomainOwner,
            authorizingResume: authorizingResume
        )
        do {
            try revalidatingDomainOwner()
            _ = try validatePostBarrierOutboundQuiescence(token)
        } catch {
            abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }
}

extension BigSyncBackgroundActor {
    @BigSyncBackgroundActor
    @discardableResult
    public func validatePostBarrierOutboundQuiescence(
        _ token: CloudKitSynchronizer.PostBarrierOutboundQuiescence
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        return try synchronizer.validatePostBarrierOutboundQuiescence(token)
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func revalidatePostBarrierOutboundQuiescence(
        _ token: CloudKitSynchronizer.PostBarrierOutboundQuiescence
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        _ = try await synchronizer.revalidatePostBarrierOutboundQuiescence(token)
        guard realmSynchronizer === synchronizer else { throw CancellationError() }
        return try synchronizer.validatePostBarrierOutboundQuiescence(token)
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func validatePostBarrierSourcePublication(
        using receipt: CloudKitSynchronizer.SynchronizationReceipt,
        ownedBy token: CloudKitSynchronizer.PostBarrierOutboundQuiescence
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        return try synchronizer.validatePostBarrierSourcePublication(using: receipt, ownedBy: token)
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func revalidatePostBarrierSourcePublication(
        using receipt: CloudKitSynchronizer.SynchronizationReceipt,
        ownedBy token: CloudKitSynchronizer.PostBarrierOutboundQuiescence
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        _ = try await synchronizer.revalidatePostBarrierSourcePublication(using: receipt, ownedBy: token)
        guard realmSynchronizer === synchronizer else { throw CancellationError() }
        return try synchronizer.validatePostBarrierSourcePublication(using: receipt, ownedBy: token)
    }

    @BigSyncBackgroundActor
    public func completePostBarrierSourcePublication(
        _ token: CloudKitSynchronizer.PostBarrierOutboundQuiescence,
        using receipt: CloudKitSynchronizer.SynchronizationReceipt,
        expected: BigSyncOutboundQuiescenceSnapshot,
        completionEvidenceID: String
    ) throws {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        try synchronizer.completePostBarrierSourcePublication(
            token, using: receipt, expected: expected, completionEvidenceID: completionEvidenceID
        )
    }

    @BigSyncBackgroundActor
    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        try await synchronizer.recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                try revalidatingDomainOwner()
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            },
            authorizingRecovery: authorizingRecovery
        )
        guard realmSynchronizer === synchronizer else { throw CancellationError() }
        try Task.checkCancellation()
    }

    @BigSyncBackgroundActor
    public func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingDomainOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> CloudKitSynchronizer.PostBarrierOutboundQuiescence {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        let token = try await synchronizer.resumePostBarrierSourcePublication(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                try revalidatingDomainOwner()
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            },
            authorizingResume: authorizingResume
        )
        do {
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
