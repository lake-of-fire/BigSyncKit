from pathlib import Path


def replace_exact(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one preimage, found {count}")
    p.write_text(text.replace(old, new, 1))


replace_exact(
    "Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift",
    """        guard realmSynchronizer === synchronizer else {
            synchronizer.revokePostBarrierDrainAuthorization(authorization)
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw CancellationError()
        }
        try Task.checkCancellation()
        return authorization
""",
    """        guard realmSynchronizer === synchronizer else {
            synchronizer.revokePostBarrierDrainAuthorization(authorization)
            synchronizer.abandonPostBarrierOutboundQuiescence(token)
            throw CancellationError()
        }
        do {
            try Task.checkCancellation()
        } catch {
            // The caller did not receive this capability. Do not leave the
            // owner-only final drain armed merely because cancellation landed
            // after the synchronizer finished establishment.
            synchronizer.revokePostBarrierDrainAuthorization(authorization)
            throw error
        }
        return authorization
""",
)

replace_exact(
    "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+OutboundQuiescence.swift",
    """    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundRecoveryPrincipal()
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            guard outboundRecoveryID == requestID, synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundRecoveryPrincipal() == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let evidence = try await authorizingRecovery(expected)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.resolveRecovery(recovery, evidenceID: evidence)
    }
""",
    """    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        try await recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in },
            authorizingRecovery: authorizingRecovery
        )
    }

    /// Internal host wrapper used when another lifecycle owner (for example the
    /// shared background worker) must remain current across every suspension.
    /// The external owner is rechecked after the final account lookup and
    /// immediately before the durable gate can be reopened.
    internal func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundRecoveryPrincipal()
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            try revalidatingExternalOwner()
            guard outboundRecoveryID == requestID, synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundRecoveryPrincipal() == principal else {
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
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.resolveRecovery(recovery, evidenceID: evidence)
    }
""",
)

replace_exact(
    "Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift",
    """        try await synchronizer.recoverOutboundQuiescence(expected: expected) { @BigSyncBackgroundActor checkpoint in
            guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            let evidence = try await authorizingRecovery(checkpoint)
            // Unlike a post-return check, this prevents the displaced worker
            // from durably reopening transport after a delayed domain proof.
            guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
            try Task.checkCancellation()
            return evidence
        }
""",
    """        try await synchronizer.recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                guard self.realmSynchronizer === synchronizer else {
                    throw CancellationError()
                }
            },
            authorizingRecovery: { @BigSyncBackgroundActor checkpoint in
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                let evidence = try await authorizingRecovery(checkpoint)
                guard self.realmSynchronizer === synchronizer else { throw CancellationError() }
                try Task.checkCancellation()
                return evidence
            }
        )
""",
)
