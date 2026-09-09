import Foundation

extension CloudKitSynchronizer {
    /// Checks the exact completed transport boundary without suspending. Use
    /// after an application certificate read to close the final reentrancy gap.
    /// This grants no authority to reset a zone, retire a reservation, or
    /// acknowledge work. Domain/head certificates still need separate checks.
    @BigSyncBackgroundActor
    public func validateTerminalReceipt(_ receipt: SynchronizationReceipt) throws {
        try validateTerminalReceiptIdentity(receipt)
        guard let lease = try accountScopeLease(),
              lease.accountScopeIdentifier == receipt.accountScopeIdentifier,
              modelAdapters.count == 1,
              let adapter = modelAdapters.first,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: receipt.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: receipt.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
              ) == receipt.consumedServerBoundaryIdentifier,
              try !adaptersHavePendingChangesAtTerminalBoundary() else {
            throw CancellationError()
        }
        try keyValueStore.bigSyncValidateDurability()
        if let domainScope = receipt.domainPublicationScopeIdentifier {
            // A live receipt is not sufficient after its durable publication
            // evidence has been revoked or the change-feed namespace changed.
            // Reuse the transport evidence check without restoring readiness,
            // acknowledging work, or issuing a replacement receipt.
            guard let context = activeRunContext,
                  let evidence = try publicationEvidenceForUnconsumedFetch(context: context),
                  evidence.runID == receipt.runID,
                  evidence.domainScopeIdentifier == domainScope else {
                throw CancellationError()
            }
        }
        try validateAccountScopeLease(lease)
        try validateTerminalReceiptIdentity(receipt)
    }

    /// Revalidates an ordinary completed drain, including the actual account
    /// provider, without arming an aggregate cutoff. A newer run, binding,
    /// account invalidation, or worker cannot adopt an older receipt.
    @BigSyncBackgroundActor
    public func revalidateTerminalReceipt(_ receipt: SynchronizationReceipt) async throws {
        try validateTerminalReceipt(receipt)
        guard let context = activeRunContext,
              let lease = try accountScopeLease() else { throw CancellationError() }
        try await revalidateRunContext(context)
        try validateAccountScopeLease(lease)
        try validateTerminalReceipt(receipt)
    }

    @BigSyncBackgroundActor
    private func validateTerminalReceiptIdentity(_ receipt: SynchronizationReceipt) throws {
        try Task.checkCancellation()
        guard receipt.issuerID == synchronizationReceiptIssuerID,
              receipt.authorizationID == activeReceiptAuthorizationID,
              !syncing, !synchronizationDrainIsActive,
              let context = activeRunContext,
              context.runID == receipt.runID,
              context.accountIdentifier == receipt.accountIdentifier,
              context.accountScopeIdentifier == receipt.accountScopeIdentifier,
              context.replicaBindingGenerationIdentifier == receipt.replicaBindingGenerationIdentifier else {
            throw CancellationError()
        }
        try checkRunContext(context)
    }
}

extension BigSyncBackgroundActor {
    @BigSyncBackgroundActor
    public func validateTerminalReceipt(
        _ receipt: CloudKitSynchronizer.SynchronizationReceipt
    ) throws {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        try synchronizer.validateTerminalReceipt(receipt)
    }

    /// A worker replacement during account validation cannot validate an old
    /// worker's receipt on behalf of its successor.
    @BigSyncBackgroundActor
    public func revalidateTerminalReceipt(
        _ receipt: CloudKitSynchronizer.SynchronizationReceipt
    ) async throws {
        guard let synchronizer = realmSynchronizer else { throw CancellationError() }
        try await synchronizer.revalidateTerminalReceipt(receipt)
        guard realmSynchronizer === synchronizer else { throw CancellationError() }
        try Task.checkCancellation()
    }
}
