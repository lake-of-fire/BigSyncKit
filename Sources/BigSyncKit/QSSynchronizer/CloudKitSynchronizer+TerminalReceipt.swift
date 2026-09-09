import Foundation

extension CloudKitSynchronizer {
    /// Revalidates an ordinary completed drain without arming an aggregate
    /// cutoff. Source publication after a committed cutover must use this
    /// receipt, not request another aggregate snapshot/cutover capability.
    ///
    /// This is a read-only transport check. The application must separately
    /// compare its current head and domain publication certificate. It grants
    /// no authority to reset a zone, retire a reservation, or acknowledge work.
    @BigSyncBackgroundActor
    public func revalidateTerminalReceipt(_ receipt: SynchronizationReceipt) async throws {
        try validateTerminalReceiptIdentity(receipt)
        guard let context = activeRunContext,
              let lease = try accountScopeLease(),
              lease.accountScopeIdentifier == receipt.accountScopeIdentifier else {
            throw CancellationError()
        }
        try await revalidateRunContext(context)
        try validateAccountScopeLease(lease)
        try validateTerminalReceiptIdentity(receipt)
        guard modelAdapters.count == 1,
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
