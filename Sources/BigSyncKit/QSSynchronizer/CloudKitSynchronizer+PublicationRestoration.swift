import Foundation

extension CloudKitSynchronizer {
    /// Inspection may return nil because another run superseded it. Such a
    /// result is not evidence that the new run's domain state is invalid.
    /// Retain the existing callback barrier through both positive and negative
    /// delivery, so replacement startup cannot overtake a suspended handler.
    /// Returns false when a successor superseded inspection before delivery.
    @discardableResult
    @BigSyncBackgroundActor
    func restoreDurablePublicationEvidence(
        deliveringTo handler: @Sendable (BigSyncDurablePublicationEvidence?) async throws -> Void
    ) async throws -> Bool {
        let attemptID = synchronizationAttemptID
        let evidence = try await restoredDurablePublicationEvidence()
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID,
              !syncing,
              !synchronizationDrainIsActive,
              beginRunCallback(for: attemptID) else { return false }
        defer { endRunCallback() }
        try await handler(evidence)
        return true
    }
}
