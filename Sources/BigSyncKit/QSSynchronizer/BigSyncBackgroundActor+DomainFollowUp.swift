import Foundation

public extension BigSyncBackgroundActor {
    /// Requests another inbound/upload pass within the current full drain.
    ///
    /// A domain may change a local-only admission declaration at prepublication
    /// without creating a mutation journal row. That change still needs a new
    /// pass under the new declaration before publication. Use the existing tail
    /// drain flag rather than recursively awaiting `synchronizeCloudKit()` from
    /// its own callback, manufacturing upload work, or scheduling an unfenced
    /// task against a potentially replaced worker.
    ///
    /// Returns false for download-only work: it cannot grant upload/cutover
    /// authority or silently promote a download request to a full drain.
    @BigSyncBackgroundActor
    @discardableResult
    func requestFollowUpSynchronization(
        after context: CloudKitSynchronizer.PrepublicationBoundaryContext
    ) throws -> Bool {
        guard let realmSynchronizer else { throw CancellationError() }
        return try realmSynchronizer.requestFollowUpSynchronization(after: context)
    }
}

extension CloudKitSynchronizer {
    @discardableResult
    func requestFollowUpSynchronization(
        after context: PrepublicationBoundaryContext
    ) throws -> Bool {
        // Validate first, including on the download-only path. An obsolete
        // callback is never permission to act on the current worker/run.
        try Task.checkCancellation()
        try validateBoundaryContext(context)
        guard activeSynchronizationMode == .sync else { return false }
        synchronizationRequestedWhileRunning = true
        return true
    }
}
