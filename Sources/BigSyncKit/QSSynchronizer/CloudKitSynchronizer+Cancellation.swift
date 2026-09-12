import Foundation

extension CloudKitSynchronizer {
    /// A thrown CancellationError does not imply that cancelSynchronization()
    /// already settled this drain. Only its owner may invoke that existing,
    /// non-suspending cleanup path. Obsolete callbacks are harmless no-ops.
    @BigSyncBackgroundActor
    func settleCancellation(ifOwnedBy attemptID: UUID) {
        guard synchronizationAttemptID == attemptID else { return }
        cancelSynchronization()
    }
}
