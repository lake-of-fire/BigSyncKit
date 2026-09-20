import Foundation

/// Read-only transport state for a domain's one-way transition after inbound.
/// This is not a publication receipt or permission to mutate outside the
/// caller's separately validated domain transaction.
public enum BigSyncDomainTransitionReadiness: Sendable, Equatable {
    case ready
    case pendingWork
    case blocked
    case downloadOnly
}

public extension BigSyncBackgroundActor {
    /// Consult the same adapter predicates used by the actual terminal drain,
    /// including unforwarded target journals and semantic publication blockers.
    /// An empty application-side journal query alone is not sufficient.
    @BigSyncBackgroundActor
    func domainTransitionReadiness(
        after context: CloudKitSynchronizer.PrepublicationBoundaryContext
    ) async throws -> BigSyncDomainTransitionReadiness {
        guard let synchronizer = realmSynchronizer else {
            throw CancellationError()
        }
        let result = try await synchronizer.domainTransitionReadiness(after: context)
        guard realmSynchronizer === synchronizer else { throw CancellationError() }
        return result
    }
}

extension CloudKitSynchronizer {
    func domainTransitionReadiness(
        after context: PrepublicationBoundaryContext
    ) async throws -> BigSyncDomainTransitionReadiness {
        try Task.checkCancellation()
        try validateBoundaryContext(context)
        guard activeSynchronizationMode == .sync else { return .downloadOnly }
        // A real configured adapter is required. An empty transport is not
        // evidence that a domain's outstanding target work has been drained.
        guard modelAdapters.count == 1 else { return .blocked }
        var hasSemanticBlockers = false
        for adapter in modelAdapters {
            let blockers = try await adapter.semanticPublicationBlockers()
            try Task.checkCancellation()
            try validateBoundaryContext(context)
            hasSemanticBlockers = hasSemanticBlockers || !blockers.isEmpty
        }
        // Account validation is the last suspension. Read the refreshed target
        // and tracking state afterwards, just as the terminal cutoff does.
        try await revalidateBoundaryContext(context)
        try Task.checkCancellation()
        if try adaptersHavePendingChangesAtTerminalBoundary() {
            return .pendingWork
        }
        return hasSemanticBlockers ? .blocked : .ready
    }
}
