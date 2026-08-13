import Foundation
import CloudKit

/// A small, redacted account-scoped view of CloudKit synchronization health.
///
/// This is diagnostic state only.  It never participates in mutation
/// discovery, cursor advancement, retry scheduling, or zone recovery.  The
/// account scope is a SHA-256 digest, not an iCloud record name.
public struct CloudKitSyncHealthSnapshot: Sendable, Equatable {
    public enum Category: String, Sendable, Codable {
        case idle
        case syncing
        case succeeded
        case transientRetry
        case notAuthenticated
        case accountTemporarilyUnavailable
        case higherModelVersion
        case terminalZoneUnavailable
        case failed
    }

    public let category: Category
    public let accountScopeIdentifier: String
    public let lastSuccessAt: Date?
    public let lastFailureAt: Date?
    public let retryNotBefore: Date?
    public let terminalZoneDeletionKind: CloudKitZoneDeletionKind?
    public let updatedAt: Date

    public init(
        category: Category,
        accountScopeIdentifier: String,
        lastSuccessAt: Date? = nil,
        lastFailureAt: Date? = nil,
        retryNotBefore: Date? = nil,
        terminalZoneDeletionKind: CloudKitZoneDeletionKind? = nil,
        updatedAt: Date
    ) {
        self.category = category
        self.accountScopeIdentifier = accountScopeIdentifier
        self.lastSuccessAt = lastSuccessAt
        self.lastFailureAt = lastFailureAt
        self.retryNotBefore = retryNotBefore
        self.terminalZoneDeletionKind = terminalZoneDeletionKind
        self.updatedAt = updatedAt
    }

    fileprivate init?(propertyList: [String: Any]) {
        guard let categoryRawValue = propertyList["category"] as? String,
              let category = Category(rawValue: categoryRawValue),
              let accountScopeIdentifier = propertyList["accountScopeIdentifier"] as? String,
              let updatedAt = propertyList["updatedAt"] as? Date else {
            return nil
        }
        self.init(
            category: category,
            accountScopeIdentifier: accountScopeIdentifier,
            lastSuccessAt: propertyList["lastSuccessAt"] as? Date,
            lastFailureAt: propertyList["lastFailureAt"] as? Date,
            retryNotBefore: propertyList["retryNotBefore"] as? Date,
            terminalZoneDeletionKind: (propertyList[
                "terminalZoneDeletionKind"
            ] as? String).flatMap(CloudKitZoneDeletionKind.init(rawValue:)),
            updatedAt: updatedAt
        )
    }

    fileprivate var propertyList: [String: Any] {
        var result: [String: Any] = [
            "category": category.rawValue,
            "accountScopeIdentifier": accountScopeIdentifier,
            "updatedAt": updatedAt,
        ]
        result["lastSuccessAt"] = lastSuccessAt
        result["lastFailureAt"] = lastFailureAt
        result["retryNotBefore"] = retryNotBefore
        result["terminalZoneDeletionKind"] =
            terminalZoneDeletionKind?.rawValue
        return result
    }
}

public extension Notification.Name {
    /// Posted after an actor-fenced durable sync-health transition.
    static let SynchronizerSyncHealthDidChange = Notification.Name(
        "QSCloudKitSynchronizerSyncHealthDidChangeNotification"
    )
}

public let cloudKitSynchronizerSyncHealthSnapshotKey =
    "CloudKitSynchronizerSyncHealthSnapshotKey"

extension CloudKitSynchronizer {
    private var syncHealthSnapshotKey: String {
        durableStateKey("CloudKitSyncHealth.v2")
    }

    private func persistedSyncHealthSnapshot() -> CloudKitSyncHealthSnapshot? {
        guard let propertyList = keyValueStore.object(forKey: syncHealthSnapshotKey)
            as? [String: Any] else {
            return nil
        }
        return CloudKitSyncHealthSnapshot(propertyList: propertyList)
    }

    /// Returns health only after proving it belongs to the currently active
    /// iCloud account.  A snapshot from a previous account is never exposed.
    @BigSyncBackgroundActor
    public func syncHealthSnapshot() async throws -> CloudKitSyncHealthSnapshot? {
        let accountIdentifier = try await accountIdentifierProvider()
        let accountScopeIdentifier = Self.accountScopeIdentifier(for: accountIdentifier)
        guard let snapshot = persistedSyncHealthSnapshot(),
              snapshot.accountScopeIdentifier == accountScopeIdentifier else {
            return nil
        }
        return snapshot
    }

    /// Records only a start or terminal state after the caller has validated
    /// the owning attempt/run context.  Retaining timestamps across categories
    /// makes a restart useful without storing an account identifier or token.
    internal func recordSyncHealth(
        _ category: CloudKitSyncHealthSnapshot.Category,
        context: RunContext,
        retryNotBefore: Date? = nil,
        terminalZoneDeletionKind: CloudKitZoneDeletionKind? = nil,
        now: Date = Date()
    ) throws {
        try checkRunContext(context)
        let previous = persistedSyncHealthSnapshot().flatMap {
            $0.accountScopeIdentifier == context.accountScopeIdentifier ? $0 : nil
        }
        let didSucceed = category == .succeeded
        let didFail = category != .idle && category != .syncing && !didSucceed
        let snapshot = CloudKitSyncHealthSnapshot(
            category: category,
            accountScopeIdentifier: context.accountScopeIdentifier,
            lastSuccessAt: didSucceed ? now : previous?.lastSuccessAt,
            lastFailureAt: didFail ? now : previous?.lastFailureAt,
            retryNotBefore: category == .transientRetry ? retryNotBefore : nil,
            terminalZoneDeletionKind: terminalZoneDeletionKind,
            updatedAt: now
        )
        try keyValueStore.bigSyncSetDurably(
            value: snapshot.propertyList,
            forKey: syncHealthSnapshotKey
        )
        postNotification(
            .SynchronizerSyncHealthDidChange,
            userInfo: [cloudKitSynchronizerSyncHealthSnapshotKey: snapshot]
        )
    }

    internal func syncHealthCategory(for error: Error) -> CloudKitSyncHealthSnapshot.Category {
        if error is CancellationError {
            return .idle
        }
        if let error = error as? SyncError {
            switch error {
            case .notAuthenticated:
                return .notAuthenticated
            case .higherModelVersionFound:
                return .higherModelVersion
            case .cancelled:
                return .idle
            }
        }
        if error is ChangeFeedMigrationError {
            return .terminalZoneUnavailable
        }
        if let error = error as? CKError, error.code == .notAuthenticated {
            return .notAuthenticated
        }
        return .failed
    }

#if DEBUG
    @BigSyncBackgroundActor
    internal func _test_recordSyncHealth(
        _ category: CloudKitSyncHealthSnapshot.Category,
        accountIdentifier: String,
        retryNotBefore: Date? = nil,
        now: Date
    ) throws {
        let context = RunContext(
            attemptID: synchronizationAttemptID,
            runID: synchronizationRunID,
            accountIdentifier: accountIdentifier,
            accountScopeIdentifier: Self.accountScopeIdentifier(for: accountIdentifier)
        )
        activeRunContext = context
        cancelSync = false
        try recordSyncHealth(
            category,
            context: context,
            retryNotBefore: retryNotBefore,
            now: now
        )
    }
#endif
}
