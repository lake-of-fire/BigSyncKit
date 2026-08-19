import Foundation

/// Durable authority for creating account-scoped domain facts while CloudKit
/// is temporarily unreachable.
///
/// `invalidationGeneration` is an ownership epoch. It changes only when
/// account authority is durably invalidated, not on routine validation of the
/// same account. Callers must revalidate the complete value at their final
/// local commit boundary.
public struct BigSyncAccountScopeLease: Sendable, Equatable {
    public let accountScopeIdentifier: String
    public let invalidationGeneration: Int64
    public let validatedAt: Date

    public init(
        accountScopeIdentifier: String,
        invalidationGeneration: Int64,
        validatedAt: Date
    ) {
        self.accountScopeIdentifier = accountScopeIdentifier
        self.invalidationGeneration = invalidationGeneration
        self.validatedAt = validatedAt
    }
}

public enum BigSyncAccountScopeInvalidationReason: Int, Sendable, Equatable {
    case accountChanged
    case accountReplaced
    case restoreDetected
}

public enum BigSyncAccountScopeLeaseError: Error, LocalizedError, Equatable {
    case unavailable
    case stale
    case corrupt

    public var errorDescription: String? {
        switch self {
        case .unavailable:
            return "No validated CloudKit account-scope lease is available."
        case .stale:
            return "The CloudKit account-scope lease was invalidated."
        case .corrupt:
            return "The durable CloudKit account-scope lease is malformed."
        }
    }
}
