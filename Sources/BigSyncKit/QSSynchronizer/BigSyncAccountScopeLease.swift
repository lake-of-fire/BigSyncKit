import CryptoKit
import CoreFoundation
import Foundation

/// Durable transport authority retained while CloudKit is temporarily
/// unreachable. Transitional account-scoped domain writers also use this lease
/// until they move to the stable local-dataset identity.
///
/// `invalidationGeneration` is an ownership epoch. It changes only when
/// account authority is durably invalidated, not on routine validation of the
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

/// Typed scalar vocabulary shared by the two v1 account-authority envelopes.
/// These are local property-list fields, not CloudKit's Boolean transport.
private enum BigSyncPersistedAccountAuthorityValue {
    static func nonnegativeInteger(_ raw: Any?) -> Int64? {
        guard let number = raw as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID() else { return nil }
        switch String(cString: number.objCType) {
        case "c", "C", "s", "S", "i", "I", "l", "L", "q", "Q":
            guard let value = Int64(exactly: number), value >= 0 else { return nil }
            return value
        default:
            return nil
        }
    }

    static func boolean(_ raw: Any?) -> Bool? {
        guard let number = raw as? NSNumber,
              CFGetTypeID(number) == CFBooleanGetTypeID() else { return nil }
        return number.boolValue
    }
}

/// Detached decoding of the existing lease envelope. An absent store value
/// remains generation zero; malformed present data must never become absence.
struct BigSyncPersistedAccountScopeLease {
    let generation: Int64
    let lease: BigSyncAccountScopeLease?

    init(generation: Int64, lease: BigSyncAccountScopeLease?) {
        self.generation = generation
        self.lease = lease
    }

    init(persistedValue raw: Any) throws {
        guard let value = raw as? [String: Any],
              BigSyncPersistedAccountAuthorityValue.nonnegativeInteger(value["version"]) == 1,
              let generation = BigSyncPersistedAccountAuthorityValue.nonnegativeInteger(value["generation"]),
              let isValid = BigSyncPersistedAccountAuthorityValue.boolean(value["isValid"]) else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        guard isValid else {
            // Invalidation retains its ordering generation, not spendable
            // authority. Ignore old payload fields exactly as before.
            self.init(generation: generation, lease: nil)
            return
        }
        guard let account = value["accountScopeIdentifier"] as? String,
              !account.isEmpty,
              let date = value["validatedAt"] as? Date,
              date.timeIntervalSinceReferenceDate.isFinite else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        self.init(generation: generation, lease: BigSyncAccountScopeLease(
            accountScopeIdentifier: account,
            invalidationGeneration: generation,
            validatedAt: date
        ))
    }
}

public enum BigSyncAccountScopeInvalidationReason: Int, Sendable, Equatable {
    case accountChanged
    case accountReplaced
    case restoreDetected
}

    static func load(
        store: any KeyValueStore,
        key: String
    ) throws -> BigSyncReplicaBindingSnapshot? {
        guard let raw = try store.bigSyncDurableObject(forKey: key) else {
            return nil
        }
        return try decode(raw)
    }

    private static func decode(_ raw: Any) throws -> BigSyncReplicaBindingSnapshot {
        guard let value = raw as? [String: Any],
              BigSyncPersistedAccountAuthorityValue.nonnegativeInteger(value["version"]) == Int64(version),
              let installationIdentityDigest = validDigest(
                value["installationIdentityDigest"]
              ),
              let activeGenerationIdentifier =
                validDigest(value["activeGenerationIdentifier"]) else {
            throw BigSyncReplicaBindingError.corrupt
        }
        // Missing means genuinely unbound. A present malformed owner must
        // not authorize first binding, or erase the owner during restore.
        func accountIdentifier(for key: String) throws -> String? {
            guard let raw = value[key] else { return nil }
            guard let identifier = raw as? String, !identifier.isEmpty else {
                throw BigSyncReplicaBindingError.corrupt
            }
            return identifier
        }
        let activeAccountScopeIdentifier = try accountIdentifier(
            for: "activeAccountScopeIdentifier"
        )
        let restoredDatasetOwnerAccountScopeIdentifier = try accountIdentifier(
            for: "restoredDatasetOwnerAccountScopeIdentifier"
        )
        guard activeAccountScopeIdentifier == nil
                || restoredDatasetOwnerAccountScopeIdentifier == nil else {
            throw BigSyncReplicaBindingError.corrupt
        }

        let pendingKeys = [
            "pendingTransitionID",
            "pendingBindingGenerationIdentifier",
            "pendingSourceAccountScopeIdentifier",
            "pendingDestinationAccountScopeIdentifier",
                  ),
                  let sourceAccountScopeIdentifier =
                    value["pendingSourceAccountScopeIdentifier"] as? String,
                  !sourceAccountScopeIdentifier.isEmpty,
                  let destinationAccountScopeIdentifier = value[
                    "pendingDestinationAccountScopeIdentifier"
                  ] as? String,
                  !destinationAccountScopeIdentifier.isEmpty,
                  sourceAccountScopeIdentifier != destinationAccountScopeIdentifier,
                  let detectedAt = value["pendingDetectedAt"] as? Date,
                  detectedAt.timeIntervalSinceReferenceDate.isFinite else {
                throw BigSyncReplicaBindingError.corrupt
            }
            pendingPort = BigSyncCloudAccountPortRequirement(
                transitionID: transitionID,
                bindingGenerationIdentifier:
                    bindingGenerationIdentifier,
                sourceAccountScopeIdentifier:
                    sourceAccountScopeIdentifier,
            value["pendingBindingGenerationIdentifier"] =
                pendingPort.bindingGenerationIdentifier
            value["pendingSourceAccountScopeIdentifier"] =
                pendingPort.sourceAccountScopeIdentifier
            value["pendingDestinationAccountScopeIdentifier"] =
                pendingPort.destinationAccountScopeIdentifier
            value["pendingDetectedAt"] = pendingPort.detectedAt
        }
        // Keep writes and reads on the same format contract without changing
        // the v1 representation, keys, or transition policy.
        _ = try decode(value)
        try store.bigSyncSetDurably(value: value, forKey: key)
    }

    private static func validDigest(_ value: Any?) -> String? {
        guard let value = value as? String,
              value.utf8.count == 64,
              value.utf8.allSatisfy({ byte in
                (48...57).contains(byte) || (97...102).contains(byte)
