import CryptoKit
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
        guard let value = raw as? [String: Any],
              (value["version"] as? NSNumber)?.intValue == version,
              let installationIdentityDigest = validDigest(
                value["installationIdentityDigest"]
              ),
              let activeGenerationIdentifier =
                validDigest(value["activeGenerationIdentifier"]) else {
            throw BigSyncReplicaBindingError.corrupt
        }
        let activeAccountScopeIdentifier =
            value["activeAccountScopeIdentifier"] as? String
        if activeAccountScopeIdentifier?.isEmpty == true {
            throw BigSyncReplicaBindingError.corrupt
        }
        let restoredDatasetOwnerAccountScopeIdentifier =
            value["restoredDatasetOwnerAccountScopeIdentifier"] as? String
        if restoredDatasetOwnerAccountScopeIdentifier?.isEmpty == true
            || (
                activeAccountScopeIdentifier != nil
                    && restoredDatasetOwnerAccountScopeIdentifier != nil
            ) {
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
                  let detectedAt = value["pendingDetectedAt"] as? Date else {
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
        try store.bigSyncSetDurably(value: value, forKey: key)
    }

    private static func validDigest(_ value: Any?) -> String? {
        guard let value = value as? String,
              value.utf8.count == 64,
              value.utf8.allSatisfy({ byte in
                (48...57).contains(byte) || (97...102).contains(byte)
