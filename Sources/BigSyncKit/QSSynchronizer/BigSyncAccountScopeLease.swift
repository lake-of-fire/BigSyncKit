import CryptoKit
import Foundation

/// Durable transport authority retained while CloudKit is temporarily
/// unreachable. Transitional account-scoped domain writers also use this lease
/// until they move to the stable local-dataset identity.
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

/// Controls what BigSync does after proving that the private CloudKit account
/// differs from the account bound to its durable synchronization state.
public enum BigSyncCloudAccountReplacementPolicy: Sendable, Equatable {
    /// Rebuild tracking from the replacement account's server state.
    ///
    /// This preserves BigSyncKit's historical behavior and is appropriate for
    /// clients that treat iCloud accounts as independent data namespaces.
    case serverReconciliation

    /// Keep the admitted local dataset and rebuild an empty replacement
    /// account replica from its durable mutation journal and live local rows.
    /// No old CloudKit zone is copied or deleted.
    case localDatasetRebootstrap

    /// Stop before touching the replacement account's replica.
    ///
    /// Clients with one stable local dataset use this policy while they run an
    /// explicit local-wins port/reseed protocol outside an ordinary sync run.
    case requireExplicitDatasetPort

    var usesDatasetReplicaBinding: Bool {
        self != .serverReconciliation
    }
}

/// Immutable transport evidence supplied before the very first CloudKit
/// replica binding is admitted.
///
/// The callback owns application-specific dataset-head discovery and its
/// durable local state. BigSync revalidates the synchronization attempt,
/// exact account, and unchanged binding generation after the callback returns
/// and before it publishes the account binding or starts ordinary sync work.
public struct BigSyncInitialReplicaBindingContext: Sendable, Equatable {
    public let accountScopeIdentifier: String
    public let replicaBindingGenerationIdentifier: String

    public init(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String
    ) {
        self.accountScopeIdentifier = accountScopeIdentifier
        self.replicaBindingGenerationIdentifier =
            replicaBindingGenerationIdentifier
    }
}

/// Durable evidence that the current local dataset needs a deliberate port to
/// a different private CloudKit account.
public struct BigSyncCloudAccountPortRequirement: Sendable, Equatable {
    public let transitionID: UUID
    public let bindingGenerationIdentifier: String
    public let sourceAccountScopeIdentifier: String
    public let destinationAccountScopeIdentifier: String
    public let detectedAt: Date

    public init(
        transitionID: UUID,
        bindingGenerationIdentifier: String,
        sourceAccountScopeIdentifier: String,
        destinationAccountScopeIdentifier: String,
        detectedAt: Date
    ) {
        self.transitionID = transitionID
        self.bindingGenerationIdentifier = bindingGenerationIdentifier
        self.sourceAccountScopeIdentifier = sourceAccountScopeIdentifier
        self.destinationAccountScopeIdentifier =
            destinationAccountScopeIdentifier
        self.detectedAt = detectedAt
    }
}

public enum BigSyncReplicaBindingError: Error, LocalizedError, Equatable {
    case corrupt
    case accountMismatch

    public var errorDescription: String? {
        switch self {
        case .corrupt:
            return "The durable CloudKit replica binding is malformed."
        case .accountMismatch:
            return "The CloudKit replica binding belongs to another account."
        }
    }
}

/// One consistent identity sample used to create a durable mutation row.
/// Sampling these fields together prevents a restore from pairing an old
/// installation with a newly rotated replica binding.
public struct BigSyncMutationJournalIdentity: Sendable, Equatable {
    public let installationIdentifier: String
    public let replicaBindingGenerationIdentifier: String?

    public init(
        installationIdentifier: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        self.installationIdentifier = installationIdentifier
        self.replicaBindingGenerationIdentifier =
            replicaBindingGenerationIdentifier
    }
}

struct BigSyncReplicaBindingSnapshot: Sendable, Equatable {
    let installationIdentityDigest: String
    let activeGenerationIdentifier: String
    let activeAccountScopeIdentifier: String?
    /// Exact owner retained while a restored installation awaits fresh
    /// transport admission. This is current restore-transition state, not
    /// compatibility with an unshipped binding format.
    let restoredDatasetOwnerAccountScopeIdentifier: String?
    let pendingPort: BigSyncCloudAccountPortRequirement?

    var mutationGenerationIdentifier: String {
        pendingPort?.bindingGenerationIdentifier
            ?? activeGenerationIdentifier
    }

    var datasetOwnerAccountScopeIdentifier: String? {
        activeAccountScopeIdentifier
            ?? restoredDatasetOwnerAccountScopeIdentifier
    }
}

enum BigSyncReplicaBindingStateStore {
    static let version = 1

    static func prepare(
        store: any KeyValueStore,
        key: String,
        installationIdentifier: String
    ) throws -> BigSyncReplicaBindingSnapshot {
        guard !installationIdentifier.isEmpty else {
            throw BigSyncReplicaBindingError.corrupt
        }
        if let existing = try load(store: store, key: key) {
            let currentInstallationDigest = digest(
                domain: "replica-binding-installation",
                fields: [installationIdentifier]
            )
            guard existing.installationIdentityDigest
                    != currentInstallationDigest else {
                return existing
            }
            let rotated = BigSyncReplicaBindingSnapshot(
                installationIdentityDigest: currentInstallationDigest,
                activeGenerationIdentifier: digest(
                    domain: "restored-replica-binding",
                    fields: [installationIdentifier]
                ),
                activeAccountScopeIdentifier: nil,
                restoredDatasetOwnerAccountScopeIdentifier:
                    existing.datasetOwnerAccountScopeIdentifier,
                pendingPort: nil
            )
            try persist(rotated, store: store, key: key)
            return rotated
        }
        let snapshot = BigSyncReplicaBindingSnapshot(
            installationIdentityDigest: digest(
                domain: "replica-binding-installation",
                fields: [installationIdentifier]
            ),
            activeGenerationIdentifier: digest(
                domain: "initial-replica-binding",
                fields: [installationIdentifier]
            ),
            activeAccountScopeIdentifier: nil,
            restoredDatasetOwnerAccountScopeIdentifier: nil,
            pendingPort: nil
        )
        try persist(snapshot, store: store, key: key)
        return snapshot
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
            "pendingDetectedAt",
        ]
        let presentPendingKeys = pendingKeys.filter { value[$0] != nil }
        let pendingPort: BigSyncCloudAccountPortRequirement?
        if presentPendingKeys.isEmpty {
            pendingPort = nil
        } else {
            guard presentPendingKeys.count == pendingKeys.count,
                  let transitionIDValue =
                    value["pendingTransitionID"] as? String,
                  let transitionID = UUID(uuidString: transitionIDValue),
                  let bindingGenerationIdentifier = validDigest(
                    value["pendingBindingGenerationIdentifier"]
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
                destinationAccountScopeIdentifier:
                    destinationAccountScopeIdentifier,
                detectedAt: detectedAt
            )
            let expectedBindingGenerationIdentifier = digest(
                domain: "pending-replica-binding",
                fields: [
                    activeGenerationIdentifier,
                    transitionID.uuidString.lowercased(),
                    sourceAccountScopeIdentifier,
                    destinationAccountScopeIdentifier,
                ]
            )
            guard (activeAccountScopeIdentifier
                    ?? restoredDatasetOwnerAccountScopeIdentifier)
                    == sourceAccountScopeIdentifier,
                  bindingGenerationIdentifier
                    == expectedBindingGenerationIdentifier else {
                throw BigSyncReplicaBindingError.corrupt
            }
        }
        return BigSyncReplicaBindingSnapshot(
            installationIdentityDigest: installationIdentityDigest,
            activeGenerationIdentifier: activeGenerationIdentifier,
            activeAccountScopeIdentifier: activeAccountScopeIdentifier,
            restoredDatasetOwnerAccountScopeIdentifier:
                restoredDatasetOwnerAccountScopeIdentifier,
            pendingPort: pendingPort
        )
    }

    static func bindInitialAccount(
        _ accountScopeIdentifier: String,
        store: any KeyValueStore,
        key: String
    ) throws -> BigSyncReplicaBindingSnapshot {
        guard !accountScopeIdentifier.isEmpty,
              let current = try load(store: store, key: key) else {
            throw BigSyncReplicaBindingError.corrupt
        }
        if let pendingPort = current.pendingPort {
            throw BigSyncCloudAccountPortError.required(pendingPort)
        }
        if let activeAccountScopeIdentifier =
            current.activeAccountScopeIdentifier {
            guard activeAccountScopeIdentifier == accountScopeIdentifier else {
                throw BigSyncReplicaBindingError.accountMismatch
            }
            return current
        }
        if let restoredDatasetOwnerAccountScopeIdentifier =
            current.restoredDatasetOwnerAccountScopeIdentifier,
           restoredDatasetOwnerAccountScopeIdentifier
            != accountScopeIdentifier {
            throw BigSyncReplicaBindingError.accountMismatch
        }
        let updated = BigSyncReplicaBindingSnapshot(
            installationIdentityDigest:
                current.installationIdentityDigest,
            activeGenerationIdentifier:
                current.activeGenerationIdentifier,
            activeAccountScopeIdentifier: accountScopeIdentifier,
            restoredDatasetOwnerAccountScopeIdentifier: nil,
            pendingPort: nil
        )
        try persist(updated, store: store, key: key)
        return updated
    }

    static func requirePort(
        sourceAccountScopeIdentifier: String,
        destinationAccountScopeIdentifier: String,
        store: any KeyValueStore,
        key: String
    ) throws -> BigSyncCloudAccountPortRequirement {
        guard !sourceAccountScopeIdentifier.isEmpty,
              !destinationAccountScopeIdentifier.isEmpty,
              sourceAccountScopeIdentifier
                != destinationAccountScopeIdentifier,
              let current = try load(store: store, key: key),
              current.datasetOwnerAccountScopeIdentifier
                == sourceAccountScopeIdentifier else {
            throw BigSyncReplicaBindingError.corrupt
        }
        // The first pending transition owns its mutation-binding generation
        // until a verified port protocol activates or retires it. Repeated
        // account notifications may report the source, destination, or a
        // third account, but none may replace that generation and strand
        // journal rows written while transport is fenced.
        if let existing = current.pendingPort {
            return existing
        }
        let transitionID = UUID()
        let requirement = BigSyncCloudAccountPortRequirement(
            transitionID: transitionID,
            bindingGenerationIdentifier: digest(
                domain: "pending-replica-binding",
                fields: [
                    current.activeGenerationIdentifier,
                    transitionID.uuidString.lowercased(),
                    sourceAccountScopeIdentifier,
                    destinationAccountScopeIdentifier,
                ]
            ),
            sourceAccountScopeIdentifier: sourceAccountScopeIdentifier,
            destinationAccountScopeIdentifier:
                destinationAccountScopeIdentifier,
            detectedAt: Date()
        )
        let updated = BigSyncReplicaBindingSnapshot(
            installationIdentityDigest:
                current.installationIdentityDigest,
            activeGenerationIdentifier:
                current.activeGenerationIdentifier,
            activeAccountScopeIdentifier:
                current.activeAccountScopeIdentifier,
            restoredDatasetOwnerAccountScopeIdentifier:
                current.restoredDatasetOwnerAccountScopeIdentifier,
            pendingPort: requirement
        )
        try persist(updated, store: store, key: key)
        return requirement
    }

    /// Activates only the exact pending generation after the application has
    /// durably verified its destination replica. Journal cleanup is separate:
    /// rows from the prior generation remain until the verified seed boundary
    /// explicitly retires them.
    static func activatePort(
        _ expected: BigSyncCloudAccountPortRequirement,
        store: any KeyValueStore,
        key: String
    ) throws -> BigSyncReplicaBindingSnapshot {
        guard let current = try load(store: store, key: key),
              current.pendingPort == expected,
              current.datasetOwnerAccountScopeIdentifier
                == expected.sourceAccountScopeIdentifier,
              current.activeGenerationIdentifier
                != expected.bindingGenerationIdentifier else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let activated = BigSyncReplicaBindingSnapshot(
            installationIdentityDigest:
                current.installationIdentityDigest,
            activeGenerationIdentifier:
                expected.bindingGenerationIdentifier,
            activeAccountScopeIdentifier:
                expected.destinationAccountScopeIdentifier,
            restoredDatasetOwnerAccountScopeIdentifier: nil,
            pendingPort: nil
        )
        try persist(activated, store: store, key: key)
        return activated
    }

    static func cancelPort(
        _ expected: BigSyncCloudAccountPortRequirement,
        store: any KeyValueStore,
        key: String
    ) throws -> BigSyncReplicaBindingSnapshot {
        guard let current = try load(store: store, key: key),
              current.pendingPort == expected,
              current.datasetOwnerAccountScopeIdentifier
                == expected.sourceAccountScopeIdentifier else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let restored = BigSyncReplicaBindingSnapshot(
            installationIdentityDigest:
                current.installationIdentityDigest,
            activeGenerationIdentifier:
                current.activeGenerationIdentifier,
            activeAccountScopeIdentifier:
                current.activeAccountScopeIdentifier,
            restoredDatasetOwnerAccountScopeIdentifier:
                current.restoredDatasetOwnerAccountScopeIdentifier,
            pendingPort: nil
        )
        try persist(restored, store: store, key: key)
        return restored
    }

    static func installationIdentityDigest(
        for installationIdentifier: String
    ) -> String {
        digest(
            domain: "replica-binding-installation",
            fields: [installationIdentifier]
        )
    }

    private static func persist(
        _ snapshot: BigSyncReplicaBindingSnapshot,
        store: any KeyValueStore,
        key: String
    ) throws {
        var value: [String: Any] = [
            "version": version,
            "installationIdentityDigest":
                snapshot.installationIdentityDigest,
            "activeGenerationIdentifier":
                snapshot.activeGenerationIdentifier,
        ]
        if let activeAccountScopeIdentifier =
            snapshot.activeAccountScopeIdentifier {
            value["activeAccountScopeIdentifier"] =
                activeAccountScopeIdentifier
        }
        if let restoredDatasetOwnerAccountScopeIdentifier =
            snapshot.restoredDatasetOwnerAccountScopeIdentifier {
            value["restoredDatasetOwnerAccountScopeIdentifier"] =
                restoredDatasetOwnerAccountScopeIdentifier
        }
        if let pendingPort = snapshot.pendingPort {
            value["pendingTransitionID"] =
                pendingPort.transitionID.uuidString
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
              }) else { return nil }
        return value
    }

    private static func digest(
        domain: String,
        fields: [String]
    ) -> String {
        var bytes = Data()
        for value in [domain] + fields {
            var length = UInt64(value.utf8.count).bigEndian
            withUnsafeBytes(of: &length) { bytes.append(contentsOf: $0) }
            bytes.append(Data(value.utf8))
        }
        return SHA256.hash(data: bytes).map {
            String(format: "%02x", $0)
        }.joined()
    }
}

final class BigSyncMutationJournalIdentityReader: @unchecked Sendable {
    private let clientIdentity: BigSyncClientIdentity
    private let store: any KeyValueStore
    private let key: String

    init(
        clientIdentity: BigSyncClientIdentity,
        store: any KeyValueStore,
        key: String
    ) {
        self.clientIdentity = clientIdentity
        self.store = store
        self.key = key
    }

    func current() -> BigSyncMutationJournalIdentity? {
        guard let installationBefore =
                clientIdentity.currentInstallationIdentifier(),
              let binding = try? BigSyncReplicaBindingStateStore.load(
                store: store,
                key: key
              ),
              let installationAfter =
                clientIdentity.currentInstallationIdentifier(),
              installationBefore == installationAfter,
              binding.installationIdentityDigest
                == BigSyncReplicaBindingStateStore
                    .installationIdentityDigest(
                        for: installationBefore
                    ) else {
            return nil
        }
        return BigSyncMutationJournalIdentity(
            installationIdentifier: installationBefore,
            replicaBindingGenerationIdentifier:
                binding.mutationGenerationIdentifier
        )
    }
}

public enum BigSyncCloudAccountPortError: Error, LocalizedError, Equatable {
    case required(BigSyncCloudAccountPortRequirement)
    case corruptRequirement
    case initialDatasetAdmissionUnavailable
    case workerRestartRequired

    public var errorDescription: String? {
        switch self {
        case .required:
            return "The local dataset must be ported before synchronizing with this iCloud account."
        case .corruptRequirement:
            return "The durable iCloud account-port requirement is malformed."
        case .initialDatasetAdmissionUnavailable:
            return "Explicit dataset-port mode requires an initial dataset-admission handler."
        case .workerRestartRequired:
            return "The verified dataset port is active; restart the CloudKit worker on its destination zone."
        }
    }
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
