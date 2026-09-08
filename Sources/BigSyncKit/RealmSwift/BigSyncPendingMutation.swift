import Foundation
import RealmSwift

/// A durable, record-level local mutation journal stored in the target Realm.
///
/// Application writes update this object in the same Realm transaction as the
/// model object. BigSyncKit forwards its generation to the separate tracking
/// Realm and only removes the row after that exact generation is uploaded.
public final class BigSyncPendingMutation: Object {
    @Persisted(primaryKey: true) public var recordName = ""
    @Persisted(indexed: true) public var entityType = ""
    @Persisted public var objectIdentifier = ""
    @Persisted public var generation = ""
    /// Retained while the hotfix Realm schema is bridged. A migrated row stays
    /// bound to the replica that created it; upstream's current journal does
    /// not reinterpret this historical value as authority for a later account.
    @Persisted(indexed: true) public var accountScopeIdentifier: String?
    @Persisted(indexed: true) public var replicaBindingGenerationIdentifier: String?
    /// A historical row that was committed with a hotfix account or replica
    /// binding. It remains durable evidence but is not eligible for the
    /// account-agnostic adapter until a fenced recovery owner is available.
    @Persisted(indexed: true) public var requiresReplicaBindingRecovery = false
    @Persisted public var changedAt = Date()

    /// The accepted upstream transport is deliberately account-agnostic. A
    /// row that carries any hotfix account or replica-binding attribution must
    /// therefore remain recovery evidence until an owner can prove a complete
    /// account/container/database binding for it. Do not rely only on the
    /// persisted quarantine flag: a backup, interrupted migration, or older
    /// writer can leave the attribution fields present while that flag has its
    /// default value.
    var isEligibleForAccountAgnosticTransport: Bool {
        !requiresReplicaBindingRecovery
            && accountScopeIdentifier == nil
            && replicaBindingGenerationIdentifier == nil
    }

    public convenience init(
        recordName: String,
        entityType: String,
        objectIdentifier: String,
        generation: String = UUID().uuidString,
        changedAt: Date = Date()
    ) {
        self.init()
        self.recordName = recordName
        self.entityType = entityType
        self.objectIdentifier = objectIdentifier
        self.generation = generation
        self.changedAt = changedAt
    }

    /// Marks hotfix-bound outbox rows as evidence-only during the schema
    /// bridge. The current adapter has no compatible account/binding recovery
    /// owner, so forwarding one under a newly selected account would be data
    /// corruption rather than recovery.
    public static func quarantineLegacyReplicaBoundRows(
        migration: Migration
    ) {
        migration.enumerateObjects(ofType: className()) { oldObject, newObject in
            guard let oldObject, let newObject else { return }
            let accountScopeIdentifier = oldObject.objectSchema.properties
                .contains(where: { $0.name == "accountScopeIdentifier" })
                ? oldObject["accountScopeIdentifier"] as? String
                : nil
            let replicaBindingGenerationIdentifier = oldObject.objectSchema.properties
                .contains(where: { $0.name == "replicaBindingGenerationIdentifier" })
                ? oldObject["replicaBindingGenerationIdentifier"] as? String
                : nil
            let hasReplicaAttribution = [
                accountScopeIdentifier,
                replicaBindingGenerationIdentifier,
            ]
                .compactMap({ $0 })
                .contains(where: { !$0.isEmpty })
            // Assign both outcomes. Schema 299 also repairs the short-lived
            // schema-298 bridge whose inverted guard could mark ordinary
            // account-agnostic rows while leaving attributed rows unmarked.
            newObject["requiresReplicaBindingRecovery"] = hasReplicaAttribution
        }
    }
}

/// One policy value shared by Realm configuration construction and the sync
/// worker, preventing their exclusion lists from drifting apart.
public struct BigSyncMutationPolicy: Sendable, Equatable {
    public let excludedClassNames: [String]

    public init(excludedClassNames: [String]) {
        self.excludedClassNames = Array(Set(excludedClassNames)).sorted()
    }

    public func install(configurations: [Realm.Configuration]) {
        for configuration in configurations {
            precondition(
                configuration.objectTypes?.contains(where: {
                    $0.className() == BigSyncPendingMutation.className()
                }) == true,
                "Every BigSyncKit target Realm must include BigSyncPendingMutation in objectTypes"
            )
        }
        BigSyncMutationTracking.install(
            configurations: configurations,
            excludedClassNames: excludedClassNames
        )
    }
}

struct BigSyncPendingMutationSnapshot: Sendable {
    let recordName: String
    let entityType: String
    let objectIdentifier: String
    let generation: String
    let changedAt: Date
    let isDeletion: Bool
}

enum BigSyncMutationTrackingRegistry {
    private static let lock = NSLock()
    private static var trackedClassNamesByRealm = [String: Set<String>]()

    static func identity(for configuration: Realm.Configuration) -> String {
        if let inMemoryIdentifier = configuration.inMemoryIdentifier {
            return "memory:\(inMemoryIdentifier)"
        }
        if let fileURL = configuration.fileURL {
            return "file:\(fileURL.standardizedFileURL.path)"
        }
        return "default"
    }

    static func register(
        configurations: [Realm.Configuration],
        excluding excludedClassNames: Set<String>
    ) {
        lock.withLock {
            for configuration in configurations {
                let classNames = Set(
                    (configuration.objectTypes ?? [])
                        .map { $0.className() }
                        .filter { !excludedClassNames.contains($0) }
                )
                let realmIdentity = identity(for: configuration)
                if let registeredClassNames = trackedClassNamesByRealm[realmIdentity] {
                    precondition(
                        registeredClassNames == classNames,
                        "Conflicting BigSync mutation policies registered for \(realmIdentity)"
                    )
                } else {
                    trackedClassNamesByRealm[realmIdentity] = classNames
                }
            }
        }
    }

    enum TrackingStatus {
        case unregistered
        case excluded
        case tracked
    }

    static func trackingStatus(className: String, in realm: Realm) -> TrackingStatus {
        lock.withLock {
            guard let classNames = trackedClassNamesByRealm[
                identity(for: realm.configuration)
            ] else {
                return .unregistered
            }
            return classNames.contains(className) ? .tracked : .excluded
        }
    }

}

/// Installs the class allowlist used by `refreshChangeMetadata` to atomically
/// append durable CloudKit mutations.
///
/// Call this while constructing a target Realm configuration, before that Realm
/// can be opened by application writers. `RealmSwiftAdapter` also installs the
/// policy defensively during initialization, but adapter setup may intentionally
/// happen later than application startup.
public enum BigSyncMutationTracking {
    public static func install(
        configurations: [Realm.Configuration],
        excludedClassNames: [String]
    ) {
        BigSyncMutationTrackingRegistry.register(
            configurations: configurations,
            excluding: Set(
                excludedClassNames + [BigSyncPendingMutation.className()]
            )
        )
    }
}
