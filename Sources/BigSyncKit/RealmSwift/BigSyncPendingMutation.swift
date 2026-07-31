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
    @Persisted public var changedAt = Date()

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
