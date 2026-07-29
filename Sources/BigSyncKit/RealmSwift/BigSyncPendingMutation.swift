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

struct BigSyncPendingMutationSnapshot: Sendable {
    let recordName: String
    let entityType: String
    let objectIdentifier: String
    let generation: String
    let changedAt: Date
}

enum BigSyncMutationTrackingRegistry {
    private static let lock = NSLock()
    private static var trackedClassNamesByRealm = [String: Set<String>]()
    private static var unboundMutations = [String: BigSyncPendingMutationSnapshot]()

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
                trackedClassNamesByRealm[identity(for: configuration)] = classNames
            }
        }
    }

    static func tracks(className: String, in realm: Realm) -> Bool {
        lock.withLock {
            trackedClassNamesByRealm[identity(for: realm.configuration)]?
                .contains(className) == true
        }
    }

    static func tracks(className: String) -> Bool {
        lock.withLock {
            trackedClassNamesByRealm.values.contains { $0.contains(className) }
        }
    }

    static func trackedClassNames(in realm: Realm) -> Set<String> {
        lock.withLock {
            trackedClassNamesByRealm[identity(for: realm.configuration)] ?? []
        }
    }

    static func enqueueUnbound(_ mutation: BigSyncPendingMutationSnapshot) {
        lock.withLock {
            unboundMutations[mutation.recordName] = mutation
        }
    }

    static func unboundMutations(for classNames: Set<String>) -> [BigSyncPendingMutationSnapshot] {
        lock.withLock {
            unboundMutations.values.filter { classNames.contains($0.entityType) }
        }
    }

    static func removeUnbound(recordName: String, generation: String) {
        lock.withLock {
            guard unboundMutations[recordName]?.generation == generation else { return }
            unboundMutations.removeValue(forKey: recordName)
        }
    }

    static func removeUnbound(recordName: String) {
        lock.withLock {
            unboundMutations.removeValue(forKey: recordName)
        }
    }
}
