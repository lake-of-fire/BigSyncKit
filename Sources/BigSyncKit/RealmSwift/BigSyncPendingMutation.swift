import Foundation
import RealmSwift

/// A durable, record-level local mutation journal stored in the target Realm.
///
/// Application writes update this object in the same Realm transaction as the
/// model object. BigSyncKit forwards its generation to the separate tracking
/// Realm and only removes the row after that exact generation is uploaded.
public final class BigSyncPendingMutation: Object {
    /// Process identity is encoded into the otherwise opaque generation rather
    /// than added to Realm's schema. This lets backup recovery distinguish an
    /// outbox row copied from the backup from a genuine mutation committed by
    /// the currently running restored app, including writes made before the
    /// synchronizer finishes configuring.
    private static let processGenerationPrefix = UUID().uuidString + ":"

    private static let installationGenerationPrefix = "installation:"

    static func makeGeneration() -> String {
        processGenerationPrefix + UUID().uuidString
    }

    static func makeGeneration(installationIdentifier: String) -> String {
        installationGenerationPrefix + installationIdentifier + ":" + UUID().uuidString
    }

    static func makeGeneration(
        installationIdentifier: String,
        replicaBindingGenerationIdentifier: String
    ) -> String {
        installationGenerationPrefix + installationIdentifier
            + ":binding:" + replicaBindingGenerationIdentifier
            + ":" + UUID().uuidString
    }

    static func wasCreatedInCurrentProcess(_ generation: String) -> Bool {
        generation.hasPrefix(processGenerationPrefix)
    }

    static func wasCreatedInInstallation(
        _ generation: String,
        installationIdentifier: String
    ) -> Bool {
        generation.hasPrefix(
            installationGenerationPrefix + installationIdentifier + ":"
        )
    }

    @Persisted(primaryKey: true) public var recordName = ""
    @Persisted(indexed: true) public var entityType = ""
    @Persisted public var objectIdentifier = ""
    /// Opaque CloudKit account scope captured from the domain object at the
    /// same transaction boundary as this generation. `nil` is reserved for
    /// explicitly unscoped model types and is never rebound to a later account
    /// by convenience.
    @Persisted(indexed: true) public var accountScopeIdentifier: String?
    /// Local transport generation that owned this mutation when it was
    /// committed. Account ports create a new generation instead of relabeling
    /// work prepared for an older remote replica.
    @Persisted(indexed: true)
    public var replicaBindingGenerationIdentifier: String?
    @Persisted public var generation = ""
    @Persisted public var changedAt = Date()

    public convenience init(
        recordName: String,
        entityType: String,
        objectIdentifier: String,
        accountScopeIdentifier: String? = nil,
        replicaBindingGenerationIdentifier: String? = nil,
        generation: String? = nil,
        changedAt: Date = Date()
    ) {
        self.init()
        self.recordName = recordName
        self.entityType = entityType
        self.objectIdentifier = objectIdentifier
        self.accountScopeIdentifier = accountScopeIdentifier
        self.replicaBindingGenerationIdentifier =
            replicaBindingGenerationIdentifier
        self.generation = generation ?? Self.makeGeneration()
        self.changedAt = changedAt
    }
}

/// One policy value shared by Realm configuration construction and the sync
/// worker, preventing their exclusion lists from drifting apart.
public struct BigSyncMutationPolicy: Sendable, Equatable {
    public let excludedClassNames: [String]
    /// Maps a synchronized Realm class name to its immutable CloudKit account
    /// scope property. The same map is installed for local journaling and
    /// passed to the worker adapter; configuration drift is a programmer error.
    public let accountScopePropertyByClassName: [String: String]

    public init(
        excludedClassNames: [String],
        accountScopePropertyByClassName: [String: String] = [:]
    ) {
        self.excludedClassNames = Array(Set(excludedClassNames)).sorted()
        precondition(
            accountScopePropertyByClassName.allSatisfy {
                !$0.key.isEmpty && !$0.value.isEmpty
            },
            "BigSync account-scope policy names must be non-empty"
        )
        self.accountScopePropertyByClassName =
            accountScopePropertyByClassName
    }

    public func install(
        configurations: [Realm.Configuration],
        installationIdentifier: String? = nil,
        installationIdentifierProvider:
            (@Sendable () -> String?)? = nil,
        mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)? = nil
    ) {
        precondition(
            installationIdentifier == nil
                || installationIdentifierProvider == nil,
            "Provide either a fixed installation identity or a provider"
        )
        precondition(
            mutationJournalIdentityProvider == nil
                || (
                    installationIdentifier == nil
                    && installationIdentifierProvider == nil
                ),
            "A combined mutation identity provider cannot be mixed with an installation identity provider"
        )
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
            excludedClassNames: excludedClassNames,
            accountScopePropertyByClassName:
                accountScopePropertyByClassName,
            installationIdentifierProvider:
                installationIdentifierProvider
                ?? installationIdentifier.map { identifier in
                    { @Sendable in identifier }
                },
            mutationJournalIdentityProvider:
                mutationJournalIdentityProvider
        )
    }
}

struct BigSyncPendingMutationSnapshot: Sendable {
    let recordName: String
    let entityType: String
    let objectIdentifier: String
    let accountScopeIdentifier: String?
    let replicaBindingGenerationIdentifier: String?
    let generation: String
    let changedAt: Date
    let isDeletion: Bool
}

enum BigSyncMutationTrackingRegistry {
    private static let lock = NSLock()
    private struct Registration {
        let classNames: Set<String>
        let accountScopePropertyByClassName: [String: String]
        var installationIdentifierProvider: (@Sendable () -> String?)?
        var mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)?
    }
    private static var registrationsByRealm = [String: Registration]()

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
        excluding excludedClassNames: Set<String>,
        accountScopePropertyByClassName: [String: String],
        installationIdentifierProvider: (@Sendable () -> String?)?,
        mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)?
    ) {
        lock.withLock {
            for configuration in configurations {
                let classNames = Set(
                    (configuration.objectTypes ?? [])
                        .map { $0.className() }
                        .filter { !excludedClassNames.contains($0) }
                )
                let realmIdentity = identity(for: configuration)
                if var registration = registrationsByRealm[realmIdentity] {
                    precondition(
                        registration.classNames == classNames,
                        "Conflicting BigSync mutation policies registered for \(realmIdentity)"
                    )
                    precondition(
                        registration.accountScopePropertyByClassName
                            == accountScopePropertyByClassName,
                        "Conflicting BigSync account-scope policies registered for \(realmIdentity)"
                    )
                    if let installationIdentifierProvider {
                        registration.installationIdentifierProvider =
                            installationIdentifierProvider
                        registration.mutationJournalIdentityProvider = nil
                    }
                    if let mutationJournalIdentityProvider {
                        registration.mutationJournalIdentityProvider =
                            mutationJournalIdentityProvider
                        registration.installationIdentifierProvider = nil
                    }
                    registrationsByRealm[realmIdentity] = registration
                } else {
                    registrationsByRealm[realmIdentity] = Registration(
                        classNames: classNames,
                        accountScopePropertyByClassName:
                            accountScopePropertyByClassName,
                        installationIdentifierProvider:
                            installationIdentifierProvider,
                        mutationJournalIdentityProvider:
                            mutationJournalIdentityProvider
                    )
                }
            }
        }
    }

    enum TrackingStatus {
        case unregistered
        case excluded
        case tracked
    }

    /// Immutable policy sampled once at the beginning of one object mutation.
    /// Provider closures are copied while the registry lock is held and are
    /// invoked only after the lock has been released.
    struct MutationContext {
        let trackingStatus: TrackingStatus
        let accountScopePropertyName: String?
        let installationIdentifierProvider: (@Sendable () -> String?)?
        let mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)?
    }

    static func mutationContext(
        className: String,
        in realm: Realm
    ) -> MutationContext {
        lock.withLock {
            guard let registration = registrationsByRealm[
                identity(for: realm.configuration)
            ] else {
                return MutationContext(
                    trackingStatus: .unregistered,
                    accountScopePropertyName: nil,
                    installationIdentifierProvider: nil,
                    mutationJournalIdentityProvider: nil
                )
            }
            guard registration.classNames.contains(className) else {
                return MutationContext(
                    trackingStatus: .excluded,
                    accountScopePropertyName: nil,
                    installationIdentifierProvider: nil,
                    mutationJournalIdentityProvider: nil
                )
            }
            return MutationContext(
                trackingStatus: .tracked,
                accountScopePropertyName:
                    registration.accountScopePropertyByClassName[className],
                installationIdentifierProvider:
                    registration.installationIdentifierProvider,
                mutationJournalIdentityProvider:
                    registration.mutationJournalIdentityProvider
            )
        }
    }

    static func makeMutationGeneration(
        in realm: Realm
    ) -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        let context = lock.withLock {
            let registration = registrationsByRealm[
                identity(for: realm.configuration)
            ]
            return MutationContext(
                trackingStatus: registration == nil
                    ? .unregistered
                    : .tracked,
                accountScopePropertyName: nil,
                installationIdentifierProvider:
                    registration?.installationIdentifierProvider,
                mutationJournalIdentityProvider:
                    registration?.mutationJournalIdentityProvider
            )
        }
        return makeMutationGeneration(context: context)
    }

    static func makeMutationGeneration(
        context: MutationContext
    ) -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        if let identityProvider = context.mutationJournalIdentityProvider {
            guard let identity = identityProvider(),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier?.isEmpty
                    != true else {
                fatalError(
                    "BigSync mutation identity is unavailable for a registered target Realm"
                )
            }
            if let binding =
                identity.replicaBindingGenerationIdentifier {
                return (
                    BigSyncPendingMutation.makeGeneration(
                        installationIdentifier:
                            identity.installationIdentifier,
                        replicaBindingGenerationIdentifier: binding
                    ),
                    binding
                )
            }
            return (
                BigSyncPendingMutation.makeGeneration(
                    installationIdentifier:
                        identity.installationIdentifier
                ),
                nil
            )
        }
        guard let installationProvider =
                context.installationIdentifierProvider else {
            return (BigSyncPendingMutation.makeGeneration(), nil)
        }
        guard let installationIdentifier = installationProvider() else {
            fatalError(
                "BigSync installation identity is unavailable for a registered target Realm"
            )
        }
        return (
            BigSyncPendingMutation.makeGeneration(
                installationIdentifier: installationIdentifier
            ),
            nil
        )
    }

    static func accountScopeIdentifier(
        for object: Object,
        entityType: String,
        propertyName: String?
    ) -> String? {
        guard let propertyName else { return nil }
        guard object.objectSchema.properties.contains(where: {
            $0.name == propertyName && $0.type == .string
        }) else {
            preconditionFailure(
                "BigSync account-scope property \(entityType).\(propertyName) is missing or not a String"
            )
        }
        guard let value = object[propertyName] as? String,
              !value.isEmpty else {
            preconditionFailure(
                "BigSync account-scoped mutation for \(entityType) has no immutable account scope"
            )
        }
        return value
    }

    static func generationWasCreatedInCurrentInstallation(
        _ generation: String,
        realm: Realm
    ) -> Bool {
        let provider = lock.withLock {
            let registration = registrationsByRealm[
                identity(for: realm.configuration)
            ]
            return (
                registration?.mutationJournalIdentityProvider,
                registration?.installationIdentifierProvider
            )
        }
        if let combinedProvider = provider.0 {
            guard let identity = combinedProvider() else { return false }
            return BigSyncPendingMutation.wasCreatedInInstallation(
                generation,
                installationIdentifier: identity.installationIdentifier
            )
        }
        guard let installationProvider = provider.1 else {
            return BigSyncPendingMutation.wasCreatedInCurrentProcess(generation)
        }
        guard let installationIdentifier = installationProvider() else {
            return false
        }
        return BigSyncPendingMutation.wasCreatedInInstallation(
            generation,
            installationIdentifier: installationIdentifier
        )
    }

    static func mutationWasCreatedInCurrentTransportIdentity(
        _ mutation: BigSyncPendingMutation,
        realm: Realm
    ) -> Bool {
        guard let mutationBinding =
                mutation.replicaBindingGenerationIdentifier else {
            return generationWasCreatedInCurrentInstallation(
                mutation.generation,
                realm: realm
            )
        }
        let provider = lock.withLock {
            registrationsByRealm[
                identity(for: realm.configuration)
            ]?.mutationJournalIdentityProvider
        }
        guard let identity = provider?(),
              identity.replicaBindingGenerationIdentifier
                == mutationBinding else { return false }
        return BigSyncPendingMutation.wasCreatedInInstallation(
            mutation.generation,
            installationIdentifier: identity.installationIdentifier
        )
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
        excludedClassNames: [String],
        accountScopePropertyByClassName: [String: String] = [:],
        installationIdentifier: String? = nil,
        installationIdentifierProvider:
            (@Sendable () -> String?)? = nil,
        mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)? = nil
    ) {
        precondition(
            installationIdentifier == nil
                || installationIdentifierProvider == nil,
            "Provide either a fixed installation identity or a provider"
        )
        precondition(
            mutationJournalIdentityProvider == nil
                || (
                    installationIdentifier == nil
                    && installationIdentifierProvider == nil
                ),
            "A combined mutation identity provider cannot be mixed with an installation identity provider"
        )
        BigSyncMutationTrackingRegistry.register(
            configurations: configurations,
            excluding: Set(
                excludedClassNames + [BigSyncPendingMutation.className()]
            ),
            accountScopePropertyByClassName:
                accountScopePropertyByClassName,
            installationIdentifierProvider:
                installationIdentifierProvider
                ?? installationIdentifier.map { identifier in
                    { @Sendable in identifier }
                },
            mutationJournalIdentityProvider:
                mutationJournalIdentityProvider
        )
    }
}
