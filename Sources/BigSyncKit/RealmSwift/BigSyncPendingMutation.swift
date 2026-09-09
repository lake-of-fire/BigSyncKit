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

    static func wasCreatedInMutationJournalIdentity(
        _ generation: String,
        identity: BigSyncMutationJournalIdentity
    ) -> Bool {
        let installationPrefix = installationGenerationPrefix
            + identity.installationIdentifier + ":"
        guard generation.hasPrefix(installationPrefix) else { return false }
        var suffix = generation.dropFirst(installationPrefix.count)
        if let binding = identity.replicaBindingGenerationIdentifier {
            let bindingPrefix = "binding:" + binding + ":"
            guard suffix.hasPrefix(bindingPrefix) else { return false }
            suffix = suffix.dropFirst(bindingPrefix.count)
        }
        // Every installation-owned generation is minted with a UUID nonce.
        // A matching authority prefix alone is not a well-formed mutation;
        // do not adopt a truncated/corrupt journal during exact handoff.
        return UUID(uuidString: String(suffix)) != nil
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

/// Transaction-local proof of a fresh metadata refresh. Only BigSyncKit can
/// construct witnesses; they are never persisted as a second outbox.
public struct BigSyncMutationJournalWitness: Sendable, Equatable {
    public let recordName: String
    public let entityType: String
    public let objectIdentifier: String
    public let accountScopeIdentifier: String?
    public let generation: String
    public let identity: BigSyncMutationJournalIdentity
}

public enum BigSyncMutationJournalError: Error, Sendable, Equatable {
    case authoritativeMutationRequired
    case objectUnavailable
    case writeTransactionRequired
    case unregisteredModel(String)
    case excludedModel(String)
    case missingJournalSchema
    case unsupportedPrimaryKey(String)
    case identityUnavailable
    case identityChanged
    case invalidAccountScope(String)
    case accountScopeChanged(String)
    case witnessMismatch(String)
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

    static func currentMutationJournalIdentity(
        in realm: Realm
    ) -> BigSyncMutationJournalIdentity? {
        let provider = lock.withLock {
            registrationsByRealm[
                identity(for: realm.configuration)
            ]?.mutationJournalIdentityProvider
        }
        return provider?()
    }

    static func makeMutationGeneration(
        context: MutationContext
    ) -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        do {
            return try makeMutationGenerationRequiringIdentity(context: context)
        } catch {
            fatalError("BigSync mutation identity is unavailable for a registered target Realm")
        }
    }

    /// Throwing transport path. A registered provider returning nil is not
    /// permission to commit an unjournaled conflict winner. Unbound test and
    /// legacy configurations with no identity provider retain process IDs.
    static func makeMutationGenerationRequiringIdentity(
        context: MutationContext
    ) throws -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        if let identityProvider = context.mutationJournalIdentityProvider {
            guard let identity = identityProvider(),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier?.isEmpty
                    != true else {
                throw BigSyncMutationJournalError.identityUnavailable
            }
            return makeMutationGeneration(identity: identity)
        }
        guard let installationProvider =
                context.installationIdentifierProvider else {
            return (BigSyncPendingMutation.makeGeneration(), nil)
        }
        guard let installationIdentifier = installationProvider(),
              !installationIdentifier.isEmpty else {
            throw BigSyncMutationJournalError.identityUnavailable
        }
        return (
            BigSyncPendingMutation.makeGeneration(
                installationIdentifier: installationIdentifier
            ),
            nil
        )
    }

    static func makeMutationGeneration(
        context: MutationContext,
        expectedIdentity: BigSyncMutationJournalIdentity
    ) throws -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        guard let identity = context.mutationJournalIdentityProvider?(),
              !identity.installationIdentifier.isEmpty,
              identity.replicaBindingGenerationIdentifier?.isEmpty != true else {
            throw BigSyncMutationJournalError.identityUnavailable
        }
        guard identity == expectedIdentity else {
            throw BigSyncMutationJournalError.identityChanged
        }
        return makeMutationGeneration(identity: identity)
    }

    private static func makeMutationGeneration(
        identity: BigSyncMutationJournalIdentity
    ) -> (
        generation: String,
        replicaBindingGenerationIdentifier: String?
    ) {
        if let binding = identity.replicaBindingGenerationIdentifier {
            return (
                BigSyncPendingMutation.makeGeneration(
                    installationIdentifier: identity.installationIdentifier,
                    replicaBindingGenerationIdentifier: binding
                ),
                binding
            )
        }
        return (
            BigSyncPendingMutation.makeGeneration(
                installationIdentifier: identity.installationIdentifier
            ),
            nil
        )
    }

    /// Recovery may discard historical outbox entries only after resolving the
    /// current identity. Provider unavailability is uncertainty, not evidence
    /// that a pending mutation came from a backup. Sample the registration once;
    /// never fall back to process identity when an installed provider returns nil.
    /// A successor provider binding is not this recovery run's authority to
    /// retire preceding work: the existing handoff/retry must reconcile it.
    static func mutationWasCreatedInCurrentTransportIdentity(
        _ mutation: BigSyncPendingMutation,
        realm: Realm,
        expectedBindingGenerationIdentifier: String?
    ) throws -> Bool {
        let registration = lock.withLock {
            registrationsByRealm[identity(for: realm.configuration)]
        }
        guard let registration else {
            throw BigSyncMutationJournalError.unregisteredModel(mutation.entityType)
        }
        if let provider = registration.mutationJournalIdentityProvider {
            guard let current = provider(), !current.installationIdentifier.isEmpty,
                  current.replicaBindingGenerationIdentifier?.isEmpty != true else {
                throw BigSyncMutationJournalError.identityUnavailable
            }
            guard current.replicaBindingGenerationIdentifier
                    == expectedBindingGenerationIdentifier else {
                throw BigSyncMutationJournalError.identityChanged
            }
            guard mutation.replicaBindingGenerationIdentifier
                    == current.replicaBindingGenerationIdentifier else { return false }
            return BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(
                mutation.generation, identity: current
            )
        }
        if let provider = registration.installationIdentifierProvider {
            guard let installation = provider(), !installation.isEmpty else {
                throw BigSyncMutationJournalError.identityUnavailable
            }
            guard expectedBindingGenerationIdentifier == nil else {
                throw BigSyncMutationJournalError.identityChanged
            }
            guard mutation.replicaBindingGenerationIdentifier == nil else { return false }
            return BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(
                mutation.generation,
                identity: .init(installationIdentifier: installation,
                                replicaBindingGenerationIdentifier: nil)
            )
        }
        // Explicitly unbound configurations use the process prefix. This is
        // not a fallback for an unavailable installation/binding provider.
        guard expectedBindingGenerationIdentifier == nil else {
            throw BigSyncMutationJournalError.identityChanged
        }
        return mutation.replicaBindingGenerationIdentifier == nil
            && BigSyncPendingMutation.wasCreatedInCurrentProcess(mutation.generation)
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

    /// Captures installation and binding together from the provider registered
    /// for this exact Realm. Missing identity is retryable, not a process trap.
    public static func requireCurrentJournalIdentity(
        in realm: Realm
    ) throws -> BigSyncMutationJournalIdentity {
        guard let identity = BigSyncMutationTrackingRegistry
            .currentMutationJournalIdentity(in: realm),
              !identity.installationIdentifier.isEmpty,
              identity.replicaBindingGenerationIdentifier?.isEmpty != true else {
            throw BigSyncMutationJournalError.identityUnavailable
        }
        return identity
    }

    /// Checks exact command authority even for a no-op, then requires each
    /// final pending row to retain the fresh generation returned by its write.
    /// A second refresh of the same record must supply its final witness only.
    public static func verifyJournalWitnesses(
        _ witnesses: [BigSyncMutationJournalWitness],
        expectedIdentity: BigSyncMutationJournalIdentity,
        in realm: Realm
    ) throws {
        guard realm.isInWriteTransaction else {
            throw BigSyncMutationJournalError.writeTransactionRequired
        }
        guard try requireCurrentJournalIdentity(in: realm) == expectedIdentity else {
            throw BigSyncMutationJournalError.identityChanged
        }
        guard realm.schema.objectSchema.contains(where: {
            $0.className == BigSyncPendingMutation.className()
        }) else {
            throw BigSyncMutationJournalError.missingJournalSchema
        }
        var verifiedNames = Set<String>()
        for witness in witnesses {
            guard witness.identity == expectedIdentity,
                  verifiedNames.insert(witness.recordName).inserted,
                  let mutation = realm.object(
                    ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: witness.recordName
                  ),
                  mutation.entityType == witness.entityType,
                  mutation.objectIdentifier == witness.objectIdentifier,
                  mutation.accountScopeIdentifier == witness.accountScopeIdentifier,
                  mutation.replicaBindingGenerationIdentifier
                    == expectedIdentity.replicaBindingGenerationIdentifier,
                  mutation.generation == witness.generation,
                  BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(
                    mutation.generation,
                    identity: expectedIdentity
                  ) else {
                throw BigSyncMutationJournalError.witnessMismatch(witness.recordName)
            }
        }
    }

    /// Returns the one live transport identity shared by the supplied
    /// automatically journaled mutations, or `nil` when an account/binding
    /// transition crossed the caller's Realm transaction.
    ///
    /// This is a verification boundary only. Application code continues to
    /// create journal rows exclusively through `refreshChangeMetadata`.
    public static func currentJournalIdentity<ObjectType: Object>(
        verifyingPendingMutationsFor objects: [ObjectType],
        in realm: Realm
    ) -> BigSyncMutationJournalIdentity? {
        precondition(realm.isInWriteTransaction)
        guard let identity = BigSyncMutationTrackingRegistry
            .currentMutationJournalIdentity(in: realm),
              !identity.installationIdentifier.isEmpty,
              identity.replicaBindingGenerationIdentifier?.isEmpty
                != true else {
            return nil
        }
        for object in objects {
            guard let objectRealm = object.realm,
                  !object.isInvalidated,
                  BigSyncMutationTrackingRegistry.identity(
                    for: objectRealm.configuration
                  ) == BigSyncMutationTrackingRegistry.identity(
                    for: realm.configuration
                  ),
                  let primaryKey = object.objectSchema.primaryKeyProperty?.name
            else { return nil }
            let entityType = object.objectSchema.className
            let objectIdentifier = RealmSwiftAdapter
                .getTargetObjectStringIdentifier(
                    for: object,
                    usingPrimaryKey: primaryKey
                )
            let recordName = entityType + "." + objectIdentifier
            guard let mutation = realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            ), mutation.entityType == entityType,
               mutation.objectIdentifier == objectIdentifier,
               mutation.replicaBindingGenerationIdentifier
                == identity.replicaBindingGenerationIdentifier,
               BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(
                    mutation.generation,
                    identity: identity
               ) else {
                return nil
            }
        }
        return identity
    }
}
