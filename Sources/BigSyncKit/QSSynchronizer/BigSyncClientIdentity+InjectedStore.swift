import Foundation

extension BigSyncClientIdentity {
    /// Uses the same state store supplied through BigSyncLocalStateConfiguration.
    /// This overload changes resource selection, not identity/restore/binding
    /// policy or the serialized format. Hosts must not prepare a second default
    /// binding when their worker uses an injected store.
    @discardableResult
    public func prepareReplicaBindingGenerationIdentifier(
        store: any KeyValueStore
    ) throws -> String {
        let installation = try prepareInstallation()
        if let durable = store as? any DurableKeyValueStore {
            try durable.prepareForUse()
        } else {
            try store.bigSyncValidateDurability()
        }
        return try BigSyncReplicaBindingStateStore.prepare(
            store: store,
            key: durableStateNamespace + ".ReplicaBinding.v1",
            installationIdentifier: installation
        ).mutationGenerationIdentifier
    }

    /// Shares the production installation-before/after and binding validation
    /// with the worker's injected store. No application-side binding decoding
    /// or snapshot caching is needed, and pending replacement is observed live.
    public func makeMutationJournalIdentityProvider(
        store: any KeyValueStore
    ) -> @Sendable () -> BigSyncMutationJournalIdentity? {
        let reader = BigSyncMutationJournalIdentityReader(
            clientIdentity: self,
            store: store,
            key: durableStateNamespace + ".ReplicaBinding.v1"
        )
        return { reader.current() }
    }

    public func currentMutationJournalIdentity(
        store: any KeyValueStore
    ) -> BigSyncMutationJournalIdentity? {
        makeMutationJournalIdentityProvider(store: store)()
    }
}
