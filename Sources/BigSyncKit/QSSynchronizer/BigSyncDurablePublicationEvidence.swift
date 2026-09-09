import CloudKit
import Foundation

/// Last terminally complete transport boundary. This is synchronization
/// metadata, not application data or another mutation journal.
public struct BigSyncDurablePublicationEvidence: Sendable, Equatable {
    public let domainScopeIdentifier: String
    public let accountScopeIdentifier: String
    public let replicaBindingGenerationIdentifier: String?
    public let zoneOwnerName: String
    public let zoneName: String
    public let changeFeedEpoch: Int
    public let consumedServerBoundaryIdentifier: String
    public let runID: UUID
    public let publishedAt: Date

    public init(
        domainScopeIdentifier: String,
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?,
        zoneOwnerName: String,
        zoneName: String,
        changeFeedEpoch: Int,
        consumedServerBoundaryIdentifier: String,
        runID: UUID,
        publishedAt: Date
    ) {
        self.domainScopeIdentifier = domainScopeIdentifier
        self.accountScopeIdentifier = accountScopeIdentifier
        self.replicaBindingGenerationIdentifier =
            replicaBindingGenerationIdentifier
        self.zoneOwnerName = zoneOwnerName
        self.zoneName = zoneName
        self.changeFeedEpoch = changeFeedEpoch
        self.consumedServerBoundaryIdentifier =
            consumedServerBoundaryIdentifier
        self.runID = runID
        self.publishedAt = publishedAt
    }
}

extension CloudKitSynchronizer {
    private static let durablePublicationEvidenceVersion = 1

    private var durablePublicationEvidenceKey: String {
        durableStateKey("TerminalPublication.v1")
    }

    func clearDurablePublicationEvidence() throws {
        try keyValueStore.bigSyncRemoveDurably(
            forKey: durablePublicationEvidenceKey
        )
    }

    func persistDurablePublicationEvidence(
        domainScopeIdentifier: String,
        context: RunContext,
        consumedServerBoundaryIdentifier: String,
        changeFeedEpoch: Int,
        at timestamp: Date = Date()
    ) throws {
        guard !domainScopeIdentifier.isEmpty,
              !consumedServerBoundaryIdentifier.isEmpty,
              changeFeedEpoch >= 0 else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
        try checkRunContext(context)
        var value: [String: Any] = [
            "version": Self.durablePublicationEvidenceVersion,
            "domainScopeIdentifier": domainScopeIdentifier,
            "accountScopeIdentifier": context.accountScopeIdentifier,
            "zoneOwnerName": recordZoneID.ownerName,
            "zoneName": recordZoneID.zoneName,
            "changeFeedEpoch": changeFeedEpoch,
            "consumedServerBoundaryIdentifier":
                consumedServerBoundaryIdentifier,
            "runID": context.runID.uuidString.lowercased(),
            "publishedAt": timestamp,
        ]
        value["replicaBindingGenerationIdentifier"] =
            context.replicaBindingGenerationIdentifier
        try keyValueStore.bigSyncSetDurably(
            value: value,
            forKey: durablePublicationEvidenceKey
        )
    }

    /// Exact prior transport evidence, without changing namespace or target state.
    /// Domain code must independently compare the current Realm scope and gate.
    func publicationEvidenceForUnconsumedFetch(context: RunContext) throws -> BigSyncDurablePublicationEvidence? {
        try checkRunContext(context)
        guard let evidence = try persistedDurablePublicationEvidence(),
              evidence.accountScopeIdentifier == context.accountScopeIdentifier,
              evidence.replicaBindingGenerationIdentifier == context.replicaBindingGenerationIdentifier,
              evidence.zoneName == recordZoneID.zoneName,
              evidence.zoneOwnerName == recordZoneID.ownerName,
              try !adaptersHavePendingChangesAtTerminalBoundary(),
              let adapter = modelAdapters.first,
              try adapter.changeFeedEpoch() == evidence.changeFeedEpoch,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: context.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: context.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier, databaseScope: database.databaseScope
              ) == evidence.consumedServerBoundaryIdentifier else { return nil }
        return evidence
    }

    private func persistedDurablePublicationEvidence() throws
        -> BigSyncDurablePublicationEvidence? {
        guard let raw = try keyValueStore.bigSyncDurableObject(
            forKey: durablePublicationEvidenceKey
        ) else {
            return nil
        }
        guard let value = raw as? [String: Any],
              (value["version"] as? NSNumber)?.intValue
                == Self.durablePublicationEvidenceVersion,
              let domainScopeIdentifier =
                value["domainScopeIdentifier"] as? String,
              !domainScopeIdentifier.isEmpty,
              let accountScopeIdentifier =
                value["accountScopeIdentifier"] as? String,
              !accountScopeIdentifier.isEmpty,
              let zoneOwnerName = value["zoneOwnerName"] as? String,
              !zoneOwnerName.isEmpty,
              let zoneName = value["zoneName"] as? String,
              !zoneName.isEmpty,
              let changeFeedEpochNumber =
                value["changeFeedEpoch"] as? NSNumber,
              changeFeedEpochNumber.intValue >= 0,
              let consumedServerBoundaryIdentifier = value[
                "consumedServerBoundaryIdentifier"
              ] as? String,
              !consumedServerBoundaryIdentifier.isEmpty,
              let runIDString = value["runID"] as? String,
              let runID = UUID(uuidString: runIDString),
              let publishedAt = value["publishedAt"] as? Date else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
        let binding = value[
            "replicaBindingGenerationIdentifier"
        ] as? String
        guard binding?.isEmpty != true else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
        return BigSyncDurablePublicationEvidence(
            domainScopeIdentifier: domainScopeIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding,
            zoneOwnerName: zoneOwnerName,
            zoneName: zoneName,
            changeFeedEpoch: changeFeedEpochNumber.intValue,
            consumedServerBoundaryIdentifier:
                consumedServerBoundaryIdentifier,
            runID: runID,
            publishedAt: publishedAt
        )
    }

    private struct PublicationRestorationSnapshot {
        let authority: PublicationRestorationAuthority
        let evidence: BigSyncDurablePublicationEvidence
        let adapters: [ObjectIdentifier]
    }

    private func validatePublicationRestoration(_ snapshot: PublicationRestorationSnapshot) throws {
        // Includes cancellation, synchronous account-notification poison, the
        // persisted invalidation generation, full binding/installation and run
        // ownership. The initial unvalidated fence is not an invalidation event.
        guard try publicationRestorationAuthority() == snapshot.authority,
              modelAdapters.map({ ObjectIdentifier($0) }) == snapshot.adapters,
              try persistedDurablePublicationEvidence() == snapshot.evidence else {
            throw CancellationError()
        }
    }

    private func revalidatePublicationRestoration(_ snapshot: PublicationRestorationSnapshot) async throws -> Bool {
        try validatePublicationRestoration(snapshot)
        let account = try await accountIdentifierProvider()
        try validatePublicationRestoration(snapshot)
        return Self.accountScopeIdentifier(for: account) == snapshot.evidence.accountScopeIdentifier
    }

    /// Read-only cold-start inspection, never a newly issued receipt or lease.
    /// Restored evidence can be handed to the domain only while the same startup
    /// attempt, saved evidence, account generation and installation still own it.
    func restoredDurablePublicationEvidence() async throws
        -> BigSyncDurablePublicationEvidence? {
        try Task.checkCancellation()
        guard let evidence = try persistedDurablePublicationEvidence(),
              evidence.zoneOwnerName == recordZoneID.ownerName,
              evidence.zoneName == recordZoneID.zoneName else { return nil }
        let authority = try publicationRestorationAuthority()
        guard authority.accountState.lease?.accountScopeIdentifier == evidence.accountScopeIdentifier,
              authority.binding?.activeGenerationIdentifier == evidence.replicaBindingGenerationIdentifier,
              authority.binding == nil || authority.binding?.activeAccountScopeIdentifier == evidence.accountScopeIdentifier else {
            return nil
        }
        let snapshot = PublicationRestorationSnapshot(
            authority: authority, evidence: evidence,
            adapters: modelAdapters.map { ObjectIdentifier($0) }
        )
        guard try await revalidatePublicationRestoration(snapshot) else { return nil }
        for adapter in modelAdapters {
            try await adapter.activateTransportNamespace(
                containerIdentifier: containerIdentifier, databaseScope: database.databaseScope
            )
            guard try await revalidatePublicationRestoration(snapshot) else { return nil }
            try await adapter.activateReplicaBinding(
                accountScopeIdentifier: evidence.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: evidence.replicaBindingGenerationIdentifier
            )
            guard try await revalidatePublicationRestoration(snapshot) else { return nil }
        }
        // Join the existing Realm setup task, retaining the original startup
        // ownership across readiness. Do not prepare a new binding or namespace.
        for case let adapter as RealmSwiftAdapter in modelAdapters {
            try await adapter.ensureSetup()
            try validatePublicationRestoration(snapshot)
        }
        // LAST actual-account await. The journal/cursor/feed/evidence checks
        // below must remain a non-suspending tail, even on a cold launch.
        guard try await revalidatePublicationRestoration(snapshot) else { return nil }
        guard try !adaptersHavePendingChangesAtTerminalBoundary(),
              let adapter = modelAdapters.first,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: evidence.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: evidence.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier, databaseScope: database.databaseScope
              ) == evidence.consumedServerBoundaryIdentifier,
              try adapter.changeFeedEpoch() == evidence.changeFeedEpoch else { return nil }
        try validatePublicationRestoration(snapshot)
        return evidence
    }

#if DEBUG
    /// Read-only E2E inventory of the exact durable bytes already validated by
    /// the terminal path. This neither restores publication nor touches Realm.
    @_spi(CloudKitE2E)
    public func cloudKitE2EDurablePublicationEvidence() throws
        -> BigSyncDurablePublicationEvidence? {
        try keyValueStore.bigSyncValidateDurability()
        return try persistedDurablePublicationEvidence()
    }
#endif
}
