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

    /// Restores terminal evidence only when the current CloudKit account,
    /// replica binding, local cursor, and feed epoch still match it exactly.
    func restoredDurablePublicationEvidence() async throws
        -> BigSyncDurablePublicationEvidence? {
        guard let evidence = try persistedDurablePublicationEvidence(),
              evidence.zoneOwnerName == recordZoneID.ownerName,
              evidence.zoneName == recordZoneID.zoneName else {
            return nil
        }
        let accountIdentifier = try await accountIdentifierProvider()
        let accountScopeIdentifier = Self.accountScopeIdentifier(
            for: accountIdentifier
        )
        guard evidence.accountScopeIdentifier == accountScopeIdentifier else {
            return nil
        }
        let replicaBindingGenerationIdentifier = try
            activeReplicaBindingGenerationIdentifierForRun(
            accountScopeIdentifier: accountScopeIdentifier
        )
        guard evidence.replicaBindingGenerationIdentifier
                == replicaBindingGenerationIdentifier else {
            return nil
        }
        for adapter in modelAdapters {
            try await adapter.activateTransportNamespace(
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
            )
            try await adapter.activateReplicaBinding(
                accountScopeIdentifier: accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    replicaBindingGenerationIdentifier
            )
        }
        guard try !adaptersHavePendingChangesAtTerminalBoundary() else {
            return nil
        }
        guard let adapter = modelAdapters.first,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
              ) == evidence.consumedServerBoundaryIdentifier,
              try adapter.changeFeedEpoch() == evidence.changeFeedEpoch else {
            return nil
        }
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
