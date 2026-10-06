import CloudKit
import CoreFoundation
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

    static let persistenceVersion = 1

    /// Decode only the typed property-list representation written below.
    /// A malformed present binding is not an intentionally unbound receipt;
    /// lossy numeric conversion must not manufacture a matching version/epoch.
    init(persistedValue raw: Any) throws {
        guard let value = raw as? [String: Any],
              Self.persistedNonnegativeInteger(value["version"])
                == Self.persistenceVersion,
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
              let changeFeedEpoch = Self.persistedNonnegativeInteger(
                value["changeFeedEpoch"]
              ),
              let consumedServerBoundaryIdentifier = value[
                "consumedServerBoundaryIdentifier"
              ] as? String,
              !consumedServerBoundaryIdentifier.isEmpty,
              let runIDString = value["runID"] as? String,
              let runID = UUID(uuidString: runIDString),
              let publishedAt = value["publishedAt"] as? Date,
              publishedAt.timeIntervalSinceReferenceDate.isFinite else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
        let binding: String?
        if let rawBinding = value["replicaBindingGenerationIdentifier"] {
            guard let identifier = rawBinding as? String, !identifier.isEmpty else {
                throw DurableKeyValueStoreError.mutationNotDurable
            }
            binding = identifier
        } else {
            binding = nil
        }
        self.init(
            domainScopeIdentifier: domainScopeIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding,
            zoneOwnerName: zoneOwnerName,
            zoneName: zoneName,
            changeFeedEpoch: changeFeedEpoch,
            consumedServerBoundaryIdentifier:
                consumedServerBoundaryIdentifier,
            runID: runID,
            publishedAt: publishedAt
        )
    }

    private static func persistedNonnegativeInteger(_ raw: Any?) -> Int? {
        guard let number = raw as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID() else { return nil }
        switch String(cString: number.objCType) {
        case "c", "C", "s", "S", "i", "I", "l", "L", "q", "Q":
            guard let value = Int(exactly: number), value >= 0 else { return nil }
            return value
        default:
            // Even an integral real is not the integer property-list field
            // emitted by this format. Never truncate a future/corrupt value.
            return nil
        }
    }
}

extension CloudKitSynchronizer {
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
              changeFeedEpoch >= 0,
              timestamp.timeIntervalSinceReferenceDate.isFinite else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
        try checkRunContext(context)
        var value: [String: Any] = [
            "version": BigSyncDurablePublicationEvidence.persistenceVersion,
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
        // The writer must never replace good evidence with a value that its
        // own persisted-value decoder would reject. Keep one v1 contract.
        _ = try BigSyncDurablePublicationEvidence(persistedValue: value)
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
        return try BigSyncDurablePublicationEvidence(persistedValue: raw)
    }

    /// Restores terminal evidence only when the current CloudKit account,
    /// replica binding, local cursor, and feed epoch still match it exactly.
    func restoredDurablePublicationEvidence() async throws
        -> BigSyncDurablePublicationEvidence? {
        let expectedAttemptID = synchronizationAttemptID
        let expectedFenceGeneration = accountScopeAuthorityFence
            .publicationInspectionGeneration
        func inspectionOwnerIsCurrent() throws -> Bool {
            try Task.checkCancellation()
            // Ineligible saved evidence is an ordinary absence, not task
            // cancellation. In particular, a restore/account fence must not
            // prevent the caller from receiving nil and discarding old UI
            // readiness. Actual canceled tasks still throw above.
            return synchronizationAttemptID == expectedAttemptID
                && !syncing && !synchronizationDrainIsActive
                && !backupRestoreDetected
                && expectedFenceGeneration != nil
                && accountScopeAuthorityFence.publicationInspectionGeneration
                    == expectedFenceGeneration
        }
        guard try inspectionOwnerIsCurrent() else { return nil }
        guard let evidence = try persistedDurablePublicationEvidence(),
              evidence.zoneOwnerName == recordZoneID.ownerName,
              evidence.zoneName == recordZoneID.zoneName else {
            return nil
        }
        let accountIdentifier = try await accountIdentifierProvider()
        guard try inspectionOwnerIsCurrent() else { return nil }
        let accountScopeIdentifier = Self.accountScopeIdentifier(for: accountIdentifier)
        guard evidence.accountScopeIdentifier == accountScopeIdentifier,
              evidence.replicaBindingGenerationIdentifier == (try
                activeReplicaBindingGenerationIdentifierForRun(
                    accountScopeIdentifier: accountScopeIdentifier
                )) else { return nil }

        if let realmAdapter = modelAdapters.first as? RealmSwiftAdapter {
            // Cold production adapters intentionally have no operational
            // provider until recovery is fenced. Inspect existing files
            // without invoking setup or changing transport ownership.
            guard let inspection = try await realmAdapter
                .preparePublicationRestorationInspection() else { return nil }
            guard try inspectionOwnerIsCurrent() else { return nil }
            let confirmedAccount = try await accountIdentifierProvider()
            guard try inspectionOwnerIsCurrent() else { return nil }
            guard confirmedAccount == accountIdentifier,
                  evidence.replicaBindingGenerationIdentifier == (try
                    activeReplicaBindingGenerationIdentifierForRun(
                        accountScopeIdentifier: accountScopeIdentifier
                    )),
                  try persistedDurablePublicationEvidence() == evidence,
                  try inspection.matches(
                    evidence,
                    containerIdentifier: containerIdentifier,
                    databaseScope: database.databaseScope
                  ) else { return nil }
            // Refresh/inspection may synchronously revoke the original owner.
            // No positive evidence can escape after that final callout.
            guard try inspectionOwnerIsCurrent() else { return nil }
            return evidence
        }

        // Non-Realm adapters retain their existing inspection contract.
        for adapter in modelAdapters {
            try await adapter.activateTransportNamespace(
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
            )
            guard try inspectionOwnerIsCurrent() else { return nil }
            try await adapter.activateReplicaBinding(
                accountScopeIdentifier: accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    evidence.replicaBindingGenerationIdentifier
            )
            guard try inspectionOwnerIsCurrent() else { return nil }
        }
        let confirmedAccount = try await accountIdentifierProvider()
        guard try inspectionOwnerIsCurrent() else { return nil }
        guard confirmedAccount == accountIdentifier,
              evidence.replicaBindingGenerationIdentifier == (try
                activeReplicaBindingGenerationIdentifierForRun(
                    accountScopeIdentifier: accountScopeIdentifier
                )),
              try persistedDurablePublicationEvidence() == evidence,
              try !adaptersHavePendingChangesAtTerminalBoundary(),
              let adapter = modelAdapters.first,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    evidence.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
              ) == evidence.consumedServerBoundaryIdentifier,
              try adapter.changeFeedEpoch() == evidence.changeFeedEpoch else {
            return nil
        }
        guard try inspectionOwnerIsCurrent() else { return nil }
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
