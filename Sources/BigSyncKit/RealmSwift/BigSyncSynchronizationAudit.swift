import CloudKit
import Foundation
import RealmSwift

public struct BigSyncSynchronizationAudit: Codable, Equatable, Sendable {
    public let serverRecordCount: Int
    public let ownedServerRecordCount: Int
    public let unknownServerRecordCount: Int
    public let localObjectCount: Int
    public let trackingRecordCount: Int
    public let pendingMutationCount: Int
    public let pendingRelationshipCount: Int
    public let issues: [String]

    public var isClean: Bool { issues.isEmpty }
}

extension RealmSwiftAdapter {
    /// Verifies the terminal local state against a raw CloudKit zone inventory.
    /// This is read-only and does not clear, enqueue, or acknowledge sync work.
    @BigSyncBackgroundActor
    public func auditSynchronizationState(
        serverRecords: [CKRecord]
    ) async throws -> BigSyncSynchronizationAudit {
        try await ensureSetup()
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              let targetReaderRealms = realmProvider.targetReaderRealms else {
            throw RealmSwiftAdapterError.setupUnavailable
        }

        persistenceRealm.refresh()
        for realm in targetReaderRealms {
            realm.refresh()
        }

        let ownedTypeNames = Set(modelTypes.keys).subtracting(excludedClassNames)
        let ownedServerRecords = serverRecords.filter {
            ownedTypeNames.contains($0.recordType)
        }
        let unknownServerRecordCount = serverRecords.count - ownedServerRecords.count
        var issues = [String]()
        var serverRecordsByName = [String: CKRecord]()
        for record in ownedServerRecords {
            if serverRecordsByName.updateValue(
                record,
                forKey: record.recordID.recordName
            ) != nil {
                issues.append("duplicate-server-record:\(record.recordID.recordName)")
            }
        }

        let trackedEntities = persistenceRealm.objects(SyncedEntity.self).filter {
            ownedTypeNames.contains($0.entityType)
        }
        var trackedEntitiesByName = [String: [SyncedEntity]]()
        for trackedEntity in trackedEntities {
            trackedEntitiesByName[trackedEntity.identifier, default: []]
                .append(trackedEntity)
        }
        for (recordName, entities) in trackedEntitiesByName where entities.count != 1 {
            issues.append("duplicate-tracking-record:\(recordName)")
        }

        var localRecordNames = Set<String>()
        var localObjectCount = 0
        var processedTypes = Set<String>()
        for (entityType, objectClass) in modelTypes.sorted(by: { $0.key < $1.key }) {
            guard ownedTypeNames.contains(entityType),
                  processedTypes.insert(entityType).inserted,
                  let targetRealm = realmProvider
                    .targetReaderRealmPerSchemaName[entityType],
                  let primaryKey = objectClass.primaryKey()
                    ?? objectClass.sharedSchema()?.primaryKeyProperty?.name else {
                continue
            }
            for object in targetRealm.objects(objectClass) {
                guard objectIsEligibleForActiveAccount(
                    object,
                    entityType: entityType
                ) else { continue }
                let recordName = "\(entityType).\(Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey))"
                localRecordNames.insert(recordName)
                if let softDeletable = object as? SoftDeletable,
                   softDeletable.isDeleted {
                    issues.append("terminal-local-tombstone:\(recordName)")
                    continue
                }
                localObjectCount += 1
                guard let serverRecord = serverRecordsByName[recordName] else {
                    issues.append("local-record-missing-on-server:\(recordName)")
                    continue
                }
                guard let trackedEntities = trackedEntitiesByName[recordName] else {
                    issues.append("local-record-missing-tracking:\(recordName)")
                    continue
                }
                guard trackedEntities.count == 1,
                      let trackedEntity = trackedEntities.first else {
                    continue
                }
                if trackedEntity.entityState != .synced {
                    issues.append("tracking-not-synced:\(recordName):\(trackedEntity.state)")
                }
                if trackedEntity.pendingGeneration != nil {
                    issues.append("tracking-generation-pending:\(recordName)")
                }
                let differenceNames = serverDifferencePropertyNames(
                    record: serverRecord,
                    object: object
                )
                if !differenceNames.isEmpty {
                    issues.append(
                        "server-field-mismatch:\(recordName):\(differenceNames.joined(separator: ","))"
                    )
                }
            }
        }

        for recordName in serverRecordsByName.keys where !localRecordNames.contains(recordName) {
            issues.append("server-record-missing-locally:\(recordName)")
        }
        for recordName in serverRecordsByName.keys where trackedEntitiesByName[recordName] == nil {
            issues.append("server-record-missing-tracking:\(recordName)")
        }
        for trackedEntity in trackedEntities {
            if !localRecordNames.contains(trackedEntity.identifier) {
                issues.append("tracking-record-missing-locally:\(trackedEntity.identifier)")
            }
            if serverRecordsByName[trackedEntity.identifier] == nil {
                issues.append("tracking-record-missing-on-server:\(trackedEntity.identifier)")
            }
        }

        let pendingMutationCount = targetReaderRealms.reduce(into: 0) { total, realm in
            guard realm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) else { return }
            total += realm.objects(BigSyncPendingMutation.self).filter {
                pendingMutationIsEligibleForActiveTransport($0)
            }.count
        }
        if pendingMutationCount > 0 {
            issues.append("pending-mutations:\(pendingMutationCount)")
        }

        let pendingRelationshipCount = persistenceRealm
            .objects(PendingRelationship.self).count
        if pendingRelationshipCount > 0 {
            issues.append("pending-relationships:\(pendingRelationshipCount)")
        }

        if let activeAccountScopeIdentifier {
            let ownedQuarantines = activeInboundSemanticQuarantines(
                accountScopeIdentifier: activeAccountScopeIdentifier,
                in: persistenceRealm
            ).filter("entityType IN %@", Array(ownedTypeNames))
            for quarantine in ownedQuarantines {
                issues.append(
                    "inbound-semantic-quarantine:\(quarantine.recordName):\(quarantine.validationCode)"
                )
            }
        }

        return BigSyncSynchronizationAudit(
            serverRecordCount: serverRecords.count,
            ownedServerRecordCount: ownedServerRecords.count,
            unknownServerRecordCount: unknownServerRecordCount,
            localObjectCount: localObjectCount,
            trackingRecordCount: trackedEntities.count,
            pendingMutationCount: pendingMutationCount,
            pendingRelationshipCount: pendingRelationshipCount,
            issues: issues.sorted()
        )
    }
}
