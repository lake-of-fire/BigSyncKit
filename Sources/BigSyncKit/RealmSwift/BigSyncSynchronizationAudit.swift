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
    /// Zero identifies older artifacts that did not inspect comparison evidence.
    /// New release qualification must require version 1 as well as `isClean`.
    public let comparisonEvidenceVersion: Int
    public let unresolvedSubmissionCount: Int
    public let acceptedBaselineCount: Int
    public let invalidatedBaselineCount: Int
    public let resolvedPreservationReceiptCount: Int
    public let retainedTombstoneCount: Int

    public var isClean: Bool { issues.isEmpty && unresolvedSubmissionCount == 0 }

    init(serverRecordCount: Int, ownedServerRecordCount: Int, unknownServerRecordCount: Int,
         localObjectCount: Int, trackingRecordCount: Int, pendingMutationCount: Int,
         pendingRelationshipCount: Int, issues: [String], comparisonEvidenceVersion: Int = 1,
         unresolvedSubmissionCount: Int = 0, acceptedBaselineCount: Int = 0,
         invalidatedBaselineCount: Int = 0, resolvedPreservationReceiptCount: Int = 0,
         retainedTombstoneCount: Int = 0) {
        self.serverRecordCount = serverRecordCount
        self.ownedServerRecordCount = ownedServerRecordCount
        self.unknownServerRecordCount = unknownServerRecordCount
        self.localObjectCount = localObjectCount
        self.trackingRecordCount = trackingRecordCount
        self.pendingMutationCount = pendingMutationCount
        self.pendingRelationshipCount = pendingRelationshipCount
        self.issues = issues
        self.comparisonEvidenceVersion = comparisonEvidenceVersion
        self.unresolvedSubmissionCount = unresolvedSubmissionCount
        self.acceptedBaselineCount = acceptedBaselineCount
        self.invalidatedBaselineCount = invalidatedBaselineCount
        self.resolvedPreservationReceiptCount = resolvedPreservationReceiptCount
        self.retainedTombstoneCount = retainedTombstoneCount
    }

    private enum CodingKeys: String, CodingKey {
        case serverRecordCount, ownedServerRecordCount, unknownServerRecordCount,
             localObjectCount, trackingRecordCount, pendingMutationCount,
             pendingRelationshipCount, issues, comparisonEvidenceVersion,
             unresolvedSubmissionCount, acceptedBaselineCount, invalidatedBaselineCount,
             resolvedPreservationReceiptCount, retainedTombstoneCount
    }

    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            serverRecordCount: try values.decode(Int.self, forKey: .serverRecordCount),
            ownedServerRecordCount: try values.decode(Int.self, forKey: .ownedServerRecordCount),
            unknownServerRecordCount: try values.decode(Int.self, forKey: .unknownServerRecordCount),
            localObjectCount: try values.decode(Int.self, forKey: .localObjectCount),
            trackingRecordCount: try values.decode(Int.self, forKey: .trackingRecordCount),
            pendingMutationCount: try values.decode(Int.self, forKey: .pendingMutationCount),
            pendingRelationshipCount: try values.decode(Int.self, forKey: .pendingRelationshipCount),
            issues: try values.decode([String].self, forKey: .issues),
            comparisonEvidenceVersion: try values.decodeIfPresent(Int.self, forKey: .comparisonEvidenceVersion) ?? 0,
            unresolvedSubmissionCount: try values.decodeIfPresent(Int.self, forKey: .unresolvedSubmissionCount) ?? 0,
            acceptedBaselineCount: try values.decodeIfPresent(Int.self, forKey: .acceptedBaselineCount) ?? 0,
            invalidatedBaselineCount: try values.decodeIfPresent(Int.self, forKey: .invalidatedBaselineCount) ?? 0,
            resolvedPreservationReceiptCount: try values.decodeIfPresent(Int.self, forKey: .resolvedPreservationReceiptCount) ?? 0,
            retainedTombstoneCount: try values.decodeIfPresent(Int.self, forKey: .retainedTombstoneCount) ?? 0)
    }
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
        if recordRebaseContext == nil,
           modelTypes.contains(where: { ownedTypeNames.contains($0.key)
               && $0.value is BigSyncRecordContractProviding.Type }) {
            issues.append("comparison-transport-namespace-unavailable")
        }
        var serverRecordsByName = [String: CKRecord]()
        for record in ownedServerRecords {
            guard record.recordID.zoneID == recordZoneID else {
                issues.append("server-record-wrong-zone:\(record.recordID.recordName)")
                continue
            }
            if serverRecordsByName.updateValue(
                record,
                forKey: record.recordID.recordName
            ) != nil {
                issues.append("duplicate-server-record:\(record.recordID.recordName)")
            }
        }

        let trackedEntities = persistenceRealm.objects(SyncedEntity.self).filter {
            ownedTypeNames.contains($0.entityType) && syncedEntityIsEligibleForActiveAccount($0)
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
        var retainedTombstoneCount = 0
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
                if BigSyncRecordLifecycle.isPhysicalDeletion(object) {
                    issues.append("terminal-local-tombstone:\(recordName)")
                    continue
                }
                if (object as? SoftDeletable)?.isDeleted == true {
                    // Retained Clear is a synchronized versioned save, not a
                    // failed physical-tombstone cleanup. Verify its payload and
                    // acknowledgement below exactly like any retained record.
                    retainedTombstoneCount += 1
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
                if let cached = getRecord(for: trackedEntity) {
                    if cached.recordID != serverRecord.recordID || cached.recordType != entityType
                        || cached.recordChangeTag != serverRecord.recordChangeTag {
                        issues.append("tracking-server-evidence-mismatch:\(recordName)")
                    }
                } else {
                    issues.append("tracking-server-evidence-missing:\(recordName)")
                }
                let comparisonRecord: CKRecord
                if objectClass is BigSyncRecordContractProviding.Type {
                    do {
                        let decoded = try decodedComparisonObject(serverRecord, type: objectClass)
                        comparisonRecord = try BigSyncRecordPayload.record(from: decoded, recordID: serverRecord.recordID)
                        let remoteFields = try BigSyncRecordFingerprint.fields(of: decoded)
                        if let context = recordRebaseContext,
                           let base = targetRealm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: recordName),
                           base.namespace == context.namespace, !base.isComparisonInvalidated {
                            if base.serverChangeTag != serverRecord.recordChangeTag
                                || base.fieldDigests != remoteFields {
                                issues.append("accepted-comparison-server-mismatch:\(recordName)")
                            }
                        } else {
                            issues.append("accepted-comparison-server-evidence-missing:\(recordName)")
                        }
                    } catch {
                        issues.append("server-representation-invalid:\(recordName)")
                        continue
                    }
                } else {
                    comparisonRecord = serverRecord
                }
                let differenceNames = serverDifferencePropertyNames(record: comparisonRecord, object: object)
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
                self.pendingMutationIsEligibleForActiveTransport($0)
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

        var unresolvedSubmissionCount = 0
        var acceptedBaselineCount = 0
        var invalidatedBaselineCount = 0
        var resolvedPreservationReceiptCount = 0
        if let context = recordRebaseContext {
            for target in targetReaderRealms {
                let inspection = try inspectRecordEvidence(in: target, context: context)
                unresolvedSubmissionCount += inspection.unresolvedSubmissionCount
                acceptedBaselineCount += inspection.acceptedBaselineCount
                invalidatedBaselineCount += inspection.invalidatedBaselineCount
                resolvedPreservationReceiptCount += inspection.resolvedPreservationReceiptCount
                issues.append(contentsOf: inspection.issues)
            }
        }
        if unresolvedSubmissionCount > 0 {
            issues.append("unresolved-record-submissions:\(unresolvedSubmissionCount)")
        }

        return BigSyncSynchronizationAudit(
            serverRecordCount: serverRecords.count,
            ownedServerRecordCount: ownedServerRecords.count,
            unknownServerRecordCount: unknownServerRecordCount,
            localObjectCount: localObjectCount,
            trackingRecordCount: trackedEntities.count,
            pendingMutationCount: pendingMutationCount,
            pendingRelationshipCount: pendingRelationshipCount,
            issues: issues.sorted(),
            unresolvedSubmissionCount: unresolvedSubmissionCount,
            acceptedBaselineCount: acceptedBaselineCount,
            invalidatedBaselineCount: invalidatedBaselineCount,
            resolvedPreservationReceiptCount: resolvedPreservationReceiptCount,
            retainedTombstoneCount: retainedTombstoneCount
        )
    }
}
