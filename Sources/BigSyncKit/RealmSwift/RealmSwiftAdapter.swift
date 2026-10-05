extension RealmSwiftAdapter {
    /// A retained record cannot be physically deleted by an inbound deletion.
    /// If the exact retained tombstone is subsequently accepted by CloudKit,
    /// that successful upload restores the server representation and resolves
    /// only the quarantine for that record. Other semantic quarantines remain
    /// unresolved until their own accepted disposition or committed feed
    /// evidence proves them.
    @BigSyncBackgroundActor
    private func retireAcceptedRetainedDeletionQuarantines(
        savedRecords: [CKRecord]
    ) async throws {
        guard !savedRecords.isEmpty,
              let context = recordRebaseContext,
              let provider = realmProvider,
              let tracking = provider.persistenceRealm else { return }

        let savedByName = Dictionary(
            uniqueKeysWithValues: savedRecords.map {
                ($0.recordID.recordName, $0)
            }
        )

        func eligibleLineageIDs(in trackingRealm: Realm) -> Set<String> {
            var result = Set<String>()
            for quarantine in activeInboundSemanticQuarantines(
                accountScopeIdentifier: context.account,
                in: trackingRealm
            ) {
                guard quarantine.eventKind == "deletion",
                      quarantine.validationCode
                        == "retained-record-physically-deleted",
                      quarantine.semanticScopeIdentifier
                        == "retained-physical-deletion:"
                            + quarantine.recordName,
                      let saved = savedByName[quarantine.recordName],
                      saved.recordType == quarantine.entityType,
                      let type = self.realmObjectClass(
                        name: quarantine.entityType
                      ),
                      BigSyncRecordLifecycle.retainsTombstone(type),
                      let liveTarget = provider
                        .targetReaderRealmPerSchemaName[
                            quarantine.entityType
                        ],
                      let objectID = self.getObjectIdentifier(
                        recordName: quarantine.recordName,
                        entityType: quarantine.entityType
                      ) else {
                    continue
                }

                // Target eligibility is a read-only proof. Never let another
                // target owner's provisional resurrection/tombstone decide
                // whether durable quarantine evidence can be retired.
                let target = committedRealmReadSnapshot(in: liveTarget)
                guard let object = target.object(
                    ofType: type,
                    forPrimaryKey: objectID
                ), let tombstone = object as? SoftDeletable,
                   tombstone.isDeleted else {
                    continue
                }

                // The tracking receipt must be terminal for the exact submitted
                // record. A newer pending generation means this acknowledgement
                // did not consume the prepared upload.
                guard let entity = trackingRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: quarantine.recordName
                ), entity.entityType == quarantine.entityType,
                   entity.entityState == .synced,
                   entity.pendingGeneration == nil,
                   let cached = self.getRecord(for: entity),
                   cached.recordID.recordName
                    == saved.recordID.recordName,
                   cached.recordID.zoneID == saved.recordID.zoneID,
                   cached.recordChangeTag == saved.recordChangeTag else {
                    continue
                }
                result.insert(quarantine.lineageID)
            }
            return result
        }

        let committedTracking = committedRealmReadSnapshot(in: tracking)
        let candidates = eligibleLineageIDs(in: committedTracking)
        guard !candidates.isEmpty else { return }

        try await tracking.asyncWritePreservingOwnership {
            try Task.checkCancellation()
            guard !cancelSync, recordRebaseContext == context,
                  activeAccountScopeIdentifier == context.account else {
                throw CancellationError()
            }
            // Candidate selection can become stale while the tracking writer
            // waits. Revalidate the exact detached lineage IDs after admission;
            // no managed quarantine row crosses the suspension.
            let lineageIDs = candidates.intersection(
                eligibleLineageIDs(in: tracking)
            )
            guard !lineageIDs.isEmpty else { return }
            let receiptIDs = try Self.retireQuarantines(
                Array(lineageIDs),
                in: tracking
            )
            Self.removeUnreferencedPageReceipts(
                receiptIDs,
                in: tracking
            )
        }
    }

    private func requiredLifecycleChoice(lifetimes: (local: String?, incoming: String?)) throws
        -> BigSyncRecordConflictChoice? {
        guard lifetimes.local != lifetimes.incoming,
              let incoming = try BigSyncLifetimeID.prefersIncoming(local: lifetimes.local,
                  incoming: lifetimes.incoming) else { return nil }
        return incoming ? .useIncoming : .keepLocal
    }

    @BigSyncBackgroundActor
    public func unresolvedRecordConflicts() throws -> [BigSyncRecordConflictSnapshot] {
        guard let context = recordRebaseContext else { return [] }
        var result = [BigSyncRecordConflictSnapshot]()
        var seen = Set<String>()
        for realm in realmProvider?.targetReaderRealms ?? [] {
            guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordConflict.className() }) else {
                continue
            }
            realm.refresh()
            for row in realm.objects(BigSyncRecordConflict.self).where({
                $0.namespace == context.namespace && !$0.isResolved
            }) where seen.insert(row.id).inserted {
                let local = try BigSyncRecordPayload.decode(row.localPayload)
                let incoming = try BigSyncRecordPayload.decode(row.incomingPayload)
                let type = realmObjectClass(name: row.entityType)
                let policy = type.flatMap { $0 as? BigSyncRecordContractProviding.Type }?.bigSyncRecordContract.policy
                let lifetimes = conflictLifetimes(local: local, incoming: incoming, policy: policy)
                result.append(.init(id: row.id, recordName: row.recordName, entityType: row.entityType,
                    reason: row.reason, generation: row.generation, createdAt: row.createdAt,
                    localText: local["text"] as? String, incomingText: incoming["text"] as? String,
                    localTitle: local["title"] as? String, incomingTitle: incoming["title"] as? String,
                    localLifetime: lifetimes.local, incomingLifetime: lifetimes.incoming,
                    localIsDeleted: BigSyncCloudKitBooleanCodec.decode(local["isDeleted"]) == true,
                    incomingIsDeleted: BigSyncCloudKitBooleanCodec.decode(incoming["isDeleted"]) == true,
                    requiredChoice: try requiredLifecycleChoice(lifetimes: lifetimes)))
            }
        }
        return result.sorted { $0.id < $1.id }
    }

    private func requireRecordEvidenceSchema(in realm: Realm, entityType: String) throws {
        let present = Set(realm.schema.objectSchema.map(\.className))
        guard BigSyncLocalRecordEvidence.objectTypes.allSatisfy({ present.contains($0.className()) }) else {
            throw BigSyncRecordContractError.missingEvidenceSchema(entityType)
        }
    }

    /// A durable target resolution is the authority for retiring its own
    /// quarantine. Replay finishes this tracking phase after a crash without
    /// claiming another inbound page or advancing any cursor.
    @BigSyncBackgroundActor
    @discardableResult
    private func retireResolvedRecordConflictQuarantines(
        validateAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void = {}
    ) async throws -> Set<String> {
        guard let context = recordRebaseContext, let provider = realmProvider,
              let tracking = provider.persistenceRealm else { return [] }
        let targetRealms = provider.targetReaderRealms ?? []
        var resolvedConflictIDs = Set<String>()
        for realm in targetRealms {
            let committedRealm = committedRealmReadSnapshot(in: realm)
            guard committedRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncRecordConflict.className()
            }) else { continue }
            for row in committedRealm.objects(BigSyncRecordConflict.self).where({
                $0.namespace == context.namespace
                    && $0.isResolved
                    && !$0.isPreservationReceipt
            }) {
                resolvedConflictIDs.insert(row.id)
            }
        }
        guard !resolvedConflictIDs.isEmpty else { return [] }
        var retiredConflictIDs = Set<String>()
        try await tracking.asyncWritePreservingOwnership {
            // Retiring quarantine/page evidence is a separate mutation from
            // the durable target decision. Its transaction wait can outlive
            // the UI account lease even when adapter namespace strings have
            // not yet changed. Reuse the caller's final-write authority here.
            try validateAuthority()
            guard recordRebaseContext == context else {
                throw CancellationError()
            }

            // The target resolution is the authority for this cleanup. It can
            // change while the tracking writer waits, so resample committed
            // target state after admission rather than carrying managed rows or
            // trusting the earlier candidate set.
            var stillResolved = Set<String>()
            for realm in targetRealms {
                let committedRealm = committedRealmReadSnapshot(in: realm)
                guard committedRealm.schema.objectSchema.contains(where: {
                    $0.className == BigSyncRecordConflict.className()
                }) else { continue }
                for row in committedRealm.objects(
                    BigSyncRecordConflict.self
                ).where({
                    $0.namespace == context.namespace
                        && $0.isResolved
                        && !$0.isPreservationReceipt
                }) where resolvedConflictIDs.contains(row.id) {
                    stillResolved.insert(row.id)
                }
            }
            guard !stillResolved.isEmpty else { return }
            let resolvedScopes = Set(
                stillResolved.map { "record-conflict:" + $0 }
            )
            let quarantines = activeInboundSemanticQuarantines(
                accountScopeIdentifier: context.account,
                in: tracking
            ).filter { row in
                guard let scope = row.semanticScopeIdentifier else {
                    return false
                }
                return resolvedScopes.contains(scope)
                    && row.accountScopeIdentifier == context.account
                    && row.containerIdentifier
                        == self.activeContainerIdentifier
                    && row.databaseScopeRawValue
                        == self.activeDatabaseScopeRawValue
                    && row.zoneOwnerName == self.recordZoneID.ownerName
                    && row.zoneName == self.recordZoneID.zoneName
            }
            let receiptIDs = try Self.retireQuarantines(
                quarantines.map(\.lineageID),
                in: tracking
            )
            Self.removeUnreferencedPageReceipts(
                receiptIDs,
                in: tracking
            )
            retiredConflictIDs = stillResolved
        }
        // Only resolutions revalidated after tracking admission completed this
        // phase. A later resolution retains its archive for a later pass.
        return retiredConflictIDs
    }

public extension RealmSwiftAdapter {
    @BigSyncBackgroundActor
    func exportPreservedRecordConflicts() throws -> Data {
        guard let context = recordRebaseContext else { throw CancellationError() }
        var snapshots = [[String: Any]]()
        var identities = Set<String>()
        for realm in realmProvider?.targetReaderRealms ?? [] {
            guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordConflict.className() }) else { continue }
            realm.refresh()
            for row in realm.objects(BigSyncRecordConflict.self).where({ $0.namespace == context.namespace && !$0.isPreservationReceipt }) {
                guard identities.insert(row.id).inserted else { continue }
                snapshots.append(["id": row.id, "recordName": row.recordName,
                    "entityType": row.entityType, "generation": row.generation,
                    "schemaSignature": row.schemaSignature, "reason": row.reason,
                    "createdAt": row.createdAt, "resolved": row.isResolved,
                    "localPayload": row.localPayload, "incomingPayload": row.incomingPayload])
            }
        }
        return try PropertyListSerialization.data(fromPropertyList:
            ["format": "BigSyncPreservedConflicts-v1", "records": snapshots], format: .binary, options: 0)
    }

    /// Explicit archive cleanup. Unresolved values are never evicted to make
    /// room, and no pending submission or mutation generation is touched.
