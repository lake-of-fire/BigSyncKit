        return objectIsEligibleForActiveAccount(
            object,
            entityType: syncedEntity.entityType
        )
    }

    @BigSyncBackgroundActor
    private func preparedGenerationIsEligibleForActiveTransport(
        recordName: String,
        entityType: String,
        generation: String
    ) -> Bool {
        guard let persistenceEntity = realmProvider?.persistenceRealm?.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: recordName
        ), persistenceEntity.entityType == entityType,
        persistenceEntity.pendingGeneration == generation,
        trackingMutationIsEligibleForActiveTransport(
            persistenceEntity
        ) else {
            return false
        }
        // For unscoped models, the tracking row already carries every
        // transport fence needed to acknowledge this prepared generation.
        // The target journal may legitimately have advanced while CloudKit
        // was saving the older record state; acknowledge the old tracking row,
        // then forward the surviving newer journal generation below.
        guard accountScopePropertyByClassName[entityType] != nil else {
            return true
        }
        guard let targetRealm = realmProvider?
            .targetReaderRealmPerSchemaName[entityType] else {
            return false
        }
        guard let mutation = targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ) else {
            return false
        }
        guard mutation.generation == generation else {
            return false
        }
        return pendingMutationIsEligibleForActiveTransport(mutation)
    }

    /// The owning synchronizer has awaited callback/adapter quiescence and
    /// revalidated its run before calling this synchronous preparation hook.
    /// Unlike unsetCancellation(), it must not open the target Realm, restart
    /// setup, or drain observed journals before migration provenance exists.
    @BigSyncBackgroundActor
    func prepareForFencedMigrationAfterCancellation() throws {
        try Task.checkCancellation()
        isPreparingFencedMigration = true
        cancelSync = false
    }

    /// Acknowledges the successful subset of a previously prepared deletion.
    @BigSyncBackgroundActor
    func acknowledgeDeletedRecordIDs(
        _ recordIDs: [CKRecord.ID],
        from batch: RealmSwiftPreparedDeletionBatch
    ) async throws {
        // Deletion acknowledgements consume tombstones, so cancellation must
        // be observed before entering the first persistence transaction.
        await Task.yield()
        try Task.checkCancellation()
        guard batch.issuerID == acknowledgementIssuerID else {
            throw RealmSwiftAdapterAcknowledgementError.batchBelongsToAnotherAdapter
        }
        let preparedRecordIDs = Set(batch.recordIDs)
        guard recordIDs.allSatisfy({ preparedRecordIDs.contains($0) }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        try await didDelete(
            recordIDs: recordIDs,
            matchingPreparedDeletions: batch.prepared
        )
    }

    @BigSyncBackgroundActor
    public func didDelete(
        recordIDs deletedRecordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        guard Set(deletedRecordIDs).count == deletedRecordIDs.count,
              deletedRecordIDs.allSatisfy({ $0.zoneID == recordZoneID }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        // Adopted contracts require the preparation-time baseline and staged
        // candidate, not merely a record-name/generation map.
        for recordID in deletedRecordIDs {
            if modelTypes.contains(where: { name, type in
                recordID.recordName.hasPrefix(name + ".") && type is BigSyncRecordContractProviding.Type
            }) {
                throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
            }
        }
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()

        for chunk in deletedRecordIDs.chunks(ofCount: 1000) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWritePreservingOwnership {
                for recordID in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    guard let syncedEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: recordID.recordName
                    ), let deletedGeneration = matchingGenerations[recordID.recordName],
                       syncedEntity.pendingGeneration == deletedGeneration,
                       preparedGenerationIsEligibleForActiveTransport(
                           recordName: recordID.recordName,
                           entityType: syncedEntity.entityType,
                           generation: deletedGeneration
                       ) else {
                        continue
                    }
                    if let type = realmObjectClass(name: syncedEntity.entityType),
                       BigSyncRecordLifecycle.retainsTombstone(type) {
                        throw BigSyncRecordContractError.unexpectedPhysicalDeletion(recordID.recordName)
                    }
                    syncedEntity.state = SyncedEntityState.deletedRemotely.rawValue
                    syncedEntity.clearPendingMutation()
                    acknowledgedGenerations[recordID.recordName] = deletedGeneration
                    acknowledgedEntityTypes[recordID.recordName] =
                        syncedEntity.entityType
                }
            }
        }

        if !acknowledgedGenerations.isEmpty,
           realmProvider.targetReaderRealms != nil {
            var generationsByRealm = [
                String: (realm: Realm, generations: [String: String])
            ]()
            for (recordName, generation) in acknowledgedGenerations {
                guard let entityType = acknowledgedEntityTypes[recordName],
                      let targetReaderRealm = realmProvider
                        .targetReaderRealmPerSchemaName[entityType],
                      targetReaderRealm.schema.objectSchema.contains(where: {
                          $0.className == BigSyncPendingMutation.className()
                      }) else { continue }
                let realmIdentity = BigSyncMutationTrackingRegistry.identity(
                    for: targetReaderRealm.configuration
                )
                generationsByRealm[
                    realmIdentity,
                    default: (targetReaderRealm, [:])
                ].generations[recordName] = generation
            }
            for group in generationsByRealm.values {
                let targetReaderRealm = group.realm
                let generations = group.generations
                try await targetReaderRealm.asyncWritePreservingOwnership {
                    for (recordName, generation) in generations {
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                    guard let mutation = targetReaderRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: recordName
                    ), mutation.generation == generation,
                        pendingMutationIsEligibleForActiveTransport(
                            mutation
                        ) else { continue }
                        targetReaderRealm.delete(mutation)
                    }
                }
                let newerMutations = pendingMutationSnapshots(
                    for: generations.keys,
                    in: targetReaderRealm
                )
                try await forwardPendingMutations(
                    newerMutations,
                    in: targetReaderRealm
                )
            }
        }

        updateHasChanges(realm: persistenceRealm)
    }

    @BigSyncBackgroundActor
    public func didFinishImport() async throws {
        try await didFinishImport(progress: { _ in })
    }

        let committedPersistence = committedRealmReadSnapshot(
            in: persistenceRealm
        )
        updateHasChanges(realm: committedPersistence)
        return hasChanges
    }

    /// Requeues only records whose pending generation is still the generation
    /// sent to CloudKit. A newer local mutation remains pending untouched.
    @BigSyncBackgroundActor
    public func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }

        for chunk in recordIDs.chunks(ofCount: 1000) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWritePreservingOwnership {
                for recordID in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    let recordName = recordID.recordName
                    guard recordID.zoneID == recordZoneID else {
                        throw BigSyncRecordRebaseError.inconsistentReceipt(recordName)
                    }
                    if let entityType = recordName.split(separator: ".", maxSplits: 1).first.map(String.init),
                       let type = realmObjectClass(name: entityType),
                       type is BigSyncRecordContractProviding.Type {
                        throw BigSyncRecordRebaseError.inconsistentReceipt(recordName)
                    }
                    guard let preparedGeneration = matchingPreparedGenerations[recordName],
                          let syncedEntity = persistenceRealm.object(
                            ofType: SyncedEntity.self,
                            forPrimaryKey: recordName
                          ),
                          syncedEntity.pendingGeneration == preparedGeneration,
                          preparedGenerationIsEligibleForActiveTransport(
                              recordName: recordName,
                              entityType: syncedEntity.entityType,
                              generation: preparedGeneration
                          ) else {
                        continue
                    }
                    syncedEntity.entityState = .new
                    syncedEntity.encodedRecord = nil
                    // Keep the prepared generation. The matching journal row
                    // remains the authority for retrying this exact mutation.
                }
            }
        }
        updateHasChanges(realm: persistenceRealm)
    }

    /// Updates CloudKit's opaque system fields for a deletion conflict without
    /// touching the target object or changing which journal generation owns
    /// the tombstone. A newer local mutation makes the prepared response stale
    /// and is deliberately ignored.
    @BigSyncBackgroundActor
    public func rebasePendingDeletionMetadata(
        using serverRecords: [CKRecord],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              !serverRecords.isEmpty else { return }
