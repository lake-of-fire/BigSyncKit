        return objectIsEligibleForActiveAccount(
            object,
            entityType: syncedEntity.entityType
        )
    }

    /// Preserve the original response owner through legacy routing as well as
    /// comparison-backed records. This closure retains existing transport facts,
    /// including nil/unbound values; it does not allocate another generation.
    /// Its checks never call the registry, model code or Realm notifications.
    @BigSyncBackgroundActor
    func captureRecordResponseAuthority() -> @BigSyncBackgroundActor @Sendable () throws -> Void {
        let provider = realmProvider
        let generation = cancellationGeneration
        let account = activeAccountScopeIdentifier
        let context = recordRebaseContext
        let container = activeContainerIdentifier
        let databaseScope = activeDatabaseScopeRawValue
        let binding = activeReplicaBindingGenerationIdentifier
        return { [self] in
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == generation,
                  realmProvider === provider,
                  activeAccountScopeIdentifier == account,
                  recordRebaseContext == context,
                  activeContainerIdentifier == container,
                  activeDatabaseScopeRawValue == databaseScope,
                  activeReplicaBindingGenerationIdentifier == binding else {
                throw CancellationError()
            }
        }
    }

    /// Existing callers retain their current transaction semantics. A legacy
    /// response owns the tracking write, not a shared target writer, and opts
    /// into a committed target observation at that boundary.
    private enum PreparedGenerationReadBoundary {
        case current
        case committedTarget
    }

    @BigSyncBackgroundActor
    private func preparedGenerationIsEligibleForActiveTransport(
        recordName: String,
        entityType: String,
        generation: String,
        readBoundary: PreparedGenerationReadBoundary = .current
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
        let evidence: Realm
        switch readBoundary {
        case .current: evidence = targetRealm
        case .committedTarget:
            evidence = committedRealmReadSnapshot(in: targetRealm)
        }
        guard let mutation = evidence.object(
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
        try await didDelete(
            recordIDs: deletedRecordIDs, matchingGenerations: matchingGenerations,
            validateResponseAuthority: captureRecordResponseAuthority()
        )
    }

    @BigSyncBackgroundActor
    func didDelete(
        recordIDs deletedRecordIDs: [CKRecord.ID],
        matchingGenerations: [String: String],
        validateResponseAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else {
            if !deletedRecordIDs.isEmpty { try validateResponseAuthority() }
            return
        }
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
            try validateResponseAuthority()
            try await persistenceRealm.asyncWritePreservingOwnership {
                try validateResponseAuthority()
                for recordID in chunk {
                    try validateResponseAuthority()
                    let name = recordID.recordName
                    guard let deletedGeneration = matchingGenerations[name],
                          let selected = persistenceRealm.object(
                            ofType: SyncedEntity.self, forPrimaryKey: name
                          ), selected.pendingGeneration == deletedGeneration else { continue }
                    let entityType = selected.entityType
                    guard preparedGenerationIsEligibleForActiveTransport(
                        recordName: name, entityType: entityType,
                        generation: deletedGeneration, readBoundary: .committedTarget
                    ) else { continue }
                    if let type = realmObjectClass(name: entityType),
                       BigSyncRecordLifecycle.retainsTombstone(type) {
                        throw BigSyncRecordContractError.unexpectedPhysicalDeletion(name)
                    }
                    try validateResponseAuthority()
                    guard let current = persistenceRealm.object(
                        ofType: SyncedEntity.self, forPrimaryKey: name
                    ), current.entityType == entityType,
                       current.pendingGeneration == deletedGeneration,
                       trackingMutationIsEligibleForActiveTransport(current) else { continue }
                    current.state = SyncedEntityState.deletedRemotely.rawValue
                    current.clearPendingMutation()
                    acknowledgedGenerations[name] = deletedGeneration
                    acknowledgedEntityTypes[name] = entityType
                }
                try validateResponseAuthority()
            }
        }

        if !acknowledgedGenerations.isEmpty,
           realmProvider.targetReaderRealms != nil {
            // This is a new mutation phase. An old response may keep its earlier
            // durable acknowledgement but cannot consume a successor's journal.
            try validateResponseAuthority()
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
                try validateResponseAuthority()
                let targetReaderRealm = group.realm
                let generations = group.generations
                try await targetReaderRealm.asyncWritePreservingOwnership {
                    try validateResponseAuthority()
                    for (recordName, generation) in generations {
                        try validateResponseAuthority()
                        guard let mutation = targetReaderRealm.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: recordName
                        ), mutation.generation == generation,
                           pendingMutationIsEligibleForActiveTransport(mutation) else { continue }
                        targetReaderRealm.delete(mutation)
                    }
                    try validateResponseAuthority()
                }
                try validateResponseAuthority()
                let newerMutations = pendingMutationSnapshots(
                    for: generations.keys,
                    in: targetReaderRealm
                )
                try validateResponseAuthority()
                try await forwardPendingMutations(
                    newerMutations,
                    in: targetReaderRealm
                )
            }
        }

        guard (try? validateResponseAuthority()) != nil else { return }
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
        try await requeueMissingServerRecords(
            recordIDs, matchingPreparedGenerations: matchingPreparedGenerations,
            validateResponseAuthority: captureRecordResponseAuthority()
        )
    }

    /// The preparation-aware entry passes its original validator through this
    /// handoff. Capturing here would let an old mixed response adopt a resumed
    /// attempt after its comparison-backed records had already completed.
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String],
        validateResponseAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void
    ) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else {
            if !recordIDs.isEmpty { try validateResponseAuthority() }
            return
        }

        for chunk in recordIDs.chunks(ofCount: 1000) {
            try validateResponseAuthority()
            try await persistenceRealm.asyncWritePreservingOwnership {
                try validateResponseAuthority()
                for recordID in chunk {
                    try validateResponseAuthority()
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
                          let selected = persistenceRealm.object(
                            ofType: SyncedEntity.self, forPrimaryKey: recordName
                          ), selected.pendingGeneration == preparedGeneration else { continue }
                    let entityType = selected.entityType
                    guard preparedGenerationIsEligibleForActiveTransport(
                        recordName: recordName, entityType: entityType,
                        generation: preparedGeneration, readBoundary: .committedTarget
                    ) else { continue }
                    // Committed-target refresh can deliver notifications.
                    // Recheck owner and the exact tracking generation/binding
                    // after that callout, before clearing its CAS template.
                    try validateResponseAuthority()
                    guard let current = persistenceRealm.object(
                        ofType: SyncedEntity.self, forPrimaryKey: recordName
                    ), current.entityType == entityType,
                       current.pendingGeneration == preparedGeneration,
                       trackingMutationIsEligibleForActiveTransport(current) else { continue }
                    current.entityState = .new
                    current.encodedRecord = nil
                    // Keep the prepared generation. Its durable journal still
                    // owns retrying this mutation; no new edit is authored.
                }
                // Includes successful early exits from per-record eligibility.
                try validateResponseAuthority()
            }
        }
        // A completed write is not rolled back by late cancellation. Suppress
        // only its optional status publication if a successor now owns it.
        guard (try? validateResponseAuthority()) != nil else { return }
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
