    @BigSyncBackgroundActor
    public func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] {
        if !hasChanges {
            if let persistenceRealm = realmProvider?.persistenceRealm {
                updateHasChanges(realm: persistenceRealm)
            }
            if !hasChanges {
                return []
            }
        }

        var recordsArray = [PreparedRecordUpload]()
        let recordLimit = limit == 0 ? Int.max : limit
        var uploadingState = SyncedEntityState.new
        let targetEntityType = restrictedToEntityType ?? prioritizedEntityTypeWithPendingUploadOrDeletion()

        var innerLimit = recordLimit
        while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deletedLocally.rawValue {
            guard !cancelSync else { throw CancellationError() }

            try await recordsArray.append(
                contentsOf: self.recordsToUpload(
                    withState: uploadingState,
                    limit: innerLimit,
                    restrictedToEntityType: targetEntityType
                )
            )
            uploadingState = self.nextStateToSync(after: uploadingState)
            innerLimit = recordLimit - recordsArray.count
        }

        return try attachingRetainedDeletionQuarantineEvidence(to: recordsArray)
    }
    @BigSyncBackgroundActor
    private func acknowledgeUploadReceipts(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String],
        comparisonReceipts: [String: RealmSwiftAcceptedComparisonReceipt],
        retainedDeletionCleanup: RetainedDeletionQuarantineCleanup? = nil
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()

        for chunk in savedRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }

            try await persistenceRealm.asyncWritePreservingOwnership {
                var acknowledgedInThisWrite = [String: String]()
                for record in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }

                    guard let syncedEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: record.recordID.recordName
                    ), let uploadedGeneration = matchingGenerations[record.recordID.recordName],
                       syncedEntity.pendingGeneration == uploadedGeneration,
                       preparedGenerationIsEligibleForActiveTransport(
                           recordName: record.recordID.recordName,
                           entityType: syncedEntity.entityType,
                           generation: uploadedGeneration
                       ) else { continue }
                    if let receipt = comparisonReceipts[record.recordID.recordName] {
                        guard let target = realmProvider.targetReaderRealmPerSchemaName[syncedEntity.entityType],
                              try comparisonReceiptIsCurrent(receipt,
                                recordName: record.recordID.recordName, in: target) else { continue }
                    }
                    try Task.checkCancellation()
                    try save(record: record, for: syncedEntity)
                    syncedEntity.state = SyncedEntityState.synced.rawValue
                    syncedEntity.clearPendingMutation()
                    acknowledgedInThisWrite[record.recordID.recordName] = uploadedGeneration
                    acknowledgedGenerations[record.recordID.recordName] = uploadedGeneration
                    acknowledgedEntityTypes[record.recordID.recordName] =
                        syncedEntity.entityType
                }
                if let retainedDeletionCleanup, !acknowledgedInThisWrite.isEmpty {
#if DEBUG
                    try _testAfterAcceptedRetainedDeletionTrackingAdmission?()
#endif
                    try retireAcceptedRetainedDeletionQuarantines(
                        retainedDeletionCleanup,
                        acknowledgedGenerations: acknowledgedInThisWrite,
                        comparisonReceipts: comparisonReceipts,
                        in: persistenceRealm
                    )
                    try requireRetainedDeletionQuarantinesSettled(
                        retainedDeletionCleanup,
                        acknowledgedGenerations: acknowledgedInThisWrite,
                        in: persistenceRealm
                    )
                }
            }
            await Task.yield()
        }

        if !acknowledgedGenerations.isEmpty {
            if realmProvider.targetReaderRealms != nil {
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
                            if let receipt = comparisonReceipts[recordName] {
                                guard try comparisonReceiptIsCurrent(receipt,
                                    recordName: recordName, in: targetReaderRealm) else { continue }
                            }
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
        }

        updateHasChanges(realm: persistenceRealm)
    }
    @BigSyncBackgroundActor
    private func preparedComparisonBase(for record: CKRecord, entityType: String) throws -> BigSyncPreparedRecordBase? {
        guard let context = recordRebaseContext,
              let realm = realmProvider?.targetReaderRealmPerSchemaName[entityType],
              BigSyncRecordBaseline.isEnabled(in: realm),
              let type = realmObjectClass(name: entityType),
              try recordRebasePolicy(for: type.init()) != .disabled else { return nil }
        let object = try decodedComparisonObject(record, type: type)
        let base = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: record.recordID.recordName)
        return .init(context: context,
                     revision: base?.revision,
                     fields: try BigSyncRecordFingerprint.fields(of: object),
                     schemaSignature: try BigSyncCompiledRecordContract.compile(object)?.signature ?? "")
    }

    @BigSyncBackgroundActor
    private func comparisonReceiptIsCurrent(
        _ receipt: RealmSwiftAcceptedComparisonReceipt,
        recordName: String, in realm: Realm
    ) throws -> Bool {
        guard recordRebaseContext == receipt.context,
              BigSyncRecordBaseline.isEnabled(in: realm) else { return false }
        if realm.isInWriteTransaction {
            try receipt.context.validate(in: realm)
        } else {
            // The tracking phase observes the target Realm; it must not call
            // the public write-transaction-only mutation verification API.
            realm.refresh()
            guard let identity = BigSyncMutationTrackingRegistry.currentMutationJournalIdentity(in: realm),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier == receipt.context.binding else {
                throw CancellationError()
            }
        }
        guard let current = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: recordName),
              !current.isComparisonInvalidated,
              current.namespace == receipt.context.namespace,
              current.revision == receipt.revision else { return false }
        if let typeName = recordName.split(separator: ".", maxSplits: 1).first.map(String.init),
           let type = realmObjectClass(name: typeName),
           let compiled = try BigSyncCompiledRecordContract.compile(type.init()),
           compiled.signature != current.schemaSignature { return false }
        return true
    }

    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord], matchingPreparedUploads prepared: [PreparedRecordUpload]) async throws {
