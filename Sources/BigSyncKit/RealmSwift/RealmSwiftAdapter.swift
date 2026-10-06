    @BigSyncBackgroundActor
    public func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] {
        let preparationCancellationGeneration = cancellationGeneration
        let preparationProvider = realmProvider
        let preparationContext = recordRebaseContext
        let preparationAccount = activeAccountScopeIdentifier
        let preparationIssuer = acknowledgementIssuerID
        func validatePreparationOwner() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == preparationCancellationGeneration,
                  realmProvider === preparationProvider,
                  recordRebaseContext == preparationContext,
                  activeAccountScopeIdentifier == preparationAccount,
                  acknowledgementIssuerID == preparationIssuer else {
                throw CancellationError()
            }
        }
        try validatePreparationOwner()
        if !hasChanges {
            if let persistenceRealm = realmProvider?.persistenceRealm {
                updateHasChanges(realm: persistenceRealm)
            }
            try validatePreparationOwner()
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
            try validatePreparationOwner()

            try await recordsArray.append(
                contentsOf: self.recordsToUpload(
                    withState: uploadingState,
                    limit: innerLimit,
                    restrictedToEntityType: targetEntityType
                )
            )
            // A newer attempt must not relabel an older selection by attaching
            // its current quarantine evidence after a suspended preparation.
            try validatePreparationOwner()
            uploadingState = self.nextStateToSync(after: uploadingState)
            innerLimit = recordLimit - recordsArray.count
        }

        try validatePreparationOwner()
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
                                    recordName: recordName, in: targetReaderRealm,
                                    readBoundary: .ownedTargetTransaction) else { continue }
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

    /// An open transaction flag says nothing about the caller's ownership.
    /// Only the journal-consuming target writer selects the live-write mode;
    /// tracking acknowledgement and quarantine cleanup observe committed data.
    private enum ComparisonReceiptReadBoundary {
        case committed
        case ownedTargetTransaction
    }

    @BigSyncBackgroundActor
    private func comparisonReceiptIsCurrent(
        _ receipt: RealmSwiftAcceptedComparisonReceipt,
        recordName: String, in realm: Realm,
        readBoundary: ComparisonReceiptReadBoundary = .committed
    ) throws -> Bool {
        guard recordRebaseContext == receipt.context,
              BigSyncRecordBaseline.isEnabled(in: realm) else { return false }
        let receiptCancellationGeneration = cancellationGeneration
        let provider = realmProvider
        func validateOwner() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == receiptCancellationGeneration,
                  recordRebaseContext == receipt.context,
                  realmProvider === provider else { throw CancellationError() }
        }
        try validateOwner()
        let evidence: Realm
        switch readBoundary {
        case .committed:
            // The helper also accepts an already-frozen view without advancing
            // it. Never borrow another target owner's provisional transaction.
            evidence = committedRealmReadSnapshot(in: realm)
            try validateOwner()
            guard let identity = BigSyncMutationTrackingRegistry
                .currentMutationJournalIdentity(in: evidence),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier
                    == receipt.context.binding else {
                throw CancellationError()
            }
        case .ownedTargetTransaction:
            precondition(realm.isInWriteTransaction && !realm.isFrozen)
            evidence = realm
            try receipt.context.validate(in: evidence)
        }
        // Snapshot refresh and the registry's caller-supplied identity provider
        // can revoke/recreate this attempt synchronously, without any await.
        try validateOwner()
        // Resolve model-owned contract code before sampling live target rows.
        // A custom model initializer/contract can synchronously change state;
        // the final predicate must inspect the value after that callout.
        let expectedSignature: String?
        if let typeName = recordName.split(separator: ".", maxSplits: 1).first.map(String.init),
           let type = realmObjectClass(name: typeName) {
            expectedSignature = try BigSyncCompiledRecordContract.compile(type.init())?.signature
        } else {
            expectedSignature = nil
        }
        try validateOwner()
        guard let current = evidence.object(
            ofType: BigSyncRecordBaseline.self, forPrimaryKey: recordName
        ), !current.isComparisonInvalidated,
           current.namespace == receipt.context.namespace,
           current.revision == receipt.revision else { return false }
        return expectedSignature == nil || expectedSignature == current.schemaSignature
    }

#if DEBUG
    /// Exercises the actual receipt boundary without exposing its private
    /// admitted-receipt type or manufacturing a transport acknowledgement.
    @BigSyncBackgroundActor
    func _test_comparisonReceiptIsCurrent(
        context: BigSyncRecordRebaseContext, revision: String,
        recordName: String, in realm: Realm,
        ownsTargetTransaction: Bool = false
    ) throws -> Bool {
        try comparisonReceiptIsCurrent(
            .init(context: context, revision: revision),
            recordName: recordName, in: realm,
            readBoundary: ownsTargetTransaction ? .ownedTargetTransaction : .committed
        )
    }
#endif

    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord], matchingPreparedUploads prepared: [PreparedRecordUpload]) async throws {
