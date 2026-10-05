    var _testAfterDisappearanceTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeMissingServerTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeRemoteDeletionTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?

            comparisonReceipts: [:]
        )
    }

    @BigSyncBackgroundActor
    private func acknowledgeUploadReceipts(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String],
        comparisonReceipts: [String: RealmSwiftAcceptedComparisonReceipt]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()

        for chunk in savedRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }

            //            await persistenceRealm.asyncRefresh()
            try await persistenceRealm.asyncWritePreservingOwnership {
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
                    acknowledgedGenerations[record.recordID.recordName] = uploadedGeneration
                    acknowledgedEntityTypes[record.recordID.recordName] =
                        syncedEntity.entityType
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
    public func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] {

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
        // Validate the complete response before any target/tracking mutation.
        // Name alone is not identity: zone and record type must also match.
        var preparedByID = [CKRecord.ID: PreparedRecordUpload]()
        for item in prepared {
            guard item.record.recordID.zoneID == recordZoneID,
                  preparedByID.updateValue(item, forKey: item.record.recordID) == nil else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(item.record.recordID.recordName)
            }
        }
        var seen = Set<CKRecord.ID>()
        var generations = [String: String]()
        var admittedRecords = [CKRecord]()
        var retainedCleanupRecords = [CKRecord]()
        var accepted = [String: RealmSwiftAcceptedComparisonReceipt]()
        typealias ReceiptItem = (saved: CKRecord, type: Object.Type, proof: BigSyncPreparedRecordBase)
        var groups = [String: (realm: Realm, items: [ReceiptItem])]()
        for saved in savedRecords {
            guard seen.insert(saved.recordID).inserted,
                  let item = preparedByID[saved.recordID],
                  item.record.recordType == saved.recordType else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            guard let proof = item.comparisonBase else {
                // Only genuinely non-comparison records use the old path.
                // A missing proof on an enabled model is not opt-out consent.
                if let realm = realmProvider?.targetReaderRealmPerSchemaName[saved.recordType],
                   BigSyncRecordBaseline.isEnabled(in: realm), recordRebaseContext != nil,
                   let type = realmObjectClass(name: saved.recordType),
                   try recordRebasePolicy(for: type.init()) != .disabled {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
                }
                generations[saved.recordID.recordName] = item.generation
                admittedRecords.append(saved)
                continue
            }
            // A superseded binding's receipt has no authority in this adapter.
            // In particular it must not be passed to the legacy fallback.
            guard proof.context == recordRebaseContext else { continue }
            guard let realm = realmProvider?.targetReaderRealmPerSchemaName[saved.recordType],
                  let type = realmObjectClass(name: saved.recordType),
                  BigSyncRecordBaseline.isEnabled(in: realm) else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            let decoded = try decodedComparisonObject(saved, type: type)
            let savedFields = try BigSyncRecordFingerprint.fields(of: decoded)
            guard savedFields == proof.fields,
                  (try BigSyncCompiledRecordContract.compile(decoded)?.signature ?? "") == proof.schemaSignature else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            let key = BigSyncMutationTrackingRegistry.identity(for: realm.configuration)
            groups[key, default: (realm, [])].items.append((saved, type, proof))
        }
        for key in groups.keys.sorted() {
            guard let group = groups[key] else { continue }
            let realm = group.realm
            for chunk in group.items.chunks(ofCount: 500) {
                try await realm.asyncWritePreservingOwnership {
                    for item in chunk {
                        try Task.checkCancellation()
                        let proof = item.proof, saved = item.saved
                        let name = saved.recordID.recordName
                        guard !cancelSync, recordRebaseContext == proof.context else { throw CancellationError() }
                        try proof.context.validate(in: realm)
                        let base = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
                        // Resume the exact accepted receipt if the target base
                        // committed before tracking acknowledgement. Tag and
                        // payload must both match; a newer base is not a match.
                        let alreadyInstalled = base?.isComparisonInvalidated == false
                            && base?.namespace == proof.context.namespace
                            && base?.fieldDigests == proof.fields
                            && base?.schemaSignature == proof.schemaSignature
                            && base?.serverChangeTag == saved.recordChangeTag
                            && (!(item.type is BigSyncRecordContractProviding.Type)
                                || (proof.submissionIdentity != nil && base?.revision == proof.submissionIdentity))
                        guard base?.revision == proof.revision || alreadyInstalled,
                              let id = getObjectIdentifier(recordName: name, entityType: item.type.className()),
                              let object = realm.object(ofType: item.type, forPrimaryKey: id),
                              !BigSyncRecordLifecycle.isPhysicalDeletion(object) else { continue }
                        let pending = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
                        if pending == nil, alreadyInstalled,
                           BigSyncRecordLifecycle.retainsTombstone(item.type),
                           (object as? SoftDeletable)?.isDeleted == true {
                            // A prior acknowledgement may have committed its
                            // exact accepted baseline and journal removal before
                            // cleanup lost authority. Resume only that receipt;
                            // this is not admission to acknowledge new work.
                            retainedCleanupRecords.append(saved)
                            continue
                        }
                        guard let pending, pendingMutationIsEligibleForActiveTransport(pending) else { continue }
                        BigSyncRecordBaseline.install(recordName: name, namespace: proof.context.namespace,
                            fields: proof.fields, serverChangeTag: saved.recordChangeTag,
                            schemaSignature: proof.schemaSignature,
                            systemFields: try BigSyncRecordPayload.systemFields(of: saved),
                            acceptedRevision: proof.submissionIdentity, in: realm)
                        guard let current = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name) else {
                            throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                        }
                        if let submitted = matchingSubmission(recordName: name, context: proof.context, in: realm),
                           submitted.generation == preparedByID[saved.recordID]?.generation,
                           submitted.schemaSignature == proof.schemaSignature,
                           submitted.comparisonRevision == proof.revision,
                           submitted.candidateIdentity == proof.submissionIdentity,
                           submitted.fields.reduce(into: [String: Data](), { $0[$1.key] = $1.value }) == proof.fields {
                            realm.delete(submitted)
                        }
                        accepted[name] = .init(context: proof.context, revision: current.revision)
                        generations[name] = preparedByID[saved.recordID]?.generation
                        admittedRecords.append(saved)
                    }
                }
            }
        }
        // Do not pass rejected records with a merely matching generation. Both
        // subsequent commit phases recheck the admitted comparison revision.
        try await acknowledgeUploadReceipts(
            savedRecords: admittedRecords, matchingGenerations: generations,
            comparisonReceipts: accepted
        )
        // A retained record's physical-disappearance quarantine is resolved
        // by an accepted upload of that same retained tombstone. This is a
        // record-scoped receipt, not a general quarantine cleanup: require
        // the tracking acknowledgement to be terminal and retire only the
        // matching retained-deletion lineage.
        try await retireAcceptedRetainedDeletionQuarantines(
            savedRecords: admittedRecords + retainedCleanupRecords
        )
    }
}

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

        let cleanupCancellationGeneration = cancellationGeneration
        func validateCleanup() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == cleanupCancellationGeneration,
                  recordRebaseContext == context,
                  activeAccountScopeIdentifier == context.account else {
                throw CancellationError()
            }
        }
        try validateCleanup()

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
        try validateCleanup()
        guard !candidates.isEmpty else { return }

        try await tracking.asyncWritePreservingOwnership {
            try validateCleanup()
#if DEBUG
            try _testAfterAcceptedRetainedDeletionTrackingAdmission?()
#endif
            // Candidate selection can become stale while the tracking writer
            // waits. Revalidate the exact detached lineage IDs after admission;
            // no managed quarantine row crosses the suspension.
            let lineageIDs = candidates.intersection(
                eligibleLineageIDs(in: tracking)
            )
            // Snapshot refresh may synchronously revoke this cleanup's owner.
            try validateCleanup()
            guard !lineageIDs.isEmpty else { return }
            let receiptIDs = try Self.retireQuarantines(
                Array(lineageIDs),
                in: tracking
            )
            Self.removeUnreferencedPageReceipts(
                receiptIDs,
                in: tracking
            )
            try validateCleanup()
        }
    }
}

// MARK: Bounded submitted-value evidence and explicit conflict resolution
extension RealmSwiftAdapter {
    func matchingSubmission(recordName: String, context: BigSyncRecordRebaseContext,
                                    in realm: Realm) -> BigSyncRecordSubmission? {
