    var _testAfterDisappearanceTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeRetainedDeletionQuarantineWrite:
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
        comparisonReceipts: [String: RealmSwiftAcceptedComparisonReceipt],
        retainedDeletionCleanup: RetainedDeletionQuarantineCleanup? = nil
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        let acknowledgementGeneration = cancellationGeneration
        let acknowledgementContext = recordRebaseContext
        let acknowledgementAccount = activeAccountScopeIdentifier
        func validateAcknowledgementOwner() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == acknowledgementGeneration,
                  self.realmProvider === realmProvider,
                  recordRebaseContext == acknowledgementContext,
                  activeAccountScopeIdentifier == acknowledgementAccount else {
                throw CancellationError()
            }
        }
        try validateAcknowledgementOwner()
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()

        for chunk in savedRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }

#if DEBUG
            if retainedDeletionCleanup != nil {
                try await _testBeforeRetainedDeletionQuarantineWrite?()
            }
#endif
            try await persistenceRealm.asyncWritePreservingOwnership {
                try validateAcknowledgementOwner()
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
                                recordName: record.recordID.recordName, in: target,
                                access: .committedObservation) else { continue }
                    }
                    // Receipt validation can refresh a target or invoke an
                    // identity provider. Re-resolve tracking after those
                    // callbacks instead of retaining a managed row across them.
                    try validateAcknowledgementOwner()
                    guard let currentEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: record.recordID.recordName
                    ), currentEntity.entityType == record.recordType,
                       currentEntity.pendingGeneration == uploadedGeneration else {
                        continue
                    }
                    // A residual target journal may be forwarded again after
                    // this exact server version was already acknowledged. That
                    // permits journal repair, not retirement of a later deletion.
                    let cachedReceipt = getRecord(for: currentEntity)
                    let repeatsCachedVersion = record.recordChangeTag?.isEmpty == false
                        && cachedReceipt?.recordID == record.recordID
                        && cachedReceipt?.recordType == record.recordType
                        && cachedReceipt?.recordChangeTag == record.recordChangeTag
                    try save(record: record, for: currentEntity)
                    try validateAcknowledgementOwner()
                    currentEntity.state = SyncedEntityState.synced.rawValue
                    currentEntity.clearPendingMutation()
                    if !repeatsCachedVersion {
                        acknowledgedInThisWrite[record.recordID.recordName] = uploadedGeneration
                    }
                    acknowledgedGenerations[record.recordID.recordName] = uploadedGeneration
                    acknowledgedEntityTypes[record.recordID.recordName] =
                        currentEntity.entityType
                }
                if let retainedDeletionCleanup, !acknowledgedInThisWrite.isEmpty {
                    try retireAcceptedRetainedDeletionQuarantines(
                        retainedDeletionCleanup,
                        acknowledgedGenerations: acknowledgedInThisWrite,
                        comparisonReceipts: comparisonReceipts,
                        in: persistenceRealm
                    )
                }
                try validateAcknowledgementOwner()
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
                        try validateAcknowledgementOwner()
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
                                    access: .ownedTargetWrite) else { continue }
                            }
                            try validateAcknowledgementOwner()
                            // The identity provider can synchronously author a
                            // successor while validating this owned write. Its
                            // journal must not be deleted through an older row
                            // reference, even when the comparison is unchanged.
                            guard let currentMutation = targetReaderRealm.object(
                                ofType: BigSyncPendingMutation.self,
                                forPrimaryKey: recordName
                            ), currentMutation.generation == generation,
                               pendingMutationIsEligibleForActiveTransport(
                                currentMutation
                               ) else { continue }
                            targetReaderRealm.delete(currentMutation)
                        }
                    }
                    try validateAcknowledgementOwner()
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

    /// Callers name their transaction role. A live Realm's transaction flag
    /// alone cannot prove that this operation owns that transaction.
    private enum ComparisonReceiptReadAccess {
        case committedObservation
        case ownedTargetWrite
    }

    @BigSyncBackgroundActor
    private func comparisonReceiptIsCurrent(
        _ receipt: RealmSwiftAcceptedComparisonReceipt,
        recordName: String, in realm: Realm,
        access: ComparisonReceiptReadAccess
    ) throws -> Bool {
        guard recordRebaseContext == receipt.context,
              BigSyncRecordBaseline.isEnabled(in: realm) else { return false }
        let snapshot: Realm
        switch access {
        case .committedObservation:
            // This accepts an already frozen view without refreshing it, and
            // never borrows another caller's open target write.
            snapshot = committedRealmReadSnapshot(in: realm)
            guard let identity = BigSyncMutationTrackingRegistry
                .currentMutationJournalIdentity(in: snapshot),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier
                    == receipt.context.binding else {
                throw CancellationError()
            }
        case .ownedTargetWrite:
            precondition(!realm.isFrozen && realm.isInWriteTransaction)
            try receipt.context.validate(in: realm)
            snapshot = realm
        }
        // Identity providers and refresh notifications are synchronous
        // callouts. They may revoke the original comparison context.
        guard recordRebaseContext == receipt.context,
              let current = snapshot.object(
                ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: recordName
              ), !current.isComparisonInvalidated,
                 current.namespace == receipt.context.namespace,
                 current.revision == receipt.revision else { return false }
        if let typeName = recordName.split(separator: ".", maxSplits: 1)
            .first.map(String.init),
           let type = realmObjectClass(name: typeName),
           let compiled = try BigSyncCompiledRecordContract.compile(type.init()),
           compiled.signature != current.schemaSignature { return false }
        return recordRebaseContext == receipt.context
    }

    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord], matchingPreparedUploads prepared: [PreparedRecordUpload]) async throws {
        let uploadCancellationGeneration = cancellationGeneration
        let uploadProvider = realmProvider
        let uploadContext = recordRebaseContext
        let uploadAccount = activeAccountScopeIdentifier
        func validateUploadOwner() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == uploadCancellationGeneration,
                  realmProvider === uploadProvider,
                  recordRebaseContext == uploadContext,
                  activeAccountScopeIdentifier == uploadAccount else {
                throw CancellationError()
            }
        }
        try validateUploadOwner()
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
        // Capture the exact original quarantine/page observation before the
        // first target-write wait. A later deletion is not this save's proof.
        let retainedDeletionCleanup = prepareRetainedDeletionQuarantineCleanup(
            savedRecords: savedRecords
        )
        try validateUploadOwner()
        for key in groups.keys.sorted() {
            guard let group = groups[key] else { continue }
            let realm = group.realm
            for chunk in group.items.chunks(ofCount: 500) {
                try await realm.asyncWritePreservingOwnership {
                    try validateUploadOwner()
                    for item in chunk {
                        try Task.checkCancellation()
                        let proof = item.proof, saved = item.saved
                        let name = saved.recordID.recordName
                        guard !cancelSync, recordRebaseContext == proof.context else { throw CancellationError() }
                        try proof.context.validate(in: realm)
                        try validateUploadOwner()
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
                    try validateUploadOwner()
                }
            }
        }
        // Only fresh tracking acknowledgements may retire their original
        // quarantine observation, in that same transaction. A historical save
        // replay cannot clear a later deletion merely because its tag matches.
        try validateUploadOwner()
        try await acknowledgeUploadReceipts(
            savedRecords: admittedRecords, matchingGenerations: generations,
            comparisonReceipts: accepted,
            retainedDeletionCleanup: retainedDeletionCleanup
        )
    }
}

extension RealmSwiftAdapter {
    /// Detached observation of the quarantine selected before this cleanup
    /// waits. Re-observation or page rebinding is not the same cleanup input,
    /// even when the deterministic lineage ID is reused.
    private struct RetainedDeletionQuarantineObservation: Equatable {
        let lineageID: String
        let recordName: String
        let entityType: String
        let importRunIdentifier: String
        let receivedRecordDigestHex: String
        let changeFeedEpoch: Int
        let committedPageSequence: Int64
        let committedPageReceiptID: String
        let committedPageOutcomeDigestHex: String
        let detectedAt: Date

        init(_ quarantine: BigSyncInboundSemanticQuarantine) {
            lineageID = quarantine.lineageID
            recordName = quarantine.recordName
            entityType = quarantine.entityType
            importRunIdentifier = quarantine.importRunIdentifier
            receivedRecordDigestHex = quarantine.receivedRecordDigestHex
            changeFeedEpoch = quarantine.changeFeedEpoch
            committedPageSequence = quarantine.committedPageSequence
            committedPageReceiptID = quarantine.committedPageReceiptID
            committedPageOutcomeDigestHex = quarantine.committedPageOutcomeDigestHex
            detectedAt = quarantine.detectedAt
        }
    }

    private struct RetainedDeletionUploadReceipt {
        let recordID: CKRecord.ID
        let recordType: String
        let changeTag: String?
    }

    /// No managed Realm value crosses tracking admission. This observation is
    /// local to an acknowledgement, not persisted as another recovery queue.
    private struct RetainedDeletionQuarantineCleanup {
        let context: BigSyncRecordRebaseContext
        let cancellationGeneration: UInt64
        let provider: RealmProvider
        let receipts: [String: RetainedDeletionUploadReceipt]
        let candidates: [RetainedDeletionQuarantineObservation]

        func matches(_ quarantine: BigSyncInboundSemanticQuarantine) -> Bool {
            Self.matches(quarantine, receipts: receipts)
        }

        static func matches(
            _ quarantine: BigSyncInboundSemanticQuarantine,
            receipts: [String: RetainedDeletionUploadReceipt]
        ) -> Bool {
            guard quarantine.eventKind == "deletion",
                  quarantine.validationCode == "retained-record-physically-deleted",
                  quarantine.semanticScopeIdentifier
                    == "retained-physical-deletion:" + quarantine.recordName,
                  let saved = receipts[quarantine.recordName],
                  saved.recordType == quarantine.entityType,
                  let tag = saved.changeTag, !tag.isEmpty else { return false }
            return true
        }
    }

    @BigSyncBackgroundActor
    private func prepareRetainedDeletionQuarantineCleanup(
        savedRecords: [CKRecord]
    ) -> RetainedDeletionQuarantineCleanup? {
        guard !savedRecords.isEmpty,
              let context = recordRebaseContext,
              let provider = realmProvider,
              let tracking = provider.persistenceRealm else { return nil }
        let cleanupCancellationGeneration = cancellationGeneration
        // CKRecord is mutable; capture the supplied version before any wait.
        let receipts = Dictionary(uniqueKeysWithValues: savedRecords.map {
            ($0.recordID.recordName, RetainedDeletionUploadReceipt(
                recordID: $0.recordID, recordType: $0.recordType,
                changeTag: $0.recordChangeTag))
        })
        // Refresh can start another owner's transaction. Freeze afterwards
        // even when the Realm was not writing at entry.
        let snapshot = committedRealmReadSnapshot(in: tracking)
        let candidates = activeInboundSemanticQuarantines(
            accountScopeIdentifier: context.account, in: snapshot
        ).filter { quarantine in
            receipts[quarantine.recordName]?.recordID.zoneID == recordZoneID
                && RetainedDeletionQuarantineCleanup.matches(quarantine, receipts: receipts)
        }.map(RetainedDeletionQuarantineObservation.init)
        guard !candidates.isEmpty else { return nil }
        return .init(context: context, cancellationGeneration: cleanupCancellationGeneration,
                     provider: provider, receipts: receipts, candidates: candidates)
    }

    /// The tracking acknowledgement and its quarantine retirement commit
    /// together. A failure rolls back both, leaving the sent journal intact
    /// for the existing retry path. An old receipt is never replayed later
    /// against newly observed quarantine evidence merely because its tag matches.
    @BigSyncBackgroundActor
    private func retireAcceptedRetainedDeletionQuarantines(
        _ cleanup: RetainedDeletionQuarantineCleanup,
        acknowledgedGenerations: [String: String],
        comparisonReceipts: [String: RealmSwiftAcceptedComparisonReceipt],
        in tracking: Realm
    ) throws {
        precondition(tracking.isInWriteTransaction)
        let context = cleanup.context
        let provider = cleanup.provider
        func validateCleanupOwner() throws {
            try Task.checkCancellation()
            guard !cancelSync,
                  cancellationGeneration == cleanup.cancellationGeneration,
                  recordRebaseContext == context,
                  activeAccountScopeIdentifier == context.account,
                  realmProvider === provider else { throw CancellationError() }
        }
        try validateCleanupOwner()
#if DEBUG
        try _testAfterAcceptedRetainedDeletionTrackingAdmission?()
#endif
        try validateCleanupOwner()
        let candidates = cleanup.candidates.filter {
            acknowledgedGenerations[$0.recordName] != nil
        }
        guard !candidates.isEmpty else { return }
        // Refresh all target views before resolving mutable tracking rows.
        // Each original Realm is frozen once, so aliases share one version.
        // These are committed per-file reads, not a cross-file transaction.
        var snapshotsByRealm = [ObjectIdentifier: Realm]()
        var targets = [String: Realm]()
        for entityType in Set(candidates.map(\.entityType)) {
            guard let target = provider.targetReaderRealmPerSchemaName[entityType] else { continue }
            let identity = ObjectIdentifier(ObjectiveCSupport.convert(object: target))
            if snapshotsByRealm[identity] == nil {
                snapshotsByRealm[identity] = committedRealmReadSnapshot(in: target)
            }
            targets[entityType] = snapshotsByRealm[identity]
        }
        // Refresh and registry/model callbacks must not let a cancelled
        // attempt borrow a resumed run with identical namespace strings.
        try validateCleanupOwner()
        // Complete model/registry validation before resolving any live
        // tracking row. A synchronous provider callback can replace earlier
        // evidence just as a refresh callback can open another transaction.
        var eligibleRecordNames = Set<String>()
        for name in Set(candidates.map(\.recordName)) {
            guard let sentGeneration = acknowledgedGenerations[name],
                  let saved = cleanup.receipts[name],
                  let type = realmObjectClass(name: saved.recordType),
                  BigSyncRecordLifecycle.retainsTombstone(type),
                  let target = targets[saved.recordType],
                  let objectID = getObjectIdentifier(recordName: name, entityType: saved.recordType),
                  let object = target.object(ofType: type, forPrimaryKey: objectID),
                  objectIsEligibleForActiveAccount(object, entityType: saved.recordType),
                  let tombstone = object as? SoftDeletable, tombstone.isDeleted else { continue }

            // Journal consumption follows this tracking commit. The sent
            // generation is allowed; a successor must retain its evidence,
            // even before forwarding has updated the tracking cache.
            if target.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }), let pending = target.object(ofType: BigSyncPendingMutation.self,
                                            forPrimaryKey: name),
               (pending.generation != sentGeneration
                || !pendingMutationIsEligibleForActiveTransport(pending)) { continue }
            if type is BigSyncRecordContractProviding.Type {
                guard BigSyncRecordBaseline.isEnabled(in: target),
                      let comparison = comparisonReceipts[name],
                      comparison.context == context,
                      try comparisonReceiptIsCurrent(comparison, recordName: name, in: target,
                                                     access: .committedObservation),
                      target.object(ofType: BigSyncRecordBaseline.self,
                                    forPrimaryKey: name)?.serverChangeTag
                        == saved.changeTag else { continue }
            }
            eligibleRecordNames.insert(name)
        }
        try validateCleanupOwner()
        let activeLineages = Set(activeInboundSemanticQuarantines(
            accountScopeIdentifier: context.account, in: tracking
        ).map(\.lineageID))
        var lineageIDs = [String]()
        for candidate in candidates where eligibleRecordNames.contains(candidate.recordName) {
            guard activeLineages.contains(candidate.lineageID),
                  let quarantine = tracking.object(
                    ofType: BigSyncInboundSemanticQuarantine.self,
                    forPrimaryKey: candidate.lineageID
                  ), RetainedDeletionQuarantineObservation(quarantine) == candidate,
                  cleanup.matches(quarantine),
                  let saved = cleanup.receipts[candidate.recordName],
                  let entity = tracking.object(
                    ofType: SyncedEntity.self, forPrimaryKey: candidate.recordName
                  ), entity.entityType == candidate.entityType,
                     entity.entityState == .synced, entity.pendingGeneration == nil,
                     let cached = getRecord(for: entity),
                     cached.recordID == saved.recordID,
                     cached.recordType == saved.recordType,
                     cached.recordChangeTag == saved.changeTag else { continue }
            lineageIDs.append(candidate.lineageID)
        }
        try validateCleanupOwner()
        let receiptIDs = try Self.retireQuarantines(lineageIDs, in: tracking)
        Self.removeUnreferencedPageReceipts(receiptIDs, in: tracking)
    }
}

// MARK: Bounded submitted-value evidence and explicit conflict resolution
extension RealmSwiftAdapter {
    func matchingSubmission(recordName: String, context: BigSyncRecordRebaseContext,
                                    in realm: Realm) -> BigSyncRecordSubmission? {
