import CloudKit
import Foundation
import RealmSwift

/// A queued target/tracking write must still belong to this transport attempt.
/// This is a local fence, not another coordinator or durable work queue.
struct BigSyncRecordEvidenceCut: Sendable {
    let context: BigSyncRecordRebaseContext
    let cancellationGeneration: UInt64
}

extension RealmSwiftAdapter {
    /// One interpretation for staged retry, receipts and read-only audit. The
    /// persisted archive is authoritative; no initializer supplies missing data.
    func validatedSubmissionRecord(
        _ submitted: BigSyncRecordSubmission, recordID: CKRecord.ID,
        type: Object.Type, context: BigSyncRecordRebaseContext,
        materializeAssets: Bool = false
    ) throws -> CKRecord {
        guard submitted.namespace == context.namespace,
              submitted.id == BigSyncRecordPayload.identity([context.namespace, recordID.recordName]),
              submitted.recordName == recordID.recordName,
              recordID.zoneID == recordZoneID,
              recordID.recordName.hasPrefix(type.className() + "."),
              !submitted.generation.isEmpty,
              let contract = try BigSyncCompiledRecordContract.compile(type.init()),
              submitted.schemaSignature == contract.signature else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
        let record = try BigSyncRecordPayload.decode(submitted.payload,
            assetManager: materializeAssets ? persistentAssetManager : nil)
        guard record.recordID == recordID, record.recordType == type.className(),
              try BigSyncRecordFingerprint.fields(of: decodedComparisonObject(record, type: type))
                == submitted.fields.reduce(into: [String: Data](), { $0[$1.key] = $1.value }) else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
        return record
    }

    /// Atomically invalidate the accepted CAS template and retire exactly the
    /// superseded candidate. An invalidated revision remains a receipt fence,
    /// including when the previous accepted comparison was nil. The latest
    /// journal/value is deliberately not changed by this evidence transition.
    @BigSyncBackgroundActor
    @discardableResult
    func commitPhysicalDisappearance(
        recordID: CKRecord.ID, type: Object.Type, cut: BigSyncRecordEvidenceCut,
        expectedRevision: String?, expectedSubmissionIdentity: String?,
        in realm: Realm
    ) throws -> String? {
        precondition(realm.isInWriteTransaction)
        try validateRecordEvidenceCut(cut, in: realm)
        guard recordID.zoneID == recordZoneID,
              recordID.recordName.hasPrefix(type.className() + "."),
              let contract = try BigSyncCompiledRecordContract.compile(type.init()),
              contract.declaration.deletion == .physical else {
            throw BigSyncRecordContractError.unexpectedPhysicalDeletion(recordID.recordName)
        }
        let name = recordID.recordName
        let base = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
        let submitted = matchingSubmission(recordName: name, context: cut.context, in: realm)
        guard base?.revision == expectedRevision,
              submitted?.candidateIdentity == expectedSubmissionIdentity else { return nil }
        if let submitted {
            _ = try validatedSubmissionRecord(submitted, recordID: recordID, type: type, context: cut.context)
        }
        if let base, !base.isComparisonInvalidated, base.namespace != cut.context.namespace {
            // A failure in this namespace is not evidence about another one.
            throw BigSyncRecordRebaseError.inconsistentReceipt(name)
        }
        if base?.isComparisonInvalidated != true || base?.revision.isEmpty != false
            || (submitted != nil && submitted?.comparisonRevision == base?.revision) {
            BigSyncRecordBaseline.invalidate(recordName: name, in: realm)
        }
        guard let fence = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name),
              fence.isComparisonInvalidated, fence.fields.count == 0,
              fence.serverChangeTag == nil, fence.acceptedSystemFields == nil else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(name)
        }
        fence.namespace = cut.context.namespace
        fence.schemaSignature = contract.signature
        if let submitted { realm.delete(submitted) }
        return fence.revision
    }

    /// Publish a durable target disposition into the existing tracking cache.
    /// Re-read both the fence and the latest journal *after* the tracking write
    /// is granted; V1 cannot overwrite V2, even if it repaired V1's ancestor.
    @BigSyncBackgroundActor
    fileprivate func publishPhysicalDisappearance(
        recordID: CKRecord.ID, type: Object.Type, cut: BigSyncRecordEvidenceCut,
        revision: String, in target: Realm
    ) async throws {
        guard let tracking = realmProvider?.persistenceRealm,
              let objectID = getObjectIdentifier(recordName: recordID.recordName, entityType: type.className()) else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
#if DEBUG
        try await _testAfterDisappearanceTargetWrite?()
#endif
        try await tracking.asyncWrite {
            target.refresh()
            try validateRecordEvidenceCut(cut, in: target)
            let name = recordID.recordName
            guard let base = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name),
                  base.namespace == cut.context.namespace,
                  base.isComparisonInvalidated, base.revision == revision else { return }
            let mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
            if let mutation, !pendingMutationIsEligibleForActiveTransport(mutation) { return }
            let object = target.object(ofType: type, forPrimaryKey: objectID)
            if let object, !objectIsEligibleForActiveAccount(object, entityType: type.className()) { return }
            let entity = tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)
                ?? SyncedEntity(entityType: type.className(), identifier: name,
                                state: SyncedEntityState.deletedRemotely.rawValue)
            guard entity.entityType == type.className() else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(name)
            }
            tracking.add(entity, update: .modified)
            entity.encodedRecord = nil
            if let mutation {
                entity.entityState = object.map(BigSyncRecordLifecycle.isPhysicalDeletion) == false
                    ? .new : .deletedLocally
                entity.setPendingMutation(generation: mutation.generation,
                    replicaBindingGenerationIdentifier: mutation.replicaBindingGenerationIdentifier)
            } else {
                // This path committed an admitted remote tombstone, not a
                // successful acknowledgement of some unobserved local edit.
                guard object == nil || object.map(BigSyncRecordLifecycle.isPhysicalDeletion) == true else {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                }
                entity.entityState = .deletedRemotely
                entity.clearPendingMutation()
            }
        }
    }

    /// The feed supplies an admitted physical disappearance, not an uncertain
    /// acceptance lookup. Capture the evidence cut before waiting, then make
    /// the decision against live objects/journals at the target transaction.
    @BigSyncBackgroundActor
    func reconcilePhysicalDeletion(
        recordID: CKRecord.ID, type: Object.Type, in target: Realm
    ) async throws -> InboundDeletionDisposition {
        let cut = try currentRecordEvidenceCut()
        let name = recordID.recordName
        let revision = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision
        let submissionIdentity = matchingSubmission(recordName: name, context: cut.context, in: target)?.candidateIdentity
        guard let objectID = getObjectIdentifier(recordName: name, entityType: type.className()) else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(name)
        }
        var committedRevision: String?
        var disposition: InboundDeletionDisposition = .alreadyDeleted
#if DEBUG
        try await _testBeforeRemoteDeletionTargetWrite?()
#endif
        try await target.asyncWrite {
            try validateRecordEvidenceCut(cut, in: target)
            let object = target.object(ofType: type, forPrimaryKey: objectID)
            if let object, !objectIsEligibleForActiveAccount(object, entityType: type.className()) {
                disposition = .ignoredExplicitAuthority
                return
            }
            var mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
            if let mutation, !pendingMutationIsEligibleForActiveTransport(mutation) {
                disposition = .preservedNewerLive(generation: mutation.generation)
                return
            }
            try (type as? BigSyncInboundSemanticDeletionValidating.Type)?
                .validateInboundSemanticDeletion(recordID, existingObject: object)
            guard let fence = try commitPhysicalDisappearance(recordID: recordID, type: type, cut: cut,
                expectedRevision: revision, expectedSubmissionIdentity: submissionIdentity, in: target) else {
                // A newer accepted observation/submission acquired this ID
                // while queued. Do not publish a stale cache reset or cursor.
                throw RealmSwiftInboundTargetChangedError(recordName: name)
            }
            // Older adapter tracking may contain dirty work before its journal
            // is forwarded. Preserve that value through the existing journal
            // API, without re-authoring timestamps or inventing a second outbox.
            if mutation == nil, let object,
               let tracking = realmProvider?.persistenceRealm {
                tracking.refresh()
                let entity = tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)
                if entity?.entityState == .new || entity?.entityState == .changed
                    || entity?.entityState == .deletedLocally {
                    (object as? ChangeMetadataRecordable)?.journalCurrentValuePreservingChangeMetadata(at: Date())
                    mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
                }
            }
            if let mutation {
                disposition = .preservedNewerLive(generation: mutation.generation)
            } else if let deleted = object as? SoftDeletable {
                disposition = deleted.isDeleted ? .alreadyDeleted : .appliedTombstone
                deleted.isDeleted = true
            } else if object != nil {
                throw BigSyncRecordRebaseError.inconsistentReceipt(name)
            }
            // Journaling a pre-existing local tombstone can create a newer
            // invalidated revision. Publish the final target revision only.
            committedRevision = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision ?? fence
        }
        if let committedRevision {
            try await publishPhysicalDisappearance(recordID: recordID, type: type, cut: cut,
                revision: committedRevision, in: target)
        }
        return disposition
    }

    @BigSyncBackgroundActor
    public func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID], matchingPreparedUploads prepared: [PreparedRecordUpload]
    ) async throws {
        var byID = [CKRecord.ID: PreparedRecordUpload]()
        for item in prepared {
            guard item.record.recordID.zoneID == recordZoneID,
                  byID.updateValue(item, forKey: item.record.recordID) == nil else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(item.record.recordID.recordName)
            }
        }
        guard Set(recordIDs).count == recordIDs.count,
              recordIDs.allSatisfy({ byID[$0] != nil }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        var legacyIDs = [CKRecord.ID]()
        var legacyGenerations = [String: String]()
        for recordID in recordIDs {
            guard let item = byID[recordID], let type = realmObjectClass(name: item.record.recordType) else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
            }
            guard let contract = try BigSyncCompiledRecordContract.compile(type.init()) else {
                legacyIDs.append(recordID)
                legacyGenerations[recordID.recordName] = item.generation
                continue
            }
            guard contract.declaration.deletion == .physical else {
                throw BigSyncRecordContractError.unexpectedPhysicalDeletion(recordID.recordName)
            }
            guard let proof = item.comparisonBase, let submittedIdentity = proof.submissionIdentity,
                  proof.schemaSignature == contract.signature,
                  let target = realmProvider?.targetReaderRealmPerSchemaName[type.className()],
                  let objectID = getObjectIdentifier(recordName: recordID.recordName, entityType: type.className()) else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
            }
            let cut = try currentRecordEvidenceCut()
            guard proof.context == cut.context else { continue }
            var committedRevision: String?
#if DEBUG
            try await _testBeforeMissingServerTargetWrite?()
#endif
            try await target.asyncWrite {
                try validateRecordEvidenceCut(cut, in: target)
                let name = recordID.recordName
                let base = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
                let submitted = matchingSubmission(recordName: name, context: cut.context, in: target)
                guard base?.revision == proof.revision,
                      let submitted, submitted.candidateIdentity == submittedIdentity,
                      submitted.comparisonRevision == proof.revision,
                      submitted.generation == item.generation,
                      let mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name),
                      pendingMutationIsEligibleForActiveTransport(mutation),
                      let object = target.object(ofType: type, forPrimaryKey: objectID),
                      objectIsEligibleForActiveAccount(object, entityType: type.className()) else { return }
                let candidate = try validatedSubmissionRecord(submitted, recordID: recordID, type: type, context: cut.context)
                guard candidate.recordChangeTag == item.record.recordChangeTag,
                      try BigSyncRecordFingerprint.fields(of: decodedComparisonObject(item.record, type: type)) == proof.fields else {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                }
                // Unknown-item for a never-server-backed first candidate does
                // not prove that its uncertain submission was never accepted.
                // Preserve its exact archive and conditional retry semantics.
                guard candidate.recordChangeTag != nil else { return }
                if let base, !base.isComparisonInvalidated {
                    guard base.namespace == cut.context.namespace,
                          base.schemaSignature == proof.schemaSignature,
                          base.serverChangeTag == candidate.recordChangeTag else {
                        throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                    }
                }
                committedRevision = try commitPhysicalDisappearance(recordID: recordID, type: type, cut: cut,
                    expectedRevision: proof.revision, expectedSubmissionIdentity: submittedIdentity, in: target)
                // Deliberately no mutation.generation == item.generation test:
                // V1 still proves its ancestor disappeared while V2 is pending.
            }
            if let committedRevision {
                try await publishPhysicalDisappearance(recordID: recordID, type: type, cut: cut,
                    revision: committedRevision, in: target)
            }
        }
        if !legacyIDs.isEmpty {
            try await requeueMissingServerRecords(legacyIDs, matchingPreparedGenerations: legacyGenerations)
        }
    }
}

/// Ephemeral evidence for one prepared physical deletion. It is carried through
/// the existing transport operation, never persisted as another work queue.
struct BigSyncPreparedDeletionEvidence: Sendable {
    let recordID: CKRecord.ID
    let entityType: String
    let cut: BigSyncRecordEvidenceCut
    let revision: String?
    let submissionIdentity: String?
    let schemaSignature: String
}

extension RealmSwiftAdapter {
    @BigSyncBackgroundActor
    func preparePhysicalDeletionEvidence(
        recordID: CKRecord.ID, type: Object.Type, generation: String, in target: Realm
    ) async throws -> BigSyncPreparedDeletionEvidence? {
        let cut = try currentRecordEvidenceCut()
        target.refresh()
        try validateRecordEvidenceCut(cut, in: target)
        guard let contract = try BigSyncCompiledRecordContract.compile(type.init()),
              contract.declaration.deletion == .physical,
              recordID.zoneID == recordZoneID,
              let objectID = getObjectIdentifier(recordName: recordID.recordName, entityType: type.className()) else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
        let name = recordID.recordName
        let object = target.object(ofType: type, forPrimaryKey: objectID)
        if let object, !objectIsEligibleForActiveAccount(object, entityType: type.className()) { return nil }
        let mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
        let base = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
        let submitted = matchingSubmission(recordName: name, context: cut.context, in: target)
        if mutation == nil {
            // A crash after the target-first delete acknowledgement can leave
            // old tracking work. The durable disposition has no journal or
            // submission and a retained invalidated revision; finish only its
            // cache phase. Do not manufacture another CloudKit deletion.
            guard submitted == nil, let base, base.namespace == cut.context.namespace,
                  base.schemaSignature == contract.signature,
                  base.isComparisonInvalidated, !base.revision.isEmpty,
                  base.fields.count == 0, base.serverChangeTag == nil, base.acceptedSystemFields == nil,
                  object == nil || object.map(BigSyncRecordLifecycle.isPhysicalDeletion) == true else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(name)
            }
            try await publishPhysicalDisappearance(recordID: recordID, type: type,
                cut: cut, revision: base.revision, in: target)
            return nil
        }
        guard let mutation, mutation.generation == generation,
              pendingMutationIsEligibleForActiveTransport(mutation),
              object == nil || object.map(BigSyncRecordLifecycle.isPhysicalDeletion) == true else { return nil }
        if let submitted {
            guard submitted.generation != generation else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(name)
            }
            _ = try validatedSubmissionRecord(submitted, recordID: recordID, type: type, context: cut.context)
        }
        return .init(recordID: recordID, entityType: type.className(), cut: cut,
            revision: base?.revision, submissionIdentity: submitted?.candidateIdentity,
            schemaSignature: contract.signature)
    }

    @BigSyncBackgroundActor
    public func didDelete(
        recordIDs: [CKRecord.ID], matchingPreparedDeletions prepared: [PreparedRecordDeletion]
    ) async throws {
        var byID = [CKRecord.ID: PreparedRecordDeletion]()
        for item in prepared {
            guard item.recordID.zoneID == recordZoneID,
                  byID.updateValue(item, forKey: item.recordID) == nil else {
                throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
            }
        }
        guard Set(recordIDs).count == recordIDs.count,
              recordIDs.allSatisfy({ byID[$0] != nil }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        var legacyIDs = [CKRecord.ID]()
        var legacyGenerations = [String: String]()
        for recordID in recordIDs {
            guard let item = byID[recordID] else {
                throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
            }
            guard let proof = item.evidence else {
                legacyIDs.append(recordID)
                legacyGenerations[recordID.recordName] = item.generation
                continue
            }
            guard proof.recordID == recordID,
                  let generation = item.generation,
                  let type = realmObjectClass(name: proof.entityType),
                  let contract = try BigSyncCompiledRecordContract.compile(type.init()),
                  contract.declaration.deletion == .physical,
                  contract.signature == proof.schemaSignature,
                  let target = realmProvider?.targetReaderRealmPerSchemaName[proof.entityType],
                  let objectID = getObjectIdentifier(recordName: recordID.recordName, entityType: proof.entityType) else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
            }
            var committedRevision: String?
            try await target.asyncWrite {
                try validateRecordEvidenceCut(proof.cut, in: target)
                let name = recordID.recordName
                let object = target.object(ofType: type, forPrimaryKey: objectID)
                if let object, !objectIsEligibleForActiveAccount(object, entityType: proof.entityType) { return }
                let mutation = target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
                if let mutation, !pendingMutationIsEligibleForActiveTransport(mutation) { return }
                let isDeleted = object == nil || object.map(BigSyncRecordLifecycle.isPhysicalDeletion) == true
                guard let fence = try commitPhysicalDisappearance(recordID: recordID, type: type,
                    cut: proof.cut, expectedRevision: proof.revision,
                    expectedSubmissionIdentity: proof.submissionIdentity, in: target) else { return }
                guard isDeleted || (mutation != nil && mutation?.generation != generation) else {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                }
                // This receipt proves a server disappearance relative to its
                // prepared evidence, not authority over a successor lifetime.
                // Retire the old staged save but consume only the sent delete.
                if let mutation, mutation.generation == generation {
                    guard isDeleted else { throw BigSyncRecordRebaseError.inconsistentReceipt(name) }
                    target.delete(mutation)
                }
                committedRevision = fence
            }
            if let committedRevision {
                try await publishPhysicalDisappearance(recordID: recordID, type: type,
                    cut: proof.cut, revision: committedRevision, in: target)
            }
        }
        if !legacyIDs.isEmpty {
            try await didDelete(recordIDs: legacyIDs, matchingGenerations: legacyGenerations)
        }
        if let tracking = realmProvider?.persistenceRealm { updateHasChanges(realm: tracking) }
    }
}
