import CloudKit
import Foundation

extension CloudKitSynchronizer {
    internal var outboundQuiescenceCoordinator: BigSyncOutboundQuiescenceCoordinator {
        BigSyncOutboundQuiescenceCoordinator(
            sharedStateBaseURL: backupDetectionBaseURL ?? BackupDetection.defaultSentinelURL(
                namespace: durableStateNamespace).deletingLastPathComponent(),
            durableStateNamespace: durableStateNamespace)
    }

    internal func currentOutboundPrincipal(for context: RunContext? = nil) throws -> BigSyncOutboundPrincipal {
        try Task.checkCancellation()
        let candidateLease: BigSyncAccountScopeLease?
        if let context { candidateLease = try outboundAccountScopeLease(for: context) }
        else { candidateLease = try accountScopeLease() }
        guard let lease = candidateLease,
              let installation = BackupDetection.installationIdentifier(
                namespace: durableStateNamespace, sharedSentinelBaseURL: backupDetectionBaseURL) else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let binding = try activeReplicaBindingGenerationIdentifierForRun(
            accountScopeIdentifier: lease.accountScopeIdentifier)
        if binding != nil {
            guard let state = try BigSyncReplicaBindingStateStore.load(
                store: keyValueStore, key: replicaBindingStateKey),
                state.installationIdentityDigest == BigSyncReplicaBindingStateStore.installationIdentityDigest(for: installation) else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        return BigSyncOutboundPrincipal(durableStateNamespace: durableStateNamespace,
            installationIdentifier: installation, accountScopeIdentifier: lease.accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding, accountInvalidationGeneration: lease.invalidationGeneration)
    }

    public func outboundQuiescenceSnapshot() throws -> BigSyncOutboundQuiescenceSnapshot {
        try outboundQuiescenceCoordinator.snapshot()
    }

    /// Explicit recovery of an exact persisted transport checkpoint. A thrown
    /// or cancelled settlement proof leaves its barrier and submissions intact.
    /// Elapsed time, a missing operation proxy or an empty journal is not proof.
    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        try await recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in },
            authorizingRecovery: authorizingRecovery
        )
    }

    /// The external owner is rechecked after the final account lookup and
    /// immediately before the exact durable checkpoint can be resolved.
    internal func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        guard !syncing, !synchronizationDrainIsActive, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundRecoveryPrincipal()
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            try revalidatingExternalOwner()
            guard outboundRecoveryID == requestID, synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundRecoveryPrincipal() == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        try validateOwnership()
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let replayedCheckpoint = try await replayRecoverableOutboundSubmissions(
            recovery,
            principal: principal,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                try validateOwnership()
            }
        )
        try validateOwnership()
        let evidence = try await authorizingRecovery(replayedCheckpoint)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.resolveRecovery(recovery, evidenceID: evidence)
    }

    private func currentOutboundRecoveryPrincipal() throws -> BigSyncOutboundPrincipal {
        // A restore-reconciliation run can validate transport identity while
        // still withholding the public domain-writer lease. Allow explicit
        // recovery of its closed gate; otherwise a restored client could need
        // to complete an upload in order to obtain permission to recover it.
        if try accountScopeLease() != nil { return try currentOutboundPrincipal() }
        guard let context = activeRunContext else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        return try currentOutboundPrincipal(for: context)
    }

    internal func admitOutboundBatch(for attemptID: UUID) async throws -> BigSyncOutboundBatchLease {
        while true {
            try checkSynchronizationAttempt(attemptID)
            guard let context = activeRunContext else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            let principal = try currentOutboundPrincipal(for: context)
            guard context.accountScopeIdentifier == principal.accountScopeIdentifier,
                  context.replicaBindingGenerationIdentifier == principal.replicaBindingGenerationIdentifier else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            do { return try outboundQuiescenceCoordinator.admit(principal: principal) }
            catch BigSyncOutboundQuiescenceError.busy { try await Task.sleep(nanoseconds: 1_000_000) }
        }
    }

    private func outboundRecoveryDescriptor(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String],
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity?
    ) -> BigSyncOutboundSubmissionRecoveryDescriptor {
        let saves = records.map { record in
            BigSyncOutboundSubmissionItem(
                mutation: .save,
                recordName: record.recordID.recordName,
                zoneName: record.recordID.zoneID.zoneName,
                zoneOwnerName: record.recordID.zoneID.ownerName,
                recordType: record.recordType,
                preparedGeneration: preparedGenerations[record.recordID.recordName],
                priorRecordChangeTag: record.recordChangeTag
            )
        }
        let deletes = recordIDs.map { recordID in
            BigSyncOutboundSubmissionItem(
                mutation: .delete,
                recordName: recordID.recordName,
                zoneName: recordID.zoneID.zoneName,
                zoneOwnerName: recordID.zoneID.ownerName,
                recordType: nil,
                preparedGeneration: preparedGenerations[recordID.recordName],
                priorRecordChangeTag: nil
            )
        }
        return BigSyncOutboundSubmissionRecoveryDescriptor(
            items: saves + deletes,
            transportIdentity: transportIdentity
        )
    }

    internal func modifyRecordsHoldingOutboundLease(
        _ outbound: BigSyncOutboundBatchLease,
        attemptID: UUID,
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String]
    ) async throws -> CloudKitRecordMutationResults {
        let recoverableStore = recordStore as? any CloudKitRecoverableRecordStore
        let preparedTransport = recoverableStore?.prepareRecoverableModifyRecords(
            saving: records,
            deleting: recordIDs,
            savePolicy: .ifServerRecordUnchanged,
            atomically: false
        )
        let recoveryDescriptor = outboundRecoveryDescriptor(
            saving: records,
            deleting: recordIDs,
            preparedGenerations: preparedGenerations,
            transportIdentity: preparedTransport?.transportIdentity
        )
        try await outbound.willSubmitCooperatively(
            recoveryDescriptor: recoveryDescriptor
        )
        do {
            try validateOutboundBatch(outbound, for: attemptID)
            try outbound.validateSubmissionAdmission()
        } catch {
            // No request has entered transport. Cancellation/epoch replacement
            // here cannot manufacture an unknown server outcome.
            try await outbound.didSettleCooperatively()
            throw error
        }
        let results: CloudKitRecordMutationResults
        do {
            if let recoverableStore, let preparedTransport {
                results = try await recoverableStore.executeRecoverableModifyRecords(
                    preparedTransport
                )
            } else {
                results = try await recordStore.modifyRecords(
                    saving: records,
                    deleting: recordIDs,
                    savePolicy: .ifServerRecordUnchanged,
                    atomically: false
                )
            }
        } catch {
            // An operation-wide definitive rejection (for example the batch
            // limit) did not commit either and requires no per-item local ack.
            // Preserve the original retry/error behavior without leaving a
            // phantom indeterminate submission.
            if CloudKitRecordMutationResults.isDefinitiveOperationRejection(error) {
                try await outbound.didSettleCooperatively()
            }
            throw error
        }
        // A definitive server response is not enough to release the durable
        // marker yet. Record it in the batch, then let the caller release the
        // marker only after generation-matched local response processing. If
        // cancellation or an authority fence wins in that window, process death
        // loses this in-memory note but deliberately leaves the durable marker.
        if results.provesDefinitiveSettlement(
            saving: records,
            deleting: recordIDs
        ) {
            try outbound.noteDefinitiveTransportOutcome()
        }
        return results
    }

    internal func validateOutboundBatch(_ batch: BigSyncOutboundBatchLease, for attemptID: UUID) throws {
        try checkSynchronizationAttempt(attemptID)
        guard let context = activeRunContext,
              try currentOutboundPrincipal(for: context) == batch.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try checkRunContext(context)
    }

    internal func revalidateOutboundBatch(_ batch: BigSyncOutboundBatchLease, for attemptID: UUID) async throws {
        try validateOutboundBatch(batch, for: attemptID)
        try await revalidateActiveRunContext(for: attemptID)
        try validateOutboundBatch(batch, for: attemptID)
    }
}

extension CloudKitRecordMutationResults {
    /// Conservatively retain uncertainty on transport failures, cancellation,
    /// absent/malformed results and unknown error codes. A definitive negative
    /// response is safe to settle even though its Realm generation stays queued.
    internal func provesDefinitiveSettlement(saving records: [CKRecord], deleting recordIDs: [CKRecord.ID]) -> Bool {
        guard Set(saveResults.keys) == Set(records.map(\.recordID)),
              Set(deleteResults.keys) == Set(recordIDs) else { return false }
        for record in records {
            switch saveResults[record.recordID] {
            case .success(let returned):
                guard returned.recordID == record.recordID, returned.recordType == record.recordType else { return false }
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil: return false
            }
        }
        for recordID in recordIDs {
            switch deleteResults[recordID] {
            case .success: break
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil: return false
            }
        }
        return true
    }

    internal static func isDefinitiveOperationRejection(_ error: Error) -> Bool {
        // Do not infer whole-batch settlement from a per-item rejection such
        // as an asset error: a non-atomic operation may have other outcomes.
        let error = error as NSError
        guard error.domain == CKErrorDomain else { return false }
        switch CKError.Code(rawValue: error.code) {
        case .limitExceeded, .invalidArguments, .badDatabase, .badContainer: return true
        default: return false
        }
    }

    internal static func isDefinitiveRejection(_ error: Error) -> Bool {
        let error = error as NSError
        guard error.domain == CKErrorDomain else { return false }
        switch CKError.Code(rawValue: error.code) {
        case .serverRecordChanged, .unknownItem, .invalidArguments, .permissionFailure,
             .notAuthenticated, .zoneNotFound, .userDeletedZone, .limitExceeded,
             .constraintViolation, .serverRejectedRequest, .assetFileNotFound,
             .assetFileModified, .badDatabase, .badContainer, .quotaExceeded:
            return true
        default: return false
        }
    }
}
