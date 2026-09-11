import CloudKit
import Foundation

/// Recovery of a persisted outbound uncertainty marker must never guess from
/// elapsed time, an empty local journal, or a vanished process lock. When the
/// default CloudKit transport can return the exact long-lived operation, replay
/// its saved callbacks and feed them through the same generation-matched local
/// reconciliation used by an ordinary synchronization. Unsupported/expired
/// operations remain in the durable checkpoint for the existing host proof.
enum BigSyncLongLivedReplayError: Error, Equatable, Sendable {
    case unexpectedOperationType
    case transportIdentityMismatch
    case requestIdentityMismatch
    case missingAdapter
}

/// Only errors delivered by the exact recovered modify operation's terminal
/// callback carry this provenance. A fetchLongLivedOperation lookup error (even
/// badContainer/invalidArguments/limitExceeded) says nothing about whether the
/// ORIGINAL mutation committed and must never retire its submission marker.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal struct BigSyncLongLivedReplayTerminalFailure: Error {
    let underlyingError: Error

    static func markingTerminalDelivery(
        _ result: Result<CloudKitRecordMutationResults, Error>
    ) -> Result<CloudKitRecordMutationResults, Error> {
        result.mapError { Self(underlyingError: $0) }
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal protocol CloudKitLongLivedRecordRecovering: CloudKitRecordStore {
    /// Raw lookup/identity/admission errors never certify the original request.
    /// Only an exact terminal modify callback may wrap its error in
    /// BigSyncLongLivedReplayTerminalFailure; item outcomes remain unwrapped.
    func recoverLongLivedModifyRecords(
        identity: BigSyncOutboundSubmissionTransportIdentity,
        descriptor: BigSyncOutboundSubmissionRecoveryDescriptor
    ) async throws -> CloudKitRecordMutationResults?
}

private extension BigSyncOutboundSubmissionItem {
    var recordID: CKRecord.ID {
        CKRecord.ID(
            recordName: recordName,
            zoneID: CKRecordZone.ID(
                zoneName: zoneName,
                ownerName: zoneOwnerName
            )
        )
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitLongLivedRecordRecovering {
    internal func recoverLongLivedModifyRecords(
        identity: BigSyncOutboundSubmissionTransportIdentity,
        descriptor: BigSyncOutboundSubmissionRecoveryDescriptor
    ) async throws -> CloudKitRecordMutationResults? {
        guard let container else { return nil }

        let fetched: CKOperation? = try await awaitCancellableCloudKitCallback(
            timeoutNanoseconds: 60_000_000_000
        ) { completion in
            container.fetchLongLivedOperation(
                withID: identity.longLivedOperationID
            ) { operation, error in
                if let error {
                    completion(.failure(error))
                } else {
                    completion(.success(operation))
                }
            }
        }
        guard let fetched else {
            // CloudKit may no longer retain the operation proxy. The caller
            // keeps the uncertainty marker and falls back to authoritative
            // domain/server proof; absence is never settlement evidence.
            return nil
        }
        guard let operation = fetched as? CKModifyRecordsOperation else {
            throw BigSyncLongLivedReplayError.unexpectedOperationType
        }
        guard operation.operationID == identity.longLivedOperationID else {
            throw BigSyncLongLivedReplayError.transportIdentityMismatch
        }
        if let recoveredClientToken = operation.clientChangeTokenData,
           recoveredClientToken != identity.clientChangeTokenData {
            throw BigSyncLongLivedReplayError.transportIdentityMismatch
        }
        guard operation.savePolicy == .ifServerRecordUnchanged,
              !operation.isAtomic else {
            throw BigSyncLongLivedReplayError.requestIdentityMismatch
        }

        let expectedSaveItems = descriptor.items.filter { $0.mutation == .save }
        let expectedDeleteItems = descriptor.items.filter { $0.mutation == .delete }
        let expectedSaveIDs = Set(expectedSaveItems.map(\.recordID))
        let expectedDeleteIDs = Set(expectedDeleteItems.map(\.recordID))

        // CloudKit normally preserves the original request arrays on the
        // recovered proxy. Treat a present array as an additional witness, but
        // do not require it: operation ID + client token are the durable
        // transport identity and saved callbacks can be replayed without the
        // request payload being exposed again.
        if let recoveredSaves = operation.recordsToSave {
            guard Set(recoveredSaves.map(\.recordID)) == expectedSaveIDs else {
                throw BigSyncLongLivedReplayError.requestIdentityMismatch
            }
            let expectedByID = Dictionary(
                uniqueKeysWithValues: expectedSaveItems.map { ($0.recordID, $0) }
            )
            for record in recoveredSaves {
                guard let expected = expectedByID[record.recordID],
                      expected.recordType == record.recordType,
                      expected.priorRecordChangeTag == record.recordChangeTag else {
                    throw BigSyncLongLivedReplayError.requestIdentityMismatch
                }
            }
        }
        if let recoveredDeletes = operation.recordIDsToDelete,
           Set(recoveredDeletes) != expectedDeleteIDs {
            throw BigSyncLongLivedReplayError.requestIdentityMismatch
        }

        // Replay uses the same single-delivery collector as live mutations.
        // Incomplete item callbacks must not become an operation-wide rejection
        // which the recovery loop could use to retire an uncertain submission.
        let prepared = CloudKitPreparedRecordMutation(
            operation: operation, transportIdentity: identity,
            expectedSaveIDs: expectedSaveIDs, expectedDeleteIDs: expectedDeleteIDs
        )
        return try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<CloudKitRecordMutationResults, Error>) in
            do {
                try prepared.installResultHandlers { result in
                    continuation.resume(with:
                        BigSyncLongLivedReplayTerminalFailure.markingTerminalDelivery(result))
                }
                // Resume this exact proxy; never allocate a replacement request.
                container.add(operation)
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension CloudKitRecordMutationResults {
    internal static func isDefinitiveReplayedOperationRejection(_ error: Error) -> Bool {
        guard let terminal = error as? BigSyncLongLivedReplayTerminalFailure else {
            return false
        }
        return isDefinitiveOperationRejection(terminal.underlyingError)
    }

    internal func provesDefinitiveSettlement(
        descriptor: BigSyncOutboundSubmissionRecoveryDescriptor
    ) -> Bool {
        let saveItems = descriptor.items.filter { $0.mutation == .save }
        let deleteItems = descriptor.items.filter { $0.mutation == .delete }
        let expectedSaveIDs = Set(saveItems.map(\.recordID))
        let expectedDeleteIDs = Set(deleteItems.map(\.recordID))
        guard Set(saveResults.keys) == expectedSaveIDs,
              Set(deleteResults.keys) == expectedDeleteIDs else {
            return false
        }

        for item in saveItems {
            switch saveResults[item.recordID] {
            case .success(let record):
                guard record.recordID == item.recordID,
                      record.recordType == item.recordType else {
                    return false
                }
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil:
                return false
            }
        }
        for item in deleteItems {
            switch deleteResults[item.recordID] {
            case .success:
                break
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil:
                return false
            }
        }
        return true
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    private func replayAdapter(
        for descriptor: BigSyncOutboundSubmissionRecoveryDescriptor
    ) throws -> ModelAdapter {
        guard let first = descriptor.items.first else {
            throw BigSyncOutboundQuiescenceError.invalidState
        }
        let zoneID = CKRecordZone.ID(
            zoneName: first.zoneName,
            ownerName: first.zoneOwnerName
        )
        guard descriptor.items.allSatisfy({ $0.recordID.zoneID == zoneID }),
              let adapter = modelAdapters.first(where: {
                  $0.recordZoneID == zoneID
              }) else {
            throw BigSyncLongLivedReplayError.missingAdapter
        }
        return adapter
    }

    @BigSyncBackgroundActor
    private func replayGenerations(
        for descriptor: BigSyncOutboundSubmissionRecoveryDescriptor
    ) -> [String: String]? {
        var generations = [String: String]()
        for item in descriptor.items {
            guard let generation = item.preparedGeneration,
                  !generation.isEmpty else {
                return nil
            }
            generations[item.recordName] = generation
        }
        return generations
    }

    @BigSyncBackgroundActor
    private func reconcileReplayedSubmission(
        _ submission: BigSyncOutboundSubmission,
        principal: BigSyncOutboundPrincipal,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void
    ) async throws -> Bool {
        guard submission.principal == principal,
              let descriptor = submission.recoveryDescriptor,
              let transportIdentity = descriptor.transportIdentity,
              let generations = replayGenerations(for: descriptor),
              let recoveringStore = recordStore as? any CloudKitLongLivedRecordRecovering else {
            return false
        }
        let adapter = try replayAdapter(for: descriptor)

        try revalidatingExternalOwner()
        try await adapter.activateTransportNamespace(
            containerIdentifier: containerIdentifier,
            databaseScope: database.databaseScope
        )
        try revalidatingExternalOwner()
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: principal.accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                principal.replicaBindingGenerationIdentifier
        )
        try revalidatingExternalOwner()

        let mutationResults: CloudKitRecordMutationResults
        do {
            guard let recovered = try await recoveringStore
                .recoverLongLivedModifyRecords(
                    identity: transportIdentity,
                    descriptor: descriptor
                ) else {
                return false
            }
            mutationResults = recovered
        } catch {
            // Only a definitive rejection from the original modify operation's
            // terminal callback can settle without per-item handling. Lookup or
            // proxy setup failures have no authority over that older mutation.
            try revalidatingExternalOwner()
            if CloudKitRecordMutationResults
                .isDefinitiveReplayedOperationRejection(error) {
                return true
            }
            throw error
        }
        try revalidatingExternalOwner()

        let saveItems = descriptor.items.filter { $0.mutation == .save }
        let deleteItems = descriptor.items.filter { $0.mutation == .delete }

        var savedRecords = [CKRecord]()
        var missingSaveIDs = [CKRecord.ID]()
        var conflictedSaveRecords = [CKRecord]()
        for item in saveItems {
            guard let result = mutationResults.saveResults[item.recordID] else {
                continue
            }
            switch result {
            case .success(let record):
                guard record.recordID == item.recordID,
                      record.recordType == item.recordType else {
                    throw BigSyncRecordMutationIdentityError.responseIdentityMismatch
                }
                savedRecords.append(record)
            case .failure(let error):
                let nsError = error as NSError
                guard nsError.domain == CKErrorDomain else { continue }
                switch CKError.Code(rawValue: nsError.code) {
                case .unknownItem:
                    missingSaveIDs.append(item.recordID)
                case .serverRecordChanged:
                    guard let serverRecord = nsError.userInfo[
                        CKRecordChangedErrorServerRecordKey
                    ] as? CKRecord else {
                        continue
                    }
                    guard serverRecord.recordID == item.recordID,
                          serverRecord.recordType == item.recordType else {
                        throw BigSyncRecordMutationIdentityError.responseIdentityMismatch
                    }
                    conflictedSaveRecords.append(serverRecord)
                default:
                    break
                }
            }
        }

        if !savedRecords.isEmpty {
            try await adapter.didUpload(
                savedRecords: savedRecords,
                matchingGenerations: generations
            )
            try revalidatingExternalOwner()
        }
        if !missingSaveIDs.isEmpty {
            try await adapter.requeueMissingServerRecords(
                missingSaveIDs,
                matchingPreparedGenerations: generations
            )
            try revalidatingExternalOwner()
        }
        if !conflictedSaveRecords.isEmpty {
            let ordered = conflictedSaveRecords.sorted {
                $0.recordID.recordName < $1.recordID.recordName
            }
            let results = try await adapter.saveChanges(
                in: ordered,
                forceSave: true
            )
            try ChangeRequestProcessor.validateInboundLiveResults(
                results,
                records: ordered
            )
            try revalidatingExternalOwner()
            try await adapter.persistImportedChanges()
            try revalidatingExternalOwner()
        }

        var acknowledgedDeleteIDs = [CKRecord.ID]()
        var conflictedDeleteRecords = [CKRecord]()
        for item in deleteItems {
            guard let result = mutationResults.deleteResults[item.recordID] else {
                continue
            }
            switch result {
            case .success:
                acknowledgedDeleteIDs.append(item.recordID)
            case .failure(let error):
                let nsError = error as NSError
                guard nsError.domain == CKErrorDomain else { continue }
                switch CKError.Code(rawValue: nsError.code) {
                case .unknownItem:
                    acknowledgedDeleteIDs.append(item.recordID)
                case .serverRecordChanged:
                    guard let serverRecord = nsError.userInfo[
                        CKRecordChangedErrorServerRecordKey
                    ] as? CKRecord else {
                        continue
                    }
                    guard serverRecord.recordID == item.recordID else {
                        throw BigSyncRecordMutationIdentityError.responseIdentityMismatch
                    }
                    conflictedDeleteRecords.append(serverRecord)
                default:
                    break
                }
            }
        }

        if !acknowledgedDeleteIDs.isEmpty {
            try await adapter.didDelete(
                recordIDs: acknowledgedDeleteIDs,
                matchingGenerations: generations
            )
            try revalidatingExternalOwner()
        }
        if !conflictedDeleteRecords.isEmpty {
            try await adapter.rebasePendingDeletionMetadata(
                using: conflictedDeleteRecords,
                matchingPreparedGenerations: generations
            )
            try revalidatingExternalOwner()
        }

        return mutationResults.provesDefinitiveSettlement(
            descriptor: descriptor
        )
    }

    /// Best-effort exact replay under already-acquired exclusive recovery
    /// ownership. A marker retires only after CloudKit supplies a definitive
    /// result and every required generation-matched local callback completes.
    /// Unsupported/expired operations are left untouched for host recovery.
    @BigSyncBackgroundActor
    internal func replayRecoverableOutboundSubmissions(
        _ recovery: BigSyncOutboundRecoveryLease,
        principal: BigSyncOutboundPrincipal,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        let candidates = recovery.snapshot.outstandingSubmissions
        for submission in candidates {
            try revalidatingExternalOwner()
            guard submission.principal == principal else { continue }
            let reconciled = try await reconcileReplayedSubmission(
                submission,
                principal: principal,
                revalidatingExternalOwner: revalidatingExternalOwner
            )
            try revalidatingExternalOwner()
            if reconciled {
                try outboundQuiescenceCoordinator.settleRecoveredSubmission(
                    recovery,
                    submission: submission,
                    principal: principal
                )
                try revalidatingExternalOwner()
            }
        }
        return recovery.snapshot
    }
}
