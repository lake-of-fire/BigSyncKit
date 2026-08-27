import CloudKit
import Foundation

struct PreparedMutationRetryKey: Hashable, Sendable {
    let recordID: CKRecord.ID
    let generation: String?
}

enum BigSyncHandledMutationRetryError: Error, Equatable, Sendable {
    case generationBudgetExceeded(PreparedMutationRetryKey)
    case drainBudgetExceeded
}

struct HandledMutationRetryBudget {
    private(set) var attemptsByKey = [PreparedMutationRetryKey: Int]()
    private(set) var totalAttempts = 0

    mutating func register(
        _ key: PreparedMutationRetryKey,
        maximumPerGeneration: Int,
        maximumPerDrain: Int
    ) throws {
        guard totalAttempts < maximumPerDrain else {
            throw BigSyncHandledMutationRetryError.drainBudgetExceeded
        }
        let attempts = attemptsByKey[key, default: 0]
        guard attempts < maximumPerGeneration else {
            throw BigSyncHandledMutationRetryError
                .generationBudgetExceeded(key)
        }
        attemptsByKey[key] = attempts + 1
        totalAttempts += 1
    }

    mutating func retire(_ key: PreparedMutationRetryKey) {
        attemptsByKey.removeValue(forKey: key)
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func validateInboundLiveResults(
        _ results: [InboundLiveResult],
        records: [CKRecord]
    ) throws {
        try ChangeRequestProcessor.validateInboundLiveResults(
            results,
            records: records
        )
    }

    @BigSyncBackgroundActor
    func validateInboundDeletionResults(
        _ results: [InboundDeletionResult],
        recordIDs: [CKRecord.ID]
    ) throws {
        try ChangeRequestProcessor.validateInboundDeletionResults(
            results,
            recordIDs: recordIDs
        )
    }

    private static let maximumHandledRecordRetries = 5
    /// A stream of continually replaced generations must not keep one drain
    /// alive forever even though every individual generation has a fresh
    /// retry allowance.
    private static let maximumHandledRetriesPerDrain = 1_000

    private func partialMutationError(
        _ failures: [CKRecord.ID: NSError]
    ) -> CKError {
        CKError(
            .partialFailure,
            userInfo: [CKPartialErrorsByItemIDKey: failures]
        )
    }

    @BigSyncBackgroundActor
    func uploadRecordsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        do {
            try await drainRecordUploadsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }

    @BigSyncBackgroundActor
    private func drainRecordUploadsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID
    ) async throws {
        var retryBudget = HandledMutationRetryBudget()
        while true {
            try checkSynchronizationAttempt(attemptID)
            let requestedBatchSize = batchSize
            let prepared = try await adapter.preparedRecordsToUpload(
                limit: requestedBatchSize,
                restrictedToEntityType: restrictedToEntityType
            )
            try checkSynchronizationAttempt(attemptID)
            guard !prepared.isEmpty else { return }

            let records = prepared.map(\.record)
            let generations = prepared.reduce(into: [String: String]()) {
                guard let generation = $1.generation else { return }
                $0[$1.record.recordID.recordName] = generation
            }
            logger.info(
                "QSCloudKitSynchronizer >> Uploading \(records.count) records to \(adapter.recordZoneID)"
            )
            if !didNotifyUpload.contains(adapter.recordZoneID) {
                didNotifyUpload.insert(adapter.recordZoneID)
                delegate?.synchronizerWillUploadChanges(
                    self,
                    to: adapter.recordZoneID
                )
            }

            addMetadata(to: records)
            try await revalidateActiveRunContext(for: attemptID)
            let mutationResults = try await recordStore.modifyRecords(
                saving: records,
                deleting: [],
                savePolicy: .ifServerRecordUnchanged,
                atomically: false
            )
            try Task.checkCancellation()
            try await revalidateActiveRunContext(for: attemptID)

            var savedRecords = [CKRecord]()
            var missingRecordIDs = Set<CKRecord.ID>()
            var conflictedRecordsByID = [CKRecord.ID: CKRecord]()
            var unresolvedFailures = [CKRecord.ID: NSError]()

            for record in records {
                let retryKey = PreparedMutationRetryKey(
                    recordID: record.recordID,
                    generation: generations[record.recordID.recordName]
                )
                guard let result = mutationResults.saveResults[record.recordID] else {
                    unresolvedFailures[record.recordID] = CocoaError(
                        .coderValueNotFound
                    ) as NSError
                    continue
                }
                switch result {
                case .success(let savedRecord):
                    retryBudget.retire(retryKey)
                    savedRecords.append(savedRecord)
                case .failure(let error):
                    let nsError = error as NSError
                    guard nsError.domain == CKErrorDomain else {
                        unresolvedFailures[record.recordID] = nsError
                        continue
                    }
                    let code = CKError.Code(rawValue: nsError.code)
                    guard code == .unknownItem || code == .serverRecordChanged else {
                        unresolvedFailures[record.recordID] = nsError
                        continue
                    }
                    do {
                        try retryBudget.register(
                            retryKey,
                            maximumPerGeneration:
                                Self.maximumHandledRecordRetries,
                            maximumPerDrain:
                                Self.maximumHandledRetriesPerDrain
                        )
                    } catch is BigSyncHandledMutationRetryError {
                        // Preserve the existing partial-failure contract when
                        // a handled conflict exhausts its retry budget. The
                        // prepared journal generation remains pending because
                        // no acknowledgement is sent for this record.
                        unresolvedFailures[record.recordID] = nsError
                        continue
                    }
                    if code == .unknownItem {
                        missingRecordIDs.insert(record.recordID)
                    } else if let serverRecord = nsError.userInfo[
                        CKRecordChangedErrorServerRecordKey
                    ] as? CKRecord {
                        conflictedRecordsByID[record.recordID] = serverRecord
                    } else {
                        unresolvedFailures[record.recordID] = nsError
                    }
                }
            }

            if !savedRecords.isEmpty {
                try await adapter.didUpload(
                    savedRecords: savedRecords,
                    matchingGenerations: generations
                )
                try await revalidateActiveRunContext(for: attemptID)
            }
            if !missingRecordIDs.isEmpty {
                try await adapter.requeueMissingServerRecords(
                    Array(missingRecordIDs),
                    matchingPreparedGenerations: generations
                )
                try await revalidateActiveRunContext(for: attemptID)
            }
            if !conflictedRecordsByID.isEmpty {
                let conflictedRecords = Array(conflictedRecordsByID.values)
                    .sorted {
                        $0.recordID.recordName < $1.recordID.recordName
                    }
                let results = try await adapter.saveChanges(
                    in: conflictedRecords,
                    forceSave: true
                )
                try ChangeRequestProcessor.validateInboundLiveResults(
                    results,
                    records: conflictedRecords
                )
                try await revalidateActiveRunContext(for: attemptID)
                try await adapter.persistImportedChanges()
                try await revalidateActiveRunContext(for: attemptID)
            }

            guard unresolvedFailures.isEmpty else {
                if unresolvedFailures.values.contains(where: {
                    $0.domain == CKErrorDomain
                        && $0.code == CKError.limitExceeded.rawValue
                }) {
                    reduceBatchSize()
                }
                throw partialMutationError(unresolvedFailures)
            }

            let handledFailures = missingRecordIDs.count
                + conflictedRecordsByID.count
            if handledFailures == 0,
               records.count >= requestedBatchSize {
                increaseBatchSize()
            }
            guard handledFailures > 0
                    || records.count >= requestedBatchSize else { return }
            await Task.yield()
        }
    }

    @BigSyncBackgroundActor
    func uploadDeletionsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        do {
            try await drainRecordDeletionsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }

    @BigSyncBackgroundActor
    private func drainRecordDeletionsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID
    ) async throws {
        var retryBudget = HandledMutationRetryBudget()
        while true {
            try checkSynchronizationAttempt(attemptID)
            let requestedBatchSize = batchSize
            let prepared = try await adapter.preparedRecordDeletions(
                limit: requestedBatchSize,
                restrictedToEntityType: restrictedToEntityType
            )
            try checkSynchronizationAttempt(attemptID)
            guard !prepared.isEmpty else { return }

            let recordIDs = prepared.map(\.recordID)
            let generations = prepared.reduce(into: [String: String]()) {
                guard let generation = $1.generation else { return }
                $0[$1.recordID.recordName] = generation
            }
            try await revalidateActiveRunContext(for: attemptID)
            let mutationResults = try await recordStore.modifyRecords(
                saving: [],
                deleting: recordIDs,
                savePolicy: .ifServerRecordUnchanged,
                atomically: false
            )
            try Task.checkCancellation()
            try await revalidateActiveRunContext(for: attemptID)

            var acknowledged = [CKRecord.ID]()
            var conflictedRecordsByID = [CKRecord.ID: CKRecord]()
            var unresolvedFailures = [CKRecord.ID: NSError]()
            for recordID in recordIDs {
                let retryKey = PreparedMutationRetryKey(
                    recordID: recordID,
                    generation: generations[recordID.recordName]
                )
                guard let result = mutationResults.deleteResults[recordID] else {
                    unresolvedFailures[recordID] = CocoaError(
                        .coderValueNotFound
                    ) as NSError
                    continue
                }
                switch result {
                case .success:
                    retryBudget.retire(retryKey)
                    acknowledged.append(recordID)
                case .failure(let error):
                    let nsError = error as NSError
                    if nsError.domain == CKErrorDomain,
                       nsError.code == CKError.unknownItem.rawValue {
                        retryBudget.retire(retryKey)
                        acknowledged.append(recordID)
                    } else if nsError.domain == CKErrorDomain,
                              nsError.code == CKError.serverRecordChanged.rawValue,
                              let serverRecord = nsError.userInfo[
                                  CKRecordChangedErrorServerRecordKey
                              ] as? CKRecord {
                        do {
                            try retryBudget.register(
                                retryKey,
                                maximumPerGeneration:
                                    Self.maximumHandledRecordRetries,
                                maximumPerDrain:
                                    Self.maximumHandledRetriesPerDrain
                            )
                        } catch is BigSyncHandledMutationRetryError {
                            // Keep the tombstone pending and surface a normal
                            // partial failure once this generation cannot be
                            // safely rebased any further.
                            unresolvedFailures[recordID] = nsError
                            continue
                        }
                        conflictedRecordsByID[recordID] = serverRecord
                    } else {
                        unresolvedFailures[recordID] = nsError
                    }
                }
            }

            if !acknowledged.isEmpty {
                try await adapter.didDelete(
                    recordIDs: acknowledged,
                    matchingGenerations: generations
                )
                try await revalidateActiveRunContext(for: attemptID)
            }
            if !conflictedRecordsByID.isEmpty {
                // Rebase only server system fields before retrying the local
                // tombstone. Applying inbound model values here would either
                // overwrite the local delete or be (correctly) ignored by a
                // local-wins importer, leaving stale conflict metadata.
                try await adapter.rebasePendingDeletionMetadata(
                    using: Array(conflictedRecordsByID.values),
                    matchingPreparedGenerations: generations
                )
                try await revalidateActiveRunContext(for: attemptID)
            }
            guard unresolvedFailures.isEmpty else {
                if unresolvedFailures.values.contains(where: {
                    $0.domain == CKErrorDomain
                        && $0.code == CKError.limitExceeded.rawValue
                }) {
                    reduceBatchSize()
                }
                throw partialMutationError(unresolvedFailures)
            }
            let handledFailures = conflictedRecordsByID.count
            if handledFailures == 0, recordIDs.count >= requestedBatchSize {
                increaseBatchSize()
            }
            guard handledFailures > 0 || recordIDs.count >= requestedBatchSize else { return }
            await Task.yield()
        }
    }
}
