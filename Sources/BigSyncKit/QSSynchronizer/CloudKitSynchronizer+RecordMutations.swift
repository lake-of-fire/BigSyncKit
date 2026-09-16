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

/// A semantic quarantine is unresolved work, not a successfully rebased
/// conflict. Stop this drain instead of spending its retry budget resending
/// the same stale change tag. Successful siblings were already acknowledged;
/// the quarantined record's local generation remains durable.
public struct BigSyncSemanticUploadConflictError: Error, Sendable {
    public let recordNames: [String]
}

func requireResolvedUploadConflictOutcomes(
    _ results: [InboundLiveResult],
    preservingFailures otherFailures: [CKRecord.ID: NSError] = [:]
) throws {
    let quarantined = results.filter {
        if case .quarantined = $0.disposition { return true }
        return false
    }
    guard !quarantined.isEmpty else { return }
    let semanticError = BigSyncSemanticUploadConflictError(
        recordNames: quarantined.map { $0.event.recordName }.sorted()
    )
    guard !otherFailures.isEmpty else { throw semanticError }

    // Account stops, retry-after deadlines and transport failures are independent
    // constraints on this same batch. A semantic failure must not hide them from
    // the synchronizer's lifecycle/retry classifier. Keep per-record evidence;
    // never replace a successful sibling or turn quarantine into a size retry.
    var failures = otherFailures
    for result in quarantined {
        let event = result.event
        let recordID = CKRecord.ID(recordName: event.recordName, zoneID: .init(
            zoneName: event.zoneName, ownerName: event.zoneOwnerName
        ))
        failures[recordID] = BigSyncSemanticUploadConflictError(
            recordNames: [event.recordName]
        ) as NSError
    }
    throw CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: failures])
}

/// Local reconciliation failures do not cancel independent transport constraints
/// already reported for sibling records. Preserve both without labelling an
/// acknowledged success as a CloudKit failure. Cancellation remains terminal.
func preservingSiblingMutationFailures(
    _ error: Error,
    failedRecordIDs: [CKRecord.ID],
    otherFailures: [CKRecord.ID: NSError]
) -> Error {
    guard !(error is CancellationError), !otherFailures.isEmpty else { return error }
    var failures = otherFailures
    for recordID in failedRecordIDs where failures[recordID] == nil {
        failures[recordID] = error as NSError
    }
    return CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: failures])
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

/// A short successful batch is not proof of quiescence: generation-matched
/// acknowledgement can expose a newer target journal generation. The owning
/// drain gets one more preparation turn whenever the adapter still reports
/// work; a differently-scoped or opposite-kind mutation then yields an empty
/// preparation immediately and returns to its owning phase.
func bigSyncMutationDrainShouldContinue(
    handledFailures: Int,
    completedCount: Int,
    requestedBatchSize: Int,
    adapterHasChanges: Bool
) -> Bool {
    handledFailures > 0
        || completedCount >= requestedBatchSize
        || adapterHasChanges
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

    /// Every immediate retry strictly reduces the attempted multi-item size.
    /// Keep that ceiling for this drain so successful pieces do not regrow
    /// into the rejected request. No journal generation is acknowledged here.
    @BigSyncBackgroundActor
    private func retrySmallerMutationBatch(
        after error: Error,
        attemptedCount: Int,
        ceiling: inout Int?
    ) -> Bool {
        let constraints = CloudKitRetryConstraints(error)
        guard constraints.codes.contains(.limitExceeded) else { return false }
        let reduced = max(1, attemptedCount / 2)
        batchSize = min(batchSize, reduced)
        ceiling = min(ceiling ?? batchSize, batchSize)
        return attemptedCount > 1
            && constraints.containsOnlySizeLimitFailures
            && !constraints.requiresDeferredRetry
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
        var sizeLimitCeiling: Int?
        while true {
            try checkSynchronizationAttempt(attemptID)
            let requestedBatchSize = min(batchSize, sizeLimitCeiling ?? batchSize)
            let prepared = try await adapter.preparedRecordsToUpload(
                limit: requestedBatchSize,
                restrictedToEntityType: restrictedToEntityType
            )
            try checkSynchronizationAttempt(attemptID)
            guard !prepared.isEmpty else { return }

            let uncertain = prepared.filter(\.requiresAcceptanceCheck)
            if !uncertain.isEmpty, let lookup = recordStore as? any CloudKitRecordFetching {
                let fetched = try await lookup.fetchRecords(with: uncertain.map { $0.record.recordID })
                try await revalidateActiveRunContext(for: attemptID)
                var observations = [CKRecord]()
                var lookupFailures = [CKRecord.ID: NSError]()
                for candidate in uncertain {
                    let id = candidate.record.recordID
                    guard let result = fetched[id] else {
                        lookupFailures[id] = CocoaError(.coderValueNotFound) as NSError
                        continue
                    }
                    switch result {
                    case let .success(record):
                        guard record.recordID == id, record.recordType == candidate.record.recordType else {
                            throw BigSyncRecordRebaseError.inconsistentReceipt(id.recordName)
                        }
                        observations.append(record)
                    case let .failure(error):
                        let ns = error as NSError
                        if ns.domain != CKErrorDomain || ns.code != CKError.unknownItem.rawValue {
                            lookupFailures[id] = ns
                        }
                        // Not found does not prove the earlier request never
                        // ran; retry the same candidate with its save fence.
                    }
                }
                if !observations.isEmpty {
                    for candidate in uncertain where observations.contains(where: {
                        $0.recordID == candidate.record.recordID
                    }) {
                        try retryBudget.register(.init(recordID: candidate.record.recordID,
                            generation: candidate.generation),
                            maximumPerGeneration: Self.maximumHandledRecordRetries,
                            maximumPerDrain: Self.maximumHandledRetriesPerDrain)
                    }
                    let outcomes: [InboundLiveResult]
                    do { outcomes = try await adapter.saveChanges(in: observations, forceSave: true) }
                    catch { throw preservingSiblingMutationFailures(error,
                        failedRecordIDs: observations.map(\.recordID), otherFailures: lookupFailures) }
                    try await adapter.persistImportedChanges()
                    try await adapter.didFinishImport()
                    try await revalidateActiveRunContext(for: attemptID)
                    try requireResolvedUploadConflictOutcomes(outcomes, preservingFailures: lookupFailures)
                    guard lookupFailures.isEmpty else { throw partialMutationError(lookupFailures) }
                    // The observed accepted base retires/supersedes the old
                    // candidate. Reprepare rather than send a stale batch.
                    continue
                }
                guard lookupFailures.isEmpty else { throw partialMutationError(lookupFailures) }
            }

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
            let mutationResults: CloudKitRecordMutationResults
            do {
                mutationResults = try await recordStore.modifyRecords(
                    saving: records,
                    deleting: [],
                    savePolicy: .ifServerRecordUnchanged,
                    atomically: false
                )
            } catch {
                try checkSynchronizationAttempt(attemptID)
                if let context = activeRunContext { try checkRunContext(context) }
                guard retrySmallerMutationBatch(
                    after: error, attemptedCount: records.count,
                    ceiling: &sizeLimitCeiling
                ) else { throw error }
                // Only pure, reducible limits retry here. Account stops,
                // server delays and unrelated failures retain the outer path.
                try await revalidateActiveRunContext(for: attemptID)
                continue
            }
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
                    matchingPreparedUploads: prepared
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
                let results: [InboundLiveResult]
                do {
                    results = try await adapter.saveChanges(
                        in: conflictedRecords,
                        forceSave: true
                    )
                    try ChangeRequestProcessor.validateInboundLiveResults(
                        results,
                        records: conflictedRecords
                    )
                } catch {
                    try Task.checkCancellation()
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    throw preservingSiblingMutationFailures(
                        error, failedRecordIDs: conflictedRecords.map(\.recordID),
                        otherFailures: unresolvedFailures
                    )
                }
                try requireResolvedUploadConflictOutcomes(
                    results, preservingFailures: unresolvedFailures
                )
                try await revalidateActiveRunContext(for: attemptID)
                do {
                    try await adapter.persistImportedChanges()
                } catch {
                    try Task.checkCancellation()
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    throw preservingSiblingMutationFailures(
                        error, failedRecordIDs: conflictedRecords.map(\.recordID),
                        otherFailures: unresolvedFailures
                    )
                }
                try await revalidateActiveRunContext(for: attemptID)
            }

            guard unresolvedFailures.isEmpty else {
                let error = partialMutationError(unresolvedFailures)
                if retrySmallerMutationBatch(
                    after: error, attemptedCount: records.count,
                    ceiling: &sizeLimitCeiling
                ) {
                    await Task.yield()
                    continue
                }
                throw error
            }

            let handledFailures = missingRecordIDs.count
                + conflictedRecordsByID.count
            if sizeLimitCeiling == nil, handledFailures == 0,
               records.count >= requestedBatchSize {
                increaseBatchSize()
            }
            guard bigSyncMutationDrainShouldContinue(
                handledFailures: handledFailures,
                completedCount: records.count,
                requestedBatchSize: requestedBatchSize,
                adapterHasChanges: adapter.hasChanges
            ) else { return }
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
        var sizeLimitCeiling: Int?
        while true {
            try checkSynchronizationAttempt(attemptID)
            let requestedBatchSize = min(batchSize, sizeLimitCeiling ?? batchSize)
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
            let mutationResults: CloudKitRecordMutationResults
            do {
                mutationResults = try await recordStore.modifyRecords(
                    saving: [],
                    deleting: recordIDs,
                    savePolicy: .ifServerRecordUnchanged,
                    atomically: false
                )
            } catch {
                try checkSynchronizationAttempt(attemptID)
                if let context = activeRunContext { try checkRunContext(context) }
                guard retrySmallerMutationBatch(
                    after: error, attemptedCount: recordIDs.count,
                    ceiling: &sizeLimitCeiling
                ) else { throw error }
                // Only pure, reducible limits retry here. Account stops,
                // server delays and unrelated failures retain the outer path.
                try await revalidateActiveRunContext(for: attemptID)
                continue
            }
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
                let error = partialMutationError(unresolvedFailures)
                if retrySmallerMutationBatch(
                    after: error, attemptedCount: recordIDs.count,
                    ceiling: &sizeLimitCeiling
                ) {
                    await Task.yield()
                    continue
                }
                throw error
            }
            let handledFailures = conflictedRecordsByID.count
            if sizeLimitCeiling == nil, handledFailures == 0,
               recordIDs.count >= requestedBatchSize {
                increaseBatchSize()
            }
            guard bigSyncMutationDrainShouldContinue(
                handledFailures: handledFailures,
                completedCount: recordIDs.count,
                requestedBatchSize: requestedBatchSize,
                adapterHasChanges: adapter.hasChanges
            ) else { return }
            await Task.yield()
        }
    }
}
