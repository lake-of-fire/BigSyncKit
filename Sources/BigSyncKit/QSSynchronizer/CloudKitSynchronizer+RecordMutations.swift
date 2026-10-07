import CloudKit
import Foundation

/// Bind each returned record to the request whose result slot contained it.
/// A batch member is not interchangeable with a different member of that batch.
private func mutationResponseRecordMatches(
    _ record: CKRecord,
    expectedID: CKRecord.ID,
    expectedType: String?
) -> Bool {
    guard record.recordID == expectedID else { return false }
    return expectedType.map { $0.utf8.elementsEqual(record.recordType.utf8) } ?? true
}

/// One result slot must represent exactly one preparation in the adapter's
/// zone. Reject ambiguous batches before lookup or mutation: receipt validation
/// after a server write is too late to protect the request boundary.
private func validatePreparedMutationRecordIDs(
    _ recordIDs: [CKRecord.ID],
    in expectedZone: CKRecordZone.ID
) throws {
    var seen = Set<CKRecord.ID>()
    for recordID in recordIDs {
        guard recordID.zoneID == expectedZone,
              seen.insert(recordID).inserted else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
    }
}

/// Validate before any local acknowledgement/import or account-routed await.
/// Keep invalid slots as failures rather than abandoning successful siblings.
/// The original conflict error remains an underlying cause so its retry-after
/// and account constraints survive rejection of the malformed record payload.
private func validatedMutationResults<Value>(
    _ results: [CKRecord.ID: Result<Value, Error>],
    expected: [(recordID: CKRecord.ID, recordType: String?)],
    successRecord: (Value) -> CKRecord?
) -> [CKRecord.ID: Result<Value, Error>] {
    var validated = [CKRecord.ID: Result<Value, Error>]()
    for identity in expected {
        let id = identity.recordID
        guard let result = results[id] else {
            validated[id] = .failure(CocoaError(.coderValueNotFound))
            continue
        }
        let record: CKRecord?
        let originalFailure: NSError?
        switch result {
        case let .success(value):
            record = successRecord(value)
            originalFailure = nil
        case let .failure(error):
            let failure = error as NSError
            record = failure.domain == CKErrorDomain
                && failure.code == CKError.serverRecordChanged.rawValue
                ? failure.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord
                : nil
            originalFailure = failure
        }
        if let record, !mutationResponseRecordMatches(
            record, expectedID: id, expectedType: identity.recordType
        ) {
            let invalid = BigSyncRecordRebaseError.inconsistentReceipt(id.recordName) as NSError
            if let originalFailure {
                var info = invalid.userInfo
                info[NSUnderlyingErrorKey] = originalFailure
                validated[id] = .failure(NSError(
                    domain: invalid.domain, code: invalid.code, userInfo: info
                ))
            } else {
                validated[id] = .failure(invalid)
            }
        } else {
            validated[id] = result
        }
    }
    return validated
}

/// A repairable record outcome is not permission to ignore operation-level
/// recovery. Preserve constrained failures for the outer synchronization
/// lifecycle instead of consuming them in an immediate repair/retry loop.
/// An incomplete bounded scan cannot prove the absence of deeper constraints.
/// Only ordinary miss/conflict codes may be handled here. Other recognized
/// CloudKit failures must not disappear merely because the outer code is one
/// of those two; internal non-CloudKit SDK details retain their existing path.
private func mutationFailureAllowsImmediateRepair(_ error: Error) -> Bool {
    let constraints = CloudKitRetryConstraints(error)
    return constraints.isErrorGraphComplete
        && !constraints.requiresDeferredRetry
        && constraints.codes.isSubset(of: [.unknownItem, .serverRecordChanged])
}

/// Absence is a valid deletion receipt, but independent conditions attached to
/// that receipt still constrain the operation. Keep those causes in a named
/// envelope, separate from the dictionary of records whose deletion failed.
private func preservingAcknowledgedDeletionConstraints(
    _ error: Error?,
    constraints: [CKRecord.ID: NSError]
) -> Error? {
    guard !constraints.isEmpty else { return error }
    if let error, error is CancellationError { return error }
    var failures = [AnyHashable: Error]()
    var info = [String: Any]()
    if let error {
        let original = error as NSError
        if original.domain == CKErrorDomain,
           original.code == CKError.partialFailure.rawValue,
           let items = original.userInfo[CKPartialErrorsByItemIDKey] as? [AnyHashable: Error] {
            failures = items
            info = original.userInfo
        } else {
            info[NSUnderlyingErrorKey] = original
        }
    }
    failures["acknowledgedDeletionConstraints"] = CKError(
        .partialFailure, userInfo: [CKPartialErrorsByItemIDKey: constraints]
    )
    info[CKPartialErrorsByItemIDKey] = failures
    return CKError(.partialFailure, userInfo: info)
}

struct PreparedMutationRetryKey: Hashable, Sendable {
    let recordID: CKRecord.ID
    let generation: String?
}

/// Keep every returned failure for the requested identities available before
/// any local processing or account-routed await can fail independently.
func returnedMutationFailures<Value>(
    in results: [CKRecord.ID: Result<Value, Error>],
    for recordIDs: [CKRecord.ID]
) -> [CKRecord.ID: NSError] {
    recordIDs.reduce(into: [:]) { failures, recordID in
        if case let .failure(error)? = results[recordID] {
            failures[recordID] = error as NSError
        }
    }
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
    // An acknowledgement failure is a local durability failure, not a failed
    // CloudKit save/delete. Such callers pass no failed record IDs: retain the
    // local cause without relabelling successful server outcomes as failures.
    return CKError(.partialFailure, userInfo: [
        CKPartialErrorsByItemIDKey: failures,
        NSUnderlyingErrorKey: error as NSError,
    ])
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

    /// Inspect returned item failures before asking CloudKit for account identity
    /// again. A returned account stop forbids that extra request; local receipt
    /// commits still use the synchronous attempt/binding fence. Other validation
    /// failures must not replace an already-known sibling retry constraint.
    @BigSyncBackgroundActor
    private func revalidateMutationResultContext(
        for attemptID: UUID,
        preserving failures: [CKRecord.ID: NSError]
    ) async throws {
        do {
            try checkSynchronizationAttempt(attemptID)
            if let context = activeRunContext { try checkRunContext(context) }
            if !failures.isEmpty,
               CloudKitRetryConstraints(partialMutationError(failures)).blocksAccountOperations {
                return
            }
            try await revalidateActiveRunContext(for: attemptID)
        } catch {
            throw preservingSiblingMutationFailures(
                error, failedRecordIDs: [], otherFailures: failures
            )
        }
    }

    /// Every immediate retry strictly reduces the requested batch limit.
    /// An adapter may return too many records; that must not keep a drain
    /// retrying forever at the same limit. Successful pieces cannot regrow
    /// into the rejected request. No journal generation is acknowledged here.
    @BigSyncBackgroundActor
    private func retrySmallerMutationBatch(
        after error: Error,
        attemptedCount: Int,
        requestedBatchSize: Int,
        ceiling: inout Int?
    ) -> Bool {
        let constraints = CloudKitRetryConstraints(error)
        guard constraints.codes.contains(.limitExceeded) else { return false }
        let reduced = max(1, min(attemptedCount, requestedBatchSize) / 2)
        batchSize = min(batchSize, reduced)
        ceiling = min(ceiling ?? batchSize, batchSize)
        return attemptedCount > 1
            && batchSize < requestedBatchSize
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
        let operationError: Error?
        do {
            try await drainRecordUploadsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            // A final synchronous adapter observation can revoke this caller.
            try checkSynchronizationAttempt(attemptID)
            operationError = nil
        } catch {
            operationError = error
        }
        // Delivery errors belong to the caller, not to the operation just
        // completed. Never feed a throwing callback back into itself.
        try await completion(operationError)
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
            try validatePreparedMutationRecordIDs(
                prepared.map { $0.record.recordID }, in: adapter.recordZoneID
            )
            try checkSynchronizationAttempt(attemptID)

            let uncertain = prepared.filter(\.requiresAcceptanceCheck)
            if !uncertain.isEmpty, let lookup = recordStore as? any CloudKitRecordFetching {
                let fetched = try await lookup.fetchRecords(with: uncertain.map { $0.record.recordID })
                // Collect missing/malformed slots before the account await,
                // while retaining the existing fail-fast lookup import policy.
                let validatedFetched = validatedMutationResults(
                    fetched,
                    expected: uncertain.map { ($0.record.recordID, $0.record.recordType) },
                    successRecord: { $0 }
                )
                let returnedFailures = returnedMutationFailures(
                    in: validatedFetched, for: uncertain.map { $0.record.recordID }
                )
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: returnedFailures
                )
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
                        guard mutationResponseRecordMatches(
                            record, expectedID: id, expectedType: candidate.record.recordType
                        ) else {
                            throw preservingSiblingMutationFailures(
                                BigSyncRecordRebaseError.inconsistentReceipt(id.recordName),
                                failedRecordIDs: [id], otherFailures: returnedFailures
                            )
                        }
                        observations.append(record)
                    case let .failure(error):
                        let ns = error as NSError
                        if ns.domain != CKErrorDomain || ns.code != CKError.unknownItem.rawValue
                            || !mutationFailureAllowsImmediateRepair(ns) {
                            lookupFailures[id] = ns
                        }
                        // An unconstrained miss may retry the same candidate
                        // with its save fence. A miss carrying a delay, account
                        // stop or token recovery must retain that constraint.
                    }
                }
                if !observations.isEmpty {
                    let outcomes: [InboundLiveResult]
                    do {
                        for candidate in uncertain where observations.contains(where: {
                            $0.recordID == candidate.record.recordID
                        }) {
                            try retryBudget.register(.init(recordID: candidate.record.recordID,
                                generation: candidate.generation),
                                maximumPerGeneration: Self.maximumHandledRecordRetries,
                                maximumPerDrain: Self.maximumHandledRetriesPerDrain)
                        }
                        outcomes = try await adapter.saveChanges(in: observations, forceSave: true)
                        try validateInboundLiveResults(outcomes, records: observations)
                        try checkSynchronizationAttempt(attemptID)
                        if let context = activeRunContext { try checkRunContext(context) }
                        try await adapter.persistImportedChanges()
                        try checkSynchronizationAttempt(attemptID)
                        if let context = activeRunContext { try checkRunContext(context) }
                        try await adapter.didFinishImport()
                        try await revalidateMutationResultContext(
                            for: attemptID, preserving: lookupFailures
                        )
                    } catch {
                        try checkSynchronizationAttempt(attemptID)
                        if let context = activeRunContext { try checkRunContext(context) }
                        throw preservingSiblingMutationFailures(
                            error, failedRecordIDs: observations.map(\.recordID),
                            otherFailures: lookupFailures
                        )
                    }
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
                    requestedBatchSize: requestedBatchSize,
                    ceiling: &sizeLimitCeiling
                ) else { throw error }
                // Only pure, reducible limits retry here. Account stops,
                // server delays and unrelated failures retain the outer path.
                try await revalidateActiveRunContext(for: attemptID)
                continue
            }
            try Task.checkCancellation()
            let saveResults = validatedMutationResults(
                mutationResults.saveResults,
                expected: records.map { ($0.recordID, $0.recordType) },
                successRecord: { $0 }
            )
            let returnedFailures = returnedMutationFailures(
                in: saveResults, for: records.map(\.recordID)
            )
            try await revalidateMutationResultContext(
                for: attemptID, preserving: returnedFailures
            )

            var savedRecords = [CKRecord]()
            var missingRecordIDs = Set<CKRecord.ID>()
            var conflictedRecordsByID = [CKRecord.ID: CKRecord]()
            var unresolvedFailures = [CKRecord.ID: NSError]()

            for record in records {
                let retryKey = PreparedMutationRetryKey(
                    recordID: record.recordID,
                    generation: generations[record.recordID.recordName]
                )
                guard let result = saveResults[record.recordID] else {
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
                    guard code == .unknownItem || code == .serverRecordChanged,
                          mutationFailureAllowsImmediateRepair(nsError) else {
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
                do {
                    try await adapter.didUpload(
                        savedRecords: savedRecords,
                        matchingPreparedUploads: prepared
                    )
                } catch {
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    throw preservingSiblingMutationFailures(
                        error, failedRecordIDs: [], otherFailures: returnedFailures
                    )
                }
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: returnedFailures
                )
            }
            if !missingRecordIDs.isEmpty {
                do {
                    try await adapter.requeueMissingServerRecords(
                        Array(missingRecordIDs),
                        matchingPreparedUploads: prepared
                    )
                } catch {
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    // A sibling conflict selected for later import has not
                    // been repaired when this earlier requeue fails. Keep its
                    // returned evidence; only this group's IDs receive the
                    // local reconciliation error.
                    let siblingFailures = returnedFailures.filter {
                        !missingRecordIDs.contains($0.key)
                    }
                    throw preservingSiblingMutationFailures(
                        error, failedRecordIDs: Array(missingRecordIDs),
                        otherFailures: siblingFailures
                    )
                }
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: returnedFailures
                )
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
                    // Reject revoked callers before interpreting their outcome.
                    try checkSynchronizationAttempt(attemptID)
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
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: unresolvedFailures
                )
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
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: unresolvedFailures
                )
            }

            guard unresolvedFailures.isEmpty else {
                let error = partialMutationError(unresolvedFailures)
                if retrySmallerMutationBatch(
                    after: error, attemptedCount: records.count,
                    requestedBatchSize: requestedBatchSize,
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
        let operationError: Error?
        do {
            try await drainRecordDeletionsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            // A final synchronous adapter observation can revoke this caller.
            try checkSynchronizationAttempt(attemptID)
            operationError = nil
        } catch {
            operationError = error
        }
        // Delivery errors belong to the caller, not to the operation just
        // completed. Never feed a throwing callback back into itself.
        try await completion(operationError)
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
            try validatePreparedMutationRecordIDs(recordIDs, in: adapter.recordZoneID)
            try checkSynchronizationAttempt(attemptID)
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
                    requestedBatchSize: requestedBatchSize,
                    ceiling: &sizeLimitCeiling
                ) else { throw error }
                // Only pure, reducible limits retry here. Account stops,
                // server delays and unrelated failures retain the outer path.
                try await revalidateActiveRunContext(for: attemptID)
                continue
            }
            try Task.checkCancellation()
            // Generic deletion preparation carries no record type. The
            // adapter retains that model-specific check; ID and zone must
            // already match before any metadata-rebase call is dispatched.
            let deleteResults = validatedMutationResults(
                mutationResults.deleteResults,
                expected: recordIDs.map { ($0, nil) },
                successRecord: { _ in nil }
            )
            let returnedFailures = returnedMutationFailures(
                in: deleteResults, for: recordIDs
            )
            try await revalidateMutationResultContext(
                for: attemptID, preserving: returnedFailures
            )

            var acknowledged = [CKRecord.ID]()
            var acknowledgedConstraints = [CKRecord.ID: NSError]()
            var conflictedRecordsByID = [CKRecord.ID: CKRecord]()
            var unresolvedFailures = [CKRecord.ID: NSError]()
            for recordID in recordIDs {
                let retryKey = PreparedMutationRetryKey(
                    recordID: recordID,
                    generation: generations[recordID.recordName]
                )
                guard let result = deleteResults[recordID] else {
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
                        if !mutationFailureAllowsImmediateRepair(nsError) {
                            acknowledgedConstraints[recordID] = nsError
                        }
                    } else if nsError.domain == CKErrorDomain,
                              nsError.code == CKError.serverRecordChanged.rawValue,
                              mutationFailureAllowsImmediateRepair(nsError),
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
                // Selecting a conflict for repair does not resolve it. Until
                // metadata rebasing finishes, every unacknowledged result must
                // survive a sibling receipt constraint or account-check failure.
                let acknowledgedIDs = Set(acknowledged)
                let failuresBeforeRepair = returnedFailures.filter {
                    !acknowledgedIDs.contains($0.key)
                }
                do {
                    try await adapter.didDelete(
                        recordIDs: acknowledged,
                        matchingPreparedDeletions: prepared
                    )
                } catch {
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    // unknownItem is an idempotent success for deletion; do not
                    // put those IDs back into the failed-item dictionary.
                    let failure = preservingSiblingMutationFailures(
                        error, failedRecordIDs: [], otherFailures: failuresBeforeRepair
                    )
                    throw preservingAcknowledgedDeletionConstraints(
                        failure, constraints: acknowledgedConstraints
                    ) ?? failure
                }
                try checkSynchronizationAttempt(attemptID)
                if let context = activeRunContext { try checkRunContext(context) }
                // Stop before any extra account request or metadata repair.
                // These IDs were acknowledged, so they are not failed items.
                if !acknowledgedConstraints.isEmpty,
                   let failure = preservingAcknowledgedDeletionConstraints(
                    failuresBeforeRepair.isEmpty ? nil : partialMutationError(failuresBeforeRepair),
                    constraints: acknowledgedConstraints
                ) {
                    throw failure
                }
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: failuresBeforeRepair
                )
            }
            if !conflictedRecordsByID.isEmpty {
                // Rebase only server system fields before retrying the local
                // tombstone. Applying inbound model values here would either
                // overwrite the local delete or be (correctly) ignored by a
                // local-wins importer, leaving stale conflict metadata.
                do {
                    try await adapter.rebasePendingDeletionMetadata(
                        using: Array(conflictedRecordsByID.values),
                        matchingPreparedGenerations: generations
                    )
                } catch {
                    try checkSynchronizationAttempt(attemptID)
                    if let context = activeRunContext { try checkRunContext(context) }
                    throw preservingSiblingMutationFailures(
                        error, failedRecordIDs: Array(conflictedRecordsByID.keys),
                        otherFailures: unresolvedFailures
                    )
                }
                try await revalidateMutationResultContext(
                    for: attemptID, preserving: unresolvedFailures
                )
            }
            guard unresolvedFailures.isEmpty else {
                let error = partialMutationError(unresolvedFailures)
                if retrySmallerMutationBatch(
                    after: error, attemptedCount: recordIDs.count,
                    requestedBatchSize: requestedBatchSize,
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
