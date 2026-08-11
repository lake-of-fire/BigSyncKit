import CloudKit
import Foundation

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension CloudKitSynchronizer {
    private static let maximumHandledRecordRetries = 5

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
        var handledRetryCounts = [CKRecord.ID: Int]()
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
                guard let result = mutationResults.saveResults[record.recordID] else {
                    unresolvedFailures[record.recordID] = CocoaError(
                        .coderValueNotFound
                    ) as NSError
                    continue
                }
                switch result {
                case .success(let savedRecord):
                    handledRetryCounts.removeValue(forKey: record.recordID)
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
                    let retryCount = handledRetryCounts[record.recordID, default: 0] + 1
                    guard retryCount <= Self.maximumHandledRecordRetries else {
                        unresolvedFailures[record.recordID] = nsError
                        continue
                    }
                    handledRetryCounts[record.recordID] = retryCount
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
                try await adapter.saveChanges(
                    in: Array(conflictedRecordsByID.values),
                    forceSave: true
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
            var unresolvedFailures = [CKRecord.ID: NSError]()
            for recordID in recordIDs {
                guard let result = mutationResults.deleteResults[recordID] else {
                    unresolvedFailures[recordID] = CocoaError(
                        .coderValueNotFound
                    ) as NSError
                    continue
                }
                switch result {
                case .success:
                    acknowledged.append(recordID)
                case .failure(let error):
                    let nsError = error as NSError
                    if nsError.domain == CKErrorDomain,
                       nsError.code == CKError.unknownItem.rawValue {
                        acknowledged.append(recordID)
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
            guard unresolvedFailures.isEmpty else {
                if unresolvedFailures.values.contains(where: {
                    $0.domain == CKErrorDomain
                        && $0.code == CKError.limitExceeded.rawValue
                }) {
                    reduceBatchSize()
                }
                throw partialMutationError(unresolvedFailures)
            }
            if recordIDs.count >= requestedBatchSize {
                increaseBatchSize()
            }
            guard recordIDs.count >= requestedBatchSize else { return }
            await Task.yield()
        }
    }
}
