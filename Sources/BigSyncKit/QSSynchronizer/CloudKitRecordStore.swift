import CloudKit
import Foundation

/// Per-record results from one non-atomic CloudKit mutation request.
///
/// BigSync consumes every item result independently so a successful record is
/// acknowledged only for the journal generation that produced it, while a
/// conflict, missing item, or transient failure remains explicit. Each save
/// value and server-conflict record must identify the requested item, including
/// its zone; save values also retain its record type. A result dictionary key
/// alone is not authority to acknowledge a differently identified value.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public struct CloudKitRecordMutationResults {
    public let saveResults: [CKRecord.ID: Result<CKRecord, Error>]
    public let deleteResults: [CKRecord.ID: Result<Void, Error>]

    public init(
        saveResults: [CKRecord.ID: Result<CKRecord, Error>],
        deleteResults: [CKRecord.ID: Result<Void, Error>]
    ) {
        self.saveResults = saveResults
        self.deleteResults = deleteResults
    }
}

/// Structured-concurrency record mutation surface used by BigSyncKit.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitRecordStore: Sendable {
    func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults
}

internal enum CloudKitPreparedRecordMutationError: Error, Equatable {
    case alreadyExecuted
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal final class CloudKitPreparedRecordMutation: @unchecked Sendable {
    let operation: CKModifyRecordsOperation
    let transportIdentity: BigSyncOutboundSubmissionTransportIdentity
    let expectedSaveIDs: Set<CKRecord.ID>
    let expectedDeleteIDs: Set<CKRecord.ID>
    private let executionLock = NSLock()
    private var hasInstalledResultHandlers = false

    init(
        operation: CKModifyRecordsOperation,
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity,
        expectedSaveIDs: Set<CKRecord.ID>,
        expectedDeleteIDs: Set<CKRecord.ID>
    ) {
        self.operation = operation
        self.transportIdentity = transportIdentity
        self.expectedSaveIDs = expectedSaveIDs
        self.expectedDeleteIDs = expectedDeleteIDs
    }

    /// The actual operation owns these callbacks, so none may retain this
    /// prepared wrapper (which in turn owns the operation). Installation is
    /// single-use: a second executor must not replace an outstanding continuation.
    func installResultHandlers(
        completion: @escaping @Sendable (Result<CloudKitRecordMutationResults, Error>) -> Void
    ) throws {
        executionLock.lock()
        defer { executionLock.unlock() }
        guard !hasInstalledResultHandlers else {
            throw CloudKitPreparedRecordMutationError.alreadyExecuted
        }
        hasInstalledResultHandlers = true
        let collector = CloudKitMutationResultCollector(
            expectedSaveIDs: expectedSaveIDs,
            expectedDeleteIDs: expectedDeleteIDs,
            completion: completion
        )
        operation.perRecordSaveBlock = { recordID, result in
            collector.recordSave(recordID, result)
        }
        operation.perRecordDeleteBlock = { recordID, result in
            collector.recordDelete(recordID, result)
        }
        operation.modifyRecordsResultBlock = { result in
            collector.finish(result)
        }
    }
}

/// Internal optional capability. Custom/test stores keep the existing public
/// protocol and are never forced to manufacture CloudKit operation identity.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal protocol CloudKitRecoverableRecordStore: CloudKitRecordStore {
    func prepareRecoverableModifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) -> CloudKitPreparedRecordMutation

    func executeRecoverableModifyRecords(
        _ prepared: CloudKitPreparedRecordMutation
    ) async throws -> CloudKitRecordMutationResults
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
private final class CloudKitMutationResultCollector: @unchecked Sendable {
    private let lock = NSLock()
    private let expectedSaveIDs: Set<CKRecord.ID>
    private let expectedDeleteIDs: Set<CKRecord.ID>
    private var completion: (@Sendable (Result<CloudKitRecordMutationResults, Error>) -> Void)?
    private var saves = [CKRecord.ID: Result<CKRecord, Error>]()
    private var deletes = [CKRecord.ID: Result<Void, Error>]()

    init(
        expectedSaveIDs: Set<CKRecord.ID>,
        expectedDeleteIDs: Set<CKRecord.ID>,
        completion: @escaping @Sendable (Result<CloudKitRecordMutationResults, Error>) -> Void
    ) {
        self.expectedSaveIDs = expectedSaveIDs
        self.expectedDeleteIDs = expectedDeleteIDs
        self.completion = completion
    }

    func recordSave(_ recordID: CKRecord.ID, _ result: Result<CKRecord, Error>) {
        lock.lock()
        defer { lock.unlock() }
        guard completion != nil else { return }
        saves[recordID] = result
    }

    func recordDelete(_ recordID: CKRecord.ID, _ result: Result<Void, Error>) {
        lock.lock()
        defer { lock.unlock() }
        guard completion != nil else { return }
        deletes[recordID] = result
    }

    func finish(_ result: Result<Void, Error>) {
        lock.lock()
        guard let completion else { lock.unlock(); return }
        self.completion = nil
        let collected = CloudKitRecordMutationResults(saveResults: saves, deleteResults: deletes)
        let hasEveryPerItemResult = Set(saves.keys) == expectedSaveIDs
            && Set(deletes.keys) == expectedDeleteIDs
        let hasAnyPerItemResult = !saves.isEmpty || !deletes.isEmpty
        // A retained CKOperation must not retain the caller's continuation or
        // completed result payloads after terminal delivery.
        saves.removeAll()
        deletes.removeAll()
        lock.unlock()

        switch result {
        case .success:
            completion(.success(collected))
        case .failure(let error):
            // Never discard an observed item outcome in favor of an operation
            // error. In particular, a partial callback set plus limitExceeded
            // must not look like a definitive whole-operation rejection.
            // Missing items remain absent: the existing settlement validator
            // retains the durable marker, while exact known successes can be
            // generation-matched locally. No missing result is synthesized.
            if hasEveryPerItemResult || hasAnyPerItemResult {
                completion(.success(collected))
            } else {
                completion(.failure(error))
            }
        }
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitRecordStore, CloudKitRecoverableRecordStore {
    private func recordMutationConfiguration(longLived: Bool = false) -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        configuration.timeoutIntervalForRequest = 60
        // Asset-backed uploads observed in the Development sandbox can make
        // steady progress for well over a minute. Bound the whole resource
        // without turning a healthy transfer into a false timeout.
        configuration.timeoutIntervalForResource = 600
        configuration.isLongLived = longLived
        if let container {
            configuration.container = container
        }
        return configuration
    }

    public func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        try await database.configuredWith(
            configuration: recordMutationConfiguration()
        ) { database in
            let results = try await database.modifyRecords(
                saving: records,
                deleting: recordIDs,
                savePolicy: savePolicy,
                atomically: atomically
            )
            return CloudKitRecordMutationResults(
                saveResults: results.saveResults,
                deleteResults: results.deleteResults
            )
        }
    }

    internal func prepareRecoverableModifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) -> CloudKitPreparedRecordMutation {
        let operation = CKModifyRecordsOperation(
            recordsToSave: records,
            recordIDsToDelete: recordIDs
        )
        let clientChangeTokenData = Data(UUID().uuidString.utf8)
        operation.clientChangeTokenData = clientChangeTokenData
        operation.savePolicy = savePolicy
        operation.isAtomic = atomically
        operation.configuration = recordMutationConfiguration(longLived: true)
        let identity = BigSyncOutboundSubmissionTransportIdentity(
            clientChangeTokenData: clientChangeTokenData,
            longLivedOperationID: operation.operationID
        )
        return CloudKitPreparedRecordMutation(
            operation: operation,
            transportIdentity: identity,
            expectedSaveIDs: Set(records.map(\.recordID)),
            expectedDeleteIDs: Set(recordIDs)
        )
    }

    internal func executeRecoverableModifyRecords(
        _ prepared: CloudKitPreparedRecordMutation
    ) async throws -> CloudKitRecordMutationResults {
        return try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<CloudKitRecordMutationResults, Error>) in
            do {
                try prepared.installResultHandlers { result in
                    continuation.resume(with: result)
                }
                database.add(prepared.operation)
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }
}
