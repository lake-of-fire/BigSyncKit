//
//  ModifyRecordsOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 06/09/2020.
//

import Foundation
import CloudKit

class ModifyRecordsOperation: CloudKitSynchronizerOperation {
    let database: CloudKitDatabaseAdapter
    let records: [CKRecord]?
    let recordIDsToDelete: [CKRecord.ID]?
    
    let completion: ([CKRecord]?, [CKRecord.ID]?, [CKRecord], Set<CKRecord.ID>, Error?) -> ()
    
    init(
        database: CloudKitDatabaseAdapter,
        records: [CKRecord]?,
        recordIDsToDelete: [CKRecord.ID]?,
        completion: @escaping ([CKRecord]?, [CKRecord.ID]?, [CKRecord], Set<CKRecord.ID>, Error?) -> ()
    ) {
        self.database = database
        self.records = records
        self.recordIDsToDelete = recordIDsToDelete
        self.completion = completion
    }
    
    private var conflictedRecords = [CKRecord]()
    private var conflictedRecordIDs = Set<CKRecord.ID>()
    private var recordIDsMissingOnServer = Set<CKRecord.ID>()
    private let resultLock = NSLock()
    private var completionDelivered = false

//    let dispatchQueue = DispatchQueue(label: "modifyRecordsDispatchQueue")
    private var internalOperation: CKModifyRecordsOperation?
        
    override func start() {
        super.start()
        guard !isFinished else { return }
        let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: recordIDsToDelete)
        
        operation.perRecordCompletionBlock = { @Sendable [weak self] record, error in
            guard let self else { return }
            self.resultLock.withLock {
                self.processErrorWithoutLock(error, recordID: record.recordID)
            }
        }
        
        operation.modifyRecordsCompletionBlock = { @Sendable [weak self] saved, deleted, operationError in
            guard let self else { return }
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                let results = self.resultLock.withLock { () -> ([CKRecord], Set<CKRecord.ID>) in
                    self.internalOperation = nil
                    if let error = operationError as? CKError {
                        self.processCKErrorWithoutLock(error)
                    }
                    return (self.conflictedRecords, self.recordIDsMissingOnServer)
                }
                self.deliverCompletion(
                    saved: saved,
                    deleted: deleted,
                    conflicted: results.0,
                    missing: results.1,
                    error: operationError
                )
                self.finish(error: nil)
            }
        }
        
        let shouldCancel = resultLock.withLock {
            internalOperation = operation
            return isCancelled || isFinished
        }
        if shouldCancel {
            operation.cancel()
        }
        database.add(operation)
    }
    
    /// Handles errors from both perRecordCompletionBlock and modifyRecordsCompletionBlock
    private func processErrorWithoutLock(_ error: Error?, recordID: CKRecord.ID) {
        guard let error = error as? CKError else { return }
        
        switch error.code {
        case .serverRecordChanged:
            if let serverRecord = error.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord {
//                debugPrint("# added conflicted record", serverRecord.recordID.recordName)
                let (inserted, _) = conflictedRecordIDs.insert(serverRecord.recordID)
                if inserted {
                    conflictedRecords.append(serverRecord)
                }
            }
        case .unknownItem:
//            debugPrint("# Record not found in CloudKit (Unknown Item)", recordID.recordName)
            recordIDsMissingOnServer.insert(recordID)
        default:
            break
        }
    }
    
    /// Processes CKError for batch errors (partial failures)
    private func processCKErrorWithoutLock(_ error: CKError) {
        if error.code == .partialFailure,
           let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError] {
            for (recordID, nsError) in errorsByItemID {
                processErrorWithoutLock(nsError, recordID: recordID)
            }
        }
    }

    private func deliverCompletion(
        saved: [CKRecord]?,
        deleted: [CKRecord.ID]?,
        conflicted: [CKRecord],
        missing: Set<CKRecord.ID>,
        error: Error?
    ) {
        let shouldDeliver = resultLock.withLock {
            guard !completionDelivered else { return false }
            completionDelivered = true
            return true
        }
        guard shouldDeliver else { return }
        completion(saved, deleted, conflicted, missing, error)
    }
    
    override func cancel() {
        let operation = resultLock.withLock { internalOperation }
        operation?.cancel()
        deliverCompletion(
            saved: nil,
            deleted: nil,
            conflicted: [],
            missing: [],
            error: CancellationError()
        )
        super.cancel()
    }
}
