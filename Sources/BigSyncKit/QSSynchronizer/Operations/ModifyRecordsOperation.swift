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

//    let dispatchQueue = DispatchQueue(label: "modifyRecordsDispatchQueue")
    weak var internalOperation: CKModifyRecordsOperation?
        
    override func start() {
        let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: recordIDsToDelete)
        
        operation.perRecordCompletionBlock = { record, error in
            self.processError(error, recordID: record.recordID)
        }
        
        operation.modifyRecordsCompletionBlock = { saved, deleted, operationError in
            if let error = operationError as? CKError {
                self.processCKError(error)
            }
            self.completion(saved, deleted, self.conflictedRecords, self.recordIDsMissingOnServer, operationError)
        }
        
        internalOperation = operation
        database.add(operation)
    }
    
    /// Handles errors from both perRecordCompletionBlock and modifyRecordsCompletionBlock
    private func processError(_ error: Error?, recordID: CKRecord.ID) {
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
    private func processCKError(_ error: CKError) {
        if error.code == .partialFailure,
           let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError] {
            for (recordID, nsError) in errorsByItemID {
                processError(nsError, recordID: recordID)
            }
        } else {
            print(error)
        }
    }
    
    override func cancel() {
        internalOperation?.cancel()
        super.cancel()
    }
}
