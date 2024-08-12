//
//  QSFetchZoneChangesOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 18/05/2018.
//

import Foundation
import CloudKit

class FetchZoneChangesOperationZoneResult: NSObject {
    var downloadedRecords = [CKRecord]()
    var deletedRecordIDs = [CKRecord.ID]()
    var serverChangeToken: CKServerChangeToken?
    var error: Error?
    var moreComing: Bool = false
}

class FetchZoneChangesOperation: CloudKitSynchronizerOperation {
    let database: CloudKitDatabaseAdapter
    let zoneIDs: [CKRecordZone.ID]
    var zoneChangeTokens: [CKRecordZone.ID: CKServerChangeToken]
    let modelVersion: Int
    let ignoreDeviceIdentifier: String?
    let onResult: ((CKRecord?, CKRecord.ID?) async -> ())?
    let completion: ([CKRecordZone.ID: FetchZoneChangesOperationZoneResult]) async -> ()
    let desiredKeys: [String]?
    
    var zoneResults = [CKRecordZone.ID: FetchZoneChangesOperationZoneResult]()
    
//    let dispatchQueue = DispatchQueue(label: "fetchZoneChangesDispatchQueue")
    weak var internalOperation: CKFetchRecordZoneChangesOperation?
    
    init(
        database: CloudKitDatabaseAdapter,
        zoneIDs: [CKRecordZone.ID],
        zoneChangeTokens: [CKRecordZone.ID: CKServerChangeToken],
        modelVersion: Int,
        ignoreDeviceIdentifier: String?,
        desiredKeys: [String]?,
        onResult: ((CKRecord?, CKRecord.ID?) async -> ())? = nil,
        completion: @escaping ([CKRecordZone.ID: FetchZoneChangesOperationZoneResult]) async -> ()
    ) {
        
        self.database = database
        self.zoneIDs = zoneIDs
        self.zoneChangeTokens = zoneChangeTokens
        self.modelVersion = modelVersion
        self.ignoreDeviceIdentifier = ignoreDeviceIdentifier
        self.desiredKeys = desiredKeys
        self.onResult = onResult
        self.completion = completion
        
        super.init()
    }
    
    override func start() {
        for zone in zoneIDs {
            zoneResults[zone] = FetchZoneChangesOperationZoneResult()
        }
        Task.detached { [weak self] in
            guard let self else { return }
            await performFetchOperation(with: zoneIDs)
        }
    }
    
    @BigSyncBackgroundActor
    func performFetchOperation(with zones: [CKRecordZone.ID]) {
        actor ModelVersionChecker {
            var higherModelVersionFound = false
            
            func setHigherModelVersionFound() {
                higherModelVersionFound = true
            }
            
            func isHigherModelVersionFound() -> Bool {
                return higherModelVersionFound
            }
        }
        
        let versionChecker = ModelVersionChecker()
        var zoneOptions = [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneOptions]()
        
        for zoneID in zones {
            let options = CKFetchRecordZoneChangesOperation.ZoneOptions()
            options.previousServerChangeToken = zoneChangeTokens[zoneID]
            options.desiredKeys = desiredKeys
            zoneOptions[zoneID] = options
        }
        
        let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: zones, optionsByRecordZoneID: zoneOptions)
        operation.fetchAllChanges = true
        
        operation.recordChangedBlock = { record in
            let ignoreDeviceIdentifier: String = self.ignoreDeviceIdentifier ?? " "
            
            if ignoreDeviceIdentifier != record[CloudKitSynchronizer.deviceUUIDKey] as? String {
                if let version = record[CloudKitSynchronizer.modelCompatibilityVersionKey] as? Int,
                   self.modelVersion > 0 && version > self.modelVersion {
                    Task {
                        await versionChecker.setHigherModelVersionFound()
                    }
                } else {
                    Task { @MainActor [weak self] in
                        self?.zoneResults[record.recordID.zoneID]?.downloadedRecords.append(record)
                        await onResult?(record, nil)
                    }
                }
            }
        }
        
        operation.recordWithIDWasDeletedBlock = { recordID, recordType in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                zoneResults[recordID.zoneID]?.deletedRecordIDs.append(recordID)
                await onResult?(nil, recordID)
            }
        }
        
        operation.recordZoneFetchCompletionBlock = {
            zoneID, serverChangeToken, clientChangeTokenData, moreComing, recordZoneError in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                let results = zoneResults[zoneID]!
                
                results.error = recordZoneError
                results.serverChangeToken = serverChangeToken
                
                if !(await versionChecker.isHigherModelVersionFound()) {
                    if moreComing {
                        results.moreComing = true
                    }
                }
            }
        }
        
        operation.fetchRecordZoneChangesCompletionBlock = { operationError in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                if let error = operationError,
                   (error as NSError).code != CKError.partialFailure.rawValue { // Partial errors are returned per zone
                    self.finish(error: error)
                } else if await versionChecker.isHigherModelVersionFound() {
                    self.finish(error: CloudKitSynchronizer.SyncError.higherModelVersionFound)
                } else if self.isCancelled {
                    self.finish(error: CloudKitSynchronizer.SyncError.cancelled)
                } else {
                    await completion(self.zoneResults)
                    self.finish(error: nil)
                }
            }
        }
        
        internalOperation = operation
        self.database.add(operation)
    }
    
    override func cancel() {
        internalOperation?.cancel()
        super.cancel()
    }
}
