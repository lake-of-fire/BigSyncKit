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
        debugPrint("!! perform Fetch Op", zones.map { $0.zoneName} )
        var higherModelVersionFound = false
        var zoneOptions = [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneOptions]()
        
        for zoneID in zones {
            let options = CKFetchRecordZoneChangesOperation.ZoneOptions()
            options.previousServerChangeToken = zoneChangeTokens[zoneID]
            options.desiredKeys = desiredKeys
            zoneOptions[zoneID] = options
        }
        
        let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: zones, optionsByRecordZoneID: zoneOptions)
//        operation.fetchAllChanges = false
        operation.fetchAllChanges = true

        operation.recordChangedBlock = { record in
//            debugPrint("!! perform Fetch, record changed block", zones.map { $0.zoneName} )
            let ignoreDeviceIdentifier: String = self.ignoreDeviceIdentifier ?? " "
            // TODO: Also check that the one being checked already exists as a SyncedEntity, otherwise dwonload again...
            if ignoreDeviceIdentifier != record[CloudKitSynchronizer.deviceUUIDKey] as? String {
                if let version = record[CloudKitSynchronizer.modelCompatibilityVersionKey] as? Int,
                   self.modelVersion > 0 && version > self.modelVersion {
//                    debugPrint("!! perform Fetch, record changed block", zones.map { $0.zoneName }, "higher model version found!")
       
                    higherModelVersionFound = true
                } else {
//                    debugPrint("!! adding fetched downloaded record", record.recordID.recordName)
                    Task { @MainActor [weak self] in
                        self?.zoneResults[record.recordID.zoneID]?.downloadedRecords.append(record)
                        await onResult?(record, nil)
                    }
                }
//            } else {
//                debugPrint("!! perform Fetch, record changed block", zones.map { $0.zoneName }, "IGNORE!!!!:", ignoreDeviceIdentifier, "user one:", record[CloudKitSynchronizer.deviceUUIDKey], record[CloudKitSynchronizer.deviceUUIDKey] as? String)

            }
        }
        
        operation.recordWithIDWasDeletedBlock = { recordID, recordType in
//            self.dispatchQueue.async {
//                autoreleasepool {
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
                
                if !higherModelVersionFound {
                    if moreComing {
                        results.moreComing = true
                    }
                }
            }
        }
        
        operation.fetchRecordZoneChangesCompletionBlock = { operationError in
            debugPrint("!! perform Fetch, fetch record zone changes comnpletion", zones.map { $0.zoneName} )
//            self.dispatchQueue.async {
//                autoreleasepool {
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                if let error = operationError,
                   (error as NSError).code != CKError.partialFailure.rawValue { // Partial errors are returned per zone
                    self.finish(error: error)
                } else if higherModelVersionFound {
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
