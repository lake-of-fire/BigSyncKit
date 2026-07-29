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
    let completion: ([CKRecordZone.ID: FetchZoneChangesOperationZoneResult]) async throws -> ()
    let desiredKeys: [String]?
    
    private let resultLock = NSLock()
    private var zoneResults = [CKRecordZone.ID: FetchZoneChangesOperationZoneResult]()
    private var higherModelVersionFound = false
    
//    let dispatchQueue = DispatchQueue(label: "fetchZoneChangesDispatchQueue")
    private var internalOperation: CKFetchRecordZoneChangesOperation?
    
    init(
        database: CloudKitDatabaseAdapter,
        zoneIDs: [CKRecordZone.ID],
        zoneChangeTokens: [CKRecordZone.ID: CKServerChangeToken],
        modelVersion: Int,
        ignoreDeviceIdentifier: String?,
        desiredKeys: [String]?,
        completion: @escaping ([CKRecordZone.ID: FetchZoneChangesOperationZoneResult]) async throws -> ()
    ) {
        self.database = database
        self.zoneIDs = zoneIDs
        self.zoneChangeTokens = zoneChangeTokens
        self.modelVersion = modelVersion
        self.ignoreDeviceIdentifier = ignoreDeviceIdentifier
        self.desiredKeys = desiredKeys
        self.completion = completion
        
        super.init()
    }
    
    override func start() {
        super.start()
        guard !isFinished else { return }
        
        resultLock.withLock {
            for zone in zoneIDs {
                zoneResults[zone] = FetchZoneChangesOperationZoneResult()
            }
        }
        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            await performFetchOperation(with: zoneIDs)
        }
    }
    
    @BigSyncBackgroundActor
    func performFetchOperation(with zones: [CKRecordZone.ID]) {
        guard !isCancelled, !isFinished else { return }
        var zoneOptions = [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneOptions]()
        
        for zoneID in zones {
            let options = CKFetchRecordZoneChangesOperation.ZoneOptions()
            options.previousServerChangeToken = zoneChangeTokens[zoneID]
            options.desiredKeys = desiredKeys
            zoneOptions[zoneID] = options
        }
        
        let operation = CKFetchRecordZoneChangesOperation(recordZoneIDs: zones, optionsByRecordZoneID: zoneOptions)
        operation.fetchAllChanges = true
        
        operation.recordChangedBlock = { @Sendable [weak self] record in
            guard let self else { return }
            let ignoredDeviceIdentifier = self.ignoreDeviceIdentifier ?? " "
            guard ignoredDeviceIdentifier != record[cloudKitSynchronizerDeviceUUIDKey] as? String else {
                return
            }
            if let version = record[cloudKitSynchronizerModelCompatibilityVersionKey] as? Int,
               self.modelVersion > 0 && version > self.modelVersion {
                self.logger?.warning("QSCloudKitSynchronizer >> Warning: Ignoring record '\(record.recordID.recordName)' because it has a higher model version (\(version)) than the one this synchronizer is configured to support (\(self.modelVersion))")
                self.resultLock.withLock {
                    self.higherModelVersionFound = true
                }
            } else {
                self.resultLock.withLock {
                    self.zoneResults[record.recordID.zoneID]?.downloadedRecords.append(record)
                }
            }
        }
        
        operation.recordWithIDWasDeletedBlock = { @Sendable [weak self] recordID, recordType in
            guard let self else { return }
            self.resultLock.withLock {
                self.zoneResults[recordID.zoneID]?.deletedRecordIDs.append(recordID)
            }
        }
        
        operation.recordZoneFetchCompletionBlock = { @Sendable [weak self]
            zoneID, serverChangeToken, clientChangeTokenData, moreComing, recordZoneError in
            guard let self else { return }
            self.resultLock.withLock {
                guard let results = self.zoneResults[zoneID] else { return }
                results.error = recordZoneError
                results.serverChangeToken = serverChangeToken
                if !self.higherModelVersionFound && moreComing {
                    results.moreComing = true
                }
            }
        }
        
        operation.fetchRecordZoneChangesCompletionBlock = { @Sendable operationError in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                self.resultLock.withLock {
                    self.internalOperation = nil
                }
                if let error = operationError,
                   (error as NSError).code != CKError.partialFailure.rawValue { // Partial errors are returned per zone
                    self.finish(error: error)
                } else if self.resultLock.withLock({ self.higherModelVersionFound }) {
                    self.finish(error: CloudKitSynchronizer.SyncError.higherModelVersionFound)
                } else if self.isCancelled {
                    self.finish(error: CloudKitSynchronizer.SyncError.cancelled)
                } else {
                    let results = self.resultLock.withLock { self.zoneResults }
                    do {
                        try await completion(results)
                        self.finish(error: nil)
                    } catch {
                        self.finish(error: error)
                    }
                }
            }
        }
        
        let shouldCancel = resultLock.withLock {
            internalOperation = operation
            return isCancelled || isFinished
        }
        if shouldCancel {
            operation.cancel()
        }
        self.database.add(operation)
    }
    
    override func cancel() {
        let operation = resultLock.withLock { internalOperation }
        operation?.cancel()
        super.cancel()
    }
}
