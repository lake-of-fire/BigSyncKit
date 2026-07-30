//
//  FetchDatabaseChangesOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 18/05/2018.
//

import Foundation
import CloudKit

class FetchDatabaseChangesOperation: CloudKitSynchronizerOperation {
    let database: CloudKitDatabaseAdapter
    let databaseToken: CKServerChangeToken?
//    let changeTokenUpdated: (CKServerChangeToken) -> ()
    let completion: (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()
    
    var changedZoneIDs = [CKRecordZone.ID]()
    var deletedZoneIDs = [CKRecordZone.ID]()
    private let resultLock = NSLock()
    private var internalOperation: CKFetchDatabaseChangesOperation?
    
    init(
        database: CloudKitDatabaseAdapter,
        databaseToken: CKServerChangeToken?,
//        changeTokenUpdated: @escaping (CKServerChangeToken) -> (),
        completion: @escaping (CKServerChangeToken?, [CKRecordZone.ID], [CKRecordZone.ID]) -> ()
    ) {
        self.databaseToken = databaseToken
        self.database = database
        self.completion = completion
//        self.changeTokenUpdated = changeTokenUpdated
        super.init()
    }
    
    override func start() {
        super.start()
        guard !isFinished else { return }
        
        let databaseChangesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseToken)
        databaseChangesOperation.fetchAllChanges = true
        
        // TODO: changeTokenUpdatedBlock
//        databaseChangesOperation.changeTokenUpdatedBlock = { token in
//        }
        
        databaseChangesOperation.recordZoneWithIDChangedBlock = { @Sendable [weak self] zoneID in
            guard let self else { return }
            self.resultLock.withLock {
                self.changedZoneIDs.append(zoneID)
            }
        }
        
        databaseChangesOperation.recordZoneWithIDWasDeletedBlock = { @Sendable [weak self] zoneID in
            guard let self else { return }
            self.resultLock.withLock {
                self.deletedZoneIDs.append(zoneID)
            }
        }
        
        databaseChangesOperation.fetchDatabaseChangesCompletionBlock = { @Sendable [weak self] serverChangeToken, moreComing, operationError in
            guard let self else { return }
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self, !self.isFinished else { return }
                if !moreComing {
                    if operationError == nil {
                        let zoneIDs = self.resultLock.withLock {
                            self.internalOperation = nil
                            return (self.changedZoneIDs, self.deletedZoneIDs)
                        }
                        self.completion(serverChangeToken, zoneIDs.0, zoneIDs.1)
                    } else {
                        self.resultLock.withLock {
                            self.internalOperation = nil
                        }
                    }
                    
                    self.finish(error: operationError)
                }
            }
        }
        
        let shouldCancel = resultLock.withLock {
            internalOperation = databaseChangesOperation
            return isCancelled || isFinished
        }
        if shouldCancel {
            databaseChangesOperation.cancel()
        }
        database.add(databaseChangesOperation)
    }
    
    override func cancel() {
        super.cancel()
        let operation = resultLock.withLock { internalOperation }
        operation?.cancel()
    }
}
