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
    weak var internalOperation: CKFetchDatabaseChangesOperation?
    
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
        logStart()
        
//        super.start()
        
        let databaseChangesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseToken)
        databaseChangesOperation.fetchAllChanges = true
        
        // TODO: changeTokenUpdatedBlock
//        databaseChangesOperation.changeTokenUpdatedBlock = { token in
//        }
        
        databaseChangesOperation.recordZoneWithIDChangedBlock = { zoneID in
            self.changedZoneIDs.append(zoneID)
        }
        
        databaseChangesOperation.recordZoneWithIDWasDeletedBlock = { zoneID in
            self.deletedZoneIDs.append(zoneID)
        }
        
        databaseChangesOperation.fetchDatabaseChangesCompletionBlock = { serverChangeToken, moreComing, operationError in
            if !moreComing {
                if operationError == nil {
                    self.completion(serverChangeToken, self.changedZoneIDs, self.deletedZoneIDs)
                }

                self.finish(error: operationError)
            }
        }
        
        internalOperation = databaseChangesOperation
        database.add(databaseChangesOperation)
    }
    
    override func cancel() {
        internalOperation?.cancel()
        super.cancel()
    }
}
