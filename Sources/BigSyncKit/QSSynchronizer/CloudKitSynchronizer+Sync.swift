//
//  CloudKitSynchronizer+Sync.swift
//  Pods
//
//  Created by Manuel Entrena on 17/04/2019.
//

import Foundation
import CloudKit
import AsyncAlgorithms

fileprivate struct ChangeRequest {
    let downloadedRecord: CKRecord?
    let deletedRecordID: CKRecord.ID?
    let adapter: ModelAdapter
}

fileprivate class ChangeRequestProcessor {
    @BigSyncBackgroundActor
    private var changeRequests = [ChangeRequest]()
    private let debounceInterval: UInt64 = 2_000_000_000 // 2 seconds in nanoseconds
    private var lastExecutionTime: UInt64 = 0
    private var debounceTask: Task<Void, Never>?
    private var localErrors: [Error] = []
    
    @BigSyncBackgroundActor
    fileprivate func addChangeRequest(_ request: ChangeRequest) {
        changeRequests.append(request)
        let currentTime = DispatchTime.now().uptimeNanoseconds
        
        if lastExecutionTime == 0 || currentTime >= lastExecutionTime + debounceInterval {            debounceTask?.cancel()
            Task {
                await processChangeRequests()
            }
        } else {
            debounceTask?.cancel()
            debounceTask = Task { @BigSyncBackgroundActor in
                let timeSinceLastExecution = currentTime >= lastExecutionTime ? currentTime - lastExecutionTime : 0
                let remainingTime = debounceInterval > timeSinceLastExecution ? debounceInterval - timeSinceLastExecution : 0
                try? await Task.sleep(nanoseconds: remainingTime)
                await processChangeRequests()
            }
        }
    }
    
    @BigSyncBackgroundActor
    private func processChangeRequests() async {
        lastExecutionTime = DispatchTime.now().uptimeNanoseconds
        let batch = changeRequests
        guard !batch.isEmpty else { return }
        changeRequests.removeAll()

        do {
            let downloadedRecords = batch.compactMap { $0.downloadedRecord }
            if !downloadedRecords.isEmpty {
                try await batch.first?.adapter.saveChanges(in: downloadedRecords)
            }
            
            let deletedRecordIDs = batch.compactMap { $0.deletedRecordID }
            if !deletedRecordIDs.isEmpty {
                try await batch.first?.adapter.deleteRecords(with: deletedRecordIDs)
            }
        } catch {
            localErrors.append(error)
        }
    }
    
    func getErrors() -> [Error] {
        return localErrors
    }
    
    func finishProcessing() async {
        debounceTask?.cancel()
        await processChangeRequests()
    }
}


fileprivate func isZoneNotFoundOrDeletedError(_ error: Error?) -> Bool {
    if let error = error {
        let nserror = error as NSError
        return nserror.code == CKError.zoneNotFound.rawValue || nserror.code == CKError.userDeletedZone.rawValue
    } else {
        return false
    }
}

extension CloudKitSynchronizer {
    @MainActor
    func performSynchronization() async {
        self.postNotification(.SynchronizerWillSynchronize)
        self.serverChangeToken = self.storedDatabaseToken
        self.uploadRetries = 0
        self.didNotifyUpload = Set<CKRecordZone.ID>()
        
        await fetchChanges()
    }
    
    @MainActor
    func finishSynchronization(error: Error?) async {
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            await adapter.didFinishImport(with: error)
        }
        
        syncing = false
        cancelSync = false
        completion?(error)
        completion = nil
        
        if let error = error {
            self.postNotification(.SynchronizerDidFailToSynchronize, userInfo: [CloudKitSynchronizer.errorKey: error])
            self.delegate?.synchronizerDidfailToSync(self, error: error)
            
            if let error = error as? CKError {
                switch error.code {
                case .changeTokenExpired:
                    // See: https://github.com/mentrena/SyncKit/issues/92#issuecomment-541362433
                    self.resetDatabaseToken()
                    for adapter in self.modelAdapters {
                        await adapter.deleteChangeTracking()
                        self.removeModelAdapter(adapter)
                    }
                    await fetchChanges()
                default:
                    break
                }
            }
        } else {
            postNotification(.SynchronizerDidSynchronize)
            delegate?.synchronizerDidSync(self)
        }
        
        //            debugPrint("QSCloudKitSynchronizer >> Finishing synchronization")
    }
}

// MARK: - Utilities

extension CloudKitSynchronizer {
    func postNotification(_ notification: Notification.Name, object: Any? = nil, userInfo: [AnyHashable: Any]? = nil) {
        let object = object ?? self
        Task(priority: .background) { @MainActor in
            NotificationCenter.default.post(name: notification, object: object, userInfo: userInfo)
        }
    }
    
    func runOperation(_ operation: CloudKitSynchronizerOperation) {
        operation.errorHandler = { [weak self] operation, error in
            Task(priority: .background) { [weak self] in
                await self?.finishSynchronization(error: error)
            }
        }
        currentOperation = operation
        operationQueue.addOperation(operation)
    }
    
    @MainActor
    func notifyProviderForDeletedZoneIDs(_ zoneIDs: [CKRecordZone.ID]) async {
        for zoneID in zoneIDs {
            await self.adapterProvider.cloudKitSynchronizer(self, zoneWasDeletedWithZoneID: zoneID)
            self.delegate?.synchronizer(self, zoneIDWasDeleted: zoneID)
        }
    }
    
    func loadTokens(for zoneIDs: [CKRecordZone.ID], loadAdapters: Bool) -> [CKRecordZone.ID] {
        var filteredZoneIDs = [CKRecordZone.ID]()
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
        
        for zoneID in zoneIDs {
            var modelAdapter = modelAdapterDictionary[zoneID]
            if modelAdapter == nil && loadAdapters {
                if let newModelAdapter = adapterProvider.cloudKitSynchronizer(self, modelAdapterForRecordZoneID: zoneID) {
                    modelAdapter = newModelAdapter
                    modelAdapterDictionary[zoneID] = newModelAdapter
                    delegate?.synchronizer(self, didAddAdapter: newModelAdapter, forRecordZoneID: zoneID)
                }
            }
            
            if let adapter = modelAdapter {
                filteredZoneIDs.append(zoneID)
                activeZoneTokens[zoneID] = adapter.serverChangeToken
            }
        }
        
        return filteredZoneIDs
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
    }
    
    func shouldRetryUpload(for error: NSError) -> Bool {
        if isServerRecordChangedError(error) || isLimitExceededError(error) {
            return uploadRetries < 2
        } else {
            return false
        }
    }
    
    func isServerRecordChangedError(_ error: NSError) -> Bool {
        if error.code == CKError.partialFailure.rawValue,
            let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError],
            errorsByItemID.values.contains(where: { (error) -> Bool in
                return error.code == CKError.serverRecordChanged.rawValue
            }) {
            
            return true
        }
        
        return error.code == CKError.serverRecordChanged.rawValue
    }
    
    func isLimitExceededError(_ error: NSError) -> Bool {
        if error.code == CKError.partialFailure.rawValue,
            let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError],
            errorsByItemID.values.contains(where: { (error) -> Bool in
                return error.code == CKError.limitExceeded.rawValue
            }) {
            
            return true
        }
        
        return error.code == CKError.limitExceeded.rawValue
    }
    
    func sequential<T>(objects: [T], closure: @escaping (T, @escaping (Error?) async -> ()) async -> (), final: @escaping (Error?) async -> ()) async {
        guard let first = objects.first else {
            await final(nil)
            return
        }
        
        do {
            try Task.checkCancellation()
        } catch {
            await final(error)
            return
        }
        
        await closure(first) { [weak self] error in
            guard error == nil else {
                await final(error)
                return
            }
            
            // For lowering CPU priority gently
            await Task.yield()
            try? await Task.sleep(nanoseconds: 20_000)
            
            var remaining = objects
            remaining.removeFirst()
            await self?.sequential(objects: remaining, closure: closure, final: final)
        }
    }
    
    func needsZoneSetup(adapter: ModelAdapter) -> Bool {
        return adapter.serverChangeToken == nil
    }
}

//MARK: - Fetch changes

extension CloudKitSynchronizer {
    @MainActor
    func fetchChanges() async {
        guard cancelSync == false else {
            await finishSynchronization(error: SyncError.cancelled)
            return
        }
        
        postNotification(.SynchronizerWillFetchChanges)
        
        debugPrint("!! fetch DB changes")
        await fetchDatabaseChanges() { [weak self] token, error in
            debugPrint("!! fetch DB changes: FINISHED", token, error)
            guard let self = self else { return }
            guard error == nil else {
                await finishSynchronization(error: error)
                return
            }
            
            serverChangeToken = token
            storedDatabaseToken = token
            if syncMode == .sync {
                await uploadChanges()
            } else {
                await finishSynchronization(error: nil)
            }
        }
    }
    
    @BigSyncBackgroundActor
    func fetchDatabaseChanges(completion: @escaping (CKServerChangeToken?, Error?) async -> ()) async {
        let operation = await FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (token, changedZoneIDs, deletedZoneIDs) in
//            debugPrint("!! inside fetchDatabaseChanges callback")
            Task.detached(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                await notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                
                let zoneIDsToFetch = await loadTokens(for: changedZoneIDs, loadAdapters: true)
                
                guard zoneIDsToFetch.count > 0 else {
                    await self.resetActiveTokens()
                    await completion(token, nil)
                    return
                }
                
                await Task(priority: .background) { @MainActor [weak self] in
                    guard let self = self else { return }
                    zoneIDsToFetch.forEach {
                        self.delegate?.synchronizerWillFetchChanges(self, in: $0)
                    }
                    
                    debugPrint("!! zoneIDs to fetch", zoneIDsToFetch.map { $0.zoneName })
                    fetchZoneChanges(zoneIDsToFetch) { [weak self] error in
                        guard let self = self else { return }
                        guard error == nil else {
                            await finishSynchronization(error: error)
                            return
                        }
                        
                        await mergeChanges() { error in
                            await completion(token, error)
                        }
                    }
                }.value
            }
        }
        await runOperation(operation)
    }
    
    @MainActor
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID], completion: @escaping (Error?) async -> ()) {
        debugPrint("!! fetchZoneChanges for zones", zoneIDs.map { $0.zoneName })
        
        let changeRequestProcessor = ChangeRequestProcessor()
        var localErrors: [Error] = []
        
        let operation = FetchZoneChangesOperation(database: database, zoneIDs: zoneIDs, zoneChangeTokens: activeZoneTokens, modelVersion: compatibilityVersion, ignoreDeviceIdentifier: deviceIdentifier, desiredKeys: nil) { [weak self] (downloadedRecord, deletedRecordID) in
            guard let self else { return }
            guard let zoneID = downloadedRecord?.recordID.zoneID ?? deletedRecordID?.zoneID else {
                debugPrint("Unexpectedly found no downloaded record or deleted record ID")
                return
            }
            
            let adapter = await modelAdapterDictionary[zoneID]
            if let adapter {
                let changeRequest = ChangeRequest(downloadedRecord: downloadedRecord, deletedRecordID: deletedRecordID, adapter: adapter)
                await changeRequestProcessor.addChangeRequest(changeRequest)
            }
        } completion: { [weak self] zoneResults in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                let error: Error? = try? zoneResults.lazy.compactMap({ [weak self] (zoneID, zoneResult) -> Error? in
                    guard let self = self else { return nil }
                    if let error = zoneResult.error {
                        if isZoneNotFoundOrDeletedError(error) {
                            Task(priority: .background) { [weak self] in
                                guard let self else { return }
                                await notifyProviderForDeletedZoneIDs([zoneID])
                            }
                        } else {
                            return error
                        }
                    }
                    return nil
                }).first
                if let error {
                    debugPrint("!! zoneResults resultError", identifier, error)
                }
                for (zoneID, zoneResult) in zoneResults {
                    if !zoneResult.downloadedRecords.isEmpty {
                        debugPrint("QSCloudKitSynchronizer >> Downloaded \(zoneResult.downloadedRecords.count) changed records >> from zone \(zoneID.zoneName)")
                    }
                    if !zoneResult.deletedRecordIDs.isEmpty {
                        debugPrint("QSCloudKitSynchronizer >> Downloaded \(zoneResult.deletedRecordIDs.count) deleted record IDs >> from zone \(zoneID.zoneName)")
                    }
                    do {
                        try await Task(priority: .background) { @MainActor [weak self] in
                            guard let self = self else { return }
                            activeZoneTokens[zoneID] = zoneResult.serverChangeToken
                        }
                    } catch {
                        await completion(error)
                        return
                    }
                }
                
                // Process any remaining change requests
                await changeRequestProcessor.finishProcessing()
                
                // Collect any errors from the processor
                localErrors.append(contentsOf: changeRequestProcessor.getErrors())
                
                if let firstError = localErrors.first {
                    await completion(firstError)
                } else {
                    await completion(error)
                }
            }
        }
        runOperation(operation)
    }
    
    @MainActor
    func mergeChanges(completion: @escaping (Error?) async -> ()) async {
        guard cancelSync == false else {
            await finishSynchronization(error: SyncError.cancelled)
            return
        }
        
        var adapterSet = [ModelAdapter]()
        activeZoneTokens.keys.forEach {
            if let adapter = self.modelAdapterDictionary[$0] {
                adapterSet.append(adapter)
            }
        }
        
        await sequential(objects: adapterSet, closure: mergeChangesIntoAdapter, final: completion)
    }
    
    @MainActor
    func mergeChangesIntoAdapter(_ adapter: ModelAdapter, completion: @escaping (Error?) async -> ()) async {
        await adapter.persistImportedChanges { @MainActor [weak self] error in
            //            self.dispatchQueue.async {
            //                autoreleasepool {
            guard let self = self else { return }
            guard error == nil else {
                await completion(error)
                return
            }
            if let token = activeZoneToken(zoneID: adapter.recordZoneID) {
                await adapter.saveToken(token)
            }
            await completion(nil)
        }
    }
}

// MARK: - Upload changes

extension CloudKitSynchronizer {
    @MainActor
    func uploadChanges() async {
        guard !cancelSync else {
            await finishSynchronization(error: SyncError.cancelled)
            return
        }
        
        postNotification(.SynchronizerWillUploadChanges)
        
        await uploadChanges() { [weak self] (error) in
            guard let self = self else { return }
            if let error = error as? NSError {
#warning("FIXME: handle zone not found...")
                if shouldRetryUpload(for: error) {
                    uploadRetries += 1
                        await fetchChanges()
                } else {
                    await finishSynchronization(error: error)
                }
            } else {
                increaseBatchSize()
                updateTokens()
            }
        }
    }
    
    @MainActor
    func uploadChanges(completion: @escaping (Error?) async -> ()) async {
        await sequential(objects: modelAdapters, closure: setupZoneAndUploadRecords) { [weak self] (error) in
            guard error == nil else { await completion(error); return }
            guard let self = self else { return }
            
            await sequential(objects: modelAdapters, closure: uploadDeletions, final: completion)
        }
    }
    
    @MainActor
    func setupZoneAndUploadRecords(adapter: ModelAdapter, completion: @escaping (Error?) async -> ()) async {
        await setupRecordZoneIfNeeded(adapter: adapter) { [weak self] (error) in
            debugPrint("!! zone needed setup", adapter.recordZoneID.zoneName)
            guard let self = self, error == nil else {
                await completion(error)
                return
            }
            await uploadRecords(adapter: adapter, completion: { (error) in
                await completion(error)
            })
        }
    }
    
    @MainActor
    func setupRecordZoneIfNeeded(adapter: ModelAdapter, completion: @escaping (Error?) async -> ()) async {
        guard needsZoneSetup(adapter: adapter) else {
            await completion(nil)
            return
        }
        
        setupRecordZoneID(adapter.recordZoneID, completion: completion)
    }
    
    @MainActor
    func setupRecordZoneID(_ zoneID: CKRecordZone.ID, completion: @escaping (Error?) async -> ()) {
        database.fetch(withRecordZoneID: zoneID) { [weak self] (zone, error) in
            guard let self = self else { return }
            if isZoneNotFoundOrDeletedError(error) {
                let newZone = CKRecordZone(zoneID: zoneID)
                database.save(zone: newZone, completionHandler: { (zone, error) in
                    if error == nil && zone != nil {
                        debugPrint("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                    }
                    Task(priority: .background) {
                        await completion(error)
                    }
                })
            } else {
                Task(priority: .background) {
                    await completion(error)
                }
            }
        }
    }
    
    func uploadRecords(adapter: ModelAdapter, completion: @escaping (Error?) async -> ()) async {
        let records = adapter.recordsToUpload(limit: batchSize)
        let recordCount = records.count
        let requestedBatchSize = batchSize
        guard recordCount > 0 else { await completion(nil); return }
        
        if !didNotifyUpload.contains(adapter.recordZoneID) {
            didNotifyUpload.insert(adapter.recordZoneID)
            delegate?.synchronizerWillUploadChanges(self, to: adapter.recordZoneID)
        }
        
        //Add metadata: device UUID and model version
        addMetadata(to: records)
        
        let modifyRecordsOperation = ModifyRecordsOperation(database: database,
                                               records: records,
                                               recordIDsToDelete: nil)
        { [weak self] (savedRecords, deleted, conflicted, operationError) in
            Task(priority: .background) { [weak self] in
                guard let self = self else { return }
                var conflicted = conflicted
                if !(savedRecords?.isEmpty ?? true) {
                    debugPrint("QSCloudKitSynchronizer >> Uploaded \(savedRecords?.count ?? 0) records")
                }
                await adapter.didUpload(savedRecords: savedRecords ?? [])
                
                if let error = operationError as? NSError {
//                    if error.code == CKError.partialFailure.rawValue,
//                       let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError] {
//                        for (recordID, error) in errorsByItemID where error.code == CKError.serverRecordChanged.rawValue {
//                            if let serverRecord = error.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord {
//                                // Handle the server record changed error
//                                if let serverMessage = error.userInfo[NSLocalizedDescriptionKey] as? String,
//                                   serverMessage.contains("record to insert already exists") {
//                                    if !conflicted.contains(where: { $0.recordID == serverRecord.recordID }) {
////                                        conflicted.append(serverRecord)
//                                        print("!! WAS GONNA ADD ::")
//                                    }
//                                    // Handle the specific case where the record already exists
//                                    print("!! Record \(recordID) already exists on the server.")
//                                    //serverRecord/
//                                }
//                            }
//                        }
//                    }
                    
                    if self.isLimitExceededError(error) {
                        reduceBatchSize()
                        await completion(error)
                    } else if !conflicted.isEmpty {
                        do {
                            try await adapter.saveChanges(in: conflicted)
                        } catch {
                            await completion(error)
                            return
                        }
                        await adapter.persistImportedChanges { (persistError) in
                            await completion(persistError)
                        }
                    } else {
                        await completion(error)
                    }
                } else {
                    if recordCount >= requestedBatchSize {
                        await uploadRecords(adapter: adapter, completion: completion)
                    } else {
                        await completion(nil)
                    }
                }
                //                }
            }
        }
        
        runOperation(modifyRecordsOperation)
    }
    
    @MainActor
    func uploadDeletions(adapter: ModelAdapter, completion: @escaping (Error?) async -> ()) async {
        let recordIDs = adapter.recordIDsMarkedForDeletion(limit: batchSize)
        let recordCount = recordIDs.count
        let requestedBatchSize = batchSize
        
        guard recordCount > 0 else {
            await completion(nil)
            return
        }
        
        let modifyRecordsOperation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: recordIDs)
        modifyRecordsOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            Task(priority: .background) { [weak self] in
                guard let self = self else { return }
                debugPrint("QSCloudKitSynchronizer >> Deleted \(recordCount) records")
                    await adapter.didDelete(recordIDs: deletedRecordIDs ?? [])
                    
                    if let error = operationError {
                        if isLimitExceededError(error as NSError) {
                            reduceBatchSize()
                        }
                        await completion(error)
                    } else {
                        if recordCount >= requestedBatchSize {
                            await uploadDeletions(adapter: adapter, completion: completion)
                        } else {
                            await completion(nil)
                        }
                    }
            }
        }
        
        currentOperation = modifyRecordsOperation
        database.add(modifyRecordsOperation)
    }
    
    // MARK: - 
    
    @MainActor
    func updateTokens() {
        let operation = FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (databaseToken, changedZoneIDs, deletedZoneIDs) in
            Task(priority: .background) { [weak self] in
                guard let self = self else { return }
                await notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                if changedZoneIDs.count > 0 {
                    let zoneIDs = loadTokens(for: changedZoneIDs, loadAdapters: false)
                    await updateServerToken(for: zoneIDs, completion: { [weak self] (needsToFetchChanges) in
                        guard let self = self else { return }
                        if needsToFetchChanges {
                            await performSynchronization()
                        } else {
                            storedDatabaseToken = databaseToken
                            await finishSynchronization(error: nil)
                        }
                    })
                } else {
                    await finishSynchronization(error: nil)
                }
            }
        }
        runOperation(operation)
    }
    
    @MainActor
    func updateServerToken(for recordZoneIDs: [CKRecordZone.ID], completion: @escaping (Bool) async -> ()) async {
        // If we found a new record zone at this point then needsToFetchChanges=true
        var hasAllTokens = true
        for zoneID in recordZoneIDs {
            if activeZoneTokens[zoneID] == nil {
                hasAllTokens = false
            }
        }
        guard hasAllTokens else {
            await completion(true)
            return
        }
        
        let operation = FetchZoneChangesOperation(database: database, zoneIDs: recordZoneIDs, zoneChangeTokens: activeZoneTokens, modelVersion: compatibilityVersion, ignoreDeviceIdentifier: deviceIdentifier, desiredKeys: ["recordID", CloudKitSynchronizer.deviceUUIDKey]) { @MainActor [weak self] zoneResults in
            guard let self = self else { return }
//            var pendingZones = [CKRecordZone.ID]()
            var needsToRefetch = false
            
            for (zoneID, result) in zoneResults {
                let adapter = modelAdapterDictionary[zoneID]
                if result.downloadedRecords.count > 0 || result.deletedRecordIDs.count > 0 {
                    needsToRefetch = true
                } else {
                    activeZoneTokens[zoneID] = result.serverChangeToken
                    await adapter?.saveToken(result.serverChangeToken)
                }
//                if result.moreComing {
//                    pendingZones.append(zoneID)
//                }
            }
            
//            if pendingZones.count > 0 && !needsToRefetch {
//                await updateServerToken(for: pendingZones, completion: completion)
//            } else {
                await completion(needsToRefetch)
//            }
        }
        runOperation(operation)
    }
    
    func reduceBatchSize() {
        self.batchSize = self.batchSize / 2
    }
    
    func increaseBatchSize() {
        if self.batchSize < CloudKitSynchronizer.defaultBatchSize {
            self.batchSize = self.batchSize + 5
        }
    }
}
