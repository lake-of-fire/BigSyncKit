//
//  CloudKitSynchronizer+Sync.swift
//  Pods
//
//  Created by Manuel Entrena on 17/04/2019.
//

import Foundation
import CloudKit
import AsyncAlgorithms
import Combine

fileprivate func isZoneNotFoundOrDeletedError(_ error: Error?) -> Bool {
    if let error = error {
        let nserror = error as NSError
        return nserror.code == CKError.zoneNotFound.rawValue || nserror.code == CKError.userDeletedZone.rawValue
    } else {
        return false
    }
}

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func performSynchronization() async {
        if let sleepUntil = self.retrySleepUntil {
            let remainingTime = sleepUntil.timeIntervalSinceNow
            if remainingTime > 0 {
                logger.info("QSCloudKitSynchronizer >> Sleeping until retry date: \(sleepUntil) (for \(remainingTime) seconds)")
                do {
                    try await Task.sleep(nanoseconds: UInt64(remainingTime * 1_000_000_000))
                    retrySleepUntil = nil
                } catch {
                    logger.error("QSCloudKitSynchronizer >> Error during sleep: \(error.localizedDescription).")
                }
            }
        }
        
        logger.info("QSCloudKitSynchronizer >> Perform synchronization...")
        self.postNotification(.SynchronizerWillSynchronize)
        self.serverChangeToken = self.storedDatabaseToken
        self.uploadRetries = 0
        self.didNotifyUpload = Set<CKRecordZone.ID>()
        
        synchronizationTask?.cancel()
        synchronizationTask = Task { @BigSyncBackgroundActor [weak self] in
            await self?.fetchChanges()
        }
        await synchronizationTask?.value
        synchronizationTask = nil
    }
    
    @BigSyncBackgroundActor
    func changesFinishedSynchronizing() async {
//        logger.info("QSCloudKitSynchronizer >> Finishing synchronization batch...")
        
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            await adapter.didFinishImport(with: nil)
        }
        
        postNotification(.SynchronizerDidSynchronize)
        delegate?.synchronizerDidSync(self)
        
//        logger.info("QSCloudKitSynchronizer >> Finished synchronization batch")
        syncing = false
    }
    
    @BigSyncBackgroundActor
    func failSynchronization(error: Error) async {
        logger.info("QSCloudKitSynchronizer >> Failing or backing off synchronization...")
        
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            await adapter.didFinishImport(with: error)
        }
        
        self.postNotification(.SynchronizerDidFailToSynchronize, userInfo: [CloudKitSynchronizer.errorKey: error])
        self.delegate?.synchronizerDidfailToSync(self, error: error)
        
        if let error = error as? BigSyncKit.CloudKitSynchronizer.SyncError {
            switch error {
                //                    case .callFailed:
                //                        print("Sync error: \(error.localizedDescription) This error could be returned by completion block when no success and no error were produced.")
            case .cancelled:
                logger.info("QSCloudKitSynchronizer >> Synchronization canceled, not retrying")
                return
            case .higherModelVersionFound:
                // TODO: This error can be detected to prompt the user to update the app to a newer version.
                // TODO: Show this error inside settings view
                print("Sync error: \(error.localizedDescription) A synchronizer with a higher `compatibilityVersion` value uploaded changes to CloudKit, so those changes won't be imported here.")
                return
            default:// break
                logger.error("QSCloudKitSynchronizer >> Error: \(error)")
                //                print("# ")
            }
        } else if let error = error as? CKError {
            switch error.code {
            case .changeTokenExpired:
                //                    debugPrint("QSCloudKitSynchronizer >> Database change token expired, resetting and re-fetching changes...")
                logger.info("QSCloudKitSynchronizer >> Database change token expired, resetting and re-fetching changes...")
                // See: https://github.com/mentrena/SyncKit/issues/92#issuecomment-541362433
                self.resetDatabaseToken()
                for adapter in modelAdapters {
                    await adapter.saveToken(nil)
                }
            case .notAuthenticated:
                logger.error("QSCloudKitSynchronizer >> Not Authenticated. Aborting sync")
                // Don't retry...
                syncing = false
                cancelSync = false
                ChangeRequestProcessor.shared.cancelSync = true
                cancelledDueToUnauthentication = true
                return
            case .serviceUnavailable, .requestRateLimited, .zoneBusy:
                let retryAfter = (error.userInfo[CKErrorRetryAfterKey] as? Double) ?? 10.0
                logger.warning("QSCloudKitSynchronizer >> Warning: \(error.localizedDescription) ( \(error)). Retrying in \(retryAfter.rounded()) seconds.")
                reduceBatchSize()
                let sleepUntil = Date().addingTimeInterval(retryAfter)
                retrySleepUntil = sleepUntil
                let delay = sleepUntil.timeIntervalSinceNow
                if delay > 0 {
                    do {
                        try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                        retrySleepUntil = nil
                    } catch {
                        logger.error("QSCloudKitSynchronizer >> Error during sleep: \(error.localizedDescription).")
                    }
                }
                logger.info("QSCloudKitSynchronizer >> Waited \(retryAfter) seconds.")
            default:
                logger.error("QSCloudKitSynchronizer >> Error: \(error)")
                //                print("# ")
                //                break
            }
        }
        
        if cancelSync || error is CancellationError {
            logger.info("QSCloudKitSynchronizer >> Synchronization canceled, not retrying")
        } else {
            logger.info("QSCloudKitSynchronizer >> Retrying synchronization...")
            //            syncing = false
            //        cancelSync = false
            //            await beginSynchronization(force: true)
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                await performSynchronization()
            }
        }
        
        //        debugPrint("QSCloudKitSynchronizer >> Finishing synchronization")
        //        logger.info("QSCloudKitSynchronizer >> Finishing synchronization")
    }
}

// MARK: - Utilities

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func postNotification(_ notification: Notification.Name, object: Any? = nil, userInfo: [AnyHashable: Any]? = nil) {
        let object = object ?? self
        Task(priority: .background) { @BigSyncBackgroundActor in
            NotificationCenter.default.post(name: notification, object: object, userInfo: userInfo)
        }
    }
    
    @BigSyncBackgroundActor
    func runOperation(_ operation: CloudKitSynchronizerOperation) {
        //        logger.info("QSCloudKitSynchronizer >> Enqueue operation: \(type(of: operation))")
        operation.logger = logger
        operation.errorHandler = { [weak self] operation, error in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                if let ckError = error as? CKError, ckError.code == .serverRecordChanged {
                    // Conflict error: skip logging and failing synchronization
                    return
                }
                logger.error("QSCloudKitSynchronizer >> Operation error (\(type(of: operation))): \(error)")
                await self?.failSynchronization(error: error)
            }
        }
        currentOperations.removeAll { $0.isFinished }
        currentOperations.append(operation)
        operationQueue.addOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func notifyProviderForDeletedZoneIDs(_ zoneIDs: [CKRecordZone.ID]) async {
        for zoneID in zoneIDs {
            await self.adapterProvider.cloudKitSynchronizer(self, zoneWasDeletedWithZoneID: zoneID)
            self.delegate?.synchronizer(self, zoneIDWasDeleted: zoneID)
        }
    }
    
    @BigSyncBackgroundActor
    func loadTokens(for zoneIDs: [CKRecordZone.ID], loadAdapters: Bool) async throws -> [CKRecordZone.ID] {
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
                activeZoneTokens[zoneID] = await adapter.serverChangeToken
            }
        }
        
        return filteredZoneIDs
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
    }
    
    func shouldRetryUpload(for error: NSError) -> Bool {
        if /*isServerRecordChangedError(error) ||*/ isLimitExceededError(error) {
            return uploadRetries < 5
        } else {
            return isServerRecordChangedError(error)
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
    
    func sequential<T>(objects: [T], closure: @escaping (T, @escaping (Error?) async throws -> ()) async throws -> (), final: @escaping (Error?) async throws -> ()) async throws {
        guard let first = objects.first else {
            try await final(nil)
            return
        }
        
        do {
            try Task.checkCancellation()
        } catch {
            try await final(error)
            return
        }
        
        guard await !cancelSync else {
            try await final(SyncError.cancelled)
            return
        }
        
        do {
            try Task.checkCancellation()
        } catch {
            try await final(error)
            return
        }
        
        //        debugPrint("# sequential closure(...)")
        try await closure(first) { [weak self] error in
            guard let self else { return }
            guard error == nil else {
                try await final(error)
                return
            }
            
            // For lowering CPU priority gently
            try? await Task.sleep(nanoseconds: 10_000)
            do {
                try Task.checkCancellation()
                guard await !cancelSync else { throw CancellationError() }
            } catch {
                try await final(error)
                return
            }

            var remaining = objects
            remaining.removeFirst()
            try await sequential(objects: remaining, closure: closure, final: final)
        }
    }
    
    @BigSyncBackgroundActor
    func needsZoneSetup(adapter: ModelAdapter) async throws -> Bool {
        //        debugPrint("# needsZoneSetup?", adapter.recordZoneID, adapter.serverChangeToken)
        return await adapter.serverChangeToken == nil
    }
}

//MARK: - Fetch changes

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func shouldDeferFetches() async throws -> Bool {
        guard syncMode == .sync else { return false }
        if let lastEmpty = lastDatabaseChangesEmptyAt,
           Date().timeIntervalSince(lastEmpty) < 45 * 60 {
            for adapter in modelAdapters {
                try Task.checkCancellation()
                if adapter.hasChanges {
//                    logger.info("QSCloudKitSynchronizer >> Skipping CloudKit token update: last fetch was empty and recent and uploads are pending")
                    return true
                }
            }
        }
        return false
    }
    
    @BigSyncBackgroundActor
    func fetchChanges() async {
        //        debugPrint("# fetchChanges()")
//        logger.info("QSCloudKitSynchronizer >> Fetch changes?")
        guard !cancelSync else {
            await failSynchronization(error: SyncError.cancelled)
            return
        }
        
        do {
            try Task.checkCancellation()
            
            if try await shouldDeferFetches() {
                try await uploadChanges()
                return
            }
            
            try Task.checkCancellation()
        } catch {
            await failSynchronization(error: error)
            return
        }
        
        postNotification(.SynchronizerWillFetchChanges)
        
        await fetchDatabaseChanges() { [weak self] token, error in
            guard let self else { return }
            if let error {
                await failSynchronization(error: error)
                return
            }
            
            serverChangeToken = token
            storedDatabaseToken = token
            if syncMode == .sync {
                try await uploadChanges()
            } else {
                await changesFinishedSynchronizing()
            }
        }
    }
    
    @BigSyncBackgroundActor
    func fetchDatabaseChanges(completion: @escaping (CKServerChangeToken?, Error?) async throws -> ()) async {
        //        debugPrint("# fetchDatabaseChanges() (calls FetchDatabaseChangesOperation)") //, containerIdentifier, serverChangeToken)
        let operation = await FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { [weak self] (token, changedZoneIDs, deletedZoneIDs) in
            guard let self else { return }
            fetchDatabaseChangesTask?.cancel()
            fetchDatabaseChangesTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                
                try Task.checkCancellation()
                guard !cancelSync else {
                    await failSynchronization(error: SyncError.cancelled)
                    return
                }
                
                await notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                
                let zoneIDsToFetch = try await loadTokens(for: changedZoneIDs, loadAdapters: true)
                
                //                debugPrint("# zoneIDsToFetch", zoneIDsToFetch)
                guard zoneIDsToFetch.count > 0 else {
                    self.lastDatabaseChangesEmptyAt = Date()
                    await self.resetActiveTokens()
                    try await completion(token, nil)
                    return
                }
                
                lastDatabaseChangesEmptyAt = nil
                
                try Task.checkCancellation()
                guard !cancelSync else {
                    await failSynchronization(error: SyncError.cancelled)
                    return
                }
                
                try await { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    guard !cancelSync else {
                        await failSynchronization(error: SyncError.cancelled)
                        return
                    }
                    
                    zoneIDsToFetch.forEach {
                        self.delegate?.synchronizerWillFetchChanges(self, in: $0)
                    }
                    
                    fetchZoneChanges(zoneIDsToFetch) { [weak self] error in
                        guard let self else { return }
                        fetchZoneChangesTask?.cancel()
                        fetchZoneChangesTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                            //                        debugPrint("# fetchZoneChanges callback")
                            guard let self else { return }
                            try Task.checkCancellation()
                            if let error {
                                await failSynchronization(error: error)
                                return
                            }
                            try Task.checkCancellation()
                            guard !cancelSync else {
                                await failSynchronization(error: SyncError.cancelled)
                                return
                            }
                            
                            try await mergeChanges() { [weak self] error in
                                guard let self else { return }
                                mergeChangesTask?.cancel()
                                mergeChangesTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                                    try Task.checkCancellation()
                                    try await completion(token, error)
                                }
                            }
                        }
                    }
                }()
            }
        }
        await runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID], completion: @escaping (Error?) async throws -> ()) {
        //        debugPrint("# fetchZoneChanges(...)", zoneIDs)
        let changeRequestProcessor = ChangeRequestProcessor.shared
        let operation = FetchZoneChangesOperation(
            database: database,
            zoneIDs: zoneIDs,
            zoneChangeTokens: activeZoneTokens,
            modelVersion: compatibilityVersion,
            ignoreDeviceIdentifier: deviceIdentifier,
            desiredKeys: nil
        ) { [weak self] (downloadedRecord, deletedRecordID) in
            guard let self else { return }
            guard let zoneID = downloadedRecord?.recordID.zoneID ?? deletedRecordID?.zoneID else {
                debugPrint("Unexpectedly found no downloaded record or deleted record ID")
                return
            }
            guard !cancelSync else { return }
            
            let adapter = await modelAdapterDictionary[zoneID]
            if let adapter {
                let changeRequest = ChangeRequest(
                    downloadedRecord: downloadedRecord,
                    deletedRecordID: deletedRecordID,
                    adapter: adapter
                )
                //                logger.info("QSCloudKitSynchronizer >> Enqueueing remote record for local merge: \(downloadedRecord?.recordID.recordName)")
                guard !cancelSync else { return }
                await changeRequestProcessor.addFetchedChangeRequest(changeRequest)
            }
        } completion: { [weak self] zoneResults in
            guard let self else { return }
            fetchZoneChangesCompletionTask?.cancel()
            fetchZoneChangesCompletionTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                
                defer {
                    changeRequestProcessor.clearErrors()
                }
                
                try Task.checkCancellation()
                let error: Error? = try? zoneResults.lazy.compactMap({ [weak self] (zoneID, zoneResult) -> Error? in
                    guard let self = self else { return nil }
                    try Task.checkCancellation()
                    if let error = zoneResult.error {
                        if isZoneNotFoundOrDeletedError(error) {
                            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                                guard let self else { return }
                                await notifyProviderForDeletedZoneIDs([zoneID])
                            }
                        } else {
                            return error
                        }
                    }
                    return nil
                }).first
                
                // Process any remaining change requests before saving server change tokens
                try await changeRequestProcessor.finishProcessing()
                try Task.checkCancellation()
                
                for (zoneID, zoneResult) in zoneResults {
                    if !zoneResult.downloadedRecords.isEmpty {
                        logger.info("QSCloudKitSynchronizer >> Downloaded \(zoneResult.downloadedRecords.count) changed records from zone \(zoneID.zoneName)")
                    }
                    if !zoneResult.deletedRecordIDs.isEmpty {
                        //                        debugPrint("QSCloudKitSynchronizer >> Downloaded \(zoneResult.deletedRecordIDs.count) deleted record IDs >> from zone \(zoneID.zoneName)")
                        logger.info("QSCloudKitSynchronizer >> Downloaded \(zoneResult.deletedRecordIDs.count) deleted record IDs from zone \(zoneID.zoneName)")
                    }
                    do {
                        try await { @BigSyncBackgroundActor [weak self] in
                            guard let self = self else { return }
                            activeZoneTokens[zoneID] = zoneResult.serverChangeToken
                        }()
                    } catch {
                        try await completion(error)
                        return
                    }
                }
                
                // Collect any errors from the processor
                if let firstError = changeRequestProcessor.getErrors().first {
                    try await completion(firstError)
                } else {
                    try await completion(error)
                }
            }
        }
        runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func mergeChanges(
        completion: @escaping (Error?) async throws -> ()
    ) async throws {
        //        debugPrint("# mergeChanges()")
        guard !cancelSync else {
            await failSynchronization(error: SyncError.cancelled)
            return
        }
        
        var adapterSet = [ModelAdapter]()
        activeZoneTokens.keys.forEach {
            if let adapter = self.modelAdapterDictionary[$0] {
                adapterSet.append(adapter)
            }
        }
        
        try await sequential(
            objects: adapterSet,
            closure: mergeChangesIntoAdapter,
            final: completion
        )
    }
    
    @BigSyncBackgroundActor
    func mergeChangesIntoAdapter(_ adapter: ModelAdapter, completion: @escaping (Error?) async throws -> ()) async throws {
        do {
            try await adapter.persistImportedChanges()
        } catch {
            try await completion(error)
            return
        }
        
        if let token = activeZoneToken(zoneID: adapter.recordZoneID) {
            await adapter.saveToken(token)
        }
        try await completion(nil)
    }
}

// MARK: - Upload changes

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func uploadChanges() async throws {
        logger.info("QSCloudKitSynchronizer >> Upload changes...")
        //        debugPrint("# uploadChanges()")
        guard !cancelSync else {
            await failSynchronization(error: SyncError.cancelled)
            return
        }
        try Task.checkCancellation()
        
        postNotification(.SynchronizerWillUploadChanges)
        
        try await uploadChanges() { [weak self] (error) in
            try Task.checkCancellation()
            guard let self else { return }
            
            if let error = error as? NSError {
#warning("FIXME: handle zone not found...")
                if shouldRetryUpload(for: error) {
                    //                    print("# uploadChanges() failed, retrying via fetchChanges()")
                    uploadRetries += 1
                    logger.info("QSCloudKitSynchronizer >> Retrying upload due to error \(error.description.prefix(200)), beginning with fetching changes...")
                    await fetchChanges()
                } else {
                    await failSynchronization(error: error)
                }
            } else {
                if try await shouldDeferFetches() {
                    //                    debugPrint("# USED TO STOP HERE, NOw LOOPIN!")
                    await performSynchronization()
                } else {
                    updateTokens()
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    func uploadChanges(completion: @escaping (Error?) async throws -> ()) async throws {
        //        debugPrint("# uploadChanges(completion)")
        try await sequential(objects: modelAdapters, closure: setupZoneAndUploadRecords) { [weak self] (error) in
            guard error == nil else {
                try await completion(error)
                return
            }
            guard let self else { return }
            guard !cancelSync else { throw CancellationError() }

            try await sequential(objects: modelAdapters, closure: uploadDeletions, final: completion)
        }
    }
    
    @BigSyncBackgroundActor
    func setupZoneAndUploadRecords(adapter: ModelAdapter, completion: @escaping (Error?) async throws -> ()) async throws {
        try await setupRecordZoneIfNeeded(adapter: adapter) { [weak self] (error) in
            guard let self, error == nil else {
                try await completion(error)
                return
            }
            guard !cancelSync else { throw CancellationError() }
            //            debugPrint("# uploadRecords from setupZoneAndUploadRecords")
            try await uploadRecords(adapter: adapter, completion: { [weak self] (error) in
                if error == nil {
                    self?.increaseBatchSize()
                }
                try await completion(error)
            })
        }
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneIfNeeded(adapter: ModelAdapter, completion: @escaping (Error?) async throws -> ()) async throws {
        guard try await needsZoneSetup(adapter: adapter) else {
            try await completion(nil)
            return
        }
        
        try await setupRecordZoneID(adapter.recordZoneID, completion: completion)
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneID(_ zoneID: CKRecordZone.ID, completion: @escaping (Error?) async throws -> ()) {
        database.fetch(withRecordZoneID: zoneID) { [weak self] (zone, error) in
            guard let self = self else { return }
            if isZoneNotFoundOrDeletedError(error) {
                let newZone = CKRecordZone(zoneID: zoneID)
                database.save(zone: newZone, completionHandler: { [weak self] (zone, error) in
                    if error == nil && zone != nil {
                        //                        debugPrint("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                        self?.logger.info("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                    }
                    Task(priority: .background) { @BigSyncBackgroundActor in
                        try await completion(error)
                    }
                })
            } else {
                Task(priority: .background) { @BigSyncBackgroundActor in
                    try await completion(error)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    func uploadRecords(adapter: ModelAdapter, completion: @escaping (Error?) async throws -> ()) async throws {
        guard !cancelSync else { throw CancellationError() }
        
        let requestedBatchSize = batchSize
        let records = try await adapter.recordsToUpload(limit: requestedBatchSize)
        let recordCount = records.count
        //        debugPrint("# uploadRecords", adapter.recordZoneID, "count", records.count, records.map { $0.recordID.recordName })
        guard recordCount > 0 else { try await completion(nil); return }
        
        logger.info("QSCloudKitSynchronizer >> Uploading \(recordCount) records to \(adapter.recordZoneID)")
//        logger.info("QSCloudKitSynchronizer >> Uploading records: \(records.map { $0.recordID.recordName } .joined(separator: " "))")
        
        guard !cancelSync else { throw CancellationError() }
        
        if !didNotifyUpload.contains(adapter.recordZoneID) {
            didNotifyUpload.insert(adapter.recordZoneID)
            delegate?.synchronizerWillUploadChanges(self, to: adapter.recordZoneID)
        }
        
        //Add metadata: device UUID and model version
        addMetadata(to: records)
        //        debugPrint("## Upload", records.map {($0.recordID, $0) })
        let modifyRecordsOperation = ModifyRecordsOperation(
            database: database,
            records: records,
            recordIDsToDelete: nil
        ) { [weak self] (savedRecords, deleted, conflicted, recordIDsMissingOnServer, operationError) in
            //            debugPrint("# uploadRecords, inside operation callback...", records.count)
            guard let self else { return }
            modifyRecordsTask?.cancel()
            modifyRecordsTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                //                debugPrint("# uploadRecords, inside operation callback Task...", records.count, "saved", savedRecords?.count, "del", deleted?.count, "conflicted", conflicted.count, operationError)
                guard let self else { return }
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                var conflicted = conflicted
                if let savedRecords, !savedRecords.isEmpty {
                    //                    debugPrint("QSCloudKitSynchronizer >> Uploaded \(savedRecords?.count ?? 0) records")
                    logger.info("QSCloudKitSynchronizer >> Uploaded \(savedRecords.count) records")
//                    logger.info("QSCloudKitSynchronizer >> Uploaded records: \(savedRecords.map { ($0.recordID.recordName, $0.debugDescription) })")
                    //                    logger.info("QSCloudKitSynchronizer >> Uploaded records: \((savedRecords?.map { $0.recordID.recordName } ?? []).joined(separator: " "))")
                    
                    try await adapter.didUpload(savedRecords: savedRecords)
                }
                
                try Task.checkCancellation()
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
                    
                    if !recordIDsMissingOnServer.isEmpty {
                        try await adapter.deleteChangeTracking(forRecordIDs: Array(recordIDsMissingOnServer))
                    }
                    
                    if let errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError] {
                        var resolvedRecords = [CKRecord]()
                        for (_, itemError) in errorsByItemID {
                            if itemError.code == CKError.serverRecordChanged.rawValue,
                               let serverRecord = itemError.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord {
                                resolvedRecords.append(serverRecord)
                            }
                        }
                        //                        debugPrint("## Resolved Recos", resolvedRecords.map {($0.recordID, $0) })
                        if !resolvedRecords.isEmpty {
                            try Task.checkCancellation()
                            do {
                                try await adapter.saveChanges(in: resolvedRecords, forceSave: true)
                                try await adapter.persistImportedChanges()
                                increaseBatchSize()
                                
                                try Task.checkCancellation()
                                guard !cancelSync else { throw CancellationError() }
                                
                                // Proceed to upload remaining records after resolving conflicts
                                try await uploadRecords(adapter: adapter, completion: completion)
                            } catch {
                                logger.info("QSCloudKitSynchronizer >> WARNING: Failed to save changes to resolved conflicted record: \(error)")
                                try await completion(error)
                            }
                            return
                        }
                    } else {
                        if self.isLimitExceededError(error) {
                            reduceBatchSize()
                        }
                        
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        try await completion(error)
                        return
                    }
                }
                
                guard !cancelSync else { throw CancellationError() }
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                if recordCount >= requestedBatchSize {
                    increaseBatchSize()
                    //                    debugPrint("# uploadRecords from inside uploadRecords")
                    
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    
                    try await uploadRecords(adapter: adapter, completion: completion)
                } else {
                    try await completion(nil)
                }
                //                }
            }
        }
        
        runOperation(modifyRecordsOperation)
    }
    
    @BigSyncBackgroundActor
    func uploadDeletions(adapter: ModelAdapter, completion: @escaping (Error?) async throws -> ()) async throws {
        let recordIDs = try await adapter.recordIDsMarkedForDeletion(limit: batchSize)
        let recordCount = recordIDs.count
        let requestedBatchSize = batchSize
        
        guard recordCount > 0 else {
            try await completion(nil)
            return
        }
        
        let modifyRecordsOperation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: recordIDs)
        modifyRecordsOperation.modifyRecordsCompletionBlock = { savedRecords, deletedRecordIDs, operationError in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                //                debugPrint("QSCloudKitSynchronizer >> Deleted \(recordCount) records")
                logger.info("QSCloudKitSynchronizer >> Deleted \(recordCount) records")
                await adapter.didDelete(recordIDs: deletedRecordIDs ?? [])
                
                if let error = operationError {
                    if isLimitExceededError(error as NSError) {
                        reduceBatchSize()
                    }
                    try await completion(error)
                } else {
                    if recordCount >= requestedBatchSize {
                        try await uploadDeletions(adapter: adapter, completion: completion)
                    } else {
                        try await completion(nil)
                    }
                }
            }
        }
        
        currentOperations.append(modifyRecordsOperation)
        database.add(modifyRecordsOperation)
    }
    
    // MARK: -
    
    @BigSyncBackgroundActor
    func updateTokens() {
        //        debugPrint("# updateTokens() (calls FetchDatabaseChangesOperation)")
        let operation = FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (databaseToken, changedZoneIDs, deletedZoneIDs) in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                
                guard !cancelSync else {
                    await failSynchronization(error: SyncError.cancelled)
                    return
                }
                
                await notifyProviderForDeletedZoneIDs(deletedZoneIDs)
                if changedZoneIDs.count > 0 {
                    let zoneIDs = try await loadTokens(for: changedZoneIDs, loadAdapters: false)
                    await updateServerToken(for: zoneIDs, completion: { [weak self] (needsToFetchChanges) in
                        guard let self = self else { return }
                        if needsToFetchChanges {
                            await performSynchronization()
                        } else {
                            storedDatabaseToken = databaseToken
                            await changesFinishedSynchronizing()
                        }
                    })
                } else {
                    await changesFinishedSynchronizing()
                }
            }
        }
        runOperation(operation)
    }
    
    @BigSyncBackgroundActor
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
        
//        logger.info("QSCloudKitSynchronizer >> Update server token....")
        
        let operation = FetchZoneChangesOperation(
            database: database,
            zoneIDs: recordZoneIDs,
            zoneChangeTokens: activeZoneTokens,
            modelVersion: compatibilityVersion,
            ignoreDeviceIdentifier: deviceIdentifier,
            desiredKeys: [
                "recordID",
                CloudKitSynchronizer.deviceUUIDKey
            ]
        ) { @BigSyncBackgroundActor [weak self] zoneResults in
            guard let self = self else { return }
            
            guard !cancelSync else {
                await failSynchronization(error: SyncError.cancelled)
                return
            }
            
            var pendingZones = [CKRecordZone.ID]()
            var needsToRefetch = false
            
            for (zoneID, result) in zoneResults {
                let adapter = modelAdapterDictionary[zoneID]
                if result.downloadedRecords.count > 0 || result.deletedRecordIDs.count > 0 {
                    needsToRefetch = true
                } else {
                    activeZoneTokens[zoneID] = result.serverChangeToken
                    await adapter?.saveToken(result.serverChangeToken)
                }
                
                if result.moreComing {
                    pendingZones.append(zoneID)
                }
            }
            
            if pendingZones.count > 0 && !needsToRefetch {
                await updateServerToken(for: pendingZones, completion: completion)
            } else {
                await completion(needsToRefetch)
            }
        }
        runOperation(operation)
    }
    
    func reduceBatchSize() {
        self.batchSize = max(1, Int((Double(self.batchSize) / 2.75).rounded()))
    }
    
    func increaseBatchSize() {
        if self.batchSize < CloudKitSynchronizer.maxBatchSize {
            //            self.batchSize = min(CloudKitSynchronizer.maxBatchSize, self.batchSize + ((CloudKitSynchronizer.maxBatchSize - CloudKitSynchronizer.defaultInitialBatchSize) / 5))
            self.batchSize = min(CloudKitSynchronizer.maxBatchSize, max(batchSize + 1, Int((Double(self.batchSize) * 1.12).rounded())))
        }
    }
}
