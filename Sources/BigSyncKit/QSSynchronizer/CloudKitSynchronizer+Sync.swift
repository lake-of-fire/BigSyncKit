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

private func cloudKitErrors(in error: Error) -> [CKError] {
    guard let cloudKitError = error as? CKError else { return [] }
    guard cloudKitError.code == .partialFailure,
          let partialErrors = cloudKitError.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error] else {
        return [cloudKitError]
    }
    return [cloudKitError] + partialErrors.values.flatMap(cloudKitErrors(in:))
}

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func performSynchronization() async {
        logger.info("QSCloudKitSynchronizer >> Perform synchronization...")
        self.postNotification(.SynchronizerWillSynchronize)
        self.serverChangeToken = self.storedDatabaseToken
        self.uploadRetries = 0
        self.didNotifyUpload = Set<CKRecordZone.ID>()
        await fetchChanges()
    }
    
    @BigSyncBackgroundActor
    func changesFinishedSynchronizing() async {
        let attemptID = synchronizationAttemptID
        do {
            try await revalidateActiveRunContext(for: attemptID)
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
//        logger.info("QSCloudKitSynchronizer >> Finishing synchronization batch...")
        
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            do {
                try await adapter.didFinishImport()
                try await revalidateActiveRunContext(for: attemptID)
                try await adapter.cleanUp()
                try await revalidateActiveRunContext(for: attemptID)
                // Cleanup can overlap a newly committed local mutation. Forward
                // journals again so that mutation requests a new
                // drain before a terminal receipt is issued.
                try await adapter.didFinishImport()
                try await revalidateActiveRunContext(for: attemptID)
            } catch is CancellationError {
                return
            } catch {
                await failSynchronization(error: error)
                return
            }
        }

        do {
            // The final import can legitimately forward zero new journal rows
            // while durable tracking work remains.
            // Recheck all adapters after the last suspension and convert any
            // pending state into a tail drain before authorizing a receipt.
            for adapter in modelAdapters {
                let hasPendingChanges: Bool
                if let terminalStateAdapter =
                    adapter as? TerminalSynchronizationStateModelAdapter {
                    hasPendingChanges = try terminalStateAdapter
                        .hasPendingChangesAtTerminalBoundary()
                } else {
                    hasPendingChanges = adapter.hasChanges
                }
                if hasPendingChanges {
                    synchronizationRequestedWhileRunning = true
                }
            }
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
        
//        logger.info("QSCloudKitSynchronizer >> Finished synchronization batch")
        syncing = false
        synchronizationTask = nil
        if synchronizationRequestedWhileRunning {
            synchronizationRequestedWhileRunning = false
            beginSynchronization()
            return
        }
        // Snapshot and publish the terminal result before invoking synchronous
        // external callbacks. A notification observer or delegate is allowed to
        // request another synchronization. If it does, that request must start a
        // fresh drain; the old completion must not subsequently clear the new
        // drain's state or mint its receipt from mutable run state.
        let receipt = activeRunContext.map { context in
            let authorizationID = UUID()
            activeReceiptAuthorizationID = authorizationID
            return SynchronizationReceipt(
                context: context,
                issuerID: synchronizationReceiptIssuerID,
                authorizationID: authorizationID
            )
        }
        let result = SynchronizationResult(
            didImportChanges: synchronizationDrainDidImportChanges,
            receipt: receipt
        )
        finishSynchronizationDrain(with: .success(result))
        postNotification(.SynchronizerDidSynchronize)
        delegate?.synchronizerDidSync(self)
    }
    
//    @BigSyncBackgroundActor
    func failSynchronization(error: Error) async {
        let attemptID = synchronizationAttemptID
        logger.info("QSCloudKitSynchronizer >> Failing or backing off synchronization...")
        
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            do {
                try await adapter.didFinishImport()
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed final import forwarding: \(error)")
            }
            guard synchronizationAttemptID == attemptID else { return }
        }
        
        self.postNotification(.SynchronizerDidFailToSynchronize, userInfo: [cloudKitSynchronizerErrorKey: error])
        self.delegate?.synchronizerDidfailToSync(self, error: error)
        
        var shouldRetry = false
        var retryDelay: TimeInterval = 0

        if let error = error as? BigSyncKit.CloudKitSynchronizer.SyncError {
            switch error {
                //                    case .callFailed:
                //                        print("Sync error: \(error.localizedDescription) This error could be returned by completion block when no success and no error were produced.")
            case .cancelled:
                logger.info("QSCloudKitSynchronizer >> Synchronization canceled, not retrying")
            case .higherModelVersionFound:
                // TODO: This error can be detected to prompt the user to update the app to a newer version.
                // TODO: Show this error inside settings view
                print("Sync error: \(error.localizedDescription) A synchronizer with a higher `compatibilityVersion` value uploaded changes to CloudKit, so those changes won't be imported here.")
            default:// break
                logger.error("QSCloudKitSynchronizer >> Error: \(error)")
                //                print("# ")
            }
        } else if let topLevelError = error as? CKError {
            let errors = cloudKitErrors(in: topLevelError)
            let codes = Set(errors.map(\.code))
            if codes.contains(.changeTokenExpired) {
                logger.info("QSCloudKitSynchronizer >> Change token expired, resetting and re-fetching changes...")
                self.resetDatabaseToken()
                do {
                    for adapter in modelAdapters {
                        try await adapter.saveToken(nil)
                    }
                    shouldRetry = true
                } catch {
                    logger.error("QSCloudKitSynchronizer >> Failed to clear expired adapter token: \(error)")
                }
            } else if codes.contains(.notAuthenticated) {
                logger.error("QSCloudKitSynchronizer >> Not Authenticated. Aborting sync")
                changeRequestProcessor.reset()
                cancelledDueToUnauthentication = true
            } else if !codes.isDisjoint(with: [
                .serviceUnavailable,
                .requestRateLimited,
                .zoneBusy,
                .networkFailure,
                .networkUnavailable,
                .accountTemporarilyUnavailable,
            ]) {
                let requestedDelays = errors.compactMap {
                    $0.userInfo[CKErrorRetryAfterKey] as? Double
                }
                let baseDelay = min(300, max(requestedDelays.max() ?? 5, 1))
                retryDelay = min(300, baseDelay * Double.random(in: 0.9...1.1))
                logger.warning(
                    "QSCloudKitSynchronizer >> Transient CloudKit error. Retrying in \(retryDelay.rounded()) seconds."
                )
                reduceBatchSize()
                shouldRetry = true
            } else {
                logger.error("QSCloudKitSynchronizer >> Error: \(topLevelError)")
            }
        }

        if error is CancellationError {
            logger.info("QSCloudKitSynchronizer >> Synchronization canceled, not retrying")
            shouldRetry = false
        }

        syncing = shouldRetry && !cancelSync
        synchronizationTask = nil

        guard shouldRetry, !cancelSync else {
            // A final journal drain can discover a local mutation while this
            // failed attempt is still marked as running. Its delegate wakeup
            // is therefore coalesced into synchronizationRequestedWhileRunning.
            // Complete the failed caller first, then give that newly discovered
            // durable work one independent tail attempt. The next failure will
            // not loop unless another journal generation is actually forwarded.
            let shouldStartDeferredLocalWorkDrain =
                synchronizationRequestedWhileRunning &&
                !cancelSync &&
                !(error is CancellationError) &&
                (error as? SyncError) != .cancelled &&
                !cancelledDueToUnauthentication
            syncing = false
            finishSynchronizationDrain(with: .failure(error))
            if shouldStartDeferredLocalWorkDrain {
                beginSynchronization()
            }
            return
        }

        retrySleepUntil = Date().addingTimeInterval(retryDelay)
        synchronizationTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            if retryDelay > 0 {
                do {
                    try await Task.sleep(nanoseconds: UInt64(retryDelay * 1_000_000_000))
                } catch {
                    return
                }
            }
            guard !cancelSync,
                  synchronizationAttemptID == attemptID else { return }
            retrySleepUntil = nil
            synchronizationTask = nil
            syncing = false
            synchronizationRequestedWhileRunning = false
            logger.info("QSCloudKitSynchronizer >> Retrying synchronization...")
            beginSynchronization()
        }
    }
}

// MARK: - Utilities

extension CloudKitSynchronizer {
//    @BigSyncBackgroundActor
    func postNotification(_ notification: Notification.Name, object: Any? = nil, userInfo: [AnyHashable: Any]? = nil) {
        let object = object ?? self
//        Task(priority: .background) { @BigSyncBackgroundActor in
            NotificationCenter.default.post(name: notification, object: object, userInfo: userInfo)
//        }
    }
    
//    @BigSyncBackgroundActor
    func runOperation(_ operation: CloudKitSynchronizerOperation) {
        let attemptID = synchronizationAttemptID
        //        logger.info("QSCloudKitSynchronizer >> Enqueue operation: \(type(of: operation))")
        operation.logger = logger
        operation.errorHandler = { [weak self] operation, error in
            guard let self else { return }
            if let ckError = error as? CKError, ckError.code == .serverRecordChanged {
                // Conflict error: skip logging and failing synchronization
                return
            }
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self,
                      beginRunCallback(for: attemptID) else { return }
                defer { endRunCallback() }
                logger.error(
                    "QSCloudKitSynchronizer >> Operation error (\(type(of: operation))): \(error)"
                )
                await failSynchronization(error: error)
            }
        }
        operation.finishedHandler = { [weak self] finishedOperation in
            Task { @BigSyncBackgroundActor [weak self] in
                self?.currentOperations.removeAll { $0 === finishedOperation }
            }
        }
        currentOperations.removeAll { $0.isFinished }
        currentOperations.append(operation)
        operationQueue.addOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func notifyProviderForDeletedZoneIDs(
        _ zoneIDs: [CKRecordZone.ID],
        attemptID: UUID
    ) async throws {
        for zoneID in zoneIDs {
            // A deleted-zone callback must not clear tracking metadata after
            // CloudKit has switched accounts.
            try await revalidateActiveRunContext(for: attemptID)
            try await self.adapterProvider.cloudKitSynchronizer(
                self,
                zoneWasDeletedWithZoneID: zoneID
            )
            try await revalidateActiveRunContext(for: attemptID)
            self.delegate?.synchronizer(self, zoneIDWasDeleted: zoneID)
        }
    }
    
    @BigSyncBackgroundActor
    func loadTokens(for zoneIDs: [CKRecordZone.ID]) async throws -> [CKRecordZone.ID] {
        var filteredZoneIDs = [CKRecordZone.ID]()
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
        
        for zoneID in zoneIDs {
            // Manabi explicitly registers its one supported synchronization
            // zone. Ignore unrelated private-database zones instead of
            // dynamically constructing an incompletely configured adapter.
            guard let adapter = modelAdapterDictionary[zoneID] else { continue }
            filteredZoneIDs.append(zoneID)
            activeZoneTokens[zoneID] = await adapter.serverChangeToken
        }
        
        return filteredZoneIDs
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
    }
    
    func shouldRetryUpload(for error: NSError) -> Bool {
        if /*isServerRecordChangedError(error) ||*/ isLimitExceededError(error) {
            return uploadRetries < 5
        } else if isZoneNotFoundOrDeletedError(error) {
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
    
    @BigSyncBackgroundActor
    func sequential<T>(
        objects: [T],
        closure: @Sendable @BigSyncBackgroundActor @escaping (T, @BigSyncBackgroundActor @escaping (Error?) async throws -> ()) async throws -> (),
        final: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
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
            
            // Cooperatively allow other work to run without imposing a
            // wall-clock delay between each sequential operation.
            await Task.yield()
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
        let attemptID = synchronizationAttemptID
        //        debugPrint("# fetchChanges()")
//        logger.info("QSCloudKitSynchronizer >> Fetch changes?")
        guard !cancelSync else {
            guard synchronizationAttemptID == attemptID else { return }
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
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: error)
            return
        }
        
        postNotification(.SynchronizerWillFetchChanges)
        
        await fetchDatabaseChanges() { [weak self] token, error in
            guard let self,
                  synchronizationAttemptID == attemptID else { return }
            if let error {
                await failSynchronization(error: error)
                return
            }
            try await revalidateActiveRunContext(for: attemptID)
            
            serverChangeToken = token
            if syncMode == .sync {
                try await uploadChanges()
            } else {
                do {
                    try await processFetchedChanges()
                    try await revalidateActiveRunContext(for: attemptID)
                    storedDatabaseToken = token
                    await changesFinishedSynchronizing()
                } catch {
                    await failSynchronization(error: error)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    func fetchDatabaseChanges(completion: @escaping (CKServerChangeToken?, Error?) async throws -> ()) async {
        //        debugPrint("# fetchDatabaseChanges() (calls FetchDatabaseChangesOperation)") //, containerIdentifier, serverChangeToken)
        let attemptID = synchronizationAttemptID
        let operation = await FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { [weak self] (token, changedZoneIDs, deletedZoneIDs) in
            guard let self else { return }
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self,
                      beginRunCallback(for: attemptID) else { return }
                defer { endRunCallback() }
                do {
                
                try Task.checkCancellation()
                guard !cancelSync else {
                    await failSynchronization(error: SyncError.cancelled)
                    return
                }
                
                try await notifyProviderForDeletedZoneIDs(
                    deletedZoneIDs,
                    attemptID: attemptID
                )
                try await revalidateActiveRunContext(for: attemptID)
                
                let zoneIDsToFetch = try await loadTokens(for: changedZoneIDs)
                try await revalidateActiveRunContext(for: attemptID)
                
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
                
                zoneIDsToFetch.forEach {
                    self.delegate?.synchronizerWillFetchChanges(self, in: $0)
                }
                
                fetchZoneChanges(zoneIDsToFetch) { [weak self] error in
                    guard let self,
                          synchronizationAttemptID == attemptID else { return }
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

                    try await completion(token, nil)
                }
                } catch {
                    // Errors thrown inside this callback-owned task must not be
                    // discarded after the CloudKit operation
                    // has already reported success, leaving the run unresolved.
                    guard synchronizationAttemptID == attemptID else { return }
                    await failSynchronization(error: error)
                }
            }
        }
        await runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func fetchZoneChanges(
        _ zoneIDs: [CKRecordZone.ID],
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) {
        let changeRequestProcessor = changeRequestProcessor
        let attemptID = synchronizationAttemptID
        let runID = synchronizationRunID
        let operation = FetchZoneChangesOperation(
            database: database,
            zoneIDs: zoneIDs,
            zoneChangeTokens: activeZoneTokens,
            modelVersion: compatibilityVersion,
            ignoreDeviceIdentifier: deviceIdentifier,
            desiredKeys: nil,
            completion: { [weak self] zoneResults in
            guard let self,
                  synchronizationRunID == runID,
                  beginRunCallback(for: attemptID) else { return }
            defer { endRunCallback() }

            try Task.checkCancellation()
            for (zoneID, zoneResult) in zoneResults {
                try Task.checkCancellation()
                if let error = zoneResult.error {
                    if isZoneNotFoundOrDeletedError(error) {
                        try await notifyProviderForDeletedZoneIDs(
                            [zoneID],
                            attemptID: attemptID
                        )
                        try await revalidateActiveRunContext(for: attemptID)
                        guard synchronizationRunID == runID else {
                            throw CancellationError()
                        }
                        continue
                    } else {
                        throw error
                    }
                }
                guard let adapter = modelAdapterDictionary[zoneID] else { continue }
                for record in zoneResult.downloadedRecords {
                    changeRequestProcessor.addFetchedChangeRequest(
                        ChangeRequest(
                            downloadedRecord: record,
                            deletedRecordID: nil,
                            adapter: adapter,
                            runID: runID
                        )
                    )
                }
                for recordID in zoneResult.deletedRecordIDs {
                    changeRequestProcessor.addFetchedChangeRequest(
                        ChangeRequest(
                            downloadedRecord: nil,
                            deletedRecordID: recordID,
                            adapter: adapter,
                            runID: runID
                        )
                    )
                }
                if !zoneResult.downloadedRecords.isEmpty
                    || !zoneResult.deletedRecordIDs.isEmpty {
                    synchronizationDrainDidImportChanges = true
                }
                if !zoneResult.downloadedRecords.isEmpty {
                    logger.info("QSCloudKitSynchronizer >> Downloaded \(zoneResult.downloadedRecords.count) changed records from zone \(zoneID.zoneName)")
                }
                if !zoneResult.deletedRecordIDs.isEmpty {
                    logger.info("QSCloudKitSynchronizer >> Downloaded \(zoneResult.deletedRecordIDs.count) deleted record IDs from zone \(zoneID.zoneName)")
                }
                try await revalidateActiveRunContext(for: attemptID)
                try await changeRequestProcessor.finishProcessing(for: adapter)
                try await revalidateActiveRunContext(for: attemptID)
                guard synchronizationRunID == runID else {
                    throw CancellationError()
                }
                if let firstError = changeRequestProcessor.getErrors().first {
                    changeRequestProcessor.clearErrors()
                    throw firstError
                }
                try await adapter.persistImportedChanges()
                try await revalidateActiveRunContext(for: attemptID)
                guard synchronizationRunID == runID else {
                    throw CancellationError()
                }
                try await adapter.saveToken(zoneResult.serverChangeToken)
                try await revalidateActiveRunContext(for: attemptID)
                guard synchronizationRunID == runID else {
                    throw CancellationError()
                }
                activeZoneTokens[zoneID] = zoneResult.serverChangeToken
            }
            changeRequestProcessor.clearErrors()
        }, didFinishPages: { [weak self] in
            guard let self else { throw CancellationError() }
            // Page publication and terminal continuation are separate
            // operation-owned async callbacks. Keep both inside the run barrier.
            guard synchronizationAttemptID == attemptID,
                  synchronizationRunID == runID,
                  beginRunCallback(for: attemptID) else {
                throw CancellationError()
            }
            defer { endRunCallback() }
            try await revalidateActiveRunContext(for: attemptID)
            try await completion(nil)
        })
        runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func processFetchedChanges() async throws {
        let attemptID = synchronizationAttemptID
        guard !cancelSync else {
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: SyncError.cancelled)
            return
        }
        
        for adapter in modelAdapters {
            try Task.checkCancellation()
            try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil)
            try await saveActiveTokenIfNeeded(for: adapter)
        }
    }
}

// MARK: - Upload changes

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func uploadChanges() async throws {
        let attemptID = synchronizationAttemptID
        logger.info("QSCloudKitSynchronizer >> Upload changes...")
        //        debugPrint("# uploadChanges()")
        guard !cancelSync else {
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: SyncError.cancelled)
            return
        }
        try Task.checkCancellation()
        
        postNotification(.SynchronizerWillUploadChanges)
        
        try await uploadChanges() { [weak self] (error) in
            try Task.checkCancellation()
            guard let self,
                  synchronizationAttemptID == attemptID else { return }
            
            if let error = error as? NSError {
                if isZoneNotFoundOrDeletedError(error) {
                    for adapter in modelAdapters {
                        try await revalidateActiveRunContext(for: attemptID)
                        activeZoneTokens[adapter.recordZoneID] = nil
                        try await adapter.saveToken(nil)
                        try await revalidateActiveRunContext(for: attemptID)
                    }
                }
                if shouldRetryUpload(for: error) {
                    //                    print("# uploadChanges() failed, retrying via fetchChanges()")
                    uploadRetries += 1
                    logger.info("QSCloudKitSynchronizer >> Retrying upload due to error \(error.description.prefix(200)), beginning with fetching changes...")
                    await fetchChanges()
                } else {
                    await failSynchronization(error: error)
                }
            } else {
                // The database token is a commit barrier for the zone changes it
                // announced. Persist it only after every downloaded zone change
                // has been applied and its zone token has been saved.
                try await revalidateActiveRunContext(for: attemptID)
                storedDatabaseToken = serverChangeToken
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
    func uploadChanges(
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        do {
            for adapter in modelAdapters {
                try Task.checkCancellation()
                try await synchronizeAdapter(adapter)
            }
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }
    
    @BigSyncBackgroundActor
    func setupZoneAndUploadRecords(
        adapter: ModelAdapter,
        restrictedToEntityType: String? = nil,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        try await setupRecordZoneIfNeeded(
            adapter: adapter,
            attemptID: attemptID
        ) { [weak self] error in
            guard let self else {
                try await completion(CancellationError())
                return
            }
            do {
                try checkSynchronizationAttempt(attemptID)
            } catch {
                try await completion(error)
                return
            }
            guard error == nil else {
                try await completion(error)
                return
            }
            do {
                try await uploadRecords(
                    adapter: adapter,
                    restrictedToEntityType: restrictedToEntityType,
                    attemptID: attemptID,
                    completion: { [weak self] (error) in
                        guard let self else {
                            try await completion(CancellationError())
                            return
                        }
                        do {
                            try checkSynchronizationAttempt(attemptID)
                        } catch {
                            try await completion(error)
                            return
                        }
                        if error == nil {
                            increaseBatchSize()
                        }
                        try await completion(error)
                    }
                )
            } catch {
                try await completion(error)
            }
        }
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneIfNeeded(
        adapter: ModelAdapter,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        let shouldSetup = try await needsZoneSetup(adapter: adapter)
        try checkSynchronizationAttempt(attemptID)
        guard shouldSetup else {
            try await completion(nil)
            return
        }
        
        try await setupRecordZoneID(
            adapter.recordZoneID,
            attemptID: attemptID,
            completion: completion
        )
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneID(
        _ zoneID: CKRecordZone.ID,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        // Validate after the preceding adapter await and immediately before
        // starting an account-routed CloudKit operation.
        try await revalidateActiveRunContext(for: attemptID)
        database.fetch(withRecordZoneID: zoneID) { [weak self] (zone, error) in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self,
                      beginRunCallback(for: attemptID) else {
                    try await completion(CancellationError())
                    return
                }
                defer { endRunCallback() }
                do {
                    try await revalidateActiveRunContext(for: attemptID)
                } catch {
                    try await completion(error)
                    return
                }
                if isZoneNotFoundOrDeletedError(error) {
                    let newZone = CKRecordZone(zoneID: zoneID)
                    do {
                        // No suspension occurs between this validation and save().
                        try await revalidateActiveRunContext(for: attemptID)
                    } catch {
                        try await completion(error)
                        return
                    }
                    database.save(zone: newZone, completionHandler: { [weak self] (zone, error) in
                        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                            guard let self,
                                  beginRunCallback(for: attemptID) else {
                                try await completion(CancellationError())
                                return
                            }
                            defer { endRunCallback() }
                            do {
                                try await revalidateActiveRunContext(for: attemptID)
                            } catch {
                                try await completion(error)
                                return
                            }
                            if error == nil && zone != nil {
                                //                        debugPrint("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                                logger.info("QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)")
                            }
                            try await completion(error)
                        }
                    })
                } else {
                    try await completion(error)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    func uploadRecords(
        adapter: ModelAdapter,
        restrictedToEntityType: String? = nil,
        attemptID: UUID,
        completion: @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        
        let requestedBatchSize = batchSize
        let preparedUploads = try await adapter.preparedRecordsToUpload(
            limit: requestedBatchSize,
            restrictedToEntityType: restrictedToEntityType
        )
        let records = preparedUploads.map(\.record)
        try checkSynchronizationAttempt(attemptID)
        let uploadGenerations = preparedUploads.reduce(into: [String: String]()) {
            guard let generation = $1.generation else { return }
            $0[$1.record.recordID.recordName] = generation
        }
        let recordCount = records.count
        guard recordCount > 0 else { try await completion(nil); return }
        
        logger.info("QSCloudKitSynchronizer >> Uploading \(recordCount) records to \(adapter.recordZoneID)")
//        logger.info("QSCloudKitSynchronizer >> Uploading records: \(records.map { $0.recordID.recordName } .joined(separator: " "))")
        
        if !didNotifyUpload.contains(adapter.recordZoneID) {
            didNotifyUpload.insert(adapter.recordZoneID)
            delegate?.synchronizerWillUploadChanges(self, to: adapter.recordZoneID)
        }
        
        //Add metadata: device UUID and model version
        addMetadata(to: records)
        try checkSynchronizationAttempt(attemptID)
        // Prepared Realm work can suspend. Revalidate the account immediately
        // before starting the mutating operation.
        try await revalidateActiveRunContext(for: attemptID)
        //        debugPrint("## Upload", records.map {($0.recordID, $0) })
        let modifyRecordsOperation = ModifyRecordsOperation(
            database: database,
            records: records,
            recordIDsToDelete: nil
        ) { [weak self] (savedRecords, _, conflictedRecords, recordIDsMissingOnServer, operationError) in
            //            debugPrint("# uploadRecords, inside operation callback...", records.count)
            guard let self else { return }
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                //                debugPrint("# uploadRecords, inside operation callback Task...", records.count, "saved", savedRecords?.count, "del", deleted?.count, "conflicted", conflicted.count, operationError)
                guard let self else { return }
                guard beginRunCallback(for: attemptID) else {
                    try? await completion(CancellationError())
                    return
                }
                defer { endRunCallback() }
                @BigSyncBackgroundActor
                func finish(_ resultError: Error?) async {
                    do {
                        try await completion(resultError)
                    } catch {
                        guard synchronizationAttemptID == attemptID else { return }
                        await failSynchronization(error: error)
                    }
                }
                do {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                if let savedRecords, !savedRecords.isEmpty {
                    //                    debugPrint("QSCloudKitSynchronizer >> Uploaded \(savedRecords?.count ?? 0) records")
                    logger.info("QSCloudKitSynchronizer >> Uploaded \(savedRecords.count) records")
//                    logger.info("QSCloudKitSynchronizer >> Uploaded records: \(savedRecords.map { ($0.recordID.recordName, $0.debugDescription) })")
                    //                    logger.info("QSCloudKitSynchronizer >> Uploaded records: \((savedRecords?.map { $0.recordID.recordName } ?? []).joined(separator: " "))")

                    try await revalidateActiveRunContext(for: attemptID)
                    try await adapter.didUpload(
                        savedRecords: savedRecords,
                        matchingGenerations: uploadGenerations
                    )
                    try await revalidateActiveRunContext(for: attemptID)
                }
                
                try await revalidateActiveRunContext(for: attemptID)
                if let error = operationError as? NSError {
                    if !recordIDsMissingOnServer.isEmpty {
                        try await revalidateActiveRunContext(for: attemptID)
                        try await adapter.requeueMissingServerRecords(
                            Array(recordIDsMissingOnServer),
                            matchingPreparedGenerations: uploadGenerations
                        )
                        try await revalidateActiveRunContext(for: attemptID)
                    }

                    let errorsByItemID =
                        error.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError] ?? [:]
                    var resolvedRecordsByID = Dictionary(
                        uniqueKeysWithValues: conflictedRecords.map { ($0.recordID, $0) }
                    )
                    for (recordID, itemError) in errorsByItemID
                    where itemError.code == CKError.serverRecordChanged.rawValue {
                        if let serverRecord =
                            itemError.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord {
                            resolvedRecordsByID[recordID] = serverRecord
                        }
                    }

                    let handledRecordIDs = recordIDsMissingOnServer
                        .union(resolvedRecordsByID.keys)
                    let unresolvedItemErrors = errorsByItemID.filter {
                        !handledRecordIDs.contains($0.key)
                    }

                    guard unresolvedItemErrors.isEmpty else {
                        if self.isLimitExceededError(error) {
                            reduceBatchSize()
                        }
                        await finish(error)
                        return
                    }

                    if !resolvedRecordsByID.isEmpty {
                        do {
                            try await revalidateActiveRunContext(for: attemptID)
                            try await adapter.saveChanges(
                                in: Array(resolvedRecordsByID.values),
                                forceSave: true
                            )
                            try await revalidateActiveRunContext(for: attemptID)
                            try await adapter.persistImportedChanges()
                            try await revalidateActiveRunContext(for: attemptID)
                        } catch {
                            logger.warning(
                                "QSCloudKitSynchronizer >> Failed to resolve conflicted records: \(error)"
                            )
                            await finish(error)
                            return
                        }
                    }

                    if !handledRecordIDs.isEmpty {
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        try await uploadRecords(
                            adapter: adapter,
                            restrictedToEntityType: restrictedToEntityType,
                            attemptID: attemptID,
                            completion: completion
                        )
                        return
                    } else {
                        if self.isLimitExceededError(error) {
                            reduceBatchSize()
                        }
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        await finish(error)
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
                    
                    try await uploadRecords(
                        adapter: adapter,
                        restrictedToEntityType: restrictedToEntityType,
                        attemptID: attemptID,
                        completion: completion
                    )
                } else {
                    await finish(nil)
                }
                } catch {
                    // Acknowledgement, account validation, and recursive-batch
                    // errors occur in an unstructured callback
                    // Task. Always resolve the attempt bridge with that error.
                    await finish(error)
                }
                //                }
            }
        }
        
        runOperation(modifyRecordsOperation)
    }
    
    @BigSyncBackgroundActor
    func uploadDeletions(
        adapter: ModelAdapter,
        restrictedToEntityType: String? = nil,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        let preparedDeletions = try await adapter.preparedRecordDeletions(
            limit: batchSize,
            restrictedToEntityType: restrictedToEntityType
        )
        let recordIDs = preparedDeletions.map(\.recordID)
        try checkSynchronizationAttempt(attemptID)
        let deletionGenerations = preparedDeletions.reduce(into: [String: String]()) {
            guard let generation = $1.generation else { return }
            $0[$1.recordID.recordName] = generation
        }
        let recordCount = recordIDs.count
        let requestedBatchSize = batchSize
        
        guard recordCount > 0 else {
            try await completion(nil)
            return
        }
        // Deletion preparation can suspend. Revalidate the account at the final
        // boundary before the CloudKit mutation starts.
        try await revalidateActiveRunContext(for: attemptID)
        let modifyRecordsOperation = ModifyRecordsOperation(
            database: database,
            records: nil,
            recordIDsToDelete: recordIDs
        ) { @Sendable [weak self] _, deletedRecordIDs, _, recordIDsMissingOnServer, operationError in
            guard let self else { return }
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                guard beginRunCallback(for: attemptID) else {
                    try? await completion(CancellationError())
                    return
                }
                defer { endRunCallback() }
                @BigSyncBackgroundActor
                func finish(_ resultError: Error?) async {
                    do {
                        try await completion(resultError)
                    } catch {
                        guard synchronizationAttemptID == attemptID else { return }
                        await failSynchronization(error: error)
                    }
                }
                do {
                    let acknowledgedRecordIDSet = Set(deletedRecordIDs ?? [])
                        .union(recordIDsMissingOnServer)
                    let acknowledgedRecordIDs = recordIDs.filter {
                        acknowledgedRecordIDSet.contains($0)
                    }
                    logger.info(
                        "QSCloudKitSynchronizer >> Deleted or confirmed absent \(acknowledgedRecordIDs.count) records"
                    )
                    try await revalidateActiveRunContext(for: attemptID)
                    try await adapter.didDelete(
                        recordIDs: acknowledgedRecordIDs,
                        matchingGenerations: deletionGenerations
                    )
                    try await revalidateActiveRunContext(for: attemptID)

                    let allDeletionsAcknowledged =
                        acknowledgedRecordIDs.count == recordCount
                    if let error = operationError, !allDeletionsAcknowledged {
                        if isLimitExceededError(error as NSError) {
                            reduceBatchSize()
                        }
                        await finish(error)
                    } else if recordCount >= requestedBatchSize {
                        try await uploadDeletions(
                            adapter: adapter,
                            restrictedToEntityType: restrictedToEntityType,
                            attemptID: attemptID,
                            completion: completion
                        )
                    } else {
                        await finish(nil)
                    }
                } catch {
                    // Recursive deletion preparation and acknowledgement
                    // failures must resolve the attempt bridge.
                    await finish(error)
                }
            }
        }
        
        runOperation(modifyRecordsOperation)
    }

    @BigSyncBackgroundActor
    func synchronizeAdapter(_ adapter: ModelAdapter) async throws {
        for priorityEntityType in adapter.priorityEntityTypeNames {
            try Task.checkCancellation()
            try await runSyncPhase(for: adapter, restrictedToEntityType: priorityEntityType)
        }

        try Task.checkCancellation()
        try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil)
        try await saveActiveTokenIfNeeded(for: adapter)
        try await uploadRecordsIfNeeded(adapter: adapter, restrictedToEntityType: nil)
        try await uploadDeletionsIfNeeded(adapter: adapter, restrictedToEntityType: nil)
    }

    @BigSyncBackgroundActor
    func runSyncPhase(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: restrictedEntityType)
        try await uploadRecordsIfNeeded(adapter: adapter, restrictedToEntityType: restrictedEntityType)
        try await uploadDeletionsIfNeeded(adapter: adapter, restrictedToEntityType: restrictedEntityType)
    }

    @BigSyncBackgroundActor
    func runFetchedChangesPhase(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let changeRequestProcessor = changeRequestProcessor
        try await changeRequestProcessor.finishProcessing(
            for: adapter,
            restrictedToEntityType: restrictedEntityType
        )
        if let firstError = changeRequestProcessor.getErrors().first {
            changeRequestProcessor.clearErrors()
            throw firstError
        }
        do {
            try await adapter.persistImportedChanges()
        } catch {
            changeRequestProcessor.clearErrors()
            throw error
        }
        changeRequestProcessor.clearErrors()
    }

    @BigSyncBackgroundActor
    func saveActiveTokenIfNeeded(for adapter: ModelAdapter) async throws {
        if let token = activeZoneToken(zoneID: adapter.recordZoneID) {
            try await revalidateActiveRunContext(
                for: synchronizationAttemptID
            )
            try await adapter.saveToken(token)
        }
    }

    @BigSyncBackgroundActor
    func uploadRecordsIfNeeded(
        adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let attemptID = synchronizationAttemptID
        try await awaitAttemptCallback(for: attemptID) { completion in
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else {
                    completion(.failure(CancellationError()))
                    return
                }
                do {
                    try checkSynchronizationAttempt(attemptID)
                    try await setupZoneAndUploadRecords(
                        adapter: adapter,
                        restrictedToEntityType: restrictedEntityType,
                        attemptID: attemptID
                    ) { error in
                        if let error {
                            completion(.failure(error))
                        } else {
                            completion(.success(()))
                        }
                    }
                } catch {
                    completion(.failure(error))
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func uploadDeletionsIfNeeded(
        adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let attemptID = synchronizationAttemptID
        try await awaitAttemptCallback(for: attemptID) { completion in
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else {
                    completion(.failure(CancellationError()))
                    return
                }
                do {
                    try checkSynchronizationAttempt(attemptID)
                    try await uploadDeletions(
                        adapter: adapter,
                        restrictedToEntityType: restrictedEntityType,
                        attemptID: attemptID
                    ) { error in
                        if let error {
                            completion(.failure(error))
                        } else {
                            completion(.success(()))
                        }
                    }
                } catch {
                    completion(.failure(error))
                }
            }
        }
    }
    
    // MARK: -
    
    @BigSyncBackgroundActor
    func updateTokens() {
        //        debugPrint("# updateTokens() (calls FetchDatabaseChangesOperation)")
        let attemptID = synchronizationAttemptID
        let operation = FetchDatabaseChangesOperation(database: database, databaseToken: serverChangeToken) { (databaseToken, changedZoneIDs, deletedZoneIDs) in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self,
                      beginRunCallback(for: attemptID) else { return }
                defer { endRunCallback() }
                do {
                
                guard !cancelSync else {
                    await failSynchronization(error: SyncError.cancelled)
                    return
                }
                
                try await revalidateActiveRunContext(for: attemptID)
                try await notifyProviderForDeletedZoneIDs(
                    deletedZoneIDs,
                    attemptID: attemptID
                )
                try await revalidateActiveRunContext(for: attemptID)
                if changedZoneIDs.count > 0 {
                    let zoneIDs = try await loadTokens(for: changedZoneIDs)
                    try await revalidateActiveRunContext(for: attemptID)
                    await updateServerToken(for: zoneIDs, completion: { [weak self] result in
                        guard let self = self,
                              synchronizationAttemptID == attemptID else { return }
                        do {
                            // This operation-owned callback can reenter after
                            // cancellation. Fence its first publication too.
                            try await revalidateActiveRunContext(for: attemptID)
                            switch result {
                            case .success(true):
                                await performSynchronization()
                            case .success(false):
                                storedDatabaseToken = databaseToken
                                await changesFinishedSynchronizing()
                            case .failure(let error):
                                await failSynchronization(error: error)
                            }
                        } catch {
                            guard synchronizationAttemptID == attemptID else { return }
                            await failSynchronization(error: error)
                        }
                    })
                } else {
                    do {
                        try await revalidateActiveRunContext(for: attemptID)
                        storedDatabaseToken = databaseToken
                        await changesFinishedSynchronizing()
                    } catch {
                        await failSynchronization(error: error)
                    }
                }
                } catch {
                    // This callback is not awaited by FetchDatabaseChangesOperation.
                    // Convert actor-task failures
                    // into synchronization failure instead of abandoning the run.
                    guard synchronizationAttemptID == attemptID else { return }
                    await failSynchronization(error: error)
                }
            }
        }
        runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func updateServerToken(
        for recordZoneIDs: [CKRecordZone.ID],
        completion: @escaping (Result<Bool, Error>) async -> ()
    ) async {
        // Database changes can mention zones this synchronizer does not own
        // (for example, the obsolete zone during a replacement migration).
        // There is no token work to perform after filtering those zones, and
        // constructing an empty CKFetchRecordZoneChangesOperation needlessly
        // relies on CloudKit invoking a terminal callback for an empty input.
        guard !recordZoneIDs.isEmpty else {
            await completion(.success(false))
            return
        }

        // If we found a new record zone at this point then needsToFetchChanges=true
        var hasAllTokens = true
        for zoneID in recordZoneIDs {
            if activeZoneTokens[zoneID] == nil {
                hasAllTokens = false
            }
        }
        guard hasAllTokens else {
            await completion(.success(true))
            return
        }
        
//        logger.info("QSCloudKitSynchronizer >> Update server token....")
        let attemptID = synchronizationAttemptID
        let runID = synchronizationRunID
        var zonesNeedingRefetch = Set<CKRecordZone.ID>()
        let operation = FetchZoneChangesOperation(
            database: database,
            zoneIDs: recordZoneIDs,
            zoneChangeTokens: activeZoneTokens,
            modelVersion: compatibilityVersion,
            ignoreDeviceIdentifier: deviceIdentifier,
            desiredKeys: [
                "recordID",
                cloudKitSynchronizerDeviceUUIDKey
            ],
            completion: { @BigSyncBackgroundActor [weak self] zoneResults in
            guard let self else { throw CancellationError() }
            // This operation callback can suspend in Realm token publication.
            // A newer run must not clear adapter cancellation until it exits.
            guard synchronizationAttemptID == attemptID,
                  synchronizationRunID == runID,
                  beginRunCallback(for: attemptID) else {
                throw CancellationError()
            }
            defer { endRunCallback() }

            try await revalidateActiveRunContext(for: attemptID)
            for (zoneID, result) in zoneResults {
                if let error = result.error {
                    throw error
                }
                let adapter = modelAdapterDictionary[zoneID]
                if result.downloadedRecords.count > 0 || result.deletedRecordIDs.count > 0 {
                    zonesNeedingRefetch.insert(zoneID)
                } else if !zonesNeedingRefetch.contains(zoneID) {
                    try await revalidateActiveRunContext(for: attemptID)
                    try await adapter?.saveToken(result.serverChangeToken)
                    try await revalidateActiveRunContext(for: attemptID)
                    activeZoneTokens[zoneID] = result.serverChangeToken
                }
                
            }
        }, didFinishPages: { [weak self] in
            guard let self else { throw CancellationError() }
            // The final completion is a second async callback owned by the same
            // operation and must participate in the same run barrier.
            guard synchronizationAttemptID == attemptID,
                  synchronizationRunID == runID,
                  beginRunCallback(for: attemptID) else {
                throw CancellationError()
            }
            defer { endRunCallback() }
            try await revalidateActiveRunContext(for: attemptID)
            await completion(.success(!zonesNeedingRefetch.isEmpty))
        })
        runOperation(operation)
    }
    
    @BigSyncBackgroundActor
    func reduceBatchSize() {
        self.batchSize = max(1, Int((Double(self.batchSize) / 2.75).rounded()))
    }
    
    @BigSyncBackgroundActor
    func increaseBatchSize() {
        if self.batchSize < CloudKitSynchronizer.maxBatchSize {
            //            self.batchSize = min(CloudKitSynchronizer.maxBatchSize, self.batchSize + ((CloudKitSynchronizer.maxBatchSize - CloudKitSynchronizer.defaultInitialBatchSize) / 5))
            self.batchSize = min(CloudKitSynchronizer.maxBatchSize, max(batchSize + 1, Int((Double(self.batchSize) * 1.12).rounded())))
        }
    }
}
