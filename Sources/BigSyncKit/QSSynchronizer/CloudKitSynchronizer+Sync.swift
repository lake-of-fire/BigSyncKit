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
        return nserror.domain == CKErrorDomain
            && (
                nserror.code == CKError.zoneNotFound.rawValue
                    || nserror.code == CKError.userDeletedZone.rawValue
            )
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
        guard beginRunCallback(for: attemptID) else { return }
        defer { endRunCallback() }
        do {
            reportProgress("terminal-tail-start")
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
            if try adaptersHavePendingChangesAtTerminalBoundary() {
                synchronizationRequestedWhileRunning = true
            }
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
        
//        logger.info("QSCloudKitSynchronizer >> Finished synchronization batch")
        if synchronizationRequestedWhileRunning {
            restartSynchronizationForTerminalWork()
            return
        }
        // The migration may finish only after final import forwarding and the
        // terminal pending-state check have proven this drain quiescent.
        do {
            if let context = activeRunContext {
                try await finishChangeFeedMigrationIfNeeded(context: context)
                try await revalidateRunContext(context)
            }
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
        let consumedServerBoundaryIdentifier: String?
        do {
            consumedServerBoundaryIdentifier = try
                currentConsumedServerBoundaryIdentifier(for: activeRunContext)
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
        guard let terminalContext = activeRunContext else {
            await failSynchronization(error: CancellationError())
            return
        }
        var publicationBlockers = [DomainBlocker]()
        var domainPublicationScopeIdentifier: String?
        var inboundIdentityDeliveries = [
            (adapter: ModelAdapter, batch: CommittedInboundIdentityBatch)
        ]()
        if let domainPrepublicationHandler {
            do {
                for adapter in modelAdapters {
                    if let batch = try adapter
                        .pendingCommittedInboundIdentityBatch() {
                        inboundIdentityDeliveries.append((adapter, batch))
                    }
                }
                publicationBlockers.append(contentsOf:
                    try await domainPrepublicationHandler(
                    PrepublicationBoundaryContext(
                        context: terminalContext,
                        consumedServerBoundaryIdentifier:
                            consumedServerBoundaryIdentifier,
                        didImportChanges:
                            synchronizationDrainDidImportChanges,
                        committedInboundIdentities: Array(Set(
                            inboundIdentityDeliveries.flatMap {
                                $0.batch.identities
                            }
                        )).sorted {
                            ($0.entityType, $0.recordName)
                                < ($1.entityType, $1.recordName)
                        }
                    )
                ))
                try await revalidateRunContext(terminalContext)
                for delivery in inboundIdentityDeliveries {
                    try await delivery.adapter
                        .acknowledgeCommittedInboundIdentityBatch(
                            deliveryID: delivery.batch.deliveryID
                        )
                    try await revalidateRunContext(terminalContext)
                }
            } catch is CancellationError {
                return
            } catch {
                await failSynchronization(error: error)
                return
            }
        }

        do {
            for adapter in modelAdapters {
                publicationBlockers.append(contentsOf:
                    try await adapter.semanticPublicationBlockers()
                )
            }
            try await revalidateRunContext(terminalContext)
            if publicationBlockers.isEmpty,
               let provider = domainPublicationScopeIdentifierProvider {
                let scope = try await provider()
                guard scope?.isEmpty != true else {
                    throw DurableKeyValueStoreError.mutationNotDurable
                }
                domainPublicationScopeIdentifier = scope
                try await revalidateRunContext(terminalContext)
            }
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }

        do {
            // Application reconciliation is allowed to create upload work.
            // Repeat the exact terminal journal predicate after it returns;
            // any new generation is drained before a receipt can exist.
            if try adaptersHavePendingChangesAtTerminalBoundary() {
                synchronizationRequestedWhileRunning = true
            }
            try await revalidateRunContext(terminalContext)
            if try currentConsumedServerBoundaryIdentifier(
                for: terminalContext
            ) != consumedServerBoundaryIdentifier {
                synchronizationRequestedWhileRunning = true
            }
        } catch is CancellationError {
            return
        } catch {
            await failSynchronization(error: error)
            return
        }
        if synchronizationRequestedWhileRunning {
            restartSynchronizationForTerminalWork()
            return
        }

        // A semantic blocker is publishable only after the same exact
        // journal and cursor predicates required by a success receipt are
        // stable. Domain reconciliation may both create upload work and
        // report a blocker; drain that work first so `.blocked` describes the
        // terminal transport boundary rather than an obsolete intermediate
        // one.
        if !publicationBlockers.isEmpty {
            do {
                try recordSyncHealth(
                    .semanticBlocked,
                    context: terminalContext
                )
                try keyValueStore.bigSyncValidateDurability()
            } catch is CancellationError {
                return
            } catch {
                await failSynchronization(error: error)
                return
            }
            activeReceiptAuthorizationID = nil
            finishSynchronizationDrain(
                with: .success(SynchronizationResult(
                    didImportChanges:
                        synchronizationDrainDidImportChanges,
                    publicationState: .blocked(publicationBlockers)
                ))
            )
            // Keep the terminal run owner until the drain waiters have been
            // resumed and shared drain state has been closed. A resumed caller
            // may immediately request another run; releasing ownership first
            // would let that run race the old drain's cleanup.
            syncing = false
            synchronizationTask = nil
            return
        }

        // Only now authorize and publish the receipt. A notification observer
        // may request a fresh synchronization, so snapshot the result before
        // releasing run ownership.
        let authorizationID = UUID()
        activeReceiptAuthorizationID = authorizationID
        let receipt = SynchronizationReceipt(
            context: terminalContext,
            issuerID: synchronizationReceiptIssuerID,
            authorizationID: authorizationID,
            consumedServerBoundaryIdentifier:
                consumedServerBoundaryIdentifier
        )
        let result = SynchronizationResult(
            didImportChanges: synchronizationDrainDidImportChanges,
            receipt: receipt
        )
        consecutiveTransientCloudKitFailures = 0
        clearPersistedTransientRetryState()
        if let domainPublicationScopeIdentifier,
           let consumedServerBoundaryIdentifier,
           let adapter = modelAdapters.first {
            do {
                guard let changeFeedEpoch = try adapter.changeFeedEpoch()
                else {
                    throw DurableKeyValueStoreError.mutationNotDurable
                }
                try persistDurablePublicationEvidence(
                    domainScopeIdentifier:
                        domainPublicationScopeIdentifier,
                    context: terminalContext,
                    consumedServerBoundaryIdentifier:
                        consumedServerBoundaryIdentifier,
                    changeFeedEpoch: changeFeedEpoch
                )
            } catch is CancellationError {
                return
            } catch {
                await failSynchronization(error: error)
                return
            }
        }
        if let context = activeRunContext {
            do {
                try recordSyncHealth(.succeeded, context: context)
            } catch is CancellationError {
                return
            } catch {
                await failSynchronization(error: error)
                return
            }
        }
        do {
            // Cursor/account/retry setters retain a mutation failure in the
            // production file store. Revalidate after every terminal write so
            // an attempt can never mint a success receipt when any critical
            // local-state commit failed.
            try keyValueStore.bigSyncValidateDurability()
        } catch {
            await failSynchronization(error: error)
            return
        }
        reportProgress("terminal-receipt")
        finishSynchronizationDrain(with: .success(result))
        // See the blocked path above: close the logical drain before allowing
        // a new synchronization to become the owner of its task/state.
        syncing = false
        synchronizationTask = nil
        postNotification(.SynchronizerDidSynchronize)
        delegate?.synchronizerDidSync(self)
    }

    @BigSyncBackgroundActor
    func adaptersHavePendingChangesAtTerminalBoundary() throws
        -> Bool {
        for adapter in modelAdapters {
            if let terminalStateAdapter =
                adapter as? TerminalSynchronizationStateModelAdapter {
                if try terminalStateAdapter
                    .hasPendingChangesAtTerminalBoundary() {
                    return true
                }
            } else if adapter.hasChanges {
                return true
            }
        }
        return false
    }

    @BigSyncBackgroundActor
    private func currentConsumedServerBoundaryIdentifier(
        for context: RunContext?
    ) throws -> String? {
        guard let context, let adapter = modelAdapters.first else {
            return nil
        }
        return try adapter.consumedServerBoundaryIdentifier(
            accountScopeIdentifier: context.accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                context.replicaBindingGenerationIdentifier,
            containerIdentifier: containerIdentifier,
            databaseScope: database.databaseScope
        )
    }

    @BigSyncBackgroundActor
    private func restartSynchronizationForTerminalWork() {
        synchronizationRequestedWhileRunning = false
        syncing = false
        synchronizationTask = nil
        activeReceiptAuthorizationID = nil
        beginSynchronization()
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
        var terminalHealthCategory = syncHealthCategory(for: error)
        let terminalZoneDeletionKind = (error as? ChangeFeedMigrationError)?
            .deletionKind

        if let migrationError = error as? ChangeFeedMigrationError,
           migrationError.deletionKind == .encryptedDataReset {
            // The database-history event already persisted a dedicated
            // recovery request. Retry immediately; the next attempt performs
            // the account-fenced journal rebuild before any upload.
            logger.info(
                "QSCloudKitSynchronizer >> Recovering after CloudKit encrypted-data reset..."
            )
            shouldRetry = true
            retryDelay = 0
        } else if let error = error as? CloudKitSynchronizer.SyncError {
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
                logger.info("QSCloudKitSynchronizer >> Change token expired, requesting a fenced server-first tracking rebuild...")
                var recoveryRequestIsDurable = false
                if let context = activeRunContext {
                    do {
                        try requestChangeFeedRecovery(context: context)
                        recoveryRequestIsDurable = true
                    } catch {
                        logger.error(
                            "QSCloudKitSynchronizer >> Could not durably request change-token recovery: \(error)"
                        )
                    }
                }
                if recoveryRequestIsDurable {
                    do {
                        try self.resetDatabaseToken()
                        for adapter in modelAdapters {
                            try await adapter.saveToken(nil)
                        }
                        shouldRetry = true
                    } catch {
                        logger.error("QSCloudKitSynchronizer >> Failed to clear expired adapter token: \(error)")
                    }
                }
            } else if codes.contains(.notAuthenticated) {
                logger.error("QSCloudKitSynchronizer >> Not Authenticated. Aborting sync")
                changeRequestProcessor.reset()
                cancelledDueToUnauthentication = true
                accountValidationRequired = true
                accountScopeAuthorityFence.poison(
                    requiresGenerationRotation: false
                )
                terminalHealthCategory = .notAuthenticated
            } else if codes.contains(.accountTemporarilyUnavailable) {
                // Apple explicitly requires waiting for CKAccountChanged and
                // rechecking account availability. Do not enqueue another
                // database/zone/record operation on the generic retry timer.
                logger.info(
                    "QSCloudKitSynchronizer >> iCloud account is temporarily unavailable; waiting for account status to change"
                )
                changeRequestProcessor.reset()
                accountValidationRequired = true
                accountScopeAuthorityFence.poison(
                    requiresGenerationRotation: false
                )
                clearPersistedTransientRetryState()
                terminalHealthCategory = .accountTemporarilyUnavailable
            } else if !codes.isDisjoint(with: [
                .serviceUnavailable,
                .requestRateLimited,
                .zoneBusy,
                .networkFailure,
                .networkUnavailable,
            ]) {
                let requestedDelays = errors.compactMap {
                    ($0.userInfo[CKErrorRetryAfterKey] as? NSNumber)?.doubleValue
                }.filter { $0.isFinite && $0 >= 0 }
                consecutiveTransientCloudKitFailures += 1
                retryDelay = CloudKitRetryBackoff.delay(
                    serverMinimum: requestedDelays.max(),
                    consecutiveFailures: consecutiveTransientCloudKitFailures
                )
                if let context = activeRunContext {
                    persistTransientRetryState(
                        context: context,
                        notBefore: Date().addingTimeInterval(retryDelay),
                        consecutiveFailures:
                            consecutiveTransientCloudKitFailures
                    )
                }
                logger.warning(
                    "QSCloudKitSynchronizer >> Transient CloudKit error. Retrying in \(retryDelay.rounded()) seconds."
                )
                reduceBatchSize()
                shouldRetry = true
            } else {
                logger.error("QSCloudKitSynchronizer >> Error: \(topLevelError)")
            }
        } else if error as? CloudKitChangeFeedError == .corruptCursor {
            logger.warning(
                "QSCloudKitSynchronizer >> Persisted CloudKit cursor was corrupt; requesting a fenced server-first tracking rebuild."
            )
            var recoveryRequestIsDurable = false
            if let context = activeRunContext {
                do {
                    try requestChangeFeedRecovery(context: context)
                    recoveryRequestIsDurable = true
                } catch {
                    logger.error(
                        "QSCloudKitSynchronizer >> Could not durably request corrupt-cursor recovery: \(error)"
                    )
                }
            }
            if recoveryRequestIsDurable {
                do {
                    try resetDatabaseToken()
                    for adapter in modelAdapters {
                        try await adapter.saveToken(nil)
                    }
                    shouldRetry = true
                } catch {
                    logger.error(
                        "QSCloudKitSynchronizer >> Failed to clear corrupt adapter cursor: \(error)"
                    )
                }
            }
        }

        if error is CancellationError {
            logger.info("QSCloudKitSynchronizer >> Synchronization canceled, not retrying")
            shouldRetry = false
        }

        syncing = shouldRetry && !cancelSync
        synchronizationTask = nil

        if let context = activeRunContext {
            do {
                if shouldRetry, !cancelSync {
                    try recordSyncHealth(
                        .transientRetry,
                        context: context,
                        retryNotBefore: Date().addingTimeInterval(retryDelay),
                        terminalZoneDeletionKind:
                            terminalZoneDeletionKind
                    )
                } else {
                    try recordSyncHealth(
                        terminalHealthCategory,
                        context: context,
                        terminalZoneDeletionKind:
                            terminalZoneDeletionKind
                    )
                }
            } catch is CancellationError {
                return
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed to persist sync health: \(error)")
            }
        }

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
                !(error is ChangeFeedMigrationError) &&
                !(error is BigSyncCloudAccountPortError) &&
                (error as? SyncError) != .cancelled &&
                terminalHealthCategory != .accountTemporarilyUnavailable &&
                !cancelledDueToUnauthentication
            finishSynchronizationDrain(with: .failure(error))
            // Preserve terminal ownership until the failed drain has released
            // its waiters, for the same reason as the successful terminal
            // paths above.
            syncing = false
            synchronizationTask = nil
            if shouldStartDeferredLocalWorkDrain {
                beginSynchronization()
            }
            return
        }

        retrySleepUntil = Date().addingTimeInterval(retryDelay)
        synchronizationTask = Task(priority: .utility) { @BigSyncBackgroundActor [weak self] in
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

/// Computes retry delays without ever retrying earlier than a delay explicitly
/// requested by CloudKit. The fallback grows only for consecutive transient
/// failures and is reset after a completed synchronization or cancellation.
///
/// CloudKit's retry-after value is deliberately not capped: reducing a
/// server-directed delay can create a retry storm. Optional jitter is added
/// *after* that minimum so clients do not synchronize their wakeups while
/// still respecting CloudKit's backpressure.
enum CloudKitRetryBackoff {
    static let initialFallbackDelay: TimeInterval = 5
    static let maximumFallbackDelay: TimeInterval = 300

    static func delay(
        serverMinimum: TimeInterval?,
        consecutiveFailures: Int,
        randomUnit: Double = Double.random(in: 0...1)
    ) -> TimeInterval {
        if let serverMinimum {
            let minimum = max(0, serverMinimum)
            let boundedRandomUnit = min(max(0, randomUnit), 1)
            let jitterCap = min(30, max(1, minimum * 0.1))
            return minimum + (boundedRandomUnit * jitterCap)
        }

        let exponent = min(max(consecutiveFailures - 1, 0), 6)
        return min(
            maximumFallbackDelay,
            initialFallbackDelay * Double(1 << exponent)
        )
    }
}

// MARK: - Utilities

extension CloudKitSynchronizer {
    /// Converts every CloudKit zone-loss shape into the synchronizer's single
    /// supported zone lifecycle. Recovery intent is persisted before the
    /// terminal fence so a process death can never leave an encrypted reset
    /// permanently blocked without a resumable migration.
    @BigSyncBackgroundActor
    func applyCloudKitLoss(
        _ disposition: CloudKitLossClassifier.ZoneDisposition,
        zoneID: CKRecordZone.ID,
        context: RunContext,
        allowsEncryptedBootstrapAbsence: Bool = false
    ) -> Error? {
        switch disposition {
        case .encryptedDataReset:
            let recoveryWasActive = isEncryptedDataResetRecoveryActive
                || hasPendingEncryptedDataResetRecovery(context: context)
            if !recoveryWasActive {
                do {
                    try requestChangeFeedRecovery(
                        context: context,
                        mode: .encryptedDataReset
                    )
                } catch {
                    // Do not publish a terminal fence unless its recovery
                    // envelope is durably readable. The current feed cursor is
                    // not committed, so CloudKit can replay the loss event.
                    return error
                }
            }
            do {
                try markConfiguredZoneTerminal(
                    zoneID,
                    kind: .encryptedDataReset,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            } catch {
                return error
            }
            if recoveryWasActive && allowsEncryptedBootstrapAbsence {
                return nil
            }
            return ChangeFeedMigrationError.establishedZoneUnavailable(
                zoneID,
                .encryptedDataReset
            )

        case .terminal(let kind):
            do {
                try markConfiguredZoneTerminal(
                    zoneID,
                    kind: kind,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            } catch {
                return error
            }
            return ChangeFeedMigrationError.establishedZoneUnavailable(
                zoneID,
                kind
            )

        case .missing:
            if allowsEncryptedBootstrapAbsence {
                // After CloudKit has reported the encrypted-key reset, later
                // zone-scoped calls may surface only ordinary zoneNotFound.
                // The already-fenced encrypted migration is the sole context
                // in which an established zone may be treated as authoritatively
                // empty and recreated.
                return nil
            }
            guard configuredZoneIsEstablished(zoneID) else { return nil }
            do {
                try markConfiguredZoneTerminal(
                    zoneID,
                    kind: .unknown,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            } catch {
                return error
            }
            return ChangeFeedMigrationError.establishedZoneUnavailable(
                zoneID,
                .unknown
            )
        }
    }

    @BigSyncBackgroundActor
    func applyCloudKitLoss(
        error: Error,
        defaultZoneID: CKRecordZone.ID,
        context: RunContext,
        allowsEncryptedBootstrapAbsence: Bool = false
    ) -> Error? {
        let classification = CloudKitLossClassifier.classify(
            error: error,
            defaultZoneID: defaultZoneID
        )
        guard let disposition = classification.zoneDispositions[defaultZoneID]
        else { return nil }
        return applyCloudKitLoss(
            disposition,
            zoneID: defaultZoneID,
            context: context,
            allowsEncryptedBootstrapAbsence:
                allowsEncryptedBootstrapAbsence
        )
    }

//    @BigSyncBackgroundActor
    func postNotification(_ notification: Notification.Name, object: Any? = nil, userInfo: [AnyHashable: Any]? = nil) {
        let object = object ?? self
//        Task(priority: .background) { @BigSyncBackgroundActor in
            NotificationCenter.default.post(name: notification, object: object, userInfo: userInfo)
//        }
    }

    @BigSyncBackgroundActor
    func notifyDelegateForDeletedZoneIDs(
        _ zoneIDs: [CKRecordZone.ID],
        attemptID: UUID
    ) async throws {
        for zoneID in zoneIDs {
            // Lifecycle state and tracking recovery are owned exclusively by
            // the fenced migration. The delegate receives an informational
            // notification only after the account/run has been revalidated.
            try await revalidateActiveRunContext(for: attemptID)
            self.delegate?.synchronizer(self, zoneIDWasDeleted: zoneID)
        }
    }
    
    @BigSyncBackgroundActor
    func loadTokens(for zoneIDs: [CKRecordZone.ID]) async throws -> [CKRecordZone.ID] {
        var filteredZoneIDs = [CKRecordZone.ID]()
        activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
        
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
        activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
    }
    
    func shouldRetryUpload(for error: NSError) -> Bool {
        if isLimitExceededError(error)
            || isZoneNotFoundOrDeletedError(error) {
            return uploadRetries < 5
        }
        // Record conflicts are reconciled and retried inside the iterative
        // mutation drain. Once that bounded loop reports a conflict, beginning
        // another outer fetch/upload cycle would reset its retry counter and
        // could loop forever on an irreconcilable record.
        return false
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
        
        guard !cancelSync else {
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
                guard !cancelSync else { throw CancellationError() }
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
    func fetchChanges(afterUpload: Bool = false) async {
        let attemptID = synchronizationAttemptID
        guard !cancelSync else {
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: SyncError.cancelled)
            return
        }

        do {
            try Task.checkCancellation()
            postNotification(.SynchronizerWillFetchChanges)

            let token = try await fetchDatabaseChanges()
            try await revalidateActiveRunContext(for: attemptID)

            // The first migration pass starts with nil database and zone
            // cursors. Reconcile only after every configured zone page has
            // imported and committed its cursor, before upload discovery.
            if let context = activeRunContext {
                try await reconcileChangeFeedMigrationIfNeeded(context: context)
                try await revalidateRunContext(context)
            }

            serverChangeToken = token
            if syncMode == .sync {
                if afterUpload,
                   !modelAdapters.contains(where: { $0.hasChanges }) {
                    // A successful upload is not terminal until one more
                    // change-feed pass advances through the server's response.
                    // When that pass leaves no durable adapter work, its
                    // database cursor is the quiescent commit boundary.
                    try persistDatabaseToken(token)
                    await changesFinishedSynchronizing()
                } else {
                    try await uploadChanges()
                }
            } else {
                try await processFetchedChanges()
                try await revalidateActiveRunContext(for: attemptID)
                try persistDatabaseToken(token)
                await changesFinishedSynchronizing()
            }
        } catch {
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: error)
        }
    }

    @BigSyncBackgroundActor
    func fetchDatabaseChanges() async throws -> DatabaseChangeCursor? {
        let attemptID = synchronizationAttemptID
        reportProgress("database-fetch-start")

        var pageCursor = serverChangeToken
        var changedZoneIDs = Set<CKRecordZone.ID>()
        var deletedZoneIDs = Set<CKRecordZone.ID>()
        var pageDeletions = [CloudKitZoneDeletion]()
        var moreComing = true

        while moreComing {
            try await revalidateActiveRunContext(for: attemptID)
            let page: CloudKitDatabaseChangePage
            do {
                page = try await changeFeed.databaseChanges(
                    since: pageCursor,
                    resultsLimit: 200
                )
            } catch {
                if let context = activeRunContext,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: recordZoneID,
                    context: context,
                    allowsEncryptedBootstrapAbsence:
                        isEncryptedDataResetRecoveryActive
                   ) {
                    throw lifecycleError
                }
                throw error
            }
            try await revalidateActiveRunContext(for: attemptID)
            changedZoneIDs.formUnion(page.changedZoneIDs)
            deletedZoneIDs.formUnion(page.deletions.map(\.zoneID))
            pageDeletions.append(contentsOf: page.deletions)
            pageCursor = page.cursor
            moreComing = page.moreComing
        }

        reportProgress("database-fetch-completion")
        let configuredZoneID = recordZoneID
        let configuredZoneIDs: Set<CKRecordZone.ID> = [configuredZoneID]
        var recoverableEncryptedZoneIDs = Set<CKRecordZone.ID>()
        let deletionClassification = CloudKitLossClassifier.classify(
            deletions: pageDeletions
        )
        if let disposition = deletionClassification
            .zoneDispositions[configuredZoneID],
           let context = activeRunContext {
            let encryptedRecoveryWasActive =
                isEncryptedDataResetRecoveryActive
            if let lifecycleError = applyCloudKitLoss(
                disposition,
                zoneID: configuredZoneID,
                context: context,
                allowsEncryptedBootstrapAbsence:
                    encryptedRecoveryWasActive
            ) {
                throw lifecycleError
            }
            if disposition == .encryptedDataReset,
               encryptedRecoveryWasActive {
                recoverableEncryptedZoneIDs.insert(configuredZoneID)
            }
        }

        // This synchronizer owns exactly one configured zone. Database history
        // may contain unrelated private-database zones from other clients;
        // never publish those to this adapter provider or delegate.
        let configuredDeletedZoneIDs = deletedZoneIDs
            .intersection(configuredZoneIDs)
            .subtracting(recoverableEncryptedZoneIDs)
        try await notifyDelegateForDeletedZoneIDs(
            Array(configuredDeletedZoneIDs),
            attemptID: attemptID
        )
        try await revalidateActiveRunContext(for: attemptID)

        changedZoneIDs.subtract(deletedZoneIDs)
        if isChangeFeedMigrationActive {
            // Database history lists zones whose metadata changed. A full
            // bootstrap must additionally read every configured zone.
            changedZoneIDs.formUnion(configuredZoneIDs)
        }

        let zoneIDsToFetch = try await loadTokens(
            for: Array(changedZoneIDs)
        )
        try await revalidateActiveRunContext(for: attemptID)
        guard !zoneIDsToFetch.isEmpty else {
            lastDatabaseChangesEmptyAt = Date()
            resetActiveTokens()
            return pageCursor
        }

        lastDatabaseChangesEmptyAt = nil
        try checkSynchronizationAttempt(attemptID)
        zoneIDsToFetch.forEach {
            delegate?.synchronizerWillFetchChanges(self, in: $0)
        }
        reportProgress("zone-fetch-start")
        try await fetchZoneChanges(zoneIDsToFetch)
        try await revalidateActiveRunContext(for: attemptID)
        return pageCursor
    }

    @BigSyncBackgroundActor
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID]) async throws {
        let attemptID = synchronizationAttemptID
        let runID = synchronizationRunID
        defer { changeRequestProcessor.clearErrors() }

        for zoneID in zoneIDs {
            var pageCursor = activeZoneTokens[zoneID]
            var pageIndex = 0
            guard let adapter = modelAdapterDictionary[zoneID] else {
                continue
            }
            let isServerBootstrap: Bool
            if let migrating = adapter as? any ChangeFeedResetMigrating {
                isServerBootstrap =
                    await migrating.isChangeFeedServerBootstrapActive()
                try await revalidateActiveRunContext(for: attemptID)
            } else {
                isServerBootstrap = false
            }

            var moreComing = true
            while moreComing {
                try await revalidateActiveRunContext(for: attemptID)
                let page: CloudKitRecordZoneChangePage
                do {
                    page = try await changeFeed.recordZoneChanges(
                        in: zoneID,
                        since: pageCursor,
                        desiredKeys: nil,
                        resultsLimit: 200
                    )
                } catch {
                    guard let context = activeRunContext else {
                        throw error
                    }
                    let classification = CloudKitLossClassifier.classify(
                        error: error,
                        defaultZoneID: zoneID
                    )
                    guard let disposition = classification
                        .zoneDispositions[zoneID] else {
                        throw error
                    }
                    if let lifecycleError = applyCloudKitLoss(
                        disposition,
                        zoneID: zoneID,
                        context: context,
                        allowsEncryptedBootstrapAbsence:
                            isChangeFeedMigrationActive
                                && isEncryptedDataResetRecoveryActive
                    ) {
                        throw lifecycleError
                    }
                    guard isChangeFeedMigrationActive else { throw error }
                    // A never-established zone is an empty authoritative
                    // bootstrap. Reconcile local journal work, then let the
                    // ordinary upload path create the zone.
                    moreComing = false
                    continue
                }
                try await revalidateActiveRunContext(for: attemptID)
                try ChangeRequestProcessor.validateInboundPageIdentities(
                    records: page.records,
                    deletedRecordIDs: page.deletedRecordIDs,
                    expectedZoneID: zoneID
                )
                pageIndex += 1
                // A stable, machine-readable progress checkpoint lets the
                // disposable E2E client prove it consumed every page through
                // the same production change-feed transport.
                reportProgress(
                    "zone-page \(zoneID.zoneName) \(pageIndex) \(page.records.count)"
                )

                if page.records.contains(where: { record in
                    guard let version = record[
                        cloudKitSynchronizerModelCompatibilityVersionKey
                    ] as? Int else {
                        return false
                    }
                    return self.compatibilityVersion > 0
                        && version > self.compatibilityVersion
                }) {
                    throw SyncError.higherModelVersionFound
                }
                let currentDeviceIdentifier = self.deviceIdentifier
                func isAuthoritativeOwnUpload(_ record: CKRecord) -> Bool {
                    !isServerBootstrap
                        && currentDeviceIdentifier == record[
                            cloudKitSynchronizerDeviceUUIDKey
                        ] as? String
                }
                let authoritativeOwnUploadRecords = page.records.filter(
                    isAuthoritativeOwnUpload
                )
                let acceptedRecords = page.records.filter {
                    !isAuthoritativeOwnUpload($0)
                }

                for record in acceptedRecords {
                    changeRequestProcessor.addFetchedChangeRequest(
                        ChangeRequest(
                            downloadedRecord: record,
                            deletedRecordID: nil,
                            adapter: adapter,
                            runID: runID
                        )
                    )
                }
                for recordID in page.deletedRecordIDs {
                    changeRequestProcessor.addFetchedChangeRequest(
                        ChangeRequest(
                            downloadedRecord: nil,
                            deletedRecordID: recordID,
                            adapter: adapter,
                            runID: runID
                        )
                    )
                }
                if !acceptedRecords.isEmpty
                    || !page.deletedRecordIDs.isEmpty {
                    synchronizationDrainDidImportChanges = true
                }

                let pageOutcomes = try await changeRequestProcessor
                    .finishProcessing(for: adapter)
                if let firstError = changeRequestProcessor.getErrors().first {
                    throw firstError
                }
                guard pageOutcomes.liveResults.count
                        == acceptedRecords.count else {
                    throw InboundDispositionValidationError.cardinality(
                        expected: acceptedRecords.count,
                        actual: pageOutcomes.liveResults.count
                    )
                }
                guard pageOutcomes.deletionResults.count
                        == page.deletedRecordIDs.count else {
                    throw InboundDispositionValidationError.cardinality(
                        expected: page.deletedRecordIDs.count,
                        actual: pageOutcomes.deletionResults.count
                    )
                }
                let authoritativeOwnUploadResults = try await adapter
                    .validateAuthoritativeOwnUploadRecords(
                        authoritativeOwnUploadRecords
                    )
                try validateInboundLiveResults(
                    authoritativeOwnUploadResults,
                    records: authoritativeOwnUploadRecords
                )
                if authoritativeOwnUploadResults.contains(where: {
                    $0.disposition != .ignoredExplicitAuthority
                }) {
                    synchronizationDrainDidImportChanges = true
                }
                try await revalidateActiveRunContext(for: attemptID)

                var acceptedResultIndex = 0
                var authoritativeOwnUploadResultIndex = 0
                let normalizedLiveResults = page.records.enumerated().map {
                    ordinal, record in
                    let disposition: InboundLiveDisposition
                    if isAuthoritativeOwnUpload(record) {
                        disposition = authoritativeOwnUploadResults[
                            authoritativeOwnUploadResultIndex
                        ].disposition
                        authoritativeOwnUploadResultIndex += 1
                    } else {
                        disposition = pageOutcomes.liveResults[
                            acceptedResultIndex
                        ].disposition
                        acceptedResultIndex += 1
                    }
                    return InboundLiveResult(
                        event: InboundEventIdentity(
                            ordinal: ordinal,
                            entityType: record.recordType,
                            recordID: record.recordID
                        ),
                        disposition: disposition
                    )
                }
                let normalizedDeletionResults = zip(
                    page.deletedRecordIDs,
                    pageOutcomes.deletionResults
                ).enumerated().map { ordinal, pair in
                    InboundDeletionResult(
                        event: InboundEventIdentity(
                            ordinal: ordinal,
                            entityType: pair.1.event.entityType,
                            recordID: pair.0
                        ),
                        disposition: pair.1.disposition
                    )
                }
                // Establishment proof is durable before the zone cursor. If
                // that safety write fails, this exact page is replayed rather
                // than advancing past a zone whose later disappearance might
                // otherwise be mistaken for a never-created zone.
                if let context = activeRunContext {
                    try markConfiguredZoneEstablished(
                        zoneID,
                        accountScopeIdentifier:
                            context.accountScopeIdentifier
                    )
                }
                // Receipt-and-token-last: a failed import or lifecycle write
                // refetches this exact page. Realm-backed adapters bind the
                // exact dispositions and proven quarantine supersessions to
                // the cursor in one tracking-Realm transaction.
                try await adapter.commitInboundPage(InboundPageCommit(
                    previousCursor: pageCursor,
                    nextCursor: page.cursor,
                    liveResults: normalizedLiveResults,
                    deletionResults: normalizedDeletionResults
                ))
                try await revalidateActiveRunContext(for: attemptID)
                // Deferred relationships are already durable in the adapter's
                // persistence Realm and were proven by commitInboundPage.
                // Apply them only after the cursor commit so a successful
                // application cannot erase the evidence that authorized this
                // page to advance.
                try await adapter.persistImportedChanges()
                try await revalidateActiveRunContext(for: attemptID)
                activeZoneTokens[zoneID] = page.cursor
                pageCursor = page.cursor
                moreComing = page.moreComing
            }
        }

        reportProgress("zone-pages-completed")
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
        reportProgress("upload-start")
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
                if let context = activeRunContext,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: recordZoneID,
                    context: context
                   ) {
                    await failSynchronization(error: lifecycleError)
                    return
                }
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
                try persistDatabaseToken(serverChangeToken)
                reportProgress("upload-completed")
                // Always re-fetch after upload. The next fetch either imports
                // concurrent server changes or reaches the terminal receipt.
                await fetchChanges(afterUpload: true)
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
                if let error,
                   let context = activeRunContext,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: adapter.recordZoneID,
                    context: context
                   ) {
                    try await completion(lifecycleError)
                    return
                }
                try await completion(error)
                return
            }
            do {
                try await uploadRecordsUsingAsyncStore(
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
        do {
            // Validate immediately before and after each account-routed await.
            try await revalidateActiveRunContext(for: attemptID)
            _ = try await zoneStore.recordZone(withID: zoneID)
            try await revalidateActiveRunContext(for: attemptID)
            if let context = activeRunContext {
                try markConfiguredZoneEstablished(
                    zoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            }
            try await completion(nil)
        } catch {
            do {
                // Account replacement or cancellation wins over interpreting
                // an obsolete zone lookup as evidence that a zone is missing.
                try await revalidateActiveRunContext(for: attemptID)
            } catch {
                try await completion(error)
                return
            }

            guard let context = activeRunContext else {
                try await completion(error)
                return
            }
            let classification = CloudKitLossClassifier.classify(
                error: error,
                defaultZoneID: zoneID
            )
            guard let disposition = classification.zoneDispositions[zoneID]
            else {
                try await completion(error)
                return
            }
            if let lifecycleError = applyCloudKitLoss(
                disposition,
                zoneID: zoneID,
                context: context,
                allowsEncryptedBootstrapAbsence:
                    isEncryptedDataResetRecoveryActive
            ) {
                try await completion(lifecycleError)
                return
            }

            let newZone = CKRecordZone(zoneID: zoneID)
            do {
                try await revalidateActiveRunContext(for: attemptID)
                let savedZone = try await zoneStore.save(recordZone: newZone)
                try await revalidateActiveRunContext(for: attemptID)
                guard savedZone.zoneID == zoneID else {
                    throw CocoaError(.coderValueNotFound)
                }
                try markConfiguredZoneEstablished(
                    zoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
                logger.info(
                    "QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)"
                )
                try await completion(nil)
            } catch {
                if let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: zoneID,
                    context: context,
                    allowsEncryptedBootstrapAbsence: false
                ) {
                    try await completion(lifecycleError)
                } else {
                    try await completion(error)
                }
            }
        }
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
                    try await uploadDeletionsUsingAsyncStore(
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
