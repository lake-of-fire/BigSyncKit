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

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func performSynchronization() async {
        let attemptID = synchronizationAttemptID
        do {
            try checkSynchronizationAttempt(attemptID)
            logger.info("QSCloudKitSynchronizer >> Perform synchronization...")
            self.postNotification(.SynchronizerWillSynchronize)
            // Notifications are synchronous callouts. An observer can cancel
            // this attempt or admit a successor before this method continues.
            try checkSynchronizationAttempt(attemptID)
            let token = self.storedDatabaseToken
            try checkSynchronizationAttempt(attemptID)
            self.serverChangeToken = token
            self.uploadRetries = 0
            self.didNotifyUpload = Set<CKRecordZone.ID>()
            await fetchChanges()
        } catch {
            await failSynchronization(error: error, for: attemptID)
        }
    }

    @BigSyncBackgroundActor
    func changesFinishedSynchronizing() async {
        let attemptID = synchronizationAttemptID
        // A progress callback is a synchronous external callout, not a safe
        // point to abandon the original drain's ownership. Reuse the existing
        // attempt predicate and cancellation settlement without a new ticket.
        func canContinue() -> Bool {
            do {
                try checkSynchronizationAttempt(attemptID)
                return true
            } catch {
                settleCancellationIfCurrentAttempt(attemptID)
                return false
            }
        }
        guard canContinue() else { return }
        let isDownloadOnly = activeSynchronizationMode == .downloadOnly
        guard beginRunCallback(for: attemptID) else { return }
        defer { endRunCallback() }
        do {
            reportProgress("terminal-tail-start")
            guard canContinue() else { return }
            try await revalidateActiveRunContext(for: attemptID)
            reportProgress("terminal-tail-account-revalidated")
            guard canContinue() else { return }
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
//        logger.info("QSCloudKitSynchronizer >> Finishing synchronization batch...")
        
        do {
            try await cleanUpAndForwardTerminalImports(for: attemptID)
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        reportProgress("terminal-tail-adapters-cleaned")
        guard canContinue() else { return }

        do {
            // The final import can legitimately forward zero new journal rows
            // while durable tracking work remains.
            // Recheck all adapters after the last suspension and convert any
            // pending state into a tail drain before authorizing a receipt.
            let hasPendingChanges = try !isDownloadOnly
                && adaptersHavePendingChangesAtTerminalBoundary()
            guard canContinue() else { return }
            if hasPendingChanges { synchronizationRequestedWhileRunning = true }
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        reportProgress("terminal-tail-pending-checked")
        guard canContinue() else { return }
        
//        logger.info("QSCloudKitSynchronizer >> Finished synchronization batch")
        if !isDownloadOnly, synchronizationRequestedWhileRunning {
            reportProgress("terminal-tail-restarting")
            guard canContinue() else { return }
            restartSynchronizationForTerminalWork()
            return
        }
        // The migration may finish only after final import forwarding and the
        // terminal pending-state check have proven this drain quiescent.
        do {
            if !isDownloadOnly, let context = activeRunContext {
                try await finishChangeFeedMigrationIfNeeded(context: context)
                try await revalidateRunContext(context)
            }
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        let consumedServerBoundaryIdentifier: String?
        do {
            consumedServerBoundaryIdentifier = try
                currentConsumedServerBoundaryIdentifier(for: activeRunContext)
            guard canContinue() else { return }
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        guard let terminalContext = activeRunContext else {
            await failSynchronization(error: CancellationError(), for: attemptID)
            return
        }
        let reconciliationBlockers: [DomainBlocker]
        do {
            reconciliationBlockers = try await reconcileDomainBeforeTerminalPublication(
                context: terminalContext,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier
            )
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        reportProgress("terminal-tail-prepublication-completed")
        guard canContinue() else { return }

        let publication: TerminalDomainPublication
        do {
            publication = try await inspectTerminalDomainPublication(
                context: terminalContext,
                isDownloadOnly: isDownloadOnly,
                reconciliationBlockers: reconciliationBlockers
            )
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }

        let disposition: TerminalPublicationDisposition
        do {
            // Account validation is the last suspension before the local
            // cutoff. Every eligible generation visible to the following
            // refreshed Realm views belongs to this drain. Later concurrent
            // writes remain journaled for the next drain; the receipt is not
            // an assertion that all writers have stopped.
            try await revalidateRunContext(terminalContext)
            disposition = try checkTerminalPublicationCutoff(
                context: terminalContext,
                isDownloadOnly: isDownloadOnly,
                publication: publication,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier
            )
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        switch disposition {
        case .restartRequired:
            reportProgress("terminal-tail-restarting")
            guard canContinue() else { return }
            restartSynchronizationForTerminalWork()
            return
        case .downloadOnly:
            // Journal forwarding and domain reconciliation may create local
            // work, but this mode promises not to upload it. Finish the inbound
            // request once without completing migration, minting a receipt or
            // persisting full-drain publication evidence.
            let result: SynchronizationResult
            do {
                result = try prepareDownloadOnlyPublication(
                    context: terminalContext,
                    consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier,
                    publication: publication
                )
            } catch {
                await failSynchronization(error: error, for: attemptID)
                return
            }
            reportProgress("download-only-completed")
            guard canContinue() else { return }
            await publishSynchronizationResult(result, context: terminalContext)
            return
        case .blocked:
            // A semantic blocker is publishable only after the same exact
            // journal and cursor predicates required by a success receipt are
            // stable. Domain reconciliation may both create upload work and
            // report a blocker; drain that work first so `.blocked` describes
            // the terminal transport boundary rather than an intermediate one.
            let result: SynchronizationResult
            do {
                result = try prepareBlockedTerminalPublication(
                    context: terminalContext,
                    consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier,
                    publication: publication
                )
            } catch is CancellationError {
                settleCancellationIfCurrentAttempt(attemptID)
                return
            } catch {
                await failSynchronization(error: error, for: attemptID)
                return
            }
            await publishSynchronizationResult(result, context: terminalContext)
            return
        case .fullDrain:
            break
        }

        // Only now authorize and publish the receipt. A notification observer
        // may request a fresh synchronization, so snapshot the result before
        // releasing run ownership.
        let result: SynchronizationResult
        do {
            result = try prepareFullTerminalPublication(
                context: terminalContext,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier,
                publication: publication
            )
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
        do {
            // Cursor/account/retry setters retain a mutation failure in the
            // production file store. Revalidate after every terminal write so
            // an attempt can never mint a success receipt when any critical
            // local-state commit failed.
            try keyValueStore.bigSyncValidateDurability()
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
#if DEBUG
        do {
            try await processKillCheckpointHandler?(
                .terminalEvidenceBeforeCompletionDelivery
            )
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        } catch {
            await failSynchronization(error: error, for: attemptID)
            return
        }
#endif
        reportProgress("terminal-receipt")
        guard canContinue() else { return }
        await publishSynchronizationResult(result, context: terminalContext)
    }

    /// The synchronous cutoff selects exactly one terminal path. This value
    /// belongs to the current attempt; it does not replace drain wakeup state.
    private enum TerminalPublicationDisposition {
        case restartRequired, downloadOnly, blocked, fullDrain
    }

    /// Domain inspection is a candidate for publication, not receipt authority.
    private struct TerminalDomainPublication {
        let blockers: [DomainBlocker]
        let domainScopeIdentifier: String?
    }

    @BigSyncBackgroundActor
    private func cleanUpAndForwardTerminalImports(for attemptID: UUID) async throws {
        try checkSynchronizationAttempt(attemptID)
        resetActiveTokens()

        uploadRetries = 0

        for adapter in modelAdapters {
            try await adapter.didFinishImport()
            try await revalidateActiveRunContext(for: attemptID)
            try await adapter.cleanUp()
            try await revalidateActiveRunContext(for: attemptID)
            // Cleanup can overlap a newly committed local mutation. Forward
            // journals again so that mutation requests a new drain before a
            // terminal receipt is issued.
            try await adapter.didFinishImport()
            try await revalidateActiveRunContext(for: attemptID)
        }
    }

    @BigSyncBackgroundActor
    private func reconcileDomainBeforeTerminalPublication(
        context terminalContext: RunContext,
        consumedServerBoundaryIdentifier: String?
    ) async throws -> [DomainBlocker] {
        try checkRunContext(terminalContext)
        var publicationBlockers = [DomainBlocker]()
        var inboundIdentityDeliveries = [
            (adapter: ModelAdapter, batch: CommittedInboundIdentityBatch)
        ]()
        if let domainPrepublicationHandler {
            for adapter in modelAdapters {
                if let batch = try adapter
                    .pendingCommittedInboundIdentityBatch() {
                    inboundIdentityDeliveries.append((adapter, batch))
                }
            }
            try checkRunContext(terminalContext)
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
            reportProgress("terminal-tail-domain-handler-completed")
            try checkRunContext(terminalContext)
            try await revalidateRunContext(terminalContext)
            reportProgress("terminal-tail-domain-context-revalidated")
            try checkRunContext(terminalContext)
            for delivery in inboundIdentityDeliveries {
                reportProgress("terminal-tail-inbound-ack-started")
                try checkRunContext(terminalContext)
                try await delivery.adapter
                    .acknowledgeCommittedInboundIdentityBatch(
                        deliveryID: delivery.batch.deliveryID
                    )
                try await revalidateRunContext(terminalContext)
                reportProgress("terminal-tail-inbound-ack-completed")
                try checkRunContext(terminalContext)
            }
            // Domain reconciliation is allowed to commit authoritative
            // local writes. Forward those durable target-journal
            // generations before deciding whether this drain is terminal.
            // In download-only mode this does not grant upload authority;
            // it merely ensures the next explicit full drain starts from
            // the newest generation instead of uploading a stale tracked
            // generation first.
            for adapter in modelAdapters {
                reportProgress("terminal-tail-import-forwarding-started")
                try checkRunContext(terminalContext)
                try await adapter.didFinishImport { checkpoint in
                    self.reportProgress("terminal-tail-\(checkpoint)")
                }
                reportProgress("terminal-tail-import-forwarding-completed")
                try checkRunContext(terminalContext)
                try await revalidateRunContext(terminalContext)
                reportProgress("terminal-tail-import-forwarding-revalidated")
                try checkRunContext(terminalContext)
            }
        }
        return publicationBlockers
    }

    @BigSyncBackgroundActor
    private func inspectTerminalDomainPublication(
        context terminalContext: RunContext,
        isDownloadOnly: Bool,
        reconciliationBlockers: [DomainBlocker]
    ) async throws -> TerminalDomainPublication {
        try checkRunContext(terminalContext)
        var publicationBlockers = reconciliationBlockers
        var domainPublicationScopeIdentifier: String?
        for adapter in modelAdapters {
            publicationBlockers.append(contentsOf:
                try await adapter.semanticPublicationBlockers()
            )
            try checkRunContext(terminalContext)
        }
        try await revalidateRunContext(terminalContext)
        if !isDownloadOnly, publicationBlockers.isEmpty,
           let provider = domainPublicationScopeIdentifierProvider {
            let scope = try await provider()
            guard scope?.isEmpty != true else {
                throw DurableKeyValueStoreError.mutationNotDurable
            }
            domainPublicationScopeIdentifier = scope
            try await revalidateRunContext(terminalContext)
        }
        return TerminalDomainPublication(
            blockers: publicationBlockers,
            domainScopeIdentifier: domainPublicationScopeIdentifier
        )
    }

    /// No suspension is allowed between the last account validation and these
    /// journal/cursor predicates or the ensuing durable publication writes.
    @BigSyncBackgroundActor
    private func checkTerminalPublicationCutoff(
        context terminalContext: RunContext,
        isDownloadOnly: Bool,
        publication: TerminalDomainPublication,
        consumedServerBoundaryIdentifier: String?
    ) throws -> TerminalPublicationDisposition {
        try checkRunContext(terminalContext)
        let hasPendingChanges = try !isDownloadOnly
            && adaptersHavePendingChangesAtTerminalBoundary()
        try checkRunContext(terminalContext)
        if hasPendingChanges {
            reportProgress("terminal-tail-pending-target")
            try checkRunContext(terminalContext)
            synchronizationRequestedWhileRunning = true
        }
        let currentBoundary = try currentConsumedServerBoundaryIdentifier(
            for: terminalContext
        )
        try checkRunContext(terminalContext)
        if currentBoundary != consumedServerBoundaryIdentifier {
            if isDownloadOnly {
                // Outbound wakeups are intentionally ignored in this
                // mode; a changed inbound cursor is not such a wakeup.
                // Do not publish a result for the stale domain boundary.
                throw SyncError.inboundBoundaryChanged
            }
            reportProgress("terminal-tail-inbound-boundary-changed")
            try checkRunContext(terminalContext)
            synchronizationRequestedWhileRunning = true
        }
        if isDownloadOnly { return .downloadOnly }
        if synchronizationRequestedWhileRunning { return .restartRequired }
        return publication.blockers.isEmpty ? .fullDrain : .blocked
    }

    @BigSyncBackgroundActor
    private func prepareDownloadOnlyPublication(
        context terminalContext: RunContext,
        consumedServerBoundaryIdentifier: String?,
        publication: TerminalDomainPublication
    ) throws -> SynchronizationResult {
        try checkRunContext(terminalContext)
        activeReceiptAuthorizationID = nil
        try keyValueStore.bigSyncValidateDurability()
        return SynchronizationResult(
            didImportChanges: synchronizationDrainDidImportChanges,
            publicationState: publication.blockers.isEmpty
                ? .complete : .blocked(publication.blockers),
            terminalBoundary: .init(
                accountScopeIdentifier: terminalContext.accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    terminalContext.replicaBindingGenerationIdentifier,
                runID: terminalContext.runID,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier
            ),
            completionScope: .downloadOnly
        )
    }

    @BigSyncBackgroundActor
    private func prepareBlockedTerminalPublication(
        context terminalContext: RunContext,
        consumedServerBoundaryIdentifier: String?,
        publication: TerminalDomainPublication
    ) throws -> SynchronizationResult {
        try checkRunContext(terminalContext)
        try recordSyncHealth(.semanticBlocked, context: terminalContext)
        try checkRunContext(terminalContext)
        try keyValueStore.bigSyncValidateDurability()
        activeReceiptAuthorizationID = nil
        return SynchronizationResult(
            didImportChanges: synchronizationDrainDidImportChanges,
            publicationState: .blocked(publication.blockers),
            terminalBoundary: .init(
                accountScopeIdentifier: terminalContext.accountScopeIdentifier,
                replicaBindingGenerationIdentifier:
                    terminalContext.replicaBindingGenerationIdentifier,
                runID: terminalContext.runID,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier
            )
        )
    }

    @BigSyncBackgroundActor
    private func prepareFullTerminalPublication(
        context terminalContext: RunContext,
        consumedServerBoundaryIdentifier: String?,
        publication: TerminalDomainPublication
    ) throws -> SynchronizationResult {
        try checkRunContext(terminalContext)
        let authorizationID = UUID()
        activeReceiptAuthorizationID = authorizationID
        let receipt = SynchronizationReceipt(
            context: terminalContext,
            issuerID: synchronizationReceiptIssuerID,
            authorizationID: authorizationID,
            consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier
        )
        let result = SynchronizationResult(
            didImportChanges: synchronizationDrainDidImportChanges,
            receipt: receipt
        )
        consecutiveTransientCloudKitFailures = 0
        clearPersistedTransientRetryState()
        if let domainPublicationScopeIdentifier = publication.domainScopeIdentifier,
           let consumedServerBoundaryIdentifier,
           let adapter = modelAdapters.first {
            guard let changeFeedEpoch = try adapter.changeFeedEpoch() else {
                throw DurableKeyValueStoreError.mutationNotDurable
            }
            try checkRunContext(terminalContext)
            try persistDurablePublicationEvidence(
                domainScopeIdentifier: domainPublicationScopeIdentifier,
                context: terminalContext,
                consumedServerBoundaryIdentifier: consumedServerBoundaryIdentifier,
                changeFeedEpoch: changeFeedEpoch
            )
        }
        try recordSyncHealth(.succeeded, context: terminalContext)
        try checkRunContext(terminalContext)
        return result
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

    /// A suspended callback must fail only the attempt that originally owned it.
    /// Check before resetting tokens, forwarding imports, or releasing waiters:
    /// a replacement can be admitted while it still waits for this callback.
    @BigSyncBackgroundActor
    func failSynchronization(error: Error, for attemptID: UUID) async {
        guard synchronizationAttemptID == attemptID else { return }
        if error is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        }
        // Failure cleanup may itself deliver synchronous callbacks or suspend.
        // Reuse the existing attempt fence; revoked authority is a reason to
        // settle this caller, never permission to mutate a successor's state.
        func canContinue() -> Bool {
            do {
                try checkSynchronizationAttempt(attemptID)
                return true
            } catch {
                settleCancellationIfCurrentAttempt(attemptID)
                return false
            }
        }
        guard canContinue() else { return }
        let failureContext = activeRunContext
        let failureAuthorityGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        logger.info("QSCloudKitSynchronizer >> Failing or backing off synchronization...")
        guard canContinue() else { return }
        
        resetActiveTokens()
        
        uploadRetries = 0
        
        for adapter in modelAdapters {
            do {
                try await adapter.didFinishImport()
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed final import forwarding: \(error)")
            }
            guard canContinue() else { return }
        }
        
        // One delivery cannot switch delegates midway through its notification.
        let failureDelegate = delegate
        self.postNotification(.SynchronizerDidFailToSynchronize, userInfo: [cloudKitSynchronizerErrorKey: error])
        guard canContinue() else { return }
        failureDelegate?.synchronizerDidfailToSync(self, error: error)
        guard canContinue() else { return }
        
        var shouldRetry = false
        var stopsAccount = false
        var retryDelay: TimeInterval = 0
        var terminalHealthCategory = syncHealthCategory(for: error)
        let terminalZoneDeletionKind = (error as? ChangeFeedMigrationError)?
            .deletionKind
        // The original error can be a local/Foundation wrapper. Its nested
        // CloudKit constraints still govern this existing recovery policy.
        let constraints = CloudKitRetryConstraints(error)
        guard canContinue() else { return }

        // A coalesced local mutation may wake an ordinary failed local drain,
        // but cannot create a fresh retry budget for a transport failure or
        // bypass a recovery prerequisite. Keep this independent of diagnostic
        // health categories, which do not grant retry authority.
        let allowsLocalWorkTail: Bool
        switch error {
        case let syncError as SyncError:
            allowsLocalWorkTail = syncError == .inboundBoundaryChanged
        case is ChangeFeedMigrationError, is BigSyncCloudAccountPortError,
             is BigSyncHandledMutationRetryError, is BigSyncSemanticUploadConflictError:
            allowsLocalWorkTail = false
        default:
            allowsLocalWorkTail = constraints.isErrorGraphComplete && constraints.codes.isEmpty
                && (error as? CloudKitChangeFeedError) != .corruptCursor
        }

        if error is RealmSwiftInboundTargetChangedError {
            // A non-journaled local write invalidated an inbound selection.
            // The page cursor did not commit. Replay through ordinary fetch,
            // with a delay so sustained cache writers cannot spin the drain.
            shouldRetry = true
            retryDelay = 1
        } else if let migrationError = error as? ChangeFeedMigrationError,
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
        } else if !constraints.codes.isEmpty {
            let codes = constraints.codes
            var recoveryRequestIsDurable = !constraints.requestsTokenRecovery
            if constraints.requestsTokenRecovery {
                logger.info("QSCloudKitSynchronizer >> Change token expired, requesting a fenced server-first tracking rebuild...")
                guard canContinue() else { return }
                if let context = failureContext {
                    do {
                        try checkRunContext(context)
                        try requestChangeFeedRecovery(context: context)
                        guard canContinue() else { return }
                        try resetDatabaseToken()
                        guard canContinue() else { return }
                        for adapter in modelAdapters {
                            try checkRunContext(context)
                            try await adapter.saveToken(nil)
                            guard canContinue() else { return }
                            try checkRunContext(context)
                        }
                        recoveryRequestIsDurable = true
                        shouldRetry = true
                    } catch is CancellationError {
                        settleCancellationIfCurrentAttempt(attemptID)
                        return
                    } catch {
                        guard canContinue() else { return }
                        logger.error("QSCloudKitSynchronizer >> Could not durably prepare token recovery: \(error)")
                    }
                }
            }

            guard canContinue() else { return }
            // Account stops take precedence over *retrying*, not over recording
            // a local recovery request. Do not issue another account/CloudKit
            // request here; CKAccountChanged reopens the availability gate.
            if codes.contains(.notAuthenticated) {
                shouldRetry = false
                stopsAccount = true
                terminalHealthCategory = .notAuthenticated
            } else if codes.contains(.accountTemporarilyUnavailable) {
                shouldRetry = false
                stopsAccount = true
                clearPersistedTransientRetryState()
                terminalHealthCategory = .accountTemporarilyUnavailable
            } else if constraints.requiresDeferredRetry {
                consecutiveTransientCloudKitFailures += 1
                retryDelay = CloudKitRetryBackoff.delay(
                    serverMinimum: constraints.serverMinimum,
                    consecutiveFailures: consecutiveTransientCloudKitFailures
                )
                if let context = failureContext {
                    persistTransientRetryState(
                        context: context,
                        notBefore: Date().addingTimeInterval(retryDelay),
                        consecutiveFailures: consecutiveTransientCloudKitFailures
                    )
                }
                guard canContinue() else { return }
                logger.warning("QSCloudKitSynchronizer >> CloudKit retry constrained to \(retryDelay.rounded()) seconds or later.")
                guard canContinue() else { return }
                reduceBatchSize()
                shouldRetry = recoveryRequestIsDurable
            } else if !constraints.requestsTokenRecovery {
                logger.error("QSCloudKitSynchronizer >> Error: \(error)")
            }
        } else if error as? CloudKitChangeFeedError == .corruptCursor {
            logger.warning(
                "QSCloudKitSynchronizer >> Persisted CloudKit cursor was corrupt; requesting a fenced server-first tracking rebuild."
            )
            guard canContinue() else { return }
            var recoveryRequestIsDurable = false
            if let context = failureContext {
                do {
                    try requestChangeFeedRecovery(context: context)
                    guard canContinue() else { return }
                    recoveryRequestIsDurable = true
                } catch {
                    logger.error(
                        "QSCloudKitSynchronizer >> Could not durably request corrupt-cursor recovery: \(error)"
                    )
                }
            }
            guard canContinue() else { return }
            if recoveryRequestIsDurable {
                do {
                    try resetDatabaseToken()
                    guard canContinue() else { return }
                    for adapter in modelAdapters {
                        try checkSynchronizationAttempt(attemptID)
                        try await adapter.saveToken(nil)
                        guard canContinue() else { return }
                        try checkSynchronizationAttempt(attemptID)
                    }
                    shouldRetry = true
                } catch {
                    guard canContinue() else { return }
                    logger.error(
                        "QSCloudKitSynchronizer >> Failed to clear corrupt adapter cursor: \(error)"
                    )
                }
            }
        }

        guard canContinue() else { return }
        // Keep known recovery intent and retry floors, but never treat a
        // bounded, incomplete error scan as permission for another attempt.
        shouldRetry = shouldRetry && constraints.isErrorGraphComplete
        // Keep the drain owned through the health notification. Its observer
        // may cancel or replace this attempt, and must not coalesce a successor
        // into a drain that has already dropped its running state.
        if !stopsAccount {
            syncing = shouldRetry && !cancelSync
            synchronizationTask = nil
        }

        if let context = failureContext {
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
                settleCancellationIfCurrentAttempt(attemptID)
                return
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed to persist sync health: \(error)")
            }
        }

        guard canContinue() else { return }
        if stopsAccount {
            finishAccountStoppedSynchronization(
                error: error, attemptID: attemptID, context: failureContext,
                authorityGeneration: failureAuthorityGeneration,
                category: terminalHealthCategory
            )
            return
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
                allowsLocalWorkTail &&
                !cancelledDueToUnauthentication
            finishSynchronizationDrain(with: .failure(error))
            // Failure observers may synchronously admit a successor. This
            // terminal tail owns only the attempt whose waiters it settled.
            guard canContinue() else { return }
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
            // Do not retain the synchronizer during a potentially long sleep.
            // This closure must not capture the local canContinue function.
            if retryDelay > 0 {
                do {
                    try await BigSyncRetrySleep.sleep(for: retryDelay)
                } catch {
                    return
                }
            }
            guard let self, !Task.isCancelled, !cancelSync,
                  synchronizationAttemptID == attemptID else { return }
            retrySleepUntil = nil
            synchronizationTask = nil
            syncing = false
            synchronizationRequestedWhileRunning = false
            logger.info("QSCloudKitSynchronizer >> Retrying synchronization...")
            guard !Task.isCancelled, !cancelSync,
                  synchronizationAttemptID == attemptID else { return }
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
enum BigSyncRetrySleepError: Error {
    case invalidDelay
}

enum BigSyncRetrySleep {
    /// Keep each integer conversion comfortably below UInt64.max. Large
    /// server-directed delays remain large: they are slept in consecutive
    /// chunks rather than clamped to an earlier retry deadline.
    static let maximumChunkSeconds: TimeInterval = 60 * 60

    static func nextChunkNanoseconds(
        remainingSeconds: TimeInterval
    ) -> UInt64? {
        guard !remainingSeconds.isNaN, remainingSeconds > 0 else {
            return nil
        }
        let chunkSeconds = remainingSeconds.isFinite
            ? min(remainingSeconds, maximumChunkSeconds)
            : maximumChunkSeconds
        // Round upward so fractional nanoseconds can never shorten the
        // server-requested minimum.
        let nanoseconds = (chunkSeconds * 1_000_000_000).rounded(.up)
        guard nanoseconds.isFinite, nanoseconds > 0,
              nanoseconds <= Double(UInt64.max) else {
            return nil
        }
        return UInt64(nanoseconds)
    }

    static func sleep(for delaySeconds: TimeInterval) async throws {
        guard !delaySeconds.isNaN, delaySeconds >= 0 else {
            throw BigSyncRetrySleepError.invalidDelay
        }
        var remaining = delaySeconds
        while remaining > 0 {
            try Task.checkCancellation()
            guard let nanoseconds = nextChunkNanoseconds(
                remainingSeconds: remaining
            ) else {
                throw BigSyncRetrySleepError.invalidDelay
            }
            try await Task.sleep(nanoseconds: nanoseconds)
            if remaining.isFinite {
                let consumed = Double(nanoseconds) / 1_000_000_000
                remaining = max(0, remaining - consumed)
            }
        }
    }
}

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
        // All callers must revalidate account-routed awaits before reaching
        // this local write. Also reject obsolete captured contexts here so a
        // late failure can never mutate either a replacement or old scope.
        do {
            try checkRunContext(context)
        } catch {
            return error
        }
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
        let constraints = CloudKitRetryConstraints(error)
        guard !constraints.blocksAccountOperations else { return nil }
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
    func loadTokens(
        for zoneIDs: [CKRecordZone.ID],
        attemptID expectedAttemptID: UUID? = nil
    ) async throws -> [CKRecordZone.ID] {
        let attemptID = expectedAttemptID ?? synchronizationAttemptID
        let runID = synchronizationRunID
        let context = activeRunContext
        // Snapshot only registered adapters before any cursor read can suspend.
        // An old read must neither adopt a replacement adapter nor publish its
        // cursor into a successor run's in-memory page state.
        let adapters = zoneIDs.compactMap { zoneID in
            modelAdapterDictionary[zoneID].map { (zoneID: zoneID, adapter: $0) }
        }
        func validateOwner() throws {
            try checkSynchronizationAttempt(attemptID)
            guard synchronizationRunID == runID, activeRunContext == context,
                  adapters.allSatisfy({ modelAdapterDictionary[$0.zoneID] === $0.adapter }) else {
                throw CancellationError()
            }
            if let context { try checkRunContext(context) }
        }
        try validateOwner()
        var loadedTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
        for (zoneID, adapter) in adapters {
            let token = await adapter.serverChangeToken
            try validateOwner()
            loadedTokens[zoneID] = token
        }
        // No suspension or callout between final validation and publication.
        // Rejection preserves the prior map; success still replaces it, even
        // for empty input or a registered adapter with no persisted cursor.
        try validateOwner()
        activeZoneTokens = loadedTokens
        return adapters.map(\.zoneID)
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
    }
    
    func shouldRetryUpload(for error: NSError) -> Bool {
        let attemptID = synchronizationAttemptID
        let constraints = CloudKitRetryConstraints(error)
        let isZoneLoss = isZoneNotFoundOrDeletedError(error)
        do { try checkSynchronizationAttempt(attemptID) }
        catch { return false }
        guard constraints.isErrorGraphComplete,
              !constraints.blocksAccountOperations,
              !constraints.requestsTokenRecovery,
              !constraints.requiresDeferredRetry else { return false }
        if constraints.containsOnlySizeLimitFailures {
            // A singleton limit cannot be repaired by resending it. Reducible
            // limits are normally handled inside the bounded shrinking drain.
            return batchSize > 1 && uploadRetries < 5
        }
        if isZoneLoss {
            return uploadRetries < 5
        }
        // Record conflict budgets belong to the mutation drain, not a fresh
        // outer attempt that would reset their counters.
        return false
    }

    func isLimitExceededError(_ error: NSError) -> Bool {
        CloudKitRetryConstraints(error).codes.contains(.limitExceeded)
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
            await failSynchronization(error: SyncError.cancelled, for: attemptID)
            return
        }

        do {
            try checkSynchronizationAttempt(attemptID)
            postNotification(.SynchronizerWillFetchChanges)
            try checkSynchronizationAttempt(attemptID)

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
            if activeSynchronizationMode == .sync {
                let shouldUpload = !afterUpload || modelAdapters.contains(where: { $0.hasChanges })
                // The pending-state getter is a synchronous callback boundary.
                try checkSynchronizationAttempt(attemptID)
                if shouldUpload {
                    try await uploadChanges()
                    return
                }
            } else {
                try await processFetchedChanges()
                try await revalidateActiveRunContext(for: attemptID)
            }
            // Both terminal paths commit their cursor under the original
            // owner. A persistence callback must not hand an old fetch to a
            // terminal method that captures a newly installed attempt.
            try checkSynchronizationAttempt(attemptID)
            try persistDatabaseToken(token)
            try checkSynchronizationAttempt(attemptID)
            await changesFinishedSynchronizing()
        } catch {
            guard synchronizationAttemptID == attemptID else { return }
            await failSynchronization(error: error, for: attemptID)
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
                try checkSynchronizationAttempt(attemptID)
                let classification = CloudKitLossClassifier.classify(
                    error: error, defaultZoneID: recordZoneID
                )
                if let context = activeRunContext,
                   let disposition = classification.zoneDispositions[recordZoneID],
                   !CloudKitRetryConstraints(error).blocksAccountOperations {
                    try await revalidateRunContext(context)
                    if let lifecycleError = applyCloudKitLoss(
                        disposition,
                        zoneID: recordZoneID,
                        context: context,
                        allowsEncryptedBootstrapAbsence:
                            isEncryptedDataResetRecoveryActive
                    ) {
                        throw lifecycleError
                    }
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
        try checkSynchronizationAttempt(attemptID)
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
            for: Array(changedZoneIDs), attemptID: attemptID
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
        try await fetchZoneChanges(zoneIDsToFetch, attemptID: attemptID)
        try await revalidateActiveRunContext(for: attemptID)
        return pageCursor
    }

    @BigSyncBackgroundActor
    func fetchZoneChanges(
        _ zoneIDs: [CKRecordZone.ID],
        attemptID expectedAttemptID: UUID? = nil
    ) async throws {
        let attemptID = expectedAttemptID ?? synchronizationAttemptID
        let runID = synchronizationRunID
        let context = activeRunContext
        try checkSynchronizationAttempt(attemptID)
        let adapters = zoneIDs.compactMap { zoneID in
            modelAdapterDictionary[zoneID].map { (zoneID: zoneID, adapter: $0) }
        }
        defer {
            // A cancelled request may unwind after another run has begun.
            // Clear only the processor errors belonging to this fetch's owner.
            if synchronizationAttemptID == attemptID,
               synchronizationRunID == runID,
               activeRunContext == context,
               adapters.allSatisfy({ modelAdapterDictionary[$0.zoneID] === $0.adapter }) {
                changeRequestProcessor.clearErrors()
            }
        }

        for (zoneID, adapter) in adapters {
            var pageCursor = activeZoneTokens[zoneID]
            var pageIndex = 0
            func validateFetchOwner() throws {
                try checkSynchronizationAttempt(attemptID)
                guard synchronizationRunID == runID,
                      activeRunContext == context,
                      modelAdapterDictionary[zoneID] === adapter else {
                    throw CancellationError()
                }
                if let context { try checkRunContext(context) }
            }
            try validateFetchOwner()
            let isServerBootstrap: Bool
            if let migrating = adapter as? any ChangeFeedResetMigrating {
                isServerBootstrap =
                    await migrating.isChangeFeedServerBootstrapActive()
                try await revalidateActiveRunContext(for: attemptID)
                try validateFetchOwner()
            } else {
                isServerBootstrap = false
            }

            var moreComing = true
            while moreComing {
                try validateFetchOwner()
                try await revalidateActiveRunContext(for: attemptID)
                try validateFetchOwner()
                let page: CloudKitRecordZoneChangePage
                do {
                    page = try await changeFeed.recordZoneChanges(
                        in: zoneID,
                        since: pageCursor,
                        desiredKeys: nil,
                        resultsLimit: 200
                    )
                } catch {
                    try validateFetchOwner()
                    guard let context = activeRunContext,
                          !CloudKitRetryConstraints(error).blocksAccountOperations else {
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
                    try await revalidateRunContext(context)
                    try validateFetchOwner()
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
                try validateFetchOwner()
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

                try validateFetchOwner()
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
                try validateFetchOwner()
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
                try validateFetchOwner()
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
                try validateFetchOwner()

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
                try validateFetchOwner()
                try await adapter.commitInboundPage(InboundPageCommit(
                    previousCursor: pageCursor,
                    nextCursor: page.cursor,
                    liveResults: normalizedLiveResults,
                    deletionResults: normalizedDeletionResults
                ))
                try await revalidateActiveRunContext(for: attemptID)
                try validateFetchOwner()
                // Deferred relationships are already durable in the adapter's
                // persistence Realm and were proven by commitInboundPage.
                // Apply them only after the cursor commit so a successful
                // application cannot erase the evidence that authorized this
                // page to advance.
                try await adapter.persistImportedChanges()
                try await revalidateActiveRunContext(for: attemptID)
                try validateFetchOwner()
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
        try checkSynchronizationAttempt(attemptID)
        for adapter in modelAdapters {
            try checkSynchronizationAttempt(attemptID)
            try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil)
            try checkSynchronizationAttempt(attemptID)
            try await saveActiveTokenIfNeeded(for: adapter)
            try checkSynchronizationAttempt(attemptID)
        }
    }
}

// MARK: - Upload changes

extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    func uploadChanges() async throws {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        logger.info("QSCloudKitSynchronizer >> Upload changes...")
        reportProgress("upload-start")
        //        debugPrint("# uploadChanges()")
        try checkSynchronizationAttempt(attemptID)

        postNotification(.SynchronizerWillUploadChanges)
        try checkSynchronizationAttempt(attemptID)

        try await uploadChanges() { [weak self] (error) in
            try Task.checkCancellation()
            guard let self,
                  synchronizationAttemptID == attemptID else { return }
            
            if let error {
                if let context = activeRunContext,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: recordZoneID,
                    context: context
                   ) {
                    await failSynchronization(error: lifecycleError, for: attemptID)
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
                if shouldRetryUpload(for: error as NSError) {
                    //                    print("# uploadChanges() failed, retrying via fetchChanges()")
                    uploadRetries += 1
                    logger.info("QSCloudKitSynchronizer >> Retrying upload due to error \((error as NSError).description.prefix(200)), beginning with fetching changes...")
                    await fetchChanges()
                } else {
                    await failSynchronization(error: error, for: attemptID)
                }
            } else {
                // The database token is a commit barrier for the zone changes it
                // announced. Persist it only after every downloaded zone change
                // has been applied and its zone token has been saved.
                try await revalidateActiveRunContext(for: attemptID)
                try persistDatabaseToken(serverChangeToken)
                reportProgress("upload-completed")
#if DEBUG
                try await processKillCheckpointHandler?(
                    .localAcknowledgementBeforeTerminalPublication
                )
#endif
                try checkSynchronizationAttempt(attemptID)
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
        let attemptID = synchronizationAttemptID
        let operationError: Error?
        do {
            try checkSynchronizationAttempt(attemptID)
            for adapter in modelAdapters {
                try checkSynchronizationAttempt(attemptID)
                try await synchronizeAdapter(adapter)
                try checkSynchronizationAttempt(attemptID)
            }
            operationError = nil
        } catch {
            operationError = error
        }
        // Delivery errors belong to the caller, not to the operation just
        // completed. Never feed a throwing callback back into itself.
        try await completion(operationError)
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
        let operationError: Error?
        do {
            try await prepareRecordZoneID(zoneID, attemptID: attemptID)
            operationError = nil
        } catch {
            operationError = error
        }
        // A downstream failure is not evidence that fetching or creating the
        // zone failed. Do not revalidate, recover the zone, or deliver twice.
        try await completion(operationError)
    }

    @BigSyncBackgroundActor
    private func prepareRecordZoneID(
        _ zoneID: CKRecordZone.ID,
        attemptID: UUID
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
        } catch {
            // A returned account stop forbids further CloudKit work,
            // including an otherwise routine account revalidation.
            try checkSynchronizationAttempt(attemptID)
            if !CloudKitRetryConstraints(error).blocksAccountOperations {
                try await revalidateActiveRunContext(for: attemptID)
            }

            guard !CloudKitRetryConstraints(error).blocksAccountOperations,
                  let context = activeRunContext else {
                throw error
            }
            let classification = CloudKitLossClassifier.classify(
                error: error,
                defaultZoneID: zoneID
            )
            guard let disposition = classification.zoneDispositions[zoneID]
            else {
                throw error
            }
            if let lifecycleError = applyCloudKitLoss(
                disposition,
                zoneID: zoneID,
                context: context,
                allowsEncryptedBootstrapAbsence:
                    isEncryptedDataResetRecoveryActive
            ) {
                throw lifecycleError
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
            } catch {
                try checkSynchronizationAttempt(attemptID)
                if !CloudKitRetryConstraints(error).blocksAccountOperations {
                    try await revalidateRunContext(context)
                }
                if !CloudKitRetryConstraints(error).blocksAccountOperations,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: zoneID,
                    context: context,
                    allowsEncryptedBootstrapAbsence: false
                ) {
                    throw lifecycleError
                } else {
                    throw error
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func synchronizeAdapter(_ adapter: ModelAdapter) async throws {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        for priorityEntityType in adapter.priorityEntityTypeNames {
            try checkSynchronizationAttempt(attemptID)
            try await runSyncPhase(for: adapter, restrictedToEntityType: priorityEntityType)
            try checkSynchronizationAttempt(attemptID)
        }

        try checkSynchronizationAttempt(attemptID)
        try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil)
        try checkSynchronizationAttempt(attemptID)
        try await saveActiveTokenIfNeeded(for: adapter)
        try checkSynchronizationAttempt(attemptID)
        try await uploadRecordsIfNeeded(adapter: adapter, restrictedToEntityType: nil)
        try checkSynchronizationAttempt(attemptID)
        try await uploadDeletionsIfNeeded(adapter: adapter, restrictedToEntityType: nil)
        try checkSynchronizationAttempt(attemptID)
    }

    @BigSyncBackgroundActor
    func runSyncPhase(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        try await runFetchedChangesPhase(for: adapter, restrictedToEntityType: restrictedEntityType)
        try checkSynchronizationAttempt(attemptID)
        try await uploadRecordsIfNeeded(adapter: adapter, restrictedToEntityType: restrictedEntityType)
        try checkSynchronizationAttempt(attemptID)
        try await uploadDeletionsIfNeeded(adapter: adapter, restrictedToEntityType: restrictedEntityType)
        try checkSynchronizationAttempt(attemptID)
    }

    @BigSyncBackgroundActor
    func runFetchedChangesPhase(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        let changeRequestProcessor = changeRequestProcessor
        try await changeRequestProcessor.finishProcessing(
            for: adapter,
            restrictedToEntityType: restrictedEntityType
        )
        try checkSynchronizationAttempt(attemptID)
        if let firstError = changeRequestProcessor.getErrors().first {
            changeRequestProcessor.clearErrors()
            throw firstError
        }
        do {
            try await adapter.persistImportedChanges()
        } catch {
            // A retired persistence callback cannot clear a successor's
            // processor errors, even when it returns an ordinary error.
            try checkSynchronizationAttempt(attemptID)
            changeRequestProcessor.clearErrors()
            throw error
        }
        try checkSynchronizationAttempt(attemptID)
        changeRequestProcessor.clearErrors()
    }

    @BigSyncBackgroundActor
    func saveActiveTokenIfNeeded(for adapter: ModelAdapter) async throws {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        if let token = activeZoneToken(zoneID: adapter.recordZoneID) {
            try await revalidateActiveRunContext(for: attemptID)
            try await adapter.saveToken(token)
            // A committed token remains committed. Reject only this stale
            // continuation before its caller can begin another mutation phase.
            try checkSynchronizationAttempt(attemptID)
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
