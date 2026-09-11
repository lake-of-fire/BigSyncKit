import CloudKit
import Foundation

extension CloudKitSynchronizer {
    /// A pending/live cutoff owner, not a certificate that peers are drained.
    /// Keep it for exact abort even when establishment is cancelled or fails.
    public struct PostBarrierOutboundQuiescence: Equatable, Sendable {
        public let identifier: UUID
        public let writerBarrierEvidenceID: String
        // Never recycle a live capability when the same synchronizer reacquires
        // the same durable barrier. Old callbacks must not own the resumed gate.
        internal let ownershipID = UUID()
        internal let issuerID: UUID
        internal let principal: BigSyncOutboundPrincipal
    }

    internal var outboundQuiescenceCoordinator: BigSyncOutboundQuiescenceCoordinator {
        BigSyncOutboundQuiescenceCoordinator(
            sharedStateBaseURL: backupDetectionBaseURL ?? BackupDetection.defaultSentinelURL(
                namespace: durableStateNamespace).deletingLastPathComponent(),
            durableStateNamespace: durableStateNamespace)
    }

    internal func currentOutboundPrincipal(for context: RunContext? = nil) throws -> BigSyncOutboundPrincipal {
        try Task.checkCancellation()
        let candidateLease: BigSyncAccountScopeLease?
        if let context { candidateLease = try outboundAccountScopeLease(for: context) }
        else { candidateLease = try accountScopeLease() }
        guard let lease = candidateLease,
              let installation = BackupDetection.installationIdentifier(
                namespace: durableStateNamespace, sharedSentinelBaseURL: backupDetectionBaseURL) else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let binding = try activeReplicaBindingGenerationIdentifierForRun(
            accountScopeIdentifier: lease.accountScopeIdentifier)
        if binding != nil {
            guard let state = try BigSyncReplicaBindingStateStore.load(
                store: keyValueStore, key: replicaBindingStateKey),
                state.installationIdentityDigest == BigSyncReplicaBindingStateStore.installationIdentityDigest(for: installation) else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        return BigSyncOutboundPrincipal(durableStateNamespace: durableStateNamespace,
            installationIdentifier: installation, accountScopeIdentifier: lease.accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding, accountInvalidationGeneration: lease.invalidationGeneration)
    }

    /// The domain must already have durably fenced every local author in its
    /// write transaction. This synchronously publishes the outbound fence and
    /// returns the exact token needed for cleanup; no peer-drain claim yet.
    public func beginPostBarrierOutboundQuiescence(
        writerBarrierEvidenceID: String
    ) throws -> PostBarrierOutboundQuiescence {
        guard !syncing, !synchronizationDrainIsActive, postBarrierOutboundLease == nil,
              postBarrierDrainAuthorization == nil, completedPostBarrierDrain == nil,
              postBarrierSnapshotIdentifierProvider != nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundPrincipal()
        let owner = try outboundQuiescenceCoordinator.begin(principal: principal,
            writerBarrierEvidenceID: writerBarrierEvidenceID)
        let token = PostBarrierOutboundQuiescence(identifier: owner.barrier.identifier,
            writerBarrierEvidenceID: writerBarrierEvidenceID, issuerID: synchronizationReceiptIssuerID,
            principal: principal)
        postBarrierOutboundLease = owner
        postBarrierOutboundTicket = token
        return token
    }

    /// Wait for existing peer batch scopes to exit, then arm exactly the next
    /// owning drain. Inbound fetching remains available; peers cannot prepare
    /// or submit a new outbound batch. Failure leaves the fence for exact abort
    /// or recovery. The caller owns domain writer-barrier cleanup separately.
    public func establishPostBarrierDrain(
        quiescence token: PostBarrierOutboundQuiescence
    ) async throws -> PostBarrierDrainAuthorization {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundEstablishmentID == nil,
              postBarrierDrainAuthorization == nil, completedPostBarrierDrain == nil,
              let owner = matchingOutboundOwner(token) else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let requestID = UUID()
        let attemptID = synchronizationAttemptID
        postBarrierOutboundEstablishmentID = requestID
        defer {
            if postBarrierOutboundEstablishmentID == requestID { postBarrierOutboundEstablishmentID = nil }
        }
        guard try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try await outboundQuiescenceCoordinator.waitUntilDrained(owner) { @BigSyncBackgroundActor in
            guard self.matchingOutboundOwner(token) === owner,
                  self.postBarrierOutboundEstablishmentID == requestID,
                  self.synchronizationAttemptID == attemptID,
                  !self.syncing, !self.synchronizationDrainIsActive,
                  try self.currentOutboundPrincipal() == token.principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        guard matchingOutboundOwner(token) === owner,
              postBarrierOutboundEstablishmentID == requestID,
              synchronizationAttemptID == attemptID,
              !syncing, !synchronizationDrainIsActive,
              try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try outboundQuiescenceCoordinator.validateDrained(owner, principal: token.principal)
        guard owner.barrier.phase == .preparing else { throw BigSyncOutboundQuiescenceError.recoveryRequired }
        guard let binding = token.principal.replicaBindingGenerationIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try owner.armFinalDrain()
        let authorization = PostBarrierDrainAuthorization(writerBarrierEvidenceID: token.writerBarrierEvidenceID,
            issuerID: synchronizationReceiptIssuerID, authorizationID: UUID(),
            accountScopeIdentifier: token.principal.accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding,
            accountInvalidationGeneration: token.principal.accountInvalidationGeneration,
            outboundQuiescenceIdentifier: token.identifier)
        postBarrierDrainAuthorization = authorization
        return authorization
    }

    /// Full drain validation plus a durable one-way transition BEFORE the
    /// domain's first reservation/CAS-capable write. Even a failed reservation
    /// now needs explicit domain recovery; generic abort cannot reopen legacy
    /// outbound work in the window between transport and Realm commits.
    @discardableResult
    public func requirePostBarrierDrainRecoveryBeforeReservation(
        _ completed: CompletedPostBarrierDrain
    ) async throws -> BigSyncOutboundQuiescenceSnapshot {
        try await revalidateCompletedPostBarrierDrain(completed)
        try validatePostBarrierDrainPrincipal(completed)
        guard !syncing, !synchronizationDrainIsActive,
              try !adaptersHavePendingChangesAtTerminalBoundary(),
              let adapter = modelAdapters.first,
              try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: completed.accountScopeIdentifier,
                replicaBindingGenerationIdentifier: completed.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier, databaseScope: database.databaseScope
              ) == completed.consumedServerBoundaryIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        guard let identifier = completed.outboundQuiescenceIdentifier,
              postBarrierOutboundTicket?.identifier == identifier,
              let owner = postBarrierOutboundLease else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.requireRecovery(owner)
        return try outboundQuiescenceCoordinator.snapshot()
    }

    /// After the host has durably committed the new authority/bootstrap, allow
    /// this exact live owner to publish source journals while every peer remains
    /// fenced. The returned checkpoint is the new durable source-publication
    /// phase and should be retained for crash/restart recovery.
    @discardableResult
    public func beginPostBarrierSourcePublication(
        _ token: PostBarrierOutboundQuiescence,
        expected: BigSyncOutboundQuiescenceSnapshot,
        sourcePublicationEvidenceID: String
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierDrainAuthorization == nil, outboundRecoveryID == nil,
              let owner = matchingOutboundOwner(token),
              try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return try outboundQuiescenceCoordinator.authorizeSourcePublication(
            owner,
            expected: expected,
            evidenceID: sourcePublicationEvidenceID
        )
    }

    /// Relinquish only this token's pre-reservation fence. Domain cancellation
    /// must first ensure it will not persist a reservation; this is not an
    /// automatic side effect of cancelSynchronization or permit revocation.
    @discardableResult
    public func abortPostBarrierOutboundQuiescence(_ token: PostBarrierOutboundQuiescence) throws -> Bool {
        guard let owner = matchingOutboundOwner(token) else { return false }
        guard !syncing, !synchronizationDrainIsActive else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        guard try currentOutboundPrincipal() == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try outboundQuiescenceCoordinator.abort(owner)
        retireOutboundCapabilities(token)
        return true
    }

    /// Drop live ownership WITHOUT removing durable state. In-flight batches
    /// retain their lease until their real transport/callback scopes unwind.
    /// A replacement worker can then take explicit recovery ownership.
    @discardableResult
    public func abandonPostBarrierOutboundQuiescence(_ token: PostBarrierOutboundQuiescence) -> Bool {
        guard let owner = matchingOutboundOwner(token) else { return false }
        owner.sealOutboundAdmission()
        retireOutboundCapabilities(token)
        return true
    }

    public func outboundQuiescenceSnapshot() throws -> BigSyncOutboundQuiescenceSnapshot {
        try outboundQuiescenceCoordinator.snapshot()
    }

    /// Resolve a live owner's fence only after the host has committed its
    /// recovery/transition and retired legacy writers. Evidence must also settle
    /// every indeterminate submission in this exact snapshot, if any. This is
    /// never a generic cancellation API and never mints an aggregate receipt.
    public func resolvePostBarrierOutboundQuiescence(
        _ token: PostBarrierOutboundQuiescence,
        expected: BigSyncOutboundQuiescenceSnapshot,
        recoveryEvidenceID: String
    ) throws {
        guard !syncing, !synchronizationDrainIsActive,
              let owner = matchingOutboundOwner(token),
              try currentOutboundPrincipal() == token.principal else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.resolveOwned(owner, expected: expected, evidenceID: recoveryEvidenceID)
        retireOutboundCapabilities(token)
    }

    /// Resume a committed domain handoff after process or worker loss. A
    /// recoveryRequired checkpoint may be promoted only after the host proves
    /// the domain commit won the crash race. An existing sourcePublication
    /// checkpoint also requires proof covering every outstanding submission.
    /// Neither path opens peers or re-arms an aggregate cutoff.
    public func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        let token = try await resumePostBarrierSourcePublication(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in },
            authorizingResume: authorizingResume
        )
        do {
            try Task.checkCancellation()
        } catch {
            abandonPostBarrierOutboundQuiescence(token)
            throw error
        }
        return token
    }

    internal func resumePostBarrierSourcePublication(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingResume: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws -> PostBarrierOutboundQuiescence {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, postBarrierOutboundTicket == nil,
              postBarrierDrainAuthorization == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundPrincipal()
        guard let persistedBarrier = expected.barrier,
              (persistedBarrier.phase == .recoveryRequired
                || persistedBarrier.phase == .sourcePublication),
              persistedBarrier.principal == principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            try revalidatingExternalOwner()
            guard outboundRecoveryID == requestID,
                  synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundPrincipal() == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        try validateOwnership()
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let replayedCheckpoint = try await replayRecoverableOutboundSubmissions(
            recovery,
            principal: principal,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                try validateOwnership()
            }
        )
        try validateOwnership()
        let evidence = try await authorizingResume(replayedCheckpoint)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        let owner = try outboundQuiescenceCoordinator.resumeSourcePublication(
            recovery,
            principal: principal,
            recoveryEvidenceID: evidence
        )
        let token = PostBarrierOutboundQuiescence(
            identifier: owner.barrier.identifier,
            writerBarrierEvidenceID: owner.barrier.writerBarrierEvidenceID,
            issuerID: synchronizationReceiptIssuerID,
            principal: principal
        )
        postBarrierOutboundLease = owner
        postBarrierOutboundTicket = token
        return token
    }

    /// Crash/account/restart recovery. Ownership is held across the host's
    /// asynchronous domain proof; snapshot and current principal are compared
    /// again afterwards. A thrown/cancelled proof leaves the fence untouched.
    /// The proof must establish that no outstanding request can later commit;
    /// elapsed time or a single fetch alone is not settlement evidence.
    public func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        try await recoverOutboundQuiescence(
            expected: expected,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in },
            authorizingRecovery: authorizingRecovery
        )
    }

    /// Internal host wrapper used when another lifecycle owner (for example the
    /// shared background worker) must remain current across every suspension.
    /// The external owner is rechecked after the final account lookup and
    /// immediately before the durable gate can be reopened.
    internal func recoverOutboundQuiescence(
        expected: BigSyncOutboundQuiescenceSnapshot,
        revalidatingExternalOwner: @Sendable @BigSyncBackgroundActor () throws -> Void,
        authorizingRecovery: @Sendable @BigSyncBackgroundActor (BigSyncOutboundQuiescenceSnapshot) async throws -> String
    ) async throws {
        guard !syncing, !synchronizationDrainIsActive,
              postBarrierOutboundLease == nil, outboundRecoveryID == nil else {
            throw BigSyncOutboundQuiescenceError.busy
        }
        let principal = try currentOutboundRecoveryPrincipal()
        let attemptID = synchronizationAttemptID
        let requestID = UUID()
        let recovery = try outboundQuiescenceCoordinator.takeRecoveryOwnership(expected: expected)
        outboundRecoveryID = requestID
        defer { if outboundRecoveryID == requestID { outboundRecoveryID = nil } }
        func validateOwnership() throws {
            try revalidatingExternalOwner()
            guard outboundRecoveryID == requestID, synchronizationAttemptID == attemptID,
                  !syncing, !synchronizationDrainIsActive,
                  try currentOutboundRecoveryPrincipal() == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
        }
        try validateOwnership()
        let account = try await accountIdentifierProvider()
        try validateOwnership()
        guard Self.accountScopeIdentifier(for: account) == principal.accountScopeIdentifier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        let replayedCheckpoint = try await replayRecoverableOutboundSubmissions(
            recovery,
            principal: principal,
            revalidatingExternalOwner: { @BigSyncBackgroundActor in
                try validateOwnership()
            }
        )
        try validateOwnership()
        let evidence = try await authorizingRecovery(replayedCheckpoint)
        try validateOwnership()
        let confirmedAccount = try await accountIdentifierProvider()
        try validateOwnership()
        guard confirmedAccount == account else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.resolveRecovery(recovery, evidenceID: evidence)
    }

    private func currentOutboundRecoveryPrincipal() throws -> BigSyncOutboundPrincipal {
        // A restore-reconciliation run can validate transport identity while
        // still withholding the public domain-writer lease. Allow explicit
        // recovery of its closed gate; otherwise a restored client could need
        // to complete an upload in order to obtain permission to recover it.
        if try accountScopeLease() != nil { return try currentOutboundPrincipal() }
        guard let context = activeRunContext else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        return try currentOutboundPrincipal(for: context)
    }

    internal func admitOutboundBatch(for attemptID: UUID) async throws -> BigSyncOutboundBatchLease {
        while true {
            try checkSynchronizationAttempt(attemptID)
            guard let context = activeRunContext else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            let principal = try currentOutboundPrincipal(for: context)
            guard context.accountScopeIdentifier == principal.accountScopeIdentifier,
                  context.replicaBindingGenerationIdentifier == principal.replicaBindingGenerationIdentifier else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            let owner: BigSyncOutboundQuiescenceLease?
            if let authorization = postBarrierDrainAuthorization,
               let identifier = authorization.outboundQuiescenceIdentifier {
                guard postBarrierOutboundTicket?.identifier == identifier else {
                    throw BigSyncOutboundQuiescenceError.staleAuthority
                }
                owner = postBarrierOutboundLease
            } else if context.sourcePublicationOwnershipID != nil {
                try validateSourcePublicationRun(context)
                owner = postBarrierOutboundLease
            } else {
                owner = nil
            }
            do { return try outboundQuiescenceCoordinator.admit(principal: principal, owner: owner) }
            catch BigSyncOutboundQuiescenceError.busy { try await Task.sleep(nanoseconds: 1_000_000) }
        }
    }

    private func outboundRecoveryDescriptor(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String],
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity?
    ) -> BigSyncOutboundSubmissionRecoveryDescriptor {
        let saves = records.map { record in
            BigSyncOutboundSubmissionItem(
                mutation: .save,
                recordName: record.recordID.recordName,
                zoneName: record.recordID.zoneID.zoneName,
                zoneOwnerName: record.recordID.zoneID.ownerName,
                recordType: record.recordType,
                preparedGeneration: preparedGenerations[record.recordID.recordName],
                priorRecordChangeTag: record.recordChangeTag
            )
        }
        let deletes = recordIDs.map { recordID in
            BigSyncOutboundSubmissionItem(
                mutation: .delete,
                recordName: recordID.recordName,
                zoneName: recordID.zoneID.zoneName,
                zoneOwnerName: recordID.zoneID.ownerName,
                recordType: nil,
                preparedGeneration: preparedGenerations[recordID.recordName],
                priorRecordChangeTag: nil
            )
        }
        return BigSyncOutboundSubmissionRecoveryDescriptor(
            items: saves + deletes,
            transportIdentity: transportIdentity
        )
    }

    internal func modifyRecordsHoldingOutboundLease(
        _ outbound: BigSyncOutboundBatchLease,
        attemptID: UUID,
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String]
    ) async throws -> CloudKitRecordMutationResults {
        let recoverableStore = recordStore as? any CloudKitRecoverableRecordStore
        let preparedTransport = recoverableStore?.prepareRecoverableModifyRecords(
            saving: records,
            deleting: recordIDs,
            savePolicy: .ifServerRecordUnchanged,
            atomically: false
        )
        let recoveryDescriptor = outboundRecoveryDescriptor(
            saving: records,
            deleting: recordIDs,
            preparedGenerations: preparedGenerations,
            transportIdentity: preparedTransport?.transportIdentity
        )
        try await outbound.willSubmitCooperatively(
            recoveryDescriptor: recoveryDescriptor
        )
        do {
            try validateOutboundBatch(outbound, for: attemptID)
            try outbound.validateSubmissionAdmission()
        } catch {
            // No request has entered transport. Cancellation/epoch replacement
            // here cannot manufacture an unknown server outcome.
            try await outbound.didSettleCooperatively()
            throw error
        }
        let results: CloudKitRecordMutationResults
        do {
            if let recoverableStore, let preparedTransport {
                results = try await recoverableStore.executeRecoverableModifyRecords(
                    preparedTransport
                )
            } else {
                results = try await recordStore.modifyRecords(
                    saving: records,
                    deleting: recordIDs,
                    savePolicy: .ifServerRecordUnchanged,
                    atomically: false
                )
            }
        } catch {
            // An operation-wide definitive rejection (for example the batch
            // limit) did not commit either and requires no per-item local ack.
            // Preserve the original retry/error behavior without leaving a
            // phantom indeterminate submission.
            if CloudKitRecordMutationResults.isDefinitiveOperationRejection(error) {
                try await outbound.didSettleCooperatively()
            }
            throw error
        }
        // A definitive server response is not enough to release the durable
        // marker yet. Record it in the batch, then let the caller release the
        // marker only after generation-matched local response processing. If
        // cancellation or an authority fence wins in that window, process death
        // loses this in-memory note but deliberately leaves the durable marker.
        if results.provesDefinitiveSettlement(
            saving: records,
            deleting: recordIDs
        ) {
            try outbound.noteDefinitiveTransportOutcome()
        }
        return results
    }

    internal func validateOutboundBatch(_ batch: BigSyncOutboundBatchLease, for attemptID: UUID) throws {
        try checkSynchronizationAttempt(attemptID)
        guard let context = activeRunContext,
              try currentOutboundPrincipal(for: context) == batch.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try checkRunContext(context)
        try validateSourcePublicationRun(context)
    }

    /// Source receipt issuance is still ordinary publication, but must not
    /// forget the held cutoff or unresolved requests from earlier source runs.
    /// Do not apply this after explicit durable completion/release: an already
    /// issued ordinary receipt retains its existing revalidation contract.
    internal func validateSourcePublicationRun(
        _ context: RunContext, requiresDrained: Bool = false
    ) throws {
        guard let ownershipID = context.sourcePublicationOwnershipID else { return }
        guard let token = postBarrierOutboundTicket,
              token.ownershipID == ownershipID,
              let owner = matchingOutboundOwner(token),
              owner.barrier.phase == .sourcePublication,
              try currentOutboundPrincipal(for: context) == token.principal else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try owner.validateOutboundAdmission(principal: token.principal)
        if requiresDrained {
            try outboundQuiescenceCoordinator.validateDrained(owner, principal: token.principal)
        }
    }

    internal func revalidateOutboundBatch(_ batch: BigSyncOutboundBatchLease, for attemptID: UUID) async throws {
        try validateOutboundBatch(batch, for: attemptID)
        try await revalidateActiveRunContext(for: attemptID)
        try validateOutboundBatch(batch, for: attemptID)
    }

    internal func validatePostBarrierOutboundPrincipal(identifier: UUID) throws {
        guard postBarrierOutboundTicket?.identifier == identifier,
              let owner = postBarrierOutboundLease else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try owner.validate(principal: currentOutboundPrincipal())
    }

    internal func validatePostBarrierOutboundDrain(_ authorization: PostBarrierDrainAuthorization) throws {
        guard let identifier = authorization.outboundQuiescenceIdentifier else { return }
        try validatePostBarrierOutboundPrincipal(identifier: identifier)
        guard let owner = postBarrierOutboundLease else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try outboundQuiescenceCoordinator.validateDrained(owner, principal: currentOutboundPrincipal())
    }

    internal func matchingOutboundOwner(_ token: PostBarrierOutboundQuiescence) -> BigSyncOutboundQuiescenceLease? {
        guard token.issuerID == synchronizationReceiptIssuerID,
              postBarrierOutboundTicket == token, let owner = postBarrierOutboundLease,
              owner.barrier.identifier == token.identifier else { return nil }
        return owner
    }

    private func retireOutboundCapabilities(_ token: PostBarrierOutboundQuiescence) {
        if postBarrierDrainAuthorization?.outboundQuiescenceIdentifier == token.identifier { postBarrierDrainAuthorization = nil }
        if completedPostBarrierDrain?.outboundQuiescenceIdentifier == token.identifier { completedPostBarrierDrain = nil }
        postBarrierOutboundEstablishmentID = nil
        postBarrierOutboundTicket = nil
        postBarrierOutboundLease = nil
    }
}

extension CloudKitRecordMutationResults {
    /// Conservatively retain uncertainty on transport failures, cancellation,
    /// absent/malformed results and unknown error codes. A definitive negative
    /// response is safe to settle even though its Realm generation stays queued.
    internal func provesDefinitiveSettlement(saving records: [CKRecord], deleting recordIDs: [CKRecord.ID]) -> Bool {
        guard Set(saveResults.keys) == Set(records.map(\.recordID)),
              Set(deleteResults.keys) == Set(recordIDs) else { return false }
        for record in records {
            switch saveResults[record.recordID] {
            case .success(let returned):
                guard returned.recordID == record.recordID, returned.recordType == record.recordType else { return false }
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil: return false
            }
        }
        for recordID in recordIDs {
            switch deleteResults[recordID] {
            case .success: break
            case .failure(let error):
                guard Self.isDefinitiveRejection(error) else { return false }
            case nil: return false
            }
        }
        return true
    }

    internal static func isDefinitiveOperationRejection(_ error: Error) -> Bool {
        // Do not infer whole-batch settlement from a per-item rejection such
        // as an asset error: a non-atomic operation may have other outcomes.
        let error = error as NSError
        guard error.domain == CKErrorDomain else { return false }
        switch CKError.Code(rawValue: error.code) {
        case .limitExceeded, .invalidArguments, .badDatabase, .badContainer: return true
        default: return false
        }
    }

    internal static func isDefinitiveRejection(_ error: Error) -> Bool {
        let error = error as NSError
        guard error.domain == CKErrorDomain else { return false }
        switch CKError.Code(rawValue: error.code) {
        case .serverRecordChanged, .unknownItem, .invalidArguments, .permissionFailure,
             .notAuthenticated, .zoneNotFound, .userDeletedZone, .limitExceeded,
             .constraintViolation, .serverRejectedRequest, .assetFileNotFound,
             .assetFileModified, .badDatabase, .badContainer, .quotaExceeded:
            return true
        default: return false
        }
    }
}
