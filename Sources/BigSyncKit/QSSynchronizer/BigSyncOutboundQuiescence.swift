import Foundation

/// The complete transport principal, not just an account name. Returning to
/// the same account with a new lease/binding cannot revive a cutoff capability.
public struct BigSyncOutboundPrincipal: Codable, Equatable, Sendable {
    public let durableStateNamespace: String
    public let installationIdentifier: String
    public let accountScopeIdentifier: String
    public let replicaBindingGenerationIdentifier: String?
    public let accountInvalidationGeneration: Int64
}

public enum BigSyncOutboundQuiescenceError: Error, Equatable, Sendable {
    case busy
    case blocked
    case staleAuthority
    case recoveryRequired
    case invalidState
    case unresolvedSubmissions([UUID])
}

public struct BigSyncOutboundBarrier: Codable, Equatable, Sendable {
    public enum Phase: String, Codable, Sendable {
        case preparing
        /// Set BEFORE the domain is allowed to persist reservation/CAS state.
        /// Generic cancellation or pre-reservation abort can never clear it.
        case recoveryRequired
        /// The committed domain has switched authority, but peer aggregate
        /// writers remain fenced while this exact principal publishes source
        /// journals through the ordinary upload/acknowledgement pipeline.
        case sourcePublication
    }
    public let identifier: UUID
    public let writerBarrierEvidenceID: String
    public let principal: BigSyncOutboundPrincipal
    public internal(set) var phase: Phase
    /// Durable host evidence that authority was committed before owner-only
    /// source publication was enabled. Nil in earlier barrier phases.
    public internal(set) var sourcePublicationEvidenceID: String? = nil
}

public struct BigSyncOutboundSubmission: Codable, Equatable, Sendable {
    public let identifier: UUID
    public let principal: BigSyncOutboundPrincipal
}

/// An exact durable recovery checkpoint. Outstanding submission IDs are only
/// transport uncertainty markers: they contain no mutations, record values,
/// record-mutation generations, retry payloads or merge clocks. The existing Realm journal is
/// still the sole mutation authority.
public struct BigSyncOutboundQuiescenceSnapshot: Codable, Equatable, Sendable {
    internal let version: Int
    public let revisionIdentifier: UUID
    public internal(set) var barrier: BigSyncOutboundBarrier?
    public internal(set) var outstandingSubmissions: [BigSyncOutboundSubmission]
    public internal(set) var lastRecoveryEvidenceID: String?

    fileprivate init(barrier: BigSyncOutboundBarrier? = nil,
                     submissions: [BigSyncOutboundSubmission] = [],
                     recoveryEvidenceID: String? = nil) {
        version = 1
        revisionIdentifier = UUID()
        self.barrier = barrier
        outstandingSubmissions = submissions
        lastRecoveryEvidenceID = recoveryEvidenceID
    }
}

/// A short admission mutex + shared/exclusive batch lease + durable cutoff.
/// All instances/processes use the same backup-excluded client directory.
/// Neither dropping an owner nor killing a process removes the durable fence.
/// Lock order: owner (if any) -> admission -> batch (NONBLOCKING). No admission
/// mutex is held across an await or while waiting for existing batches.
internal final class BigSyncOutboundQuiescenceCoordinator: @unchecked Sendable {
    let directory: URL
    private var stateURL: URL { directory.appendingPathComponent("state.json") }
    private var initializedURL: URL { directory.appendingPathComponent("initialized") }

    init(sharedStateBaseURL: URL, durableStateNamespace: String) {
        directory = sharedStateBaseURL.standardizedFileURL
            .appendingPathComponent("OutboundQuiescence", isDirectory: true)
            .appendingPathComponent(durableStateNamespace, isDirectory: true)
    }

    func snapshot() throws -> BigSyncOutboundQuiescenceSnapshot {
        try withState { $0 }
    }

    /// Called before preparing a batch. The returned lease stays alive through
    /// preparation, submission, response handling AND generation-matched ack.
    func admit(principal: BigSyncOutboundPrincipal,
               owner: BigSyncOutboundQuiescenceLease? = nil) throws -> BigSyncOutboundBatchLease {
        guard validPrincipal(principal) else { throw BigSyncOutboundQuiescenceError.invalidState }
        if let owner {
            return try owner.withLock {
                try owner.validateOutboundAdmission(principal: principal)
                owner.activeBatches += 1
                return BigSyncOutboundBatchLease(coordinator: self, principal: principal,
                                                 batchLease: nil, owner: owner)
            }
        }
        return try withState { state in
            guard state.barrier == nil else { throw BigSyncOutboundQuiescenceError.blocked }
            let batch = try fileLease("batches.lock", exclusive: false)
            return BigSyncOutboundBatchLease(coordinator: self, principal: principal,
                                             batchLease: batch, owner: nil)
        }
    }

    /// Publish the fence before waiting. New peers now fail admission; current
    /// holders may finish. The owner lock also covers this waiting interval so
    /// recovery cannot mistake a live, waiting candidate for a dead owner.
    func begin(principal: BigSyncOutboundPrincipal,
               writerBarrierEvidenceID: String) throws -> BigSyncOutboundQuiescenceLease {
        guard validEvidence(writerBarrierEvidenceID), validPrincipal(principal),
              principal.replicaBindingGenerationIdentifier?.isEmpty == false else {
            throw BigSyncOutboundQuiescenceError.invalidState
        }
        let ownership = try fileLease("owner.lock", exclusive: true)
        return try withState { state in
            guard state.barrier == nil else { throw BigSyncOutboundQuiescenceError.recoveryRequired }
            let barrier = BigSyncOutboundBarrier(identifier: UUID(),
                writerBarrierEvidenceID: writerBarrierEvidenceID, principal: principal, phase: .preparing)
            try write(BigSyncOutboundQuiescenceSnapshot(barrier: barrier,
                submissions: state.outstandingSubmissions, recoveryEvidenceID: state.lastRecoveryEvidenceID))
            return BigSyncOutboundQuiescenceLease(coordinator: self, ownerLease: ownership, barrier: barrier)
        }
    }

    /// Poll cooperatively; callers revalidate their synchronizer principal
    /// around this await. Cancellation deliberately leaves the durable fence.
    func waitUntilDrained(
        _ owner: BigSyncOutboundQuiescenceLease,
        revalidating: @Sendable () async throws -> Void = {}
    ) async throws {
        while true {
            try Task.checkCancellation()
            try await revalidating()
            do {
                if try tryFinishDraining(owner) { return }
            } catch BigSyncOutboundQuiescenceError.busy {
                // A peer may briefly hold admission while persisting its ack.
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func tryFinishDraining(_ owner: BigSyncOutboundQuiescenceLease) throws -> Bool {
        try owner.withLock {
            try validateOwner(owner, principal: owner.barrier.principal, requiresDrained: false)
            if owner.batchLease == nil {
                let batch = try BigSyncFileLease(at: directory.appendingPathComponent("batches.lock"))
                guard try batch.tryLock(exclusive: true) else { return false }
                owner.batchLease = batch
            }
            let state = try snapshot()
            guard state.outstandingSubmissions.isEmpty else {
                // An OS lock disappearing is not proof that a submitted server
                // request cannot still commit after a process crash/timeout.
                throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                    state.outstandingSubmissions.map(\.identifier))
            }
            return true
        }
    }

    func validateOwner(_ owner: BigSyncOutboundQuiescenceLease,
                       principal: BigSyncOutboundPrincipal, requiresDrained: Bool = true) throws {
        // Caller holds owner.mutex. This method performs no actor suspension.
        guard owner.coordinator.directory == directory, !owner.closed,
              owner.barrier.principal == principal else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try owner.ownerLease?.validateIdentity()
        if requiresDrained {
            guard let batch = owner.batchLease else { throw BigSyncOutboundQuiescenceError.busy }
            try batch.validateIdentity()
        }
        guard try snapshot().barrier == owner.barrier else { throw BigSyncOutboundQuiescenceError.staleAuthority }
    }

    func validateDrained(_ owner: BigSyncOutboundQuiescenceLease,
                         principal: BigSyncOutboundPrincipal) throws {
        try owner.withLock {
            try validateOwner(owner, principal: principal)
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            let state = try snapshot()
            guard state.outstandingSubmissions.isEmpty else {
                throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(state.outstandingSubmissions.map(\.identifier))
            }
        }
    }

    /// Persist this before ANY domain reservation write, not after it.
    func requireRecovery(_ owner: BigSyncOutboundQuiescenceLease) throws {
        try owner.withLock {
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            guard owner.barrier.phase != .sourcePublication else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            if owner.barrier.phase == .recoveryRequired {
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = false
                return
            }
            try withState { state in
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\.identifier))
                }
                var barrier = owner.barrier
                barrier.phase = .recoveryRequired
                barrier.sourcePublicationEvidenceID = nil
                try write(BigSyncOutboundQuiescenceSnapshot(barrier: barrier,
                    submissions: state.outstandingSubmissions, recoveryEvidenceID: state.lastRecoveryEvidenceID))
                owner.barrier = barrier
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = false
            }
        }
    }

    /// After the host durably commits the new authority, keep the same exclusive
    /// transport owner but permit ordinary source-journal batches. Peer/legacy
    /// batches remain fenced by the persisted barrier. This never arms another
    /// aggregate cutoff or manufactures a terminal receipt.
    func authorizeSourcePublication(
        _ owner: BigSyncOutboundQuiescenceLease,
        expected: BigSyncOutboundQuiescenceSnapshot,
        evidenceID: String
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        try owner.withLock {
            guard validEvidence(evidenceID) else { throw BigSyncOutboundQuiescenceError.invalidState }
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.barrier.phase == .recoveryRequired else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            return try withState { state in
                guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\.identifier))
                }
                var barrier = owner.barrier
                barrier.phase = .sourcePublication
                barrier.sourcePublicationEvidenceID = evidenceID
                let updated = BigSyncOutboundQuiescenceSnapshot(
                    barrier: barrier,
                    submissions: [],
                    recoveryEvidenceID: state.lastRecoveryEvidenceID
                )
                try write(updated)
                owner.barrier = barrier
                owner.allowsFinalDrain = false
                owner.allowsSourcePublication = true
                return updated
            }
        }
    }

    /// Revoke only this live owner's pre-reservation fence. Never a successor,
    /// never a recovery-required phase, never while an owner batch is in flight.
    func abort(_ owner: BigSyncOutboundQuiescenceLease) throws {
        try owner.withLock {
            try validateOwner(owner, principal: owner.barrier.principal, requiresDrained: false)
            guard owner.barrier.phase == .preparing else { throw BigSyncOutboundQuiescenceError.recoveryRequired }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            try withState { state in
                try write(BigSyncOutboundQuiescenceSnapshot(submissions: state.outstandingSubmissions,
                    recoveryEvidenceID: state.lastRecoveryEvidenceID))
            }
            owner.close()
        }
    }

    /// Take OS ownership for explicit crash/restart recovery, never mint a
    /// final-drain permit from a persisted barrier. A live owner or peer batch
    /// makes this fail, not wait while holding the short admission mutex.
    func takeRecoveryOwnership(expected: BigSyncOutboundQuiescenceSnapshot) throws -> BigSyncOutboundRecoveryLease {
        let ownership = try fileLease("owner.lock", exclusive: true)
        return try withState { state in
            guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            let batches = try fileLease("batches.lock", exclusive: true)
            return BigSyncOutboundRecoveryLease(directory: directory, owner: ownership, batches: batches, snapshot: state)
        }
    }

    /// The host must prove its exact domain recovery decision AND settle every
    /// indeterminate submitted operation in `expected` before calling this.
    /// A fetch, elapsed time, or OS-lock disappearance alone is NOT that proof.
    func resolveRecovery(_ recovery: BigSyncOutboundRecoveryLease, evidenceID: String) throws {
        guard recovery.directory == directory, validEvidence(evidenceID), !recovery.closed else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        try recovery.owner?.validateIdentity()
        try recovery.batches?.validateIdentity()
        try withState { state in
            guard state == recovery.snapshot else { throw BigSyncOutboundQuiescenceError.staleAuthority }
            try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
        }
        recovery.close()
    }

    /// Convert exact crash/restart recovery ownership into owner-only source
    /// publication without ever opening peer admission. The caller's durable
    /// proof authorizes settlement of every uncertainty marker in `snapshot`;
    /// the barrier itself remains installed under the original principal.
    func resumeSourcePublication(
        _ recovery: BigSyncOutboundRecoveryLease,
        principal: BigSyncOutboundPrincipal,
        recoveryEvidenceID: String
    ) throws -> BigSyncOutboundQuiescenceLease {
        guard recovery.directory == directory, validEvidence(recoveryEvidenceID),
              validPrincipal(principal), !recovery.closed else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try recovery.owner?.validateIdentity()
        try recovery.batches?.validateIdentity()
        let barrier = try withState { state -> BigSyncOutboundBarrier in
            guard state == recovery.snapshot,
                  var barrier = state.barrier,
                  barrier.principal == principal else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            switch barrier.phase {
            case .recoveryRequired:
                // The host must prove that the domain commit preceded the crash;
                // the persisted transport phase alone is not that proof.
                barrier.phase = .sourcePublication
                barrier.sourcePublicationEvidenceID = recoveryEvidenceID
            case .sourcePublication:
                guard barrier.sourcePublicationEvidenceID.map(validEvidence) == true else {
                    throw BigSyncOutboundQuiescenceError.staleAuthority
                }
            case .preparing:
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            // The host proof covers the exact checkpoint, including every
            // indeterminate source request. Clear only those exact markers while
            // retaining the peer fence and recording the recovery decision.
            try write(BigSyncOutboundQuiescenceSnapshot(
                barrier: barrier,
                submissions: [],
                recoveryEvidenceID: recoveryEvidenceID
            ))
            return barrier
        }
        guard let ownership = recovery.owner, let batches = recovery.batches else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        recovery.owner = nil
        recovery.batches = nil
        recovery.closed = true
        let owner = BigSyncOutboundQuiescenceLease(
            coordinator: self,
            ownerLease: ownership,
            barrier: barrier
        )
        owner.batchLease = batches
        owner.allowsSourcePublication = true
        return owner
    }

    func resolveOwned(_ owner: BigSyncOutboundQuiescenceLease,
                      expected: BigSyncOutboundQuiescenceSnapshot, evidenceID: String) throws {
        try owner.withLock {
            guard validEvidence(evidenceID) else { throw BigSyncOutboundQuiescenceError.invalidState }
            try validateOwner(owner, principal: owner.barrier.principal)
            guard owner.barrier.phase == .recoveryRequired
                    || owner.barrier.phase == .sourcePublication else {
                throw BigSyncOutboundQuiescenceError.recoveryRequired
            }
            guard owner.activeBatches == 0 else { throw BigSyncOutboundQuiescenceError.busy }
            try withState { state in
                guard state == expected else { throw BigSyncOutboundQuiescenceError.staleAuthority }
                guard state.outstandingSubmissions.isEmpty else {
                    throw BigSyncOutboundQuiescenceError.unresolvedSubmissions(
                        state.outstandingSubmissions.map(\.identifier))
                }
                try write(BigSyncOutboundQuiescenceSnapshot(recoveryEvidenceID: evidenceID))
            }
            owner.close()
        }
    }

    fileprivate func willSubmit(_ batch: BigSyncOutboundBatchLease) throws -> UUID {
        try batch.validateSubmissionAdmission()
        let id = UUID()
        try withState { state in
            // Already-admitted peers may finish even after a cutoff was
            // published. Their shared batch leases still prevent exclusive
            // acquisition; rejecting them here is unnecessary and loses work.
            var submissions = state.outstandingSubmissions
            guard submissions.count < 4_096 else { throw BigSyncOutboundQuiescenceError.recoveryRequired }
            submissions.append(BigSyncOutboundSubmission(identifier: id, principal: batch.principal))
            try write(BigSyncOutboundQuiescenceSnapshot(barrier: state.barrier, submissions: submissions,
                recoveryEvidenceID: state.lastRecoveryEvidenceID))
        }
        return id
    }

    fileprivate func didSettle(_ id: UUID, batch: BigSyncOutboundBatchLease) throws {
        try batch.validateLease()
        try withState { state in
            guard state.outstandingSubmissions.contains(where: { $0.identifier == id && $0.principal == batch.principal }) else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            try write(BigSyncOutboundQuiescenceSnapshot(barrier: state.barrier,
                submissions: state.outstandingSubmissions.filter { $0.identifier != id },
                recoveryEvidenceID: state.lastRecoveryEvidenceID))
        }
    }

    private func fileLease(_ name: String, exclusive: Bool) throws -> BigSyncFileLease {
        let lease = try BigSyncFileLease(at: directory.appendingPathComponent(name))
        guard try lease.tryLock(exclusive: exclusive) else { throw BigSyncOutboundQuiescenceError.busy }
        return lease
    }

    private func withState<T>(_ body: (BigSyncOutboundQuiescenceSnapshot) throws -> T) throws -> T {
        let admission = try fileLease("admission.lock", exclusive: true)
        defer { withExtendedLifetime(admission) {} }
        var state: BigSyncOutboundQuiescenceSnapshot
        do {
            let data = try Data(contentsOf: stateURL)
            guard data.count <= 8 * 1_024 * 1_024 else { throw BigSyncOutboundQuiescenceError.invalidState }
            state = try JSONDecoder().decode(BigSyncOutboundQuiescenceSnapshot.self, from: data)
            guard state.version == 1,
                  state.outstandingSubmissions.count <= 4_096,
                  state.outstandingSubmissions.allSatisfy({ validPrincipal($0.principal) }),
                  state.barrier.map(validBarrier) ?? true,
                  Set(state.outstandingSubmissions.map(\.identifier)).count == state.outstandingSubmissions.count else {
                throw BigSyncOutboundQuiescenceError.invalidState
            }
        } catch let error as CocoaError where error.code == .fileReadNoSuchFile {
            // After initialization, losing only the state file is corruption,
            // not evidence that the previously closed gate has reopened.
            if FileManager.default.fileExists(atPath: initializedURL.path) {
                throw BigSyncOutboundQuiescenceError.invalidState
            }
            state = BigSyncOutboundQuiescenceSnapshot()
            try write(state)
        }
        // Publish the marker even after a crash between initial state and
        // marker publication. Both happen while holding admission.lock.
        if !FileManager.default.fileExists(atPath: initializedURL.path) {
            try bigSyncWriteDataDurably(Data("1".utf8), to: initializedURL)
        }
        return try body(state)
    }

    private func write(_ state: BigSyncOutboundQuiescenceSnapshot) throws {
        try bigSyncWriteDataDurably(JSONEncoder().encode(state), to: stateURL)
    }

    private func validBarrier(_ barrier: BigSyncOutboundBarrier) -> Bool {
        guard validEvidence(barrier.writerBarrierEvidenceID),
              validPrincipal(barrier.principal) else { return false }
        switch barrier.phase {
        case .preparing, .recoveryRequired:
            return barrier.sourcePublicationEvidenceID == nil
        case .sourcePublication:
            return barrier.sourcePublicationEvidenceID.map(validEvidence) == true
        }
    }

    private func validPrincipal(_ principal: BigSyncOutboundPrincipal) -> Bool {
        principal.durableStateNamespace == directory.lastPathComponent
            && validEvidence(principal.installationIdentifier)
            && validEvidence(principal.accountScopeIdentifier)
            && principal.replicaBindingGenerationIdentifier.map(validEvidence) != false
            && principal.accountInvalidationGeneration >= 0
    }

    private func validEvidence(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.count <= 1_024
    }
}

internal final class BigSyncOutboundQuiescenceLease: @unchecked Sendable {
    let coordinator: BigSyncOutboundQuiescenceCoordinator
    fileprivate let mutex = NSRecursiveLock()
    fileprivate var ownerLease: BigSyncFileLease?
    fileprivate var batchLease: BigSyncFileLease?
    fileprivate(set) var barrier: BigSyncOutboundBarrier
    fileprivate var activeBatches = 0
    fileprivate var closed = false
    fileprivate var allowsFinalDrain = false
    fileprivate var allowsSourcePublication = false
    private var hasArmedFinalDrain = false

    fileprivate init(coordinator: BigSyncOutboundQuiescenceCoordinator, ownerLease: BigSyncFileLease,
                     barrier: BigSyncOutboundBarrier) {
        self.coordinator = coordinator; self.ownerLease = ownerLease; self.barrier = barrier
    }

    func withLock<T>(_ body: () throws -> T) rethrows -> T {
        mutex.lock(); defer { mutex.unlock() }; return try body()
    }

    func validate(principal: BigSyncOutboundPrincipal) throws {
        try withLock { try coordinator.validateOwner(self, principal: principal) }
    }

    func validateOutboundAdmission(principal: BigSyncOutboundPrincipal) throws {
        try withLock {
            try coordinator.validateOwner(self, principal: principal)
            guard (allowsFinalDrain && barrier.phase == .preparing)
                    || (allowsSourcePublication && barrier.phase == .sourcePublication) else {
                throw BigSyncOutboundQuiescenceError.blocked
            }
        }
    }

    func armFinalDrain() throws {
        try withLock {
            guard !closed, !hasArmedFinalDrain, barrier.phase == .preparing else {
                throw BigSyncOutboundQuiescenceError.staleAuthority
            }
            try coordinator.validateDrained(self, principal: barrier.principal)
            hasArmedFinalDrain = true
            allowsFinalDrain = true
        }
    }

    func sealFinalDrain() { withLock { allowsFinalDrain = false } }

    func sealOutboundAdmission() {
        withLock {
            allowsFinalDrain = false
            allowsSourcePublication = false
        }
    }

    fileprivate func close() {
        closed = true
        allowsFinalDrain = false
        allowsSourcePublication = false
        batchLease = nil
        ownerLease = nil
    }
    // No deinit disk mutation. A batch retains this owner until its actual
    // operation and callback scope exits, even if the synchronizer is replaced.
}

internal final class BigSyncOutboundBatchLease {
    fileprivate let coordinator: BigSyncOutboundQuiescenceCoordinator
    let principal: BigSyncOutboundPrincipal
    private let batchLease: BigSyncFileLease?
    private let owner: BigSyncOutboundQuiescenceLease?
    private var submissionID: UUID?
    /// Process-local only. Losing it must never lose the durable submission
    /// marker; that is precisely why the marker is not cleared at server return.
    private var transportOutcomeIsDefinitive = false

    fileprivate init(coordinator: BigSyncOutboundQuiescenceCoordinator, principal: BigSyncOutboundPrincipal,
                     batchLease: BigSyncFileLease?, owner: BigSyncOutboundQuiescenceLease?) {
        self.coordinator = coordinator; self.principal = principal
        self.batchLease = batchLease; self.owner = owner
    }

    deinit { owner?.withLock { owner?.activeBatches -= 1 } }

    func willSubmit() throws {
        guard submissionID == nil else { throw BigSyncOutboundQuiescenceError.invalidState }
        transportOutcomeIsDefinitive = false
        submissionID = try coordinator.willSubmit(self)
    }

    /// Remember a definitive server outcome without yet clearing durable
    /// uncertainty. Local generation-matched response processing is part of the
    /// physical batch lifetime and must finish first.
    func noteDefinitiveTransportOutcome() throws {
        guard submissionID != nil else { throw BigSyncOutboundQuiescenceError.invalidState }
        transportOutcomeIsDefinitive = true
    }

    /// Remove this marker only after the request has a definitive outcome AND
    /// all required generation-matched local response processing has completed.
    /// A whole-operation definitive rejection that entered no per-item callback
    /// may settle immediately. Never call from cancellation, timeout, deinit, or
    /// a malformed/indeterminate response path.
    func didSettle() throws {
        guard let submissionID else { throw BigSyncOutboundQuiescenceError.invalidState }
        try coordinator.didSettle(submissionID, batch: self)
        self.submissionID = nil
        transportOutcomeIsDefinitive = false
    }

    /// Called after the caller has finished all required generation-matched
    /// acknowledgement/requeue/conflict callbacks for the returned response.
    /// Non-definitive responses intentionally leave their marker unresolved.
    func completeLocalResponseProcessingCooperatively() async throws {
        guard transportOutcomeIsDefinitive else { return }
        try await didSettleCooperatively()
    }

    /// A short metadata-lock collision must not manufacture an unresolved
    /// server operation. Preparation honours task cancellation; settlement is
    /// physical bookkeeping and may complete after its run has been cancelled.
    func willSubmitCooperatively() async throws {
        while true {
            try Task.checkCancellation()
            do { try willSubmit(); return }
            catch BigSyncOutboundQuiescenceError.busy {
                try await Task.sleep(nanoseconds: 1_000_000)
            }
        }
    }

    func didSettleCooperatively() async throws {
        for _ in 0..<1_000 {
            do { try didSettle(); return }
            catch BigSyncOutboundQuiescenceError.busy {
                // An unstructured child would inherit actor isolation, but not
                // cancellation. A detached sleeper cannot touch durable state.
                try await Task.detached { try await Task.sleep(nanoseconds: 1_000_000) }.value
            }
        }
        // Preserve the uncertainty marker rather than ignoring persistence
        // failure or waiting forever on a peer suspended inside admission.
        throw BigSyncOutboundQuiescenceError.busy
    }

    /// Recheck at submission, not only at preparation. Revocation may occur
    /// while the owning batch is suspended in adapter preparation or metadata
    /// contention. Already-admitted ordinary peers still finish across a fence.
    func validateSubmissionAdmission() throws {
        if let owner { try owner.validateOutboundAdmission(principal: principal) }
        else { try validateLease() }
    }

    fileprivate func validateLease() throws {
        if let owner { try owner.validate(principal: principal) }
        else { try batchLease?.validateIdentity() }
    }
}

internal final class BigSyncOutboundRecoveryLease {
    fileprivate let directory: URL
    fileprivate var owner: BigSyncFileLease?
    fileprivate var batches: BigSyncFileLease?
    let snapshot: BigSyncOutboundQuiescenceSnapshot
    fileprivate var closed = false
    fileprivate init(directory: URL, owner: BigSyncFileLease, batches: BigSyncFileLease, snapshot: BigSyncOutboundQuiescenceSnapshot) {
        self.directory = directory; self.owner = owner; self.batches = batches; self.snapshot = snapshot
    }
    fileprivate func close() { closed = true; batches = nil; owner = nil }
}
