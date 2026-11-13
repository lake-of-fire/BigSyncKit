import CloudKit
import Darwin
import Foundation

/// A manual restore could otherwise replace an app-group Realm while an
/// extension has a write transaction in flight.  Every configured client
/// process retains a shared advisory lock for its lifetime; restore briefly
/// upgrades its own holder to an exclusive nonblocking lock before it touches
/// either the Realm files or the installation identity.
public enum BigSyncClientIdentityLeaseError: Error, Equatable, Sendable {
    case restoreInProgress
    case leaseUnavailable(Int32)
}

/// The durable identity handoff for one caller-owned backup restore.
/// Persist this receipt with the caller's restore journal; passing the same
/// transaction identifier again is the only supported way to resume a crash
/// after event publication and before that journal commit.
public struct BigSyncManualBackupRestoreReceipt: Equatable, Sendable {
    public let transactionIdentifier: UUID
    public let restoreEventIdentifier: UUID
    public let oldInstallationIdentifier: String
    public let newInstallationIdentifier: String

    public init(
        transactionIdentifier: UUID,
        restoreEventIdentifier: UUID,
        oldInstallationIdentifier: String,
        newInstallationIdentifier: String
    ) {
        self.transactionIdentifier = transactionIdentifier
        self.restoreEventIdentifier = restoreEventIdentifier
        self.oldInstallationIdentifier = oldInstallationIdentifier
        self.newInstallationIdentifier = newInstallationIdentifier
    }
}

/// A manual restore reached a state in which BigSyncKit can no longer safely
/// ask the caller to restore the old Realm files.
///
/// In particular, once the restore event is durable, rolling the replacement
/// back would make that event describe a Realm installation that is no longer
/// present. Callers must retain their replacement, persist their own
/// transaction as incomplete, and retry with the same transaction identifier.
public enum BigSyncManualBackupRestoreError: Error, Equatable, Sendable {
    /// The replacement is installed and its restore event is durable, but
    /// installation-identity publication has not yet been proven complete.
    case handoffPending(BigSyncManualBackupRestoreReceipt)

    /// Durable state cannot prove which installation owns the current Realm.
    /// Continuing or rolling back would both be guesses, so the host must stop
    /// before opening the target Realm and surface repair/retry diagnostics.
    case stateAmbiguous

    /// Another durable manual-restore transaction already owns this client.
    case transactionMismatch
}

enum BigSyncClientIdentityLeaseRegistry {
    private enum Mode {
        case shared
        case exclusive
        case publication
    }

    private final class Lease {
        let descriptor: Int32
        var mode: Mode
        var cachedInstallationIdentifier: String?

        init(descriptor: Int32, mode: Mode) {
            self.descriptor = descriptor
            self.mode = mode
        }

        deinit {
            Darwin.close(descriptor)
        }
    }

    // Identity validation may be queried by a replacement/rollback closure
    // while `withExclusive` owns this registry on the same thread. Recursive
    // locking keeps that fail-closed probe from deadlocking; cross-thread
    // callers still wait until the restore boundary finishes.
    private static let lock = NSRecursiveLock()
    private static var leases = [String: Lease]()

    static func retainShared(at url: URL) throws {
        lock.lock()
        defer { lock.unlock() }
        // An existing exclusive holder already satisfies this minimum lease.
        // Identity setup can reenter from a replacement/rollback callback;
        // only the outer restore owner may downgrade its exclusive lock.
        _ = try lease(at: url)
    }

    static func cachedInstallationIdentifier(at url: URL) -> String? {
        lock.lock()
        defer { lock.unlock() }
        return leases[url.standardizedFileURL.path]?
            .cachedInstallationIdentifier
    }

    static func publishInstallationIdentifier(
        _ identifier: String,
        at url: URL
    ) {
        guard !identifier.isEmpty else { return }
        lock.lock()
        defer { lock.unlock() }
        leases[url.standardizedFileURL.path]?
            .cachedInstallationIdentifier = identifier
    }

    static func invalidateInstallationIdentifier(at url: URL) {
        lock.lock()
        defer { lock.unlock() }
        leases[url.standardizedFileURL.path]?
            .cachedInstallationIdentifier = nil
    }

    static func withExclusive<T>(
        at url: URL,
        _ operation: () throws -> T
    ) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        let lease = try lease(at: url)
        // A recursive registry lock permits read-only identity probes, not
        // another restore of this client. Its nested defer would otherwise
        // release the outer operation's cross-process exclusion too early.
        guard lease.mode == .shared else {
            throw BigSyncClientIdentityLeaseError.restoreInProgress
        }
        guard bigSyncFlock(lease.descriptor, LOCK_UN) == 0 else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
        }
        guard bigSyncFlock(lease.descriptor, LOCK_EX | LOCK_NB) == 0 else {
            let lockError = Int32(errno)
            // Restore the process-lifetime writer lease before reporting a
            // competing app/extension holder. A later launch can retry the
            // staged restore without a window for an unfenced local write.
            guard bigSyncFlock(lease.descriptor, LOCK_SH) == 0 else {
                throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
            }
            lease.mode = .shared
            if lockError == EWOULDBLOCK || lockError == EAGAIN {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        lease.mode = .exclusive
        // No mutation may reuse the pre-restore identity after this boundary.
        // The durable intent publication below also invalidates this cache for
        // callers that exercise BackupDetection directly.
        lease.cachedInstallationIdentifier = nil
        defer {
            // Downgrading before returning prevents subsequent mutations in
            // this process from escaping the cross-process restore fence.
            if bigSyncFlock(lease.descriptor, LOCK_SH) == 0 {
                lease.mode = .shared
            }
        }
        return try operation()
    }

    /// Uses the same process-lifetime shared lease and registry frontier as
    /// restore. No identity/storage callbacks may reenter this registry.
    static func withPublication<T>(at url: URL, _ body: () throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        let current = try lease(at: url)
        guard current.mode == .shared else { throw BigSyncClientIdentityLeaseError.restoreInProgress }
        current.mode = .publication
        defer { current.mode = .shared }
        return try body()
    }

    private static func lease(at url: URL) throws -> Lease {
        let key = url.standardizedFileURL.path
        if let lease = leases[key] { return lease }
        try bigSyncCreateDirectoryDurably(
            at: url.deletingLastPathComponent()
        )
        let descriptor = Darwin.open(url.path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
        }
        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        var mutableURL = url
        do {
            try mutableURL.setResourceValues(values)
        } catch {
            Darwin.close(descriptor)
            throw error
        }
        guard bigSyncFlock(descriptor, LOCK_SH) == 0 else {
            let lockError = Int32(errno)
            Darwin.close(descriptor)
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        let lease = Lease(descriptor: descriptor, mode: .shared)
        leases[key] = lease
        return lease
    }
}

/// Complete local identity for one BigSync client.
///
/// The identity binds backup detection and mutation attribution to the same
/// container/database/zone namespace used by synchronization. The shared base
/// must live beside the app-group state used by every process that can write
/// the target Realms.
public struct BigSyncClientIdentity: Sendable {
    public let durableStateNamespace: String
    public let sharedStateBaseURL: URL

    /// File used by the production synchronizer's durable key-value store.
    /// Mutation writers resolve the same path before the worker is configured
    /// so every app-group process captures one replica-binding generation.
    public var synchronizationStateFileURL: URL {
        sharedStateBaseURL
            .appendingPathComponent("LocalState", isDirectory: true)
            .appendingPathComponent(
                durableStateNamespace,
                isDirectory: true
            )
            .appendingPathComponent("state.plist")
    }

    public init(
        synchronizerName: String,
        containerName: String,
        recordZoneID: CKRecordZone.ID,
        databaseScope: CKDatabase.Scope = .private,
        sharedStateBaseURL: URL
    ) {
        durableStateNamespace = CloudKitSynchronizer.makeDurableStateNamespace(
            identifier: synchronizerName,
            containerIdentifier: containerName,
            databaseScope: databaseScope,
            recordZoneID: recordZoneID
        )
        self.sharedStateBaseURL = sharedStateBaseURL.standardizedFileURL
    }

    /// Establishes backup detection before a target Realm can be opened and
    /// returns the backup-excluded installation identity used by its journal.
    @discardableResult
    public func prepareInstallation() throws -> String {
        guard !BigSyncReplicaPublicationReadScope.isActive else {
            throw BigSyncReplicaPublicationError.reentrantPublication
        }
        try BigSyncClientIdentityLeaseRegistry.retainShared(at: leaseURL)
        _ = try BackupDetection.run(
            store: UserDefaultsAdapter(userDefaults: .standard),
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
        guard let identifier = publishedInstallationIdentifier() else {
            throw CocoaError(.fileReadCorruptFile)
        }
        let pendingManualEvent = BackupDetection.manualRestoreReceipt(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
        let pendingManualIntent = BackupDetection.manualRestoreIntentReceipt(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
        if BackupDetection.manualRestoreIntentIsRequired(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ) {
            // An intent is the pre-replacement fence. Even if the event and
            // sentinel were subsequently written, the caller has not proved
            // its handoff cleanup finished. Do not let another process open
            // the target Realm until that same transaction resumes it.
            guard let pendingManualIntent else {
                throw BigSyncManualBackupRestoreError.stateAmbiguous
            }
            guard pendingManualEvent == nil
                    || pendingManualEvent == pendingManualIntent else {
                throw BigSyncManualBackupRestoreError.stateAmbiguous
            }
            if let pendingManualEvent {
                // Once the matching event is durable, rollback is no longer
                // permitted. Surface the exact resumable receipt even though
                // intent cleanup is still pending, so the owning coordinator
                // can finish this handoff without guessing.
                throw BigSyncManualBackupRestoreError.handoffPending(
                    makeManualRestoreReceipt(pendingManualEvent)
                )
            }
            throw BigSyncManualBackupRestoreError.stateAmbiguous
        }
        if let pendingManualRestore = pendingManualEvent {
            let receipt = makeManualRestoreReceipt(pendingManualRestore)
            if identifier == receipt.oldInstallationIdentifier {
                // The target Realm was replaced under an exclusive lease, but
                // the new identity has not reached the sentinel yet. Refuse to
                // let a newly launched app/extension attribute writes to the
                // replacement with the old installation identity.
                throw BigSyncManualBackupRestoreError.handoffPending(receipt)
            }
            guard identifier == receipt.newInstallationIdentifier,
                  pendingManualEvent != nil else {
                throw BigSyncManualBackupRestoreError.stateAmbiguous
            }
        } else if BackupDetection.restoreResetIsRequired(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ), BackupDetection.restoreResetEventIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ) == nil {
            // An unreadable restore event cannot prove whether it is an
            // automatic backup restore or a partially published manual
            // replacement. Never open a target Realm on that ambiguity.
            throw BigSyncManualBackupRestoreError.stateAmbiguous
        }
        BigSyncClientIdentityLeaseRegistry.publishInstallationIdentifier(
            identifier,
            at: leaseURL
        )
        return identifier
    }

    public func currentInstallationIdentifier() -> String? {
        if let borrowed = BigSyncReplicaPublicationReadScope.current(identity: self) {
            return borrowed.identity.installationIdentifier
        }
        if let cached = BigSyncClientIdentityLeaseRegistry
            .cachedInstallationIdentifier(at: leaseURL) {
            return cached
        }
        guard let identifier = publishedInstallationIdentifier() else {
            return nil
        }
        let pendingManualEvent = BackupDetection.manualRestoreReceipt(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
        if BackupDetection.manualRestoreIntentIsRequired(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ) {
            // See `prepareInstallation()`: an intent deliberately fences all
            // mutation attribution until its owning transaction resumes or
            // cancels it. An event alongside it is still not enough; cleanup
            // must be durably completed first.
            return nil
        }
        if let pendingManualRestore = pendingManualEvent {
            // A dynamic mutation-generation provider may be called after a
            // runtime handoff failure. Return no identity until the sentinel
            // proves the event's new installation, making such a write fail
            // closed instead of attributing it to the old installation.
            guard identifier == pendingManualRestore.newInstallationIdentifier,
                  pendingManualEvent != nil
            else { return nil }
        } else if BackupDetection.restoreResetIsRequired(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ), BackupDetection.restoreResetEventIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ) == nil {
            return nil
        }
        BigSyncClientIdentityLeaseRegistry.publishInstallationIdentifier(
            identifier,
            at: leaseURL
        )
        return identifier
    }

    /// Establishes and returns the binding generation used by new mutation
    /// journal rows before the CloudKit worker starts.
    @discardableResult
    public func prepareReplicaBindingGenerationIdentifier() throws -> String {
        let installationIdentifier = try prepareInstallation()
        let store = FileKeyValueStore(
            fileURL: synchronizationStateFileURL
        )
        try store.prepareForUse()
        return try BigSyncReplicaBindingStateStore.prepare(
            store: store,
            key: durableStateNamespace + ".ReplicaBinding.v1",
            installationIdentifier: installationIdentifier
        ).mutationGenerationIdentifier
    }

    public func currentReplicaBindingGenerationIdentifier() -> String? {
        currentMutationJournalIdentity()?
            .replicaBindingGenerationIdentifier
    }

    /// Creates a live journal-identity provider that reuses its thread-safe
    /// replica-binding store. Every invocation still reloads and validates the
    /// durable state, so another process's restore or binding rotation is
    /// observed before a mutation is attributed. Reusing the reader only
    /// avoids constructing a store (and its eager plist read) for every Realm
    /// object mutation.
    public func makeMutationJournalIdentityProvider()
        -> @Sendable () -> BigSyncMutationJournalIdentity? {
        let reader = BigSyncMutationJournalIdentityReader(
            clientIdentity: self,
            store: FileKeyValueStore(
                fileURL: synchronizationStateFileURL
            ),
            key: durableStateNamespace + ".ReplicaBinding.v1"
        )
        return { reader.current() }
    }

    /// Reads installation and replica-binding identity as one verified
    /// snapshot. A restore or partial handoff returns nil instead of combining
    /// fields from opposite sides of the identity boundary.
    public func currentMutationJournalIdentity()
        -> BigSyncMutationJournalIdentity? {
        currentMutationJournalIdentity(storeURL: synchronizationStateFileURL)
    }

    /// Publishes a manual Realm-backup restore before synchronization metadata
    /// is discarded. The returned fresh installation identity must be installed
    /// for every restored target configuration before it is reopened.
    @discardableResult
    public func beginManualBackupRestore(
        transactionIdentifier: UUID
    ) throws -> BigSyncManualBackupRestoreReceipt {
        try withManualBackupRestore(transactionIdentifier: transactionIdentifier) {}
    }

    /// Removes only a matching pre-replacement intent after the caller has
    /// durably verified rollback. A durable restore event can never be
    /// cancelled through this API because it already owns the replacement.
    /// With no durable manual records, cancellation is an idempotent no-op
    /// even before first installation; it never establishes identity.
    public func cancelManualBackupRestoreIntent(
        transactionIdentifier: UUID
    ) throws {
        guard !BigSyncReplicaPublicationReadScope.isActive else {
            throw BigSyncReplicaPublicationError.reentrantPublication
        }
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
            let preflight: BackupDetection.ManualRestorePreflight
            do {
                preflight = try BackupDetection.manualRestorePreflight(
                    namespace: durableStateNamespace,
                    transactionIdentifier: transactionIdentifier,
                    sharedSentinelBaseURL: sharedStateBaseURL,
                    allowsAbsentManualRecordsForCancellation: true
                )
            } catch BackupDetection.Error.manualRestoreTransactionMismatch {
                throw BigSyncManualBackupRestoreError.transactionMismatch
            } catch {
                throw BigSyncManualBackupRestoreError.stateAmbiguous
            }
            switch preflight {
            case .newTransaction:
                return
            case .resumeIntent(let receipt):
                try BackupDetection.cancelManualRestoreIntent(
                    namespace: durableStateNamespace,
                    receipt: receipt,
                    sharedSentinelBaseURL: sharedStateBaseURL
                )
            case .resumeEvent(let receipt), .completed(let receipt):
                throw BigSyncManualBackupRestoreError.handoffPending(
                    makeManualRestoreReceipt(receipt)
                )
            }
        }
    }

    @available(*, deprecated, message: "Supply a stable transactionIdentifier and persist the returned receipt.")
    public func beginManualBackupRestore() throws -> String {
        try beginManualBackupRestore(transactionIdentifier: UUID()).newInstallationIdentifier
    }

    /// Runs a replacement while no other configured app-group process can
    /// commit a Realm mutation. The fresh identity is published only after the
    /// replacement closure succeeds.
    @discardableResult
    public func withManualBackupRestore(
        transactionIdentifier: UUID,
        validation: (() throws -> Void)? = nil,
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {}
    ) throws -> BigSyncManualBackupRestoreReceipt {
        try withManualBackupRestore(
            transactionIdentifier: transactionIdentifier,
            validation: validation,
            replacement,
            rollback: rollback,
            sentinelPublisher: nil
        )
    }

    /// Test seam for the event-before-sentinel crash prefix. Production always
    /// uses BackupDetection's atomic excluded-sentinel publisher.
    func withManualBackupRestore(
        transactionIdentifier: UUID,
        validation: (() throws -> Void)? = nil,
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {},
        sentinelPublisher: ((URL, FileManager) throws -> Void)?
    ) throws -> BigSyncManualBackupRestoreReceipt {
        guard !BigSyncReplicaPublicationReadScope.isActive else {
            throw BigSyncReplicaPublicationError.reentrantPublication
        }
        let before = currentInstallationIdentifier()
        defer {
            if before != currentInstallationIdentifier() {
                // Outside the exclusive lease, including incomplete handoffs.
                NotificationCenter.default.post(name: .BigSyncReplicaPublicationAuthorityDidChange, object: nil)
            }
        }
        return try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
            // Read-only admission must precede identity preflight: a rejected
            // archive may be encountered before the first installation exists.
            // The caller must still validate again immediately before replacement.
            try validation?()
            let preflight: BackupDetection.ManualRestorePreflight
            do {
                preflight = try BackupDetection.manualRestorePreflight(
                    namespace: durableStateNamespace,
                    transactionIdentifier: transactionIdentifier,
                    sharedSentinelBaseURL: sharedStateBaseURL
                )
            } catch BackupDetection.Error.manualRestoreTransactionMismatch {
                throw BigSyncManualBackupRestoreError.transactionMismatch
            } catch BackupDetection.Error.manualRestoreStateAmbiguous {
                throw BigSyncManualBackupRestoreError.stateAmbiguous
            }

            switch preflight {
            case .completed(let existingReceipt):
                return makeManualRestoreReceipt(existingReceipt)

            case .resumeEvent(let existingReceipt):
                let publicReceipt = makeManualRestoreReceipt(existingReceipt)
                // The caller's journal determines whether this closure is an
                // idempotent verification/no-op or must finish installing the
                // replacement. The durable event means rollback is no longer
                // safe, even if this closure or sentinel publication fails.
                do {
                    try replacement()
                } catch {
                    throw BigSyncManualBackupRestoreError.handoffPending(
                        publicReceipt
                    )
                }
                do {
                    return try makeManualRestoreReceipt(
                        BackupDetection.beginManualRestore(
                            namespace: durableStateNamespace,
                            transactionIdentifier: transactionIdentifier,
                            sharedSentinelBaseURL: sharedStateBaseURL,
                            sentinelPublisher: sentinelPublisher
                        )
                    )
                } catch {
                    let currentIdentifier = publishedInstallationIdentifier()
                    guard currentIdentifier
                            == publicReceipt.oldInstallationIdentifier
                            || currentIdentifier
                            == publicReceipt.newInstallationIdentifier else {
                        throw BigSyncManualBackupRestoreError.stateAmbiguous
                    }
                    throw BigSyncManualBackupRestoreError.handoffPending(
                        publicReceipt
                    )
                }

            case .newTransaction, .resumeIntent(_):
                let intentReceipt: BackupDetection.ManualRestoreReceipt
                do {
                    intentReceipt = try BackupDetection.prepareManualRestoreIntent(
                        namespace: durableStateNamespace,
                        transactionIdentifier: transactionIdentifier,
                        sharedSentinelBaseURL: sharedStateBaseURL
                    )
                } catch BackupDetection.Error.manualRestoreTransactionMismatch {
                    throw BigSyncManualBackupRestoreError.transactionMismatch
                } catch BackupDetection.Error.manualRestoreStateAmbiguous {
                    throw BigSyncManualBackupRestoreError.stateAmbiguous
                }
                do {
                    try replacement()
                } catch let replacementError {
                    try rollback()
                    try BackupDetection.cancelManualRestoreIntent(
                        namespace: durableStateNamespace,
                        receipt: intentReceipt,
                        sharedSentinelBaseURL: sharedStateBaseURL
                    )
                    throw replacementError
                }

                do {
                    return try makeManualRestoreReceipt(
                        BackupDetection.beginManualRestore(
                            namespace: durableStateNamespace,
                            transactionIdentifier: transactionIdentifier,
                            sharedSentinelBaseURL: sharedStateBaseURL,
                            sentinelPublisher: sentinelPublisher
                        )
                    )
                } catch let publicationError {
                    // Determine whether event publication crossed its durable
                    // boundary. An intent alone still permits verified
                    // rollback; a durable event never does.
                    let stateAfterFailure: BackupDetection.ManualRestorePreflight
                    do {
                        stateAfterFailure = try BackupDetection.manualRestorePreflight(
                            namespace: durableStateNamespace,
                            transactionIdentifier: transactionIdentifier,
                            sharedSentinelBaseURL: sharedStateBaseURL
                        )
                    } catch BackupDetection.Error.manualRestoreTransactionMismatch {
                        throw BigSyncManualBackupRestoreError.transactionMismatch
                    } catch {
                        throw BigSyncManualBackupRestoreError.stateAmbiguous
                    }
                    switch stateAfterFailure {
                    case .completed(let receipt):
                        return makeManualRestoreReceipt(receipt)
                    case .resumeEvent(let receipt):
                        let publicReceipt = makeManualRestoreReceipt(receipt)
                        let currentIdentifier = publishedInstallationIdentifier()
                        guard currentIdentifier
                                == publicReceipt.oldInstallationIdentifier
                                || currentIdentifier
                                == publicReceipt.newInstallationIdentifier else {
                            throw BigSyncManualBackupRestoreError.stateAmbiguous
                        }
                        throw BigSyncManualBackupRestoreError.handoffPending(
                            publicReceipt
                        )
                    case .resumeIntent(let receipt):
                        try rollback()
                        try BackupDetection.cancelManualRestoreIntent(
                            namespace: durableStateNamespace,
                            receipt: receipt,
                            sharedSentinelBaseURL: sharedStateBaseURL
                        )
                        throw publicationError
                    case .newTransaction:
                        // The intent was durably published before replacement;
                        // losing it without a matching event is ambiguous.
                        throw BigSyncManualBackupRestoreError.stateAmbiguous
                    }
                }
            }
        }
    }

    private func makeManualRestoreReceipt(
        _ receipt: BackupDetection.ManualRestoreReceipt
    ) -> BigSyncManualBackupRestoreReceipt {
        BigSyncManualBackupRestoreReceipt(
            transactionIdentifier: receipt.transactionIdentifier,
            restoreEventIdentifier: receipt.restoreEventIdentifier,
            oldInstallationIdentifier: receipt.oldInstallationIdentifier,
            newInstallationIdentifier: receipt.newInstallationIdentifier
        )
    }

    private func publishedInstallationIdentifier() -> String? {
        BackupDetection.installationIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
    }

    @available(*, deprecated, message: "Supply a stable transactionIdentifier and persist the returned receipt.")
    public func withManualBackupRestore<T>(
        _ replacement: () throws -> T,
        rollback: () throws -> Void = {}
    ) throws -> String {
        try withManualBackupRestore(
            transactionIdentifier: UUID(),
            {
                _ = try replacement()
            },
            rollback: rollback
        ).newInstallationIdentifier
    }

    private var leaseURL: URL {
        BackupDetection.defaultSentinelURL(
            namespace: durableStateNamespace,
            sharedBaseURL: sharedStateBaseURL
        ).appendingPathExtension("lease")
    }

    /// Captures publication authority from the exact store used by the worker.
    /// Custom non-file stores cannot establish cross-process admission.
    public func capturePublicationAdmission(store: FileKeyValueStore? = nil) throws -> BigSyncReplicaPublicationAdmission {
        guard !BigSyncReplicaPublicationReadScope.isActive else {
            throw BigSyncReplicaPublicationError.reentrantPublication
        }
        let store = store ?? FileKeyValueStore(fileURL: synchronizationStateFileURL)
        return try BigSyncClientIdentityLeaseRegistry.withPublication(at: leaseURL) {
            guard let installation = publicationInstallationIdentifier() else {
                throw BigSyncReplicaPublicationError.unavailable
            }
            return try store.withDurableSnapshot { values in
                let key = durableStateNamespace + ".ReplicaBinding.v1"
                guard let raw = values[key] else { throw BigSyncReplicaPublicationError.unavailable }
                let binding = try BigSyncReplicaBindingStateStore.decode(raw)
                guard let epoch = try BigSyncReplicaBindingStateStore.publicationEpoch(from: raw),
                      binding.pendingPort == nil,
                      binding.installationIdentityDigest == BigSyncReplicaBindingStateStore.installationIdentityDigest(for: installation)
                else { throw BigSyncReplicaPublicationError.unavailable }
                return BigSyncReplicaPublicationAdmission(identity: self, store: store,
                    installationIdentifier: installation, binding: binding, publicationEpoch: epoch)
            }
        }
    }

    // Called only under the registry's admission. Deliberately bypasses its
    // cached getter: this must neither recurse into a lock nor accept an intent.
    fileprivate func publicationInstallationIdentifier() -> String? {
        guard !BackupDetection.manualRestoreIntentIsRequired(namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL), let installation = publishedInstallationIdentifier() else { return nil }
        if let receipt = BackupDetection.manualRestoreReceipt(namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL), receipt.newInstallationIdentifier != installation { return nil }
        if BackupDetection.restoreResetIsRequired(namespace: durableStateNamespace, sharedSentinelBaseURL: sharedStateBaseURL),
           BackupDetection.restoreResetEventIdentifier(namespace: durableStateNamespace, sharedSentinelBaseURL: sharedStateBaseURL) == nil { return nil }
        return installation
    }

    fileprivate func withPublication<Value>(_ body: () throws -> Value) throws -> Value {
        try BigSyncClientIdentityLeaseRegistry.withPublication(at: leaseURL, body)
    }
}

public extension Notification.Name {
    /// Wake for identity owners. Delivered after durable binding writes/restore
    /// locks release. It is not publication authority; retain an admission instead.
    static let BigSyncReplicaPublicationAuthorityDidChange = Notification.Name("BigSyncReplicaPublicationAuthorityDidChange")
}

public enum BigSyncReplicaPublicationError: Error, Equatable { case unavailable, superseded, reentrantPublication }

/// Read-only borrowing for Realm's synchronous mutation-journal attribution
/// inside a held publication. This is never retained beyond that stack frame.
/// It avoids recursively acquiring installation/binding locks during COMMIT.
enum BigSyncReplicaPublicationReadScope {
    private static let key = "BigSyncReplicaPublicationReadScope"
    static var isActive: Bool { Thread.current.threadDictionary[key] != nil }
    final class Borrowed: NSObject {
        let namespace: String
        let root: URL
        let storeURL: URL
        let identity: BigSyncMutationJournalIdentity
        let binding: BigSyncReplicaBindingSnapshot
        let publicationEpoch: String
        init(client: BigSyncClientIdentity, store: FileKeyValueStore, identity: BigSyncMutationJournalIdentity,
             binding: BigSyncReplicaBindingSnapshot, publicationEpoch: String) {
            namespace = client.durableStateNamespace; root = client.sharedStateBaseURL
            storeURL = store.fileURL.standardizedFileURL; self.identity = identity
            self.binding = binding; self.publicationEpoch = publicationEpoch
        }
    }
    static func current(identity: BigSyncClientIdentity) -> Borrowed? {
        guard let value = Thread.current.threadDictionary[key] as? Borrowed,
              value.namespace == identity.durableStateNamespace, value.root == identity.sharedStateBaseURL else { return nil }
        return value
    }
    static func withValue<Value>(_ value: Borrowed, _ body: () throws -> Value) throws -> Value {
        guard Thread.current.threadDictionary[key] == nil else { throw BigSyncReplicaPublicationError.reentrantPublication }
        Thread.current.threadDictionary[key] = value
        defer { Thread.current.threadDictionary.removeObject(forKey: key) }
        return try body()
    }
}

/// Pins the accepted installation and complete binding state, including owner.
/// Pending port generations are journal attribution, not publication authority.
public struct BigSyncReplicaPublicationAdmission: @unchecked Sendable {
    private let identity: BigSyncClientIdentity
    private let store: FileKeyValueStore
    public let installationIdentifier: String
    public var replicaBindingGenerationIdentifier: String { binding.activeGenerationIdentifier }
    private let binding: BigSyncReplicaBindingSnapshot
    private let publicationEpoch: String

    fileprivate init(identity: BigSyncClientIdentity, store: FileKeyValueStore,
        installationIdentifier: String, binding: BigSyncReplicaBindingSnapshot, publicationEpoch: String) {
        self.identity = identity; self.store = store
        self.installationIdentifier = installationIdentifier; self.binding = binding
        self.publicationEpoch = publicationEpoch
    }

    /// For a composed inner consumer already enclosed by withCurrent(). This
    /// checks exact authority without recursively entering replica/store locks.
    public func validateHeldPublication() throws {
        guard let borrowed = BigSyncReplicaPublicationReadScope.current(identity: identity),
              borrowed.storeURL == store.fileURL.standardizedFileURL,
              borrowed.identity.installationIdentifier == installationIdentifier,
              borrowed.binding == binding, borrowed.publicationEpoch == publicationEpoch else {
            throw BigSyncReplicaPublicationError.superseded
        }
    }

    /// Holds installation/restore admission and the existing cross-process
    /// binding file guard THROUGH the caller's COMMIT. Enter after SQL/file and
    /// account admission. The bounded synchronous callback may install values or
    /// transact on already-admitted destination handles. It must not await,
    /// enter Session, SQL executors, store APIs, or start lifecycle/replacement work. Realm's
    /// journal may read the already-held identity via its read-only borrowed
    /// scope; nested publication admission is rejected before acquiring locks.
    public func withCurrent<Value>(_ body: () throws -> Value) throws -> Value {
        guard !BigSyncReplicaPublicationReadScope.isActive else {
            throw BigSyncReplicaPublicationError.reentrantPublication
        }
        return try identity.withPublication {
            guard identity.publicationInstallationIdentifier() == installationIdentifier else {
                throw BigSyncReplicaPublicationError.superseded
            }
            return try store.withDurableSnapshot { values in
                guard let raw = values[identity.durableStateNamespace + ".ReplicaBinding.v1"],
                      try BigSyncReplicaBindingStateStore.decode(raw) == binding,
                      try BigSyncReplicaBindingStateStore.publicationEpoch(from: raw) == publicationEpoch,
                      binding.pendingPort == nil else { throw BigSyncReplicaPublicationError.superseded }
                return try BigSyncReplicaPublicationReadScope.withValue(.init(client: identity, store: store,
                    identity: .init(installationIdentifier: installationIdentifier,
                        replicaBindingGenerationIdentifier: replicaBindingGenerationIdentifier),
                    binding: binding, publicationEpoch: publicationEpoch), body)
            }
        }
    }
}
