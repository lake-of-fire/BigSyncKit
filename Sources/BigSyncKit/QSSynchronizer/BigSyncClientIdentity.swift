import CloudKit
import Darwin
import Foundation

/// A manual restore could otherwise replace an app-group Realm while an
/// extension still has open Realm handles. Configured processes publish a
/// presence file for their lifetime; restore checks those files under a short
/// exclusive lock before touching Realm files or installation identity.
/// No advisory lock remains held while a configured process is idle/suspended.
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
    /// A presence file is a conservative veto, not an expiring lease. A
    /// suspended process is alive and must continue to prevent replacement.
    /// PID reuse and unavailable liveness probes retain the veto. Only ESRCH
    /// or a proven different boot permits collecting another process's file.
    struct ProcessOwner: Codable {
        let identifier: UUID
        let pid: Int32
        let bootSession: UUID?

        func isDefinitelyExited(
            currentBootSession: UUID?,
            isProcessAlive: (Int32) -> Bool
        ) -> Bool {
            guard pid > 0 else { return false }
            if let bootSession, let currentBootSession,
               bootSession != currentBootSession { return true }
            return !isProcessAlive(pid)
        }
    }

    private enum Mode { case registered, exclusive }

    private final class Lease {
        let descriptor: Int32
        var mode = Mode.registered
        var cachedInstallationIdentifier: String?

        init(descriptor: Int32) { self.descriptor = descriptor }
        deinit { Darwin.close(descriptor) }
    }

    // Replacement/rollback can synchronously reenter identity validation.
    // Reentry may inspect the cache, but cannot release the outer restore fence.
    private static let lock = NSRecursiveLock()
    private static var leases = [String: Lease]()
    private static let processOwner = ProcessOwner(
        identifier: UUID(), pid: getpid(), bootSession: currentBootSession()
    )

    static func retainProcessPresence(at url: URL) throws {
        lock.lock()
        defer { lock.unlock() }
        _ = try lease(at: url)
    }

    static func cachedInstallationIdentifier(at url: URL) -> String? {
        lock.lock()
        defer { lock.unlock() }
        return leases[url.standardizedFileURL.path]?.cachedInstallationIdentifier
    }

    static func publishInstallationIdentifier(_ identifier: String, at url: URL) {
        guard !identifier.isEmpty else { return }
        lock.lock()
        defer { lock.unlock() }
        leases[url.standardizedFileURL.path]?.cachedInstallationIdentifier = identifier
    }

    static func invalidateInstallationIdentifier(at url: URL) {
        lock.lock()
        defer { lock.unlock() }
        leases[url.standardizedFileURL.path]?.cachedInstallationIdentifier = nil
    }

    static func withExclusive<T>(at url: URL, _ operation: () throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        for _ in 0 ..< 3 {
            let lease = try lease(at: url, waitsForRegistration: false)
            guard lease.mode == .registered else {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            guard bigSyncFlock(lease.descriptor, LOCK_EX | LOCK_NB) == 0 else {
                throw leaseError(errno)
            }
            // Reopening a replaced path must not split coordination between
            // this cached descriptor and a newly registering app/extension.
            guard descriptorMatchesPath(lease.descriptor, at: url) else {
                _ = bigSyncFlock(lease.descriptor, LOCK_UN)
                leases.removeValue(forKey: url.standardizedFileURL.path)
                continue
            }
            lease.mode = .exclusive
            defer {
                lease.mode = .registered
                // Closing on an unlock failure still releases the kernel lock;
                // process presence survives and the next call can reopen it.
                if bigSyncFlock(lease.descriptor, LOCK_UN) != 0 {
                    leases.removeValue(forKey: url.standardizedFileURL.path)
                }
            }
            try requireNoOtherProcesses(at: url)
            // No mutation may reuse the pre-restore identity after this boundary.
            lease.cachedInstallationIdentifier = nil
            return try operation()
        }
        throw BigSyncClientIdentityLeaseError.leaseUnavailable(EBUSY)
    }

    static func ownerDirectory(at url: URL) -> URL {
        url.appendingPathExtension("owners")
    }

    private static func requireNoOtherProcesses(at url: URL) throws {
        let directory = ownerDirectory(at: url)
        for file in try FileManager.default.contentsOfDirectory(
            at: directory, includingPropertiesForKeys: nil
        ) where file.pathExtension == "plist" {
            // An unreadable/malformed claim is never evidence of process exit.
            guard let owner = try? PropertyListDecoder().decode(
                ProcessOwner.self, from: Data(contentsOf: file)
            ), owner.pid > 0 else {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            if owner.identifier == processOwner.identifier { continue }
            guard owner.isDefinitelyExited(
                currentBootSession: processOwner.bootSession,
                isProcessAlive: { kill($0, 0) == 0 || errno != ESRCH }
            ) else { throw BigSyncClientIdentityLeaseError.restoreInProgress }
            try FileManager.default.removeItem(at: file)
        }
    }

    private static func lease(at url: URL, waitsForRegistration: Bool = true) throws -> Lease {
        let key = url.standardizedFileURL.path
        if let lease = leases[key] {
            // Read-only reentry cannot disturb an active restore, even if its
            // caller is inspecting/replacing files under the exclusive fence.
            if lease.mode == .exclusive || descriptorMatchesPath(lease.descriptor, at: url) {
                return lease
            }
            leases.removeValue(forKey: key)
        }
        try bigSyncCreateDirectoryDurably(at: url.deletingLastPathComponent())
        for _ in 0 ..< 3 {
            if let lease = try registerLease(at: url, waits: waitsForRegistration) {
                leases[key] = lease
                return lease
            }
        }
        throw BigSyncClientIdentityLeaseError.leaseUnavailable(EBUSY)
    }

    /// Returns nil only when the coordination path changed during acquisition.
    private static func registerLease(at url: URL, waits: Bool) throws -> Lease? {
        let descriptor = Darwin.open(url.path, O_CREAT | O_RDWR | O_CLOEXEC, S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else { throw leaseError(errno) }
        var retained = false
        defer { if !retained { Darwin.close(descriptor) } }
        // Ordinary installation preparation may wait for a running restore.
        // A nonblocking restore API must remain nonblocking even before this
        // process has registered or when a stale descriptor needs reopening.
        guard bigSyncFlock(descriptor, LOCK_SH | (waits ? 0 : LOCK_NB)) == 0 else {
            throw leaseError(errno)
        }
        guard descriptorMatchesPath(descriptor, at: url) else { return nil }
        let directory = ownerDirectory(at: url)
        try bigSyncCreateDirectoryDurably(at: directory)
        var excluded = URLResourceValues()
        excluded.isExcludedFromBackup = true
        var mutableURL = url
        try mutableURL.setResourceValues(excluded)
        var mutableDirectory = directory
        try mutableDirectory.setResourceValues(excluded)
        // SH protects only atomic presence publication, never idle lifetime.
        try PropertyListEncoder().encode(processOwner).write(
            to: directory.appendingPathComponent(processOwner.identifier.uuidString + ".plist"),
            options: .atomic
        )
        guard bigSyncFlock(descriptor, LOCK_UN) == 0 else { throw leaseError(errno) }
        let lease = Lease(descriptor: descriptor)
        retained = true
        return lease
    }

    private static func descriptorMatchesPath(_ descriptor: Int32, at url: URL) -> Bool {
        var held = stat()
        var current = stat()
        return Darwin.fstat(descriptor, &held) == 0
            && Darwin.lstat(url.path, &current) == 0
            && held.st_dev == current.st_dev && held.st_ino == current.st_ino
    }

    private static func leaseError(_ code: Int32) -> BigSyncClientIdentityLeaseError {
        code == EWOULDBLOCK || code == EAGAIN ? .restoreInProgress : .leaseUnavailable(code)
    }

    private static func currentBootSession() -> UUID? {
        var size = 0
        guard sysctlbyname("kern.bootsessionuuid", nil, &size, nil, 0) == 0,
              size > 1, size < 256 else { return nil }
        var buffer = [CChar](repeating: 0, count: size)
        guard sysctlbyname("kern.bootsessionuuid", &buffer, &size, nil, 0) == 0 else { return nil }
        let value = String(decoding: buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self)
        return UUID(uuidString: value)
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
        try BigSyncClientIdentityLeaseRegistry.retainProcessPresence(at: leaseURL)
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
        makeMutationJournalIdentityProvider()()
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
    public func cancelManualBackupRestoreIntent(
        transactionIdentifier: UUID
    ) throws {
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
            let preflight: BackupDetection.ManualRestorePreflight
            do {
                preflight = try BackupDetection.manualRestorePreflight(
                    namespace: durableStateNamespace,
                    transactionIdentifier: transactionIdentifier,
                    sharedSentinelBaseURL: sharedStateBaseURL,
                    allowsUninitializedCancellation: true
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
            replacement,
            rollback: rollback,
            sentinelPublisher: nil,
            validation: validation
        )
    }

    /// Test seam for the event-before-sentinel crash prefix. Production always
    /// uses BackupDetection's atomic excluded-sentinel publisher.
    func withManualBackupRestore(
        transactionIdentifier: UUID,
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {},
        sentinelPublisher: ((URL, FileManager) throws -> Void)?,
        validation: (() throws -> Void)? = nil
    ) throws -> BigSyncManualBackupRestoreReceipt {
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
            // Validate the captured account/storage boundary under the same
            // exclusion as replacement, including retries of completed restores.
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
}
