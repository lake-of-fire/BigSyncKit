import CloudKit
import Darwin
import Foundation
import RealmSwiftGaps

/// A manual restore could otherwise replace an app-group Realm while an
/// extension has an open Realm. Configured clients publish process presence;
/// restore excludes live peers before touching files or installation identity.
/// Idle iOS processes must not retain shared-container advisory locks.
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

/// Process presence preserves the lifetime restore fence without retaining a
/// file lock across iOS suspension. Records are immutable and atomically saved.
/// A restore publishes its gate before scanning records; registration publishes
/// its record before checking that gate. Either the restorer sees the client or
/// the client sees the restorer, so neither can miss the other's admission.
final class BigSyncClientProcessPresence {
    struct Birth: Codable, Equatable, Sendable {
        let seconds: Int64
        let microseconds: Int64

        static func observed(seconds: Int64, microseconds: Int64) -> Self? {
            guard seconds > 0, (0..<1_000_000).contains(microseconds) else { return nil }
            return Self(seconds: seconds, microseconds: microseconds)
        }
    }

    struct Owner: Codable, Equatable, Sendable {
        let pid: Int32
        let birth: Birth?
        let nonce: UUID
    }

    private struct Record: Codable {
        let version: Int
        let owner: Owner
    }

    private enum Observation {
        case alive(Birth?)
        case dead
    }

    let owner: Owner
    let directoryURL: URL
    let recordURL: URL
    let gateURL: URL
    private let isAlive: @Sendable (Owner) -> Bool

    init(
        leaseURL: URL,
        owner: Owner? = nil,
        isAlive: @escaping @Sendable (Owner) -> Bool = BigSyncClientProcessPresence.processIsAlive
    ) throws {
        let owner = owner ?? Self.currentOwner()
        self.owner = owner
        self.isAlive = isAlive
        directoryURL = leaseURL.appendingPathExtension("clients")
        recordURL = directoryURL.appendingPathComponent(owner.nonce.uuidString + ".json")
        gateURL = directoryURL.appendingPathComponent("restore")
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        let values = try directoryURL.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey])
        guard values.isDirectory == true, values.isSymbolicLink != true else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(EINVAL)
        }
        var exclusions = URLResourceValues()
        exclusions.isExcludedFromBackup = true
        var directory = directoryURL
        try directory.setResourceValues(exclusions)
        do {
            try publishOwner(at: recordURL)
            try rejectForeignRestore()
            pruneDeadClients()
        } catch {
            try? FileManager.default.removeItem(at: recordURL)
            throw error
        }
    }

    deinit { try? FileManager.default.removeItem(at: recordURL) }

    private func pruneDeadClients() {
        // Widget processes can exit without running deinit. Retire their
        // immutable records on later registrations, without revoking unknown
        // or unreadable peers and without waiting for a manual restore.
        guard let urls = try? FileManager.default.contentsOfDirectory(
            at: directoryURL, includingPropertiesForKeys: nil
        ) else { return }
        for url in urls where url.pathExtension == "json" {
            guard let peer = try? readOwner(at: url), peer != owner, !isAlive(peer) else { continue }
            try? FileManager.default.removeItem(at: url)
        }
    }

    /// Caller holds the namespace's exclusive advisory lock throughout this
    /// method and the actual replacement, and serializes same-process reentry.
    func beginExclusive() throws {
        try rejectForeignRestore()
        try publishOwner(at: gateURL)
        var admitted = false
        defer { if !admitted { endExclusive() } }
        for url in try FileManager.default.contentsOfDirectory(
            at: directoryURL, includingPropertiesForKeys: nil
        ) where url.pathExtension == "json" {
            let peer: Owner
            do { peer = try readOwner(at: url) }
            catch where Self.isMissing(error) { continue }
            if peer == owner { continue }
            guard !isAlive(peer) else {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            // Nonces never change ownership. A dead client's immutable record
            // cannot become a newly registered client's record through PID reuse.
            try? FileManager.default.removeItem(at: url)
        }
        admitted = true
    }

    func endExclusive() {
        // Only restore owners write this path and the caller still holds the
        // exclusive namespace lock. A failed unlink stays fail-closed for peers
        // until the owner exits; this process remains serialized by its registry.
        try? FileManager.default.removeItem(at: gateURL)
    }

    private func rejectForeignRestore() throws {
        let restore: Owner
        do { restore = try readOwner(at: gateURL) }
        catch where Self.isMissing(error) { return }
        if restore != owner, isAlive(restore) {
            throw BigSyncClientIdentityLeaseError.restoreInProgress
        }
    }

    private func publishOwner(at url: URL) throws {
        let data = try JSONEncoder().encode(Record(version: 1, owner: owner))
        try data.write(to: url, options: .atomic)
    }

    private func readOwner(at url: URL) throws -> Owner {
        let values = try url.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey])
        guard values.isRegularFile == true, values.isSymbolicLink != true,
              let size = values.fileSize, size <= 16_384 else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(EINVAL)
        }
        let record = try JSONDecoder().decode(Record.self, from: Data(contentsOf: url))
        guard record.version == 1, record.owner.pid > 0,
              record.owner.birth.map({ $0.seconds > 0 && (0..<1_000_000).contains($0.microseconds) }) != false else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(EINVAL)
        }
        return record.owner
    }

    private static func isMissing(_ error: Error) -> Bool {
        let error = error as NSError
        return (error.domain == NSCocoaErrorDomain && (error.code == NSFileReadNoSuchFileError || error.code == NSFileNoSuchFileError))
            || (error.domain == NSPOSIXErrorDomain && error.code == Int(ENOENT))
    }

    private static func currentOwner() -> Owner {
        let pid = getpid()
        let birth: Birth?
        if case .alive(let observed) = observe(pid) { birth = observed }
        else { birth = nil }
        return Owner(pid: pid, birth: birth, nonce: UUID())
    }

    static func processIsAlive(_ owner: Owner) -> Bool {
        guard owner.pid > 0 else { return true }
        switch observe(owner.pid) {
        case .dead: return false
        case .alive(let actual):
            if let expected = owner.birth, let actual { return expected == actual }
            // A sandbox denial is not proof that another client exited.
            return true
        }
    }

    private static func observe(_ pid: Int32) -> Observation {
        var query: [Int32] = [CTL_KERN, KERN_PROC, KERN_PROC_PID, pid]
        var info = kinfo_proc()
        var size = MemoryLayout<kinfo_proc>.stride
        let result = query.withUnsafeMutableBufferPointer {
            sysctl($0.baseAddress, UInt32($0.count), &info, &size, nil, 0)
        }
        if result == 0 {
            if size == MemoryLayout<kinfo_proc>.stride, info.kp_proc.p_pid == pid {
                let start = info.kp_proc.p_un.__p_starttime
                // XNU stores this at process creation and preserves it through
                // exec. Missing/redacted values cannot prove PID reuse.
                return .alive(Birth.observed(seconds: Int64(start.tv_sec), microseconds: Int64(start.tv_usec)))
            }
        } else if errno == ESRCH {
            return .dead
        }
        // A zero-length or denied sysctl response is not proof of exit.
        // Signal zero is a liveness query; it sends no signal to the process.
        if Darwin.kill(pid, 0) == -1, errno == ESRCH { return .dead }
        return .alive(nil)
    }
}

enum BigSyncClientIdentityLeaseRegistry {
    // Desktop app copies can overlap across versions. Older restorers know only
    // flock, so desktop clients must preserve that fence. iOS updates replace
    // the containing app and its extensions together; their idle clients use
    // presence instead of retaining a shared-container lock through suspension.
    private static var platformRetainsSharedLock: Bool {
        #if os(macOS) || targetEnvironment(macCatalyst)
        true
        #elseif os(iOS)
        ProcessInfo.processInfo.isiOSAppOnMac
        #else
        false
        #endif
    }

    #if DEBUG
    @TaskLocal static var retainsSharedLockForTesting: Bool? = nil
    #endif

    private static var retainsSharedLock: Bool {
        #if DEBUG
        retainsSharedLockForTesting ?? platformRetainsSharedLock
        #else
        platformRetainsSharedLock
        #endif
    }

    private enum Mode {
        case registered
        case exclusive
    }

    private final class Lease {
        let descriptor: Int32
        let presence: BigSyncClientProcessPresence
        let retainsSharedLock: Bool
        var mode: Mode
        var cachedInstallationIdentifier: String?

        init(descriptor: Int32, presence: BigSyncClientProcessPresence, retainsSharedLock: Bool) {
            self.descriptor = descriptor
            self.presence = presence
            self.retainsSharedLock = retainsSharedLock
            self.mode = .registered
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
        let execution = try RealmWriteExecutionProtection.acquireSynchronousLease()
        defer { execution?.release() }
        try execution?.checkpoint()
        lock.lock()
        defer { lock.unlock() }
        let lease = try lease(at: url)
        // A recursive registry lock permits read-only identity probes, not
        // another restore of this client. Its nested defer would otherwise
        // release the outer operation's cross-process exclusion too early.
        guard lease.mode == .registered else {
            throw BigSyncClientIdentityLeaseError.restoreInProgress
        }
        if lease.retainsSharedLock, bigSyncFlock(lease.descriptor, LOCK_UN) != 0 {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
        }
        guard bigSyncFlock(lease.descriptor, LOCK_EX | LOCK_NB) == 0 else {
            let lockError = Int32(errno)
            // Desktop clients must reacquire their legacy fence before any
            // caller can continue using an already-open Realm.
            if lease.retainsSharedLock, bigSyncFlock(lease.descriptor, LOCK_SH) != 0 {
                throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
            }
            if lockError == EWOULDBLOCK || lockError == EAGAIN {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        defer {
            if lease.retainsSharedLock {
                _ = bigSyncFlock(lease.descriptor, LOCK_SH)
            } else {
                _ = bigSyncFlock(lease.descriptor, LOCK_UN)
            }
        }
        try execution?.checkpoint()
        try lease.presence.beginExclusive()
        defer { lease.presence.endExclusive() }
        lease.mode = .exclusive
        // No mutation may reuse the pre-restore identity after this boundary.
        // The durable intent publication below also invalidates this cache for
        // callers that exercise BackupDetection directly.
        lease.cachedInstallationIdentifier = nil
        defer { lease.mode = .registered }
        return try operation()
    }

    private static func lease(at url: URL) throws -> Lease {
        let key = url.standardizedFileURL.path
        if let lease = leases[key] { return lease }
        let execution = try RealmWriteExecutionProtection.acquireSynchronousLease()
        defer { execution?.release() }
        try execution?.checkpoint()
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
        // Even suspension-safe clients reject an already-active legacy
        // restorer. This short registration lock is execution-protected.
        guard bigSyncFlock(descriptor, LOCK_SH | LOCK_NB) == 0 else {
            let lockError = Int32(errno)
            Darwin.close(descriptor)
            if lockError == EWOULDBLOCK || lockError == EAGAIN {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        let presence: BigSyncClientProcessPresence
        do {
            presence = try BigSyncClientProcessPresence(leaseURL: url)
        } catch {
            Darwin.close(descriptor)
            throw error
        }
        let retainsSharedLock = Self.retainsSharedLock
        if !retainsSharedLock, bigSyncFlock(descriptor, LOCK_UN) != 0 {
            let lockError = Int32(errno)
            Darwin.close(descriptor)
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        let lease = Lease(descriptor: descriptor, presence: presence, retainsSharedLock: retainsSharedLock)
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
        let provider = makeThrowingMutationJournalIdentityProvider()
        return { try? provider() }
    }

    /// Mutation writers preserve storage/admission errors so the owning Realm
    /// transaction can roll back instead of treating cancellation as corruption.
    public func makeThrowingMutationJournalIdentityProvider()
        -> @Sendable () throws -> BigSyncMutationJournalIdentity? {
        let reader = BigSyncMutationJournalIdentityReader(
            clientIdentity: self,
            store: FileKeyValueStore(
                fileURL: synchronizationStateFileURL
            ),
            key: durableStateNamespace + ".ReplicaBinding.v1"
        )
        return { try reader.currentThrowing() }
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
    /// With no durable manual records, cancellation is an idempotent no-op
    /// even before first installation; it never establishes identity.
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
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
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
}
