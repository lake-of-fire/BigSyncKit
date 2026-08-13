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

private enum BigSyncClientIdentityLeaseRegistry {
    private enum Mode {
        case shared
        case exclusive
    }

    private final class Lease {
        let descriptor: Int32
        var mode: Mode

        init(descriptor: Int32, mode: Mode) {
            self.descriptor = descriptor
            self.mode = mode
        }

        deinit {
            Darwin.close(descriptor)
        }
    }

    private static let lock = NSLock()
    private static var leases = [String: Lease]()

    static func retainShared(at url: URL) throws {
        lock.lock()
        defer { lock.unlock() }
        let lease = try lease(at: url)
        guard lease.mode != .shared else { return }
        guard flock(lease.descriptor, LOCK_SH) == 0 else {
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
        }
        lease.mode = .shared
    }

    static func withExclusive<T>(
        at url: URL,
        _ operation: () throws -> T
    ) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        let lease = try lease(at: url)
        if lease.mode == .shared {
            guard flock(lease.descriptor, LOCK_UN) == 0 else {
                throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
            }
        }
        guard flock(lease.descriptor, LOCK_EX | LOCK_NB) == 0 else {
            let lockError = Int32(errno)
            // Restore the process-lifetime writer lease before reporting a
            // competing app/extension holder. A later launch can retry the
            // staged restore without a window for an unfenced local write.
            guard flock(lease.descriptor, LOCK_SH) == 0 else {
                throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
            }
            lease.mode = .shared
            if lockError == EWOULDBLOCK || lockError == EAGAIN {
                throw BigSyncClientIdentityLeaseError.restoreInProgress
            }
            throw BigSyncClientIdentityLeaseError.leaseUnavailable(lockError)
        }
        lease.mode = .exclusive
        defer {
            // Downgrading before returning prevents subsequent mutations in
            // this process from escaping the cross-process restore fence.
            if flock(lease.descriptor, LOCK_SH) == 0 {
                lease.mode = .shared
            }
        }
        return try operation()
    }

    private static func lease(at url: URL) throws -> Lease {
        let key = url.standardizedFileURL.path
        if let lease = leases[key] { return lease }
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
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
        guard flock(descriptor, LOCK_SH) == 0 else {
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
        guard let identifier = BackupDetection.installationIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        ) else {
            throw CocoaError(.fileReadCorruptFile)
        }
        return identifier
    }

    public func currentInstallationIdentifier() -> String? {
        BackupDetection.installationIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: sharedStateBaseURL
        )
    }

    /// Publishes a manual Realm-backup restore before synchronization metadata
    /// is discarded. The returned fresh installation identity must be installed
    /// for every restored target configuration before it is reopened.
    @discardableResult
    public func beginManualBackupRestore() throws -> String {
        try withManualBackupRestore {}
    }

    /// Runs a replacement while no other configured app-group process can
    /// commit a Realm mutation. The fresh identity is published only after the
    /// replacement closure succeeds.
    @discardableResult
    public func withManualBackupRestore<T>(
        _ replacement: () throws -> T,
        rollback: () throws -> Void = {}
    ) throws -> String {
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
            do {
                _ = try replacement()
                return try BackupDetection.beginManualRestore(
                    namespace: durableStateNamespace,
                    sharedSentinelBaseURL: sharedStateBaseURL
                )
            } catch {
                let replacementError = error
                // The caller's target Realm rollback must complete before the
                // exclusive app-group lease is downgraded. Otherwise a peer
                // could journal a write into the transient replacement.
                // A throwing rollback deliberately replaces the original
                // error: callers must treat an unverified repair as the
                // terminal failure and must not continue into Realm startup.
                try rollback()
                throw replacementError
            }
        }
    }

    private var leaseURL: URL {
        BackupDetection.defaultSentinelURL(
            namespace: durableStateNamespace,
            sharedBaseURL: sharedStateBaseURL
        ).appendingPathExtension("lease")
    }
}
