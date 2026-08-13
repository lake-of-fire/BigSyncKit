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
        guard bigSyncFlock(lease.descriptor, LOCK_SH) == 0 else {
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
            guard bigSyncFlock(lease.descriptor, LOCK_UN) == 0 else {
                throw BigSyncClientIdentityLeaseError.leaseUnavailable(Int32(errno))
            }
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
        defer {
            // Downgrading before returning prevents subsequent mutations in
            // this process from escaping the cross-process restore fence.
            if bigSyncFlock(lease.descriptor, LOCK_SH) == 0 {
                lease.mode = .shared
            }
        }
        return try operation()
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
        return identifier
    }

    public func currentInstallationIdentifier() -> String? {
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
        return identifier
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
                    sharedSentinelBaseURL: sharedStateBaseURL
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
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {}
    ) throws -> BigSyncManualBackupRestoreReceipt {
        try withManualBackupRestore(
            transactionIdentifier: transactionIdentifier,
            replacement,
            rollback: rollback,
            sentinelPublisher: nil
        )
    }

    /// Test seam for the event-before-sentinel crash prefix. Production always
    /// uses BackupDetection's atomic excluded-sentinel publisher.
    func withManualBackupRestore(
        transactionIdentifier: UUID,
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {},
        sentinelPublisher: ((URL, FileManager) throws -> Void)?
    ) throws -> BigSyncManualBackupRestoreReceipt {
        try BigSyncClientIdentityLeaseRegistry.withExclusive(at: leaseURL) {
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
