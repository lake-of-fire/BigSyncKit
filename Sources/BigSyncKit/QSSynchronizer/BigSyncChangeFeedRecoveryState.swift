import Foundation

public enum ChangeFeedResetMode: String, Sendable {
    /// The bounded ordinary discovery of objects that predate the durable
    /// mutation journal. Explicit retained-data recovery modes separately
    /// authorize re-upload under their own recovery obligation.
    case initialImport
    /// Reconcile a full server bootstrap conservatively. A previously
    /// server-backed record that is now absent must not be resurrected.
    case serverReconciliation
    /// A device/app backup contains a historical snapshot of the local outbox.
    /// Keep target Realm user objects, but do not replay copied mutation
    /// generations or rediscover untracked objects as current local intent.
    case backupRestore
    /// CloudKit explicitly reset the account's encrypted data. The direct
    /// database API documents that locally retained live data may be
    /// re-uploaded, so rebuild durable upload generations without changing the
    /// target objects themselves.
    case encryptedDataReset
    /// The authenticated account changed while the application retained one
    /// admitted local dataset. Rebuild the destination replica from local
    /// rows without copying an old CloudKit zone.
    case localDatasetRebootstrap

    var reuploadsRetainedLocalData: Bool {
        self == .encryptedDataReset || self == .localDatasetRebootstrap
    }
}

/// Durable progress for initial feed adoption and later account-scoped recovery.
/// This lives beside the synchronizer's other metadata rather than in a target
/// Realm, so recovery can resume before an adapter opens its tracking Realm.
struct ChangeFeedMigrationState: Equatable {
    static let version = 3
    // Epochs are durable adapter identities, not merely counters inside one KVS
    // key. Reserve a disjoint range for each future migration version so a v2
    // reset can never be mistaken for an already-completed v1 reset.
    static let epochRangeSize = 1_000_000_000
    static var initialEpoch: Int {
        (version * epochRangeSize) + 1
    }

    enum Phase: String {
        case requested
        case prepared
        case serverBootstrap
        case reconciled
        case finishing
        case completed
    }

    let key: String
    let accountScopeIdentifier: String
    let zoneName: String
    let zoneOwnerName: String
    let epoch: Int
    var mode: ChangeFeedResetMode
    var phase: Phase
    let backupRestoreEventIdentifier: String?
    let replicaJournalHandoff: BigSyncReplicaJournalHandoff?

    init(
        key: String,
        accountScopeIdentifier: String,
        zoneName: String,
        zoneOwnerName: String,
        epoch: Int,
        mode: ChangeFeedResetMode,
        phase: Phase,
        backupRestoreEventIdentifier: String? = nil,
        replicaJournalHandoff: BigSyncReplicaJournalHandoff? = nil
    ) {
        self.key = key
        self.accountScopeIdentifier = accountScopeIdentifier
        self.zoneName = zoneName
        self.zoneOwnerName = zoneOwnerName
        self.epoch = epoch
        self.mode = mode
        self.phase = phase
        self.backupRestoreEventIdentifier = backupRestoreEventIdentifier
        self.replicaJournalHandoff = replicaJournalHandoff
    }

    init?(
        key: String,
        propertyList: [String: Any],
        accountScopeIdentifier: String,
        zoneName: String,
        zoneOwnerName: String
    ) {
        guard let versionNumber = propertyList["version"] as? NSNumber,
              Int(exactly: versionNumber) == Self.version,
              propertyList["accountScopeIdentifier"] as? String
                == accountScopeIdentifier,
              propertyList["zoneName"] as? String == zoneName,
              propertyList["zoneOwnerName"] as? String == zoneOwnerName,
              let epochNumber = propertyList["epoch"] as? NSNumber,
              let epoch = Int(exactly: epochNumber),
              epoch >= Self.initialEpoch,
              let rawMode = propertyList["mode"] as? String,
              let mode = ChangeFeedResetMode(rawValue: rawMode),
              let rawPhase = propertyList["phase"] as? String,
              let phase = Phase(rawValue: rawPhase) else {
            return nil
        }
        let backupRestoreEventIdentifier: String?
        if let rawEvent = propertyList["backupRestoreEventIdentifier"] {
            guard (mode == .backupRestore || mode == .encryptedDataReset),
                  let event = rawEvent as? String,
                  UUID(uuidString: event) != nil else { return nil }
            backupRestoreEventIdentifier = event
        } else {
            guard mode != .backupRestore else { return nil }
            backupRestoreEventIdentifier = nil
        }
        let replicaJournalHandoff: BigSyncReplicaJournalHandoff?
        if let rawHandoff = propertyList["replicaJournalHandoff"] {
            guard mode == .localDatasetRebootstrap || mode == .encryptedDataReset,
                  let fields = rawHandoff as? [String: Any],
                  let decoded = try? BigSyncReplicaJournalHandoff(
                    propertyList: fields
                  ) else { return nil }
            replicaJournalHandoff = decoded
        } else {
            replicaJournalHandoff = nil
        }
        self.init(
            key: key,
            accountScopeIdentifier: accountScopeIdentifier,
            zoneName: zoneName,
            zoneOwnerName: zoneOwnerName,
            epoch: epoch,
            mode: mode,
            phase: phase,
            backupRestoreEventIdentifier: backupRestoreEventIdentifier,
            replicaJournalHandoff: replicaJournalHandoff
        )
    }

    /// Select the durable recovery envelope before any cursor or target reset.
    /// Retrying the same operation preserves its epoch and provenance. A new
    /// backup event may supersede an old one; weaker history repair cannot erase
    /// retained-data work. An encrypted reset can also carry completion of the
    /// already-prepared restore that observed it.
    static func requesting(
        current: Self?,
        key: String,
        accountScopeIdentifier: String,
        zoneName: String,
        zoneOwnerName: String,
        mode: ChangeFeedResetMode,
        backupRestoreEventIdentifier: String? = nil,
        replicaJournalHandoff: BigSyncReplicaJournalHandoff? = nil
    ) throws -> Self {
        if let current {
            guard current.key == key,
                  current.accountScopeIdentifier == accountScopeIdentifier,
                  current.zoneName == zoneName,
                  current.zoneOwnerName == zoneOwnerName else {
                throw ChangeFeedMigrationPersistenceError.stateNotDurable
            }
        }
        guard replicaJournalHandoff == nil
                || mode == .localDatasetRebootstrap
                || mode == .encryptedDataReset else {
            throw BigSyncReplicaJournalHandoffError.invalidIdentity
        }
        guard mode == .backupRestore
                ? backupRestoreEventIdentifier.flatMap(UUID.init(uuidString:)) != nil
                : backupRestoreEventIdentifier == nil else {
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
        if let current, current.mode == .encryptedDataReset,
           mode == .backupRestore,
           current.backupRestoreEventIdentifier == backupRestoreEventIdentifier {
            // A retry of the same verified restore resumes its stronger recovery.
            // A genuinely new restore event is handled below as a new epoch.
            return current
        }
        if let current, current.phase != .completed,
           current.mode == .backupRestore, mode == .encryptedDataReset,
           current.phase == .requested {
            // The normal feed cannot observe an encrypted reset until adapter
            // preparation has retired the copied outbox. Do not skip that work
            // when an out-of-order internal request arrives earlier.
            throw ChangeFeedMigrationPersistenceError.restorePreparationRequired
        }
        // Token expiry cannot downgrade retained-data recovery or discard its
        // exact handoff. An encrypted reset retains local data too; carry the
        // handoff into that stronger mode instead of dropping its journal work.
        if let current, current.phase != .completed,
           current.mode == .localDatasetRebootstrap,
           mode == .serverReconciliation || mode == .initialImport {
            return current
        }
        let effectiveMode: ChangeFeedResetMode = {
            if let current, current.phase != .completed,
               current.mode == .encryptedDataReset,
               mode == .localDatasetRebootstrap {
                return .encryptedDataReset
            }
            return mode
        }()
        let requestedHandoff: BigSyncReplicaJournalHandoff?
        if let replicaJournalHandoff {
            if let current, current.phase != .completed,
               let previousHandoff = current.replicaJournalHandoff {
                requestedHandoff = try replicaJournalHandoff.retainingUnfinished(
                    previousHandoff
                )
            } else {
                requestedHandoff = replicaJournalHandoff
            }
        } else if let current, current.phase != .completed,
                  effectiveMode == .encryptedDataReset {
            requestedHandoff = current.replicaJournalHandoff
        } else {
            requestedHandoff = nil
        }

        // A migration already in progress owns valid provenance for this exact
        // epoch. Restarting its nil-token bootstrap is idempotent; incrementing
        // here would discard evidence captured before the interrupted fetch.
        if let current, current.phase != .completed, current.mode == effectiveMode {
            if (mode != .backupRestore
                || current.backupRestoreEventIdentifier == backupRestoreEventIdentifier),
               requestedHandoff == nil
                || requestedHandoff == current.replicaJournalHandoff {
                return current
            }
            // A fresh restore event supersedes an unfinished backup-recovery
            // envelope copied from an older installation. Allocate a new
            // epoch below so a copied envelope cannot consume the newer
            // installation's restore event.
        }

        // A verified restore event describes the provenance of every migration
        // envelope copied in that backup. Replace it with a fresh restore epoch.
        // A subsequently observed encrypted-reset error can still supersede it.
        if let current, current.phase != .completed,
           current.mode == .backupRestore,
           mode != .backupRestore && mode != .encryptedDataReset {
            return current
        }

        // An encrypted-data reset supersedes a conservative server
        // reconciliation already in flight. Its next epoch must rebuild all
        // live local records rather than interpret the empty server as remote
        // deletion. A conservative recovery never downgrades an encrypted
        // reset already in progress.
        if let current, current.phase != .completed,
           current.mode == .encryptedDataReset,
           effectiveMode != .encryptedDataReset,
           mode != .backupRestore {
            return current
        }

        let previousEpoch = current?.epoch
            ?? (ChangeFeedMigrationState.initialEpoch - 1)
        let (nextEpoch, overflow) = previousEpoch.addingReportingOverflow(1)
        guard !overflow else {
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
        let retainedRestoreEvent = backupRestoreEventIdentifier
            ?? (effectiveMode == .encryptedDataReset && current?.phase != .completed
                ? current?.backupRestoreEventIdentifier : nil)
        return ChangeFeedMigrationState(
            key: key,
            accountScopeIdentifier: accountScopeIdentifier,
            zoneName: zoneName,
            zoneOwnerName: zoneOwnerName,
            epoch: max(nextEpoch, ChangeFeedMigrationState.initialEpoch),
            mode: effectiveMode,
            phase: .requested,
            backupRestoreEventIdentifier: retainedRestoreEvent,
            replicaJournalHandoff: requestedHandoff
        )
    }

    var propertyList: [String: Any] {
        var value: [String: Any] = [
            "version": Self.version,
            "accountScopeIdentifier": accountScopeIdentifier,
            "zoneName": zoneName,
            "zoneOwnerName": zoneOwnerName,
            "epoch": epoch,
            "mode": mode.rawValue,
            "phase": phase.rawValue,
        ]
        if let backupRestoreEventIdentifier {
            value["backupRestoreEventIdentifier"] =
                backupRestoreEventIdentifier
        }
        if let replicaJournalHandoff {
            value["replicaJournalHandoff"] = replicaJournalHandoff.propertyList
        }
        return value
    }
}

internal enum ChangeFeedMigrationPersistenceError: Error, Equatable {
    case stateNotDurable
    case stateSuperseded
    case restorePreparationRequired
}

