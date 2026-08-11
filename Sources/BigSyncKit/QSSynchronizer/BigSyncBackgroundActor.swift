import CloudKit
import Realm
import RealmSwift
import Combine
import Logging

private enum BigSyncDeadlineOutcome: Sendable {
    case completed(CloudKitSynchronizer.SynchronizationResult?)
    case timedOut
}

private actor BigSyncDeadlineRace {
    private var outcome: BigSyncDeadlineOutcome?
    private var continuation:
        CheckedContinuation<BigSyncDeadlineOutcome, Never>?

    func resolve(_ outcome: BigSyncDeadlineOutcome) {
        guard self.outcome == nil else { return }
        self.outcome = outcome
        continuation?.resume(returning: outcome)
        continuation = nil
    }

    func value() async -> BigSyncDeadlineOutcome {
        if let outcome { return outcome }
        return await withCheckedContinuation { continuation in
            if let outcome {
                continuation.resume(returning: outcome)
            } else {
                self.continuation = continuation
            }
        }
    }
}

public struct BigSyncBackgroundWorkerConfiguration {
    let synchronizerName: String
    let containerName: String
    let configurations: [Realm.Configuration]
    let excludedClassNames: [String]
    let priorityClassNames: [String]
    let suiteName: String?
    let recordZoneID: CKRecordZone.ID?
    let logger: Logging.Logger
    let localState: BigSyncLocalStateConfiguration?
    let performsAccountAvailabilityPreflight: Bool
    let accountIdentifierProvider: CloudKitSynchronizer.AccountIdentifierProvider?
    let progressHandler: CloudKitSynchronizer.ProgressHandler?
    var changeFeedOverride: (any CloudKitChangeFeed)?
    var recordStoreOverride: (any CloudKitRecordStore)?
    /// Production synchronizers never delete CloudKit zones. Only an isolated
    /// per-run test client may opt into deleting its own disposable zone.
    private(set) var allowsDisposableZoneDeletion: Bool

    public init(
        synchronizerName: String,
        containerName: String,
        configurations: [Realm.Configuration],
        mutationPolicy: BigSyncMutationPolicy,
        priorityObjectTypes: [RealmSwift.Object.Type] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        localState: BigSyncLocalStateConfiguration? = nil,
        performsAccountAvailabilityPreflight: Bool = true,
        accountIdentifierProvider: CloudKitSynchronizer.AccountIdentifierProvider? = nil,
        progressHandler: CloudKitSynchronizer.ProgressHandler? = nil,
        logger: Logging.Logger
    ) {
        mutationPolicy.install(configurations: configurations)
        self.synchronizerName = synchronizerName
        self.containerName = containerName
        self.configurations = configurations
        self.excludedClassNames = mutationPolicy.excludedClassNames
        self.priorityClassNames = priorityObjectTypes.map { $0.className() }
        self.suiteName = suiteName
        self.recordZoneID = recordZoneID
        self.localState = localState
        self.performsAccountAvailabilityPreflight =
            performsAccountAvailabilityPreflight
        self.accountIdentifierProvider = accountIdentifierProvider
        self.progressHandler = progressHandler
        self.changeFeedOverride = nil
        self.recordStoreOverride = nil
        self.allowsDisposableZoneDeletion = false
        self.logger = logger
    }

#if DEBUG
    /// Installs an observable transport wrapper for the isolated E2E client.
    /// Production configurations always use the default CloudKit transport.
    @_spi(CloudKitE2E)
    public mutating func installCloudKitE2ETransport(
        changeFeed: any CloudKitChangeFeed,
        recordStore: any CloudKitRecordStore
    ) {
        changeFeedOverride = changeFeed
        recordStoreOverride = recordStore
    }

    /// Grants the destructive zone-deletion capability only to Manabi's
    /// isolated, per-run CloudKit E2E client. This is SPI rather than a public
    /// boolean so an ordinary debug synchronizer cannot opt an arbitrary app
    /// zone into deletion.
    @_spi(CloudKitE2E)
    @discardableResult
    public mutating func authorizeDisposableZoneDeletionForE2E(runID: UUID) -> Bool {
        let canonicalRunID = runID.uuidString.lowercased()
        let expectedZoneName = "ManabiPlatform.e2e.v2.\(canonicalRunID)"
        guard let recordZoneID,
              recordZoneID.ownerName == CKCurrentUserDefaultName,
              recordZoneID.zoneName == expectedZoneName,
              synchronizerName == "\(expectedZoneName).source"
                || synchronizerName == "\(expectedZoneName).restore"
                || synchronizerName == "\(expectedZoneName).cleanup",
              let localState else {
            return false
        }

        let trackingParent = localState.trackingRealmDirectoryURL
            .standardizedFileURL
            .deletingLastPathComponent()
        let assetParent = localState.assetDirectoryURL
            .standardizedFileURL
            .deletingLastPathComponent()
        let phase = trackingParent.lastPathComponent
        let runRoot = trackingParent.deletingLastPathComponent()
        guard trackingParent == assetParent,
              ["source", "restore", "cleanup"].contains(phase),
              runRoot.lastPathComponent == canonicalRunID,
              runRoot.deletingLastPathComponent().lastPathComponent == "CloudKitE2E" else {
            return false
        }
        allowsDisposableZoneDeletion = true
        return true
    }
#endif

}

@globalActor
public actor BigSyncBackgroundActor {
    public static let shared = BigSyncBackgroundActor()
    private static let initialSynchronizationDelayNanoseconds: UInt64 = 10_000_000_000

    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    private let accountAvailabilityGate: CloudKitAccountAvailabilityGate
    @BigSyncBackgroundActor
    private var initialSynchronizationTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var accountAvailabilityRetryTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var performsAccountAvailabilityPreflight = true

    @BigSyncBackgroundActor
    public private(set) var realmSynchronizer: CloudKitSynchronizer?
    @BigSyncBackgroundActor
    public private(set) var logger: Logging.Logger?

    public init() {
        accountAvailabilityGate = CloudKitAccountAvailabilityGate()
    }

    init(accountAvailabilityGate: CloudKitAccountAvailabilityGate) {
        self.accountAvailabilityGate = accountAvailabilityGate
    }

    @BigSyncBackgroundActor
    public func configure(_ configuration: BigSyncBackgroundWorkerConfiguration) {
        precondition(
            realmSynchronizer == nil,
            "BigSyncKit worker configuration is one-shot; await shutdown before introducing runtime replacement"
        )
        logger = configuration.logger

        let synchronizer = CloudKitSynchronizer.privateSynchronizer(
            synchronizerName: configuration.synchronizerName,
            containerName: configuration.containerName,
            configurations: configuration.configurations,
            excludedClassNames: configuration.excludedClassNames,
            priorityClassNames: configuration.priorityClassNames,
            suiteName: configuration.suiteName,
            recordZoneID: configuration.recordZoneID,
            localState: configuration.localState,
            accountIdentifierProvider: configuration.accountIdentifierProvider,
            progressHandler: configuration.progressHandler,
            changeFeed: configuration.changeFeedOverride,
            recordStore: configuration.recordStoreOverride,
            allowsDisposableZoneDeletion: configuration.allowsDisposableZoneDeletion,
            compatibilityVersion: Int(configuration.configurations.map { $0.schemaVersion } .reduce(0, +)),
            logger: configuration.logger
        )

        realmSynchronizer = synchronizer
        performsAccountAvailabilityPreflight =
            configuration.performsAccountAvailabilityPreflight

        (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.mergePolicy = .custom

        let compatibilityVersion = synchronizer.compatibilityVersion
        configuration.logger.info("QSCloudKitSynchronizer >> Local compatibility version: \(compatibilityVersion)")

        initialSynchronizationTask = Task(priority: .utility) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            do {
                try await Task.sleep(nanoseconds: Self.initialSynchronizationDelayNanoseconds)
            } catch {
                return
            }
            _ = await self.synchronizeCloudKit(expectedSynchronizer: synchronizer)
        }
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func synchronizeCloudKit()
        async -> CloudKitSynchronizer.SynchronizationResult? {
        // An explicit request supersedes the delayed startup request. Leaving
        // both alive performs a second full drain ten seconds after every
        // configuration, or queues it behind a long initial reupload.
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = nil

        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Synchronization requested before background synchronizer configuration completed")
            return nil
        }

        return await synchronizeCloudKit(expectedSynchronizer: realmSynchronizer)
    }

    /// Returns at the deadline even when an underlying CloudKit await does not
    /// cooperate with Swift task cancellation. The losing request task is
    /// canceled and fenced; an already-running shared synchronization may still
    /// finish for other waiters, but it cannot hold a lifecycle completion
    /// handler hostage.
    @BigSyncBackgroundActor
    @discardableResult
    public func synchronizeCloudKit(
        deadlineNanoseconds: UInt64
    ) async -> CloudKitSynchronizer.SynchronizationResult? {
        let race = BigSyncDeadlineRace()
        let synchronizationTask = Task { @BigSyncBackgroundActor [weak self] in
            let result = await self?.synchronizeCloudKit()
            await race.resolve(.completed(result))
        }
        let deadlineTask = Task.detached { [race, deadlineNanoseconds] in
            do {
                try await Task.sleep(nanoseconds: deadlineNanoseconds)
            } catch {
                return
            }
            await race.resolve(.timedOut)
        }

        let outcome = await withTaskCancellationHandler {
            await race.value()
        } onCancel: {
            synchronizationTask.cancel()
            Task { await race.resolve(.timedOut) }
        }
        synchronizationTask.cancel()
        deadlineTask.cancel()
        switch outcome {
        case .completed(let result):
            return result
        case .timedOut:
            logger?.warning(
                "QSCloudKitSynchronizer >> Lifecycle synchronization deadline elapsed"
            )
            return nil
        }
    }

    @BigSyncBackgroundActor
    private func synchronizeCloudKit(
        expectedSynchronizer: CloudKitSynchronizer
    ) async -> CloudKitSynchronizer.SynchronizationResult? {
        guard realmSynchronizer === expectedSynchronizer,
              let containerIdentifier = expectedSynchronizer.containerIdentifier else {
            return nil
        }

        if performsAccountAvailabilityPreflight {
            switch await accountAvailabilityGate.availability(
                for: containerIdentifier
            ) {
            case .available:
                accountAvailabilityRetryTask?.cancel()
                accountAvailabilityRetryTask = nil
                expectedSynchronizer.cancelledDueToUnauthentication = false
            case .unavailable(let status):
                logger?.info(
                    "QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status is \(status.rawValue)"
                )
                if status == .temporarilyUnavailable
                    || status == .couldNotDetermine {
                    scheduleAccountAvailabilityRetry(
                        expectedSynchronizer: expectedSynchronizer
                    )
                }
                return nil
            case .failed:
                logger?.warning("QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status failed")
                scheduleAccountAvailabilityRetry(
                    expectedSynchronizer: expectedSynchronizer
                )
                return nil
            }
        } else {
            // Callers may already have proved private-database access with a
            // successful CloudKit operation. Avoid repeating the callback-only
            // account-status preflight in that case; synchronize() still
            // fetches and fences the exact account identifier before any work.
            expectedSynchronizer.cancelledDueToUnauthentication = false
        }

        guard !Task.isCancelled,
              realmSynchronizer === expectedSynchronizer else { return nil }
        do {
            return try await expectedSynchronizer.synchronize()
        } catch is CancellationError {
            return nil
        } catch {
            logger?.error(
                "QSCloudKitSynchronizer >> Synchronization failed: \(error)"
            )
            return nil
        }
    }

    @BigSyncBackgroundActor
    private func scheduleAccountAvailabilityRetry(
        expectedSynchronizer: CloudKitSynchronizer
    ) {
        guard realmSynchronizer === expectedSynchronizer else { return }
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = Task(priority: .utility) {
            @BigSyncBackgroundActor [weak self, weak expectedSynchronizer] in
            guard let self, let expectedSynchronizer else { return }
            do {
                try await Task.sleep(nanoseconds: 30_000_000_000)
            } catch {
                return
            }
            guard realmSynchronizer === expectedSynchronizer else { return }
            accountAvailabilityRetryTask = nil
            _ = await synchronizeCloudKit(
                expectedSynchronizer: expectedSynchronizer
            )
        }
    }

    @BigSyncBackgroundActor
    public func cancelSynchronization() async {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = nil
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cancellation requested before background synchronizer configuration completed")
            return
        }

        await realmSynchronizer.cancelSynchronizationAndWait()
    }

    /// Restores the current account's redacted, durable CloudKit health state.
    /// A synchronizer that has not yet been configured, or a snapshot belonging
    /// to another iCloud account, intentionally yields `nil`.
    @BigSyncBackgroundActor
    public func cloudKitSyncHealthSnapshot()
        async -> CloudKitSyncHealthSnapshot? {
        guard let realmSynchronizer else { return nil }
        return try? await realmSynchronizer.syncHealthSnapshot()
    }

#if DEBUG
    @BigSyncBackgroundActor
    var _test_hasScheduledInitialSynchronization: Bool {
        initialSynchronizationTask != nil
    }

    @BigSyncBackgroundActor
    var _test_hasScheduledAccountAvailabilityRetry: Bool {
        accountAvailabilityRetryTask != nil
    }

    @BigSyncBackgroundActor
    func _test_installSynchronizer(
        _ synchronizer: CloudKitSynchronizer,
        performsAccountAvailabilityPreflight: Bool = true
    ) {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        realmSynchronizer = synchronizer
        self.performsAccountAvailabilityPreflight =
            performsAccountAvailabilityPreflight
    }

    @BigSyncBackgroundActor
    func _test_scheduleDormantInitialSynchronization() {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = Task {
            try? await Task.sleep(nanoseconds: 60_000_000_000)
        }
    }
#endif
}
