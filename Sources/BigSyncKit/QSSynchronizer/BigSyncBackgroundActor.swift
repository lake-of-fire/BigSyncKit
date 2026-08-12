import CloudKit
import Realm
import RealmSwift
import Combine
import Logging

enum BigSyncDeadlineOutcome: Sendable {
    case completed(CloudKitSynchronizer.SynchronizationResult?)
    case timedOut
}

actor BigSyncDeadlineRace {
    private var outcome: BigSyncDeadlineOutcome?
    private var continuation: CheckedContinuation<BigSyncDeadlineOutcome, Never>?

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
    
    public init(
        synchronizerName: String,
        containerName: String,
        configurations: [Realm.Configuration],
        mutationPolicy: BigSyncMutationPolicy,
        priorityObjectTypes: [RealmSwift.Object.Type] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
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
        self.logger = logger
    }

}

public enum BigSyncManualRebuildOutcome: Sendable, Equatable {
    /// The current synchronizer reset its local caches and finished a new drain.
    case succeeded
    /// The worker was not configured, iCloud was unavailable, or configuration
    /// superseded the request before it could safely drain.
    case deferred
    /// Resetting the current synchronizer's local caches failed.
    case failed(String)
}

@globalActor
public actor BigSyncBackgroundActor {
    private enum SynchronizationPreparationState {
        case unprepared
        case preparing
        case prepared
    }

    public static let shared = BigSyncBackgroundActor()
    private static let initialSynchronizationDelayNanoseconds: UInt64 = 10_000_000_000
    
    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    private let accountAvailabilityGate: CloudKitAccountAvailabilityGate
    @BigSyncBackgroundActor
    private var initialSynchronizationTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var synchronizationPreparationState = SynchronizationPreparationState.unprepared
    @BigSyncBackgroundActor
    private var synchronizationPreparationTask: Task<Void, Error>?
    @BigSyncBackgroundActor
    private var manualRebuildTask: Task<BigSyncManualRebuildOutcome, Never>?
    @BigSyncBackgroundActor
    private var manualRebuildID: UUID?
    
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
            compatibilityVersion: Int(configuration.configurations.map { $0.schemaVersion } .reduce(0, +)),
            logger: configuration.logger
        )
        
        realmSynchronizer = synchronizer
        
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

        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Synchronization requested before background synchronizer configuration completed")
            return nil
        }

        return await synchronizeCloudKit(expectedSynchronizer: realmSynchronizer)
    }

    /// Bounds lifecycle callers even when an underlying CloudKit await does not
    /// cooperate with Swift task cancellation.
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

        switch await accountAvailabilityGate.availability(for: containerIdentifier) {
        case .available:
            expectedSynchronizer.cancelledDueToUnauthentication = false
        case .unavailable(let status):
            logger?.info(
                "QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status is \(status.rawValue)"
            )
            return nil
        case .failed:
            logger?.warning("QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status failed")
            return nil
        }

        guard !Task.isCancelled,
              realmSynchronizer === expectedSynchronizer else { return nil }
        switch synchronizationPreparationState {
        case .prepared:
            break
        case .preparing:
            do {
                try await synchronizationPreparationTask?.value
            } catch {
                logger?.error(
                    "QSCloudKitSynchronizer >> Synchronization preparation failed: \(error)"
                )
                return nil
            }
        case .unprepared:
            synchronizationPreparationState = .preparing
            let preparationTask = Task {
                // A valid legacy token remains retryable until the adapter
                // durably accepts it.
                for modelAdapter in expectedSynchronizer.modelAdapters {
                    try await CloudKitSynchronizer.transferOldServerChangeToken(
                        to: modelAdapter,
                        userDefaults: expectedSynchronizer.keyValueStore,
                        containerName: containerIdentifier
                    )
                }
                try Task.checkCancellation()
                guard realmSynchronizer === expectedSynchronizer else {
                    throw CancellationError()
                }
            }
            synchronizationPreparationTask = preparationTask
            do {
                try await preparationTask.value
            } catch {
                synchronizationPreparationTask = nil
                synchronizationPreparationState = .unprepared
                logger?.error(
                    "QSCloudKitSynchronizer >> Synchronization preparation failed: \(error)"
                )
                return nil
            }
            synchronizationPreparationTask = nil
            guard !Task.isCancelled,
                  realmSynchronizer === expectedSynchronizer else {
                synchronizationPreparationState = .unprepared
                return nil
            }
            synchronizationPreparationState = .prepared
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
    public func cancelSynchronization() async {
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cancellation requested before background synchronizer configuration completed")
            return
        }

        await realmSynchronizer.cancelSynchronizationAndWait()
    }
    
    /// Non-destructively rebuilds the synchronization caches and drains the
    /// exact synchronizer that was current when the request began. Concurrent
    /// requests coalesce; a later configuration cancels the in-flight rebuild.
    @BigSyncBackgroundActor
    public func rebuildAndReuploadCloudKitData() async -> BigSyncManualRebuildOutcome {
        if let manualRebuildTask {
            return await manualRebuildTask.value
        }
        guard let expectedSynchronizer = realmSynchronizer else {
            logger?.warning(
                "QSCloudKitSynchronizer >> Manual rebuild requested before background synchronizer configuration completed"
            )
            return .deferred
        }

        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        let rebuildID = UUID()
        let task: Task<BigSyncManualRebuildOutcome, Never> = Task {
            @BigSyncBackgroundActor [weak self] in
            guard let self else { return BigSyncManualRebuildOutcome.deferred }
            return await self.performManualRebuild(
                expectedSynchronizer: expectedSynchronizer,
                rebuildID: rebuildID
            )
        }
        manualRebuildID = rebuildID
        manualRebuildTask = task

        let outcome = await task.value
        if manualRebuildID == rebuildID {
            manualRebuildTask = nil
            manualRebuildID = nil
        }
        return outcome
    }

    @BigSyncBackgroundActor
    private func performManualRebuild(
        expectedSynchronizer: CloudKitSynchronizer,
        rebuildID: UUID
    ) async -> BigSyncManualRebuildOutcome {
        guard !Task.isCancelled,
              manualRebuildID == rebuildID,
              realmSynchronizer === expectedSynchronizer else {
            return .deferred
        }
        do {
            try await expectedSynchronizer.resetSyncCaches(
                cancelSynchronization: true
            )
        } catch is CancellationError {
            return .deferred
        } catch {
            logger?.error("QSCloudKitSynchronizer >> Manual rebuild failed: \(error)")
            return .failed(error.localizedDescription)
        }

        guard !Task.isCancelled,
              manualRebuildID == rebuildID,
              realmSynchronizer === expectedSynchronizer else {
            return .deferred
        }
        return await synchronizeCloudKit(expectedSynchronizer: expectedSynchronizer) == nil
            ? .deferred
            : .succeeded
    }

#if DEBUG
    @BigSyncBackgroundActor
    var _test_hasScheduledInitialSynchronization: Bool {
        initialSynchronizationTask != nil
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
