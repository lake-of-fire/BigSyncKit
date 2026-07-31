import CloudKit
import Realm
import RealmSwift
import Combine
import Logging

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

    @available(
        *,
        deprecated,
        message: "Use init(... mutationPolicy:) so Realm journaling and worker exclusions share one policy"
    )
    public init(
        synchronizerName: String,
        containerName: String,
        configurations: [Realm.Configuration],
        excludedClassNames: [String],
        priorityObjectTypes: [RealmSwift.Object.Type] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        logger: Logging.Logger
    ) {
        self.init(
            synchronizerName: synchronizerName,
            containerName: containerName,
            configurations: configurations,
            mutationPolicy: BigSyncMutationPolicy(
                excludedClassNames: excludedClassNames
            ),
            priorityObjectTypes: priorityObjectTypes,
            suiteName: suiteName,
            recordZoneID: recordZoneID,
            logger: logger
        )
    }
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
        initialSynchronizationTask?.cancel()
        realmSynchronizer?.cancelSynchronization()
        synchronizationPreparationTask?.cancel()
        synchronizationPreparationTask = nil
        synchronizationPreparationState = .unprepared
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
            await self.synchronizeCloudKit(expectedSynchronizer: synchronizer)
        }
    }

    @BigSyncBackgroundActor
    @available(
        *,
        deprecated,
        message: "Cleanup is now an account-fenced terminal synchronization phase; call synchronizeCloudKit()"
    )
    public func cleanUp() async {
        _ = await synchronizeCloudKit()
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
                for modelAdapter in expectedSynchronizer.modelAdapters {
                    await CloudKitSynchronizer.transferOldServerChangeToken(
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
    
    public func synchronizeCloudKit(using configuration: BigSyncBackgroundWorkerConfiguration) async {
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
