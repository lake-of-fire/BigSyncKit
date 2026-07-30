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
        excludedClassNames: [String],
        priorityObjectTypes: [RealmSwift.Object.Type] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        logger: Logging.Logger
    ) {
        self.synchronizerName = synchronizerName
        self.containerName = containerName
        self.configurations = configurations
        self.excludedClassNames = excludedClassNames
        self.priorityClassNames = priorityObjectTypes.map { $0.className() }
        self.suiteName = suiteName
        self.recordZoneID = recordZoneID
        self.logger = logger
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
    public func cleanUp() async {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        synchronizationPreparationState = .unprepared
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cleanup requested before background synchronizer configuration completed")
            return
        }

        realmSynchronizer.cancelSynchronization()

        for modelAdapter in realmSynchronizer.modelAdapters {
            do {
                try await modelAdapter.cleanUp()
            } catch {
                logger?.error("QSCloudKitSynchronizer >> Cleanup failed: \(error)")
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func synchronizeCloudKit() async {
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Synchronization requested before background synchronizer configuration completed")
            return
        }

        await synchronizeCloudKit(expectedSynchronizer: realmSynchronizer)
    }

    @BigSyncBackgroundActor
    private func synchronizeCloudKit(expectedSynchronizer: CloudKitSynchronizer) async {
        guard realmSynchronizer === expectedSynchronizer,
              let containerIdentifier = expectedSynchronizer.containerIdentifier else {
            return
        }

        switch await accountAvailabilityGate.availability(for: containerIdentifier) {
        case .available:
            expectedSynchronizer.cancelledDueToUnauthentication = false
        case .unavailable(let status):
            logger?.info(
                "QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status is \(status.rawValue)"
            )
            return
        case .failed:
            logger?.warning("QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status failed")
            return
        }

        guard !Task.isCancelled, realmSynchronizer === expectedSynchronizer else { return }
        switch synchronizationPreparationState {
        case .prepared:
            break
        case .preparing:
            return
        case .unprepared:
            synchronizationPreparationState = .preparing
            for modelAdapter in expectedSynchronizer.modelAdapters {
                await CloudKitSynchronizer.transferOldServerChangeToken(
                    to: modelAdapter,
                    userDefaults: expectedSynchronizer.keyValueStore,
                    containerName: containerIdentifier
                )
            }

            guard !Task.isCancelled, realmSynchronizer === expectedSynchronizer else {
                synchronizationPreparationState = .unprepared
                return
            }
            expectedSynchronizer.subscribeForChangesInDatabase { [logger] error in
                if let error {
                    logger?.error("QSCloudKitSynchronizer >> Database subscription failed: \(error)")
                }
            }
            guard !Task.isCancelled, realmSynchronizer === expectedSynchronizer else {
                synchronizationPreparationState = .unprepared
                return
            }
            synchronizationPreparationState = .prepared
        }

        expectedSynchronizer.beginSynchronization()
    }
    
    @BigSyncBackgroundActor
    public func cancelSynchronization() async {
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cancellation requested before background synchronizer configuration completed")
            return
        }

        realmSynchronizer.cancelSynchronization()
    }
    
    public func synchronizeCloudKit(using configuration: BigSyncBackgroundWorkerConfiguration) async {
    }
}
