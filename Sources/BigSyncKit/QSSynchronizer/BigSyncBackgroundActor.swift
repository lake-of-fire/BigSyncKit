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
    public static let shared = BigSyncBackgroundActor()
    
    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    
    @BigSyncBackgroundActor
    public private(set) var realmSynchronizer: CloudKitSynchronizer?
    @BigSyncBackgroundActor
    public private(set) var logger: Logging.Logger?
    
    public init() { }
    
    @BigSyncBackgroundActor
    public func configure(_ configuration: BigSyncBackgroundWorkerConfiguration) {
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
        
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            if let containerIdentifier = synchronizer.containerIdentifier {
                for modelAdapter in synchronizer.modelAdapters {
                    await CloudKitSynchronizer.transferOldServerChangeToken(
                        to: modelAdapter,
                        userDefaults: synchronizer.keyValueStore,
                        containerName: containerIdentifier
                    )
                }
            }
            
            await synchronizer.subscribeForChangesInDatabase { error in
                if let error = error {
                    print("Change in DB error: \(error)")
                    return
                }
            }
            
            await self.synchronizeCloudKit()
        }
    }

    @BigSyncBackgroundActor
    public func cleanUp() async {
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cleanup requested before background synchronizer configuration completed")
            return
        }

        realmSynchronizer.cancelSynchronization()

        for modelAdapter in realmSynchronizer.modelAdapters {
            modelAdapter.cleanUp()
        }
    }
    
    @BigSyncBackgroundActor
    public func synchronizeCloudKit() async {
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Synchronization requested before background synchronizer configuration completed")
            return
        }

        await realmSynchronizer.beginSynchronization()
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
