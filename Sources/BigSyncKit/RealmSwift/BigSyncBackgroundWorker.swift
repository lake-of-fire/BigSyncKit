import SwiftUI
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
    let suiteName: String?
    let recordZoneID: CKRecordZone.ID?
    let logger: Logging.Logger
    
    public init(
        synchronizerName: String,
        containerName: String,
        configurations: [Realm.Configuration],
        excludedClassNames: [String],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        logger: Logging.Logger
    ) {
        self.synchronizerName = synchronizerName
        self.containerName = containerName
        self.configurations = configurations
        self.excludedClassNames = excludedClassNames
        self.suiteName = suiteName
        self.recordZoneID = recordZoneID
        self.logger = logger
    }
}

@BigSyncBackgroundActor
public class BigSyncBackgroundWorker: BigSyncBackgroundWorkerBase {
    public var realmSynchronizer: CloudKitSynchronizer
    
//    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    
    let logger: Logging.Logger

    private var subscriptions = Set<AnyCancellable>()
//    private let notificationQueue = DispatchQueue(label: "BigSyncBackgroundWorker.notificationQueue")
    
#warning("need to manually refresh() in bg threads (after write block) for notifs to work here (?)")
    
    public init(
        configuration: BigSyncBackgroundWorkerConfiguration//,
//        delegate: RealmSwiftAdapterDelegate? = nil // If we start using this, ensure acceptRemoveChange / realmSwiftAdapter gotChanges delegate method gets implemented per RealmSwiftAdapter's conflict resolution!
    ) {
//        synchronizerDelegate = delegate
        
        let synchronizer = CloudKitSynchronizer.privateSynchronizer(
            synchronizerName: configuration.synchronizerName,
            containerName: configuration.containerName,
            configurations: configuration.configurations,
            excludedClassNames: configuration.excludedClassNames,
            suiteName: configuration.suiteName,
            recordZoneID: configuration.recordZoneID,
            compatibilityVersion: Int(configuration.configurations.map { $0.schemaVersion } .reduce(0, +)),
            logger: configuration.logger
        )
        
        logger = configuration.logger
        
        (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.mergePolicy = .custom
//        (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.delegate = self.synchronizerDelegate
        realmSynchronizer = synchronizer

        logger.info("QSCloudKitSynchronizer >> Local compatibility version: \(synchronizer.compatibilityVersion)")
        
        super.init()
        
        start { [weak self] in
            guard let self else { return }
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
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
                
                synchronizer.subscribeForChangesInDatabase { error in
                    if let error = error {
                        print("Change in DB error: \(error)")
                        return
                    }
                }
                
                await synchronizeCloudKit()
            }
        }
    }
    
    /// Call this on app start before accessing Realm to delete objects without invalidating them during use.
    public func cleanUp() {
        for adapter in realmSynchronizer.modelAdapters {
            adapter.cleanUp()
        }
    }
    
    @BigSyncBackgroundActor
    public func synchronizeCloudKit() async {
        await synchronizeCloudKit(using: realmSynchronizer)
    }
    
    @BigSyncBackgroundActor
    public func cancelSynchronization() {
        realmSynchronizer.cancelSynchronization()
    }
    
    @BigSyncBackgroundActor
    public func synchronizeCloudKit(using synchronizer: CloudKitSynchronizer) {
        synchronizer.beginSynchronization()
    }
}
