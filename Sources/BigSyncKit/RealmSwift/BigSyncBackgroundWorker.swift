import SwiftUI
import CloudKit
import Realm
import RealmSwift
import Combine

public struct BigSyncBackgroundWorkerConfiguration {
    let synchronizerName: String
    let containerName: String
    let configuration: Realm.Configuration
    let excludedClassNames: [String]
    let suiteName: String?
    let recordZoneID: CKRecordZone.ID?
    
    public init(synchronizerName: String, containerName: String, configuration: Realm.Configuration, excludedClassNames: [String], suiteName: String? = nil, recordZoneID: CKRecordZone.ID? = nil) {
        self.synchronizerName = synchronizerName
        self.containerName = containerName
        self.configuration = configuration
        self.excludedClassNames = excludedClassNames
        self.suiteName = suiteName
        self.recordZoneID = recordZoneID
    }
}

public class BigSyncBackgroundWorker: BigSyncBackgroundWorkerBase {
    public var realmSynchronizers: [CloudKitSynchronizer] = []
    
    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    
    private var subscriptions = Set<AnyCancellable>()
//    private let notificationQueue = DispatchQueue(label: "BigSyncBackgroundWorker.notificationQueue")
    
#warning("need to manually refresh() in bg threads (after write block) for notifs to work here (?)")
    
    public init(configurations: [BigSyncBackgroundWorkerConfiguration], delegate: RealmSwiftAdapterDelegate? = nil) {
        synchronizerDelegate = delegate
        
        super.init()
        
        for config in configurations {
            let synchronizer = CloudKitSynchronizer.privateSynchronizer(
                synchronizerName: config.synchronizerName,
                containerName: config.containerName,
                configuration: config.configuration,
                excludedClassNames: config.excludedClassNames,
                suiteName: config.suiteName,
                recordZoneID: config.recordZoneID
            )
            
            (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.mergePolicy = .custom
            (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.delegate = self.synchronizerDelegate
            synchronizer.compatibilityVersion = Int(config.configuration.schemaVersion)
            self.realmSynchronizers.append(synchronizer)
        }

        start { [weak self] in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                
                for synchronizer in realmSynchronizers {
                    if let containerIdentifier = synchronizer.containerIdentifier {
                        for modelAdapter in synchronizer.modelAdapters {
                            await CloudKitSynchronizer.transferOldServerChangeToken(to: modelAdapter, userDefaults: synchronizer.keyValueStore, containerName: containerIdentifier)
                        }
                    }
                    
                    NotificationCenter.default.publisher(for: .ModelAdapterHasChangesNotification)
                        .sink { [weak self] _ in
                            Task { @MainActor [weak self] in
                                await self?.synchronizeCloudKit()
                            }
                        }
                        .store(in: &self.subscriptions)
                    
                    synchronizer.subscribeForChangesInDatabase { error in
                        if let error = error {
                            print("Change in DB error: \(error)")
                            return
                        }
                    }
                }
            }
        }
    }
    
    /// Call this on app start before accessing Realm to delete objects without invalidating them during use.
    public func cleanUp() {
        for synchronizer in realmSynchronizers {
            for adapter in synchronizer.modelAdapters {
                adapter.cleanUp()
            }
        }
    }
    
    @MainActor
    public func synchronizeCloudKit() async {
        for synchronizer in realmSynchronizers {
            await synchronizeCloudKit(using: synchronizer)
        }
    }
    
    @MainActor
    public func cancelSynchronization() {
        for synchronizer in realmSynchronizers {
            synchronizer.cancelSynchronization()
        }
    }
    
    @MainActor
    public func synchronizeCloudKit(using synchronizer: CloudKitSynchronizer) async {
        guard !synchronizer.syncing else { return }
        
        await withCheckedContinuation { continuation in
            synchronizer.synchronize { error in
                if let error = error as? BigSyncKit.CloudKitSynchronizer.SyncError {
#warning("Tell user about this error")
                    switch error {
                        //                    case .callFailed:
                        //                        print("Sync error: \(error.localizedDescription) This error could be returned by completion block when no success and no error were produced.")
                    case .alreadySyncing:
                        // Received when synchronize is called while there was an ongoing synchronization.
                        break
                    case .cancelled:
                        print("Sync error: \(error.localizedDescription) Synchronization was manually cancelled.")
                    case .higherModelVersionFound:
                        print("Sync error: \(error.localizedDescription) A synchronizer with a higer `compatibilityVersion` value uploaded changes to CloudKit, so those changes won't be imported here. This error can be detected to prompt the user to update the app to a newer version.")
                    case .recordNotFound:
                        print("Sync error: \(error.localizedDescription) A record for the provided object was not found, so the object cannot be shared on CloudKit.")
                    }
                } else if let error = error as? NSError {
                    print("CloudKit sync error: \(error.localizedDescription) \(error)")
                }
                continuation.resume()
            }
        }
    }
}
