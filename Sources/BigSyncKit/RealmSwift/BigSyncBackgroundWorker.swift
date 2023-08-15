import SwiftUI
import Realm
import RealmSwift
import Combine
//import RealmSwiftGaps

public class BigSyncBackgroundWorker: BackgroundWorker {
    public var realmSynchronizer: CloudKitSynchronizer?
    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    
    private var subscriptions = Set<AnyCancellable>()
    private let notificationQueue = DispatchQueue(label: "BigSyncBackgroundWorker.notificationQueue")

    public init(containerName: String, configuration: Realm.Configuration, delegate: RealmSwiftAdapterDelegate) {
        synchronizerDelegate = delegate

        super.init()
        
        #warning("need to manually refresh() in bg threads (after write block) for notifs to work here")
        start { [weak self] in
            autoreleasepool {
                guard let self = self else { return }
                self.realmSynchronizer = CloudKitSynchronizer.privateSynchronizer(containerName: containerName, configuration: configuration)
        
                (self.realmSynchronizer?.modelAdapters.first as? RealmSwiftAdapter)?.mergePolicy = .custom
                (self.realmSynchronizer?.modelAdapters.first as? RealmSwiftAdapter)?.delegate = self.synchronizerDelegate
                self.realmSynchronizer?.compatibilityVersion = Int(configuration.schemaVersion)
                
                NotificationCenter.default.publisher(for: .ModelAdapterHasChangesNotification)
                    .sink { [weak self] _ in
                        self?.synchronizeCloudKit()
                    }
                    .store(in: &self.subscriptions)
                
                self.realmSynchronizer?.subscribeForChangesInDatabase { error in
                    if let error = error {
    #warning("Tell user about this error")
                        print("Change in DB error: \(error)")
                        return
                    }
                }
            }
        }
    }
    
    deinit {
    }
    
    public func synchronizeCloudKit() {
        guard !(realmSynchronizer?.syncing ?? true) else { return }
        realmSynchronizer?.synchronize { error in
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
                return
            } else if let error = error {
                print("Unknown CloudKit sync error: \(error.localizedDescription) \(error)")
            }
        }
    }
}
