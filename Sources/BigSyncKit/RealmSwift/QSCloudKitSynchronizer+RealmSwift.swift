//
//  QSCloudKitSynchronizer+RealmSwift.swift
//  Pods
//
//  Created by Manuel Entrena on 01/09/2017.
//
//

import Foundation
import RealmSwift
import CloudKit
import Logging

extension CloudKitSynchronizer {
    /**
     *  Creates a new `QSCloudKitSynchronizer` prepared to work with a Realm model and the SyncKit default record zone in the private database.
     - Parameters:
     - containerName: Identifier of the iCloud container to be used. The application must have the right entitlements to be able to access this container.
     - configuration: Configuration of the Realm that is to be tracked and synchronized.
     - suiteName: Identifier of shared App Group for the app. This will store the tracking database in the shared container.
     
     -Returns: A new CloudKit synchronizer for the given realm.
     */
    class func privateSynchronizer(
        synchronizerName: String = "DefaultRealmSwiftPrivateSynchronizer",
        containerName: String,
        configurations: [Realm.Configuration],
        excludedClassNames: [String],
        priorityClassNames: [String] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        localState: BigSyncLocalStateConfiguration? = nil,
        accountIdentifierProvider: AccountIdentifierProvider? = nil,
        progressHandler: ProgressHandler? = nil,
        changeFeed: (any CloudKitChangeFeed)? = nil,
        recordStore: (any CloudKitRecordStore)? = nil,
        allowsDisposableZoneDeletion: Bool = false,
        compatibilityVersion: Int = 0,
        logger: Logging.Logger
    ) -> CloudKitSynchronizer {
        let zoneID = recordZoneID ?? defaultCustomZoneID
        let provider = DefaultRealmSwiftAdapterProvider(
            targetConfigurations: configurations,
            excludedClassNames: excludedClassNames,
            priorityClassNames: priorityClassNames,
            zoneID: zoneID,
            appGroup: suiteName,
            persistenceNamespace:
                "\(containerName)|\(synchronizerName)|private",
            persistenceDirectoryURL: localState?.trackingRealmDirectoryURL,
            assetDirectoryURL: localState?.assetDirectoryURL,
            logger: logger
        )
        let keyValueStore: any KeyValueStore
        if let localState {
            keyValueStore = localState.keyValueStore
        } else {
            let userDefaults = UserDefaults(suiteName: suiteName)!
            keyValueStore = UserDefaultsAdapter(userDefaults: userDefaults)
        }
        let container = CKContainer(identifier: containerName)
        let database = DefaultCloudKitDatabaseAdapter(
            database: container.privateCloudDatabase
        )
        let synchronizer = CloudKitSynchronizer(
            identifier: synchronizerName,
            containerIdentifier: containerName,
            database: database,
            adapterProvider: provider,
            keyValueStore: keyValueStore,
            compatibilityVersion: compatibilityVersion,
            accountIdentifierProvider: accountIdentifierProvider,
            progressHandler: progressHandler,
            changeFeed: changeFeed ?? database,
            recordStore: recordStore ?? database,
            logger: logger
        )
#if DEBUG
        if allowsDisposableZoneDeletion, localState != nil {
            synchronizer._enableDisposableZoneDeletionForTesting()
        }
#endif
        provider.beforeInitialSetup = {
            synchronizer.clearDeviceIdentifier()
        }
        synchronizer.addModelAdapter(provider.adapter)
        
        return synchronizer
    }
    
//    /**
//     *  Creates a new `QSCloudKitSynchronizer` prepared to work with a Realm model and the shared database.
//     - Parameters:
//     - containerName: Identifier of the iCloud container to be used. The application must have the right entitlements to be able to access this container.
//     - configuration: Configuration of the Realm that is to be tracked and synchronized.
//     - suiteName: Identifier of shared App Group for the app. This will store the tracking database in the shared container.
//     
//     -Returns: A new CloudKit synchronizer for the given realm.
//     */
//    public class func sharedSynchronizer(containerName: String, configuration: Realm.Configuration, excludedClassNames: [String], suiteName: String? = nil) -> CloudKitSynchronizer {
//        let userDefaults = UserDefaults(suiteName: suiteName)!
//        let userDefaultsAdapter = UserDefaultsAdapter(userDefaults: userDefaults)
//        let container = CKContainer(identifier: containerName)
//        let provider = DefaultRealmProvider(
//            identifier: "DefaultRealmSwiftSharedStackProvider",
//            realmConfiguration: configuration,
//            appGroup: suiteName,
//            excludedClassNames: excludedClassNames
//        )
//        let synchronizer = CloudKitSynchronizer(identifier: "DefaultRealmSwiftSharedSynchronizer",
//                                                containerIdentifier: containerName,
//                                                database: DefaultCloudKitDatabaseAdapter(database: container.sharedCloudDatabase),
//                                                adapterProvider: provider,
//                                                keyValueStore: userDefaultsAdapter)
//        for adapter in provider.adapterDictionary.values {
//            synchronizer.addModelAdapter(adapter)
//        }
//        
//        return synchronizer
//    }
    
}
