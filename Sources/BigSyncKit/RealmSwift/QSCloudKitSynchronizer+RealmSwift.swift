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
        accountStatusProvider: AccountStatusProvider? = nil,
        progressHandler: ProgressHandler? = nil,
        changeFeed: (any CloudKitChangeFeed)? = nil,
        recordStore: (any CloudKitRecordStore)? = nil,
        allowsDisposableZoneDeletion: Bool = false,
        compatibilityVersion: Int = 0,
        logger: Logging.Logger
    ) -> CloudKitSynchronizer {
        let zoneID = recordZoneID ?? defaultCustomZoneID
        let durableStateNamespace = makeDurableStateNamespace(
            identifier: synchronizerName,
            containerIdentifier: containerName,
            databaseScope: .private,
            recordZoneID: zoneID
        )
        let keyValueStore: any KeyValueStore
        let backupDetectionBaseURL: URL?
        if let localState {
            keyValueStore = localState.keyValueStore
            backupDetectionBaseURL = localState.trackingRealmDirectoryURL
                .deletingLastPathComponent()
                .appendingPathComponent("BigSyncKitBackupDetection", isDirectory: true)
        } else {
            // Do not migrate the former UserDefaults state. Its writes are
            // not a durable cross-process boundary and borrowing it would
            // revive tokens from a different client identity. The target
            // Realm remains intact; normal reconciliation repopulates this
            // fresh namespace on the first production launch.
            let applicationSupportURL: URL
            if let suiteName {
                guard let groupContainerURL = FileManager.default
                    .containerURL(forSecurityApplicationGroupIdentifier: suiteName) else {
                    preconditionFailure(
                        "BigSyncKit cannot resolve the configured App Group \(suiteName); refusing to create process-local sync state"
                    )
                }
                applicationSupportURL = groupContainerURL
                    .appendingPathComponent("Library", isDirectory: true)
                    .appendingPathComponent("Application Support", isDirectory: true)
            } else {
                applicationSupportURL = FileManager.default.urls(
                    for: .applicationSupportDirectory,
                    in: .userDomainMask
                )[0]
            }
            let localStateDirectory = applicationSupportURL
                .appendingPathComponent("BigSyncKit", isDirectory: true)
                .appendingPathComponent("LocalState", isDirectory: true)
                .appendingPathComponent(durableStateNamespace, isDirectory: true)
            let fileStore = FileKeyValueStore(
                fileURL: localStateDirectory.appendingPathComponent("state.plist")
            )
            keyValueStore = fileStore
            backupDetectionBaseURL = applicationSupportURL
                .appendingPathComponent("BigSyncKit", isDirectory: true)
        }
        if let durableStore = keyValueStore as? any DurableKeyValueStore {
            do {
                // Construction itself is a durability boundary. Prove the
                // complete current snapshot can be read and atomically
                // rewritten before Realm setup or CloudKit transport objects
                // exist. This also covers explicit integration-test stores.
                try durableStore.prepareForUse()
            } catch {
                preconditionFailure(
                    "BigSyncKit durable local state failed validation before provider setup: \(error)"
                )
            }
        } else {
            precondition(
                keyValueStore.synchronize?() == true,
                "BigSyncKit requires a durable local-state store before provider setup"
            )
        }
        let provider = DefaultRealmSwiftAdapterProvider(
            targetConfigurations: configurations,
            excludedClassNames: excludedClassNames,
            priorityClassNames: priorityClassNames,
            zoneID: zoneID,
            appGroup: suiteName,
            persistenceNamespace: durableStateNamespace,
            persistenceDirectoryURL: localState?.trackingRealmDirectoryURL,
            assetDirectoryURL: localState?.assetDirectoryURL,
            // The synchronizer must establish backup/account recovery mode
            // before Realm setup is allowed to perform broad initial discovery.
            startSetupTask: false,
            logger: logger
        )
        let container = CKContainer(identifier: containerName)
        let database = DefaultCloudKitDatabaseAdapter(
            database: container.privateCloudDatabase
        )
        let synchronizer = CloudKitSynchronizer(
            identifier: synchronizerName,
            containerIdentifier: containerName,
            database: database,
            recordZoneID: zoneID,
            keyValueStore: keyValueStore,
            compatibilityVersion: compatibilityVersion,
            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: accountStatusProvider,
            progressHandler: progressHandler,
            changeFeed: changeFeed ?? database,
            recordStore: recordStore ?? database,
            backupDetectionBaseURL: backupDetectionBaseURL,
            logger: logger
        )
        precondition(
            synchronizer.durableStateNamespace == durableStateNamespace,
            "BigSyncKit provider and synchronizer durable namespaces diverged"
        )
        BigSyncMutationPolicy(
            excludedClassNames: excludedClassNames
        ).install(
            configurations: configurations,
            installationIdentifierProvider: {
                BackupDetection.installationIdentifier(
                    namespace: durableStateNamespace,
                    sharedSentinelBaseURL: backupDetectionBaseURL
                )
            }
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
