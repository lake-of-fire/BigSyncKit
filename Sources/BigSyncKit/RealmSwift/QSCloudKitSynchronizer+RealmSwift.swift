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
    nonisolated internal static func productionLocalStateDirectoryURL(
        applicationSupportURL: URL,
        durableStateNamespace: String
    ) -> URL {
        applicationSupportURL
            .appendingPathComponent("BigSyncKit", isDirectory: true)
            .appendingPathComponent("LocalState", isDirectory: true)
            .appendingPathComponent(durableStateNamespace, isDirectory: true)
    }

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
        accountScopePropertyByClassName: [String: String] = [:],
        priorityClassNames: [String] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        durableStateRecordZoneID: CKRecordZone.ID? = nil,
        localState: BigSyncLocalStateConfiguration? = nil,
        accountIdentifierProvider: AccountIdentifierProvider? = nil,
        accountStatusProvider: AccountStatusProvider? = nil,
        progressHandler: ProgressHandler? = nil,
        changeFeed: (any CloudKitChangeFeed)? = nil,
        recordStore: (any CloudKitRecordStore)? = nil,
        allowsDisposableZoneDeletion: Bool = false,
        initialReplicaBindingAdmissionHandler:
            BigSyncBackgroundWorkerConfiguration
                .InitialReplicaBindingAdmissionHandler? = nil,
        accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy =
            .serverReconciliation,
        compatibilityVersion: Int = 0,
        logger: Logging.Logger
    ) -> CloudKitSynchronizer {
        let zoneID = recordZoneID ?? defaultCustomZoneID
        let durableStateZoneID = durableStateRecordZoneID ?? zoneID
        let durableStateNamespace = makeDurableStateNamespace(
            identifier: synchronizerName,
            containerIdentifier: containerName,
            databaseScope: .private,
            recordZoneID: durableStateZoneID
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
            let localStateDirectory = productionLocalStateDirectoryURL(
                applicationSupportURL: applicationSupportURL,
                durableStateNamespace: durableStateNamespace
            )
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
        // Retain this process's shared restore lease before an adapter can
        // open either target or tracking Realm state. Manabi also prepares
        // the same identity from its mutation-policy bootstrap, but the
        // library factory must be safe for clients that use it directly.
        let clientIdentity = BigSyncClientIdentity(
            synchronizerName: synchronizerName,
            containerName: containerName,
            recordZoneID: durableStateZoneID,
            databaseScope: .private,
            sharedStateBaseURL: backupDetectionBaseURL
                ?? BackupDetection.defaultSentinelURL(
                    namespace: durableStateNamespace
                ).deletingLastPathComponent()
        )
        let installationIdentifier: String
        do {
            installationIdentifier = try clientIdentity.prepareInstallation()
        } catch {
            preconditionFailure(
                "BigSyncKit installation identity failed before provider setup: \(error)"
            )
        }
        let mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)?
        if accountReplacementPolicy.usesDatasetReplicaBinding {
            let replicaBindingKey = durableStateNamespace
                + ".ReplicaBinding.v1"
            do {
                _ = try BigSyncReplicaBindingStateStore.prepare(
                    store: keyValueStore,
                    key: replicaBindingKey,
                    installationIdentifier: installationIdentifier
                )
            } catch {
                preconditionFailure(
                    "BigSyncKit replica binding failed before provider setup: \(error)"
                )
            }
            let mutationIdentityReader =
                BigSyncMutationJournalIdentityReader(
                    clientIdentity: clientIdentity,
                    store: keyValueStore,
                    key: replicaBindingKey
                )
            mutationJournalIdentityProvider = {
                mutationIdentityReader.current()
            }
        } else {
            mutationJournalIdentityProvider = nil
        }
        let provider = DefaultRealmSwiftAdapterProvider(
            targetConfigurations: configurations,
            excludedClassNames: excludedClassNames,
            accountScopePropertyByClassName:
                accountScopePropertyByClassName,
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
            durableStateRecordZoneID: durableStateRecordZoneID,
            keyValueStore: keyValueStore,
            compatibilityVersion: compatibilityVersion,
            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: accountStatusProvider,
            progressHandler: progressHandler,
            changeFeed: changeFeed ?? database,
            recordStore: recordStore ?? database,
            backupDetectionBaseURL: backupDetectionBaseURL,
            initialReplicaBindingAdmissionHandler:
                initialReplicaBindingAdmissionHandler,
            accountReplacementPolicy: accountReplacementPolicy,
            logger: logger
        )
        precondition(
            synchronizer.durableStateNamespace == durableStateNamespace,
            "BigSyncKit provider and synchronizer durable namespaces diverged"
        )
        // Passing the exact optional base preserves BackupDetection's
        // platform-specific default when the client has no shared app-group
        // state, while production app/extension clients share the explicit
        // app-group base computed above.
        precondition(
            clientIdentity.durableStateNamespace == durableStateNamespace,
            "BigSyncKit mutation and synchronizer durable namespaces diverged"
        )
        let mutationPolicy = BigSyncMutationPolicy(
            excludedClassNames: excludedClassNames,
            accountScopePropertyByClassName:
                accountScopePropertyByClassName
        )
        if let mutationJournalIdentityProvider {
            mutationPolicy.install(
                configurations: configurations,
                mutationJournalIdentityProvider:
                    mutationJournalIdentityProvider
            )
        } else {
            mutationPolicy.install(
                configurations: configurations,
                installationIdentifierProvider: {
                    clientIdentity.currentInstallationIdentifier()
                }
            )
        }
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
