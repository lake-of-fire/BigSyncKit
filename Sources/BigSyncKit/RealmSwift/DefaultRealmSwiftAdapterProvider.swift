//
//  DefaultRealmSwiftAdapterProvider.swift
//  Pods
//
//  Created by Manuel Entrena on 18/11/2018.
//

import Foundation
import CloudKit
import CryptoKit
import RealmSwift
import Logging

public class DefaultRealmSwiftAdapterProvider: NSObject {
    let zoneID: CKRecordZone.ID
    let persistenceConfiguration: Realm.Configuration
    let targetConfigurations: [Realm.Configuration]
    let excludedClassNames: [String]
    let accountScopePropertyByClassName: [String: String]
    let priorityClassNames: [String]
    let appGroup: String?
    let assetDirectoryURL: URL?
    let logger: Logging.Logger
    let startsSetupTask: Bool
    public private(set) var adapter: RealmSwiftAdapter!
   
    public var beforeInitialSetup: (() -> Void)? {
        didSet {
            adapter.beforeInitialSetup = beforeInitialSetup
        }
    }
    
    public init(
        targetConfigurations: [Realm.Configuration],
        excludedClassNames: [String],
        accountScopePropertyByClassName: [String: String] = [:],
        priorityClassNames: [String] = [],
        zoneID: CKRecordZone.ID,
        appGroup: String? = nil,
        persistenceNamespace: String? = nil,
        persistenceDirectoryURL: URL? = nil,
        assetDirectoryURL: URL? = nil,
        startSetupTask: Bool = true,
        logger: Logging.Logger
    ) {
        self.targetConfigurations = targetConfigurations
        self.excludedClassNames = excludedClassNames
        self.accountScopePropertyByClassName =
            accountScopePropertyByClassName
        self.priorityClassNames = priorityClassNames
        self.zoneID = zoneID
        self.appGroup = appGroup
        self.assetDirectoryURL = assetDirectoryURL
        startsSetupTask = startSetupTask
        self.logger = logger
        persistenceConfiguration = DefaultRealmSwiftAdapterProvider.createPersistenceConfiguration(
            suiteName: appGroup,
            zoneID: zoneID,
            persistenceNamespace: persistenceNamespace,
            persistenceDirectoryURL: persistenceDirectoryURL
        )
        super.init()
        adapter = createAdapter()
    }

    init(adapter: RealmSwiftAdapter, logger: Logging.Logger) {
        zoneID = adapter.recordZoneID
        persistenceConfiguration = adapter.persistenceRealmConfiguration
        targetConfigurations = adapter.targetRealmConfigurations
        excludedClassNames = adapter.excludedClassNames
        accountScopePropertyByClassName =
            adapter.accountScopePropertyByClassName
        priorityClassNames = adapter.priorityEntityTypeNames
        appGroup = nil
        assetDirectoryURL = nil
        startsSetupTask = false
        self.logger = logger
        super.init()
        self.adapter = adapter
    }
    
    fileprivate func createAdapter() -> RealmSwiftAdapter {
        return RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: targetConfigurations,
            excludedClassNames: excludedClassNames,
            accountScopePropertyByClassName:
                accountScopePropertyByClassName,
            priorityEntityTypeNames: priorityClassNames,
            recordZoneID: zoneID,
            logger: logger,
            startSetupTask: startsSetupTask,
            assetDirectoryURL: assetDirectoryURL
        )
    }
    
    // MARK: - File directory
    
    /**
     *  If using app groups, SyncKit offers the option to store its tracking database in the shared container so that it's
     *  accessible by SyncKit from any of the apps in the group. This method returns the path used in this case.
     *
     *  @param  appGroup   Identifier of an App Group this app belongs to.
     *
     *  @return File path, in the shared container, where SyncKit will store its tracking database.
     */
    
    public static func realmPath(
        appGroup: String?,
        zoneID: CKRecordZone.ID,
        persistenceNamespace: String? = nil
    ) -> String {
        return applicationBackupRealmPath(suiteName: appGroup).appending(
            "/" + realmFileName(
                zoneID: zoneID,
                persistenceNamespace: persistenceNamespace
            )
        )
    }
    
    fileprivate static func applicationBackupRealmPath(suiteName: String?) -> String! {
        let rootDirectory: String?
        if let suiteName = suiteName {
            rootDirectory = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: suiteName)?.path
        } else {
            rootDirectory = applicationDocumentsDirectory()
        }
        return rootDirectory?.appending("/BigSyncKit")
    }
    
    fileprivate static func applicationDocumentsDirectory() -> String? {
#if os(iOS)
        return NSSearchPathForDirectoriesInDomains(.libraryDirectory, .userDomainMask, true).last
#elseif os(macOS)
        let urls = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)
        //        return urls.last?.appendingPathComponent("com.lake-of-fire.BigSyncKit").path
        return urls.last?.path
#endif
    }
    
    fileprivate static func realmFileName(
        zoneID: CKRecordZone.ID,
        persistenceNamespace: String?
    ) -> String {
        guard let persistenceNamespace else {
            return zoneID.zoneName + ".realm"
        }
        let digest = SHA256.hash(
            data: Data(persistenceNamespace.utf8)
        ).map { String(format: "%02x", $0) }.joined()
        return "\(digest)-\(zoneID.zoneName).realm"
    }
    
    fileprivate static func createPersistenceConfiguration(
        suiteName: String?,
        zoneID: CKRecordZone.ID,
        persistenceNamespace: String?,
        persistenceDirectoryURL: URL?
    ) -> Realm.Configuration {
        if let persistenceDirectoryURL {
            try? FileManager.default.createDirectory(
                at: persistenceDirectoryURL,
                withIntermediateDirectories: true
            )
        } else {
            ensurePathAvailable(suiteName: suiteName)
        }
        var configuration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        let fileName = realmFileName(
            zoneID: zoneID,
            persistenceNamespace: persistenceNamespace
        )
        if let persistenceDirectoryURL {
            configuration.fileURL = persistenceDirectoryURL
                .appendingPathComponent(fileName, isDirectory: false)
        } else {
            configuration.fileURL = URL(
                fileURLWithPath: realmPath(
                    appGroup: suiteName,
                    zoneID: zoneID,
                    persistenceNamespace: persistenceNamespace
                )
            )
        }
        return configuration
    }
    
    fileprivate static func ensurePathAvailable(suiteName: String?) {
        if !FileManager.default.fileExists(atPath: applicationBackupRealmPath(suiteName: suiteName)) {
            try? FileManager.default.createDirectory(atPath: applicationBackupRealmPath(suiteName: suiteName), withIntermediateDirectories: true, attributes: [:])
        }
    }
}
