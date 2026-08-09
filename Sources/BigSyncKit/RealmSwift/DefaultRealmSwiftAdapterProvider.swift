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

public class DefaultRealmSwiftAdapterProvider: NSObject, AdapterProvider {
    let zoneID: CKRecordZone.ID
    let persistenceConfiguration: Realm.Configuration
    let targetConfigurations: [Realm.Configuration]
    let excludedClassNames: [String]
    let priorityClassNames: [String]
    let appGroup: String?
    let logger: Logging.Logger
    public private(set) var adapter: RealmSwiftAdapter!
   
    public var beforeInitialSetup: (() -> Void)? {
        didSet {
            adapter.beforeInitialSetup = beforeInitialSetup
        }
    }
    
    public init(
        targetConfigurations: [Realm.Configuration],
        excludedClassNames: [String],
        priorityClassNames: [String] = [],
        zoneID: CKRecordZone.ID,
        appGroup: String? = nil,
        persistenceNamespace: String? = nil,
        logger: Logging.Logger
    ) {
        self.targetConfigurations = targetConfigurations
        self.excludedClassNames = excludedClassNames
        self.priorityClassNames = priorityClassNames
        self.zoneID = zoneID
        self.appGroup = appGroup
        self.logger = logger
        persistenceConfiguration = DefaultRealmSwiftAdapterProvider.createPersistenceConfiguration(
            suiteName: appGroup,
            zoneID: zoneID,
            persistenceNamespace: persistenceNamespace
        )
        super.init()
        adapter = createAdapter()
    }

    init(adapter: RealmSwiftAdapter, logger: Logging.Logger) {
        zoneID = adapter.recordZoneID
        persistenceConfiguration = adapter.persistenceRealmConfiguration
        targetConfigurations = adapter.targetRealmConfigurations
        excludedClassNames = adapter.excludedClassNames
        priorityClassNames = adapter.priorityEntityTypeNames
        appGroup = nil
        self.logger = logger
        super.init()
        self.adapter = adapter
    }
    
    @BigSyncBackgroundActor
    public func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID recordZoneID: CKRecordZone.ID) async throws {
        if recordZoneID == zoneID {
            try await adapter.resetSyncCaches()
        }
    }
    
    fileprivate func createAdapter() -> RealmSwiftAdapter {
        return RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: targetConfigurations,
            excludedClassNames: excludedClassNames,
            priorityEntityTypeNames: priorityClassNames,
            recordZoneID: zoneID,
            logger: logger
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
        persistenceNamespace: String?
    ) -> Realm.Configuration {
        ensurePathAvailable(suiteName: suiteName)
        var configuration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        configuration.fileURL = URL(
            fileURLWithPath: realmPath(
                appGroup: suiteName,
                zoneID: zoneID,
                persistenceNamespace: persistenceNamespace
            )
        )
        return configuration
    }
    
    fileprivate static func ensurePathAvailable(suiteName: String?) {
        if !FileManager.default.fileExists(atPath: applicationBackupRealmPath(suiteName: suiteName)) {
            try? FileManager.default.createDirectory(atPath: applicationBackupRealmPath(suiteName: suiteName), withIntermediateDirectories: true, attributes: [:])
        }
    }
}
