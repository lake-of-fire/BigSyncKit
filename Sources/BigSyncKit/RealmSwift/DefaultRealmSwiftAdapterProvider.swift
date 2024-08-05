//
//  DefaultRealmSwiftAdapterProvider.swift
//  Pods
//
//  Created by Manuel Entrena on 18/11/2018.
//

import Foundation
import CloudKit
import RealmSwift

public class DefaultRealmSwiftAdapterProvider: NSObject, AdapterProvider {
    let zoneID: CKRecordZone.ID
    let persistenceConfiguration: Realm.Configuration
    let targetConfiguration: Realm.Configuration
    let excludedClassNames: [String]
    let appGroup: String?
    public private(set) var adapter: RealmSwiftAdapter!
   
    public var beforeInitialSetup: (() -> Void)? {
        didSet {
            adapter.beforeInitialSetup = beforeInitialSetup
        }
    }
    
    public init(targetConfiguration: Realm.Configuration, excludedClassNames: [String], zoneID: CKRecordZone.ID, appGroup: String? = nil) {
        self.targetConfiguration = targetConfiguration
        self.excludedClassNames = excludedClassNames
        self.zoneID = zoneID
        self.appGroup = appGroup
        persistenceConfiguration = DefaultRealmSwiftAdapterProvider.createPersistenceConfiguration(suiteName: appGroup)
        super.init()
        adapter = createAdapter()
    }
    
    @MainActor
    public func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, modelAdapterForRecordZoneID recordZoneID: CKRecordZone.ID) -> ModelAdapter? {
        guard recordZoneID == zoneID else { return nil }
        return adapter
    }
    
    @MainActor
    public func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID recordZoneID: CKRecordZone.ID) async {
        let adapterHasSyncedBefore = adapter.serverChangeToken != nil
        if recordZoneID == zoneID && adapterHasSyncedBefore {
            await adapter.deleteChangeTracking()
            synchronizer.removeModelAdapter(adapter)
            
            adapter = createAdapter()
            synchronizer.addModelAdapter(adapter)
        }
    }
    
    fileprivate func createAdapter() -> RealmSwiftAdapter {
        return RealmSwiftAdapter(persistenceRealmConfiguration: persistenceConfiguration, targetRealmConfiguration: targetConfiguration, excludedClassNames: excludedClassNames, recordZoneID: zoneID)
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
    
    public static func realmPath(appGroup: String?) -> String {
        return applicationBackupRealmPath(suiteName: appGroup).appending("/" + realmFileName())
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
    
    fileprivate static func realmFileName() -> String {
        return "BigSyncKitMetadata.realm"
    }
    
    fileprivate static func createPersistenceConfiguration(suiteName: String?) -> Realm.Configuration {
        ensurePathAvailable(suiteName: suiteName)
        var configuration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        configuration.fileURL = URL(fileURLWithPath: realmPath(appGroup: suiteName))
        return configuration
    }
    
    fileprivate static func ensurePathAvailable(suiteName: String?) {
        if !FileManager.default.fileExists(atPath: applicationBackupRealmPath(suiteName: suiteName)) {
            try? FileManager.default.createDirectory(atPath: applicationBackupRealmPath(suiteName: suiteName), withIntermediateDirectories: true, attributes: [:])
        }
    }
}
