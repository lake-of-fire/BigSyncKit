//
//  CloudKitSynchronizer+Private.swift
//  OCMock
//
//  Created by Manuel Entrena on 05/04/2019.
//

import Foundation
import CloudKit

private let customZoneName = "BigSyncKit"
private let storedDeviceUUIDKey = "QSCloudKitStoredDeviceUUIDKey"
private let subscriptionIdentifierKey = "QSSubscriptionIdentifierKey"
private let databaseServerChangeTokenKey = "QSDatabaseServerChangeTokenKey"

extension CloudKitSynchronizer {
    
    static var defaultCustomZoneID: CKRecordZone.ID {
        return CKRecordZone.ID(zoneName: customZoneName, ownerName: CKCurrentUserDefaultName)
    }
    
//    @BigSyncBackgroundActor
    var deviceUUID: String? {
        get {
            return keyValueStore.object(forKey: userDefaultsKey(for: storedDeviceUUIDKey)) as? String
        }
        set {
            let key = userDefaultsKey(for: storedDeviceUUIDKey)
            if let value = newValue {
                keyValueStore.set(value: value, forKey: key)
            } else {
                keyValueStore.removeObject(forKey: key)
            }
        }
    }
    
    @BigSyncBackgroundActor
    var storedDatabaseToken: DatabaseChangeCursor? {
        get {
            guard let encodedToken = keyValueStore.object(forKey: userDefaultsKey(for: databaseServerChangeTokenKey)) as? Data else {
                return nil
            }
            return DatabaseChangeCursor(serializedData: encodedToken)
        }
        set {
            let key = userDefaultsKey(for: databaseServerChangeTokenKey)
            if let token = newValue {
                keyValueStore.set(value: token.serializedData, forKey: key)
            } else {
                keyValueStore.removeObject(forKey: key)
            }
        }
    }
    
    @BigSyncBackgroundActor
    var databaseSubscriptionID: String? {
        get {
            return getStoredSubscriptionIDsDictionary()?[storeKey(for: database)]
        }
        set {
            var dictionary: [String: String]! = getStoredSubscriptionIDsDictionary()
            if dictionary == nil {
                dictionary = [String: String]()
            }
            dictionary[storeKey(for: database)] = newValue
            setStoredSubscriptionIDsDictionary(dictionary)
        }
    }
    
    @BigSyncBackgroundActor
    func getStoredSubscriptionID(for recordZoneID: CKRecordZone.ID) -> String? {
        return getStoredSubscriptionIDsDictionary()?[storeKey(for: recordZoneID)]
    }
    
    @BigSyncBackgroundActor
    func storeSubscriptionID(_ subscriptionID: String, for recordZoneID: CKRecordZone.ID) {
        var dictionary: [String: String]! = getStoredSubscriptionIDsDictionary()
        if dictionary == nil {
            dictionary = [String: String]()
        }
        dictionary[storeKey(for: recordZoneID)] = subscriptionID
        setStoredSubscriptionIDsDictionary(dictionary)
    }

    @BigSyncBackgroundActor
    func clearStoredSubscriptionID(for recordZoneID: CKRecordZone.ID) {
        var dictionary = getStoredSubscriptionIDsDictionary()
        dictionary?.removeValue(forKey: storeKey(for: recordZoneID))
        setStoredSubscriptionIDsDictionary(dictionary)
    }
    
    @BigSyncBackgroundActor
    func clearSubscriptionID(_ subscriptionID: String) {
        var dictionary: [String: String]? = getStoredSubscriptionIDsDictionary()
        dictionary = dictionary?.filter { $0.value != subscriptionID}
        setStoredSubscriptionIDsDictionary(dictionary)
    }
    
    @BigSyncBackgroundActor
    func clearAllStoredSubscriptionIDs() {
        setStoredSubscriptionIDsDictionary(nil)
    }
    
    @BigSyncBackgroundActor
    func addMetadata(to records: [CKRecord]) {
        records.forEach {
            $0[cloudKitSynchronizerDeviceUUIDKey] = self.deviceIdentifier
            if self.compatibilityVersion > 0 {
                $0[cloudKitSynchronizerModelCompatibilityVersionKey] = self.compatibilityVersion
            }
        }
    }
    
    @BigSyncBackgroundActor
    fileprivate func getStoredSubscriptionIDsDictionary() -> [String: String]? {
        return keyValueStore.object(forKey: userDefaultsKey(for: subscriptionIdentifierKey)) as? [String: String]
    }
    
    @BigSyncBackgroundActor
    fileprivate func setStoredSubscriptionIDsDictionary(_ dict: [String: String]?) {
        let key = userDefaultsKey(for: subscriptionIdentifierKey)
        if dict != nil {
            keyValueStore.set(value: dict, forKey: key)
        } else {
            keyValueStore.removeObject(forKey: key)
        }
    }
    
    fileprivate func userDefaultsKey(for key: String) -> String {
        // Do not touch CKContainer.default when the synchronizer was created
        // with an explicit container. Besides avoiding unnecessary CloudKit
        // initialization, this keeps injected/test databases independent of
        // host-process entitlements.
        let prefix: String
        if let containerIdentifier {
            prefix = containerIdentifier
        } else {
            prefix = CKContainer.default().containerIdentifier ?? ""
        }
        return "\(prefix)-\(identifier)-\(key)"
    }
    
    fileprivate func storeKey(for zoneID: CKRecordZone.ID) -> String {
        return userDefaultsKey(for: "\(zoneID.ownerName).\(zoneID.zoneName)")
    }
    
    fileprivate func storeKey(for database: CloudKitDatabaseAdapter) -> String {
        return userDefaultsKey(for: "\(database.databaseScope == .private ? "privateDatabase" : "sharedDatabase")")
    }
}
