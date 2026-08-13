//
//  CloudKitSynchronizer+Private.swift
//  OCMock
//
//  Created by Manuel Entrena on 05/04/2019.
//

import Foundation
import CloudKit

private let customZoneName = "BigSyncKit"
#if DEBUG
private let storedDeviceUUIDKey = "QSCloudKitStoredDeviceUUIDKey"
#endif
private let subscriptionIdentifierKey = "QSSubscriptionIdentifierKey"
private let databaseServerChangeTokenKey = "QSDatabaseServerChangeTokenKey"

extension CloudKitSynchronizer {
    
    static var defaultCustomZoneID: CKRecordZone.ID {
        return CKRecordZone.ID(zoneName: customZoneName, ownerName: CKCurrentUserDefaultName)
    }

#if DEBUG
    /// Fixture-only access to the obsolete persisted echo tag. Production
    /// uses a process-scoped tag and never publishes this value.
    var deviceUUID: String? {
        get {
            keyValueStore.object(
                forKey: userDefaultsKey(for: storedDeviceUUIDKey)
            ) as? String
        }
        set {
            let key = userDefaultsKey(for: storedDeviceUUIDKey)
            if let newValue {
                keyValueStore.set(value: newValue, forKey: key)
            } else {
                keyValueStore.removeObject(forKey: key)
            }
        }
    }
#endif
    
#if DEBUG
    /// Fixture-only nonthrowing token seam. Production commits every cursor
    /// through `persistDatabaseToken(_:)` so a disk failure cannot be reported
    /// as a completed CloudKit page.
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
#else
    @BigSyncBackgroundActor
    var storedDatabaseToken: DatabaseChangeCursor? {
        guard let encodedToken = keyValueStore.object(
            forKey: userDefaultsKey(for: databaseServerChangeTokenKey)
        ) as? Data else { return nil }
        return DatabaseChangeCursor(serializedData: encodedToken)
    }
#endif

    @BigSyncBackgroundActor
    func persistDatabaseToken(_ token: DatabaseChangeCursor?) throws {
        let key = userDefaultsKey(for: databaseServerChangeTokenKey)
        if let token {
            try keyValueStore.bigSyncSetDurably(
                value: token.serializedData,
                forKey: key
            )
        } else {
            try keyValueStore.bigSyncRemoveDurably(forKey: key)
        }
    }
    
#if DEBUG
    @BigSyncBackgroundActor
    var databaseSubscriptionID: String? {
        get {
            getStoredSubscriptionIDsDictionary()?[storeKey(for: database)]
        }
        set {
            var dictionary = getStoredSubscriptionIDsDictionary() ?? [:]
            dictionary[storeKey(for: database)] = newValue
            setStoredSubscriptionIDsDictionaryForTesting(
                dictionary.isEmpty ? nil : dictionary
            )
        }
    }
#else
    @BigSyncBackgroundActor
    var databaseSubscriptionID: String? {
        getStoredSubscriptionIDsDictionary()?[storeKey(for: database)]
    }
#endif

    @BigSyncBackgroundActor
    func persistDatabaseSubscriptionID(_ subscriptionID: String?) throws {
        var dictionary = getStoredSubscriptionIDsDictionary() ?? [:]
        dictionary[storeKey(for: database)] = subscriptionID
        try persistStoredSubscriptionIDsDictionary(
            dictionary.isEmpty ? nil : dictionary
        )
    }
    
    @BigSyncBackgroundActor
    func getStoredSubscriptionID(for recordZoneID: CKRecordZone.ID) -> String? {
        return getStoredSubscriptionIDsDictionary()?[storeKey(for: recordZoneID)]
    }
    
    @BigSyncBackgroundActor
    func persistSubscriptionID(
        _ subscriptionID: String?,
        for recordZoneID: CKRecordZone.ID
    ) throws {
        var dictionary = getStoredSubscriptionIDsDictionary() ?? [:]
        dictionary[storeKey(for: recordZoneID)] = subscriptionID
        try persistStoredSubscriptionIDsDictionary(
            dictionary.isEmpty ? nil : dictionary
        )
    }

#if DEBUG
    @BigSyncBackgroundActor
    func storeSubscriptionID(
        _ subscriptionID: String,
        for recordZoneID: CKRecordZone.ID
    ) {
        var dictionary = getStoredSubscriptionIDsDictionary() ?? [:]
        dictionary[storeKey(for: recordZoneID)] = subscriptionID
        setStoredSubscriptionIDsDictionaryForTesting(dictionary)
    }
#endif

    @BigSyncBackgroundActor
    func clearAllStoredSubscriptionIDs() throws {
        try persistStoredSubscriptionIDsDictionary(nil)
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
    fileprivate func persistStoredSubscriptionIDsDictionary(
        _ dictionary: [String: String]?
    ) throws {
        let key = userDefaultsKey(for: subscriptionIdentifierKey)
        if let dictionary {
            try keyValueStore.bigSyncSetDurably(
                value: dictionary,
                forKey: key
            )
        } else {
            try keyValueStore.bigSyncRemoveDurably(forKey: key)
        }
    }

#if DEBUG
    @BigSyncBackgroundActor
    fileprivate func setStoredSubscriptionIDsDictionaryForTesting(
        _ dictionary: [String: String]?
    ) {
        let key = userDefaultsKey(for: subscriptionIdentifierKey)
        if let dictionary {
            keyValueStore.set(value: dictionary, forKey: key)
        } else {
            keyValueStore.removeObject(forKey: key)
        }
    }
#endif

    @BigSyncBackgroundActor
    func persistRemovingSubscriptionID(_ identifier: String) throws {
        let dictionary = getStoredSubscriptionIDsDictionary()?
            .filter { $0.value != identifier }
        try persistStoredSubscriptionIDsDictionary(
            dictionary?.isEmpty == true ? nil : dictionary
        )
    }
    
    fileprivate func userDefaultsKey(for key: String) -> String {
        durableStateKey(key)
    }
    
    fileprivate func storeKey(for zoneID: CKRecordZone.ID) -> String {
        return userDefaultsKey(for: "\(zoneID.ownerName).\(zoneID.zoneName)")
    }
    
    fileprivate func storeKey(for database: CloudKitDatabaseAdapter) -> String {
        return userDefaultsKey(for: "\(database.databaseScope == .private ? "privateDatabase" : "sharedDatabase")")
    }
}
