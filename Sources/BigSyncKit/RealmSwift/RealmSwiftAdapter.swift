//
//  RealmSwiftAdapter.swift
//  Pods
//
//  Created by Manuel Entrena on 29/08/2017.
//
//

import Foundation
import CloudKit
#if os(macOS)
import Cocoa
#else
import UIKit
#endif
import RealmSwift
import Realm

extension Realm {
    public func safeWrite(_ block: (() throws -> Void)) throws {
        if isInWriteTransaction {
            try block()
        } else {
            try write(block)
        }
    }
}

public protocol RealmSwiftAdapterDelegate: AnyObject {
    /**
     *  Asks the delegate to resolve conflicts for a managed object when using a custom mergePolicy.
     *  The delegate is expected to examine the change dictionary and optionally apply any of those changes to the managed object.
     *
     *  @param adapter    The `QSRealmSwiftAdapter` that is providing the changes.
     *  @param changeDictionary Dictionary containing keys and values with changes for the managed object. Values can be [NSNull null] to represent a nil value.
     *  @param object           The `RLMObject` that has changed on iCloud.
     */
    func realmSwiftAdapter(_ adapter:RealmSwiftAdapter, gotChanges changes: [String: Any], object: Object) -> Bool
}

public protocol RealmSwiftAdapterRecordProcessing: AnyObject {
    /**
     *  Called by the adapter before copying a property from the Realm object to the CloudKit record to upload to CloudKit.
     *  The method can then apply custom logic to encode the property in the record.
     *
     *  @param propertyname     The name of the property that is being processed
     *  @param object   The `RLMObject` that is going to have its record uploaded.
     *  @param record   The `CKRecord` that is being configured before being sent to CloudKit.
     *
     *  @return Boolean indicating whether the adapter should process property normally. Return false if property was already handled in this method.
     */
    func shouldProcessPropertyBeforeUpload(propertyName: String, object: Object, record: CKRecord) -> Bool
    
    /**
     *  Called by the adapter before copying a property from the CloudKit record that was just downloaded to the Realm object.
     *  The method can apply custom logic to save the property from the record to the object. An object implementing this method *should not* change the record itself.
     *
     *  @param propertyname     The name of the property that is being processed
     *  @param object   The `RLMObject` that corresponds to the downloaded record.
     *  @param record   The `CKRecord` that was downloaded from CloudKit.
     *
     *  @return Boolean indicating whether the adapter should process property normally. Return false if property was already handled in this method.
     */
    func shouldProcessPropertyInDownload(propertyName: String, object: Object, record: CKRecord) -> Bool
}

struct ChildRelationship {
    
    let parentEntityName: String
    let childEntityName: String
    let childParentKey: String
}

struct RealmProvider {
    let persistenceConfiguration: Realm.Configuration
    let targetConfiguration: Realm.Configuration
    
    var persistenceRealm: Realm {
        return try! Realm(configuration: persistenceConfiguration)
    }
    var targetRealm: Realm {
        return try! Realm(configuration: targetConfiguration)
    }
    
    init?(persistenceConfiguration: Realm.Configuration, targetConfiguration: Realm.Configuration) {
        guard (try? Realm(configuration: persistenceConfiguration)) != nil &&
            (try? Realm(configuration: targetConfiguration)) != nil else {
                return nil
        }
        
        self.persistenceConfiguration = persistenceConfiguration
        self.targetConfiguration = targetConfiguration
    }
}

struct ObjectUpdate {
    enum UpdateType {
        case insertion
        case modification
        case deletion
    }
    
    let object: Object
    let identifier: String
    let entityType: String
    let updateType: UpdateType
}


public class RealmSwiftAdapter: NSObject, ModelAdapter {
    static let shareRelationshipKey = "com.syncKit.shareRelationship"
    
    public let persistenceRealmConfiguration: Realm.Configuration
    public let targetRealmConfiguration: Realm.Configuration
    public let zoneID: CKRecordZone.ID
    public var mergePolicy: MergePolicy = .server
    public weak var delegate: RealmSwiftAdapterDelegate?
    public weak var recordProcessingDelegate: RealmSwiftAdapterRecordProcessing?
    public var forceDataTypeInsteadOfAsset: Bool = false
    
    private lazy var tempFileManager: TempFileManager = {
        TempFileManager(identifier: "\(recordZoneID.ownerName).\(recordZoneID.zoneName).\(targetRealmConfiguration.fileURL?.lastPathComponent ?? UUID().uuidString).\(targetRealmConfiguration.schemaVersion)")
    }()
    
    var realmProvider: RealmProvider!
    
    var collectionNotificationTokens = [NotificationToken]()
    var pendingTrackingUpdates = [ObjectUpdate]()
    var childRelationships = [String: Array<ChildRelationship>]()
    var modelTypes = [String: Object.Type]()
    public private(set) var hasChanges = false
    
    public init(persistenceRealmConfiguration: Realm.Configuration, targetRealmConfiguration: Realm.Configuration, recordZoneID: CKRecordZone.ID) {
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfiguration = targetRealmConfiguration
        self.zoneID = recordZoneID
        
        super.init()
        
        BackgroundWorker.shared.start { [weak self] in
            self?.setupTypeNamesLookup()
            self?.setup()
            self?.setupChildrenRelationshipsLookup()
        }
    }
    
    deinit {
        invalidateRealmAndTokens()
    }
    
    func invalidateRealmAndTokens() {
        for token in collectionNotificationTokens {
            token.invalidate()
        }
        collectionNotificationTokens.removeAll()
        
        realmProvider?.persistenceRealm.invalidate()
        realmProvider = nil
    }
    
    static public func defaultPersistenceConfiguration() -> Realm.Configuration {
        var configuration = Realm.Configuration()
        configuration.schemaVersion = 2
        configuration.migrationBlock = { migration, oldSchemaVersion in
            
        }
        configuration.objectTypes = [SyncedEntity.self, Record.self, PendingRelationship.self, ServerToken.self]
        return configuration
    }
    
    func setupTypeNamesLookup() {
        targetRealmConfiguration.objectTypes?.forEach { objectType in
            modelTypes[objectType.className()] = objectType as? Object.Type
        }
    }
    
    func setup() {
        realmProvider = RealmProvider(persistenceConfiguration: persistenceRealmConfiguration, targetConfiguration: targetRealmConfiguration)
        
        let needsInitialSetup = realmProvider.persistenceRealm.objects(SyncedEntity.self).count <= 0
        
        for schema in realmProvider.targetRealm.schema.objectSchema {
            let objectClass = realmObjectClass(name: schema.className)
            let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
            let results = realmProvider.targetRealm.objects(objectClass)
            
            // Register for collection notifications
            let token = results.observe({ [weak self] (collectionChange) in
                guard let self = self else { return }
                switch collectionChange {
                case .update(let collection, _, let insertions, let modifications):
                    // Deletions are covered via soft-delete (SyncedDeletable) under modifications.
                    
                    for index in insertions {
                        let object = collection[index]
                        
                        if (object as? (any SyncableObject))?.isDeleted ?? false {
                            continue
                        }
                        
                        let identifier = self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                        /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
                         * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
                         */
                        if object.realm!.isInWriteTransaction {
                            self.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: .insertion))
                        } else {
                            self.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: true, modified: false, deleted: false, realmProvider: self.realmProvider)
                        }
                    }
                    
                    for index in modifications {
                        let object = collection[index]
                        let identifier = self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                        let isDeletion = (object as? (any SyncableObject))?.isDeleted ?? false
                        /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
                         * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
                         */
                        if object.realm!.isInWriteTransaction {
                            self.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: isDeletion ? .deletion : .modification))
                        } else {
                            self.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: false, modified: !isDeletion, deleted: isDeletion, realmProvider: self.realmProvider)
                        }
                    }
                    
                default: break
                }
            })
            collectionNotificationTokens.append(token)
            
            if needsInitialSetup {
                for object in results {
                    let identifier = self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                    createSyncedEntity(entityType: schema.className, identifier: identifier, realm: realmProvider.persistenceRealm)
                }
            }
        }
        
        let token = realmProvider.targetRealm.observe { [weak self] (_, _) in
            self?.enqueueObjectUpdates()
        }
        collectionNotificationTokens.append(token)
        
        updateHasChanges(realm: realmProvider.persistenceRealm)
        
        if hasChanges {
            Task { @MainActor in
                NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
            }
        }
        
        startObservingTermination()
    }
    
    public func hasRealmObjectClass(name: String) -> Bool {
        return modelTypes.keys.contains(name)
    }
    
    func realmObjectClass(name: String) -> Object.Type {
        return modelTypes[name]!
    }
    
    func updateHasChanges(realm: Realm) {
        let predicate = NSPredicate(format: "state != %ld", SyncedEntityState.synced.rawValue)
        let results = realm.objects(SyncedEntity.self).filter(predicate)
        
        hasChanges = results.count > 0;
    }
    
    func setupChildrenRelationshipsLookup() {
        childRelationships.removeAll()
        
        for objectSchema in realmProvider.targetRealm.schema.objectSchema {
            let objectClass = realmObjectClass(name: objectSchema.className)
            if let parentClass = objectClass.self as? ParentKey.Type {
                let parentKey = parentClass.parentKey()
                let parentProperty = objectSchema.properties.first { $0.name == parentKey }
                
                let parentClassName = parentProperty!.objectClassName!
                let relationship = ChildRelationship(parentEntityName: parentClassName, childEntityName: objectSchema.className, childParentKey: parentKey)
                if childRelationships[parentClassName] == nil {
                    childRelationships[parentClassName] = Array<ChildRelationship>()
                }
                childRelationships[parentClassName]!.append(relationship)
            }
        }
    }
    
    func enqueueObjectUpdates() {
        if pendingTrackingUpdates.count > 0 {
            if realmProvider.targetRealm.isInWriteTransaction {
                BackgroundWorker.shared.start { [weak self] in
                    self?.enqueueObjectUpdates()
                }
            } else {
                updateObjectTracking()
            }
        }
    }
    
    func updateObjectTracking() {
        for update in pendingTrackingUpdates {
            switch update.updateType {
            case .insertion:
                self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: true, modified: false, deleted: false, realmProvider: realmProvider)
            case .modification:
                self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: false, modified: true, deleted: false, realmProvider: realmProvider)
            case .deletion:
                self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: false, modified: false, deleted: true, realmProvider: realmProvider)
            }
        }
        
        pendingTrackingUpdates.removeAll()
    }
    
    func updateTracking(objectIdentifier: String, entityName: String, inserted: Bool, modified: Bool, deleted: Bool, realmProvider: RealmProvider) {
        var isNewChange = false
        let identifier = "\(entityName).\(objectIdentifier)"
        let syncedEntity = getSyncedEntity(objectIdentifier: identifier, realm: realmProvider.persistenceRealm)
        
        if deleted {
            isNewChange = true
            
            if let syncedEntity = syncedEntity {
                try? realmProvider.persistenceRealm.safeWrite {
                    syncedEntity.state = SyncedEntityState.deleted.rawValue
                }
            }
        } else if syncedEntity == nil {
            self.createSyncedEntity(entityType: entityName, identifier: objectIdentifier, realm: self.realmProvider.persistenceRealm)
            
            if inserted {
                isNewChange = true
            }
        } else if !inserted {
            guard let syncedEntity = syncedEntity else {
                return
            }
            
            isNewChange = true
            
            try? realmProvider.persistenceRealm.safeWrite {
                if syncedEntity.state == SyncedEntityState.synced.rawValue && modified {
                    syncedEntity.state = SyncedEntityState.newOrChanged.rawValue
                    // If state was New (or Modified already) then leave it as that
                }
            }
        }
        
        if !hasChanges && isNewChange {
            hasChanges = true
            Task { @MainActor in
                NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
            }
        }
    }
    
    func commitTargetWriteTransactionWithoutNotifying() {
        try? realmProvider.targetRealm.commitWrite(withoutNotifying: collectionNotificationTokens)
    }
    
    @discardableResult
    func createSyncedEntity(entityType: String, identifier: String, realm: Realm) -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: entityType, identifier: "\(entityType).\(identifier)", state: SyncedEntityState.newOrChanged.rawValue)
        
        try? realm.safeWrite {
            realm.add(syncedEntity, update: .modified)
        }
        
        return syncedEntity
    }
    
    func createSyncedEntity(record: CKRecord, realmProvider: RealmProvider) -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: record.recordType, identifier: record.recordID.recordName, state: SyncedEntityState.synced.rawValue)
        
        realmProvider.persistenceRealm.add(syncedEntity)
        
        let objectClass = realmObjectClass(name: record.recordType)
        let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
        let object = objectClass.init()
        object.setValue(objectIdentifier, forKey: primaryKey)
        realmProvider.targetRealm.add(object)
        
        return syncedEntity;
    }
    
    func getObjectIdentifier(for syncedEntity: SyncedEntity) -> Any {
        let range = syncedEntity.identifier.range(of: syncedEntity.entityType)!
        let index = syncedEntity.identifier.index(range.upperBound, offsetBy: 1)
        let objectIdentifier = String(syncedEntity.identifier[index...])
        let objectClass = realmObjectClass(name: syncedEntity.entityType)
        
        guard let objectSchema = objectClass.sharedSchema(),
              let keyType = objectSchema.primaryKeyProperty?.type else {
            return objectIdentifier
        }
        
        switch keyType {
        case .int:
            return Int(objectIdentifier)!
        case .objectId:
            return try! ObjectId(string: objectIdentifier)
        case .string:
            return objectIdentifier
        case .UUID:
            return UUID(uuidString: objectIdentifier)!
        default:
            return objectIdentifier
        }
    }
    
    func getObjectIdentifier(stringObjectId: String, entityType: String) -> Any? {
        let objectClass = realmObjectClass(name: entityType)
        guard let schema = objectClass.sharedSchema(),
              let keyType = schema.primaryKeyProperty?.type else {
            return nil
        }
        
        switch keyType {
        case .int:
            return Int(stringObjectId)!
        case .objectId:
            return try! ObjectId(string: stringObjectId)
        case .string:
            return stringObjectId
        case .UUID:
            return UUID(uuidString: stringObjectId)!
        default:
            return stringObjectId
        }
    }
    
    func syncedEntity(for object: Object, realm: Realm) -> SyncedEntity? {
        let objectClass = realmObjectClass(name: object.objectSchema.className)
        let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
        let identifier = object.objectSchema.className + "." + getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
        return getSyncedEntity(objectIdentifier: identifier, realm: realm)
    }
    
    func getStringIdentifier(for object: Object, usingPrimaryKey key: String) -> String {
        let objectId = object.value(forKey: key)
        if let value = objectId as? CustomStringConvertible {
            return String(describing: value)
        } else {
            return objectId as! String
        }
    }
    
    func getSyncedEntity(objectIdentifier: String, realm: Realm) -> SyncedEntity? {
        return realm.object(ofType: SyncedEntity.self, forPrimaryKey: objectIdentifier)
    }
    
    func shouldIgnore(key: String) -> Bool {
        return CloudKitSynchronizer.metadataKeys.contains(key)
    }
    
    func applyChanges(in record: CKRecord, to object: Object, syncedEntity: SyncedEntity, realmProvider: RealmProvider) {
        if syncedEntity.state == SyncedEntityState.newOrChanged.rawValue {
            if mergePolicy == .server {
                for property in object.objectSchema.properties {
                    if shouldIgnore(key: property.name) {
                        continue
                    }
                    if property.type == PropertyType.linkingObjects {
                        continue
                    }
                    
                    applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
                }
            } else if mergePolicy == .custom {
                var recordChanges = [String: Any]()
                
                for property in object.objectSchema.properties {
                    if property.type == PropertyType.linkingObjects {
                        continue
                    }
                    
                    if !shouldIgnore(key: property.name) {
                        if let asset = record[property.name] as? CKAsset {
                            recordChanges[property.name] = asset.fileURL != nil ? NSData(contentsOf: asset.fileURL!) : NSNull()
                        } else {
                            recordChanges[property.name] = record[property.name] ?? NSNull()
                        }
                    }
                }
                
                if delegate?.realmSwiftAdapter(self, gotChanges: recordChanges, object: object) ?? false {
                    for property in object.objectSchema.properties {
                        if shouldIgnore(key: property.name) {
                            continue
                        }
                        if property.type == PropertyType.linkingObjects {
                            continue
                        }
                        
                        applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
                    }
                }
            }
        } else {
            for property in object.objectSchema.properties {
                if shouldIgnore(key: property.name) {
                    continue
                }
                if property.type == PropertyType.linkingObjects {
                    continue
                }
                
                applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
            }
        }
    }
    
    func applyChange(property: Property, record: CKRecord, object: Object, syncedEntity: SyncedEntity, realmProvider: RealmProvider) {
        let key = property.name
        if key == object.objectSchema.primaryKeyProperty!.name {
            return
        }
        
        if let recordProcessingDelegate = recordProcessingDelegate,
           !recordProcessingDelegate.shouldProcessPropertyInDownload(propertyName: key, object: object, record: record) {
            return
        }
        
        let value = record[key]
        
        // List support forked from IceCream: https://github.com/caiyue1993/IceCream/blob/master/IceCream/Classes/CKRecordRecoverable.swift
        var recordValue: Any?
        if property.isArray {
            switch property.type {
            case .int:
                guard let value = record.value(forKey: property.name) as? [Int] else { break }
                let list = List<Int>()
                list.append(objectsIn: value)
                recordValue = list
            case .string:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                let list = List<String>()
                list.append(objectsIn: value)
                recordValue = list
            case .bool:
                guard let value = record.value(forKey: property.name) as? [Bool] else { break }
                let list = List<Bool>()
                list.append(objectsIn: value)
                recordValue = list
            case .float:
                guard let value = record.value(forKey: property.name) as? [Float] else { break }
                let list = List<Float>()
                list.append(objectsIn: value)
                recordValue = list
            case .double:
                guard let value = record.value(forKey: property.name) as? [Double] else { break }
                let list = List<Double>()
                list.append(objectsIn: value)
                recordValue = list
            case .data:
                guard let value = record.value(forKey: property.name) as? [Data] else { break }
                let list = List<Data>()
                list.append(objectsIn: value)
                recordValue = list
            case .date:
                guard let value = record.value(forKey: property.name) as? [Date] else { break }
                let list = List<Date>()
                list.append(objectsIn: value)
                recordValue = list
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realmProvider.persistenceRealm)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realmProvider.persistenceRealm)
                    }
                }
            default:
                break
            }
            object.setValue(recordValue, forKey: property.name)
        } else if let reference = value as? CKRecord.Reference {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            let recordName = reference.recordID.recordName
            let separatorRange = recordName.range(of: ".")!
            let objectIdentifier = String(recordName[separatorRange.upperBound...])
            savePendingRelationship(name: key, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realmProvider.persistenceRealm)
        } else if property.type == .object {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            guard let recordName = record.value(forKey: property.name) as? String else { return }
            let separatorRange = recordName.range(of: ".")!
            let objectIdentifier = String(recordName[separatorRange.upperBound...])
            savePendingRelationship(name: key, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realmProvider.persistenceRealm)
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
                let data =  NSData(contentsOf: fileURL) {
                object.setValue(data, forKey: key)
            }
        } else if value != nil || property.isOptional == true {
            // If property is not a relationship or value is nil and property is optional.
            // If value is nil and property is non-optional, it is ignored. This is something that could happen
            // when extending an object model with a new non-optional property, when an old record is applied to the object.
            object.setValue(value, forKey: key)
        }
    }
    
    func savePendingRelationship(name: String, syncedEntity: SyncedEntity, targetIdentifier: String, realm: Realm) {
        let pendingRelationship = PendingRelationship()
        pendingRelationship.relationshipName = name
        pendingRelationship.forSyncedEntity = syncedEntity
        pendingRelationship.targetIdentifier = targetIdentifier
        realm.add(pendingRelationship)
    }
    
    func saveShareRelationship(for entity: SyncedEntity, record: CKRecord) {
        if let share = record.share {
            let relationship = PendingRelationship()
            relationship.relationshipName = RealmSwiftAdapter.shareRelationshipKey
            relationship.targetIdentifier = share.recordID.recordName
            relationship.forSyncedEntity = entity
            entity.realm?.add(relationship)
        }
    }
    
    func applyPendingRelationships(realmProvider: RealmProvider) {
        let pendingRelationships = realmProvider.persistenceRealm.objects(PendingRelationship.self)
        
        if pendingRelationships.count == 0 {
            return
        }
        
        realmProvider.persistenceRealm.beginWrite()
        realmProvider.targetRealm.beginWrite()
        for relationship in pendingRelationships {
            
            let entity = relationship.forSyncedEntity
            
            guard let syncedEntity = entity,
                syncedEntity.entityState != .deleted else { continue }
            
            let originObjectClass = realmObjectClass(name: syncedEntity.entityType)
            let objectIdentifier = getObjectIdentifier(for: syncedEntity)
            guard let originObject = realmProvider.targetRealm.object(ofType: originObjectClass, forPrimaryKey: objectIdentifier) else { continue }
            
            if relationship.relationshipName == RealmSwiftAdapter.shareRelationshipKey {
                syncedEntity.share = getSyncedEntity(objectIdentifier: relationship.targetIdentifier, realm: realmProvider.persistenceRealm)
                realmProvider.persistenceRealm.delete(relationship)
                continue;
            }
            
            var targetClassName: String?
            for property in originObject.objectSchema.properties {
                if property.name == relationship.relationshipName {
                    targetClassName = property.objectClassName
                    break
                }
            }
            
            guard let className = targetClassName else {
                continue
            }
            
            let targetObjectClass = realmObjectClass(name: className)
            let targetObjectIdentifier = getObjectIdentifier(stringObjectId: relationship.targetIdentifier, entityType: className)
            let targetObject = realmProvider.targetRealm.object(ofType: targetObjectClass, forPrimaryKey: targetObjectIdentifier)
            
            guard let target = targetObject else {
                continue
            }
            originObject.setValue(target, forKey: relationship.relationshipName)
            
            realmProvider.persistenceRealm.delete(relationship)
        }
        
        try? realmProvider.persistenceRealm.commitWrite()
        commitTargetWriteTransactionWithoutNotifying()
        debugPrint("Finished applying pending relationships")
    }
    
    func save(record: CKRecord, for syncedEntity: SyncedEntity) {
        if syncedEntity.record == nil {
            syncedEntity.record = Record()
        }
        
        syncedEntity.record!.encodedRecord = encodedRecord(record, onlySystemFields: true)
    }
    
    func encodedRecord(_ record: CKRecord, onlySystemFields: Bool) -> Data {
        let data = NSMutableData()
        let archiver = NSKeyedArchiver(forWritingWith: data)
        if onlySystemFields {
            record.encodeSystemFields(with: archiver)
        } else {
            record.encode(with: archiver)
        }
        archiver.finishEncoding()
        return data as Data
    }
    
    func getRecord(for syncedEntity: SyncedEntity) -> CKRecord? {
        var record: CKRecord?
        if let recordData = syncedEntity.record?.encodedRecord {
            let unarchiver = NSKeyedUnarchiver(forReadingWith: recordData)
            record = CKRecord(coder: unarchiver)
            unarchiver.finishDecoding()
        }
        return record
    }
    
    func save(share: CKShare, forSyncedEntity entity: SyncedEntity, realmProvider: RealmProvider) {
        var qsRecord: Record?
        if let entityForShare = entity.share {
            qsRecord = entityForShare.record
        } else {
            let entityForShare = createSyncedEntity(for: share, realmProvider: realmProvider)
            qsRecord = Record()
            realmProvider.persistenceRealm.add(qsRecord!)
            entityForShare.record = qsRecord
            entity.share = entityForShare
        }
        
        qsRecord?.encodedRecord = encodedRecord(share, onlySystemFields: false)
    }
    
    func getShare(for entity: SyncedEntity) -> CKShare? {
        if let recordData = entity.share?.record?.encodedRecord {
            let unarchiver = NSKeyedUnarchiver(forReadingWith: recordData)
            let share = CKShare(coder: unarchiver)
            unarchiver.finishDecoding()
            return share
        } else {
            return nil
        }
    }
    
    func createSyncedEntity(for share: CKShare, realmProvider: RealmProvider) -> SyncedEntity {
        let entityForShare = SyncedEntity()
        entityForShare.entityType = "CKShare"
        entityForShare.identifier = share.recordID.recordName
        entityForShare.updated = Date()
        entityForShare.state = SyncedEntityState.synced.rawValue
        realmProvider.persistenceRealm.add(entityForShare)
        
        return entityForShare
    }
    
    func nextStateToSync(after state: SyncedEntityState) -> SyncedEntityState {
        return SyncedEntityState(rawValue: state.rawValue + 1)!
    }
    
    func recordsToUpload(withState state: SyncedEntityState, limit: Int, realmProvider: RealmProvider) -> [CKRecord] {
        let predicate = NSPredicate(format: "state == %ld", state.rawValue)
        let results = realmProvider.persistenceRealm.objects(SyncedEntity.self).filter(predicate)
        var resultArray = [CKRecord]()
        var includedEntityIDs = Set<String>()
        for syncedEntity in results {
            
            if resultArray.count > limit {
                break
            }
            
            if !hasRealmObjectClass(name: syncedEntity.entityType) {
                continue
            }
            
            var entity: SyncedEntity! = syncedEntity
            while entity != nil && entity.state == state.rawValue && !includedEntityIDs.contains(entity.identifier) {
                var parentEntity: SyncedEntity? = nil
                guard let record = recordToUpload(syncedEntity: entity, realmProvider: realmProvider, parentSyncedEntity: &parentEntity) else {
                    entity = nil
                    continue
                }
                resultArray.append(record)
                includedEntityIDs.insert(entity.identifier)
                entity = parentEntity
            }
        }
        
        return resultArray
    }
    
    func recordToUpload(syncedEntity: SyncedEntity, realmProvider: RealmProvider, parentSyncedEntity: inout SyncedEntity?) -> CKRecord? {
        let record = getRecord(for: syncedEntity) ?? CKRecord(recordType: syncedEntity.entityType, recordID: CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        
        let objectClass = realmObjectClass(name: syncedEntity.entityType)
        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
        let object = realmProvider.targetRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
        let entityState = syncedEntity.state
        
        guard let object = object else {
            // Object does not exist, but tracking syncedEntity thinks it does.
            // We mark it as deleted so the iCloud record will get deleted too
            try? realmProvider.persistenceRealm.write {
                syncedEntity.entityState = .deleted
            }
            return nil
        }
        
//        let changedKeys = (syncedEntity.changedKeys ?? "").components(separatedBy: ",")
        
        var parentKey: String?
        if let childObject = object as? ParentKey {
            parentKey = type(of: childObject).parentKey()
        }
        
        for property in object.objectSchema.properties {
            if entityState == SyncedEntityState.newOrChanged.rawValue {
                if let recordProcessingDelegate = recordProcessingDelegate,
                   !recordProcessingDelegate.shouldProcessPropertyBeforeUpload(propertyName: property.name, object: object, record: record) {
                    continue
                }
                
                if property.type == PropertyType.object {
                    if let target = object.value(forKey: property.name) as? Object {
                        let targetPrimaryKey = (type(of: target).primaryKey() ?? target.objectSchema.primaryKeyProperty?.name)!
                        let targetIdentifier = self.getStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
                        let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                        let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                        record[property.name] = recordID.recordName as CKRecordValue
                    }
                } else if property.isArray {
                    // Array handling forked from IceCream: https://github.com/caiyue1993/IceCream/blob/b29dfe81e41cc929c8191c3266189a7070cb5bc5/IceCream/Classes/CKRecordConvertible.swift
                    let value = object.value(forKey: property.name)
                    switch property.type {
                    case .object:
                        /// We may get List<Cat> here
                        /// The item cannot be casted as List<Object>
                        /// It can be casted at a low-level type `ListBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let list = value as? RLMSwiftCollectionBase, list._rlmCollection.count > 0 else { break }
                        var referenceArray = [String]()
                        let wrappedArray = list._rlmCollection
                        for index in 0..<wrappedArray.count {
                            guard let object = wrappedArray[index] as? Object, let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            #warning("Confirm here that isDeleted is false before referencing, as icecream does (link above)")
                            let targetIdentifier = self.getStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray as CKRecordValue
                    case .int:
                        guard let list = value as? List<Int>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .string:
                        guard let list = value as? List<String>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .bool:
                        guard let list = value as? List<Bool>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .float:
                        guard let list = value as? List<Float>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .double:
                        guard let list = value as? List<Double>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .data:
                        guard let list = value as? List<Data>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .date:
                        guard let list = value as? List<Date>, !list.isEmpty else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of List is not supported yet
                        break
                    }
                } else if (
                    property.type != PropertyType.linkingObjects &&
                    !(property.name == (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!)
                ) {
                    let value = object.value(forKey: property.name)
                    if property.type == PropertyType.data,
                        let data = value as? Data,
                        forceDataTypeInsteadOfAsset == false  {
                        
                        let fileURL = self.tempFileManager.store(data: data)
                        let asset = CKAsset(fileURL: fileURL)
                        record[property.name] = asset
                    } else if value == nil {
                        record[property.name] = nil
                    } else if let recordValue = value as? CKRecordValue {
                        record[property.name] = recordValue
                    }
                }
            }
        }
        
        return record;
    }
    
    func startObservingTermination() {
        Task { @MainActor in
#if os(iOS) || os(tvOS)
            NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willTerminateNotification, object: nil)
#elseif os(macOS)
            NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willTerminateNotification, object: nil)
#endif
        }
    }
    
    /// Deletes soft-deleted objects.
    @objc func cleanUp() {
        for schema in realmProvider.targetRealm.schema.objectSchema {
            let objectClass = realmObjectClass(name: schema.className)
            guard objectClass.self is any SyncableObject.Type else { continue }
            
            let results = realmProvider.targetRealm.objects(objectClass).filter { ($0 as? (any SyncableObject))?.isDeleted ?? false }
            realmProvider.targetRealm.beginWrite()
            results.forEach({ realmProvider.targetRealm.delete($0) })
            commitTargetWriteTransactionWithoutNotifying()
        }
    }
    
    // MARK: - Children records
    
    func childrenRecords(for syncedEntity: SyncedEntity) -> [CKRecord] {
        var records = [CKRecord]()
        var parent: SyncedEntity?
        guard let record = recordToUpload(syncedEntity: syncedEntity, realmProvider: realmProvider, parentSyncedEntity: &parent) else {
            return []
        }
        records.append(record)
        
        if let relationships = childRelationships[syncedEntity.entityType] {
            for relationship in relationships {
                let objectID = getObjectIdentifier(for: syncedEntity)
                let objectClass = realmObjectClass(name: syncedEntity.entityType) as Object.Type
                if let object = realmProvider.targetRealm.object(ofType: objectClass.self, forPrimaryKey: objectID) {
                    
                    // Get children
                    let childObjectClass = realmObjectClass(name: relationship.childEntityName)
                    let predicate = NSPredicate(format: "%K == %@", relationship.childParentKey, object)
                    let children = realmProvider.targetRealm.objects(childObjectClass.self).filter(predicate)
                    
                    for child in children {
                        if let childEntity = self.syncedEntity(for: child, realm: realmProvider.persistenceRealm) {
                            records.append(contentsOf: childrenRecords(for: childEntity))
                        }
                    }
                }
            }
        }
        
        return records
    }
    
    // MARK: - QSModelAdapter
    
    public func prepareToImport() {
        
    }
    
    public func saveChanges(in records: [CKRecord]) {
        guard records.count != 0,
            realmProvider != nil else {
            return
        }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            self.realmProvider.persistenceRealm.beginWrite()
            self.realmProvider.targetRealm .beginWrite()
            
            for record in records {
                var syncedEntity: SyncedEntity! = self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: self.realmProvider.persistenceRealm)
                if syncedEntity == nil {
                    if #available(iOS 10.0, *) {
                        if let share = record as? CKShare {
                            syncedEntity = self.createSyncedEntity(for: share, realmProvider: self.realmProvider)
                        } else {
                            syncedEntity = self.createSyncedEntity(record: record, realmProvider: self.realmProvider)
                        }
                    } else {
                        syncedEntity = self.createSyncedEntity(record: record, realmProvider: self.realmProvider)
                    }
                }
                
                if syncedEntity.entityState != .deleted && syncedEntity.entityType != "CKShare" {
                    let objectClass = self.realmObjectClass(name: record.recordType)
                    let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                    guard let object = self.realmProvider.targetRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier) else {
                        continue
                    }
                    
                    self.applyChanges(in: record, to: object, syncedEntity: syncedEntity, realmProvider: self.realmProvider)
                    self.saveShareRelationship(for: syncedEntity, record: record)
                }
                
                self.save(record: record, for: syncedEntity)
            }
            // Order is important here. Notifications might be delivered after targetRealm is saved and
            // it's convenient if the persistenceRealm is not in a write transaction
            try? self.realmProvider.persistenceRealm.commitWrite()
            self.commitTargetWriteTransactionWithoutNotifying()
        }
    }
    
    public func deleteRecords(with recordIDs: [CKRecord.ID]) {
        debugPrint("Deleting records with record ids \(recordIDs)")
        guard recordIDs.count != 0,
            realmProvider != nil else {
            return
        }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            self.realmProvider.persistenceRealm.beginWrite()
            self.realmProvider.targetRealm.beginWrite()
            
            for recordID in recordIDs {
                if let syncedEntity = self.getSyncedEntity(objectIdentifier: recordID.recordName, realm: self.realmProvider.persistenceRealm) {
                    
                    if syncedEntity.entityType != "CKShare" {
                        let objectClass = self.realmObjectClass(name: syncedEntity.entityType)
                        let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                        let object = self.realmProvider.targetRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
                        
                        if let object = object {
                            self.realmProvider.targetRealm.delete(object)
                        }
                    }
                    
                    if let record = syncedEntity.record {
                        self.realmProvider.persistenceRealm.delete(record);
                    }
                    
                    self.realmProvider.persistenceRealm.delete(syncedEntity)
                }
            }
            
            try? self.realmProvider.persistenceRealm.commitWrite()
            self.commitTargetWriteTransactionWithoutNotifying()
        }
    }
    
    public func persistImportedChanges(completion: @escaping ((Error?) -> Void)) {
        guard realmProvider != nil else {
            completion(nil)
            return
        }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let realmProvider = self?.realmProvider else { return }
            self?.applyPendingRelationships(realmProvider: realmProvider)
        }
        
        completion(nil)
    }
    
    public func recordsToUpload(limit: Int) -> [CKRecord] {
        guard realmProvider != nil else { return [] }
        
        var recordsArray = [CKRecord]()
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            let recordLimit = limit == 0 ? Int.max : limit
            var uploadingState = SyncedEntityState.newOrChanged
            
            var innerLimit = recordLimit
            while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deleted.rawValue {
                recordsArray.append(contentsOf: self.recordsToUpload(withState: uploadingState, limit: innerLimit, realmProvider: self.realmProvider))
                uploadingState = self.nextStateToSync(after: uploadingState)
                innerLimit = recordLimit - recordsArray.count
            }
        }
        
        return recordsArray
    }
    
    public func didUpload(savedRecords: [CKRecord]) {
        guard realmProvider != nil else { return }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            self.realmProvider.persistenceRealm.beginWrite()
            for record in savedRecords {
                if let syncedEntity = self.realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: record.recordID.recordName) {
                    syncedEntity.state = SyncedEntityState.synced.rawValue
                    self.save(record: record, for: syncedEntity)
                }
                
            }
            try? self.realmProvider.persistenceRealm.commitWrite()
        }
    }
    
    public func recordIDsMarkedForDeletion(limit: Int) -> [CKRecord.ID] {
        guard realmProvider != nil else { return [] }
        
        var recordIDs = [CKRecord.ID]()
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            let predicate = NSPredicate(format: "state == %ld", SyncedEntityState.deleted.rawValue)
            let deletedEntities = self.realmProvider.persistenceRealm.objects(SyncedEntity.self).filter(predicate)
            
            for syncedEntity in deletedEntities {
                if recordIDs.count >= limit {
                    break
                }
                recordIDs.append(CKRecord.ID(recordName: syncedEntity.identifier, zoneID: self.zoneID))
            }
        }
        
        return recordIDs
    }
    
    public func didDelete(recordIDs deletedRecordIDs: [CKRecord.ID]) {
        guard realmProvider != nil else { return }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            self.realmProvider.persistenceRealm.beginWrite()
            for recordID in deletedRecordIDs {
                
                if let syncedEntity = self.realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName) {
                    if let record = syncedEntity.record {
                        self.realmProvider.persistenceRealm.delete(record)
                    }
                    self.realmProvider.persistenceRealm.delete(syncedEntity)
                }
            }
            try? self.realmProvider.persistenceRealm.commitWrite()
        }
    }
    
    public func hasRecordID(_ recordID: CKRecord.ID) -> Bool {
        guard realmProvider != nil else { return false }
        
        var hasRecord = false
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            let syncedEntity = self.realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName)
            hasRecord = syncedEntity != nil
        }
        return hasRecord
    }
    
    public func didFinishImport(with error: Error?) {
    
        guard realmProvider != nil else { return }
        
        tempFileManager.clearTempFiles()
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            self.updateHasChanges(realm: self.realmProvider.persistenceRealm)
        }
    }
    
    public func record(for object: AnyObject) -> CKRecord? {
        guard realmProvider != nil,
            let realmObject = object as? Object else {
            return nil
        }
        
        var record: CKRecord?
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            if let syncedEntity = self.syncedEntity(for: realmObject, realm: self.realmProvider.persistenceRealm) {
                var parent: SyncedEntity?
                record = self.recordToUpload(syncedEntity: syncedEntity, realmProvider: self.realmProvider, parentSyncedEntity: &parent)
            }
        }
        
        return record
    }
    
    public func share(for object: AnyObject) -> CKShare? {
        guard realmProvider != nil,
            let realmObject = object as? Object else {
            return nil
        }
        
        var share: CKShare?
        
        if let syncedEntity = syncedEntity(for: realmObject, realm: realmProvider.persistenceRealm) {
            share = getShare(for: syncedEntity)
        }
        
        return share
    }
    
    public func save(share: CKShare, for object: AnyObject) {
        guard realmProvider != nil,
            let realmObject = object as? Object else {
            return
        }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            if let syncedEntity = self.syncedEntity(for: realmObject, realm: self.realmProvider.persistenceRealm) {
                self.realmProvider.persistenceRealm.beginWrite()
                self.save(share: share, forSyncedEntity: syncedEntity, realmProvider: self.realmProvider)
                try? self.realmProvider.persistenceRealm.commitWrite()
            }
        }
    }
    
    public func deleteShare(for object: AnyObject) {
        guard realmProvider != nil,
            let realmObject = object as? Object else {
            return
        }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            if let syncedEntity = self.syncedEntity(for: realmObject, realm: self.realmProvider.persistenceRealm),
                let shareEntity = syncedEntity.share {
                
                self.realmProvider.persistenceRealm.beginWrite()
                syncedEntity.share = nil
                if let record = shareEntity.record {
                    self.realmProvider.persistenceRealm.delete(record)
                }
                self.realmProvider.persistenceRealm.delete(shareEntity)
                try? self.realmProvider.persistenceRealm.commitWrite()
            }
        }
    }
    
    public func deleteChangeTracking() {
        BackgroundWorker.shared.start { [weak self] in
            self?.invalidateRealmAndTokens()
        }
        
        let config = self.persistenceRealmConfiguration
        let realmFileURLs: [URL] = [config.fileURL,
                             config.fileURL?.appendingPathExtension("lock"),
                             config.fileURL?.appendingPathExtension("note"),
                             config.fileURL?.appendingPathExtension("management")
            ].compactMap { $0 }
        
        for url in realmFileURLs {
            do {
                try FileManager.default.removeItem(at: url)
            } catch {
                print("Error deleting file at \(url): \(error)")
            }
        }
    }
    
    public var recordZoneID: CKRecordZone.ID {
        return zoneID
    }
    
    public var serverChangeToken: CKServerChangeToken? {
        guard realmProvider != nil else { return nil }
        
        var token: CKServerChangeToken?
        let serverToken = realmProvider.persistenceRealm.objects(ServerToken.self).first
        if let tokenData = serverToken?.token {
            token = NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
        }
        return token
    }
    
    public func saveToken(_ token: CKServerChangeToken?) {
        guard realmProvider != nil else { return }
        
        BackgroundWorker.shared.start { [weak self] in
            guard let self = self else { return }
            var serverToken: ServerToken! = self.realmProvider.persistenceRealm.objects(ServerToken.self).first
            
            self.realmProvider.persistenceRealm.beginWrite()
            
            if serverToken == nil {
                serverToken = ServerToken()
                self.realmProvider.persistenceRealm.add(serverToken)
            }
            
            if let token = token {
                serverToken.token = NSKeyedArchiver.archivedData(withRootObject: token)
            } else {
                serverToken.token = nil
            }
            
            try? self.realmProvider.persistenceRealm.commitWrite()
        }
    }
}
