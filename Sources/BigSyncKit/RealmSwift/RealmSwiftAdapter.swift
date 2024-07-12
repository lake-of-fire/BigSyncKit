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
import Combine
import RealmSwiftGaps

@globalActor
public actor BigSyncBackgroundActor {
    public static var shared = BigSyncBackgroundActor()
    
    public init() { }
}

//extension Realm {
//    public func safeWrite(_ block: (() throws -> Void)) throws {
//        if isInWriteTransaction {
//            try block()
//        } else {
//            try write(block)
//        }
//    }
//}

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
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

struct SyncRealmProvider {
    let persistenceConfiguration: Realm.Configuration
    let targetConfiguration: Realm.Configuration
    
    var syncPersistenceRealm: Realm {
        get {
            return try! Realm(configuration: persistenceConfiguration)
        }
    }
    var syncTargetRealm: Realm {
        get {
            return try! Realm(configuration: targetConfiguration)
        }
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

actor RealmProvider {
    let persistenceConfiguration: Realm.Configuration
    let targetConfiguration: Realm.Configuration
    
    @BigSyncBackgroundActor
    let persistenceRealm: Realm
    @BigSyncBackgroundActor
    let targetReaderRealm: Realm
    @RealmBackgroundActor
    let targetWriterRealm: Realm
//    var persistenceRealm: Realm {
//        get async {
//            return try! await Realm(configuration: persistenceConfiguration, actor: BigSyncBackgroundActor.shared)
//        }
//    }
//    var targetRealm: Realm {
//        get async {
//            return try! await Realm(configuration: targetConfiguration, actor: BigSyncBackgroundActor.shared)
//        }
//    }
    
    @BigSyncBackgroundActor
    init?(persistenceConfiguration: Realm.Configuration, targetConfiguration: Realm.Configuration) async {
        self.persistenceConfiguration = persistenceConfiguration
        self.targetConfiguration = targetConfiguration
        
        do {
            persistenceRealm = try await Realm(configuration: persistenceConfiguration, actor: BigSyncBackgroundActor.shared)
            targetReaderRealm = try await Realm(configuration: targetConfiguration, actor: BigSyncBackgroundActor.shared)
            targetWriterRealm = try await Realm(configuration: targetConfiguration, actor: RealmBackgroundActor.shared)
        } catch {
            return nil
        }
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
//    static let shareRelationshipKey = "com.syncKit.shareRelationship"
    
    public let persistenceRealmConfiguration: Realm.Configuration
    public let targetRealmConfiguration: Realm.Configuration
    public let excludedClassNames: [String]
    public let zoneID: CKRecordZone.ID
    public var mergePolicy: MergePolicy = .server
    public weak var delegate: RealmSwiftAdapterDelegate?
    public weak var recordProcessingDelegate: RealmSwiftAdapterRecordProcessing?
    public var forceDataTypeInsteadOfAsset: Bool = false
    
    private lazy var tempFileManager: TempFileManager = {
        TempFileManager(identifier: "\(recordZoneID.ownerName).\(recordZoneID.zoneName).\(targetRealmConfiguration.fileURL?.lastPathComponent ?? UUID().uuidString).\(targetRealmConfiguration.schemaVersion)")
    }()
    
    var syncRealmProvider: SyncRealmProvider?
    var realmProvider: RealmProvider?
    
//    var collectionNotificationTokens = [NotificationToken]()
//    var collectionNotificationTokens = Set<AnyCancellable>()
    var pendingTrackingUpdates = [ObjectUpdate]()
    var childRelationships = [String: Array<ChildRelationship>]()
    var modelTypes = [String: Object.Type]()
    public private(set) var hasChanges = false
    
    private var cancellables = Set<AnyCancellable>()
    
    public init(persistenceRealmConfiguration: Realm.Configuration, targetRealmConfiguration: Realm.Configuration, excludedClassNames: [String], recordZoneID: CKRecordZone.ID) {
        
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfiguration = targetRealmConfiguration
        self.excludedClassNames = excludedClassNames
        self.zoneID = recordZoneID
        
        super.init()
        
//        Task.detached(priority: .utility) { [weak self] in
//        executeOnMainQueue {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self = self else { return }
//            autoreleasepool {
                setupTypeNamesLookup()
                await setup()
                await setupChildrenRelationshipsLookup()
//            }
        }
    }
    
    deinit {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self = self else { return }
            await invalidateRealmAndTokens()
        }
    }
    
    @BigSyncBackgroundActor
    func invalidateRealmAndTokens() async {
//        executeOnMainQueue {
//        DispatchQueue(label: "BigSyncKit").sync {
//            autoreleasepool {
                for cancellable in cancellables {
                    cancellable.cancel()
                }
                cancellables.removeAll()
//                for token in collectionNotificationTokens {
//                    token.invalidate()
//                    //                token.cancel()
//                }
//                collectionNotificationTokens.removeAll()
                
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
    
    @BigSyncBackgroundActor
    func setupTypeNamesLookup() {
        targetRealmConfiguration.objectTypes?.forEach { objectType in
            modelTypes[objectType.className()] = objectType as? Object.Type
        }
    }
    
    @BigSyncBackgroundActor
    func setup() async {
        syncRealmProvider = SyncRealmProvider(persistenceConfiguration: persistenceRealmConfiguration, targetConfiguration: targetRealmConfiguration)
        realmProvider = await RealmProvider(persistenceConfiguration: persistenceRealmConfiguration, targetConfiguration: targetRealmConfiguration)
        guard let realmProvider = realmProvider else { return }
        
        let needsInitialSetup = realmProvider.persistenceRealm.objects(SyncedEntity.self).count <= 0
        
        for schema in realmProvider.targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
            guard let objectClass = self.realmObjectClass(name: schema.className) else {
                continue
            }
            let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
            let results = realmProvider.targetReaderRealm.objects(objectClass)
            
            // Register for collection notifications
            results.changesetPublisher
                .freeze()
//                .receive(on: DispatchQueue(label: "BigSyncKit"))
                .threadSafeReference()
//                .debounce(for: 0.0001, scheduler: DispatchSerialQueue(label: "BigSyncKit.RealmSwiftAdapter"))
                .sink(receiveValue: { [weak self] collectionChange in
//                    let ref = ThreadSafeReference(to: collectionChange)
//                    Task { @MainActor [weak self] in
//                        guard let self = self, let collectionChange = realmProvider.targetRealm.resolve(ref) else { return }
                    Task { @BigSyncBackgroundActor [weak self] in
                        guard let self = self else { return }
                        switch collectionChange {
                        case .update(let results, _, let insertions, let modifications):
                            for index in insertions {
                                let object = results[index]
                                let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                                /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
                                 * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
                                 */
                                if object.realm!.isInWriteTransaction {
                                    self.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: .insertion))
                                } else {
                                    await self.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: true, modified: false, deleted: false, realmProvider: realmProvider)
                                    //                                       self.updateTracking(insertedObject: object, identifier: identifier, entityName: schema.className, provider: self.realmProvider)
                                }
                            }
                            
                            for index in modifications {
                                let object = results[index]
                                let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                                /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
                                 * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
                                 */
                                if object.realm!.isInWriteTransaction {
                                    self.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: .modification))
                                } else {
                                    await self.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: false, modified: true, deleted: false, realmProvider: realmProvider)
                                    //                                       self.updateTracking(insertedObject: object, identifier: identifier, entityName: schema.className, provider: self.realmProvider)
                                }
                            }
                        default: break
                        }
                    }
//                    }
                })
                .store(in: &cancellables)
//            collectionNotificationTokens.append(token)
            // Register for collection notifications
//            results.changesetPublisher
//                .threadSafeReference()
//                .freeze()
////                .receive(on: DispatchQueue(label: "BigSyncKit_RealmSwiftAdapter", qos: .utility))
////                .receive(on: DispatchQueue.main)
//                .sink { [weak self] collectionChange in
//                    switch collectionChange {
//                    case .update(let collection, _, let insertions, let modifications):
//                        // Deletions are covered via soft-delete (SyncedDeletable) under modifications.
//                        for chunk in insertions.filter({ $0 < collection.count }).chunked(into: 25) {
////                            try? realmProvider.persistenceRealm.writeAsync {
//                                for index in chunk {
//                                    let collection = collection.map { $0.thaw() }
//                                    guard let object = collection[index] else { return }
//                                    if (object as? (any SyncableObject))?.isDeleted ?? false {
//                                        return
//                                    }
//                                    let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
//                                    /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
//                                     * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
//                                     */
//                                    if object.realm!.isInWriteTransaction {
//                                        self?.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: .insertion))
//                                    } else {
//                                        guard let realmProvider = self?.realmProvider else { return }
//                                        self?.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: true, modified: false, deleted: false, realmProvider: realmProvider)
//                                    }
//                                }
////                            }
//                        }
//
////                        for chunk in modifications.filter({ $0 < collection.count }).chunked(into: 25) {
////                            guard let realmProvider = self?.realmProvider else { return }
////                            try? realmProvider.persistenceRealm.writeAsync {
////                                for index in chunk {
////                                    let collection = collection.map { $0.thaw() }
////                                    guard let object = collection[index] else { return }
////                                    let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
////                                    let isDeletion = (object as? (any SyncableObject))?.isDeleted ?? false
////                                    /* This can be called during a transaction, and it's illegal to add a notification block during a transaction,
////                                     * so we keep all the insertions in a list to be processed as soon as the realm finishes the current transaction
////                                     */
////                                    if object.realm!.isInWriteTransaction {
////                                        self?.pendingTrackingUpdates.append(ObjectUpdate(object: object, identifier: identifier, entityType: schema.className, updateType: isDeletion ? .deletion : .modification))
////                                    } else {
////                                        guard let realmProvider = self?.realmProvider else { return }
////                                        self?.updateTracking(objectIdentifier: identifier, entityName: schema.className, inserted: false, modified: !isDeletion, deleted: isDeletion, realmProvider: realmProvider)
////                                    }
////                                }
////                            }
////                        }
//
//                    default: break
//                    }
//                }
//                .store(in: &collectionNotificationTokens)
            
            if needsInitialSetup {
//                Task.detached(priority: .utility) { [weak self] in
//                    guard let self = self else { return }
//                    let realm = realmProvider.persistenceRealm
                let results = realmProvider.targetReaderRealm.objects(objectClass)
//                let identifiers = results.map { Self.getStringIdentifier(for: $0, usingPrimaryKey: primaryKey) }
                
//                realm.writeAsync {
//                    for identifier in identifiers {
                for result in Array(results) {
                    let identifier = Self.getStringIdentifier(for: result, usingPrimaryKey: primaryKey)
                        //                        autoreleasepool { [weak self] in
                        //                            try? realm.safeWrite {
                        //                                guard let identifier = getStringIdentifier(for: object, usingPrimaryKey: primaryKey) else { return }
                    await Self.createSyncedEntity(entityType: schema.className, identifier: identifier, getRealm: { realmProvider.persistenceRealm })
                        //                            }
                        //                        }
                    }
                    //                }
            }
        }
        
        //        let token = realmProvider.targetRealm.observe { (_, _) in
//        results.changesetPublisher
//            .subscribe(on: DispatchQueue(label: "BigSyncKit_RealmSwiftAdapter"))
//            .threadSafeReference()
//            .sink { [weak self] collectionChange in
//                //            let token = results.observe({ collectionChange in
//                Task.detached(priority: .utility) { [weak self] in
//
//            Task.detached(priority: .utility) { [weak self] in
//                await self?.enqueueObjectUpdates()
//            }
//        }
//        collectionNotificationTokens.append(token)
        
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
    
    func realmObjectClass(name: String) -> Object.Type? {
        return modelTypes[name]
    }
    
    @BigSyncBackgroundActor
    func updateHasChanges(realm: Realm) {
        let results = realm.objects(SyncedEntity.self).where { $0.state != SyncedEntityState.synced.rawValue }
        hasChanges = results.count > 0
    }
    
    @BigSyncBackgroundActor
    func setupChildrenRelationshipsLookup() async {
        childRelationships.removeAll()
        
        guard let realmProvider = realmProvider else { return }
        for objectSchema in realmProvider.targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(objectSchema.className) {
            guard let objectClass = self.realmObjectClass(name: objectSchema.className) else {
                continue
            }
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
    
//    func updateObjectTracking() async{
//        for update in pendingTrackingUpdates {
//            switch update.updateType {
//            case .insertion:
//                await self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: true, modified: false, deleted: false, realmProvider: realmProvider)
//            case .modification:
//                await self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: false, modified: true, deleted: false, realmProvider: realmProvider)
//            case .deletion:
//                await self.updateTracking(objectIdentifier: update.identifier, entityName: update.entityType, inserted: false, modified: false, deleted: true, realmProvider: realmProvider)
//            }
//        }
//
//        pendingTrackingUpdates.removeAll()
//    }
    
    @BigSyncBackgroundActor
    func updateTracking(objectIdentifier: String, entityName: String, inserted: Bool, modified: Bool, deleted: Bool, realmProvider: RealmProvider) async {
        let identifier = "\(entityName).\(objectIdentifier)"
        var isNewChange = false
        let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: realmProvider.persistenceRealm)
        
        if deleted {
            isNewChange = true
            
            if let syncedEntity = syncedEntity {
//                try? realmProvider.persistenceRealm.safeWrite {
                try? await realmProvider.persistenceRealm.asyncWrite {
                    syncedEntity.state = SyncedEntityState.deleted.rawValue
                }
            }
        } else if syncedEntity == nil {
            await Self.createSyncedEntity(entityType: entityName, identifier: objectIdentifier, getRealm: { realmProvider.persistenceRealm })
            
            if inserted {
                isNewChange = true
            }
        } else if !inserted {
            guard let syncedEntity = syncedEntity else {
                return
            }
            
            isNewChange = true
            
            if syncedEntity.state == SyncedEntityState.synced.rawValue && modified {
                // Hack to avoid crashing issue: https://github.com/realm/realm-swift/issues/8333
                realmProvider.persistenceRealm.refresh()
                if let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: realmProvider.persistenceRealm) {
//                    try? realmProvider.persistenceRealm.safeWrite {
                    try? await realmProvider.persistenceRealm.asyncWrite {
                        syncedEntity.state = SyncedEntityState.newOrChanged.rawValue
                        // If state was New (or Modified already) then leave it as that
                    }
                }
            }
        }
        
        let isNewChangeFinal = isNewChange
//        Task { @MainActor [weak self] in
//        executeOnMainQueue {
            if !hasChanges && isNewChangeFinal {
                hasChanges = true
                Task { @MainActor in
                    NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
                }
            }
//        }
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    static func createSyncedEntity(entityType: String, identifier: String, getRealm: () async -> Realm) async -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: entityType, identifier: "\(entityType).\(identifier)", state: SyncedEntityState.newOrChanged.rawValue)
        
        let realm = await getRealm()
        realm.refresh()
        try? await realm.asyncWrite {
            realm.add(syncedEntity, update: .modified)
        }
        return syncedEntity
    }
    
    @BigSyncBackgroundActor
    func createSyncedEntity(record: CKRecord, realmProvider: RealmProvider) async -> SyncedEntity? {
        let syncedEntity = SyncedEntity(entityType: record.recordType, identifier: record.recordID.recordName, state: SyncedEntityState.synced.rawValue)
        
        let persistenceRealm = realmProvider.persistenceRealm
        try? await realmProvider.persistenceRealm.asyncWrite {
            persistenceRealm.add(syncedEntity, update: .modified)
        }
        
        guard let objectClass = self.realmObjectClass(name: record.recordType) else {
            return nil
        }
        let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
         
        // If the row already exists somehow (for some reasons outside of syncing), merge changes instead of crashing.
        if let object = realmProvider.targetReaderRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier) {
            await applyChanges(in: record, to: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
//            saveShareRelationship(for: syncedEntity, record: record)
        } else {
            try? await Task(priority: .background) { @RealmBackgroundActor in
                let object = objectClass.init()
                object.setValue(objectIdentifier, forKey: primaryKey)
                let targetRealm = realmProvider.targetWriterRealm
                try? await targetRealm.asyncWrite {
                    targetRealm.add(object)
                }
//                debugPrint("createSyncedEntity added object", record.recordID, primaryKey, object.description)
            }.value
        }
        
        return syncedEntity
    }
    
    func getObjectIdentifier(for syncedEntity: SyncedEntity) -> Any {
        let range = syncedEntity.identifier.range(of: syncedEntity.entityType)!
        let index = syncedEntity.identifier.index(range.upperBound, offsetBy: 1)
        let objectIdentifier = String(syncedEntity.identifier[index...])
        let objectClass = realmObjectClass(name: syncedEntity.entityType)
        
        guard let objectSchema = objectClass?.sharedSchema(),
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
    
    @BigSyncBackgroundActor
    func getObjectIdentifier(stringObjectId: String, entityType: String) -> Any? {
        guard let schema = self.realmObjectClass(name: entityType)?.sharedSchema(),
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
    
    @BigSyncBackgroundActor
    func syncedEntity(for object: Object, realm: Realm) -> SyncedEntity? {
        guard let objectClass = self.realmObjectClass(name: object.objectSchema.className) else {
            return nil
        }
        let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
        let identifier = object.objectSchema.className + "." + Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
        return Self.getSyncedEntity(objectIdentifier: identifier, realm: realm)
    }
    
    static func getStringIdentifier(for object: Object, usingPrimaryKey key: String) -> String {
        let objectId = object.value(forKey: key)
        let identifier: String
        if let value = objectId as? CustomStringConvertible {
            identifier = String(describing: value)
        } else {
            identifier = objectId as! String
        }
//        guard identifier.count <= 255 else {
//            
//        }
        return identifier
    }
    
    @BigSyncBackgroundActor
    static func getSyncedEntity(objectIdentifier: String, realm: Realm) -> SyncedEntity? {
        return realm.object(ofType: SyncedEntity.self, forPrimaryKey: objectIdentifier)
    }
    
    @MainActor
    func shouldIgnore(key: String) -> Bool {
        return CloudKitSynchronizer.metadataKeys.contains(key)
    }
    
    @BigSyncBackgroundActor
    func applyChanges(in record: CKRecord, to object: Object, syncedEntity: SyncedEntity, realmProvider: RealmProvider) async {
        if syncedEntity.state == SyncedEntityState.newOrChanged.rawValue {
            if mergePolicy == .server {
                for property in object.objectSchema.properties {
                    if await shouldIgnore(key: property.name) {
                        continue
                    }
                    if property.type == PropertyType.linkingObjects {
                        continue
                    }
                    
                    await applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
                }
            } else if mergePolicy == .custom {
                var recordChanges = [String: Any]()
                
                for property in object.objectSchema.properties {
                    if property.type == PropertyType.linkingObjects {
                        continue
                    }
                    
                    if await !shouldIgnore(key: property.name) {
                        if let asset = record[property.name] as? CKAsset {
                            recordChanges[property.name] = asset.fileURL != nil ? NSData(contentsOf: asset.fileURL!) : NSNull()
                        } else {
                            recordChanges[property.name] = record[property.name] ?? NSNull()
                        }
                    }
                }
                
                let acceptRemoteChange: Bool
                if let delegate = delegate {
                    acceptRemoteChange = delegate.realmSwiftAdapter(self, gotChanges: recordChanges, object: object)
                } else {
                    // Default conflict resolution.
                    // Forkable into delegate.
                    acceptRemoteChange = { adapter, changes, object in
#if os(macOS)
                        guard adapter.hasRealmObjectClass(name: object.className) else { return false }
#else
                        guard adapter.hasRealmObjectClass(name: String(describing: type(of: object))) else { return false }
#endif
                        
                        if let remoteChangeAt = changes["modifiedAt"] as? Date, let localChangeAt = object.value(forKey: "modifiedAt") as? Date, remoteChangeAt <= localChangeAt {
                            return false
                        }
                        return true
                    }(self, recordChanges, object)
                }
                
                if acceptRemoteChange {
                    for property in object.objectSchema.properties {
                        if await shouldIgnore(key: property.name) {
                            continue
                        }
                        if property.type == PropertyType.linkingObjects {
                            continue
                        }
                        
                        await applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
                    }
                }
            }
        } else {
            for property in object.objectSchema.properties {
                if await shouldIgnore(key: property.name) {
                    continue
                }
                if property.type == PropertyType.linkingObjects {
                    continue
                }
                
                await applyChange(property: property, record: record, object: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
            }
        }
    }
    
    @BigSyncBackgroundActor
    func applyChange(property: Property, record: CKRecord, object: Object, syncedEntity: SyncedEntity, realmProvider: RealmProvider) async {
        let key = property.name
        if key == object.objectSchema.primaryKeyProperty!.name {
            return
        }
        
        if let recordProcessingDelegate = recordProcessingDelegate,
           !recordProcessingDelegate.shouldProcessPropertyInDownload(propertyName: key, object: object, record: record) {
            return
        }
        
        let value = record[key]
        
        // TODO: Compare existing value before writing new one.
        // List/Set support forked from IceCream: https://github.com/caiyue1993/IceCream/blob/master/IceCream/Classes/CKRecordRecoverable.swift
        var recordValue: Any?
        if property.isSet {
            switch property.type {
            case .int:
                guard let value = record.value(forKey: property.name) as? [Int] else { break }
                var set = Set<Int>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .string:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                var set = Set<String>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .bool:
                guard let value = record.value(forKey: property.name) as? [Bool] else { break }
                var set = Set<Bool>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .float:
                guard let value = record.value(forKey: property.name) as? [Float] else { break }
                var set = Set<Float>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .double:
                guard let value = record.value(forKey: property.name) as? [Double] else { break }
                var set = Set<Double>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .data:
                guard let value = record.value(forKey: property.name) as? [Data] else { break }
                var set = Set<Data>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .date:
                guard let value = record.value(forKey: property.name) as? [Date] else { break }
                var set = Set<Date>()
                value.forEach { set.insert($0) }
                recordValue = set
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        if let realm = self.realmProvider?.persistenceRealm {
                            await savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
                        }
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        if let realm = self.realmProvider?.persistenceRealm {
                            await savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
                        }
                    }
                }
            default:
                break
            }
            let ref = ThreadSafeReference(to: object)
            let recordValueToSet = recordValue
            try? await Task(priority: .background) { @RealmBackgroundActor in
                if let object = realmProvider.targetWriterRealm.resolve(ref) {
                    try? await realmProvider.targetWriterRealm.asyncWrite {
                        object.setValue(recordValueToSet, forKey: property.name)
                    }
                }
            }.value
        } else if property.isArray {
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
                        if let realm = self.realmProvider?.persistenceRealm {
                            await savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
                        }
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        if let realm = self.realmProvider?.persistenceRealm {
                            await savePendingRelationship(name: property.name, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
                        }
                    }
                }
            default:
                break
            }
            let ref = ThreadSafeReference(to: object)
            let recordValueToSet = recordValue
            try? await Task(priority: .background) { @RealmBackgroundActor in
                if let object = realmProvider.targetWriterRealm.resolve(ref) {
                    try? await realmProvider.targetWriterRealm.asyncWrite {
                        object.setValue(recordValueToSet, forKey: property.name)
                    }
                }
            }.value
        } else if let reference = value as? CKRecord.Reference {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            let recordName = reference.recordID.recordName
            let separatorRange = recordName.range(of: ".")!
            let objectIdentifier = String(recordName[separatorRange.upperBound...])
            if let realm = self.realmProvider?.persistenceRealm {
                await savePendingRelationship(name: key, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
            }
        } else if property.type == .object {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            guard let recordName = record.value(forKey: property.name) as? String else { return }
            let separatorRange = recordName.range(of: ".")!
            let objectIdentifier = String(recordName[separatorRange.upperBound...])
            if let realm = self.realmProvider?.persistenceRealm {
                await savePendingRelationship(name: key, syncedEntity: syncedEntity, targetIdentifier: objectIdentifier, realm: realm)
            }
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
                let data = NSData(contentsOf: fileURL) {
                
                let ref = ThreadSafeReference(to: object)
                let recordValueToSet = recordValue
                try? await Task(priority: .background) { @RealmBackgroundActor in
                    if let object = realmProvider.targetWriterRealm.resolve(ref) {
                        try? await realmProvider.targetWriterRealm.asyncWrite {
                            object.setValue(data, forKey: key)
                        }
                    }
                }.value
            }
        } else if value != nil || property.isOptional == true {
            // If property is not a relationship or value is nil and property is optional.
            // If value is nil and property is non-optional, it is ignored. This is something that could happen
            // when extending an object model with a new non-optional property, when an old record is applied to the object.
            let ref = ThreadSafeReference(to: object)
            let recordValueToSet = recordValue
            try? await Task(priority: .background) { @RealmBackgroundActor in
                if let object = realmProvider.targetWriterRealm.resolve(ref) {
                    try? await realmProvider.targetWriterRealm.asyncWrite {
                        object.setValue(value, forKey: key)
                    }
                }
            }.value
        }
    }
    
    @BigSyncBackgroundActor
    func savePendingRelationship(name: String, syncedEntity: SyncedEntity, targetIdentifier: String, realm: Realm) async {
//        realm.writeAsync {
        try? await realm.asyncWrite {
            let pendingRelationship = PendingRelationship()
            pendingRelationship.relationshipName = name
            pendingRelationship.forSyncedEntity = syncedEntity
            pendingRelationship.targetIdentifier = targetIdentifier
            realm.add(pendingRelationship)
        }
    }
    
//    func saveShareRelationship(for entity: SyncedEntity, record: CKRecord) {
//        if let share = record.share {
//            let relationship = PendingRelationship()
//            relationship.relationshipName = RealmSwiftAdapter.shareRelationshipKey
//            relationship.targetIdentifier = share.recordID.recordName
//            relationship.forSyncedEntity = entity
//            entity.realm?.add(relationship)
//        }
//    }
    
    @BigSyncBackgroundActor
    func applyPendingRelationships(realmProvider: RealmProvider) async {
        let pendingRelationships = realmProvider.persistenceRealm.objects(PendingRelationship.self)
        
        if pendingRelationships.count == 0 {
            return
        }
        
//        realmProvider.persistenceRealm.beginWrite()
//        realmProvider.targetRealm.beginWrite()
//        for relationship in Array(pendingRelationships) {
        for relationship in Array(pendingRelationships) {
            let entity = relationship.forSyncedEntity
            
            guard let syncedEntity = entity,
                syncedEntity.entityState != .deleted else { continue }
            
            guard let originObjectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                continue
            }
            let objectIdentifier = getObjectIdentifier(for: syncedEntity)
            guard let originObject = realmProvider.targetReaderRealm.object(ofType: originObjectClass, forPrimaryKey: objectIdentifier) else { continue }
            
//            if relationship.relationshipName == RealmSwiftAdapter.shareRelationshipKey {
//                let persistenceRealm = realmProvider.persistenceRealm
//                try? await realmProvider.targetRealm.asyncWrite {
//                    syncedEntity.share = Self.getSyncedEntity(objectIdentifier: relationship.targetIdentifier, realm: persistenceRealm)
//                    persistenceRealm.delete(relationship)
//                }
//                continue
//            }
            
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
            
            guard let targetObjectClass = realmObjectClass(name: className) else { continue }
            let targetObjectIdentifier = getObjectIdentifier(stringObjectId: relationship.targetIdentifier, entityType: className)
            
            let relationshipName = relationship.relationshipName
            let originRef = ThreadSafeReference(to: originObject)
            let targetExisted = try? await Task(priority: .background) { @RealmBackgroundActor in
                guard let relationshipName = relationshipName else {
                    return false
                }
                
                let targetObject = realmProvider.targetWriterRealm.object(ofType: targetObjectClass, forPrimaryKey: targetObjectIdentifier)
                
                guard let target = targetObject else {
                    return false
                }
                if let originObject = realmProvider.targetWriterRealm.resolve(originRef) {
                    try await realmProvider.targetWriterRealm.asyncWrite {
                        originObject.setValue(target, forKey: relationshipName)
                    }
                }
                return true
            }.value
            if !(targetExisted ?? false) {
                continue
            }
            
            let persistenceRealm = realmProvider.persistenceRealm
            try? await realmProvider.persistenceRealm.asyncWrite {
                persistenceRealm.delete(relationship)
            }
        }
        
//        try? realmProvider.persistenceRealm.commitWrite()
//        commitTargetWriteTransactionWithoutNotifying()
//        try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        debugPrint("Finished applying pending relationships")
//        let pendingRelationships = realmProvider.persistenceRealm.objects(PendingRelationship.self)
//
//        if pendingRelationships.count == 0 {
//            completion()
//            return
//        }
//
//        realmProvider.persistenceRealm.beginWrite()
//        realmProvider.targetRealm.beginWrite()
//        for relationship in pendingRelationships {
//            print("pending rel... \(relationship.description)")
//            let entity = relationship.forSyncedEntity
//
//            guard let syncedEntity = entity,
//                syncedEntity.entityState != .deleted else { continue }
//
//            guard let originObjectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
//                continue
//            }
//
//            let objectIdentifier = getObjectIdentifier(for: syncedEntity)
//            guard let relationshipName = relationship.relationshipName else { continue }
//            guard let targetIdentifier = relationship.targetIdentifier else { continue }
//            let targetRealm = realmProvider.targetRealm
////            targetRealm.writeAsync { [weak self] in
////                guard let self = self else { return }
//                guard let originObject = targetRealm.object(ofType: originObjectClass, forPrimaryKey: objectIdentifier) else {
////                    continue
//                    return
//                }
//
//                //            if relationship.relationshipName == RealmSwiftAdapter.shareRelationshipKey {
//                //                syncedEntity.share = Self.getSyncedEntity(objectIdentifier: relationship.targetIdentifier, realm: realmProvider.persistenceRealm)
//                //                    realmProvider.persistenceRealm.delete(relationship)
//                //                continue;
//                //            }
//
//                var targetClassName: String?
//                for property in originObject.objectSchema.properties {
//                    if property.name == relationshipName {
//                        targetClassName = property.objectClassName
//                        break
//                    }
//                }
//
//                guard let className = targetClassName else {
////                    continue
//                    return
//                }
//
//                guard let targetObjectClass = self.realmObjectClass(name: className) else {
////                    continue
//                    return
//                }
//                let targetObjectIdentifier = self.getObjectIdentifier(stringObjectId: targetIdentifier, entityType: className)
//                let targetObject = targetRealm.object(ofType: targetObjectClass, forPrimaryKey: targetObjectIdentifier)
//
//                guard let target = targetObject else {
//                    //                    continue
//                    return
//                }
//
//                originObject.setValue(target, forKey: relationshipName)
//            } onComplete: { error in
//                guard error == nil else {
//                    print("Failed to set pending relationship")
//                    completion()
//                    return
//                }
//                let persistenceRealm = realmProvider.persistenceRealm
//                persistenceRealm.writeAsync {
//                    persistenceRealm.delete(relationship)
//                }
//                debugPrint("Finished applying pending relationships")
//                completion()
//            }
//        }
//
////        try? realmProvider.persistenceRealm.commitWrite()
////        commitTargetWriteTransactionWithoutNotifying()
////        try? realmProvider.targetRealm.commitWrite()
    }
    
    @BigSyncBackgroundActor
    func save(record: CKRecord, for syncedEntity: SyncedEntity) {
        if syncedEntity.record == nil {
            syncedEntity.record = Record()
        }
        
        syncedEntity.record!.encodedRecord = encodedRecord(record, onlySystemFields: true)
    }
    
    @BigSyncBackgroundActor
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
    
    func nextStateToSync(after state: SyncedEntityState) -> SyncedEntityState {
        return SyncedEntityState(rawValue: state.rawValue + 1)!
    }
    
    func recordsToUpload(withState state: SyncedEntityState, limit: Int, syncRealmProvider: SyncRealmProvider) -> [CKRecord] {
        let results = syncRealmProvider.syncPersistenceRealm.objects(SyncedEntity.self).where { $0.state == state.rawValue }
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
                guard let record = recordToUpload(syncedEntity: entity, syncRealmProvider: syncRealmProvider, parentSyncedEntity: &parentEntity) else {
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
    
    func recordToUpload(syncedEntity: SyncedEntity, syncRealmProvider: SyncRealmProvider, parentSyncedEntity: inout SyncedEntity?) -> CKRecord? {
        let record = getRecord(for: syncedEntity) ?? CKRecord(recordType: syncedEntity.entityType, recordID: CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        
        guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
            return nil
        }
        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
        let object = syncRealmProvider.syncTargetRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
        let entityState = syncedEntity.state
        
        guard let object = object else {
            // Object does not exist, but tracking syncedEntity thinks it does.
            // We mark it as deleted so the iCloud record will get deleted too
            syncRealmProvider.syncPersistenceRealm.writeAsync {
                syncedEntity.entityState = .deleted
            }
            return nil
        }
        
//        let changedKeys = (syncedEntity.changedKeys ?? "").components(separatedBy: ",")
        
//        var parentKey: String?
//        if let childObject = object as? ParentKey {
//            parentKey = type(of: childObject).parentKey()
//        }
        
        for property in object.objectSchema.properties {
            if entityState == SyncedEntityState.newOrChanged.rawValue {
                if let recordProcessingDelegate = recordProcessingDelegate,
                   !recordProcessingDelegate.shouldProcessPropertyBeforeUpload(propertyName: property.name, object: object, record: record) {
                    continue
                }
                
                if property.type == PropertyType.object {
                    if let target = object.value(forKey: property.name) as? Object {
                        let targetPrimaryKey = (type(of: target).primaryKey() ?? target.objectSchema.primaryKeyProperty?.name)!
                        let targetIdentifier = Self.getStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
                        let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                        let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                        record[property.name] = recordID.recordName as CKRecordValue
                    }
                } else if property.isSet {
                    let value = object.value(forKey: property.name)
                    switch property.type {
                    case .object:
                        /// We may get MutableSet<Cat> here
                        /// The item cannot be casted as MutableSet<Object>
                        /// It can be casted at a low-level type `SetBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let set = value as? RLMSwiftCollectionBase, set._rlmCollection.count > 0 else { break }
                        var referenceArray = [String]()
                        let wrappedSet = set._rlmCollection
                        for index in 0..<wrappedSet.count {
                            guard let object = wrappedSet[index] as? Object, let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            #warning("Confirm here that isDeleted is false before referencing, as icecream does (link above)")
                            let targetIdentifier = Self.getStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray as CKRecordValue
                    case .int:
                        guard let set = value as? Set<Int>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .string:
                        guard let set = value as? Set<String>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .bool:
                        guard let set = value as? Set<Bool>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .float:
                        guard let set = value as? Set<Float>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .double:
                        guard let set = value as? Set<Double>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .data:
                        guard let set = value as? Set<Data>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .date:
                        guard let set = value as? Set<Date>, !set.isEmpty else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of Set is not supported yet
                        break
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
                            let targetIdentifier = Self.getStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
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
                        forceDataTypeInsteadOfAsset == false {
                        
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
        Task { @RealmBackgroundActor in
//        DispatchQueue(label: "BigSyncKit").sync {
//            autoreleasepool {
                //        DispatchQueue(label: "RealmSwiftAadapter.cleanUp").async { [weak self] in
                //            autoreleasepool {
                //        guard let self = self else { return }
                guard let realmProvider = realmProvider else { return }
                for schema in realmProvider.targetWriterRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                    guard let objectClass = self.realmObjectClass(name: schema.className) else {
                        continue
                    }
                    guard objectClass.self is any SyncableObject.Type else {
                        continue
                    }
                    
                    let results = realmProvider.targetWriterRealm.objects(objectClass).filter { ($0 as? (any SyncableObject))?.isDeleted ?? false }
                    if results.isEmpty {
                        continue
                    }
//                    realmProvider.targetRealm.beginWrite()
//                                realmProvider.targetRealm.writeAsync {
                    let targetRealm = realmProvider.targetWriterRealm
                    try? await targetRealm.asyncWrite {
                        results.forEach({ targetRealm.delete($0) })
                    }
                        //                    commitTargetWriteTransactionWithoutNotifying()
                        //                    try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
                    //            }
                }
//            }
        }
        //            }
        //        }
    }
    
    // MARK: - Children records
    
    @BigSyncBackgroundActor
    func childrenRecords(for syncedEntity: SyncedEntity) async -> [CKRecord] {
        var records = [CKRecord]()
        var parent: SyncedEntity?
        
        guard let syncRealmProvider = syncRealmProvider, let realmProvider = realmProvider else { return [] }
        guard let record = recordToUpload(syncedEntity: syncedEntity, syncRealmProvider: syncRealmProvider, parentSyncedEntity: &parent) else {
            return []
        }
        records.append(record)
        
        if let relationships = childRelationships[syncedEntity.entityType] {
            for relationship in relationships {
                let objectID = getObjectIdentifier(for: syncedEntity)
                guard let objectClass = realmObjectClass(name: syncedEntity.entityType) else {
                    continue
                }
                if let object = realmProvider.targetReaderRealm.object(ofType: (objectClass as Object.Type).self, forPrimaryKey: objectID) {
                    // Get children
                    guard let childObjectClass = realmObjectClass(name: relationship.childEntityName) else {
                        continue
                    }
                    let predicate = NSPredicate(format: "%K == %@", relationship.childParentKey, object)
                    let children = realmProvider.targetReaderRealm.objects(childObjectClass.self).filter(predicate)
                    
                    for child in children {
                        if let childEntity = self.syncedEntity(for: child, realm: realmProvider.persistenceRealm) {
                            await records.append(contentsOf: childrenRecords(for: childEntity))
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
    
    @BigSyncBackgroundActor
    public func saveChanges(in records: [CKRecord]) async {
        guard let realmProvider = realmProvider else { return }
        guard records.count != 0 else {
            return
        }
        
        //        executeOnMainQueue {
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        //                await realmProvider.persistenceRealm.beginWrite()
        //                await realmProvider.targetRealm.beginWrite()
        
        //        DispatchQueue(label: "RealmSwiftAadapter.saveChanges").async { [weak self] in
        //            autoreleasepool {
        //        guard let self = self else { return }
        //            realmProvider.persistenceRealm.beginWrite()
        //            realmProvider.targetRealm.beginWrite()
        
        //            for chunk in records.chunked(into: 2000) {
        //                DispatchQueue(label: "RealmSwiftAadapter.saveChanges").async { [weak self] in
        //                    autoreleasepool { [weak self] in
        //            let persistenceRealm = realmProvider.persistenceRealm
        //            try! persistenceRealm.write {
        //            persistenceRealm.writeAsync {
        for record in records {
//            debugPrint("save changes to record \(record.description)")
            //                            realmProvider.persistenceRealm.beginWrite()
            //                            realmProvider.targetRealm.beginWrite()
            //                            guard let self = self else { return }
            var syncedEntity: SyncedEntity? = Self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: realmProvider.persistenceRealm)
            if syncedEntity == nil {
                //                                if #available(iOS 10.0, *) {
                //                                    if let share = record as? CKShare {
                //                                        syncedEntity = self.createSyncedEntity(for: share, realmProvider: realmProvider)
                //                                    } else {
                //                                        syncedEntity = self.createSyncedEntity(record: record, realmProvider: realmProvider)
                //                                    }
                //                                } else {
                //                        realmProvider.targetRealm.beginWrite()
                syncedEntity = await self.createSyncedEntity(record: record, realmProvider: realmProvider)
                //                        try? realmProvider.targetRealm.commitWrite()
                //                                }
            }
            
            if let syncedEntity = syncedEntity {
                if syncedEntity.entityState != .deleted && syncedEntity.entityType != "CKShare" {
                    guard let objectClass = self.realmObjectClass(name: record.recordType) else {
                        continue
                        //                            return
                    }
                    let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                    guard let object = realmProvider.targetReaderRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier) else {
                        continue
                        //                            return
                    }
                    
                    //                        realmProvider.targetRealm.writeAsync {
                    
                    await self.applyChanges(in: record, to: object, syncedEntity: syncedEntity, realmProvider: realmProvider)
                    //                        } onComplete: { error in
                    //                            }
                }
            } else {
                // Can happen when iCloud has records for a model that no longer exists locally.
                continue
            }
            //                                self.saveShareRelationship(for: syncedEntity, record: record)
            
            // Refresh to avoid invalidated crash
            if let syncedEntity = Self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: realmProvider.persistenceRealm) {
                try? await realmProvider.persistenceRealm.asyncWrite {
                    self.save(record: record, for: syncedEntity)
                }
            }
            // Order is important here. Notifications might be delivered after targetRealm is saved and
            // it's convenient if the persistenceRealm is not in a write transaction
            //                            try? realmProvider.persistenceRealm.commitWrite()
            //                            try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        }
        //            } onComplete: { error in
        //                completion()
        //            }
        //                try? realmProvider.persistenceRealm.commitWrite()
        //                try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        //            }
        //                    }
        //                }
        //            }
        // Order is important here. Notifications might be delivered after targetRealm is saved and
        // it's convenient if the persistenceRealm is not in a write transaction
        //            try? realmProvider.persistenceRealm.commitWrite()
        //                self.commitTargetWriteTransactionWithoutNotifying()
        //            try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        //            }
        //        }
    }
    
    @BigSyncBackgroundActor
    public func deleteRecords(with recordIDs: [CKRecord.ID]) async {
        guard let realmProvider = realmProvider else { return }
        guard recordIDs.count != 0 else { return }
        debugPrint("Deleting records with record ids \(recordIDs.map { $0.recordName })")
        
        //        executeOnMainQueue {
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        //        DispatchQueue(label: "RealmSwiftAadapter.deleteRecords").async { [weak self] in
        //            autoreleasepool {
        //        guard let self = self else { return }
        //                realmProvider.persistenceRealm.beginWrite()
        //                realmProvider.targetRealm.beginWrite()
        
        //            for chunk in recordIDs.chunked(into: 2000) {
        //                DispatchQueue(label: "RealmSwiftAadapter.deleteRecords").async { [weak self] in
        //                    autoreleasepool { [weak self] in
        for recordID in recordIDs {
            //                            realmProvider.persistenceRealm.beginWrite()
            //                            realmProvider.targetRealm.beginWrite()
            if let syncedEntity = Self.getSyncedEntity(objectIdentifier: recordID.recordName, realm: realmProvider.persistenceRealm) {
                
                if syncedEntity.entityType != "CKShare" {
                    guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                        //                                    continue
                        return
                    }
                    let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                    
                    try? await Task(priority: .background) { @RealmBackgroundActor in
                        let object = realmProvider.targetWriterRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
                        
                        if let object = object {
                            let targetRealm = realmProvider.targetWriterRealm
                            try? await realmProvider.targetWriterRealm.asyncWrite {
                                targetRealm.delete(object)
                            }
                        }
                    }.value
                }
                
                if let record = syncedEntity.record {
                    let persistenceRealm = realmProvider.persistenceRealm
                    try? await realmProvider.persistenceRealm.asyncWrite {
                        persistenceRealm.delete(record);
                    }
                }
                
                let persistenceRealm = realmProvider.persistenceRealm
                try? await realmProvider.persistenceRealm.asyncWrite {
                    persistenceRealm.delete(syncedEntity)
                }
            }
            //                            try? realmProvider.persistenceRealm.commitWrite()
            //                            try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        }
        //                    }
        //                }
        //            }
        //                try? realmProvider.persistenceRealm.commitWrite()
        //                self.commitTargetWriteTransactionWithoutNotifying()
        //                try? realmProvider.targetRealm.commitWrite() // No need to use withoutNotifying
        //            }
        //            }
    }
    
    @BigSyncBackgroundActor
    public func persistImportedChanges(completion: @escaping ((Error?) async -> Void)) async {
        guard let realmProvider = realmProvider else {
            await completion(nil)
            return
        }
        
        //        executeOnMainQueue {
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        await applyPendingRelationships(realmProvider: realmProvider)
        await completion(nil)
        //            }
    }
    
    public func recordsToUpload(limit: Int) -> [CKRecord] {
        guard let syncRealmProvider = syncRealmProvider else { return [] }
        
        var recordsArray = [CKRecord]()
        
//        executeOnMainQueue {
//        DispatchQueue(label: "BigSyncKit").sync {
//            autoreleasepool {
                //        DispatchQueue(label: "recordsToUpload").async { [weak self] in
                //            autoreleasepool { // Silence notifications on writes in thread
                //                guard let self = self else { return }
                let recordLimit = limit == 0 ? Int.max : limit
                var uploadingState = SyncedEntityState.newOrChanged
                
                var innerLimit = recordLimit
                while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deleted.rawValue {
                    recordsArray.append(contentsOf: self.recordsToUpload(withState: uploadingState, limit: innerLimit, syncRealmProvider: syncRealmProvider))
                    uploadingState = self.nextStateToSync(after: uploadingState)
                    innerLimit = recordLimit - recordsArray.count
                }
//            }
//        }
        
        return recordsArray
    }
    
    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord]) async {
        guard let realmProvider = realmProvider else { return }
        
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        //                realmProvider.persistenceRealm.beginWrite()
        for record in savedRecords {
            //            executeOnMainQueue {
            //            DispatchQueue(label: "didUpload").async { [weak self] in
            //                autoreleasepool { // Silence notifications on writes in thread
            //        guard let self = self else { return }
            //                    try! realmProvider.persistenceRealm.safeWrite {
            //            for record in savedRecords {
            if let syncedEntity = realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: record.recordID.recordName) {
                try? await realmProvider.persistenceRealm.asyncWrite {
                    syncedEntity.state = SyncedEntityState.synced.rawValue
                    self.save(record: record, for: syncedEntity)
                }
            }
            //                            }
            //                    }
        }
        //            }
        //                try? realmProvider.persistenceRealm.commitWrite()
        //            }
    }
    
    public func recordIDsMarkedForDeletion(limit: Int) -> [CKRecord.ID] {
        guard let syncRealmProvider = syncRealmProvider else { return [] }
        
        var recordIDs = [CKRecord.ID]()
        
//        DispatchQueue(label: "BigSyncKit").sync {
//            autoreleasepool {
                //        executeOnMainQueue {
                //        DispatchQueue(label: "recordIDsMarkedForDeletion").async { [weak self] in
                //            autoreleasepool { // Silence notifications on writes in thread
                //                guard let self = self else { return }
                let deletedEntities = syncRealmProvider.syncPersistenceRealm.objects(SyncedEntity.self).where { $0.state == SyncedEntityState.deleted.rawValue }
                
                for syncedEntity in Array(deletedEntities) {
                    if recordIDs.count >= limit {
                        break
                    }
                    recordIDs.append(CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
                }
                //            }
//            }
//        }
        
        return recordIDs
    }
    
    @BigSyncBackgroundActor
    public func didDelete(recordIDs deletedRecordIDs: [CKRecord.ID]) async {
        guard let realmProvider = realmProvider else { return }
        
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        //        executeOnMainQueue {
        //            autoreleasepool { // Silence notifications on writes in thread
        //        guard let self = self else { return }
        //                realmProvider.persistenceRealm.beginWrite()
        //            realmProvider.persistenceRealm.writeAsync {
        for recordID in deletedRecordIDs {
            if let syncedEntity = realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName) {
                let persistenceRealm = realmProvider.persistenceRealm
                try? await realmProvider.persistenceRealm.asyncWrite {
                    if let record = syncedEntity.record {
                        persistenceRealm.delete(record)
                    }
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
        //                try? realmProvider.persistenceRealm.commitWrite()
        //            }
        //            }
    }
    
//    @BigSyncBackgroundActor
//    public func hasRecordID(_ recordID: CKRecord.ID) async -> Bool {
//        guard let realmProvider = realmProvider else { return false }
//        
//        var hasRecord = false
////        executeOnMainQueue {
////        DispatchQueue(label: "BigSyncKit").sync {
////            autoreleasepool {
//                //        DispatchQueue(label: "hasRecordID").async { [weak self] in
//                //            autoreleasepool { // Silence notifications on writes in thread
//                //                guard let self = self else { return }
//                let syncedEntity = await realmProvider.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName)
//                hasRecord = syncedEntity != nil
//                //            }
////            }
////        }
//        return hasRecord
//    }
    
    @BigSyncBackgroundActor
    public func didFinishImport(with error: Error?) async {
        guard realmProvider != nil else { return }
        
        
//        DispatchQueue(label: "BigSyncKit").sync {
//            autoreleasepool {
                //        executeOnMainQueue {
                //            autoreleasepool { // Silence notifications on writes in thread
                //                guard let self = self else { return }
        tempFileManager.clearTempFiles()
        guard let realmProvider = self.realmProvider else { return }
        self.updateHasChanges(realm: realmProvider.persistenceRealm)
//            }
//        }
    }
    
//    public func record(for object: AnyObject) async -> CKRecord? {
//        guard let realmProvider = realmProvider else { return nil }
//        guard let realmObject = object as? Object else { return nil }
//        
//        var record: CKRecord?
////        DispatchQueue(label: "BigSyncKit").sync {
////            autoreleasepool {
//                //        executeOnMainQueue {
//                //        DispatchQueue(label: "recordForObject").async { [weak self] in
//                //            autoreleasepool { // Silence notifications on writes in thread
//                //                guard let self = self else { return }
//        if let syncedEntity = await syncedEntity(for: realmObject, realm: realmProvider.persistenceRealm) {
//            var parent: SyncedEntity?
//            record = await recordToUpload(syncedEntity: syncedEntity, realmProvider: realmProvider, parentSyncedEntity: &parent)
//        }
////            }
////        }
//        
//        return record
//    }
    
    @BigSyncBackgroundActor
    public func deleteChangeTracking() async {
        await invalidateRealmAndTokens()
        
        let config = self.persistenceRealmConfiguration
        let realmFileURLs: [URL] = [config.fileURL,
                                    config.fileURL?.appendingPathExtension("lock"),
                                    config.fileURL?.appendingPathExtension("note"),
                                    config.fileURL?.appendingPathExtension("management")
        ].compactMap { $0 }
        
        for url in realmFileURLs where FileManager.default.fileExists(atPath: url.path) {
            do {
                try FileManager.default.removeItem(at: url)
            } catch {
                print("Error deleting file at \(url): \(error)")
            }
        }
    }
    
//    @BigSyncBackgroundActor
//    @MainActor
    public var recordZoneID: CKRecordZone.ID {
        return zoneID
    }
    
    public var serverChangeToken: CKServerChangeToken? {
        get {
            guard let syncRealmProvider = syncRealmProvider else { return nil }
            
            var token: CKServerChangeToken?
            //        DispatchQueue(label: "BigSyncKit").sync {
            //            autoreleasepool {
            //        executeOnMainQueue {
            let serverToken = syncRealmProvider.syncPersistenceRealm.objects(ServerToken.self).first
            if let tokenData = serverToken?.token {
                token = NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
            }
            //            }
            //        }
            return token
        }
    }
    
    @BigSyncBackgroundActor
    public func saveToken(_ token: CKServerChangeToken?) async {
        guard let realmProvider = realmProvider else { return }
        //        executeOnMainQueue {
        //        DispatchQueue(label: "BigSyncKit").sync {
        //            autoreleasepool {
        //        DispatchQueue(label: "saveTokenCKServerChangeToken").async { [weak self] in
        //            autoreleasepool { // Silence notifications on writes in thread
        //        guard let self = self else { return }
        var serverToken: ServerToken! = realmProvider.persistenceRealm.objects(ServerToken.self).first
        
        //                realmProvider.persistenceRealm.beginWrite()
        //            try! realmProvider.persistenceRealm.write {
        //            realmProvider.persistenceRealm.writeAsync {
        let persistenceRealm = realmProvider.persistenceRealm
        try? await realmProvider.persistenceRealm.asyncWrite {
            if serverToken == nil {
                serverToken = ServerToken()
                persistenceRealm.add(serverToken)
            }
            
            if let token = token {
                serverToken.token = NSKeyedArchiver.archivedData(withRootObject: token)
            } else {
                serverToken.token = nil
            }
        }
        //            }
        
        //                try? realmProvider.persistenceRealm.commitWrite()
        //            }
        //        }
    }
}
