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
import Algorithms

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

fileprivate struct PendingRelationshipRequest {
    let name: String
    let syncedEntityID: String
    let targetIdentifier: String
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
    var persistenceRealm: Realm? {
        get {
            do {
                try Task.checkCancellation()
            } catch {
                return nil
            }
            return persistenceRealmObject
        }
    }
    @BigSyncBackgroundActor
    var targetReaderRealm: Realm? {
        get {
            do {
                try Task.checkCancellation()
            } catch {
                return nil
            }
            return targetReaderRealmObject
        }
    }
    @RealmBackgroundActor
    var targetWriterRealm: Realm? {
        get {
            do {
                try Task.checkCancellation()
            } catch {
                return nil
            }
            return targetWriterRealmObject
        }
    }
    
    @BigSyncBackgroundActor
    let persistenceRealmObject: Realm
    @BigSyncBackgroundActor
    let targetReaderRealmObject: Realm
    @RealmBackgroundActor
    let targetWriterRealmObject: Realm
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
            persistenceRealmObject = try await Realm(configuration: persistenceConfiguration, actor: BigSyncBackgroundActor.shared)
            targetReaderRealmObject = try await Realm(configuration: targetConfiguration, actor: BigSyncBackgroundActor.shared)
            targetWriterRealmObject = try await Realm(configuration: targetConfiguration, actor: RealmBackgroundActor.shared)
        } catch {
            return nil
        }
    }
}

struct ResultsChangeSet {
    var insertions: [String: Set<String>] = [:] // schemaName -> Set of insertions
    var modifications: [String: Set<String>] = [:] // schemaName -> Set of modifications
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
    
    public var beforeInitialSetup: (() -> Void)?
    
    private lazy var tempFileManager: TempFileManager = {
        TempFileManager(identifier: "\(recordZoneID.ownerName).\(recordZoneID.zoneName).\(targetRealmConfiguration.fileURL?.lastPathComponent ?? UUID().uuidString).\(targetRealmConfiguration.schemaVersion)")
    }()
    
    var syncRealmProvider: SyncRealmProvider?
    var realmProvider: RealmProvider?
    
    //    var collectionNotificationTokens = [NotificationToken]()
    //    var collectionNotificationTokens = Set<AnyCancellable>()
    //    var pendingTrackingUpdates = [ObjectUpdate]()
    var childRelationships = [String: Array<ChildRelationship>]()
    var modelTypes = [String: Object.Type]()
    public private(set) var hasChanges = false
    
    private var resultsChangeSet = ResultsChangeSet()
    private let resultsChangeSetPublisher = PassthroughSubject<Void, Never>()
    
    private var pendingRelationshipQueue = [PendingRelationshipRequest]()
    
    private var cancellables = Set<AnyCancellable>()
    
    public init(persistenceRealmConfiguration: Realm.Configuration, targetRealmConfiguration: Realm.Configuration, excludedClassNames: [String], recordZoneID: CKRecordZone.ID) {
        
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfiguration = targetRealmConfiguration
        self.excludedClassNames = excludedClassNames
        self.zoneID = recordZoneID
        
        super.init()
        
        //        Task.detached(priority: .utility) { [weak self] in
        //        executeOnMainQueue {
        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self = self else { return }
            //            autoreleasepool {
            setupTypeNamesLookup()
            await setup()
            await setupPublisherDebouncer()
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
    
    public func resetSyncCaches() {
        Task { @BigSyncBackgroundActor in
            if let persistenceRealm = realmProvider?.persistenceRealm {
                try await persistenceRealm.asyncWrite {
                    persistenceRealm.delete(persistenceRealm.objects(SyncedEntity.self))
                }
            }
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
        
        realmProvider?.persistenceRealm?.invalidate()
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
        
        guard let syncEmpty = realmProvider.persistenceRealm?.objects(SyncedEntity.self).isEmpty else { return }
        let needsInitialSetup = syncEmpty
        
        guard let targetReaderRealm = realmProvider.targetReaderRealm else { return }
        for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
            guard let objectClass = self.realmObjectClass(name: schema.className) else {
                continue
            }
            guard objectClass.conforms(to: SoftDeletable.self) else {
                fatalError("\(objectClass.className()) must conform to SoftDeletable in order to sync")
            }
            
            let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
            guard let results = realmProvider.targetReaderRealm?.objects(objectClass) else { return }
            
            // Register for collection notifications
            results.changesetPublisher
                .freeze()
                .threadSafeReference()
                .sink(receiveValue: { [weak self] collectionChange in
                    guard let self = self else { return }
                    switch collectionChange {
                    case .update(let results, _, let insertions, let modifications):
                        for index in insertions {
                            let object = results[index]
                            let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                            self.resultsChangeSet.insertions[schema.className, default: []].insert(identifier)
                        }
//                        if !insertions.isEmpty {
//                            debugPrint("!! INS RECS", insertions.compactMap { results[$0].description.prefix(50) })
//                        }

                        for index in modifications {
                            let object = results[index]
                            let identifier = Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                            self.resultsChangeSet.modifications[schema.className, default: []].insert(identifier)
                        }
//                        if !modifications.isEmpty {
//                            debugPrint("!! MODIFY RECS", modifications.compactMap { results[$0].description.prefix(50) })
//                        }
                        
                        self.resultsChangeSetPublisher.send(())
                    default: break
                    }
                })
                .store(in: &cancellables)
            
            if needsInitialSetup {
                beforeInitialSetup?()
                
                guard let results = realmProvider.targetReaderRealm?.objects(objectClass) else { return }
                
                let identifiers = Array(results).map {
                    Self.getStringIdentifier(for: $0, usingPrimaryKey: primaryKey)
                }
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                await Self.createSyncedEntities(entityType: schema.className, identifiers: identifiers, realm: persistenceRealm)
            }
        }
        
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        updateHasChanges(realm: persistenceRealm)
        
        if hasChanges {
            Task { @MainActor in
                NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
            }
        }
        
        startObservingTermination()
        cleanUp()
    }
    
    @BigSyncBackgroundActor
    private func processEnqueuedChanges() async {
        guard let realmProvider = realmProvider else { return }
        let currentChangeSet: ResultsChangeSet
        currentChangeSet = self.resultsChangeSet
        self.resultsChangeSet = ResultsChangeSet() // Reset for next batch
        
        for (schema, identifiers) in currentChangeSet.insertions {
            for chunk in Array(identifiers).chunked(into: 500) {
                try? await realmProvider.persistenceRealm?.asyncWrite {
                    for identifier in chunk {
                        self.updateTracking(objectIdentifier: identifier, entityName: schema, inserted: true, modified: false, deleted: false, realmProvider: realmProvider)
                    }
                }
            }
        }
        
        for (schema, identifiers) in currentChangeSet.modifications {
            for chunk in Array(identifiers).chunked(into: 500) {
                try? await realmProvider.persistenceRealm?.asyncWrite {
                    for identifier in chunk {
                        self.updateTracking(objectIdentifier: identifier, entityName: schema, inserted: false, modified: true, deleted: false, realmProvider: realmProvider)
                    }
                }
            }
        }
    }
    
    private func setupPublisherDebouncer() {
        resultsChangeSetPublisher
            .debounce(for: .seconds(1), scheduler: DispatchQueue.global())
            .sink { [weak self] _ in
                Task(priority: .background) { [weak self] in
                    await self?.processEnqueuedChanges()
                }
            }
            .store(in: &cancellables)
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
        
        guard let targetReaderRealm = realmProvider?.targetReaderRealm else { return }
        for objectSchema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(objectSchema.className) {
            guard let objectClass = self.realmObjectClass(name: objectSchema.className) else {
                continue
            }
            guard objectClass.conforms(to: SoftDeletable.self) else {
                fatalError("\(objectClass.className()) must conform to SoftDeletable in order to sync")
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
    
    @BigSyncBackgroundActor
    func updateTracking(objectIdentifier: String, entityName: String, inserted: Bool, modified: Bool, deleted: Bool, realmProvider: RealmProvider) {
        let identifier = "\(entityName).\(objectIdentifier)"
        var isNewChange = false
        
        //        debugPrint("!! updateTracking", identifier, "ins", inserted, "mod", modified)
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm)
        
        if deleted {
            isNewChange = true
            
            if let syncedEntity = syncedEntity {
                //                try? realmProvider.persistenceRealm.safeWrite {
                syncedEntity.state = SyncedEntityState.deleted.rawValue
            }
        } else if syncedEntity == nil {
            guard let persistenceRealm = realmProvider.persistenceRealm else { return }
            Self.createSyncedEntity(entityType: entityName, identifier: objectIdentifier, realm: persistenceRealm)
            //            debugPrint("!! createSyncedEntity for inserted", objectIdentifier)
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
                if let persistenceRealm = realmProvider.persistenceRealm {
//                    persistenceRealm.refresh()
                    if let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm) {
                        //                    try? realmProvider.persistenceRealm.safeWrite {
                        syncedEntity.state = SyncedEntityState.newOrChanged.rawValue
                        // If state was New (or Modified already) then leave it as that
                    }
                }
            }
        }
        
        let isNewChangeFinal = isNewChange
        if !hasChanges && isNewChangeFinal {
            hasChanges = true
            Task(priority: .background) { @MainActor in
                NotificationCenter.default.post(name: .ModelAdapterHasChangesNotification, object: self)
            }
        }
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    static func createSyncedEntities(entityType: String, identifiers: [String], realm: Realm) async {
        for chunk in identifiers.chunked(into: 5000) {
//            realm.refresh()
            try? await realm.asyncWrite {
                for identifier in chunk {
                    let syncedEntity = SyncedEntity(entityType: entityType, identifier: entityType + "." + identifier, state: SyncedEntityState.newOrChanged.rawValue)
                    realm.add(syncedEntity, update: .modified)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    static func createSyncedEntity(entityType: String, identifier: String, realm: Realm) -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: entityType, identifier: "\(entityType).\(identifier)", state: SyncedEntityState.newOrChanged.rawValue)
        
//        realm.refresh()
        realm.add(syncedEntity) //, update: .modified)
        return syncedEntity
    }
    
    @BigSyncBackgroundActor
    func writeSyncedEntities(syncedEntities: [SyncedEntity], realmProvider: RealmProvider) async throws {
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        try? await persistenceRealm.asyncWrite {
            for entity in syncedEntities {
                persistenceRealm.add(entity, update: .modified)
            }
        }
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
    
    func shouldIgnore(key: String) -> Bool {
        return CloudKitSynchronizer.metadataKeys.contains(key)
    }
    
    public func hasChanges(record: CKRecord, object: Object) -> Bool {
        let objectProperties = object.objectSchema.properties
        
        for property in objectProperties {
            let key = property.name
            
            // Skip the primary key
            if key == object.objectSchema.primaryKeyProperty?.name {
                continue
            }
            
            let newValue = record[key]
            let existingValue = object.value(forKey: key)
            
            if let newValue = newValue as? CKRecord.Reference {
                let recordName = newValue.recordID.recordName
                let separatorRange = recordName.range(of: ".")!
                let newObjectIdentifier = String(recordName[separatorRange.upperBound...])
                
                if let existingValue = existingValue as? String {
                    if existingValue != newObjectIdentifier {
                        return true
                    }
                }
            } else if let newValue = newValue as? CKAsset {
                if let fileURL = newValue.fileURL,
                   let newData = NSData(contentsOf: fileURL),
                   let existingData = existingValue as? NSData {
                    if newData != existingData {
                        return true
                    }
                }
            } else if let newValue = newValue as? [Int], let existingValue = existingValue as? Set<Int> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [String], let existingValue = existingValue as? Set<String> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Bool], let existingValue = existingValue as? Set<Bool> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Float], let existingValue = existingValue as? Set<Float> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Double], let existingValue = existingValue as? Set<Double> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Data], let existingValue = existingValue as? Set<Data> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Date], let existingValue = existingValue as? Set<Date> {
                if Set(newValue) != existingValue {
                    return true
                }
            } else if let newValue = newValue as? [Int], let existingValue = existingValue as? RealmSwift.List<Int> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.List<String> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.List<Bool> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [Float], let existingValue = existingValue as? RealmSwift.List<Float> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [Double], let existingValue = existingValue as? RealmSwift.List<Double> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [Data], let existingValue = existingValue as? RealmSwift.List<Data> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else if let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.List<Date> {
                if !newValue.elementsEqual(existingValue) {
                    return true
                }
            } else {
                if let newValue = newValue, let existingValue = existingValue {
                    if !(newValue as AnyObject).isEqual(existingValue) {
                        return true
                    }
                } else if newValue == nil && existingValue == nil {
                    continue
                } else {
                    return true
                }
            }
        }
        
        return false
    }
    
    @RealmBackgroundActor
    func applyChanges(in record: CKRecord, to object: Object, syncedEntityID: String, syncedEntityState: SyncedEntityState, entityType: String, realmProvider: RealmProvider) {
        let objectProperties = object.objectSchema.properties
        
        if syncedEntityState == .newOrChanged {
            if mergePolicy == .server {
                for property in objectProperties {
                    if shouldIgnore(key: property.name) {
                        continue
                    }
                    if property.type == .linkingObjects {
                        continue
                    }
                    applyChange(property: property, record: record, object: object, syncedEntityIdentifier: syncedEntityID, realmProvider: realmProvider)
                }
            } else if mergePolicy == .custom {
                var recordChanges = [String: Any]()
                for property in objectProperties {
                    if property.type == .linkingObjects {
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
                
                let acceptRemoteChange: Bool
                if let delegate = delegate {
                    acceptRemoteChange = delegate.realmSwiftAdapter(self, gotChanges: recordChanges, object: object)
                } else {
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
                    for property in objectProperties {
                        if shouldIgnore(key: property.name) {
                            continue
                        }
                        if property.type == .linkingObjects {
                            continue
                        }
                        applyChange(property: property, record: record, object: object, syncedEntityIdentifier: syncedEntityID, realmProvider: realmProvider)
                    }
                }
            }
        } else {
            for property in objectProperties {
                if shouldIgnore(key: property.name) {
                    continue
                }
                if property.type == .linkingObjects {
                    continue
                }
                applyChange(property: property, record: record, object: object, syncedEntityIdentifier: syncedEntityID, realmProvider: realmProvider)
            }
        }
    }
    
    func applyChange(property: Property, record: CKRecord, object: Object, syncedEntityIdentifier: String, realmProvider: RealmProvider) {
        let key = property.name
        if key == object.objectSchema.primaryKeyProperty!.name {
            return
        }
        
        if let recordProcessingDelegate = recordProcessingDelegate,
           !recordProcessingDelegate.shouldProcessPropertyInDownload(propertyName: key, object: object, record: record) {
            return
        }
        
        let value = record[key]
        
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
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                }
            default:
                break
            }
            object.setValue(recordValue, forKey: property.name)
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
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
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
            savePendingRelationshipAsync(name: key, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
        } else if property.type == .object {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            guard let recordName = record.value(forKey: property.name) as? String else { return }
            let separatorRange = recordName.range(of: ".")!
            let objectIdentifier = String(recordName[separatorRange.upperBound...])
            savePendingRelationshipAsync(name: key, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
               let data = NSData(contentsOf: fileURL) {
                object.setValue(data, forKey: key)
            }
        } else if value != nil || property.isOptional == true {
            // If property is not a relationship or value is nil and property is optional.
            // If value is nil and property is non-optional, it is ignored. This is something that could happen
            // when extending an object model with a new non-optional property, when an old record is applied to the object.
            //            let ref = ThreadSafeReference(to: object)
//            debugPrint("!! applyChange", type(of: object), key, value.debugDescription.prefix(100))
            object.setValue(value, forKey: key)
        }
    }
    
    func savePendingRelationshipAsync(name: String, syncedEntityID: String, targetIdentifier: String) {
        let request = PendingRelationshipRequest(name: name, syncedEntityID: syncedEntityID, targetIdentifier: targetIdentifier)
        pendingRelationshipQueue.append(request)
    }
    
    @BigSyncBackgroundActor
    func persistPendingRelationships() async throws {
        while !pendingRelationshipQueue.isEmpty {
            let chunk = Array(pendingRelationshipQueue.prefix(5000))
            try Task.checkCancellation()
            
            guard let persistenceRealm = await realmProvider?.persistenceRealm else { break }
            
            do {
                try await persistenceRealm.asyncWrite {
                    for request in chunk {
                        let pendingRelationship = PendingRelationship()
                        pendingRelationship.relationshipName = request.name
                        pendingRelationship.forSyncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: request.syncedEntityID)
                        pendingRelationship.targetIdentifier = request.targetIdentifier
                        persistenceRealm.add(pendingRelationship)
                    }
                }
                
                for processedRequest in chunk {
                    if let index = pendingRelationshipQueue.firstIndex(where: {
                        $0.name == processedRequest.name &&
                        $0.syncedEntityID == processedRequest.syncedEntityID &&
                        $0.targetIdentifier == processedRequest.targetIdentifier
                    }) {
                        pendingRelationshipQueue.remove(at: index)
                    }
                }
            } catch {
                debugPrint("Error during persistPendingRelationships:", error)
                break
            }
        }
    }
    
    @BigSyncBackgroundActor
    func applyPendingRelationships(realmProvider: RealmProvider) async throws {
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let pendingRelationships = persistenceRealm.objects(PendingRelationship.self)
        guard !pendingRelationships.isEmpty else { return }
        
        // De-dupe
        var duplicatesToDelete = [PendingRelationship]()
        var uniqueRelationships = Set<String>()
        for relationship in pendingRelationships {
            let relationshipName = relationship.relationshipName ?? ""
            let targetIdentifier = relationship.targetIdentifier ?? ""
            let syncedEntityID = relationship.forSyncedEntity.identifier ?? ""
            let uniqueKey = relationshipName + ":" + targetIdentifier + ":" + syncedEntityID
            if uniqueRelationships.contains(uniqueKey) {
                duplicatesToDelete.append(relationship)
            } else {
                uniqueRelationships.insert(uniqueKey)
            }
        }
        if !duplicatesToDelete.isEmpty {
            try await persistenceRealm.asyncWrite {
                persistenceRealm.delete(duplicatesToDelete)
            }
        }
        
        for relationship in Array(pendingRelationships) {
            let entity = relationship.forSyncedEntity
            
            guard let syncedEntity = entity,
                syncedEntity.entityState != .deleted else { continue }
            
            guard let originObjectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                continue
            }
            let objectIdentifier = getObjectIdentifier(for: syncedEntity)
            guard let originObject = realmProvider.targetReaderRealm?.object(ofType: originObjectClass, forPrimaryKey: objectIdentifier) else { continue }
            
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
            let targetExisted = try? await { @RealmBackgroundActor in
                guard let relationshipName = relationshipName else {
                    return false
                }
                
                guard let targetObject = realmProvider.targetWriterRealm?.object(ofType: targetObjectClass, forPrimaryKey: targetObjectIdentifier) else { return false }
                
                guard let targetWriterRealm = realmProvider.targetWriterRealm else { return false }
                if let originObject = targetWriterRealm.resolve(originRef) {
                    try await targetWriterRealm.asyncWrite {
                        originObject.setValue(targetObject, forKey: relationshipName)
                    }
                }
                return true
            }()
            if !(targetExisted ?? false) {
                continue
            }
            
            try? await persistenceRealm.asyncWrite {
                persistenceRealm.delete(relationship)
            }
            
            await Task.yield()
            try Task.checkCancellation()
        }
        debugPrint("Finished applying pending relationships")
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
            guard let targetWriterRealm = realmProvider?.targetWriterRealm else { return }
            for schema in targetWriterRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else {
                    continue
                }
                let predicate = NSPredicate(format: "isDeleted == %@", NSNumber(booleanLiteral: true))
                guard let lazyResults = realmProvider?.targetWriterRealm?.objects(objectClass).filter(predicate) else { continue }
                var results = Array(lazyResults)
                if results.isEmpty {
                    continue
                }
                if objectClass.self is any SyncableBase.Type {
                    results = results.filter { result in
                        guard let result = result as? (any SyncableBase) else {
                            fatalError("SyncableBase object class unexpectedly had non-SyncableBase element")
                        }
                        return !result.needsSyncToServer
                    }
                }
                var identifiersToDelete = [String]()
                results = try await Array(results.async.filter { @RealmBackgroundActor object in
                    // TODO: Consolidate with syncedEntity(for ...)
                    guard let objectClass = self.realmObjectClass(name: object.objectSchema.className) else {
                        debugPrint("Unexpectedly could not get realm object class for", object.objectSchema.className)
                        return false
                    }
                    let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
                    let identifier = object.objectSchema.className + "." + Self.getStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                    identifiersToDelete
                    let isSynced = try await { @BigSyncBackgroundActor [weak self] in
                        guard let self else { return false }
                        guard let persistenceRealm = realmProvider?.persistenceRealm else { return false }
                        let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm)
                        return syncedEntity?.entityState == .synced
                    }()
                    if isSynced {
                        identifiersToDelete.append(identifier)
                    }
                    return isSynced
                })
                
                guard let targetRealm = realmProvider?.targetWriterRealm else { return }
                for chunk in Array(results).chunks(ofCount: 500) {
                    try? await targetRealm.asyncWrite {
                        for item in chunk {
                            targetRealm.delete(item)
                        }
                    }
                    
                    await Task.yield()
                    try? await Task.sleep(nanoseconds: 10_000_000)
                }
                
                await didDelete(identifiers: identifiersToDelete)
                
                await Task.yield()
                try? await Task.sleep(nanoseconds: 10_000_000)
            }
        }
    }
    
    // MARK: - Children records
    
    @BigSyncBackgroundActor
    func childrenRecords(for syncedEntity: SyncedEntity) async -> [CKRecord] {
        var records = [CKRecord]()
        var parent: SyncedEntity?
        
        // TODO: Should be an exception rather than silently error return a result
        guard let syncRealmProvider = syncRealmProvider, let persistenceRealm = realmProvider?.persistenceRealm, let targetReaderRealm = realmProvider?.targetReaderRealm else { return [] }
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
                if let object = targetReaderRealm.object(ofType: (objectClass as Object.Type).self, forPrimaryKey: objectID) {
                    // Get children
                    guard let childObjectClass = realmObjectClass(name: relationship.childEntityName) else {
                        continue
                    }
                    let predicate = NSPredicate(format: "%K == %@", relationship.childParentKey, object)
                    let children = targetReaderRealm.objects(childObjectClass.self).filter(predicate)
                    
                    for child in children {
                        if let childEntity = self.syncedEntity(for: child, realm: persistenceRealm) {
                            await records.append(contentsOf: childrenRecords(for: childEntity))
                        }
                    }
                }
            }
        }
        
        return records
    }
    
    // MARK: - QSModelAdapter
    
    @BigSyncBackgroundActor
    public func saveChanges(in records: [CKRecord]) async throws {
        guard let realmProvider = realmProvider else { return }
        guard records.count != 0 else { return }
        
        var recordsToSave: [(record: CKRecord, objectClass: RealmSwift.Object.Type, objectIdentifier: Any, syncedEntityID: String, syncedEntityState: SyncedEntityState, entityType: String)] = []
        var syncedEntitiesToCreate: [SyncedEntity] = []
        
        for chunk in records.chunked(into: 2000) {
            for record in chunk {
                try Task.checkCancellation()
                
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                var syncedEntity: SyncedEntity? = Self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: persistenceRealm)
                if syncedEntity == nil {
                    let newSyncedEntity = SyncedEntity(entityType: record.recordType, identifier: record.recordID.recordName, state: SyncedEntityState.synced.rawValue)
                    syncedEntitiesToCreate.append(newSyncedEntity)
                    syncedEntity = newSyncedEntity
                }
                if let syncedEntity = syncedEntity {
                    if syncedEntity.entityState != .deleted && syncedEntity.entityType != "CKShare" {
                        guard let objectClass = self.realmObjectClass(name: record.recordType) else {
                            continue
                        }
                        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
                        
                        let recordToSave = (record, objectClass, objectIdentifier, syncedEntity.identifier, syncedEntity.entityState, syncedEntity.entityType)
                        
                        guard let object = realmProvider.targetReaderRealm?.object(ofType: objectClass, forPrimaryKey: objectIdentifier) else {
                            recordsToSave.append(recordToSave)
                            continue
                        }
                        
                        if hasChanges(record: record, object: object) {
                            recordsToSave.append(recordToSave)
//                        } else {
//                            debugPrint("!! no Changes found with object", record.recordID.recordName)
                        }
                    }
                } else {
                    // Can happen when iCloud has records for a model that no longer exists locally.
                    continue
                }
            }
            
            // Batch write all syncedEntities after processing each chunk
            if !syncedEntitiesToCreate.isEmpty {
                try? await writeSyncedEntities(syncedEntities: syncedEntitiesToCreate, realmProvider: realmProvider)
                syncedEntitiesToCreate.removeAll()
            }
            
            try? await Task.sleep(nanoseconds: 20_000_000)
        }
        
        if !recordsToSave.isEmpty {
            for chunk in recordsToSave.chunked(into: 1000) {
                try await realmProvider.persistenceRealm?.asyncWrite { [weak self] in
                    guard let self = self else { return }
                    
                    for (record, _, objectIdentifier, syncedEntityID, syncedEntityState, _) in chunk {
                        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                        if let syncedEntity = Self.getSyncedEntity(objectIdentifier: syncedEntityID, realm: persistenceRealm) {
                            self.save(record: record, for: syncedEntity)
                        }
                    }
                }
                
                try await { @RealmBackgroundActor in
                    guard let targetWriterRealm = await realmProvider.targetWriterRealm else { return }
//                    debugPrint("!! save changes to record types", Set(chunk.map { $0.record.recordID.recordName.split(separator: ".").first! }), "total count", chunk.count, chunk.map { $0.record.recordID.recordName.split(separator: ".").last! })
                    guard let targetWriterRealm = await realmProvider.targetWriterRealm else { return }
                    try await targetWriterRealm.asyncWrite { [weak self] in
                        guard let self = self else { return }
                        for (record, objectType, objectIdentifier, syncedEntityID, syncedEntityState, entityType) in chunk {
                            var object = targetWriterRealm.object(ofType: objectType, forPrimaryKey: objectIdentifier)
                            if object == nil {
                                object = objectType.init()
                                if let object {
                                    object.setValue(objectIdentifier, forKey: (objectType.primaryKey() ?? objectType.sharedSchema()?.primaryKeyProperty?.name)!)
                                    targetWriterRealm.add(object, update: .modified)
                                }
                            }
                            if let object {
                                self.applyChanges(in: record, to: object, syncedEntityID: syncedEntityID, syncedEntityState: syncedEntityState, entityType: entityType, realmProvider: realmProvider)
                            }
                        }
                    }
                }()
                
                try? await persistPendingRelationships()
                
                try? await Task.sleep(nanoseconds: 20_000_000)
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func deleteRecords(with recordIDs: [CKRecord.ID]) async {
        guard let realmProvider = realmProvider else { return }
        guard recordIDs.count != 0 else { return }
        debugPrint("Deleting records with record ids \(recordIDs.map { $0.recordName })")
        
        for recordID in recordIDs {
            guard let persistenceRealm = realmProvider.persistenceRealm else { return }
            if let syncedEntity = Self.getSyncedEntity(objectIdentifier: recordID.recordName, realm: persistenceRealm) {
                
                if syncedEntity.entityType != "CKShare" {
                    guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                        //                                    continue
                        return
                    }
                    let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                    
                    try? await { @RealmBackgroundActor in
                        guard let targetWriterRealm = realmProvider.targetWriterRealm else { return }
                        let object = targetWriterRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
                        
                        if let object = object {
                            guard let targetRealm = realmProvider.targetWriterRealm else { return }
                            try? await targetRealm.asyncWrite {
                                targetRealm.delete(object)
                            }
                        }
                    }()
                }
                
                if let record = syncedEntity.record {
                    guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                    try? await realmProvider.persistenceRealm?.asyncWrite {
                        persistenceRealm.delete(record);
                    }
                }
                
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                try? await realmProvider.persistenceRealm?.asyncWrite {
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func persistImportedChanges(completion: @escaping ((Error?) async -> Void)) async {
        guard let realmProvider = realmProvider else {
            await completion(nil)
            return
        }
        
        do {
            try await applyPendingRelationships(realmProvider: realmProvider)
        } catch {
            await completion(error)
            return
        }
        await completion(nil)
    }
    
    public func recordsToUpload(limit: Int) -> [CKRecord] {
        guard let syncRealmProvider = syncRealmProvider else { return [] }
        var recordsArray = [CKRecord]()
        let recordLimit = limit == 0 ? Int.max : limit
        var uploadingState = SyncedEntityState.newOrChanged
        
        var innerLimit = recordLimit
        while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deleted.rawValue {
            recordsArray.append(contentsOf: self.recordsToUpload(withState: uploadingState, limit: innerLimit, syncRealmProvider: syncRealmProvider))
            uploadingState = self.nextStateToSync(after: uploadingState)
            innerLimit = recordLimit - recordsArray.count
        }
        
        return recordsArray
    }
    
    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord]) async {
        guard let realmProvider = realmProvider else { return }
        
        for chunk in savedRecords.chunked(into: 500) {
            try? await realmProvider.persistenceRealm?.asyncWrite {
                for record in chunk {
                    if let syncedEntity = realmProvider.persistenceRealm?.object(ofType: SyncedEntity.self, forPrimaryKey: record.recordID.recordName) {
                        syncedEntity.state = SyncedEntityState.synced.rawValue
                        save(record: record, for: syncedEntity)
                    }
                }
            }
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }
    
    public func recordIDsMarkedForDeletion(limit: Int) -> [CKRecord.ID] {
        guard let syncRealmProvider = syncRealmProvider else { return [] }
        
        var recordIDs = [CKRecord.ID]()
        
        let deletedEntities = syncRealmProvider.syncPersistenceRealm.objects(SyncedEntity.self).where { $0.state == SyncedEntityState.deleted.rawValue }
        
        for syncedEntity in Array(deletedEntities) {
            if recordIDs.count >= limit {
                break
            }
            recordIDs.append(CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        }
        
        return recordIDs
    }
    
    @BigSyncBackgroundActor
    public func didDelete(recordIDs deletedRecordIDs: [CKRecord.ID]) async {
        guard let realmProvider = realmProvider else { return }
        
        for recordID in deletedRecordIDs {
            guard let persistenceRealm = realmProvider.persistenceRealm else { return }
            if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName) {
                try? await persistenceRealm.asyncWrite {
                    if let record = syncedEntity.record {
                        persistenceRealm.delete(record)
                    }
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func didDelete(identifiers: [String]) async {
        guard let realmProvider = realmProvider else { return }
        
        for identifier in identifiers {
            guard let persistenceRealm = realmProvider.persistenceRealm else { return }
            if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: identifier) {
                try? await persistenceRealm.asyncWrite {
                    if let record = syncedEntity.record {
                        persistenceRealm.delete(record)
                    }
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
    }
    @BigSyncBackgroundActor
    public func didFinishImport(with error: Error?) async {
        guard let realmProvider else { return }
        
        tempFileManager.clearTempFiles()
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        self.updateHasChanges(realm: persistenceRealm)
    }
    
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
    
    public var recordZoneID: CKRecordZone.ID {
        return zoneID
    }
    
    public var serverChangeToken: CKServerChangeToken? {
        get {
            guard let syncRealmProvider = syncRealmProvider else { return nil }
            
            var token: CKServerChangeToken?
            let serverToken = syncRealmProvider.syncPersistenceRealm.objects(ServerToken.self).first
            if let tokenData = serverToken?.token {
                token = NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
            }
            return token
        }
    }
    
    @BigSyncBackgroundActor
    public func saveToken(_ token: CKServerChangeToken?) async {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        var serverToken: ServerToken! = persistenceRealm.objects(ServerToken.self).first
        
        try? await persistenceRealm.asyncWrite {
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
    }
}
