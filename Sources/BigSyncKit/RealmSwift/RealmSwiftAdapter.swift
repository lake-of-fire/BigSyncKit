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
import AsyncAlgorithms
import Logging
import libzstd

//extension Realm {
//    public func safeWrite(_ block: (() throws -> Void)) throws {
//        if isInWriteTransaction {
//            try block()
//        } else {
//            try write(block)
//        }
//    }
//}

let bigSyncKitQueue = DispatchQueue(label: "BigSyncKit")

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

fileprivate struct PendingRelationshipRequest {
    let name: String
    let syncedEntityID: String
    let targetIdentifier: String
}

struct SyncRealmProvider {
    let persistenceConfiguration: Realm.Configuration
    let targetConfigurations: [Realm.Configuration]
    
    var syncPersistenceRealm: Realm {
        get {
            return try! Realm(configuration: persistenceConfiguration)
        }
    }
    var syncTargetRealms: [Realm] {
        get {
            return targetConfigurations.map { try! Realm(configuration: $0) }
        }
    }
    
    init?(
        persistenceConfiguration: Realm.Configuration,
        targetConfigurations: [Realm.Configuration]
    ) {
        guard (try? Realm(configuration: persistenceConfiguration)) != nil else {
            return nil
        }
        
        self.persistenceConfiguration = persistenceConfiguration
        self.targetConfigurations = targetConfigurations
        
        guard syncTargetRealms.count == targetConfigurations.count else {
            return nil
        }
    }
}

actor RealmProvider {
    let persistenceConfiguration: Realm.Configuration
    let targetConfigurations: [Realm.Configuration]
    
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
    var targetReaderRealms: [Realm]? {
        get {
            do {
                try Task.checkCancellation()
            } catch {
                return nil
            }
            return targetReaderRealmObjects
        }
    }
    @RealmBackgroundActor
    var targetWriterRealms: [Realm]? {
        get {
            do {
                try Task.checkCancellation()
            } catch {
                return nil
            }
            return targetWriterRealmObjects
        }
    }
    
    
    
    @BigSyncBackgroundActor
    let persistenceRealmObject: Realm
    @BigSyncBackgroundActor
    let targetReaderRealmObjects: [Realm]
    @RealmBackgroundActor
    let targetWriterRealmObjects: [Realm]
    
    @BigSyncBackgroundActor
    let targetReaderRealmPerSchemaName: [String: Realm]
    @RealmBackgroundActor
    let targetWriterRealmPerSchemaName: [String: Realm]
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
    init?(
        persistenceConfiguration: Realm.Configuration,
        targetConfigurations: [Realm.Configuration]
    ) async {
        self.persistenceConfiguration = persistenceConfiguration
        self.targetConfigurations = targetConfigurations
        
        do {
            persistenceRealmObject = try await Realm(configuration: persistenceConfiguration, actor: BigSyncBackgroundActor.shared)
            //            debugPrint("# persistence realm", persistenceRealmObject.configuration.fileURL)
            
            var targetReaderRealmObjects = [Realm]()
            for targetConfiguration in targetConfigurations {
                try await targetReaderRealmObjects.append(Realm(configuration: targetConfiguration, actor: BigSyncBackgroundActor.shared))
            }
            self.targetReaderRealmObjects = targetReaderRealmObjects
            
            var targetWriterRealmObjects = [Realm]()
            for targetConfiguration in targetConfigurations {
                let realmBackgroundActorRealm = try await RealmBackgroundActor.shared.cachedRealm(for: targetConfiguration)
                targetWriterRealmObjects.append(realmBackgroundActorRealm)
            }
            self.targetWriterRealmObjects = targetWriterRealmObjects
        } catch {
            print(error)
            return nil
        }
        
        var targetReaderRealmPerSchemaName = [String: Realm]()
        for targetReaderRealmObject in targetReaderRealmObjects {
            for objectType in targetReaderRealmObject.configuration.objectTypes ?? [] {
                targetReaderRealmPerSchemaName[objectType.className()] = targetReaderRealmObject
            }
        }
        self.targetReaderRealmPerSchemaName = targetReaderRealmPerSchemaName
        
        var targetWriterRealmPerSchemaName = [String: Realm]()
        for targetWriterRealmObject in targetWriterRealmObjects {
            for objectType in targetWriterRealmObject.configuration.objectTypes ?? [] {
                targetWriterRealmPerSchemaName[objectType.className()] = targetWriterRealmObject
            }
        }
        self.targetWriterRealmPerSchemaName = targetWriterRealmPerSchemaName
    }
}

struct ResultsChangeSet {
    var insertions: [String: (Set<String>, Date?)] = [:] // schemaName -> Set of insertions and latests explicitlyModifiedAt
    var modifications: [String: (Set<String>, Date?)] = [:] // schemaName -> Set of modification and latests explicitlyModifiedAts
}

public class RealmSwiftAdapter: NSObject, ModelAdapter {
    public let persistenceRealmConfiguration: Realm.Configuration
    public let targetRealmConfigurations: [Realm.Configuration]
    public let excludedClassNames: [String]
    public let zoneID: CKRecordZone.ID
    public var mergePolicy: MergePolicy = .custom
    public weak var delegate: RealmSwiftAdapterDelegate?
    public weak var recordProcessingDelegate: RealmSwiftAdapterRecordProcessing?
    public weak var modelAdapterDelegate: ModelAdapterDelegate?
    public var forceDataTypeInsteadOfAsset: Bool = false
    
    public var beforeInitialSetup: (() -> Void)?
    
    private let logger: Logging.Logger
    
    @BigSyncBackgroundActor
    private var cancelSync: Bool = false
    
    private lazy var persistentAssetManager: PersistentAssetManager = {
        PersistentAssetManager(identifier: "\(recordZoneID.ownerName).\(recordZoneID.zoneName).\(targetRealmConfigurations.map { $0.fileURL?.lastPathComponent ?? UUID().uuidString } .joined(separator: "-")).\(targetRealmConfigurations.map { $0.schemaVersion } .reduce(0, +))")
    }()
    
    var syncRealmProvider: SyncRealmProvider?
    var realmProvider: RealmProvider?
    
    //    var collectionNotificationTokens = [NotificationToken]()
    //    var collectionNotificationTokens = Set<AnyCancellable>()
    //    var pendingTrackingUpdates = [ObjectUpdate]()
    var modelTypes = [String: Object.Type]()
    public private(set) var hasChanges = false
    public private(set) var hasChangesCount: Int?
    
    private var resultsChangeSet = ResultsChangeSet()
    private let resultsChangeSetPublisher = PassthroughSubject<Void, Never>()
    
    private var lastRealmCheckDates: [URL: Date] = [:]
    private var lastRealmFileModDates: [URL: Date] = [:]
    private var recentlyFetchedRecordModifiedAts = [String: Date]()
    
    private var appForegroundCancellable: AnyCancellable?
    private let immediateChecksSubject = PassthroughSubject<Void, Never>()
    private let realmChangesSubject = PassthroughSubject<Realm, Never>()
    
    private var pendingRelationshipQueue = [PendingRelationshipRequest]()
    
    private var cancellables = Set<AnyCancellable>()
    
    private let zstdDictData: Data? = {
        guard let dictURL = Bundle.module.url(forResource: "ckrecordDictionary", withExtension: nil, subdirectory: "zstd"),
              let data = try? Data(contentsOf: dictURL) else {
            print("Error: Failed to load zstd dictionary during init")
            return nil
        }
        return data
    }()
    
#if DEBUG
    private var dummyRecordIdentifiers = Set<String>()
#endif
    
    public init(
        persistenceRealmConfiguration: Realm.Configuration,
        targetRealmConfigurations: [Realm.Configuration],
        excludedClassNames: [String],
        recordZoneID: CKRecordZone.ID,
        logger: Logging.Logger
    ) {
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfigurations = targetRealmConfigurations
        self.excludedClassNames = excludedClassNames
        self.zoneID = recordZoneID
        self.logger = logger
        
        super.init()
        
        setupTypeNamesLookup()
        
        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self = self else { return }
            try await setup()
        }
    }
    
    deinit {
        invalidateTokens()
    }
    
    @BigSyncBackgroundActor
    public func resetSyncCaches() async throws {
        invalidateTokens()
        
        if let persistenceRealm = realmProvider?.persistenceRealm {
            //            await persistenceRealm.asyncRefresh()
            try await persistenceRealm.asyncWrite {
                let objectTypes = (persistenceRealm.configuration.objectTypes ?? []).compactMap { $0 as? RealmSwift.Object.Type }
                for objectType in objectTypes {
                    persistenceRealm.delete(persistenceRealm.objects(objectType))
                }
            }
        }
        
        try await setup()
    }
    
    func invalidateTokens() {
        //        debugPrint("# invalidateRealmAndTokens()")
        for cancellable in cancellables {
            cancellable.cancel()
        }
        cancellables.removeAll()
    }
    
    static public func defaultPersistenceConfiguration() -> Realm.Configuration {
        var configuration = Realm.Configuration()
        configuration.schemaVersion = 6
        configuration.shouldCompactOnLaunch = { totalBytes, usedBytes in
            // totalBytes refers to the size of the file on disk in bytes (data + free space)
            // usedBytes refers to the number of bytes used by data in the file
            
            // Compact if the file is over size and less than some % 'used'
            let targetBytes = 30 * 1024 * 1024
            return (totalBytes > targetBytes) && (Double(usedBytes) / Double(totalBytes)) < 0.8
        }
        configuration.migrationBlock = { migration, oldSchemaVersion in
        }
        configuration.objectTypes = [
            SyncedEntity.self,
            SyncedEntityType.self,
            PendingRelationship.self,
            ServerToken.self
        ]
        return configuration
    }
    
    func setupTypeNamesLookup() {
        for targetRealmConfiguration in targetRealmConfigurations {
            targetRealmConfiguration.objectTypes?.forEach { objectType in
                modelTypes[objectType.className()] = objectType as? Object.Type
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func cancelSynchronization() {
        //        debugPrint("# cancel")
        cancelSync = true
    }
    
    @BigSyncBackgroundActor
    public func unsetCancellation() {
        //        debugPrint("# unset cancel")
        cancelSync = false
    }
    
    @BigSyncBackgroundActor
    func setup() async throws {
        logger.info("QSCloudKitSynchronizer >> Setup synchronization...")
        //        debugPrint("# setup() ...")
        realmProvider = await RealmProvider(
            persistenceConfiguration: persistenceRealmConfiguration,
            targetConfigurations: targetRealmConfigurations
        )
        guard let realmProvider else { return }
        
        guard let syncEmpty = realmProvider.persistenceRealm?.objects(SyncedEntity.self).isEmpty else { return }
        let needsInitialSetup = syncEmpty
        
        if needsInitialSetup {
            do {
                try await modelAdapterDelegate?.needsInitialSetup()
            } catch {
                //                print(error)
                logger.error("\(error)")
            }
        }
        
        guard let targetReaderRealms = realmProvider.targetReaderRealms else { return }
        
#if DEBUG
        // Create a dummy record for each Realm type that has no data
        for targetReaderRealm in targetReaderRealms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else { continue }
                let exists = targetReaderRealm.objects(objectClass).first != nil
                if exists {
                    try await { @RealmBackgroundActor in
                        do {
                            guard let targetWriterRealm = realmProvider.targetWriterRealmPerSchemaName[schema.className] else { return }
                            try await targetWriterRealm.asyncWrite {
                                let dummy = objectClass.init()
                                if let softDeletable = dummy as? SoftDeletable {
                                    softDeletable.isDeleted = true
                                }
                                targetWriterRealm.add(dummy, update: .modified)
                                print("[Debug] Inserted dummy record for schema: \(schema.className)")
                                let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
                                let dummyID = "\(schema.className).\(Self.getTargetObjectStringIdentifier(for: dummy, usingPrimaryKey: primaryKey))"
                                dummyRecordIdentifiers.insert(dummyID)
                            }
                        } catch {
                            print("⚠️ Failed to create dummy for \(schema.className): \(error)")
                        }
                    }()
                }
            }
        }
#endif
        
        for targetReaderRealm in targetReaderRealms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else {
                    continue
                }
                guard objectClass.conforms(to: SoftDeletable.self) else {
                    fatalError("\(objectClass.className()) must conform to SoftDeletable in order to sync with iCloud via BigSyncKit")
                }
                guard objectClass.conforms(to: ChangeMetadataRecordable.self) else {
                    fatalError("\(objectClass.className()) must conform to ChangeMetadataRecordable in order to sync with iCloud via BigSyncKit")
                }
                
                if needsInitialSetup {
                    beforeInitialSetup?()
                    
                    let results = targetReaderRealm.objects(objectClass)
                    let entityTypePrefix = schema.className + "."
                    let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
                    let identifiers = Array(results).map {
                        entityTypePrefix + Self.getTargetObjectStringIdentifier(for: $0, usingPrimaryKey: primaryKey)
                    }
                    try await createSyncedEntities(entityType: schema.className, identifiers: identifiers)
                }
            }
        }
        
        //        if !needsInitialSetup {
        try await createMissingSyncedEntities()
        //        }
        
        // Removed startPollingForChanges() call
        
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        updateHasChanges(realm: persistenceRealm)
        
        await setupPublisherDebouncer()
        observeRealmChanges()
        
        //        if hasChanges {
        //            Task { @BigSyncBackgroundActor in
        //                await modelAdapterDelegate?.hasChangesToUpload()
        //            }
        //        }
    }
    
    private func observeAppForegroundNotifications() {
#if canImport(UIKit)
        NotificationCenter.default
            .publisher(for: UIApplication.willEnterForegroundNotification)
            .merge(with: NotificationCenter.default
                .publisher(for: UIApplication.didBecomeActiveNotification)
            )
            .sink { [weak self] _ in
                self?.immediateChecksSubject.send(())
            }
            .store(in: &cancellables)
#endif
        
        appForegroundCancellable = immediateChecksSubject
            .debounce(for: .seconds(6), scheduler: bigSyncKitQueue)
            .sink { [weak self] _ in
                guard let self = self else { return }
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    guard let self, let persistenceRealm = self.realmProvider?.persistenceRealm else { return }
                    updateHasChanges(realm: persistenceRealm)
                }
            }
        
        immediateChecksSubject.send(())
    }
    
    @BigSyncBackgroundActor
    private func observeRealmChanges() {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else { return }
        
        // Subscribe to the subject with a 6-second debounce
        realmChangesSubject
            .debounce(for: .seconds(6), scheduler: bigSyncKitQueue)
            .sink { [weak self] changedRealm in
                guard let self = self else { return }
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    guard let self = self else { return }
                    await enqueueCreatedAndModified(in: changedRealm)
                }
            }
            .store(in: &cancellables)
        
        // For each realm, observe changes and send an event to the subject
        for targetReaderRealm in targetReaderRealms {
            let token = targetReaderRealm.observe { [weak self] _, _ in
                guard let self else { return }
                realmChangesSubject.send(targetReaderRealm)
            }
            cancellables.insert(AnyCancellable { token.invalidate() })
        }
    }
    
    private func modificationDateForFile(at url: URL) -> Date? {
        do {
            let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
            return attrs[.modificationDate] as? Date
        } catch {
            //            print("Could not read file attributes for \(url.path): \(error)")
            logger.error("Could not read file attributes for \(url.path): \(error)")
            return nil
        }
    }
    
    /// Immediately updates.
    @BigSyncBackgroundActor
    private func updateCreatedAndModified() async throws {
        await enqueueCreatedAndModified()
        try await processEnqueuedChanges()
    }
    
    @BigSyncBackgroundActor
    private func enqueueCreatedAndModified(in realm: Realm? = nil) async {
        let realms: [Realm]
        if let realm {
            realms = [realm]
        } else {
            realms = realmProvider?.targetReaderRealmObjects ?? []
        }
        for targetReaderRealm in realms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else { continue }
                guard objectClass.conforms(to: ChangeMetadataRecordable.self) else { continue }
                await self.enqueueCreatedAndModified(
                    in: objectClass,
                    schemaName: schema.className
                )
            }
        }
        
        if let persistenceRealm = realmProvider?.persistenceRealm {
            updateHasChanges(realm: persistenceRealm)
        }
    }
    
    @BigSyncBackgroundActor
    private func enqueueCreatedAndModified(
        in objectClass: Object.Type,
        schemaName: String
    ) async {
        guard let persistenceRealm = realmProvider?.persistenceRealm,
              let targetReaderRealm = realmProvider?.targetReaderRealmPerSchemaName[schemaName],
              let syncedEntityType = try? await getOrCreateSyncedEntityType(schemaName)
        else {
            //            print("Could not get realms or syncedEntityType for \(schemaName)")
            logger.error("Could not get realms or syncedEntityType for \(schemaName)")
            return
        }
        
        // TODO: Optimize by not checking records that we just fetched which triggered this to be called
        let lastTrackedChangesAt = syncedEntityType.lastTrackedChangesAt ?? .distantPast
        let createdPredicate = NSPredicate(format: "createdAt > %@ AND explicitlyModifiedAt != nil", lastTrackedChangesAt as NSDate)
        let modifiedPredicate = NSPredicate(format: "explicitlyModifiedAt > %@ AND createdAt <= %@", lastTrackedChangesAt as NSDate, lastTrackedChangesAt as NSDate)
        
        let created = Array(targetReaderRealm.objects(objectClass).filter(createdPredicate))
        let modified = Array(targetReaderRealm.objects(objectClass).filter(modifiedPredicate))
        
        let primaryKey = objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name ?? ""
        
        let filteredCreated = created.filter { object in
            let id = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey)
            guard let fetchedModified = recentlyFetchedRecordModifiedAts["\(schemaName).\(id)"],
                  let objectModified = object.value(forKey: "modifiedAt") as? Date else {
                return true
            }
            return objectModified != fetchedModified
        }
        
        let filteredModified = modified.filter { object in
            let id = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey)
            guard let fetchedModified = recentlyFetchedRecordModifiedAts["\(schemaName).\(id)"],
                  let objectModified = object.value(forKey: "modifiedAt") as? Date else {
                return true
            }
            return objectModified != fetchedModified
        }
        
        //        if created.isEmpty && modified.isEmpty {
        //            let (maxCreatedAt, maxModifiedAt) =  (
        //                targetReaderRealm.objects(objectClass as! Object.Type)
        //                    .max(ofProperty: "createdAt") as Date?,
        //                targetReaderRealm.objects(objectClass as! Object.Type)
        //                    .max(ofProperty: "modifiedAt") as Date?
        //            )
        //            debugPrint("Warning: enueueCreatedAndModified called without any matching records to enqueue as created or modified. Object class:", objectClass, "Last tracked changes at:", lastTrackedChangesAt, "Last created at:", maxCreatedAt, "Last modified at:", maxModifiedAt)
        //        }
        
        if !filteredCreated.isEmpty {
            let insertions = resultsChangeSet.insertions[schemaName, default: ([], nil)]
            let latestCreatedExplicitlyModifiedAt = filteredCreated.compactMap { ($0 as? ChangeMetadataRecordable)?.explicitlyModifiedAt } .max()
            let updatedInsertions: (Set<String>, Date?) = (
                insertions.0.union(filteredCreated.map { Self.getTargetObjectStringIdentifier(for: $0, usingPrimaryKey: primaryKey)
                }),
                latestCreatedExplicitlyModifiedAt
            )
            resultsChangeSet.insertions[schemaName] = updatedInsertions
        }
        if !filteredModified.isEmpty {
            let modifications = resultsChangeSet.modifications[schemaName, default: ([], nil)]
            let latestModifiedExplicitlyModifiedAt = filteredModified.compactMap { ($0 as? ChangeMetadataRecordable)?.explicitlyModifiedAt } .max()
            let updatedModifications: (Set<String>, Date?) = (
                modifications.0.union(filteredModified.map { Self.getTargetObjectStringIdentifier(for: $0, usingPrimaryKey: primaryKey)
                }),
                latestModifiedExplicitlyModifiedAt
            )
            resultsChangeSet.modifications[schemaName] = updatedModifications
        }
        
        // Persist the new lastTrackedChangesAt
        //        await persistenceRealm.asyncRefresh()
        if !created.isEmpty || !modified.isEmpty {
            let processedIDs = filteredCreated.map { "\(schemaName).\($0.value(forKey: primaryKey)!)" } +
            filteredModified.map { "\(schemaName).\($0.value(forKey: primaryKey)!)" }
            for id in processedIDs {
                self.recentlyFetchedRecordModifiedAts.removeValue(forKey: id)
            }
            
            //            debugPrint("# created or modified non-empty, resultsChangeSetPublisher send...", created.count, modified.count, resultsChangeSet.insertions, resultsChangeSet.modifications)
            resultsChangeSetPublisher.send(())
        }
    }
    
    @BigSyncBackgroundActor
    private func processEnqueuedChanges() async throws {
        guard let realmProvider = realmProvider else { return }
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let currentChangeSet: ResultsChangeSet
        currentChangeSet = self.resultsChangeSet
        self.resultsChangeSet = ResultsChangeSet() // Reset for next batch
        
        //        if !currentChangeSet.insertions.isEmpty {                            debugPrint("# processEnqueuedChanges INSERT RECS", currentChangeSet.insertions.compactMap { $0 })                        }
        //        if !currentChangeSet.modifications.isEmpty {                            debugPrint("# processEnqueuedChanges MODIFY RECS", currentChangeSet.modifications.values.compactMap { $0 })                        }
        
        for (schema, identifiers) in currentChangeSet.insertions.mapValues(\.0) {
            guard let syncedEntityType = try? await getOrCreateSyncedEntityType(schema) else { return }
            
            for chunk in Array(identifiers).chunked(into: 500) {
                //                await persistenceRealm.asyncRefresh()
                try? await persistenceRealm.asyncWrite {
                    for identifier in chunk {
                        guard !cancelSync else { throw CancellationError() }
                        self.updateTracking(
                            objectIdentifier: identifier,
                            entityName: schema,
                            inserted: true,
                            modified: false,
                            deleted: false,
                            persistenceRealm: persistenceRealm
                        )
                    }
                }
            }
        }
        
        for (schema, identifiers) in currentChangeSet.modifications.mapValues(\.0) {
            guard let syncedEntityType = try? await getOrCreateSyncedEntityType(schema) else { return }
            
            for chunk in Array(identifiers).chunked(into: 500) {
                //                await persistenceRealm.asyncRefresh()
                try? await persistenceRealm.asyncWrite {
                    for identifier in chunk {
                        guard !cancelSync else { throw CancellationError() }
                        self.updateTracking(
                            objectIdentifier: identifier,
                            entityName: schema,
                            inserted: false,
                            modified: true,
                            deleted: false,
                            persistenceRealm: persistenceRealm
                        )
                    }
                }
            }
        }
        
        try? await Task.sleep(nanoseconds: 10_000_000)
        await persistenceRealm.asyncRefresh()
        
        var lastTrackedChangesAtUpdates: [(String, Date)] = []
        for changeSet in [currentChangeSet.insertions, currentChangeSet.modifications] {
            for (schema, latestExplicitlyModifiedAt) in changeSet.map({ ($0.key, $0.value.1) }) {
                guard !cancelSync else { throw CancellationError() }
                guard let syncedEntityType = try? await getOrCreateSyncedEntityType(schema) else { continue }
                guard !cancelSync else { throw CancellationError() }
                
                if let latestExplicitlyModifiedAt, syncedEntityType.lastTrackedChangesAt != latestExplicitlyModifiedAt {
                    lastTrackedChangesAtUpdates.append((syncedEntityType.entityType, latestExplicitlyModifiedAt))
                }
            }
        }
        guard !cancelSync else { throw CancellationError() }
        if !lastTrackedChangesAtUpdates.isEmpty {
            try? await persistenceRealm.asyncWrite {
                for (syncedEntityType, latestExplicitlyModifiedAt) in lastTrackedChangesAtUpdates {
                    persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: syncedEntityType)?.lastTrackedChangesAt = latestExplicitlyModifiedAt
                }
            }
        }
        
        if hasChanges {
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    
    private func setupPublisherDebouncer() {
        resultsChangeSetPublisher
            .debounce(for: .seconds(6), scheduler: bigSyncKitQueue)
            .sink { [weak self] _ in
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    try await self?.processEnqueuedChanges()
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
        let count = results.count
        if hasChangesCount != count {
            let syncedCount = realm.objects(SyncedEntity.self).where { $0.state == SyncedEntityState.synced.rawValue } .count
            logger.info("QSCloudKitSynchronizer >> \(count) changed records remaining to upload. \(syncedCount) records already marked as synced.")
        }
        hasChangesCount = count
        hasChanges = count > 0
        Task(priority: .background) { @BigSyncBackgroundActor in
            NotificationCenter.default.post(
                name: .SynchronizerChangesRemainingToUpload,
                object: nil,
                userInfo: ["CloudKitSynchronizerChangesRemainingToUploadKey": count]
            )
        }
    }
    
    @BigSyncBackgroundActor
    func updateTracking(
        objectIdentifier: String,
        entityName: String,
        inserted: Bool,
        modified: Bool,
        deleted: Bool,
        persistenceRealm: Realm
    ) {
        let identifier = entityName + "." + objectIdentifier
        var isNewChange = false
        
        let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm)
        //        debugPrint("# updateTracking", identifier, "ins", inserted, "mod", modified, "syncedentity exists?", syncedEntity != nil)
        
        if deleted {
            isNewChange = true
            
            if let syncedEntity = syncedEntity {
                //                try? realmProvider.persistenceRealm.safeWrite {
                syncedEntity.state = SyncedEntityState.deleted.rawValue
            }
        } else if syncedEntity == nil {
            Self.createSyncedEntity(
                entityType: entityName,
                identifier: objectIdentifier,
                modified: modified,
                realm: persistenceRealm
            )
            //            debugPrint("!! createSyncedEntity for inserted", objectIdentifier)
            if inserted {
                isNewChange = true
            }
        } else if !inserted {
            guard let syncedEntity else {
                return
            }
            
            isNewChange = true
            
            if syncedEntity.state == SyncedEntityState.synced.rawValue && modified {
                // Hack to avoid crashing issue: https://github.com/realm/realm-swift/issues/8333
                //                    persistenceRealm.refresh()
                if let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm), syncedEntity.state != SyncedEntityState.changed.rawValue {
                    //                    try? realmProvider.persistenceRealm.safeWrite {
                    syncedEntity.state = SyncedEntityState.changed.rawValue
                    // If state was New (or Modified already) then leave it as that
                }
            }
        }
        
        if !hasChanges && isNewChange {
            hasChanges = true
        }
    }
    
    @BigSyncBackgroundActor
    func createMissingSyncedEntities() async throws {
        guard let targetReaderRealms = await realmProvider?.targetReaderRealms, let persistenceRealm = realmProvider?.persistenceRealm else { return }
        
        var missingEntities = [String: [String]]()
        
        for targetReaderRealm in targetReaderRealms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else {
                    continue
                }
                
                let objects = targetReaderRealm.objects(objectClass)
                for object in Array(objects) {
                    guard let (entityType, identifier) = syncedEntityTypeAndIdentifier(for: object) else { continue }
                    let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm)
                    if syncedEntity == nil {
                        missingEntities[entityType, default: []].append(identifier)
                    }
                }
            }
        }
        
        for (entityType, identifiers) in missingEntities {
            //            debugPrint("Create", identifiers.count, "missing synced entities for", entityType)
            logger.info("QSCloudKitSynchronizer >> Create \(identifiers.count) missing synced entities for \(entityType)")
            try await createSyncedEntities(entityType: entityType, identifiers: identifiers)
        }
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    func createSyncedEntities(entityType: String, identifiers: [String]) async throws {
        //                debugPrint("Create synced entities", entityType, identifiers.count)
        logger.info("QSCloudKitSynchronizer >> Creating \(identifiers.count) SyncedEntity records for \(entityType)…")
        for chunk in identifiers.chunked(into: 500) {
            guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
            try await persistenceRealm.asyncWrite {
                for identifier in chunk {
                    guard !cancelSync else { throw CancellationError() }
                    let syncedEntity = SyncedEntity(entityType: entityType, identifier: identifier, state: SyncedEntityState.new.rawValue)
                    persistenceRealm.add(syncedEntity, update: .modified)
                }
            }
            try? await Task.sleep(nanoseconds: 20_000_000)
            //            await persistenceRealm.asyncRefresh()
        }
        logger.info("QSCloudKitSynchronizer >> Created \(identifiers.count) SyncedEntity records for \(entityType)")
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    static func createSyncedEntity(entityType: String, identifier: String, modified: Bool, realm: Realm) -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: entityType, identifier: "\(entityType).\(identifier)", state: modified ? SyncedEntityState.changed.rawValue : SyncedEntityState.new.rawValue)
        
        //        realm.refresh()
        realm.add(syncedEntity, update: .modified)
        return syncedEntity
    }
    
    @BigSyncBackgroundActor
    func writeSyncedEntities(syncedEntities: [SyncedEntity], realmProvider: RealmProvider) async throws {
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        //        await persistenceRealm.asyncRefresh()
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
        return getObjectIdentifier(stringObjectId: objectIdentifier, entityType: syncedEntity.entityType) ?? objectIdentifier
    }
    
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
        guard let (_, identifier) = syncedEntityTypeAndIdentifier(for: object) else { return nil }
        return Self.getSyncedEntity(objectIdentifier: identifier, realm: realm)
    }
    
    @BigSyncBackgroundActor
    func syncedEntityTypeAndIdentifier(for object: Object) -> (String, String)? {
        let entityType = object.objectSchema.className
        guard let objectClass = self.realmObjectClass(name: entityType) else {
            return nil
        }
        let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
        let identifier = object.objectSchema.className + "." + Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey)
        return (entityType, identifier)
    }
    
    @inlinable
    static func getTargetObjectStringIdentifier(for object: Object, usingPrimaryKey key: String) -> String {
        let objectId = object.value(forKey: key)
        let identifier: String
        if let value = objectId as? String {
            identifier = value
        } else if let value = objectId as? CustomStringConvertible {
            identifier = String(describing: value)
        } else {
            identifier = objectId as! String
        }
        //        guard identifier.count <= 255 else {
        //
        //        }
        return identifier
    }
    
    static func getSyncedEntity(objectIdentifier: String, realm: Realm) -> SyncedEntity? {
        return realm.object(ofType: SyncedEntity.self, forPrimaryKey: objectIdentifier)
    }
    
    @BigSyncBackgroundActor
    func getOrCreateSyncedEntityType(_ entityType: String) async throws -> SyncedEntityType? {
        guard let persistenceRealm = await realmProvider?.persistenceRealm else { return nil }
        
        if let syncedEntityType = persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: entityType) {
            return syncedEntityType
        }
        let syncedEntityType = SyncedEntityType(
            entityType: entityType
        )
        //        await persistenceRealm.asyncRefresh()
        try await persistenceRealm.asyncWrite {
            persistenceRealm.add(syncedEntityType, update: .modified)
        }
        return syncedEntityType
    }
    
    func shouldIgnore(key: String) -> Bool {
        return CloudKitSynchronizer.metadataKeys.contains(key)
    }
    
    public func hasChanges(record: CKRecord, object: Object) -> Bool {
        let objectProperties = object.objectSchema.properties
        
        let skippedKeys: Set<String>
        if let skippable = object as? SyncSkippablePropertiesModel {
            skippedKeys = skippable.skipSyncingProperties() ?? []
        } else {
            skippedKeys = []
        }
        
        for property in objectProperties where !skippedKeys.contains(property.name) {
            let key = property.name
            
            // Skip the primary key
            if key == object.objectSchema.primaryKeyProperty?.name {
                continue
            }
            
            let newValue = record[key]
            let existingValue = object.value(forKey: key)
            
            let propertyChanged = {
                // Handle one side being nil first
                guard !(newValue == nil && existingValue == nil) else {
                    return false
                }
                if (newValue == nil && existingValue != nil) || (newValue != nil && existingValue == nil) {
                    return true
                }
                
                if let newValue = newValue as? CKRecord.Reference {
                    let recordName = newValue.recordID.recordName
                    let separatorRange = recordName.range(of: ".")!
                    let newObjectIdentifier = String(recordName[separatorRange.upperBound...])
                    
                    if let existingValue = existingValue as? String {
                        return existingValue != newObjectIdentifier
                    }
                } else if let newValue = newValue as? CKAsset {
                    if let fileURL = newValue.fileURL,
                       let newData = NSData(contentsOf: fileURL),
                       let existingData = existingValue as? NSData {
                        return newData != existingData
                    }
                }
                
                if property.isSet {
                    switch property.type {
                    case .int:
                        guard let newValue = newValue as? [Int], let existingValue = existingValue as? RealmSwift.MutableSet<Int> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .string:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<String> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.MutableSet<Bool> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .float:
                        guard let newValue = newValue as? [Float], let existingValue = existingValue as? RealmSwift.MutableSet<Float> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .double:
                        guard let newValue = newValue as? [Double], let existingValue = existingValue as? RealmSwift.MutableSet<Double> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .data:
                        guard let newValue = newValue as? [Data], let existingValue = existingValue as? RealmSwift.MutableSet<Data> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.MutableSet<Date> else { return true }
                        return Set(newValue) != Set(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.MutableSet<UUID> else { return true }
                        return Set(newValue) != Set(Array(existingValue).map { $0.uuidString })
                    default:
                        break
                    }
                } else if property.isArray {
                    switch property.type {
                    case .int:
                        guard let newValue = newValue as? [Int], let existingValue = existingValue as? RealmSwift.List<Int> else { return true }
                        return newValue != Array(existingValue)
                    case .string:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.List<String> else { return true }
                        return newValue != Array(existingValue)
                    case .bool:
                        guard let newValue = newValue as? [Bool], let existingValue = existingValue as? RealmSwift.List<Bool> else { return true }
                        return newValue != Array(existingValue)
                    case .float:
                        guard let newValue = newValue as? [Float], let existingValue = existingValue as? RealmSwift.List<Float> else { return true }
                        return newValue != Array(existingValue)
                    case .double:
                        guard let newValue = newValue as? [Double], let existingValue = existingValue as? RealmSwift.List<Double> else { return true }
                        return newValue != Array(existingValue)
                    case .data:
                        guard let newValue = newValue as? [Data], let existingValue = existingValue as? RealmSwift.List<Data> else { return true }
                        return newValue != Array(existingValue)
                    case .date:
                        guard let newValue = newValue as? [Date], let existingValue = existingValue as? RealmSwift.List<Date> else { return true }
                        return newValue != Array(existingValue)
                    case .UUID:
                        guard let newValue = newValue as? [String], let existingValue = existingValue as? RealmSwift.List<UUID> else { return true }
                        return newValue != Array(existingValue).map { $0.uuidString }
                    default:
                        break
                    }
                } else if property.isMap {
                    guard let newValue = newValue as? [NSArray], newValue.count == 2,
                          let keyArray = newValue[0] as? [String], let valueArray = newValue[1] as? [Any],
                          keyArray.count == valueArray.count else {
                        logger.warning("QSCloudKitSynchronizer >> Found unexpected property value: \(newValue)")
                        return true
                    }
                    var result: [String: Any] = [:]
                    for (index, key) in keyArray.enumerated() {
                        switch property.type {
                        case .int:
                            if let val = valueArray[index] as? Int { result[key] = val }
                        case .string:
                            if let val = valueArray[index] as? String { result[key] = val }
                        case .bool:
                            if let val = valueArray[index] as? Bool { result[key] = val }
                        case .float:
                            if let val = valueArray[index] as? Float { result[key] = val }
                        case .double:
                            if let val = valueArray[index] as? Double { result[key] = val }
                        case .date:
                            if let val = valueArray[index] as? Date { result[key] = val }
                        case .UUID:
                            if let val = valueArray[index] as? String, let uuid = UUID(uuidString: val) {
                                result[key] = uuid
                            }
                        default:
                            break
                        }
                    }
                    switch property.type {
                    case .int:
                        guard let newValue = result as? [String: Int], let existingValue = existingValue as? RealmSwift.Map<String, Int> else { return true }
                        return newValue != existingValue.reduce(into: [String: Int]()) { $0[$1.key] = $1.value }
                    case .string:
                        guard let newValue = result as? [String: String], let existingValue = existingValue as? RealmSwift.Map<String, String> else { return true }
                        return newValue != existingValue.reduce(into: [String: String]()) { $0[$1.key] = $1.value }
                    case .bool:
                        guard let newValue = result as? [String: Bool], let existingValue = existingValue as? RealmSwift.Map<String, Bool> else { return true }
                        return newValue != existingValue.reduce(into: [String: Bool]()) { $0[$1.key] = $1.value }
                    case .float:
                        guard let newValue = result as? [String: Float], let existingValue = existingValue as? RealmSwift.Map<String, Float> else { return true }
                        return newValue != existingValue.reduce(into: [String: Float]()) { $0[$1.key] = $1.value }
                    case .double:
                        guard let newValue = result as? [String: Double], let existingValue = existingValue as? RealmSwift.Map<String, Double> else { return true }
                        return newValue != existingValue.reduce(into: [String: Double]()) { $0[$1.key] = $1.value }
                    case .date:
                        guard let newValue = result as? [String: Date], let existingValue = existingValue as? RealmSwift.Map<String, Date> else { return true }
                        return newValue != existingValue.reduce(into: [String: Date]()) { $0[$1.key] = $1.value }
                    case .UUID:
                        guard let newValue = result as? [String: UUID], let existingValue = existingValue as? RealmSwift.Map<String, UUID> else { return true }
                        return newValue != existingValue.reduce(into: [String: UUID]()) { $0[$1.key] = $1.value }
                    default:
                        break
                    }
                } else {
                    switch property.type {
                    case .int:
                        guard let newValue = newValue as? Int, let existingValue = existingValue as? Int else { return true }
                        return newValue != existingValue
                    case .string:
                        guard let newValue = newValue as? String, let existingValue = existingValue as? String else { return true }
                        return newValue != existingValue
                    case .bool:
                        guard let newValue = newValue as? Bool, let existingValue = existingValue as? Bool else { return true }
                        return newValue != existingValue
                    case .float:
                        guard let newValue = newValue as? Float, let existingValue = existingValue as? Float else { return true }
                        return newValue != existingValue
                    case .double:
                        guard let newValue = newValue as? Double, let existingValue = existingValue as? Double else { return true }
                        return newValue != existingValue
                    case .data:
                        guard let newValue = newValue as? Data, let existingValue = existingValue as? Data else { return true }
                        return newValue != existingValue
                    case .date:
                        guard let newValue = newValue as? Date, let existingValue = existingValue as? Date else { return true }
                        return newValue != existingValue
                    case .UUID:
                        guard let newValue = newValue as? String, let newUUID = UUID(uuidString: newValue), let existingValue = existingValue as? UUID else { return true }
                        return newUUID != existingValue
                    default:
                        break
                    }
                }
                
                logger.warning("QSCloudKitSynchronizer >> Found unexpected property value: \(newValue)")
                return true
            }()
            if propertyChanged {
                return true
            }
        }
        return false
    }
    
    @RealmBackgroundActor
    func applyChanges(
        in record: CKRecord,
        to object: Object,
        syncedEntityID: String,
        syncedEntityState: SyncedEntityState,
        entityType: String
    ) {
        let objectProperties = object.objectSchema.properties
        
        let skippedKeys: Set<String>
        if let skippable = object as? SyncSkippablePropertiesModel {
            skippedKeys = skippable.skipSyncingProperties() ?? []
        } else {
            skippedKeys = []
        }
        
        func applyChanges() {
            //                logger.info("QSCloudKitSynchronizer >> Applying changes (no conflict): \(object.objectSchema.className) – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date)")
            for property in objectProperties where !skippedKeys.contains(property.name) {
                if shouldIgnore(key: property.name) {
                    continue
                }
                if property.type == .linkingObjects {
                    continue
                }
                applyChange(property: property, record: record, object: object, syncedEntityIdentifier: syncedEntityID)
            }
        }
        
        if mergePolicy == .server {
            applyChanges()
        } else if mergePolicy == .custom {
            var recordChanges = [String: Any]()
            for property in objectProperties where !skippedKeys.contains(property.name) {
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
            if let delegate {
                acceptRemoteChange = delegate.realmSwiftAdapter(self, gotChanges: recordChanges, object: object)
            } else {
                acceptRemoteChange = { adapter, changes, object in
#if os(macOS)
                    guard adapter.hasRealmObjectClass(name: object.className) else { return false }
#else
                    guard adapter.hasRealmObjectClass(name: String(describing: type(of: object))) else { return false }
#endif
                    let remoteExplicitlyModifiedAt = changes["explicitlyModifiedAt"] as? Date ?? .distantPast
                    let localExplicitlyModifiedAt = object.value(forKey: "explicitlyModifiedAt") as? Date ?? .distantPast
                    let result: Bool
                    if remoteExplicitlyModifiedAt > localExplicitlyModifiedAt {
                        result = true
                    } else if remoteExplicitlyModifiedAt == localExplicitlyModifiedAt {
                        let remoteModifiedAt = changes["modifiedAt"] as? Date ?? .distantPast
                        let localModifiedAt = object.value(forKey: "modifiedAt") as? Date ?? .distantPast
                        result = remoteModifiedAt > localModifiedAt
                    } else {
                        result = false
                    }
                    logger.info("QSCloudKitSynchronizer >> Conflict resolution: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\(localExplicitlyModifiedAt), remote explicitly modified=\(remoteExplicitlyModifiedAt) => accepted remote: \(result)")
                    //                        logger.info("QSCloudKitSynchronizer >> Conflict resolution object - local: \(object.description.prefix(5000))")
                    //                        logger.info("QSCloudKitSynchronizer >> Conflict resolution object - remote: \(changes.description.prefix(5000))")
                    return result
                }(self, recordChanges, object)
            }
            
            if acceptRemoteChange {
                if let remoteExplicitlyModifiedAt = record["explicitlyModifiedAt"] as? Date, let localExplicitlyModifiedAt = (object as? ChangeMetadataRecordable)?.explicitlyModifiedAt, remoteExplicitlyModifiedAt < localExplicitlyModifiedAt {
                    logger.warning("QSCloudKitSynchronizer >> WARNING: Applying changes with lower explicitlyModifiedAt: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date), syncedEntityState=\(syncedEntityState.rawValue)")
                }
                
                applyChanges()
            } else {
                if let remoteExplicitlyModifiedAt = record["explicitlyModifiedAt"] as? Date, let localExplicitlyModifiedAt = (object as? ChangeMetadataRecordable)?.explicitlyModifiedAt, remoteExplicitlyModifiedAt < localExplicitlyModifiedAt {
                    logger.info("QSCloudKitSynchronizer >> Rejecting remote changes with lower explicitlyModifiedAt: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date), syncedEntityState=\(syncedEntityState.rawValue)")
                }
                // TODO: Ensure this local object is pending upload...
            }
        }
    }
    
    func applyChange(property: Property, record: CKRecord, object: Object, syncedEntityIdentifier: String) {
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
            case .UUID:
                if let stringArray = value as? [String] {
                    let set = Set(stringArray.compactMap(UUID.init(uuidString:)))
                    object.setValue(set, forKey: key)
                }
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
            case .UUID:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                let list = List<UUID>()
                list.append(objectsIn: value.compactMap { UUID(uuidString: $0) })
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
        } else if property.isMap {
            guard let value = value as? [NSArray], value.count == 2,
                  let keyArray = value[0] as? [String], let valueArray = value[1] as? [Any],
                  keyArray.count == valueArray.count else {
                return
            }
            var result: [String: Any] = [:]
            for (index, key) in keyArray.enumerated() {
                switch property.type {
                case .int:
                    if let val = valueArray[index] as? Int { result[key] = val }
                case .string:
                    if let val = valueArray[index] as? String { result[key] = val }
                case .bool:
                    if let val = valueArray[index] as? Bool { result[key] = val }
                case .float:
                    if let val = valueArray[index] as? Float { result[key] = val }
                case .double:
                    if let val = valueArray[index] as? Double { result[key] = val }
                case .date:
                    if let val = valueArray[index] as? Date { result[key] = val }
                case .data:
                    if let val = valueArray[index] as? Data { result[key] = val }
                case .UUID:
                    if let val = valueArray[index] as? String, let uuid = UUID(uuidString: val) {
                        result[key] = uuid
                    }
                default:
                    break
                }
            }
            object.setValue(result, forKey: key)
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
        } else if property.type == .UUID {
            if let uuidString = record.value(forKey: key) as? String,
               let uuid = UUID(uuidString: uuidString) {
                object.setValue(uuid, forKey: key)
            }
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
                //                await persistenceRealm.asyncRefresh()
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
                //                debugPrint("Error during persistPendingRelationships:", error)
                logger.error("Error during persistPendingRelationships: \(error)")
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
            let syncedEntityID = relationship.forSyncedEntity?.identifier ?? ""
            let uniqueKey = relationshipName + ":" + targetIdentifier + ":" + syncedEntityID
            if uniqueRelationships.contains(uniqueKey) {
                duplicatesToDelete.append(relationship)
            } else {
                uniqueRelationships.insert(uniqueKey)
            }
        }
        if !duplicatesToDelete.isEmpty {
            //            await persistenceRealm.asyncRefresh()
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
            guard let originObject = realmProvider.targetReaderRealmPerSchemaName[originObjectClass.className()]?.object(ofType: originObjectClass, forPrimaryKey: objectIdentifier) else { continue }
            
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
                
                guard let targetObject = realmProvider.targetWriterRealmPerSchemaName[targetObjectClass.className()]?.object(ofType: targetObjectClass, forPrimaryKey: targetObjectIdentifier) else { return false }
                
                guard let targetWriterRealm = realmProvider.targetWriterRealmPerSchemaName[originObjectClass.className()] else { return false }
                if let originObject = targetWriterRealm.resolve(originRef) {
                    await targetWriterRealm.asyncRefresh()
                    try await targetWriterRealm.asyncWrite {
                        originObject.setValue(targetObject, forKey: relationshipName)
                    }
                }
                return true
            }()
            if !(targetExisted ?? false) {
                continue
            }
            
            //            await persistenceRealm.asyncRefresh()
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
        syncedEntity.encodedRecord = encodedRecord(record, onlySystemFields: true)
    }
    
    @BigSyncBackgroundActor
    func encodedRecord(_ record: CKRecord, onlySystemFields: Bool) -> Data? {
        let data = NSMutableData()
        let archiver = NSKeyedArchiver(forWritingWith: data)
        if onlySystemFields {
            record.encodeSystemFields(with: archiver)
        } else {
            record.encode(with: archiver)
        }
        archiver.finishEncoding()
        
        guard let dictData = zstdDictData else {
            print("Error: Zstd dictionary not loaded")
            return nil
        }
        
        guard let compressed = zstdCompress(data: data as Data, dictionary: dictData, level: 1) else {
            print("Error: Zstd compression failed")
            return nil
        }
        
        return compressed
    }
    
    func getRecord(for syncedEntity: SyncedEntity) -> CKRecord? {
        guard let recordData = syncedEntity.encodedRecord,
              let dictData = zstdDictData,
              let decompressed = zstdDecompress(data: recordData, dictionary: dictData),
              let unarchiver = try? NSKeyedUnarchiver(forReadingWith: decompressed) else {
            return nil
        }
        let record = CKRecord(coder: unarchiver)
        unarchiver.finishDecoding()
        return record
    }
    
    func nextStateToSync(after state: SyncedEntityState) -> SyncedEntityState {
        return SyncedEntityState(rawValue: state.rawValue + 1)!
    }
    
    @BigSyncBackgroundActor
    func recordsToUpload(withState state: SyncedEntityState, limit: Int) async throws -> [CKRecord] {
        guard let results = realmProvider?.persistenceRealm?.objects(SyncedEntity.self).where({ $0.state == state.rawValue }) else { return [] }
        var resultArray = [CKRecord]()
        var includedEntityIDs = Set<String>()
        
#if DEBUG
        // Ensure dummy records are uploaded first
        let resultsToUpload = Array(results).sorted {
            let isFirstDummy = dummyRecordIdentifiers.contains($0.identifier)
            let isSecondDummy = dummyRecordIdentifiers.contains($1.identifier)
            
            if isFirstDummy && !isSecondDummy {
                return true
            } else if !isFirstDummy && isSecondDummy {
                return false
            } else {
                return $0.identifier > $1.identifier // fallback sort
            }
        }
#else
        let resultsToUpload = Array(results)
#endif
        
        for syncedEntity in resultsToUpload {
            if resultArray.count >= limit {
                break
            }
            
            if !hasRealmObjectClass(name: syncedEntity.entityType) {
                continue
            }
            
            var entity: SyncedEntity! = syncedEntity
            while entity != nil && entity.state == state.rawValue && !includedEntityIDs.contains(entity.identifier) {
                var parentEntity: SyncedEntity? = nil
                guard let record = try await recordToUpload(syncedEntity: entity, parentSyncedEntity: &parentEntity) else {
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
    
    @BigSyncBackgroundActor
    func recordToUpload(syncedEntity: SyncedEntity, parentSyncedEntity: inout SyncedEntity?) async throws -> CKRecord? {
        let record = getRecord(for: syncedEntity) ?? CKRecord(recordType: syncedEntity.entityType, recordID: CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        
        guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
            return nil
        }
        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
        let object = realmProvider?.targetReaderRealmPerSchemaName[objectClass.className()]?.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
        let entityState = syncedEntity.state
        
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
        guard let object else {
            // Object does not exist, but tracking syncedEntity thinks it does.
            // We mark it as deleted so the iCloud record will get deleted too
            try await persistenceRealm.asyncWrite {
                syncedEntity.entityState = .deleted
            }
            return nil
        }
        
        let skippedKeys: Set<String>
        if let skippable = object as? SyncSkippablePropertiesModel {
            skippedKeys = Set(await skippable.skipSyncingProperties() ?? [])
        } else {
            skippedKeys = []
        }
        let defaultObject: Object? = skippedKeys.isEmpty ? nil : type(of: object).init()
        
        //        let changedKeys = (syncedEntity.changedKeys ?? "").components(separatedBy: ",")
        
        //        var parentKey: String?
        //        if let childObject = object as? ParentKey {
        //            parentKey = type(of: childObject).parentKey()
        //        }
        
        for property in object.objectSchema.properties {
            guard !cancelSync else { throw CancellationError() }
            
            //            if object.objectSchema.className == "HistoryRecord" && property.name == "content" && record.id == "6657C67E-95EC-479B-B5F5-9F7F44EAB1C5" {
            //                debugPrint(property)
            //            }
            if entityState == SyncedEntityState.new.rawValue || entityState == SyncedEntityState.changed.rawValue {
                if skippedKeys.contains(property.name) {
                    let defaultValue = defaultObject?.value(forKey: property.name)
                    if let ckValue = defaultValue as? CKRecordValue {
                        record[property.name] = ckValue
                    } else {
                        record[property.name] = nil
                    }
                    continue
                }
                
                if let recordProcessingDelegate = recordProcessingDelegate,
                   !recordProcessingDelegate.shouldProcessPropertyBeforeUpload(propertyName: property.name, object: object, record: record) {
                    continue
                }
                
                if property.type == PropertyType.object {
                    if let target = object.value(forKey: property.name) as? Object {
                        let targetPrimaryKey = (type(of: target).primaryKey() ?? target.objectSchema.primaryKeyProperty?.name)!
                        let targetIdentifier = Self.getTargetObjectStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
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
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
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
                    case .UUID:
                        guard let set = value as? Set<UUID>, !set.isEmpty else { break }
                        let array = set.map { $0.uuidString }
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of Set is not supported yet
                        debugPrint("Warning: Unsupported recordToUpload set property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if property.isMap {
                    // CloudKit does not support native dictionary/map values
                    // Convert to a 2-tuple: (keys: [String], values: [CKRecordValue])
                    guard let dict = object.value(forKey: property.name) as? [String: Any] else { break }
                    let keys = NSMutableArray()
                    let values = NSMutableArray()
                    for (key, value) in dict {
                        keys.add(key)
                        switch property.type {
                        case .int:
                            if let val = value as? Int { values.add(val as CKRecordValue) }
                        case .string:
                            if let val = value as? String { values.add(val as CKRecordValue) }
                        case .bool:
                            if let val = value as? Bool { values.add(val as CKRecordValue) }
                        case .float:
                            if let val = value as? Float { values.add(val as CKRecordValue) }
                        case .double:
                            if let val = value as? Double { values.add(val as CKRecordValue) }
                        case .date:
                            if let val = value as? Date { values.add(val as CKRecordValue) }
                        case .UUID:
                            if let val = value as? UUID { values.add(val.uuidString as CKRecordValue) }
                        default:
                            debugPrint("Warning: Unsupported recordToUpload map property type \(property.type) for \(String(describing: type(of: object)))")
                            break
                        }
                    }
                    record[property.name] = [keys, values] as CKRecordValue
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
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
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
                    case .UUID:
                        guard let list = value as? List<UUID>, !list.isEmpty else { break }
                        let array = Array(list.map { $0.uuidString })
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of List is not supported yet
                        debugPrint("Warning: Unsupported recordToUpload array property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if (
                    property.type != PropertyType.linkingObjects &&
                    !(property.name == (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!)
                ) {
                    let value = object.value(forKey: property.name)
                    if property.type == PropertyType.data,
                       let data = value as? Data,
                       !forceDataTypeInsteadOfAsset {
                        let fileURL = self.persistentAssetManager.store(data: data, forRecordID: syncedEntity.identifier)
                        guard !cancelSync else { throw CancellationError() }
                        
                        //                        logger.info("QSCloudKitSynchronizer >> Stored CKAsset data at \(fileURL) for \(property.name) of \(syncedEntity.identifier)")
                        let asset = CKAsset(fileURL: fileURL)
                        record[property.name] = asset
                    } else if value == nil {
                        record[property.name] = nil
                    } else if property.type == PropertyType.UUID, let uuid = value as? UUID {
                        record[property.name] = uuid.uuidString as CKRecordValue
                    } else if let recordValue = value as? CKRecordValue {
                        record[property.name] = recordValue
                    }
                }
                
            }
        }
        
#if DEBUG
        if dummyRecordIdentifiers.contains(syncedEntity.identifier) {
            for property in object.objectSchema.properties {
                let isNil = record[property.name] == nil
                let isEmptyArrayOrSet = (property.isArray || property.isSet) && ((record[property.name] as? [Any])?.isEmpty ?? false)
                let isEmptyMap = property.isMap && ((record[property.name] as? [String: Any])?.isEmpty ?? false)
                if isNil || isEmptyArrayOrSet || isEmptyMap {
                    if property.isMap {
                        // Store dummy map as 2-tuple: ([keys], [values])
                        switch property.type {
                        case .int:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: 0)] as CKRecordValue
                        case .string:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: "dummy")] as CKRecordValue
                        case .bool:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: false)] as CKRecordValue
                        case .float:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: Float(0.0))] as CKRecordValue
                        case .double:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: Double(0.0))] as CKRecordValue
                        case .date:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: Date())] as CKRecordValue
                        case .UUID:
                            record[property.name] = [NSArray(object: "dummyKey"), NSArray(object: UUID().uuidString)] as CKRecordValue
                        default:
                            fatalError("Unaccounted for property \(property)")
                        }
                    } else if property.isArray || property.isSet {
                        switch property.type {
                        case .int:
                            record[property.name] = [0] as CKRecordValue
                        case .string:
                            record[property.name] = ["dummy"] as CKRecordValue
                        case .bool:
                            record[property.name] = [false] as CKRecordValue
                        case .float:
                            record[property.name] = [Float(0.0)] as CKRecordValue
                        case .double:
                            record[property.name] = [Double(0.0)] as CKRecordValue
                        case .date:
                            record[property.name] = [Date()] as CKRecordValue
                        case .UUID:
                            record[property.name] = [UUID().uuidString] as CKRecordValue
                        default:
                            fatalError("Unaccounted for property \(property)")
                        }
                    } else {
                        switch property.type {
                        case .string:
                            record[property.name] = "dummy" as CKRecordValue
                        case .int:
                            record[property.name] = 0 as CKRecordValue
                        case .bool:
                            record[property.name] = false as CKRecordValue
                        case .date:
                            record[property.name] = Date() as CKRecordValue
                        case .float:
                            record[property.name] = Float(0.0) as CKRecordValue
                        case .double:
                            record[property.name] = Double(0.0) as CKRecordValue
                        case .data:
                            let dummyData = "dummy".data(using: .utf8)!
                            let fileURL = self.persistentAssetManager.store(data: dummyData, forRecordID: "Dummy.123")
                            let asset = CKAsset(fileURL: fileURL)
                            record[property.name] = asset
                        case .UUID:
                            record[property.name] = UUID().uuidString as CKRecordValue
                        default:
                            fatalError("Unaccounted for property \(property)")
                        }
                    }
                }
            }
        }
#endif
        
        return record
    }
    
    /// Deletes soft-deleted objects.
    public func cleanUp() {
        guard let targetWriterRealms = syncRealmProvider?.syncTargetRealms else { return }
        
        for targetWriterRealm in targetWriterRealms {
            for schema in targetWriterRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else {
                    continue
                }
                let predicate = NSPredicate(format: "isDeleted == %@", NSNumber(booleanLiteral: true))
                let lazyResults = targetWriterRealm.objects(objectClass).filter(predicate)
                var results = Array(lazyResults)
                if results.isEmpty {
                    continue
                }
                if objectClass.self is any SyncableBase.Type {
                    results = results.filter { result in
                        guard let result = result as? (any SyncableBase) else {
                            fatalError("SyncableBase object class unexpectedly had non-SyncableBase element")
                        }
                        // Don't delete before the application server received the deletion.
                        return !result.needsSyncToAppServer
                    }
                }
#if DEBUG
                results = results.filter { object in
                    let objectClassName = object.objectSchema.className
                    let primaryKey = (object.objectSchema.primaryKeyProperty?.name)!
                    let identifier = "\(objectClassName).\(object.value(forKey: primaryKey)!)"
                    return !dummyRecordIdentifiers.contains(identifier)
                }
#endif
                var identifiersToDelete = [String]()
                results = results.filter { object in
                    // TODO: Consolidate with syncedEntity(for ...)
                    guard let objectClass = self.realmObjectClass(name: object.objectSchema.className) else {
                        //                    debugPrint("Unexpectedly could not get realm object class for", object.objectSchema.className)
                        logger.error("Unexpectedly could not get realm object class for \(object.objectSchema.className)")
                        return false
                    }
                    let primaryKey = (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!
                    let identifier = object.objectSchema.className + "." + Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey)
                    let isSynced = {
                        guard let persistenceRealm = syncRealmProvider?.syncPersistenceRealm else { return false }
                        guard let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm) else {
                            //                        debugPrint("Warning: No synced entity found for identifier", identifier)
                            logger.error("Warning: No synced entity found for identifier \(identifier)")
                            return false
                        }
                        return syncedEntity.entityState == .synced || syncedEntity.entityState == .new
                    }()
                    if isSynced {
                        identifiersToDelete.append(identifier)
                    }
                    return isSynced
                }
                
                for chunk in results.chunks(ofCount: 500) {
                    try? targetWriterRealm.write {
                        for item in chunk {
                            targetWriterRealm.delete(item)
                        }
                    }
                }
                
                try? didDelete(identifiers: identifiersToDelete)
            }
        }
    }
    
    // MARK: - QSModelAdapter
    
    @BigSyncBackgroundActor
    public func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {
        guard let realmProvider = realmProvider else { return }
        guard !records.isEmpty else { return }
        
        //        debugPrint("# To save from icloud:", records.map { $0.recordID.recordName })
        var recordsToSave: [(record: CKRecord, objectClass: RealmSwift.Object.Type, objectIdentifier: Any, syncedEntityID: String, syncedEntityState: SyncedEntityState, entityType: String)] = []
        var syncedEntitiesToCreate: [SyncedEntity] = []
        
        for chunk in records.chunked(into: 100) {
            for record in chunk {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                var syncedEntity: SyncedEntity? = Self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: persistenceRealm)
                if syncedEntity == nil {
                    let newSyncedEntity = SyncedEntity(entityType: record.recordType, identifier: record.recordID.recordName, state: SyncedEntityState.synced.rawValue)
                    syncedEntitiesToCreate.append(newSyncedEntity)
                    syncedEntity = newSyncedEntity
                }
                if let syncedEntity {
                    if syncedEntity.entityState != .deleted && syncedEntity.entityType != "CKShare" {
                        guard let objectClass = self.realmObjectClass(name: record.recordType) else {
                            continue
                        }
                        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
                        
                        let recordToSave = (record, objectClass, objectIdentifier, syncedEntity.identifier, syncedEntity.entityState, syncedEntity.entityType)
                        
                        try Task.checkCancellation()
                        guard let object = realmProvider.targetReaderRealmPerSchemaName[objectClass.className()]?.object(ofType: objectClass, forPrimaryKey: objectIdentifier) else {
                            recordsToSave.append(recordToSave)
                            continue
                        }
                        
                        if forceSave || hasChanges(record: record, object: object) {
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
            
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try? await Task.sleep(nanoseconds: 10_000_000)
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
        }
        
        // TODO: Chunk based on target writer Realm
        if !recordsToSave.isEmpty {
            for chunk in recordsToSave.chunked(into: 100) {
                guard !cancelSync else { throw CancellationError() }
                
                //                await realmProvider.persistenceRealm?.asyncRefresh()
                try await realmProvider.persistenceRealm?.asyncWrite { [weak self] in
                    guard let self = self else { return }
                    
                    for (record, _, _, syncedEntityID, syncedEntityState, _) in chunk {
                        guard !cancelSync else { throw CancellationError() }
                        
                        if let remoteModified = record["modifiedAt"] as? Date {
                            self.recentlyFetchedRecordModifiedAts[syncedEntityID] = remoteModified
                        }
                        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                        if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: syncedEntityID) {
                            guard !cancelSync else { throw CancellationError() }
                            self.save(record: record, for: syncedEntity)
                        }
                    }
                }
                
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                try await { @RealmBackgroundActor in
                    guard let targetWriterRealms = realmProvider.targetWriterRealms else { return }
                    
                    // Group the chunk by the configuration from the associated target writer realm
                    let groupedChunk = Dictionary(grouping: chunk) { (record, objectType, objectIdentifier, syncedEntityID, syncedEntityState, entityType) -> AnyHashable? in
                        return realmProvider.targetWriterRealmPerSchemaName[objectType.className()]?.configuration as? AnyHashable
                    }
                    
                    for targetWriterRealm in targetWriterRealms {
                        await targetWriterRealm.asyncRefresh()
                        guard await !cancelSync else { throw CancellationError() }
                        
                        try await targetWriterRealm.asyncWrite { [weak self] in
                            guard let self else { return }
                            // Get the group for the current target writer realm's configuration
                            let group = groupedChunk[targetWriterRealm.configuration as? AnyHashable] ?? []
   
                            for (record, objectType, objectIdentifier, syncedEntityID, syncedEntityState, entityType) in group {
                                try Task.checkCancellation()
                                
                                var object = targetWriterRealm.object(ofType: objectType, forPrimaryKey: objectIdentifier)
                                if object == nil {
                                    object = objectType.init()
                                    if let object {
                                        object.setValue(objectIdentifier, forKey: (objectType.primaryKey() ?? objectType.sharedSchema()?.primaryKeyProperty?.name)!)
                                        targetWriterRealm.add(object, update: .modified)
                                    }
                                }
                                
                                try Task.checkCancellation()
                                if let object {
                                    self.applyChanges(
                                        in: record,
                                        to: object,
                                        syncedEntityID: syncedEntityID,
                                        syncedEntityState: syncedEntityState,
                                        entityType: entityType
                                    )
                                }
                            }
                        }
                    }
                }()
                
                try? await persistPendingRelationships()
                
                try Task.checkCancellation()
                try? await Task.sleep(nanoseconds: 100_000)
                try Task.checkCancellation()
            }
            
            logger.info("QSCloudKitSynchronizer >> Persisted \(recordsToSave.count) downloaded records")
            let savedRecordNames = Set(recordsToSave.map { $0.record.recordID.recordName })
            let skipped = records.map { $0.recordID.recordName } .filter { !savedRecordNames.contains($0) } .joined(separator: " ")
            if !skipped.isEmpty {
                logger.info("QSCloudKitSynchronizer >> Skipped downloaded records for having no changes: \(skipped)")
            }
            logger.info("QSCloudKitSynchronizer >> Persisted downloaded records: \(savedRecordNames.joined(separator: " "))")
        }
    }
    
    @BigSyncBackgroundActor
    public func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {
        guard let realmProvider = realmProvider else { return }
        guard recordIDs.count != 0 else { return }
        //        debugPrint("Deleting records with record ids \(recordIDs.map { $0.recordName })")
        
        var countDeleted = 0
        for recordID in recordIDs {
            try Task.checkCancellation()
            
            guard let persistenceRealm = realmProvider.persistenceRealm else { return }
            if let syncedEntity = Self.getSyncedEntity(objectIdentifier: recordID.recordName, realm: persistenceRealm) {
                try Task.checkCancellation()
                
                if syncedEntity.entityType != "CKShare" {
                    guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                        //                                    continue
                        return
                    }
                    let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                    
                    try? await { @RealmBackgroundActor in
                        try Task.checkCancellation()
                        guard let targetWriterRealm = realmProvider.targetWriterRealmPerSchemaName[objectClass.className()] else { return }
                        let object = targetWriterRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
                        
                        if let object {
                            await targetWriterRealm.asyncRefresh()
                            try Task.checkCancellation()
                            try? await targetWriterRealm.asyncWrite {
                                try Task.checkCancellation()
                                if let object = object as? SoftDeletable {
                                    object.isDeleted = true
                                } else {
                                    targetWriterRealm.delete(object)
                                }
                            }
                        }
                    }()
                }
                
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                //                await persistenceRealm.asyncRefresh()
                try? await persistenceRealm.asyncWrite {
                    try Task.checkCancellation()
                    persistenceRealm.delete(syncedEntity)
                }
            }
            
            countDeleted += 1
            if countDeleted % 20 == 0 {
                try? await Task.sleep(nanoseconds: 20_000)
                try Task.checkCancellation()
            }
        }
        
        logger.info("Deleted \(countDeleted) local records which were previously deleted from iCloud")
    }
    
    @BigSyncBackgroundActor
    public func persistImportedChanges() async throws {
        guard let realmProvider else { return }
        try await applyPendingRelationships(realmProvider: realmProvider)
    }
    
    @BigSyncBackgroundActor
    public func recordsToUpload(limit: Int) async throws -> [CKRecord] {
        guard let realmProvider else { return [] }
        var recordsArray = [CKRecord]()
        let recordLimit = limit == 0 ? Int.max : limit
        var uploadingState = SyncedEntityState.new
        
        var innerLimit = recordLimit
        while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deleted.rawValue {
            guard !cancelSync else { throw CancellationError() }
            
            try await recordsArray.append(
                contentsOf: self.recordsToUpload(
                    withState: uploadingState,
                    limit: innerLimit
                )
            )
            uploadingState = self.nextStateToSync(after: uploadingState)
            innerLimit = recordLimit - recordsArray.count
        }
        
        return recordsArray
    }
    
    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord]) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        
        for chunk in savedRecords.chunked(into: 500) {
            guard !cancelSync else { throw CancellationError() }
            
            //            await persistenceRealm.asyncRefresh()
            try? await persistenceRealm.asyncWrite {
                for record in chunk {
                    guard !cancelSync else { throw CancellationError() }
                    
                    if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: record.recordID.recordName) {
                        syncedEntity.state = SyncedEntityState.synced.rawValue
                        save(record: record, for: syncedEntity)
                    }
                }
            }
            try? await Task.sleep(nanoseconds: 20_000_000)
        }
        
        updateHasChanges(realm: persistenceRealm)
    }
    
    @BigSyncBackgroundActor
    public func recordIDsMarkedForDeletion(limit: Int) async throws -> [CKRecord.ID] {
        var recordIDs = [CKRecord.ID]()
        
        guard let deletedEntities = realmProvider?.persistenceRealm?.objects(SyncedEntity.self).where({ $0.state == SyncedEntityState.deleted.rawValue }) else { return [] }
        
        for syncedEntity in Array(deletedEntities) {
            guard !cancelSync else { throw CancellationError() }
            
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
                //                await persistenceRealm.asyncRefresh()
                try? await persistenceRealm.asyncWrite {
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
    }
    
    public func didDelete(identifiers: [String]) throws {
        guard let persistenceRealm = syncRealmProvider?.syncPersistenceRealm else { return }
        for identifier in identifiers {
            if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: identifier) {
                try persistenceRealm.write {
                    persistenceRealm.delete(syncedEntity)
                }
            }
        }
    }
    
    @BigSyncBackgroundActor
    public func didFinishImport(with error: Error?) async {
        guard let realmProvider, let persistenceRealm = realmProvider.persistenceRealm else { return }
        
        //        logger.info("QSCloudKitSynchronizer >> Clearing temporary CKAsset files")
        try? await updateCreatedAndModified()
        let pendingEntities = persistenceRealm.objects(SyncedEntity.self).where({ $0.state.in([SyncedEntityState.new.rawValue, SyncedEntityState.changed.rawValue]) })
        let pendingRecordIDs = Set(pendingEntities.map { $0.identifier })
        persistentAssetManager.clearAssetFiles(excludingSyncedEntityIDs: pendingRecordIDs)
        updateHasChanges(realm: persistenceRealm)
    }
    
    //    @BigSyncBackgroundActor
    //    public func deleteChangeTracking() async {
    //        await invalidateRealmAndTokens()
    //
    //        let config = self.persistenceRealmConfiguration
    //        let realmFileURLs: [URL] = [config.fileURL,
    //                                    config.fileURL?.appendingPathExtension("lock"),
    //                                    config.fileURL?.appendingPathExtension("note"),
    //                                    config.fileURL?.appendingPathExtension("management")
    //        ].compactMap { $0 }
    //
    //        for url in realmFileURLs where FileManager.default.fileExists(atPath: url.path) {
    //            do {
    //                try FileManager.default.removeItem(at: url)
    //            } catch {
    //                print("Error deleting file at \(url): \(error)")
    //            }
    //        }
    //    }
    
    @BigSyncBackgroundActor
    public func deleteChangeTracking(forRecordIDs recordIDs: [CKRecord.ID]) async throws {
        //        debugPrint("# deleteChangeTracking", recordIDs.map { $0.recordName })
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        
        for chunk in recordIDs.chunks(ofCount: 1000) {
            try await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    let identifier = recordID.recordName
                    guard let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm) else { continue }
                    
                    if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: identifier) {
                        persistenceRealm.delete(syncedEntity)
                    }
                }
            }
        }
    }
    
    public var recordZoneID: CKRecordZone.ID {
        return zoneID
    }
    
    public var serverChangeToken: CKServerChangeToken? {
        get async {
            return await { @BigSyncBackgroundActor in
                guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
                var token: CKServerChangeToken?
                let serverToken = persistenceRealm.objects(ServerToken.self).first
                if let tokenData = serverToken?.token {
                    token = NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
                }
                return token
            }()
        }
    }
    
    @BigSyncBackgroundActor
    public func saveToken(_ token: CKServerChangeToken?) async {
        //        debugPrint("# saveToken", token, recordZoneID)
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        //        await persistenceRealm.asyncRefresh()
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

private func zstdCompress(data: Data, dictionary: Data, level: Int) -> Data? {
    guard let cdict = dictionary.withUnsafeBytes({
        ZSTD_createCDict($0.baseAddress, dictionary.count, Int32(level))
    }) else { return nil }
    guard let cctx = ZSTD_createCCtx() else {
        ZSTD_freeCDict(cdict)
        return nil
    }
    
    let bound = ZSTD_compressBound(data.count)
    let dstBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bound)
    defer { dstBuffer.deallocate() }
    
    let compressedSize = data.withUnsafeBytes {
        ZSTD_compress_usingCDict(cctx, dstBuffer, bound, $0.baseAddress, data.count, cdict)
    }
    
    if ZSTD_isError(compressedSize) != 0 {
        print("Zstd compression error: \(String(cString: ZSTD_getErrorName(compressedSize)))")
        ZSTD_freeCDict(cdict)
        ZSTD_freeCCtx(cctx)
        return nil
    }
    
    ZSTD_freeCDict(cdict)
    ZSTD_freeCCtx(cctx)
    return Data(bytes: dstBuffer, count: compressedSize)
}

private func zstdDecompress(data: Data, dictionary: Data) -> Data? {
    guard let ddict = dictionary.withUnsafeBytes({
        ZSTD_createDDict($0.baseAddress, dictionary.count)
    }) else { return nil }
    guard let dctx = ZSTD_createDCtx() else {
        ZSTD_freeDDict(ddict)
        return nil
    }
    
    let expectedSize = ZSTD_getFrameContentSize(data.withUnsafeBytes { $0.baseAddress }, data.count)
    guard expectedSize != ZSTD_CONTENTSIZE_ERROR && expectedSize != ZSTD_CONTENTSIZE_UNKNOWN else {
        ZSTD_freeDDict(ddict)
        ZSTD_freeDCtx(dctx)
        return nil
    }
    
    let dstBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: Int(expectedSize))
    defer { dstBuffer.deallocate() }
    
    let actualSize = data.withUnsafeBytes {
        ZSTD_decompress_usingDDict(dctx, dstBuffer, Int(expectedSize), $0.baseAddress, data.count, ddict)
    }
    
    if ZSTD_isError(actualSize) != 0 {
        print("Zstd decompression error: \(String(cString: ZSTD_getErrorName(actualSize)))")
        ZSTD_freeDDict(ddict)
        ZSTD_freeDCtx(dctx)
        return nil
    }
    
    ZSTD_freeDDict(ddict)
    ZSTD_freeDCtx(dctx)
    return Data(bytes: dstBuffer, count: actualSize)
}
