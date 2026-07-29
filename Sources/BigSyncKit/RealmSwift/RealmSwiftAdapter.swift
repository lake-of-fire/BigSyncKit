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
import SwiftUtilities
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

extension Realm: @unchecked Sendable { }
extension CKRecord: @unchecked Sendable { }

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
    
    let targetWriterRealmPerSchemaName: [String: Realm]
    
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
        
        var targetWriterRealmPerSchemaName = [String: Realm]()
        for targetWriterRealmObject in targetConfigurations.map({ try! Realm(configuration: $0) }) {
            for objectType in targetWriterRealmObject.configuration.objectTypes ?? [] {
                targetWriterRealmPerSchemaName[objectType.className()] = targetWriterRealmObject
            }
        }
        self.targetWriterRealmPerSchemaName = targetWriterRealmPerSchemaName
        
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
            persistenceRealmObject = try await Realm(
                configuration: persistenceConfiguration,
                actor: BigSyncBackgroundActor.shared
            )
            //            debugPrint("# persistence realm", persistenceRealmObject.configuration.fileURL)
            
            var targetReaderRealmObjects = [Realm]()
            for targetConfiguration in targetConfigurations {
                try await targetReaderRealmObjects.append(
                    Realm(
                        configuration: targetConfiguration,
                        actor: BigSyncBackgroundActor.shared
                    )
                )
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
    var trackedChangeHighWatermarks: [String: Date] = [:]
}

private struct PendingObjectChange {
    let objectID: String
    let modifiedAt: Date?
    let explicitlyModifiedAt: Date?
}

private func latestExplicitlyModifiedAt(in changes: [PendingObjectChange]) -> Date? {
    changes.compactMap(\.explicitlyModifiedAt).max()
}

private func maxDate(_ lhs: Date?, _ rhs: Date?) -> Date? {
    switch (lhs, rhs) {
    case let (lhs?, rhs?):
        return max(lhs, rhs)
    case let (lhs?, nil):
        return lhs
    case let (nil, rhs?):
        return rhs
    case (nil, nil):
        return nil
    }
}

private func decodedCloudKitMap(_ value: Any?) -> [String: Any]? {
    if let data = value as? Data,
       let propertyList = try? PropertyListSerialization.propertyList(
            from: data,
            options: [],
            format: nil
       ) as? [String: Any] {
        return propertyList
    }

    // Read compatibility for records produced by early BigSyncKit builds.
    if let arrays = value as? [NSArray],
       arrays.count == 2,
       let keys = arrays[0] as? [String],
       let values = arrays[1] as? [Any],
       keys.count == values.count {
        return Dictionary(uniqueKeysWithValues: zip(keys, values))
    }
    return nil
}

private func encodedCloudKitMap(_ value: [String: Any]) throws -> Data {
    try PropertyListSerialization.data(
        fromPropertyList: value,
        format: .binary,
        options: 0
    )
}

extension RealmSwiftAdapter: @unchecked Sendable { }

public final class RealmSwiftAdapter: NSObject, @preconcurrency PrioritySyncCapableModelAdapter, UploadGenerationTrackingModelAdapter {
    private static var shouldSkipDebugDummySetup: Bool {
        let environment = ProcessInfo.processInfo.environment
        guard environment["MANABI_BIGSYNC_DEBUG_DUMMY_RECORDS"] == "1" else { return true }
        if environment["MANABI_UI_TEST_BYPASS_PASTEBOARD"] == "1" { return true }
        if environment["TEST_RUNNER_MANABI_TEST_USE_YOMITAN"] == "1" { return true }
        if environment["XCTestConfigurationFilePath"] != nil { return true }
        return false
    }

    public let persistenceRealmConfiguration: Realm.Configuration
    public let targetRealmConfigurations: [Realm.Configuration]
    public let excludedClassNames: [String]
    public let priorityEntityTypeNames: [String]
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
    @BigSyncBackgroundActor
    private let realmChangesSubject = PassthroughSubject<Int, Never>()
    
    private var pendingRelationshipQueue = [PendingRelationshipRequest]()
    
    @BigSyncBackgroundActor
    private var cancellables = Set<AnyCancellable>()
    
#if DEBUG
    @RealmBackgroundActor
    private var dummyRecordIdentifiers = Set<String>()
#endif
    
    private var isSetupInterrupted: Bool = false
    
    public init(
        persistenceRealmConfiguration: Realm.Configuration,
        targetRealmConfigurations: [Realm.Configuration],
        excludedClassNames: [String],
        priorityEntityTypeNames: [String] = [],
        recordZoneID: CKRecordZone.ID,
        logger: Logging.Logger,
        startSetupTask: Bool = true
    ) {
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfigurations = targetRealmConfigurations
        let internalClassNames = [BigSyncPendingMutation.className()]
        self.excludedClassNames = Array(Set(excludedClassNames + internalClassNames))
        self.priorityEntityTypeNames = priorityEntityTypeNames
        self.zoneID = recordZoneID
        self.logger = logger
        
        super.init()

        BigSyncMutationTrackingRegistry.register(
            configurations: targetRealmConfigurations,
            excluding: Set(self.excludedClassNames)
        )
        
        setupTypeNamesLookup()
        
        syncRealmProvider = SyncRealmProvider(
            persistenceConfiguration: persistenceRealmConfiguration,
            targetConfigurations: targetRealmConfigurations
        )
        
        if startSetupTask {
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                try await setup()
            }
        }
    }
    
    deinit {
        for cancellable in cancellables {
            cancellable.cancel()
        }
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
    
    @BigSyncBackgroundActor
    func invalidateTokens() {
        //        debugPrint("# invalidateRealmAndTokens()")
        for cancellable in cancellables {
            cancellable.cancel()
        }
        cancellables.removeAll()
    }
    
    static public func defaultPersistenceConfiguration() -> Realm.Configuration {
        var configuration = Realm.Configuration()
        configuration.schemaVersion = 7
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
    public func unsetCancellation() async throws {
        //        debugPrint("# unset cancel")
        cancelSync = false
        if isSetupInterrupted {
            try await setup()
        }
    }
    
    @BigSyncBackgroundActor
    func setup() async throws {
        logger.info("QSCloudKitSynchronizer >> Setup synchronization...")
        //        debugPrint("# setup() ...")
        isSetupInterrupted = false
        realmProvider = await RealmProvider(
            persistenceConfiguration: persistenceRealmConfiguration,
            targetConfigurations: targetRealmConfigurations
        )
        guard let realmProvider else { return }

        if let persistenceRealm = realmProvider.persistenceRealm {
            let pendingStates = [
                SyncedEntityState.new.rawValue,
                SyncedEntityState.changed.rawValue,
                SyncedEntityState.deletedLocally.rawValue,
            ]
            let entitiesMissingGeneration = persistenceRealm.objects(SyncedEntity.self)
                .where { $0.state.in(pendingStates) && $0.pendingGeneration == nil }
            if !entitiesMissingGeneration.isEmpty {
                try await persistenceRealm.asyncWrite {
                    for entity in entitiesMissingGeneration {
                        entity.pendingGeneration = UUID().uuidString
                    }
                }
            }
        }
        
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
        if !Self.shouldSkipDebugDummySetup {
        // Create a dummy record for each Realm type that has no data.
        // Check and write against the same writer realm, and only process each schema once.
        var processedDummySchemas = Set<String>()
        for targetReaderRealm in targetReaderRealms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard !processedDummySchemas.contains(schema.className) else { continue }
                processedDummySchemas.insert(schema.className)
                guard let objectClass = self.realmObjectClass(name: schema.className) else { continue }
                await { @RealmBackgroundActor in
                    do {
                        guard let targetWriterRealm = realmProvider.targetWriterRealmPerSchemaName[schema.className] else { return }
                        let writerURL = targetWriterRealm.configuration.fileURL?.path ?? "nil"
                        let writerTypedCount = targetWriterRealm.objects(objectClass).count
                        if writerTypedCount == 0 {
                            try targetWriterRealm.write {
                                let dummy = objectClass.init()
                                if let softDeletable = dummy as? SoftDeletable {
                                    softDeletable.isDeleted = true
                                }
                                targetWriterRealm.add(dummy, update: .modified)
                                let primaryKey = objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name
                                if let primaryKey {
                                    let dummyID = "\(schema.className).\(Self.getTargetObjectStringIdentifier(for: dummy, usingPrimaryKey: primaryKey))"
                                    dummyRecordIdentifiers.insert(dummyID)
                                    ()
                                } else {
                                    ()
                                }
                                let writerTypedCountAfter = targetWriterRealm.objects(objectClass).count
                                let writerDynamicCountAfter = targetWriterRealm.dynamicObjects(schema.className).count
                                ()
                            }
                        }
                    } catch {
                        ()
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
                    var identifiers: [String] = []
                    identifiers.reserveCapacity(results.count)
                    for result in results {
                        identifiers.append(entityTypePrefix + Self.getTargetObjectStringIdentifier(for: result, usingPrimaryKey: primaryKey))
                    }
                    do {
                        try await createSyncedEntities(entityType: schema.className, identifiers: identifiers)
                    } catch is CancellationError {
                        isSetupInterrupted = true
                        return
                    } catch {
                        isSetupInterrupted = true
                        throw error
                    }
                }
            }
        }
        
        //        if !needsInitialSetup {
        do {
            try await createMissingSyncedEntities()
        } catch is CancellationError {
            isSetupInterrupted = true
            return
        } catch {
            isSetupInterrupted = true
            throw error
        }
        //        }
        
        // Removed startPollingForChanges() call
        
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        updateHasChanges(realm: persistenceRealm)

        // One timestamp-based recovery pass covers writes made by older builds,
        // unmanaged-object call sites, or a crash before the journal schema was
        // available. Normal Realm notifications use the changed-ID journal.
        if !needsInitialSetup {
            await enqueueCreatedAndModified()
            try await processEnqueuedChanges()
        }
        
        await setupPublisherDebouncer()
        observeRealmChanges()
        
        //        if hasChanges {
        //            Task { @BigSyncBackgroundActor in
        //                await modelAdapterDelegate?.hasChangesToUpload()
        //            }
        //        }
    }
    
    @BigSyncBackgroundActor
    private func observeAppForegroundNotifications() {
#if canImport(UIKit)
        NotificationCenter.default
            .publisher(for: UIApplication.willEnterForegroundNotification)
            .merge(with: NotificationCenter.default
                .publisher(for: UIApplication.didBecomeActiveNotification)
            )
            .sink { @Sendable [weak self] _ in
                guard let self else { return }
                Task { @MainActor [weak self] in
                    guard let self else { return }
                    immediateChecksSubject.send(())
                }
            }
            .store(in: &cancellables)
#endif
        
        appForegroundCancellable = immediateChecksSubject
            .debounceLeadingTrailing(for: .seconds(6), scheduler: bigSyncKitQueue)
            .sink { @Sendable [weak self] _ in
                guard let self else { return }
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
#if DEBUG
            .debounceLeadingTrailing(for: .seconds(2), scheduler: bigSyncKitQueue)
#else
            .delay(for: .seconds(4), scheduler: bigSyncKitQueue)
            .debounceLeadingTrailing(for: .seconds(10), scheduler: bigSyncKitQueue)
#endif
            .sink { @Sendable [weak self] idx in
                guard let self else { return }
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    guard let targetReaderRealms = self.realmProvider?.targetReaderRealms, idx < targetReaderRealms.count else { return }
                    let changedRealm = targetReaderRealms[idx]
                    if changedRealm.schema.objectSchema.contains(where: {
                        $0.className == BigSyncPendingMutation.className()
                    }) {
                        try await self.forwardPendingMutations(in: changedRealm)
                    } else {
                        // Compatibility fallback for clients that have not added
                        // BigSyncPendingMutation to their target Realm schema yet.
                        await self.enqueueCreatedAndModified(in: changedRealm)
                    }
                }
            }
            .store(in: &cancellables)
        
        // For each realm, observe changes and send an event to the subject
        for (idx, targetReaderRealm) in targetReaderRealms.enumerated() {
            let token = targetReaderRealm.observe { [weak self] _, _ in
                guard let self else { return }
                Task { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    realmChangesSubject.send(idx)
                }
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
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else { return }
        for targetReaderRealm in targetReaderRealms {
            if targetReaderRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
                try await forwardPendingMutations(in: targetReaderRealm)
            } else {
                await enqueueCreatedAndModified(in: targetReaderRealm)
            }
        }
        try await processEnqueuedChanges()
    }

    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(in targetReaderRealm: Realm) async throws -> Int {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return 0 }
        let trackedClassNames = BigSyncMutationTrackingRegistry.trackedClassNames(
            in: targetReaderRealm
        )
        let unboundMutations = BigSyncMutationTrackingRegistry.unboundMutations(
            for: trackedClassNames
        )
        if !unboundMutations.isEmpty {
            var persistedUnboundMutations = [BigSyncPendingMutationSnapshot]()
            try await targetReaderRealm.asyncWrite {
                for mutation in unboundMutations {
                    guard let objectClass = realmObjectClass(name: mutation.entityType),
                          let primaryKey = getObjectIdentifier(
                            stringObjectId: mutation.objectIdentifier,
                            entityType: mutation.entityType
                          ),
                          targetReaderRealm.object(
                            ofType: objectClass,
                            forPrimaryKey: primaryKey
                          ) != nil else {
                        continue
                    }
                    let existing = targetReaderRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: mutation.recordName
                    )
                    if let existing, existing.changedAt > mutation.changedAt {
                        persistedUnboundMutations.append(mutation)
                        continue
                    }
                    targetReaderRealm.add(
                        BigSyncPendingMutation(
                            recordName: mutation.recordName,
                            entityType: mutation.entityType,
                            objectIdentifier: mutation.objectIdentifier,
                            generation: mutation.generation,
                            changedAt: mutation.changedAt
                        ),
                        update: .modified
                    )
                    persistedUnboundMutations.append(mutation)
                }
            }
            for mutation in persistedUnboundMutations {
                BigSyncMutationTrackingRegistry.removeUnbound(
                    recordName: mutation.recordName,
                    generation: mutation.generation
                )
            }
        }

        let mutations = targetReaderRealm.objects(BigSyncPendingMutation.self)
        let pending = mutations.map { mutation in
            BigSyncPendingMutationSnapshot(
                recordName: mutation.recordName,
                entityType: mutation.entityType,
                objectIdentifier: mutation.objectIdentifier,
                generation: mutation.generation,
                changedAt: mutation.changedAt
            )
        }
        guard !pending.isEmpty else { return 0 }

        var forwardedCount = 0
        for chunk in pending.chunks(ofCount: 1000) {
            try await persistenceRealm.asyncWrite {
                for mutation in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    if persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: mutation.recordName
                    )?.pendingGeneration == mutation.generation {
                        continue
                    }
                    updateTracking(
                        objectIdentifier: mutation.objectIdentifier,
                        entityName: mutation.entityType,
                        inserted: false,
                        modified: true,
                        deleted: false,
                        generation: mutation.generation,
                        persistenceRealm: persistenceRealm
                    )
                    forwardedCount += 1
                }
            }
        }

        if forwardedCount > 0 {
            updateHasChanges(realm: persistenceRealm)
            await modelAdapterDelegate?.hasChangesToUpload()
        }
        return forwardedCount
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
        guard let targetReaderRealm = realmProvider?.targetReaderRealmPerSchemaName[schemaName] else {
            //            print("Could not get realms or syncedEntityType for \(schemaName)")
            logger.error("Could not get realms or syncedEntityType for \(schemaName)")
            return
        }
        
        // TODO: Optimize by not checking records that we just fetched which triggered this to be called
        
        let lastTrackedChangesAt = await getLastTrackedChangesAt(forEntityType: schemaName) ?? .distantPast
        let createdPredicate = NSPredicate(format: "createdAt > %@ AND explicitlyModifiedAt > %@", lastTrackedChangesAt as NSDate, lastTrackedChangesAt as NSDate)
        // TODO: Slightly possible for this to miss records that are created with the same explicitlyModifiedAt as the last enqueueing but that didn't exist yet the last time it was called
        let modifiedPredicate = NSPredicate(format: "explicitlyModifiedAt > %@ AND createdAt <= %@", lastTrackedChangesAt as NSDate, lastTrackedChangesAt as NSDate)
        
        let primaryKey = objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name ?? ""
        
        let prefix = schemaName + "."
        func pendingChanges(
            matching predicate: NSPredicate
        ) -> (changes: [PendingObjectChange], latestExplicitlyModifiedAt: Date?) {
            var changes: [PendingObjectChange] = []
            var latestExplicitlyModifiedAt: Date?
            for object in targetReaderRealm.objects(objectClass).filter(predicate) {
                let change = PendingObjectChange(
                    objectID: Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKey),
                    modifiedAt: object["modifiedAt"] as? Date,
                    explicitlyModifiedAt: object["explicitlyModifiedAt"] as? Date
                )
                latestExplicitlyModifiedAt = maxDate(latestExplicitlyModifiedAt, change.explicitlyModifiedAt)
                guard let fetchedModified = recentlyFetchedRecordModifiedAts[prefix + change.objectID],
                      let modifiedAt = change.modifiedAt else {
                    changes.append(change)
                    continue
                }
                if modifiedAt != fetchedModified {
                    changes.append(change)
                }
            }
            return (changes, latestExplicitlyModifiedAt)
        }
        
        let created = pendingChanges(matching: createdPredicate)
        let modified = pendingChanges(matching: modifiedPredicate)
        let filteredCreated = created.changes
        let filteredModified = modified.changes
        let observedCreatedOrModified = created.latestExplicitlyModifiedAt != nil || modified.latestExplicitlyModifiedAt != nil
        if let latestObservedExplicitlyModifiedAt = maxDate(
            created.latestExplicitlyModifiedAt,
            modified.latestExplicitlyModifiedAt
        ) {
            resultsChangeSet.trackedChangeHighWatermarks[schemaName] = max(
                resultsChangeSet.trackedChangeHighWatermarks[schemaName] ?? .distantPast,
                latestObservedExplicitlyModifiedAt
            )
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
            let updatedInsertions: (Set<String>, Date?) = (
                insertions.0.union(filteredCreated.map(\.objectID)),
                maxDate(insertions.1, latestExplicitlyModifiedAt(in: filteredCreated))
            )
            resultsChangeSet.insertions[schemaName] = updatedInsertions
        }
        if !filteredModified.isEmpty {
            let modifications = resultsChangeSet.modifications[schemaName, default: ([], nil)]
            let updatedModifications: (Set<String>, Date?) = (
                modifications.0.union(filteredModified.map(\.objectID)),
                maxDate(modifications.1, latestExplicitlyModifiedAt(in: filteredModified))
            )
            resultsChangeSet.modifications[schemaName] = updatedModifications
        }
        
        // Persist the new lastTrackedChangesAt
        //        await persistenceRealm.asyncRefresh()
        if observedCreatedOrModified {
            for change in filteredCreated {
                recentlyFetchedRecordModifiedAts.removeValue(forKey: prefix + change.objectID)
            }
            for change in filteredModified {
                recentlyFetchedRecordModifiedAts.removeValue(forKey: prefix + change.objectID)
            }
            
            //            debugPrint("# created or modified non-empty, resultsChangeSetPublisher send...", created.count, modified.count, resultsChangeSet.insertions, resultsChangeSet.modifications)
            resultsChangeSetPublisher.send(())
        }
    }

#if DEBUG
    @BigSyncBackgroundActor
    func _test_enqueueCreatedAndModifiedAndProcess(in realm: Realm) async throws {
        await enqueueCreatedAndModified(in: realm)
        try await processEnqueuedChanges()
    }

    @BigSyncBackgroundActor
    func _test_markRecentlyFetchedRecord(entityType: String, identifier: String, modifiedAt: Date) {
        recentlyFetchedRecordModifiedAts[entityType + "." + identifier] = modifiedAt
    }

    @BigSyncBackgroundActor
    @discardableResult
    func _test_forwardPendingMutations(in realm: Realm) async throws -> Int {
        try await forwardPendingMutations(in: realm)
    }
#endif
    
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
            
            for chunk in identifiers.chunks(ofCount: 2000) {
                //                await persistenceRealm.asyncRefresh()
                try await persistenceRealm.asyncWrite {
                    for identifier in chunk {
                        try Task.checkCancellation()
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
            
            for chunk in identifiers.chunks(ofCount: 2000) {
                //                await persistenceRealm.asyncRefresh()
                try await persistenceRealm.asyncWrite {
                    for identifier in chunk {
                        try Task.checkCancellation()
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
        
        try await Task.sleep(nanoseconds: 10_000_000)
        await persistenceRealm.asyncRefresh()
        
        var lastTrackedChangesAtUpdates: [(String, Date)] = []
        for (schema, latestExplicitlyModifiedAt) in currentChangeSet.trackedChangeHighWatermarks {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            guard let syncedEntityType = try? await getOrCreateSyncedEntityType(schema) else { continue }
            guard !cancelSync else { throw CancellationError() }

            if latestExplicitlyModifiedAt > (syncedEntityType.lastTrackedChangesAt ?? .distantPast) {
                lastTrackedChangesAtUpdates.append((syncedEntityType.entityType, latestExplicitlyModifiedAt))
            }
        }
        try Task.checkCancellation()
        guard !cancelSync else { throw CancellationError() }
        
        if !lastTrackedChangesAtUpdates.isEmpty {
            try await persistenceRealm.asyncWrite {
                for (syncedEntityType, latestExplicitlyModifiedAt) in lastTrackedChangesAtUpdates {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: syncedEntityType)?.lastTrackedChangesAt = latestExplicitlyModifiedAt
                }
            }
        }
        
        if hasChanges {
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    
    @BigSyncBackgroundActor
    private func setupPublisherDebouncer() {
        resultsChangeSetPublisher
            .debounceLeadingTrailing(for: .seconds(6), scheduler: bigSyncKitQueue)
            .sink { @Sendable [weak self] _ in
                guard let self else { return }
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    try await processEnqueuedChanges()
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
        let pendingStates = [
            SyncedEntityState.new.rawValue,
            SyncedEntityState.changed.rawValue,
            SyncedEntityState.deletedLocally.rawValue,
        ]
        let results = realm.objects(SyncedEntity.self).where { $0.state.in(pendingStates) }
        let count = results.count
        if hasChangesCount != count {
            let syncedCount = realm.objects(SyncedEntity.self).where { $0.state == SyncedEntityState.synced.rawValue } .count
            logger.debug("QSCloudKitSynchronizer >> \(count) changed records remaining to upload. \(syncedCount) records already marked as synced.")
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
        generation: String? = nil,
        persistenceRealm: Realm
    ) {
        let identifier = entityName + "." + objectIdentifier
        let pendingGeneration = generation ?? UUID().uuidString
        var isNewChange = false
        
        let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm)
        //        debugPrint("# updateTracking", identifier, "ins", inserted, "mod", modified, "syncedentity exists?", syncedEntity != nil)
        
        if deleted {
            isNewChange = true
            
            if let syncedEntity = syncedEntity {
                //                try? realmProvider.persistenceRealm.safeWrite {
                syncedEntity.state = SyncedEntityState.deletedLocally.rawValue
                syncedEntity.pendingGeneration = pendingGeneration
            }
        } else if syncedEntity == nil {
            let createdEntity = Self.createSyncedEntity(
                entityType: entityName,
                identifier: objectIdentifier,
                modified: false,
                realm: persistenceRealm
            )
            createdEntity.pendingGeneration = pendingGeneration
            //            debugPrint("!! createSyncedEntity for inserted", objectIdentifier)
            if inserted {
                isNewChange = true
            }
        } else if inserted {
            guard let syncedEntity else { return }
            isNewChange = true
            
            if syncedEntity.state != SyncedEntityState.new.rawValue {
                // Hack to avoid crashing issue: https://github.com/realm/realm-swift/issues/8333
                if let syncedEntity = Self.getSyncedEntity(objectIdentifier: identifier, realm: persistenceRealm), syncedEntity.state != SyncedEntityState.new.rawValue {
                    syncedEntity.state = SyncedEntityState.new.rawValue
                }
            }
            syncedEntity.pendingGeneration = pendingGeneration
        } else {
            guard let syncedEntity else { return }
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
            if modified {
                syncedEntity.pendingGeneration = pendingGeneration
            }
        }
        
        if !hasChanges && isNewChange {
            hasChanges = true
        }
    }
    
    @BigSyncBackgroundActor
    func createMissingSyncedEntities() async throws {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms, let persistenceRealm = realmProvider?.persistenceRealm else { return }
        
        var identifiersPerEntityType = [String: Set<String>]()

        for targetReaderRealm in targetReaderRealms {
            for schema in targetReaderRealm.schema.objectSchema where !excludedClassNames.contains(schema.className) {
                guard let objectClass = self.realmObjectClass(name: schema.className) else {
                    continue
                }

                guard let primaryKeyName = objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name else {
                    continue
                }

                let objects = targetReaderRealm.objects(objectClass)
                if objects.isEmpty {
                    continue
                }

                var identifierSet = identifiersPerEntityType[schema.className] ?? Set<String>()
                for object in objects {
                    guard !cancelSync else {
                        throw CancellationError()
                    }

                    let identifierSuffix = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: primaryKeyName)
                    identifierSet.insert(schema.className + "." + identifierSuffix)
                }
                identifiersPerEntityType[schema.className] = identifierSet
            }
        }

        guard !identifiersPerEntityType.isEmpty else { return }

        let syncedEntities = persistenceRealm.objects(SyncedEntity.self)
        var missingEntities = [String: [String]]()

        for (entityType, identifierSet) in identifiersPerEntityType {
            guard !cancelSync else {
                throw CancellationError()
            }

            let existingIdentifiers = Set(
                syncedEntities
                    .where { $0.entityType == entityType }
                    .map(\.identifier)
            )
            let missingIdentifiers = identifierSet.subtracting(existingIdentifiers)

            if !missingIdentifiers.isEmpty {
                missingEntities[entityType] = Array(missingIdentifiers)
            }
        }

        for (entityType, identifiers) in missingEntities {
            logger.info("QSCloudKitSynchronizer >> Create \(identifiers.count) missing synced entities for \(entityType)")
            try await createSyncedEntities(entityType: entityType, identifiers: identifiers)
        }
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    func createSyncedEntities(entityType: String, identifiers: [String]) async throws {
        //                debugPrint("Create synced entities", entityType, identifiers.count)
        //        logger.info("QSCloudKitSynchronizer >> Creating \(identifiers.count) SyncedEntity records for \(entityType)…")
        for chunk in identifiers.chunks(ofCount: 500) {
            guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
            try await persistenceRealm.asyncWrite {
                for identifier in chunk {
                    guard !cancelSync else {
                        throw CancellationError()
                    }
                    let syncedEntity = SyncedEntity(entityType: entityType, identifier: identifier, state: SyncedEntityState.new.rawValue)
                    syncedEntity.pendingGeneration = UUID().uuidString
                    persistenceRealm.add(syncedEntity, update: .modified)
                }
            }
            try await Task.sleep(nanoseconds: 20_000_000)
            //            await persistenceRealm.asyncRefresh()
        }
        //        logger.info("QSCloudKitSynchronizer >> Created \(identifiers.count) SyncedEntity records for \(entityType)")
    }
    
    @BigSyncBackgroundActor
    @discardableResult
    static func createSyncedEntity(entityType: String, identifier: String, modified: Bool, realm: Realm) -> SyncedEntity {
        let syncedEntity = SyncedEntity(entityType: entityType, identifier: "\(entityType).\(identifier)", state: modified ? SyncedEntityState.changed.rawValue : SyncedEntityState.new.rawValue)
        syncedEntity.pendingGeneration = UUID().uuidString
        
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
    
    @inline(__always)
    static func getTargetObjectStringIdentifier(for object: Object, usingPrimaryKey key: String) -> String {
        let objectId = object[key]
        let identifier: String
        if let value = objectId as? String {
            identifier = value
        } else if let value = objectId as? CustomStringConvertible {
            identifier = String(describing: value)
        } else {
            assertionFailure("Expected primary key \(key) on \(object.objectSchema.className) to be string-convertible")
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
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
        
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
    
    @BigSyncBackgroundActor
    func getLastTrackedChangesAt(forEntityType entityType: String) async -> Date? {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
        
        let syncedEntityType = persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: entityType)
        return syncedEntityType?.lastTrackedChangesAt
    }
    
    func shouldIgnore(key: String) -> Bool {
        return cloudKitSynchronizerMetadataKeys.contains(key)
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
            let existingValue = object[key]
            
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
                    guard let result = decodedCloudKitMap(newValue) else {
                        logger.warning("QSCloudKitSynchronizer >> Found unexpected property value: \(newValue)")
                        return true
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
    ) throws {
        let objectProperties = object.objectSchema.properties
        
        let skippedKeys: Set<String>
        if let skippable = object as? SyncSkippablePropertiesModel {
            skippedKeys = skippable.skipSyncingProperties() ?? []
        } else {
            skippedKeys = []
        }
        
        func applyChanges() throws {
            //            logger.info("QSCloudKitSynchronizer >> Applying changes (no conflict): \(object.objectSchema.className) – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date)")
            //#if DEBUG
            //            logger.info("QSCloudKitSynchronizer >> Applying changes (no conflict), local object: \(object.debugDescription) – remote object: \(record.debugDescription)")
            //#endif
            for property in objectProperties where !skippedKeys.contains(property.name) {
                try Task.checkCancellation()
                if shouldIgnore(key: property.name) {
                    continue
                }
                if property.type == .linkingObjects {
                    continue
                }
                try applyChange(
                    property: property,
                    record: record,
                    object: object,
                    syncedEntityIdentifier: syncedEntityID
                )
            }
        }
        
        if mergePolicy == .server {
            try applyChanges()
        } else if mergePolicy == .custom {
            var recordChanges = [String: Any]()
            for property in objectProperties where !skippedKeys.contains(property.name) {
                try Task.checkCancellation()
                if property.type == .linkingObjects {
                    continue
                }
                if !shouldIgnore(key: property.name) {
                    if let asset = record[property.name] as? CKAsset {
                        try Task.checkCancellation()
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
                acceptRemoteChange = try { adapter, changes, object in
                    guard adapter.hasRealmObjectClass(name: object.objectSchema.className) else {
                        logger.warning("QSCloudKitSynchronizer >> No object class found for '\(object.objectSchema.className)' in adapter")
                        return false
                    }
                    let remoteExplicitlyModifiedAt = changes["explicitlyModifiedAt"] as? Date ?? .distantPast
                    let localExplicitlyModifiedAt = object["explicitlyModifiedAt"] as? Date ?? .distantPast
                    let result: Bool
                    if remoteExplicitlyModifiedAt > localExplicitlyModifiedAt {
                        result = true
                    } else if remoteExplicitlyModifiedAt == localExplicitlyModifiedAt {
                        let remoteModifiedAt = changes["modifiedAt"] as? Date ?? .distantPast
                        let localModifiedAt = object["modifiedAt"] as? Date ?? .distantPast
                        result = remoteModifiedAt >= localModifiedAt
                    } else {
                        result = false
                    }
                    try Task.checkCancellation()
                    logger.info("QSCloudKitSynchronizer >> Conflict resolution: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\(localExplicitlyModifiedAt), remote explicitly modified=\(remoteExplicitlyModifiedAt) => accepted remote: \(result)")
                    //#if DEBUG
                    //                    try Task.checkCancellation()
                    //                    logger.info("QSCloudKitSynchronizer >> Conflict resolution object - local: \(object.description.prefix(5000))")
                    //                    try Task.checkCancellation()
                    //                    logger.info("QSCloudKitSynchronizer >> Conflict resolution object - remote: \(changes.description.prefix(5000))")
                    //#endif
                    return result
                }(self, recordChanges, object)
            }
            
            if acceptRemoteChange {
                if let remoteExplicitlyModifiedAt = record["explicitlyModifiedAt"] as? Date, let localExplicitlyModifiedAt = (object as? ChangeMetadataRecordable)?.explicitlyModifiedAt, remoteExplicitlyModifiedAt < localExplicitlyModifiedAt {
                    logger.warning("QSCloudKitSynchronizer >> WARNING: Applying changes with lower explicitlyModifiedAt: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date), syncedEntityState=\(syncedEntityState.rawValue)")
                }
                
                try applyChanges()
            } else {
                if let remoteExplicitlyModifiedAt = record["explicitlyModifiedAt"] as? Date, let localExplicitlyModifiedAt = (object as? ChangeMetadataRecordable)?.explicitlyModifiedAt, remoteExplicitlyModifiedAt < localExplicitlyModifiedAt {
                    logger.info("QSCloudKitSynchronizer >> Rejecting remote changes with lower explicitlyModifiedAt: \(object.objectSchema.className) \(object.primaryKeyValue ?? "") – local explicitly modified=\((object as? ChangeMetadataRecordable)?.explicitlyModifiedAt), remote explicitly modified=\(record["explicitlyModifiedAt"] as? Date), syncedEntityState=\(syncedEntityState.rawValue)")
                }
                // TODO: Ensure this local object is pending upload...
            }
        }
    }
    
    func applyChange(
        property: Property,
        record: CKRecord,
        object: Object,
        syncedEntityIdentifier: String
    ) throws {
        let key = property.name
        if key == object.objectSchema.primaryKeyProperty!.name {
            return
        }
        
        if let recordProcessingDelegate = recordProcessingDelegate,
           !recordProcessingDelegate.shouldProcessPropertyInDownload(propertyName: key, object: object, record: record) {
            return
        }
        
        let value = record[key]
        if (property.isSet || property.isArray || property.isMap),
           !record.allKeys().contains(key) {
            // A collection added in a newer schema is absent from older records.
            // Preserve the local/default value instead of assigning nil to Realm.
            return
        }
        
        // List/Set support forked from IceCream: https://github.com/caiyue1993/IceCream/blob/master/IceCream/Classes/CKRecordRecoverable.swift
        var recordValue: Any?
        if property.isSet {
            switch property.type {
            case .int:
                guard let value = record.value(forKey: property.name) as? [Int] else { break }
                var set = Set<Int>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .string:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                var set = Set<String>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .bool:
                guard let value = record.value(forKey: property.name) as? [Bool] else { break }
                var set = Set<Bool>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .float:
                guard let value = record.value(forKey: property.name) as? [Float] else { break }
                var set = Set<Float>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .double:
                guard let value = record.value(forKey: property.name) as? [Double] else { break }
                var set = Set<Double>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .data:
                guard let value = record.value(forKey: property.name) as? [Data] else { break }
                var set = Set<Data>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .date:
                guard let value = record.value(forKey: property.name) as? [Date] else { break }
                var set = Set<Date>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .UUID:
                if let stringArray = value as? [String] {
                    let set = try Set(stringArray.compactMap {
                        try Task.checkCancellation()
                        return UUID.init(uuidString: $0)
                    })
                    object.setValue(set, forKey: key)
                }
                return
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        try Task.checkCancellation()
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        try Task.checkCancellation()
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                }
                return
            default:
                break
            }
            try Task.checkCancellation()
            if let recordValue {
                object.setValue(recordValue, forKey: property.name)
            }
        } else if property.isArray {
            switch property.type {
            case .int:
                guard let value = record.value(forKey: property.name) as? [Int] else { break }
                let list = List<Int>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .string:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                let list = List<String>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .bool:
                guard let value = record.value(forKey: property.name) as? [Bool] else { break }
                let list = List<Bool>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .float:
                guard let value = record.value(forKey: property.name) as? [Float] else { break }
                let list = List<Float>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .double:
                guard let value = record.value(forKey: property.name) as? [Double] else { break }
                let list = List<Double>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .data:
                guard let value = record.value(forKey: property.name) as? [Data] else { break }
                let list = List<Data>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .date:
                guard let value = record.value(forKey: property.name) as? [Date] else { break }
                let list = List<Date>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .UUID:
                guard let value = record.value(forKey: property.name) as? [String] else { break }
                let list = List<UUID>()
                let newValues = try value.compactMap {
                    try Task.checkCancellation()
                    return UUID(uuidString: $0)
                }
                for item in newValues {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        // TODO: (If used anymore?) Maybe recordName should be let recordName = reference.recordID.recordName instead - GPT thought so... See elsewhere too
                        guard let recordName = reference.value(forKey: property.name) as? String else { return }
                        let separatorRange = recordName.range(of: ".")!
                        let objectIdentifier = String(recordName[separatorRange.upperBound...])
                        savePendingRelationshipAsync(name: property.name, syncedEntityID: syncedEntityIdentifier, targetIdentifier: objectIdentifier)
                    }
                }
                return
            default:
                break
            }
            try Task.checkCancellation()
            if let recordValue {
                object.setValue(recordValue, forKey: property.name)
            }
        } else if property.isMap {
            guard var result = decodedCloudKitMap(value) else { return }
            if property.type == .UUID {
                result = result.reduce(into: [:]) { converted, entry in
                    if let string = entry.value as? String, let uuid = UUID(uuidString: string) {
                        converted[entry.key] = uuid
                    }
                }
            }
            try Task.checkCancellation()
            object.setValue(result, forKey: property.name)
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
                try Task.checkCancellation()
                object.setValue(uuid, forKey: key)
            }
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
               let data = NSData(contentsOf: fileURL) {
                try Task.checkCancellation()
                object.setValue(data, forKey: key)
            }
        } else if value != nil || property.isOptional == true {
            // If property is not a relationship or value is nil and property is optional.
            // If value is nil and property is non-optional, it is ignored. This is something that could happen
            // when extending an object model with a new non-optional property, when an old record is applied to the object.
            //            let ref = ThreadSafeReference(to: object)
            //            debugPrint("!! applyChange", type(of: object), key, value.debugDescription.prefix(100))
            try Task.checkCancellation()
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
            
            guard let persistenceRealm = realmProvider?.persistenceRealm else { break }
            
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
                  syncedEntity.entityState != .deletedLocally && syncedEntity.entityState != .deletedRemotely else { continue }
            
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
                    //                    await targetWriterRealm.asyncRefresh()
                    try await targetWriterRealm.asyncWrite {
                        try Task.checkCancellation()
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
    func save(record: CKRecord, for syncedEntity: SyncedEntity) throws {
        try syncedEntity.encodedRecord = encodedRecord(record, onlySystemFields: true)
    }
    
    @BigSyncBackgroundActor
    func encodedRecord(_ record: CKRecord, onlySystemFields: Bool) throws -> Data? {
        let data = NSMutableData()
        let archiver = NSKeyedArchiver(forWritingWith: data)
        try Task.checkCancellation()
        if onlySystemFields {
            record.encodeSystemFields(with: archiver)
        } else {
            record.encode(with: archiver)
        }
        try Task.checkCancellation()
        archiver.finishEncoding()
        try Task.checkCancellation()
        
        guard let compressed = try ZSTDCompressor.shared.compress(data: data as Data) else {
            print("Error: Zstd compression failed")
            return nil
        }
        
        return compressed
    }
    
    @BigSyncBackgroundActor
    func getRecord(for syncedEntity: SyncedEntity) -> CKRecord? {
        guard let recordData = syncedEntity.encodedRecord,
              let decompressed = ZSTDCompressor.shared.decompress(data: recordData),
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

    private func prioritizedEntityType(
        from entityTypes: some Sequence<String>
    ) -> String? {
        let pendingEntityTypes = Set(entityTypes)
        guard !pendingEntityTypes.isEmpty else { return nil }
        for entityType in priorityEntityTypeNames where pendingEntityTypes.contains(entityType) {
            return entityType
        }
        return nil
    }

    @BigSyncBackgroundActor
    private func prioritizedEntityTypeWithPendingUploadOrDeletion() -> String? {
        guard let persistenceRealm = realmProvider?.persistenceRealm,
              !priorityEntityTypeNames.isEmpty else { return nil }
        let pendingStates = [
            SyncedEntityState.new.rawValue,
            SyncedEntityState.changed.rawValue,
            SyncedEntityState.deletedLocally.rawValue,
        ]
        let pendingEntities = persistenceRealm.objects(SyncedEntity.self)
        for entityType in priorityEntityTypeNames {
            if pendingEntities.where({
                $0.state.in(pendingStates) && $0.entityType == entityType
            }).first != nil {
                return entityType
            }
        }
        return nil
    }
    
    @BigSyncBackgroundActor
    func recordsToUpload(
        withState state: SyncedEntityState,
        limit: Int,
        restrictedToEntityType restrictedEntityType: String? = nil
    ) async throws -> [PreparedRecordUpload] {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return [] }
        let allResults = persistenceRealm.objects(SyncedEntity.self)
        let results: Results<SyncedEntity>
        if let restrictedEntityType {
            results = allResults.where {
                $0.state == state.rawValue && $0.entityType == restrictedEntityType
            }
        } else {
            results = allResults.where { $0.state == state.rawValue }
        }
        var resultArray = [PreparedRecordUpload]()
        var includedEntityIDs = Set<String>()

        func appendUploadRecords(
            matching include: (SyncedEntity) -> Bool
        ) async throws {
            for candidate in results where include(candidate) {
                if resultArray.count >= limit {
                    return
                }
                guard let syncedEntity = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: candidate.identifier
                ) else {
                    continue
                }
                try await appendUploadRecords(startingAt: syncedEntity)
            }
        }

        func appendUploadRecords(startingAt syncedEntity: SyncedEntity) async throws {
            if let restrictedEntityType, syncedEntity.entityType != restrictedEntityType {
                return
            }
            if resultArray.count >= limit {
                return
            }
            
            if !hasRealmObjectClass(name: syncedEntity.entityType) {
                return
            }
            
            var entity: SyncedEntity! = syncedEntity
            while entity != nil,
                  entity.state == state.rawValue,
                  !includedEntityIDs.contains(entity.identifier),
                  resultArray.count < limit {
                let entityIdentifier = entity.identifier
                let generation = entity.pendingGeneration
                var parentEntity: SyncedEntity? = nil
                guard let record = try await recordToUpload(syncedEntity: entity, parentSyncedEntity: &parentEntity) else {
                    entity = nil
                    continue
                }
                resultArray.append(
                    PreparedRecordUpload(
                        record: record,
                        generation: generation
                    )
                )
                includedEntityIDs.insert(entityIdentifier)
                entity = parentEntity
            }
        }

#if DEBUG
        // Ensure dummy records are uploaded first.
        let dummyRecordIdentifiers = await dummyRecordIdentifiers
        if dummyRecordIdentifiers.isEmpty {
            try await appendUploadRecords { _ in true }
        } else {
            try await appendUploadRecords { dummyRecordIdentifiers.contains($0.identifier) }
            if resultArray.count < limit {
                try await appendUploadRecords { !dummyRecordIdentifiers.contains($0.identifier) }
            }
        }
#else
        try await appendUploadRecords { _ in true }
#endif
        
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
                syncedEntity.entityState = .deletedLocally
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
                    let defaultValue = defaultObject?[property.name]
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
                    if let target = object[property.name] as? Object {
                        let targetPrimaryKey = (type(of: target).primaryKey() ?? target.objectSchema.primaryKeyProperty?.name)!
                        let targetIdentifier = Self.getTargetObjectStringIdentifier(for: target, usingPrimaryKey: targetPrimaryKey)
                        let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                        let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                        record[property.name] = recordID.recordName as CKRecordValue
                    } else {
                        record[property.name] = nil
                    }
                } else if property.isSet {
                    let value = object[property.name]
                    switch property.type {
                    case .object:
                        /// We may get MutableSet<Cat> here
                        /// The item cannot be casted as MutableSet<Object>
                        /// It can be casted at a low-level type `SetBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let set = value as? RLMSwiftCollectionBase else { break }
                        var referenceArray = [String]()
                        let wrappedSet = set._rlmCollection
                        for index in 0..<wrappedSet.count {
                            guard let object = wrappedSet[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray as CKRecordValue
                    case .int:
                        guard let set = value as? MutableSet<Int> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .string:
                        guard let set = value as? MutableSet<String> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .bool:
                        guard let set = value as? MutableSet<Bool> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .float:
                        guard let set = value as? MutableSet<Float> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .double:
                        guard let set = value as? MutableSet<Double> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .data:
                        guard let set = value as? MutableSet<Data> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .date:
                        guard let set = value as? MutableSet<Date> else { break }
                        let array = Array(set)
                        record[property.name] = array as CKRecordValue
                    case .UUID:
                        guard let set = value as? MutableSet<UUID> else { break }
                        let array = Array(set.map { $0.uuidString })
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of Set is not supported yet
                        logger.warning("Warning: Unsupported recordToUpload set property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if property.isMap {
                    // CloudKit rejects nested arrays and has no native dictionary
                    // value. A binary property list preserves supported Realm
                    // scalar map values in one CloudKit Data field.
                    let mapValue: [String: Any]?
                    switch property.type {
                    case .int:
                        mapValue = (object[property.name] as? Map<String, Int>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .string:
                        mapValue = (object[property.name] as? Map<String, String>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .bool:
                        mapValue = (object[property.name] as? Map<String, Bool>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .float:
                        mapValue = (object[property.name] as? Map<String, Float>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .double:
                        mapValue = (object[property.name] as? Map<String, Double>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .date:
                        mapValue = (object[property.name] as? Map<String, Date>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .data:
                        mapValue = (object[property.name] as? Map<String, Data>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value }
                        }
                    case .UUID:
                        mapValue = (object[property.name] as? Map<String, UUID>).map { realmMap in
                            realmMap.reduce(into: [:]) { $0[$1.key] = $1.value.uuidString }
                        }
                    default:
                        mapValue = nil
                    }
                    if let mapValue {
                        record[property.name] = try encodedCloudKitMap(mapValue) as CKRecordValue
                    } else {
                        logger.warning("Warning: Unsupported recordToUpload map property type \(property.type) for \(String(describing: type(of: object)))")
                    }
                } else if property.isArray {
                    // Array handling forked from IceCream: https://github.com/caiyue1993/IceCream/blob/b29dfe81e41cc929c8191c3266189a7070cb5bc5/IceCream/Classes/CKRecordConvertible.swift
                    let value = object[property.name]
                    switch property.type {
                    case .object:
                        /// We may get List<Cat> here
                        /// The item cannot be casted as List<Object>
                        /// It can be casted at a low-level type `ListBase`
                        /// Updated -- see: https://github.com/caiyue1993/IceCream/pull/256#issuecomment-1034336992
                        guard let list = value as? RLMSwiftCollectionBase else { break }
                        var referenceArray = [String]()
                        let wrappedArray = list._rlmCollection
                        for index in 0..<wrappedArray.count {
                            guard let object = wrappedArray[index] as? Object,
                                  let targetPrimaryKey = (type(of: object).primaryKey() ?? object.objectSchema.primaryKeyProperty?.name) else { continue }
                            if (object as? SoftDeletable)?.isDeleted == true { continue }
                            let targetIdentifier = Self.getTargetObjectStringIdentifier(for: object, usingPrimaryKey: targetPrimaryKey)
                            let referenceIdentifier = "\(property.objectClassName!).\(targetIdentifier)"
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray as CKRecordValue
                    case .int:
                        guard let list = value as? List<Int> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .string:
                        guard let list = value as? List<String> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .bool:
                        guard let list = value as? List<Bool> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .float:
                        guard let list = value as? List<Float> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .double:
                        guard let list = value as? List<Double> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .data:
                        guard let list = value as? List<Data> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .date:
                        guard let list = value as? List<Date> else { break }
                        let array = Array(list)
                        record[property.name] = array as CKRecordValue
                    case .UUID:
                        guard let list = value as? List<UUID> else { break }
                        let array = Array(list.map { $0.uuidString })
                        record[property.name] = array as CKRecordValue
                    default:
                        // Other inner types of List is not supported yet
                        logger.warning("Warning: Unsupported recordToUpload array property type \(property.type) for \(String(describing: type(of: object)))")
                        break
                    }
                } else if (
                    property.type != PropertyType.linkingObjects &&
                    !(property.name == (objectClass.primaryKey() ?? objectClass.sharedSchema()?.primaryKeyProperty?.name)!)
                ) {
                    let value = object[property.name]
                    if property.type == PropertyType.data,
                       let data = value as? Data,
                       !forceDataTypeInsteadOfAsset {
                        let fileURL = self.persistentAssetManager.store(
                            data: data,
                            forRecordID: syncedEntity.identifier,
                            propertyName: property.name
                        )
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
        if await dummyRecordIdentifiers.contains(syncedEntity.identifier) {
            for property in object.objectSchema.properties {
                let isNil = record[property.name] == nil
                let isEmptyArrayOrSet = (property.isArray || property.isSet) && ((record[property.name] as? [Any])?.isEmpty ?? false)
                let isEmptyMap = property.isMap && ((record[property.name] as? [String: Any])?.isEmpty ?? false)
                if isNil || isEmptyArrayOrSet || isEmptyMap {
                    if property.isMap {
                        // Store dummy map as 2-tuple: ([keys], [values])
                        switch property.type {
                        case .int:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": 0]) as CKRecordValue
                        case .string:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": "dummy"]) as CKRecordValue
                        case .bool:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": false]) as CKRecordValue
                        case .float:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": Float(0.0)]) as CKRecordValue
                        case .double:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": Double(0.0)]) as CKRecordValue
                        case .date:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": Date()]) as CKRecordValue
                        case .UUID:
                            record[property.name] = try encodedCloudKitMap(["dummyKey": UUID().uuidString]) as CKRecordValue
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
                            let fileURL = self.persistentAssetManager.store(
                                data: dummyData,
                                forRecordID: "Dummy.123",
                                propertyName: property.name
                            )
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
        
        //        debugPrint("# TO UPLOAD:", record.debugDescription)
        return record
    }
    
    /// Deletes soft-deleted objects.
    public func cleanUp() {
        guard let targetWriterRealmPerSchemaName = syncRealmProvider?.targetWriterRealmPerSchemaName, let persistenceRealm = syncRealmProvider?.syncPersistenceRealm else {
            print("WARNING: No sync realms found.")
            return
        }
        
        let remotelyDeletedEntities = Dictionary(
            grouping: persistenceRealm.objects(SyncedEntity.self).where { $0.state == SyncedEntityState.deletedRemotely.rawValue },
            by: { $0.entityType }
        ).filter { !excludedClassNames.contains($0.key) }
        
        // TODO: Group by target writer realms instead of write transaction per class name, for speed
        for className in remotelyDeletedEntities.keys {
            guard let targetWriterRealm = targetWriterRealmPerSchemaName[className] else { return }
            guard let objectClass = self.realmObjectClass(name: className) else {
                logger.warning("QSCloudKitSynchronizer >> Clean Up: No Realm object class found for \(className)")
                continue
            }
            
            do {
                var skippedIdentifiers = Set<String>()
                try targetWriterRealm.write {
                    for syncedEntity in remotelyDeletedEntities[className] ?? [] {
                        let objectIdentifier = self.getObjectIdentifier(for: syncedEntity)
                        if let object = targetWriterRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier) {
                            if let object = object as? SoftDeletable {
                                guard object.isDeleted else {
                                    logger.warning("QSCloudKitSynchronizer >> Clean Up: Object \(objectIdentifier) of type \(className) is not marked as deleted locally, skipping deletion")
                                    skippedIdentifiers.insert(syncedEntity.identifier)
                                    continue
                                }
                            }
                            
                            if let object = object as? any SyncableBase, object.needsSyncToAppServer {
                                // Don't hard-delete before the application server received the deletion.
                                skippedIdentifiers.insert(syncedEntity.identifier)
                                continue
                            }
                            
                            targetWriterRealm.delete(object)
                            logger.warning("QSCloudKitSynchronizer >> Clean Up: Hard-deleted local object \(objectIdentifier) of type \(className)")
                        } else {
                            logger.warning("QSCloudKitSynchronizer >> Clean Up: Object \(objectIdentifier) of type \(className) not found for deletion")
                        }
                    }
                }
                
                try persistenceRealm.write {
                    for syncedEntity in remotelyDeletedEntities[className] ?? [] where !skippedIdentifiers.contains(syncedEntity.identifier) {
                        logger.warning("QSCloudKitSynchronizer >> Clean Up: Deleting tracking of object \(syncedEntity.identifier)")
                        persistenceRealm.delete(syncedEntity)
                    }
                }
            } catch {
                logger.error("\(error)")
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
        try Task.checkCancellation()
        
        for chunk in records.chunks(ofCount: 200) {
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
                try Task.checkCancellation()
                
                if let syncedEntity {
                    if syncedEntity.entityState != .deletedLocally && syncedEntity.entityState != .deletedRemotely && syncedEntity.entityType != "CKShare" {
                        guard let objectClass = self.realmObjectClass(name: record.recordType) else {
                            continue
                        }
                        let objectIdentifier = getObjectIdentifier(for: syncedEntity)
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        
                        let recordToSave = (record, objectClass, objectIdentifier, syncedEntity.identifier, syncedEntity.entityState, syncedEntity.entityType)
                        guard !cancelSync else { throw CancellationError() }
                        try Task.checkCancellation()
                        
                        guard let object = realmProvider.targetReaderRealmPerSchemaName[objectClass.className()]?.object(ofType: objectClass, forPrimaryKey: objectIdentifier) else {
                            recordsToSave.append(recordToSave)
                            continue
                        }
                        guard !cancelSync else { throw CancellationError() }
                        try Task.checkCancellation()
                        
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
            try await Task.sleep(nanoseconds: 10_000_000)
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
        }
        
        // TODO: Chunk based on target writer Realm
        if !recordsToSave.isEmpty {
            for chunk in recordsToSave.chunks(ofCount: 100) {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                //                                await realmProvider.persistenceRealm?.asyncRefresh()
                try await realmProvider.persistenceRealm?.asyncWrite { [weak self] in
                    guard let self else { return }
                    
                    for (record, _, _, syncedEntityID, syncedEntityState, _) in chunk {
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        
                        if let remoteModified = record["modifiedAt"] as? Date {
                            recentlyFetchedRecordModifiedAts[syncedEntityID] = remoteModified
                        }
                        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                        try Task.checkCancellation()
                        
                        if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: syncedEntityID) {
                            guard !cancelSync else { throw CancellationError() }
                            try Task.checkCancellation()
                            try save(record: record, for: syncedEntity)
                        }
                    }
                }
                
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                let safeChunk: [(CKRecord, Object.Type, any Sendable, String, SyncedEntityState, String)] = chunk.map {
                    ($0.record, $0.objectClass, $0.objectIdentifier as! any Sendable, $0.syncedEntityID, $0.syncedEntityState, $0.entityType)
                }
                try await { @RealmBackgroundActor in
                    guard let targetWriterRealms = realmProvider.targetWriterRealms else { return }
                    
                    for targetWriterRealm in targetWriterRealms {
                        try Task.checkCancellation()
                        guard await !cancelSync else { throw CancellationError() }
                        await targetWriterRealm.asyncRefresh()
                        try Task.checkCancellation()
                        
                        try await targetWriterRealm.asyncWrite { [weak self] in
                            guard let self else { return }
                            for (record, objectType, objectIdentifier, syncedEntityID, syncedEntityState, entityType) in safeChunk {
                                try Task.checkCancellation()
                                guard realmProvider.targetWriterRealmPerSchemaName[objectType.className()]?.configuration == targetWriterRealm.configuration else {
                                    continue
                                }
                                try Task.checkCancellation()
                                
                                var object = targetWriterRealm.object(ofType: objectType, forPrimaryKey: objectIdentifier)
                                try Task.checkCancellation()
                                
                                if object == nil {
                                    object = objectType.init()
                                    try Task.checkCancellation()
                                    
                                    if let object {
                                        object.setValue(objectIdentifier, forKey: (objectType.primaryKey() ?? objectType.sharedSchema()?.primaryKeyProperty?.name)!)
                                        targetWriterRealm.add(object, update: .modified)
                                    }
                                }
                                
                                try Task.checkCancellation()
                                if let object {
                                    try self.applyChanges(
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
                guard !cancelSync else { throw CancellationError() }
                try await Task.sleep(nanoseconds: 100_000)
            }
            
            logger.info("QSCloudKitSynchronizer >> Persisted \(recordsToSave.count) downloaded records")
            let savedRecordNames = Set(recordsToSave.map { $0.record.recordID.recordName })
            let skipped = records.map { $0.recordID.recordName } .filter { !savedRecordNames.contains($0) } .joined(separator: " ")
            if !skipped.isEmpty {
                logger.info("QSCloudKitSynchronizer >> Skipped downloaded records for having no changes: \(skipped)")
            }
            //            logger.info("QSCloudKitSynchronizer >> Persisted downloaded record names: \(savedRecordNames.joined(separator: " "))")
            //#if DEBUG
            //            logger.info("QSCloudKitSynchronizer >> Persisted downloaded records: \(recordsToSave.map { ($0.record.recordID.recordName, $0.record.debugDescription) })")
            //#endif
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
                    
                    try await { @RealmBackgroundActor in
                        try Task.checkCancellation()
                        guard let targetWriterRealm = realmProvider.targetWriterRealmPerSchemaName[objectClass.className()] else { return }
                        let object = targetWriterRealm.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
                        
                        if let object {
                            //                            await targetWriterRealm.asyncRefresh()
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
                try await persistenceRealm.asyncWrite {
                    try Task.checkCancellation()
                    syncedEntity.state = SyncedEntityState.deletedRemotely.rawValue
                    syncedEntity.pendingGeneration = nil
                    //                    persistenceRealm.delete(syncedEntity)
                }
            }
            
            countDeleted += 1
            if countDeleted % 20 == 0 {
                try await Task.sleep(nanoseconds: 20_000)
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
    func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] {
        if !hasChanges {
            if let persistenceRealm = realmProvider?.persistenceRealm {
                updateHasChanges(realm: persistenceRealm)
            }
            if !hasChanges {
                return []
            }
        }

        var recordsArray = [PreparedRecordUpload]()
        let recordLimit = limit == 0 ? Int.max : limit
        var uploadingState = SyncedEntityState.new
        let targetEntityType = restrictedToEntityType ?? prioritizedEntityTypeWithPendingUploadOrDeletion()
        
        var innerLimit = recordLimit
        while recordsArray.count < recordLimit && uploadingState.rawValue < SyncedEntityState.deletedLocally.rawValue {
            guard !cancelSync else { throw CancellationError() }
            
            try await recordsArray.append(
                contentsOf: self.recordsToUpload(
                    withState: uploadingState,
                    limit: innerLimit,
                    restrictedToEntityType: targetEntityType
                )
            )
            uploadingState = self.nextStateToSync(after: uploadingState)
            innerLimit = recordLimit - recordsArray.count
        }
        
        return recordsArray
    }

    @BigSyncBackgroundActor
    func recordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [CKRecord] {
        try await preparedRecordsToUpload(
            limit: limit,
            restrictedToEntityType: restrictedToEntityType
        ).map(\.record)
    }

    @BigSyncBackgroundActor
    public func recordsToUpload(limit: Int) async throws -> [CKRecord] {
        try await recordsToUpload(limit: limit, restrictedToEntityType: nil)
    }
    
    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord]) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        let matchingGenerations = savedRecords.reduce(into: [String: String]()) {
            guard let syncedEntity = persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: $1.recordID.recordName
            ), let generation = syncedEntity.pendingGeneration else { return }
            $0[$1.recordID.recordName] = generation
        }
        try await didUpload(
            savedRecords: savedRecords,
            matchingGenerations: matchingGenerations
        )
    }

    @BigSyncBackgroundActor
    func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()
        
        for chunk in savedRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            
            //            await persistenceRealm.asyncRefresh()
            try await persistenceRealm.asyncWrite {
                for record in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    
                    if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: record.recordID.recordName) {
                        try Task.checkCancellation()
                        try save(record: record, for: syncedEntity)
                        if let uploadedGeneration = matchingGenerations[record.recordID.recordName],
                           syncedEntity.pendingGeneration == uploadedGeneration {
                            syncedEntity.state = SyncedEntityState.synced.rawValue
                            syncedEntity.pendingGeneration = nil
                            acknowledgedGenerations[record.recordID.recordName] = uploadedGeneration
                        }
                    }
                }
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        if !acknowledgedGenerations.isEmpty {
            if let targetReaderRealms = realmProvider.targetReaderRealms {
                for targetReaderRealm in targetReaderRealms where targetReaderRealm.schema.objectSchema.contains(where: {
                    $0.className == BigSyncPendingMutation.className()
                }) {
                    try await targetReaderRealm.asyncWrite {
                        for (recordName, generation) in acknowledgedGenerations {
                            guard let mutation = targetReaderRealm.object(
                                ofType: BigSyncPendingMutation.self,
                                forPrimaryKey: recordName
                            ), mutation.generation == generation else { continue }
                            targetReaderRealm.delete(mutation)
                        }
                    }
                    try await forwardPendingMutations(in: targetReaderRealm)
                }
            }
        }
        
        updateHasChanges(realm: persistenceRealm)
    }
    
    @BigSyncBackgroundActor
    func recordIDsMarkedForDeletion(limit: Int, restrictedToEntityType: String?) async throws -> [CKRecord.ID] {
        var recordIDs = [CKRecord.ID]()
        
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return [] }
        let targetEntityType = restrictedToEntityType ?? prioritizedEntityTypeWithPendingUploadOrDeletion()
        let allEntities = persistenceRealm.objects(SyncedEntity.self)
        let deletedEntities: Results<SyncedEntity>
        if let targetEntityType {
            deletedEntities = allEntities.where {
                $0.state == SyncedEntityState.deletedLocally.rawValue &&
                $0.entityType == targetEntityType
            }
        } else {
            deletedEntities = allEntities.where {
                $0.state == SyncedEntityState.deletedLocally.rawValue
            }
        }
        
        for syncedEntity in deletedEntities {
            guard !cancelSync else { throw CancellationError() }
            if recordIDs.count >= limit {
                break
            }
            recordIDs.append(CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        }
        
        return recordIDs
    }

    @BigSyncBackgroundActor
    public func recordIDsMarkedForDeletion(limit: Int) async throws -> [CKRecord.ID] {
        try await recordIDsMarkedForDeletion(limit: limit, restrictedToEntityType: nil)
    }
    
    @BigSyncBackgroundActor
    public func didDelete(recordIDs deletedRecordIDs: [CKRecord.ID]) async {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }

        for chunk in deletedRecordIDs.chunks(ofCount: 1000) {
            try? await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    guard let syncedEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: recordID.recordName
                    ) else { continue }
                    syncedEntity.state = SyncedEntityState.deletedRemotely.rawValue
                    syncedEntity.pendingGeneration = nil
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
    public func didFinishImport() async {
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
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        
        for chunk in recordIDs.chunks(ofCount: 1000) {
            try await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    let identifier = recordID.recordName
                    guard let syncedEntity = Self.getSyncedEntity(
                        objectIdentifier: identifier,
                        realm: persistenceRealm
                    ) else { continue }
                    syncedEntity.entityState = .new
                    syncedEntity.encodedRecord = nil
                    syncedEntity.pendingGeneration = UUID().uuidString
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

@BigSyncBackgroundActor
final class ZSTDCompressor {
    /// A single shared compressor instance for the entire app.
    static let shared = ZSTDCompressor()
    
    private let cdict: OpaquePointer?
    private let ddict: OpaquePointer?
    
    private init() {
        guard let dictURL = Bundle.module.url(forResource: "ckrecordDictionary", withExtension: nil, subdirectory: "zstd"),
              let data = try? Data(contentsOf: dictURL) else {
            fatalError("Error: Failed to load zstd dictionary during init")
        }
        
        let level: Int32 = 1
        let tempCDict = data.withUnsafeBytes {
            ZSTD_createCDict($0.baseAddress, data.count, level)
        }
        if tempCDict == nil {
            fatalError("Failed to create ZSTD dictionaries.")
        }
        self.cdict = tempCDict
        
        let tempDDict = data.withUnsafeBytes {
            ZSTD_createDDict($0.baseAddress, data.count)
        }
        if tempDDict == nil {
            fatalError("Failed to create ZSTD dictionaries.")
        }
        self.ddict = tempDDict
    }
    
    //    func close() {
    //        if let cdict {
    //            ZSTD_freeCDict(cdict)
    //        }
    //        if let ddict {
    //            ZSTD_freeDDict(ddict)
    //        }
    //    }
    
    /// Compress the provided data using the cached dictionary.
    func compress(data: Data) throws -> Data? {
        guard let cdict else {
            return nil
        }
        // Create a new compression context for each call.
        guard let cctx = ZSTD_createCCtx() else {
            return nil
        }
        defer { ZSTD_freeCCtx(cctx) }
        
        let bound = ZSTD_compressBound(data.count)
        try Task.checkCancellation()
        let dstBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bound)
        defer { dstBuffer.deallocate() }
        try Task.checkCancellation()
        
        let compressedSize = try data.withUnsafeBytes {
            try Task.checkCancellation()
            return ZSTD_compress_usingCDict(
                cctx,
                dstBuffer,
                bound,
                $0.baseAddress,
                data.count,
                cdict
            )
        }
        
        if ZSTD_isError(compressedSize) != 0 {
            print("Zstd compression error: \(String(cString: ZSTD_getErrorName(compressedSize)))")
            return nil
        }
        
        try Task.checkCancellation()
        return Data(bytes: dstBuffer, count: compressedSize)
    }
    
    /// Decompress data using the cached dictionary.
    func decompress(data: Data) -> Data? {
        guard let ddict else {
            return nil
        }
        // Create a new decompression context each time.
        guard let dctx = ZSTD_createDCtx() else {
            return nil
        }
        defer { ZSTD_freeDCtx(dctx) }
        
        let expectedSize = ZSTD_getFrameContentSize(data.withUnsafeBytes { $0.baseAddress }, data.count)
        guard expectedSize != ZSTD_CONTENTSIZE_ERROR && expectedSize != ZSTD_CONTENTSIZE_UNKNOWN else {
            return nil
        }
        
        let dstBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: Int(expectedSize))
        defer { dstBuffer.deallocate() }
        
        let actualSize = data.withUnsafeBytes {
            ZSTD_decompress_usingDDict(
                dctx,
                dstBuffer,
                Int(expectedSize),
                $0.baseAddress,
                data.count,
                ddict
            )
        }
        
        if ZSTD_isError(actualSize) != 0 {
            print("Zstd decompression error: \(String(cString: ZSTD_getErrorName(actualSize)))")
            return nil
        }
        
        return Data(bytes: dstBuffer, count: actualSize)
    }
}
