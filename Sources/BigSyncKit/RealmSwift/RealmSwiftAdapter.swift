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

enum BigSyncCloudKitRecordNameError: Error, Equatable, LocalizedError {
    case empty
    case nonASCII(String)
    case exceedsMaximumLength(actual: Int)
    case reservedPrefix

    var errorDescription: String? {
        switch self {
        case .empty:
            return "CloudKit record names must not be empty."
        case .nonASCII(let recordName):
            return "CloudKit record name contains non-ASCII characters: \(recordName)"
        case .exceedsMaximumLength(let actual):
            return "CloudKit record name is \(actual) characters; the maximum is 255."
        case .reservedPrefix:
            return "CloudKit record names must not start with an underscore."
        }
    }
}
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

struct PendingRelationshipRequest: Sendable {
    let name: String
    let syncedEntityID: String
    let targetIdentifiers: [String]
    let sourceRecordChangeTag: String?
    let expectedModifiedAt: Date?
    let expectedExplicitlyModifiedAt: Date?
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

private struct RemoteDeletionSnapshot: Sendable {
    let recordName: String
    let entityType: String
    let objectIdentifier: String
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

private func objectIdentifier(fromCloudKitRecordName recordName: String) -> String? {
    guard let separator = recordName.firstIndex(of: ".") else { return nil }
    let identifierStart = recordName.index(after: separator)
    guard identifierStart < recordName.endIndex else { return nil }
    return String(recordName[identifierStart...])
}

extension RealmSwiftAdapter: @unchecked Sendable { }

public final class RealmSwiftAdapter: NSObject, @preconcurrency PrioritySyncCapableModelAdapter, UploadGenerationTrackingModelAdapter {
    private static let mutationJournalRecoveryEntityType =
        "__BigSyncKitMutationJournalRecovery"
    private static let mutationJournalRecoveryVersion = 1

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
    private let realmChangesSubject = PassthroughSubject<Void, Never>()
    @BigSyncBackgroundActor
    private var observedJournalMutations = [
        Int: [String: BigSyncPendingMutationSnapshot]
    ]()
    @BigSyncBackgroundActor
    private var changedLegacyRealmIndexes = Set<Int>()
    
    @BigSyncBackgroundActor
    private var cancellables = Set<AnyCancellable>()
    
#if DEBUG
    @RealmBackgroundActor
    private var dummyRecordIdentifiers = Set<String>()
    var _testBeforeRemoteDeletionTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeCleanupTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
#endif
    
    private var isSetupInterrupted: Bool = false
    @BigSyncBackgroundActor
    private var setupTask: Task<Void, Error>?
    @BigSyncBackgroundActor
    private var setupGeneration = UUID()
    
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

        BigSyncMutationTracking.install(
            configurations: targetRealmConfigurations,
            excludedClassNames: self.excludedClassNames
        )
        
        setupTypeNamesLookup()
        
        if startSetupTask {
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                try await ensureSetup()
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
        let activeSetupTask = setupTask
        activeSetupTask?.cancel()
        _ = try? await activeSetupTask?.value
        setupTask = nil
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
        
        try await ensureSetup()
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
        configuration.schemaVersion = 10
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
        setupTask?.cancel()
    }
    
    @BigSyncBackgroundActor
    public func unsetCancellation() async throws {
        //        debugPrint("# unset cancel")
        cancelSync = false
        if isSetupInterrupted {
            try await ensureSetup()
        }
    }
    
    @BigSyncBackgroundActor
    private func ensureSetup() async throws {
        if let setupTask {
            try await setupTask.value
            return
        }

        let generation = UUID()
        setupGeneration = generation
        let task = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            try await performSetup()
        }
        setupTask = task
        do {
            try await task.value
            if setupGeneration == generation {
                setupTask = nil
            }
        } catch {
            if setupGeneration == generation {
                setupTask = nil
            }
            throw error
        }
    }

    @BigSyncBackgroundActor
    private func performSetup() async throws {
        logger.info("QSCloudKitSynchronizer >> Setup synchronization...")
        //        debugPrint("# setup() ...")
        // Setup can be retried after cancellation or a cache reset. Tear down any
        // prior notification graph so retries never accumulate duplicate Realm
        // observers or debounced processors.
        invalidateTokens()
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
        
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let syncEmpty = persistenceRealm.objects(SyncedEntity.self).isEmpty
        // An empty user Realm is still initialized. Without this durable marker,
        // empty databases repeated the full initial scan on every launch.
        let needsInitialSetup =
            syncEmpty && needsMutationJournalRecovery(in: persistenceRealm)
        
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
                    
                    var results = targetReaderRealm.objects(objectClass)
                    if let eligibilityType = objectClass
                        as? CloudKitInitialSyncEligibilityModel.Type {
                        results = results.filter(
                            eligibilityType.initialCloudKitSyncEligibilityPredicate
                        )
                    }
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
        
        // Install observation before the final journal drain. Writes do not depend
        // on the observer for durability, but this ordering avoids adding debounce
        // latency to a mutation committed while setup is finishing.
        await setupPublisherDebouncer()
        observeRealmChanges()

        if needsInitialSetup {
            try await updateCreatedAndModified(notifyDelegate: false)
            try await markMutationJournalRecoveryComplete(in: persistenceRealm)
        } else if needsMutationJournalRecovery(in: persistenceRealm) {
            // This is the only broad scan for clients that carry the journal
            // schema. It recovers changes made by older builds and records that
            // predate durable changed-ID tracking.
            do {
                try await createMissingSyncedEntities()
            } catch is CancellationError {
                isSetupInterrupted = true
                return
            } catch {
                isSetupInterrupted = true
                throw error
            }
            await enqueueCreatedAndModified(
                includeOnlyInitialSyncEligible: true
            )
            try await processEnqueuedChanges(notifyDelegate: false)
            try await updateCreatedAndModified(notifyDelegate: false)
            try await markMutationJournalRecoveryComplete(in: persistenceRealm)
        } else {
            // Normal launches touch only durable changed IDs. Target Realms that
            // have not adopted BigSyncPendingMutation retain the timestamp fallback
            // inside updateCreatedAndModified().
            try await updateCreatedAndModified(notifyDelegate: false)
        }

        updateHasChanges(realm: persistenceRealm)
        
        //        if hasChanges {
        //            Task { @BigSyncBackgroundActor in
        //                await modelAdapterDelegate?.hasChangesToUpload()
        //            }
        //        }
    }

    @BigSyncBackgroundActor
    private func needsMutationJournalRecovery(in persistenceRealm: Realm) -> Bool {
        persistenceRealm.object(
            ofType: SyncedEntityType.self,
            forPrimaryKey: Self.mutationJournalRecoveryEntityType
        )?.recoveryVersion != Self.mutationJournalRecoveryVersion
    }

    @BigSyncBackgroundActor
    private func markMutationJournalRecoveryComplete(in persistenceRealm: Realm) async throws {
        try await persistenceRealm.asyncWrite {
            let state = persistenceRealm.object(
                ofType: SyncedEntityType.self,
                forPrimaryKey: Self.mutationJournalRecoveryEntityType
            ) ?? SyncedEntityType(entityType: Self.mutationJournalRecoveryEntityType)
            state.recoveryVersion = Self.mutationJournalRecoveryVersion
            persistenceRealm.add(state, update: .modified)
        }
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
            .sink { @Sendable [weak self] _ in
                guard let self else { return }
                Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    guard let targetReaderRealms = self.realmProvider?.targetReaderRealms else { return }
                    let observed = observedJournalMutations
                    let legacyIndexes = changedLegacyRealmIndexes
                    observedJournalMutations.removeAll(keepingCapacity: true)
                    changedLegacyRealmIndexes.removeAll(keepingCapacity: true)
                    for (idx, mutationsByRecordName) in observed
                    where idx < targetReaderRealms.count {
                        try await self.forwardPendingMutations(
                            Array(mutationsByRecordName.values),
                            in: targetReaderRealms[idx]
                        )
                    }
                    for idx in legacyIndexes where idx < targetReaderRealms.count {
                        await self.enqueueCreatedAndModified(in: targetReaderRealms[idx])
                    }
                }
            }
            .store(in: &cancellables)
        
        // Observe only the durable mutation journal when it is present. A broad
        // Realm notification is retained solely for legacy schemas that cannot
        // atomically journal changed IDs.
        for (idx, targetReaderRealm) in targetReaderRealms.enumerated() {
            let token: NotificationToken
            if targetReaderRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
                token = targetReaderRealm.objects(BigSyncPendingMutation.self).observe {
                    [weak self] changes in
                    guard let self else { return }
                    switch changes {
                    case .initial:
                        return
                    case .update(let collection, let deletions, let insertions, let modifications):
                        guard !deletions.isEmpty || !insertions.isEmpty || !modifications.isEmpty else {
                            return
                        }
                        let changedIndexes = Set(insertions).union(modifications)
                        let snapshots = changedIndexes.map { index in
                            let mutation = collection[index]
                            return BigSyncPendingMutationSnapshot(
                                recordName: mutation.recordName,
                                entityType: mutation.entityType,
                                objectIdentifier: mutation.objectIdentifier,
                                generation: mutation.generation,
                                changedAt: mutation.changedAt
                            )
                        }
                        guard !snapshots.isEmpty else { return }
                        Task { @BigSyncBackgroundActor [weak self] in
                            self?.enqueueObservedJournalMutations(snapshots, realmIndex: idx)
                        }
                        return
                    case .error(let error):
                        logger.error("BigSyncKit mutation journal observation failed: \(error)")
                        return
                    }
                }
            } else {
                token = targetReaderRealm.observe { [weak self] _, _ in
                    guard let self else { return }
                    Task { @BigSyncBackgroundActor [weak self] in
                        self?.changedLegacyRealmIndexes.insert(idx)
                        self?.realmChangesSubject.send(())
                    }
                }
            }
            cancellables.insert(AnyCancellable { token.invalidate() })
        }
    }

    @BigSyncBackgroundActor
    private func enqueueObservedJournalMutations(
        _ snapshots: [BigSyncPendingMutationSnapshot],
        realmIndex: Int
    ) {
        for snapshot in snapshots {
            observedJournalMutations[realmIndex, default: [:]][snapshot.recordName] = snapshot
        }
        realmChangesSubject.send(())
    }

    private func pendingMutationSnapshots(
        for recordNames: some Sequence<String>,
        in realm: Realm
    ) -> [BigSyncPendingMutationSnapshot] {
        recordNames.compactMap { recordName in
            guard let mutation = realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            ) else { return nil }
            return BigSyncPendingMutationSnapshot(
                recordName: mutation.recordName,
                entityType: mutation.entityType,
                objectIdentifier: mutation.objectIdentifier,
                generation: mutation.generation,
                changedAt: mutation.changedAt
            )
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
    private func updateCreatedAndModified(notifyDelegate: Bool = true) async throws {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else { return }
        for targetReaderRealm in targetReaderRealms {
            if targetReaderRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
                try await forwardPendingMutations(
                    in: targetReaderRealm,
                    notifyDelegate: notifyDelegate
                )
            } else {
                await enqueueCreatedAndModified(in: targetReaderRealm)
            }
        }
        try await processEnqueuedChanges(notifyDelegate: notifyDelegate)
    }

    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(
        in targetReaderRealm: Realm,
        notifyDelegate: Bool = true
    ) async throws -> Int {
        let mutations = targetReaderRealm.objects(BigSyncPendingMutation.self)
        let pending = Array(mutations.map { mutation in
            BigSyncPendingMutationSnapshot(
                recordName: mutation.recordName,
                entityType: mutation.entityType,
                objectIdentifier: mutation.objectIdentifier,
                generation: mutation.generation,
                changedAt: mutation.changedAt
            )
        })
        return try await forwardPendingMutations(
            pending,
            in: targetReaderRealm,
            notifyDelegate: notifyDelegate
        )
    }

    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(
        _ pending: [BigSyncPendingMutationSnapshot],
        in targetReaderRealm: Realm,
        notifyDelegate: Bool = true
    ) async throws -> Int {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return 0 }
        guard !pending.isEmpty else { return 0 }
        let ignoredGenerationsByRecordName = pending.reduce(into: [String: String]()) {
            result, mutation in
            if self.modelTypes[mutation.entityType] == nil
                || self.excludedClassNames.contains(mutation.entityType) {
                result[mutation.recordName] = mutation.generation
            }
        }
        let trackedPending = pending.filter {
            self.modelTypes[$0.entityType] != nil
                && !self.excludedClassNames.contains($0.entityType)
        }

        var forwardedCount = 0
        for chunk in trackedPending.chunks(ofCount: 1000) {
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

        if !ignoredGenerationsByRecordName.isEmpty {
            try await targetReaderRealm.asyncWrite {
                for (recordName, ignoredGeneration) in ignoredGenerationsByRecordName {
                    if let mutation = targetReaderRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: recordName
                    ), mutation.generation == ignoredGeneration {
                        targetReaderRealm.delete(mutation)
                    }
                }
            }
        }

        if forwardedCount > 0 {
            updateHasChanges(realm: persistenceRealm)
            if notifyDelegate {
                await modelAdapterDelegate?.hasChangesToUpload()
            }
        }
        return forwardedCount
    }
    
    @BigSyncBackgroundActor
    private func enqueueCreatedAndModified(
        in realm: Realm? = nil,
        includeOnlyInitialSyncEligible: Bool = false
    ) async {
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
                    schemaName: schema.className,
                    includeOnlyInitialSyncEligible: includeOnlyInitialSyncEligible
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
        schemaName: String,
        includeOnlyInitialSyncEligible: Bool = false
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
            var matchingObjects = targetReaderRealm.objects(objectClass)
                .filter(predicate)
            if includeOnlyInitialSyncEligible,
               let eligibilityType = objectClass
                    as? CloudKitInitialSyncEligibilityModel.Type {
                matchingObjects = matchingObjects.filter(
                    eligibilityType.initialCloudKitSyncEligibilityPredicate
                )
            }
            for object in matchingObjects {
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

    @BigSyncBackgroundActor
    func _test_setup() async throws {
        try await ensureSetup()
    }
#endif
    
    @BigSyncBackgroundActor
    private func processEnqueuedChanges(notifyDelegate: Bool = true) async throws {
        guard let realmProvider = realmProvider else { return }
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let currentChangeSet: ResultsChangeSet
        currentChangeSet = self.resultsChangeSet
        self.resultsChangeSet = ResultsChangeSet() // Reset for next batch
        guard !currentChangeSet.insertions.isEmpty
                || !currentChangeSet.modifications.isEmpty
                || !currentChangeSet.trackedChangeHighWatermarks.isEmpty else {
            return
        }
        
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
        
        if hasChanges && notifyDelegate {
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
            
            if modified && (
                syncedEntity.state == SyncedEntityState.deletedLocally.rawValue ||
                syncedEntity.state == SyncedEntityState.deletedRemotely.rawValue
            ) {
                syncedEntity.state = SyncedEntityState.new.rawValue
                syncedEntity.encodedRecord = nil
            } else if syncedEntity.state == SyncedEntityState.synced.rawValue && modified {
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

                var objects = targetReaderRealm.objects(objectClass)
                if let eligibilityType = objectClass
                    as? CloudKitInitialSyncEligibilityModel.Type {
                    objects = objects.filter(
                        eligibilityType.initialCloudKitSyncEligibilityPredicate
                    )
                }
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
        try await persistenceRealm.asyncWrite {
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
            return Int(stringObjectId)
        case .objectId:
            return try? ObjectId(string: stringObjectId)
        case .string:
            return stringObjectId
        case .UUID:
            return UUID(uuidString: stringObjectId)
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
        return identifier
    }

    static func validateCloudKitRecordName(_ recordName: String) throws {
        guard !recordName.isEmpty else {
            throw BigSyncCloudKitRecordNameError.empty
        }
        guard recordName.first != "_" else {
            throw BigSyncCloudKitRecordNameError.reservedPrefix
        }
        guard recordName.unicodeScalars.allSatisfy(\.isASCII) else {
            throw BigSyncCloudKitRecordNameError.nonASCII(recordName)
        }
        guard recordName.count <= 255 else {
            throw BigSyncCloudKitRecordNameError.exceedsMaximumLength(
                actual: recordName.count
            )
        }
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
                        guard let newValue = newValue as? [String] else {
                            return true
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<String> {
                            return newValue != Array(existingValue)
                        }
                        if let existingValue =
                            existingValue as? RealmSwift.List<URL> {
                            return newValue
                                != existingValue.map(\.absoluteString)
                        }
                        return true
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
    ) throws -> [PendingRelationshipRequest] {
        let objectProperties = object.objectSchema.properties
        var pendingRelationships = [PendingRelationshipRequest]()
        
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
                    syncedEntityIdentifier: syncedEntityID,
                    pendingRelationships: &pendingRelationships
                )
            }
        }
        
        if mergePolicy == .server {
            try applyChanges()
        } else if mergePolicy == .custom {
            let acceptRemoteChange: Bool
            if let delegate {
                var recordChanges = [String: Any]()
                for property in objectProperties
                    where !skippedKeys.contains(property.name) {
                    try Task.checkCancellation()
                    if property.type == .linkingObjects {
                        continue
                    }
                    if !shouldIgnore(key: property.name) {
                        if let asset = record[property.name] as? CKAsset {
                            try Task.checkCancellation()
                            recordChanges[property.name] = asset.fileURL.flatMap {
                                NSData(contentsOf: $0)
                            } ?? NSNull()
                        } else {
                            recordChanges[property.name] =
                                record[property.name] ?? NSNull()
                        }
                    }
                }
                acceptRemoteChange = delegate.realmSwiftAdapter(
                    self,
                    gotChanges: recordChanges,
                    object: object
                )
            } else {
                acceptRemoteChange = try { adapter, record, object in
                    guard adapter.hasRealmObjectClass(name: object.objectSchema.className) else {
                        logger.warning("QSCloudKitSynchronizer >> No object class found for '\(object.objectSchema.className)' in adapter")
                        return false
                    }
                    // The default merge policy only compares metadata. Avoid
                    // eagerly reading every CKAsset merely to build a delegate
                    // payload that no delegate will consume.
                    let remoteExplicitlyModifiedAt =
                        record["explicitlyModifiedAt"] as? Date
                        ?? .distantPast
                    let localExplicitlyModifiedAt = object["explicitlyModifiedAt"] as? Date ?? .distantPast
                    let result: Bool
                    if remoteExplicitlyModifiedAt > localExplicitlyModifiedAt {
                        result = true
                    } else if remoteExplicitlyModifiedAt == localExplicitlyModifiedAt {
                        let remoteModifiedAt =
                            record["modifiedAt"] as? Date
                            ?? .distantPast
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
                }(self, record, object)
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
        return pendingRelationships
    }
    
    func applyChange(
        property: Property,
        record: CKRecord,
        object: Object,
        syncedEntityIdentifier: String
    ) throws {
        var pendingRelationships = [PendingRelationshipRequest]()
        try applyChange(
            property: property,
            record: record,
            object: object,
            syncedEntityIdentifier: syncedEntityIdentifier,
            pendingRelationships: &pendingRelationships
        )
    }

    func applyChange(
        property: Property,
        record: CKRecord,
        object: Object,
        syncedEntityIdentifier: String,
        pendingRelationships: inout [PendingRelationshipRequest]
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
            // Full zone-change fetches use desiredKeys == nil, so an absent
            // collection field is the CloudKit representation of an empty
            // collection. Realm collections cannot be assigned nil.
            clearCollection(property: property, on: object)
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
                var targetIdentifiers = [String]()
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: recordName
                        ) else { continue }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: reference.recordID.recordName
                        ) else { continue }
                        targetIdentifiers.append(objectIdentifier)
                    }
                }
                appendPendingRelationship(
                    name: property.name,
                    syncedEntityID: syncedEntityIdentifier,
                    targetIdentifiers: targetIdentifiers,
                    record: record,
                    to: &pendingRelationships
                )
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
                if object[property.name] is List<URL> {
                    let list = List<URL>()
                    for item in value {
                        try Task.checkCancellation()
                        if let url = URL(string: item) {
                            list.append(url)
                        }
                    }
                    recordValue = list
                } else {
                    let list = List<String>()
                    for item in value {
                        try Task.checkCancellation()
                        list.append(item)
                    }
                    recordValue = list
                }
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
                var targetIdentifiers = [String]()
                if let value = record.value(forKey: property.name) as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: recordName
                        ) else { continue }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else if let value = record.value(forKey: property.name) as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: reference.recordID.recordName
                        ) else { continue }
                        targetIdentifiers.append(objectIdentifier)
                    }
                }
                appendPendingRelationship(
                    name: property.name,
                    syncedEntityID: syncedEntityIdentifier,
                    targetIdentifiers: targetIdentifiers,
                    record: record,
                    to: &pendingRelationships
                )
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
            guard let objectIdentifier = objectIdentifier(
                fromCloudKitRecordName: reference.recordID.recordName
            ) else { return }
            appendPendingRelationship(
                name: key,
                syncedEntityID: syncedEntityIdentifier,
                targetIdentifiers: [objectIdentifier],
                record: record,
                to: &pendingRelationships
            )
        } else if property.type == .object {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            let targetIdentifiers: [String]
            if let recordName = record.value(forKey: property.name) as? String,
               let objectIdentifier = objectIdentifier(
                    fromCloudKitRecordName: recordName
               ) {
                targetIdentifiers = [objectIdentifier]
            } else if value == nil, property.isOptional {
                targetIdentifiers = []
            } else {
                return
            }
            appendPendingRelationship(
                name: key,
                syncedEntityID: syncedEntityIdentifier,
                targetIdentifiers: targetIdentifiers,
                record: record,
                to: &pendingRelationships
            )
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

    private func clearCollection(property: Property, on object: Object) {
        let key = property.name
        if property.isSet {
            switch property.type {
            case .int:
                object.setValue(Set<Int>(), forKey: key)
            case .string:
                object.setValue(Set<String>(), forKey: key)
            case .bool:
                object.setValue(Set<Bool>(), forKey: key)
            case .float:
                object.setValue(Set<Float>(), forKey: key)
            case .double:
                object.setValue(Set<Double>(), forKey: key)
            case .data:
                object.setValue(Set<Data>(), forKey: key)
            case .date:
                object.setValue(Set<Date>(), forKey: key)
            case .UUID:
                object.setValue(Set<UUID>(), forKey: key)
            default:
                object.setValue([], forKey: key)
            }
        } else if property.isArray {
            switch property.type {
            case .int:
                object.setValue(List<Int>(), forKey: key)
            case .string:
                if object[key] is List<URL> {
                    object.setValue(List<URL>(), forKey: key)
                } else {
                    object.setValue(List<String>(), forKey: key)
                }
            case .bool:
                object.setValue(List<Bool>(), forKey: key)
            case .float:
                object.setValue(List<Float>(), forKey: key)
            case .double:
                object.setValue(List<Double>(), forKey: key)
            case .data:
                object.setValue(List<Data>(), forKey: key)
            case .date:
                object.setValue(List<Date>(), forKey: key)
            case .UUID:
                object.setValue(List<UUID>(), forKey: key)
            default:
                object.setValue([], forKey: key)
            }
        } else if property.isMap {
            object.setValue([String: Any](), forKey: key)
        }
    }
    
    private func appendPendingRelationship(
        name: String,
        syncedEntityID: String,
        targetIdentifiers: [String],
        record: CKRecord,
        to pendingRelationships: inout [PendingRelationshipRequest]
    ) {
        pendingRelationships.append(
            PendingRelationshipRequest(
                name: name,
                syncedEntityID: syncedEntityID,
                targetIdentifiers: targetIdentifiers,
                sourceRecordChangeTag: record.recordChangeTag,
                expectedModifiedAt: record["modifiedAt"] as? Date,
                expectedExplicitlyModifiedAt:
                    record["explicitlyModifiedAt"] as? Date
            )
        )
    }
    
    @BigSyncBackgroundActor
    func persistPendingRelationships(
        _ requests: [PendingRelationshipRequest]
    ) async throws {
        for chunk in requests.chunks(ofCount: 5000) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            
            guard let persistenceRealm = realmProvider?.persistenceRealm else { break }
            
            do {
                //                await persistenceRealm.asyncRefresh()
                try await persistenceRealm.asyncWrite {
                    for request in chunk {
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        let existingRelationships = persistenceRealm.objects(
                            PendingRelationship.self
                        ).filter(
                            "relationshipName == %@ AND forSyncedEntity.identifier == %@",
                            request.name,
                            request.syncedEntityID
                        )
                        persistenceRealm.delete(existingRelationships)

                        guard let syncedEntity = persistenceRealm.object(
                            ofType: SyncedEntity.self,
                            forPrimaryKey: request.syncedEntityID
                        ) else { continue }
                        for (position, targetIdentifier) in
                            request.targetIdentifiers.enumerated() {
                            let pendingRelationship = PendingRelationship()
                            pendingRelationship.relationshipName = request.name
                            pendingRelationship.forSyncedEntity = syncedEntity
                            pendingRelationship.targetIdentifier = targetIdentifier
                            pendingRelationship.position = position
                            pendingRelationship.sourceRecordChangeTag =
                                request.sourceRecordChangeTag
                            pendingRelationship.expectedModifiedAt =
                                request.expectedModifiedAt
                            pendingRelationship.expectedExplicitlyModifiedAt =
                                request.expectedExplicitlyModifiedAt
                            persistenceRealm.add(pendingRelationship)
                        }
                        if request.targetIdentifiers.isEmpty {
                            let pendingRelationship = PendingRelationship()
                            pendingRelationship.relationshipName = request.name
                            pendingRelationship.forSyncedEntity = syncedEntity
                            pendingRelationship.targetIdentifier = nil
                            pendingRelationship.sourceRecordChangeTag =
                                request.sourceRecordChangeTag
                            pendingRelationship.expectedModifiedAt =
                                request.expectedModifiedAt
                            pendingRelationship.expectedExplicitlyModifiedAt =
                                request.expectedExplicitlyModifiedAt
                            persistenceRealm.add(pendingRelationship)
                        }
                    }
                }
            } catch {
                logger.error("Error during persistPendingRelationships: \(error)")
                throw error
            }
        }
    }
    
    @BigSyncBackgroundActor
    func applyPendingRelationships(realmProvider: RealmProvider) async throws {
        guard let persistenceRealm = realmProvider.persistenceRealm else { return }
        let pendingRelationships = persistenceRealm.objects(PendingRelationship.self)
        guard !pendingRelationships.isEmpty else { return }

        struct RelationshipGroupKey: Hashable {
            let syncedEntityID: String
            let relationshipName: String
        }
        let groupedRelationships = Dictionary(
            grouping: Array(pendingRelationships)
        ) { relationship in
            RelationshipGroupKey(
                syncedEntityID: relationship.forSyncedEntity?.identifier ?? "",
                relationshipName: relationship.relationshipName ?? ""
            )
        }

        for (key, relationships) in groupedRelationships {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            guard !key.syncedEntityID.isEmpty,
                  !key.relationshipName.isEmpty,
                  let syncedEntity = relationships.first?.forSyncedEntity,
                  syncedEntity.entityState != .deletedLocally && syncedEntity.entityState != .deletedRemotely else { continue }

            guard let originObjectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
                continue
            }
            let objectIdentifier = getObjectIdentifier(for: syncedEntity)
            guard let targetRealm = realmProvider.targetReaderRealmPerSchemaName[
                originObjectClass.className()
            ] else { continue }
            await targetRealm.asyncRefresh()
            guard let originObject = targetRealm.object(
                ofType: originObjectClass,
                forPrimaryKey: objectIdentifier
            ) else { continue }

            let expectedModifiedAt =
                relationships.first?.expectedModifiedAt
            let expectedExplicitlyModifiedAt =
                relationships.first?.expectedExplicitlyModifiedAt
            func datesMatch(_ lhs: Date?, _ rhs: Date?) -> Bool {
                switch (lhs, rhs) {
                case (.none, .none):
                    return true
                case let (.some(lhs), .some(rhs)):
                    // Realm and CloudKit can round date storage at different
                    // sub-millisecond precision.
                    return abs(lhs.timeIntervalSince(rhs)) < 0.001
                default:
                    return false
                }
            }
            let hasServerVersion =
                relationships.first?.sourceRecordChangeTag != nil
            func hasInterveningLocalMutation() -> Bool {
                let hasPendingMutation =
                    targetRealm.schema.objectSchema.contains {
                        $0.className == BigSyncPendingMutation.className()
                    }
                    && targetRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: syncedEntity.identifier
                    ) != nil
                let localMetadataChanged =
                    !datesMatch(
                        originObject["modifiedAt"] as? Date,
                        expectedModifiedAt
                    )
                    || !datesMatch(
                        originObject["explicitlyModifiedAt"] as? Date,
                        expectedExplicitlyModifiedAt
                    )
                return hasPendingMutation
                    || syncedEntity.pendingGeneration != nil
                    || (hasServerVersion && localMetadataChanged)
            }
            if hasInterveningLocalMutation() {
                logger.info(
                    "QSCloudKitSynchronizer >> Discarding stale deferred relationship \(key.relationshipName) for \(key.syncedEntityID)"
                )
                // The deferred relationship belongs to an older remote record.
                // BigSyncKit resolves conflicts at record granularity, so any
                // intervening local mutation wins over this stale intent.
                try await persistenceRealm.asyncWrite {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    persistenceRealm.delete(relationships)
                }
                continue
            }

            guard let property = originObject.objectSchema.properties.first(
                where: { $0.name == key.relationshipName }
            ), let className = property.objectClassName else { continue }
            guard let targetObjectClass = realmObjectClass(name: className) else { continue }
            let targetIdentifiers = relationships
                .sorted { lhs, rhs in lhs.position < rhs.position }
                .compactMap(\.targetIdentifier)
            let relationshipName = key.relationshipName
            let isArray = property.isArray
            let isSet = property.isSet
            var targetObjects = [Object]()
            targetObjects.reserveCapacity(targetIdentifiers.count)
            for targetIdentifier in targetIdentifiers {
                try Task.checkCancellation()
                guard let parsedIdentifier = getObjectIdentifier(
                    stringObjectId: targetIdentifier,
                    entityType: className
                ), let targetObject = targetRealm.object(
                    ofType: targetObjectClass,
                    forPrimaryKey: parsedIdentifier
                ) else {
                    targetObjects.removeAll()
                    break
                }
                targetObjects.append(targetObject)
            }
            guard targetObjects.count == targetIdentifiers.count else { continue }

            var becameStaleBeforeWrite = false
            try await targetRealm.asyncWrite {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                guard !hasInterveningLocalMutation() else {
                    becameStaleBeforeWrite = true
                    return
                }
                if isArray {
                    guard let collection = originObject[relationshipName]
                        as? RLMSwiftCollectionBase,
                          let array = collection._rlmCollection
                        as? RLMArray<AnyObject> else { return }
                    array.removeAllObjects()
                    for targetObject in targetObjects {
                        array.add(targetObject)
                    }
                } else if isSet {
                    guard let collection = originObject[relationshipName]
                        as? RLMSwiftCollectionBase,
                          let set = collection._rlmCollection
                        as? RLMSet<AnyObject> else { return }
                    set.removeAllObjects()
                    for targetObject in targetObjects {
                        set.add(targetObject)
                    }
                } else {
                    originObject.setValue(
                        targetObjects.first,
                        forKey: relationshipName
                    )
                }
            }

            try await persistenceRealm.asyncWrite {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                persistenceRealm.delete(relationships)
            }
            if becameStaleBeforeWrite {
                logger.info(
                    "QSCloudKitSynchronizer >> Discarding deferred relationship \(key.relationshipName) for \(key.syncedEntityID) after a concurrent local mutation"
                )
            }

            await Task.yield()
            try Task.checkCancellation()
        }
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
            
            guard syncedEntity.state == state.rawValue,
                  !includedEntityIDs.contains(syncedEntity.identifier) else {
                return
            }
            let entityIdentifier = syncedEntity.identifier
            let generation = syncedEntity.pendingGeneration
            guard let record = try await recordToUpload(
                syncedEntity: syncedEntity
            ) else { return }
            resultArray.append(
                PreparedRecordUpload(
                    record: record,
                    generation: generation
                )
            )
            includedEntityIDs.insert(entityIdentifier)
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
    func recordToUpload(
        syncedEntity: SyncedEntity
    ) async throws -> CKRecord? {
        try Self.validateCloudKitRecordName(syncedEntity.identifier)
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
                        try Self.validateCloudKitRecordName(referenceIdentifier)
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
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let set = value as? MutableSet<Int> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        guard let set = value as? MutableSet<String> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let set = value as? MutableSet<Bool> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let set = value as? MutableSet<Float> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let set = value as? MutableSet<Double> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let set = value as? MutableSet<Data> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let set = value as? MutableSet<Date> else { break }
                        let array = Array(set)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let set = value as? MutableSet<UUID> else { break }
                        let array = Array(set.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
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
                            try Self.validateCloudKitRecordName(referenceIdentifier)
                            let recordID = CKRecord.ID(recordName: referenceIdentifier, zoneID: zoneID)
                            referenceArray.append(recordID.recordName)
                        }
                        record[property.name] = referenceArray.isEmpty
                            ? nil
                            : referenceArray as CKRecordValue
                    case .int:
                        guard let list = value as? List<Int> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .string:
                        let array: [String]
                        if let list = value as? List<String> {
                            array = Array(list)
                        } else if let list = value as? List<URL> {
                            array = list.map(\.absoluteString)
                        } else {
                            break
                        }
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .bool:
                        guard let list = value as? List<Bool> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .float:
                        guard let list = value as? List<Float> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .double:
                        guard let list = value as? List<Double> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .data:
                        guard let list = value as? List<Data> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .date:
                        guard let list = value as? List<Date> else { break }
                        let array = Array(list)
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
                    case .UUID:
                        guard let list = value as? List<UUID> else { break }
                        let array = Array(list.map { $0.uuidString })
                        record[property.name] = array.isEmpty ? nil : array as CKRecordValue
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
                        let fileURL = try self.persistentAssetManager.store(
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
                            let fileURL = try self.persistentAssetManager.store(
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
    @BigSyncBackgroundActor
    public func cleanUp() async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else {
            logger.warning("QSCloudKitSynchronizer >> Cleanup requested before Realm setup completed")
            return
        }

        let remotelyDeleted = persistenceRealm.objects(SyncedEntity.self)
            .where { $0.state == SyncedEntityState.deletedRemotely.rawValue }
            .compactMap { entity -> RemoteDeletionSnapshot? in
                guard !self.excludedClassNames.contains(entity.entityType) else { return nil }
                let prefix = entity.entityType + "."
                guard entity.identifier.hasPrefix(prefix) else { return nil }
                return RemoteDeletionSnapshot(
                    recordName: entity.identifier,
                    entityType: entity.entityType,
                    objectIdentifier: String(entity.identifier.dropFirst(prefix.count))
                )
            }

        let deletionsByRealm = Dictionary(grouping: remotelyDeleted) { deletion in
            realmProvider.targetReaderRealmPerSchemaName[deletion.entityType]
                .map { BigSyncMutationTrackingRegistry.identity(for: $0.configuration) }
        }
        var committedRecordNames = Set<String>()

        for (realmIdentity, deletions) in deletionsByRealm {
            guard realmIdentity != nil,
                  let targetRealm = deletions.lazy.compactMap({
                      realmProvider.targetReaderRealmPerSchemaName[$0.entityType]
                  }).first else {
                continue
            }
#if DEBUG
            try await _testBeforeCleanupTargetWrite?()
#endif
            try await targetRealm.asyncWrite {
                for deletion in deletions {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    guard persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: deletion.recordName
                    )?.entityState == .deletedRemotely else {
                        continue
                    }
                    if targetRealm.schema.objectSchema.contains(where: {
                        $0.className == BigSyncPendingMutation.className()
                    }), targetRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: deletion.recordName
                    ) != nil {
                        continue
                    }
                    guard let objectClass = realmObjectClass(
                        name: deletion.entityType
                    ), let identifier = getObjectIdentifier(
                            stringObjectId: deletion.objectIdentifier,
                            entityType: deletion.entityType
                          ) else {
                        continue
                    }
                    guard let object = targetRealm.object(
                        ofType: objectClass,
                        forPrimaryKey: identifier
                    ) else {
                        committedRecordNames.insert(deletion.recordName)
                        continue
                    }
                    if let softDeletable = object as? SoftDeletable,
                       !softDeletable.isDeleted {
                        continue
                    }
                    if let syncable = object as? any SyncableBase,
                       syncable.needsSyncToAppServer {
                        continue
                    }
                    targetRealm.delete(object)
                    committedRecordNames.insert(deletion.recordName)
                }
            }
        }

        try await persistenceRealm.asyncWrite {
            for recordName in committedRecordNames {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                if let entity = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: recordName
                ), entity.entityState == .deletedRemotely,
                   entity.pendingGeneration == nil {
                    persistenceRealm.delete(entity)
                }
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
                try await writeSyncedEntities(
                    syncedEntities: syncedEntitiesToCreate,
                    realmProvider: realmProvider
                )
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
                let relationshipRequests =
                    try await { @RealmBackgroundActor () async throws
                        -> [PendingRelationshipRequest] in
                    var relationshipRequests =
                        [PendingRelationshipRequest]()
                    guard let targetWriterRealms =
                        realmProvider.targetWriterRealms else { return [] }
                    
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
                                    relationshipRequests.append(
                                        contentsOf: try self.applyChanges(
                                            in: record,
                                            to: object,
                                            syncedEntityID: syncedEntityID,
                                            syncedEntityState: syncedEntityState,
                                            entityType: entityType
                                        )
                                    )
                                }
                            }
                        }
                    }
                    return relationshipRequests
                }()
                
                try await persistPendingRelationships(relationshipRequests)
                
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
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              !recordIDs.isEmpty else { return }

        var deletions = [RemoteDeletionSnapshot]()
        var localWins = [
            (deletion: RemoteDeletionSnapshot, generation: String?)
        ]()
        deletions.reserveCapacity(recordIDs.count)
        for recordID in recordIDs {
            try Task.checkCancellation()
            let syncedEntity = Self.getSyncedEntity(
                objectIdentifier: recordID.recordName,
                realm: persistenceRealm
            )
            let parsedEntityType = recordID.recordName
                .split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
                .first
                .map(String.init)
            guard let entityType = syncedEntity?.entityType ?? parsedEntityType,
                  !excludedClassNames.contains(entityType),
                  realmObjectClass(name: entityType) != nil else { continue }
            let prefix = entityType + "."
            guard recordID.recordName.hasPrefix(prefix) else { continue }
            let deletion = RemoteDeletionSnapshot(
                recordName: recordID.recordName,
                entityType: entityType,
                objectIdentifier: String(
                    recordID.recordName.dropFirst(prefix.count)
                )
            )
            let targetRealm =
                realmProvider.targetReaderRealmPerSchemaName[entityType]
            let pendingMutation = targetRealm?.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordID.recordName
            )
            if pendingMutation != nil
                || syncedEntity?.entityState == .new
                || syncedEntity?.entityState == .changed {
                localWins.append(
                    (deletion, pendingMutation?.generation)
                )
            } else {
                deletions.append(deletion)
            }
        }

        let deletionsByEntityType = Dictionary(grouping: deletions, by: \.entityType)
        var committedRemoteDeletions = [RemoteDeletionSnapshot]()
        for (entityType, entityDeletions) in deletionsByEntityType where entityType != "CKShare" {
            try Task.checkCancellation()
            guard let objectClass = realmObjectClass(name: entityType),
                  let targetRealm = realmProvider.targetReaderRealmPerSchemaName[entityType] else {
                continue
            }
#if DEBUG
            try await _testBeforeRemoteDeletionTargetWrite?()
#endif
            try await targetRealm.asyncWrite {
                for deletion in entityDeletions {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    if targetRealm.schema.objectSchema.contains(where: {
                        $0.className == BigSyncPendingMutation.className()
                    }), let mutation = targetRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: deletion.recordName
                    ) {
                        localWins.append((deletion, mutation.generation))
                        continue
                    }
                    if let objectIdentifier = getObjectIdentifier(
                        stringObjectId: deletion.objectIdentifier,
                        entityType: entityType
                    ), let object = targetRealm.object(
                        ofType: objectClass,
                        forPrimaryKey: objectIdentifier
                    ) {
                        if let object = object as? SoftDeletable {
                            object.isDeleted = true
                        }
                    }
                    committedRemoteDeletions.append(deletion)
                }
            }
        }

        // A local mutation can commit immediately after the inbound-deletion
        // transaction. Its journal is durable even if forwarding is debounced,
        // so let it win before publishing tracking state.
        var lateLocalRecordNames = Set<String>()
        for deletion in committedRemoteDeletions {
            guard let targetRealm = realmProvider
                .targetReaderRealmPerSchemaName[deletion.entityType] else {
                continue
            }
            await targetRealm.asyncRefresh()
            if let mutation = targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: deletion.recordName
            ) {
                lateLocalRecordNames.insert(deletion.recordName)
                localWins.append((deletion, mutation.generation))
            }
        }
        committedRemoteDeletions.removeAll {
            lateLocalRecordNames.contains($0.recordName)
        }

        try await persistenceRealm.asyncWrite {
            for localWin in localWins {
                try Task.checkCancellation()
                let syncedEntity = Self.getSyncedEntity(
                    objectIdentifier: localWin.deletion.recordName,
                    realm: persistenceRealm
                ) ?? SyncedEntity(
                    entityType: localWin.deletion.entityType,
                    identifier: localWin.deletion.recordName,
                    state: SyncedEntityState.new.rawValue
                )
                persistenceRealm.add(syncedEntity, update: .modified)
                syncedEntity.state = SyncedEntityState.new.rawValue
                syncedEntity.encodedRecord = nil
                if syncedEntity.pendingGeneration == nil {
                    syncedEntity.pendingGeneration =
                        localWin.generation ?? UUID().uuidString
                }
            }
            for deletion in committedRemoteDeletions {
                try Task.checkCancellation()
                let syncedEntity = Self.getSyncedEntity(
                    objectIdentifier: deletion.recordName,
                    realm: persistenceRealm
                ) ?? SyncedEntity(
                    entityType: deletion.entityType,
                    identifier: deletion.recordName,
                    state: SyncedEntityState.deletedRemotely.rawValue
                )
                persistenceRealm.add(syncedEntity, update: .modified)
                guard syncedEntity.pendingGeneration == nil,
                      syncedEntity.entityState != .new,
                      syncedEntity.entityState != .changed,
                      syncedEntity.entityState != .deletedLocally else {
                    continue
                }
                syncedEntity.state = SyncedEntityState.deletedRemotely.rawValue
            }
        }
        updateHasChanges(realm: persistenceRealm)
        logger.info(
            "Deleted \(committedRemoteDeletions.count) local records which were previously deleted from iCloud"
        )
        if !localWins.isEmpty {
            logger.info(
                "Requeued \(localWins.count) locally changed records after a concurrent iCloud deletion"
            )
        }
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
                            try Task.checkCancellation()
                            guard !cancelSync else { throw CancellationError() }
                            guard let mutation = targetReaderRealm.object(
                                ofType: BigSyncPendingMutation.self,
                                forPrimaryKey: recordName
                            ), mutation.generation == generation else { continue }
                            targetReaderRealm.delete(mutation)
                        }
                    }
                    let newerMutations = pendingMutationSnapshots(
                        for: acknowledgedGenerations.keys,
                        in: targetReaderRealm
                    )
                    try await forwardPendingMutations(
                        newerMutations,
                        in: targetReaderRealm
                    )
                }
            }
        }
        
        updateHasChanges(realm: persistenceRealm)
    }
    
    @BigSyncBackgroundActor
    func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] {
        var deletions = [PreparedRecordDeletion]()
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
            if deletions.count >= limit {
                break
            }
            deletions.append(
                PreparedRecordDeletion(
                    recordID: CKRecord.ID(
                        recordName: syncedEntity.identifier,
                        zoneID: zoneID
                    ),
                    generation: syncedEntity.pendingGeneration
                )
            )
        }
        
        return deletions
    }

    @BigSyncBackgroundActor
    func recordIDsMarkedForDeletion(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [CKRecord.ID] {
        try await preparedRecordDeletions(
            limit: limit,
            restrictedToEntityType: restrictedToEntityType
        ).map(\.recordID)
    }

    @BigSyncBackgroundActor
    public func recordIDsMarkedForDeletion(limit: Int) async throws -> [CKRecord.ID] {
        try await recordIDsMarkedForDeletion(limit: limit, restrictedToEntityType: nil)
    }
    
    @BigSyncBackgroundActor
    public func didDelete(recordIDs deletedRecordIDs: [CKRecord.ID]) async {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        let matchingGenerations = deletedRecordIDs.reduce(into: [String: String]()) {
            guard let generation = persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: $1.recordName
            )?.pendingGeneration else { return }
            $0[$1.recordName] = generation
        }
        try? await didDelete(
            recordIDs: deletedRecordIDs,
            matchingGenerations: matchingGenerations
        )
    }

    @BigSyncBackgroundActor
    func didDelete(
        recordIDs deletedRecordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()

        for chunk in deletedRecordIDs.chunks(ofCount: 1000) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    guard let syncedEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: recordID.recordName
                    ), let deletedGeneration = matchingGenerations[recordID.recordName],
                       syncedEntity.pendingGeneration == deletedGeneration else {
                        continue
                    }
                    syncedEntity.state = SyncedEntityState.deletedRemotely.rawValue
                    syncedEntity.pendingGeneration = nil
                    acknowledgedGenerations[recordID.recordName] = deletedGeneration
                }
            }
        }

        if !acknowledgedGenerations.isEmpty,
           let targetReaderRealms = realmProvider.targetReaderRealms {
            for targetReaderRealm in targetReaderRealms where targetReaderRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
                try await targetReaderRealm.asyncWrite {
                    for (recordName, generation) in acknowledgedGenerations {
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        guard let mutation = targetReaderRealm.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: recordName
                        ), mutation.generation == generation else { continue }
                        targetReaderRealm.delete(mutation)
                    }
                }
                let newerMutations = pendingMutationSnapshots(
                    for: acknowledgedGenerations.keys,
                    in: targetReaderRealm
                )
                try await forwardPendingMutations(
                    newerMutations,
                    in: targetReaderRealm
                )
            }
        }

        updateHasChanges(realm: persistenceRealm)
    }
    
    @BigSyncBackgroundActor
    public func didDelete(identifiers: [String]) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        for identifier in identifiers {
            if let syncedEntity = persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: identifier) {
                try await persistenceRealm.asyncWrite {
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
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
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
    public func saveToken(_ token: CKServerChangeToken?) async throws {
        //        debugPrint("# saveToken", token, recordZoneID)
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }
        //        await persistenceRealm.asyncRefresh()
        var serverToken: ServerToken! = persistenceRealm.objects(ServerToken.self).first
        try Task.checkCancellation()
        guard !cancelSync else { throw CancellationError() }
        try await persistenceRealm.asyncWrite {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
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
