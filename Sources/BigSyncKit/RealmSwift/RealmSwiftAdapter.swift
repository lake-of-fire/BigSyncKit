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
#elseif os(iOS)
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
import CryptoKit

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

enum RealmSwiftAdapterError: Error, LocalizedError {
    case setupUnavailable
    case malformedRecordIdentifier(recordName: String, entityType: String)
    case duplicateTrackedEntityType(entityType: String)
    case missingChangeMetadataConformance(entityType: String)
    case systemFieldEncodingFailed(recordName: String)

    var errorDescription: String? {
        switch self {
        case .setupUnavailable:
            return "The Realm adapter has not completed setup."
        case let .malformedRecordIdentifier(recordName, entityType):
            return "Record name \(recordName) does not contain a valid \(entityType) identifier."
        case let .duplicateTrackedEntityType(entityType):
            return "Tracked Realm entity type \(entityType) is present in more than one target Realm."
        case let .missingChangeMetadataConformance(entityType):
            return "Tracked Realm entity type \(entityType) must conform to ChangeMetadataRecordable."
        case let .systemFieldEncodingFailed(recordName):
            return "Could not durably encode CloudKit system fields for \(recordName)."
        }
    }
}

enum RealmSwiftRemoteRecordDecodingError: Error, LocalizedError {
    case malformedField(recordName: String, propertyName: String, expected: String)

    var errorDescription: String? {
        switch self {
        case let .malformedField(recordName, propertyName, expected):
            "CloudKit record \(recordName) has a malformed \(propertyName) field; expected \(expected)."
        }
    }
}

/// Acknowledging an upload requires the generation captured when its batch was
/// prepared. Sampling the current generation can acknowledge a newer edit.
enum RealmSwiftAdapterAcknowledgementError: Error, LocalizedError {
    case batchBelongsToAnotherAdapter
    case recordWasNotPrepared

    var errorDescription: String? {
        switch self {
        case .batchBelongsToAnotherAdapter:
            "The prepared batch belongs to another RealmSwiftAdapter."
        case .recordWasNotPrepared:
            "The acknowledgement contains a record that was not in the prepared batch."
        }
    }
}

/// Records prepared for one upload attempt. The mutation generations are kept
/// opaque so acknowledgements cannot accidentally sample newer local edits.
struct RealmSwiftPreparedUploadBatch: @unchecked Sendable {
    let records: [CKRecord]
    fileprivate let matchingGenerations: [String: String]
    fileprivate let issuerID: UUID
}

/// Record identifiers prepared for one deletion attempt. The mutation
/// generations are kept opaque for generation-matched acknowledgement.
struct RealmSwiftPreparedDeletionBatch: @unchecked Sendable {
    let recordIDs: [CKRecord.ID]
    fileprivate let matchingGenerations: [String: String]
    fileprivate let issuerID: UUID
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
    init(
        persistenceConfiguration: Realm.Configuration,
        targetConfigurations: [Realm.Configuration]
    ) async throws {
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
            throw error
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

private struct RemoteRecordWriteCandidate {
    let record: CKRecord
    let objectType: Object.Type
    let objectIdentifier: any Sendable
    let syncedEntityID: String
    let syncedEntityState: SyncedEntityState
    let entityType: String
    let expectedMutationGeneration: String?
    let expectedModifiedAt: Date?
    let expectedExplicitlyModifiedAt: Date?
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
       keys.count == values.count,
       Set(keys).count == keys.count {
        return zip(keys, values).reduce(into: [String: Any]()) {
            $0[$1.0] = $1.1
        }
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

private func objectIdentifier(
    fromCloudKitRecordName recordName: String,
    expectedEntityType: String
) -> String? {
    let prefix = expectedEntityType + "."
    guard recordName.hasPrefix(prefix) else { return nil }
    let separator = recordName.index(before: prefix.endIndex)
    let identifierStart = recordName.index(after: separator)
    guard identifierStart < recordName.endIndex else { return nil }
    return String(recordName[identifierStart...])
}

private func objectIdentifier(
    from reference: CKRecord.Reference,
    expectedEntityType: String,
    expectedZoneID: CKRecordZone.ID
) -> String? {
    guard reference.recordID.zoneID == expectedZoneID else { return nil }
    return objectIdentifier(
        fromCloudKitRecordName: reference.recordID.recordName,
        expectedEntityType: expectedEntityType
    )
}

extension RealmSwiftAdapter: @unchecked Sendable { }

public final class RealmSwiftAdapter:
    NSObject,
    @preconcurrency ModelAdapter,
    TerminalSynchronizationStateModelAdapter,
    ChangeFeedResetMigrating {
    private static let mutationJournalRecoveryEntityTypePrefix =
        "__BigSyncKitMutationJournalRecovery.v2."
    private static let mutationJournalRecoveryVersion = 2

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
    private let acknowledgementIssuerID = UUID()
    
    @BigSyncBackgroundActor
    private var cancelSync: Bool = false
    @BigSyncBackgroundActor
    private var cancellationGeneration: UInt64 = 0
    
    private lazy var persistentAssetManager: PersistentAssetManager = {
        PersistentAssetManager(
            identifier: "\(recordZoneID.ownerName).\(recordZoneID.zoneName).\(targetRealmConfigurations.map { $0.fileURL?.lastPathComponent ?? UUID().uuidString } .joined(separator: "-")).\(targetRealmConfigurations.map { $0.schemaVersion } .reduce(0, +))",
            rootDirectoryURL: assetDirectoryURL
        )
    }()
    private let assetDirectoryURL: URL?
    
    var realmProvider: RealmProvider?
    
    //    var collectionNotificationTokens = [NotificationToken]()
    //    var collectionNotificationTokens = Set<AnyCancellable>()
    //    var pendingTrackingUpdates = [ObjectUpdate]()
    var modelTypes = [String: Object.Type]()
    public private(set) var hasChanges = false
    public private(set) var hasChangesCount: Int?
    
    private var resultsChangeSet = ResultsChangeSet()
    
    private var recentlyFetchedRecordModifiedAts = [String: Date]()
    
    private var appForegroundCancellable: AnyCancellable?
    private let immediateChecksSubject = PassthroughSubject<Void, Never>()
    @BigSyncBackgroundActor
    private let realmChangesSubject = PassthroughSubject<Void, Never>()
    @BigSyncBackgroundActor
    private var observedJournalRecordNames = [Int: Set<String>]()
    @BigSyncBackgroundActor
    private var observedRealmChangesTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var observedRealmChangesTaskID: UUID?
    
    @BigSyncBackgroundActor
    private var cancellables = Set<AnyCancellable>()
    
#if DEBUG
    @RealmBackgroundActor
    private var dummyRecordIdentifiers = Set<String>()
    var _testBeforeImportedRecordTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeImportedRecordPersistenceWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeRemoteDeletionTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeCleanupTargetWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforePendingMutationTrackingWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testAfterPendingMutationTrackingWrite:
        (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeChangeFeedResetCompletionMarkerWrite:
        (@BigSyncBackgroundActor @Sendable () throws -> Void)?
#endif
    
    private var isSetupInterrupted: Bool = false
    @BigSyncBackgroundActor
    private var setupTask: Task<Void, Error>?
    @BigSyncBackgroundActor
    private var setupGeneration = UUID()
    @BigSyncBackgroundActor
    private var initialSetupTask: Task<Void, Never>?

    public init(
        persistenceRealmConfiguration: Realm.Configuration,
        targetRealmConfigurations: [Realm.Configuration],
        excludedClassNames: [String],
        priorityEntityTypeNames: [String] = [],
        recordZoneID: CKRecordZone.ID,
        logger: Logging.Logger,
        startSetupTask: Bool = true,
        assetDirectoryURL: URL? = nil
    ) {
        if persistenceRealmConfiguration.encryptionKey != nil
            || targetRealmConfigurations.contains(where: {
                $0.encryptionKey != nil
            }) {
            fatalError(
                "BigSyncKit does not support encrypted Realm configurations"
            )
        }
        self.persistenceRealmConfiguration = persistenceRealmConfiguration
        self.targetRealmConfigurations = targetRealmConfigurations
        let internalClassNames = [BigSyncPendingMutation.className()]
        self.excludedClassNames = Array(Set(excludedClassNames + internalClassNames))
        self.priorityEntityTypeNames = priorityEntityTypeNames
        self.zoneID = recordZoneID
        self.logger = logger
        self.assetDirectoryURL = assetDirectoryURL
        
        super.init()

        BigSyncMutationPolicy(
            excludedClassNames: self.excludedClassNames
        ).install(
            configurations: targetRealmConfigurations
        )
        
        setupTypeNamesLookup()
        
        if startSetupTask {
            initialSetupTask = Task(priority: .utility) {
                @BigSyncBackgroundActor [weak self] in
                guard let self = self else { return }
                do {
                    try Task.checkCancellation()
                    try await ensureSetup()
                } catch is CancellationError {
                    // A synchronizer reset owns retrying setup after its
                    // cancellation barrier has cleared the tracking Realm.
                } catch {
                    logger.error("BigSyncKit initial Realm setup failed: \(error)")
                }
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
        let shouldResumeAfterReset = !cancelSync
        if shouldResumeAfterReset {
            cancelSynchronization()
        }
        let resetCancellationGeneration = cancellationGeneration
        await waitForCancellation()

        let activeSetupTask = setupTask
        activeSetupTask?.cancel()
        _ = try? await activeSetupTask?.value
        setupTask = nil
        // A destructive reset invalidates setup readiness before any tracking
        // mutation. If a Realm write below fails, the next
        // synchronization must retry setup rather than use a partially cleared
        // provider.
        isSetupInterrupted = true
        invalidateTokens()

        // Account replacement and backup-restore recovery can run before the
        // asynchronous RealmProvider setup task has published its Realms. A
        // missing provider must not turn a destructive reset into a successful
        // no-op: open just the tracking Realm and establish the same durable
        // empty-state postcondition. The target Realms intentionally remain
        // untouched; their mutation journals are the recovery source that the
        // next setup forwards into fresh tracking state.
        let persistenceRealm: Realm
        if let configuredPersistenceRealm = realmProvider?.persistenceRealm {
            persistenceRealm = configuredPersistenceRealm
        } else {
            persistenceRealm = try await Realm(
                configuration: persistenceRealmConfiguration,
                actor: BigSyncBackgroundActor.shared
            )
        }
        try await persistenceRealm.asyncWrite {
            let rebuildState = persistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            )
            let rebuildingForChangeFeed = rebuildState?.isActive == true
            // Capture all existing rows before clearing them. Repeating this
            // transaction after a crash is safe: primary-key upserts retain
            // the original proof while the old rows are still present.
            if rebuildingForChangeFeed, let rebuildState {
                // Do not let a failed old-account migration contribute proof
                // to this account/epoch. This transaction is the durable
                // capture-and-clear boundary.
                persistenceRealm.delete(persistenceRealm.objects(RebuildProvenance.self))
                for entity in persistenceRealm.objects(SyncedEntity.self) where isOwnedEntityType(entity.entityType) {
                    let provenance = RebuildProvenance()
                    provenance.identifier = entity.identifier
                    provenance.entityType = entity.entityType
                    // A decoded archive is strongest proof, but a formerly
                    // server-backed tracking state must remain conservative if
                    // its system-fields archive was lost or corrupted.
                    // `.new` and `.deletedLocally` remain local intent and
                    // are handled by their durable pending generation below.
                    provenance.hadValidServerRecord =
                        hasValidServerRecordProof(entity)
                        || hadPriorServerMembership(entity)
                    provenance.priorState = entity.state
                    provenance.priorPendingGeneration = entity.pendingGeneration
                    provenance.accountScopeIdentifier = rebuildState.accountScopeIdentifier
                    provenance.epoch = rebuildState.epoch
                    persistenceRealm.add(provenance, update: .modified)
                }
                rebuildState.serverBootstrapStarted = false
                rebuildState.phase = "trackingReset"
            }
            // The tracking schema is owned by BigSyncKit. Delete the known
            // types explicitly so a configuration with `objectTypes == nil`
            // cannot accidentally turn reset into another successful no-op.
            persistenceRealm.delete(
                persistenceRealm.objects(PendingRelationship.self)
            )
            persistenceRealm.delete(persistenceRealm.objects(SyncedEntity.self))
            persistenceRealm.delete(
                persistenceRealm.objects(SyncedEntityType.self)
            )
            persistenceRealm.delete(persistenceRealm.objects(ServerToken.self))
        }

        // Nothing from the prior account/setup may be carried into the fresh
        // provider. Durable target-journal entries are deliberately not
        // cleared and will be rediscovered by setup.
        realmProvider = nil
        resultsChangeSet = ResultsChangeSet()
        recentlyFetchedRecordModifiedAts.removeAll(keepingCapacity: false)
        observedJournalRecordNames.removeAll(keepingCapacity: false)
        hasChanges = false
        hasChangesCount = 0
        persistentAssetManager.clearAssetFiles()
        
        guard shouldResumeAfterReset,
              cancellationGeneration == resetCancellationGeneration else {
            isSetupInterrupted = true
            return
        }
        isSetupInterrupted = true
        try await unsetCancellation()
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
        configuration.schemaVersion = 12
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
            ServerToken.self,
            RebuildProvenance.self,
            RebuildProvenanceState.self
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
        cancellationGeneration &+= 1
        initialSetupTask?.cancel()
        setupTask?.cancel()
        observedRealmChangesTask?.cancel()
    }

    @BigSyncBackgroundActor
    public func waitForCancellation() async {
        let activeInitialSetupTask = initialSetupTask
        let activeSetupTask = setupTask
        let activeObservedRealmChangesTask = observedRealmChangesTask
        activeInitialSetupTask?.cancel()
        activeSetupTask?.cancel()
        activeObservedRealmChangesTask?.cancel()
        await activeInitialSetupTask?.value
        _ = try? await activeSetupTask?.value
        await activeObservedRealmChangesTask?.value
        initialSetupTask = nil
    }
    
    @BigSyncBackgroundActor
    public func unsetCancellation() async throws {
        //        debugPrint("# unset cancel")
        cancelSync = false
        // `waitForCancellation()` also owns a queued bootstrap task. If that
        // task was cancelled before it could install the provider, a normal
        // synchronizer start must restart setup instead of leaving this adapter
        // permanently inert.
        if isSetupInterrupted || realmProvider == nil {
            try await ensureSetup()
        }
        if !observedJournalRecordNames.isEmpty {
            realmChangesSubject.send(())
        }
    }
    
    @BigSyncBackgroundActor
    // Internal so same-module readiness-boundary extensions (for example the
    // read-only synchronization audit) can share the established setup path.
    // This remains hidden from BigSyncKit clients.
    func ensureSetup() async throws {
        try Task.checkCancellation()
        // A non-nil provider is not sufficient while setup is in progress or
        // failed. Conversely, a completed setup must not be rerun
        // by terminal forwarding because that drain intentionally suppresses
        // delegate wakeups.
        if !isSetupInterrupted, realmProvider != nil {
            return
        }
        if let setupTask {
            try await setupTask.value
            return
        }

        let generation = UUID()
        setupGeneration = generation
        let task = Task(priority: .utility) { @BigSyncBackgroundActor [weak self] in
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
        // Setup can be retried after cancellation or a cache reset. Tear down
        // any prior notification graph so retries never accumulate
        // duplicate Realm observers or debounced processors. Readiness remains
        // interrupted until every setup step, including journal forwarding,
        // succeeds.
        isSetupInterrupted = true
        invalidateTokens()
        try validateUniqueTrackedEntityOwnership()
        let provider = try await RealmProvider(
            persistenceConfiguration: persistenceRealmConfiguration,
            targetConfigurations: targetRealmConfigurations
        )
        self.realmProvider = provider
        let realmProvider = provider

        guard let persistenceRealm = realmProvider.persistenceRealm else {
            try Task.checkCancellation()
            throw RealmSwiftAdapterError.setupUnavailable
        }
        // An empty user Realm is still initialized. Without this durable marker,
        // empty databases repeated the full initial scan on every launch.
        let recoveryMarkerIDs = mutationJournalRecoveryMarkerIDs()
        let needsMutationJournalRecovery = needsMutationJournalRecovery(
            in: persistenceRealm,
            markerIDs: recoveryMarkerIDs
        )
        if needsMutationJournalRecovery {
            // The marker signature changes when synchronized object types or
            // exclusions change. Retire tracking metadata that the current
            // adapter no longer owns before deciding whether initial setup is
            // empty. These are local bookkeeping rows, not CloudKit tombstones.
            try await reconcilePersistedTrackingOwnership(
                in: persistenceRealm
            )
        }

        let pendingStates = [
            SyncedEntityState.new.rawValue,
            SyncedEntityState.changed.rawValue,
            SyncedEntityState.deletedLocally.rawValue,
        ]
        let entitiesMissingGeneration = persistenceRealm.objects(SyncedEntity.self)
            .where { $0.state.in(pendingStates) && $0.pendingGeneration == nil }
        // Assigning a generation removes the row from this live query.
        // Snapshot primary keys first, then resolve each row inside the
        // transaction so Realm never mutates a collection while its fast
        // enumerator is active.
        let identifiersMissingGeneration = entitiesMissingGeneration
            .map(\.identifier)
        if !identifiersMissingGeneration.isEmpty {
            try await persistenceRealm.asyncWrite {
                for identifier in identifiersMissingGeneration {
                    guard let entity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: identifier
                    ), entity.pendingGeneration == nil,
                       pendingStates.contains(entity.state),
                       isOwnedEntityType(entity.entityType) else {
                        continue
                    }
                    entity.pendingGeneration = UUID().uuidString
                }
            }
        }

        let rebuildState = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        )
        // Completed reset state is historical. A later schema/exclusion
        // signature change must still receive one broad recovery pass.
        let skipsBroadDiscovery = rebuildState?.isActive == true
        let syncEmpty = persistenceRealm.objects(SyncedEntity.self).isEmpty
        // A reset is server-first. It must not turn all surviving target
        // objects into `.new` before the nil-token CloudKit bootstrap has had
        // an opportunity to import remote records and deletions.
        let needsInitialSetup = syncEmpty && needsMutationJournalRecovery && !skipsBroadDiscovery
        
        if needsInitialSetup {
            // Reset/setup failures are synchronization failures. Continuing
            // would make an unprepared adapter look like an empty data set.
            try await modelAdapterDelegate?.needsInitialSetup()
        }
        
        guard let targetReaderRealms = realmProvider.targetReaderRealms else {
            try Task.checkCancellation()
            throw RealmSwiftAdapterError.setupUnavailable
        }
        
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
                        throw CancellationError()
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
        observeRealmChanges()

        if needsInitialSetup {
            try await updateCreatedAndModified(notifyDelegate: false)
            try await markMutationJournalRecoveryComplete(
                in: persistenceRealm,
                markerIDs: recoveryMarkerIDs
            )
        } else if needsMutationJournalRecovery && !skipsBroadDiscovery {
            // This is the only broad scan for clients that carry the journal
            // schema. It recovers changes made by older builds and records that
            // predate durable changed-ID tracking.
            do {
                try await createMissingSyncedEntities()
            } catch is CancellationError {
                isSetupInterrupted = true
                throw CancellationError()
            } catch {
                isSetupInterrupted = true
                throw error
            }
            await enqueueCreatedAndModified(
                includeOnlyInitialSyncEligible: true
            )
            try await processEnqueuedChanges(notifyDelegate: false)
            try await updateCreatedAndModified(notifyDelegate: false)
            try await markMutationJournalRecoveryComplete(
                in: persistenceRealm,
                markerIDs: recoveryMarkerIDs
            )
        } else {
            // Normal launches touch only the durable record-level journal.
            try await updateCreatedAndModified(notifyDelegate: false)
        }

        updateHasChanges(realm: persistenceRealm)
        isSetupInterrupted = false
        
        //        if hasChanges {
        //            Task { @BigSyncBackgroundActor in
        //                await modelAdapterDelegate?.hasChangesToUpload()
        //            }
        //        }
    }

    private func validateUniqueTrackedEntityOwnership() throws {
        var ownedEntityTypes = Set<String>()
        for configuration in targetRealmConfigurations {
            let entityTypes = Set(
                (configuration.objectTypes ?? []).map { $0.className() }
            ).subtracting(excludedClassNames)
            for entityType in entityTypes {
                guard ownedEntityTypes.insert(entityType).inserted else {
                    throw RealmSwiftAdapterError.duplicateTrackedEntityType(
                        entityType: entityType
                    )
                }
            }
        }
    }

    private var ownedEntityTypeNames: Set<String> {
        Set(modelTypes.keys).subtracting(excludedClassNames)
    }

    private func isOwnedEntityType(_ entityType: String) -> Bool {
        modelTypes[entityType] != nil
            && !excludedClassNames.contains(entityType)
    }

    @BigSyncBackgroundActor
    private func reconcilePersistedTrackingOwnership(
        in persistenceRealm: Realm
    ) async throws {
        let ownedEntityTypes = ownedEntityTypeNames
        let staleEntityIdentifiers: [String] = Array(
            persistenceRealm.objects(SyncedEntity.self)
        )
            .compactMap { entity in
                ownedEntityTypes.contains(entity.entityType)
                    ? nil
                    : entity.identifier
            }
        guard !staleEntityIdentifiers.isEmpty else { return }

        let staleIdentifierSet = Set(staleEntityIdentifiers)
        try await persistenceRealm.asyncWrite {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }

            // PendingRelationship has no primary key, so resolve the current
            // relationship set inside the same transaction that removes its
            // retired origin tracking rows.
            let staleRelationships = Array(
                persistenceRealm.objects(PendingRelationship.self)
            ).filter { relationship in
                guard let ownerIdentifier = relationship.forSyncedEntity?
                    .identifier else {
                    return true
                }
                return staleIdentifierSet.contains(ownerIdentifier)
            }
            persistenceRealm.delete(staleRelationships)

            for identifier in staleEntityIdentifiers {
                guard let entity = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: identifier
                ), !ownedEntityTypes.contains(entity.entityType) else {
                    continue
                }
                persistenceRealm.delete(entity)
            }
        }
        logger.info(
            "QSCloudKitSynchronizer >> Retired \(staleEntityIdentifiers.count) tracking records for unowned model types"
        )
    }

    @BigSyncBackgroundActor
    private func mutationJournalRecoveryMarkerIDs() -> Set<String> {
        Set(targetRealmConfigurations.map { configuration in
            let realmIdentity: String
            if let fileURL = configuration.fileURL {
                realmIdentity = fileURL.standardizedFileURL.path
            } else if let inMemoryIdentifier = configuration.inMemoryIdentifier {
                realmIdentity = "memory:\(inMemoryIdentifier)"
            } else {
                realmIdentity = "default"
            }
            let synchronizedTypeNames = (configuration.objectTypes ?? [])
                .map { $0.className() }
                .filter { !excludedClassNames.contains($0) }
                .sorted()
                .joined(separator: ",")
            let component =
                "\(realmIdentity)|schema:\(configuration.schemaVersion)|\(synchronizedTypeNames)"
            let digest = SHA256.hash(data: Data(component.utf8))
                .map { String(format: "%02x", $0) }
                .joined()
            return Self.mutationJournalRecoveryEntityTypePrefix + digest
        })
    }

    private func needsMutationJournalRecovery(
        in persistenceRealm: Realm,
        markerIDs: Set<String>
    ) -> Bool {
        markerIDs.contains { markerID in
            persistenceRealm.object(
                ofType: SyncedEntityType.self,
                forPrimaryKey: markerID
            )?.recoveryVersion != Self.mutationJournalRecoveryVersion
        }
    }

    @BigSyncBackgroundActor
    private func markMutationJournalRecoveryComplete(
        in persistenceRealm: Realm,
        markerIDs: Set<String>
    ) async throws {
        try await persistenceRealm.asyncWrite {
            for markerID in markerIDs {
                let state = persistenceRealm.object(
                    ofType: SyncedEntityType.self,
                    forPrimaryKey: markerID
                ) ?? SyncedEntityType(entityType: markerID)
                state.recoveryVersion = Self.mutationJournalRecoveryVersion
                persistenceRealm.add(state, update: .modified)
            }
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
                    self?.startObservedRealmChangesTaskIfNeeded()
                }
            }
            .store(in: &cancellables)
        
        // Every supported target Realm has the durable record-level journal.
        // Older timestamp metadata is consulted only by the bounded recovery
        // scan above, never by steady-state observation.
        for (idx, targetReaderRealm) in targetReaderRealms.enumerated() {
            let token = targetReaderRealm.objects(BigSyncPendingMutation.self).observe {
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
                    let recordNames = changedIndexes.map {
                        collection[$0].recordName
                    }
                    guard !recordNames.isEmpty else { return }
                    Task { @BigSyncBackgroundActor [weak self] in
                        self?.enqueueObservedJournalRecordNames(
                            recordNames,
                            realmIndex: idx
                        )
                    }
                    return
                case .error(let error):
                    // RealmSwift 20 documents collection observation errors
                    // as unreachable. The durable rows remain available for
                    // the next setup/terminal drain if the runtime ever
                    // violates that contract.
                    assertionFailure(
                        "Unexpected Realm collection observation error: \(error)"
                    )
                    return
                }
            }
            cancellables.insert(AnyCancellable { token.invalidate() })
        }
    }

    @BigSyncBackgroundActor
    private func enqueueObservedJournalRecordNames(
        _ recordNames: [String],
        realmIndex: Int
    ) {
        observedJournalRecordNames[realmIndex, default: []]
            .formUnion(recordNames)
        realmChangesSubject.send(())
    }

    @BigSyncBackgroundActor
    private func startObservedRealmChangesTaskIfNeeded() {
        guard !cancelSync, observedRealmChangesTask == nil else { return }
        let taskID = UUID()
        observedRealmChangesTaskID = taskID
        observedRealmChangesTask = Task(priority: .background) {
            @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            do {
                try await processObservedRealmChanges()
            } catch is CancellationError {
                // The durable journal names were requeued. unsetCancellation()
                // will signal the processor after the cancellation barrier.
            } catch {
                logger.error(
                    "BigSyncKit mutation journal forwarding failed: \(error)"
                )
            }
            if observedRealmChangesTaskID == taskID {
                observedRealmChangesTask = nil
                observedRealmChangesTaskID = nil
                if !cancelSync,
                   !observedJournalRecordNames.isEmpty {
                    realmChangesSubject.send(())
                }
            }
        }
    }

    @BigSyncBackgroundActor
    private func processObservedRealmChanges() async throws {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else {
            return
        }
        let observed = observedJournalRecordNames
        observedJournalRecordNames.removeAll(keepingCapacity: true)

        do {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            for (idx, recordNames) in observed
            where idx < targetReaderRealms.count {
                let realm = targetReaderRealms[idx]
                try await forwardPendingMutations(
                    pendingMutationSnapshots(
                        for: recordNames,
                        in: realm
                    ),
                    in: realm
                )
            }
            // Forwarding can suspend after a durable tracking write (for
            // example while waking the synchronizer). Do not acknowledge this
            // batch as complete after a reset has cancelled it: the catch below
            // requeues its durable journal identities for the resumed adapter.
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
        } catch {
            for (idx, recordNames) in observed {
                observedJournalRecordNames[idx, default: []]
                    .formUnion(recordNames)
            }
            if !cancelSync {
                realmChangesSubject.send(())
            }
            throw error
        }
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
            return pendingMutationSnapshot(mutation, in: realm)
        }
    }

    private func pendingMutationSnapshot(
        _ mutation: BigSyncPendingMutation,
        in realm: Realm
    ) -> BigSyncPendingMutationSnapshot {
        BigSyncPendingMutationSnapshot(
            recordName: mutation.recordName,
            entityType: mutation.entityType,
            objectIdentifier: mutation.objectIdentifier,
            generation: mutation.generation,
            changedAt: mutation.changedAt,
            isDeletion: pendingMutationTargetsDeletedObject(mutation, in: realm)
        )
    }

    private func pendingMutationTargetsDeletedObject(
        _ mutation: BigSyncPendingMutation,
        in realm: Realm
    ) -> Bool {
        guard let objectType = modelTypes[mutation.entityType],
              let primaryKey = objectType.primaryKey()
                ?? objectType.sharedSchema()?.primaryKeyProperty?.name,
              let primaryKeyProperty = objectType.sharedSchema()?.properties
                .first(where: { $0.name == primaryKey }) else {
            return false
        }
        let primaryKeyValue: Any
        switch primaryKeyProperty.type {
        case .string:
            primaryKeyValue = mutation.objectIdentifier
        case .int:
            guard let value = Int64(mutation.objectIdentifier) else { return false }
            primaryKeyValue = value
        case .UUID:
            guard let value = UUID(uuidString: mutation.objectIdentifier) else { return false }
            primaryKeyValue = value
        case .objectId:
            guard let value = try? ObjectId(string: mutation.objectIdentifier) else { return false }
            primaryKeyValue = value
        default:
            return false
        }
        return (realm.object(ofType: objectType, forPrimaryKey: primaryKeyValue)
            as? SoftDeletable)?.isDeleted == true
    }
    
    /// Immediately updates.
    @BigSyncBackgroundActor
    private func updateCreatedAndModified(notifyDelegate: Bool = true) async throws {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else { return }
        for targetReaderRealm in targetReaderRealms {
            try await forwardPendingMutations(
                in: targetReaderRealm,
                notifyDelegate: notifyDelegate
            )
        }
    }

    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(
        in targetReaderRealm: Realm,
        notifyDelegate: Bool = true
    ) async throws -> Int {
        // Freeze the journal boundary so paging does not change which generations
        // this drain promises to forward, while avoiding one O(N) snapshot array.
        let mutations = targetReaderRealm.objects(BigSyncPendingMutation.self)
            .sorted(byKeyPath: "recordName")
            .freeze()
        let pageSize = 1_000
        var forwardedCount = 0
        var offset = 0
        while offset < mutations.count {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            let end = min(offset + pageSize, mutations.count)
            var pending = [BigSyncPendingMutationSnapshot]()
            pending.reserveCapacity(end - offset)
            for index in offset..<end {
                pending.append(
                    pendingMutationSnapshot(
                        mutations[index],
                        in: targetReaderRealm
                    )
                )
            }
            forwardedCount += try await forwardPendingMutations(
                pending,
                in: targetReaderRealm,
                notifyDelegate: false,
                updateStatus: false
            )
            offset = end
            await Task.yield()
        }

        if forwardedCount > 0,
           let persistenceRealm = realmProvider?.persistenceRealm {
            updateHasChanges(realm: persistenceRealm)
            if notifyDelegate {
                await modelAdapterDelegate?.hasChangesToUpload()
            }
        }
        return forwardedCount
    }

    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(
        _ pending: [BigSyncPendingMutationSnapshot],
        in targetReaderRealm: Realm,
        notifyDelegate: Bool = true,
        updateStatus: Bool = true
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
#if DEBUG
            try await _testBeforePendingMutationTrackingWrite?()
#endif
            // Queue behind any persistence transaction already admitted on
            // this actor. A direct `write` can attempt a nested begin while an
            // earlier `asyncWrite` is suspended at its commit boundary.
            try await persistenceRealm.asyncWrite {
                // A frozen/page snapshot can become stale while this task is
                // waiting for the tracking transaction. Re-resolve each
                // identity only after that transaction is acquired, keeping
                // the live-journal read and tracking publication in one
                // non-suspending boundary so an older pass cannot overwrite a
                // newer generation already forwarded by reentrant work.
                targetReaderRealm.refresh()
                let currentMutations: [BigSyncPendingMutationSnapshot] =
                    chunk.compactMap { mutation in
                        guard let current = targetReaderRealm.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: mutation.recordName
                        ) else { return nil }
                        return pendingMutationSnapshot(
                            current,
                            in: targetReaderRealm
                        )
                    }
                for mutation in currentMutations {
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
                        modified: !mutation.isDeletion,
                        deleted: mutation.isDeletion,
                        generation: mutation.generation,
                        persistenceRealm: persistenceRealm
                    )
                    forwardedCount += 1
                }
            }
#if DEBUG
            try await _testAfterPendingMutationTrackingWrite?()
#endif
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

        if forwardedCount > 0, updateStatus {
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
            
            // The owning caller drains resultsChangeSet before returning. This
            // keeps legacy scans inside the same cancellation barrier instead
            // of launching a second debounced task.
        }
    }

#if DEBUG
    @BigSyncBackgroundActor
    func _test_enqueueObservedJournalRecordNames(
        _ recordNames: [String],
        realmIndex: Int = 0
    ) {
        enqueueObservedJournalRecordNames(
            recordNames,
            realmIndex: realmIndex
        )
    }

    @BigSyncBackgroundActor
    func _test_processObservedRealmChanges() async throws {
        try await processObservedRealmChanges()
    }

    @BigSyncBackgroundActor
    func _test_startObservedRealmChangesTaskIfNeeded() {
        startObservedRealmChangesTaskIfNeeded()
    }

    @BigSyncBackgroundActor
    func _test_hasPendingObservedRealmChanges() -> Bool {
        !observedJournalRecordNames.isEmpty
    }

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
        // Tests that model relaunch/recovery explicitly force setup. Production
        // readiness checks use the completed-state fast path.
        isSetupInterrupted = true
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
            guard try await getOrCreateSyncedEntityType(schema) != nil else {
                throw RealmSwiftAdapterError.setupUnavailable
            }
            
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
            guard try await getOrCreateSyncedEntityType(schema) != nil else {
                throw RealmSwiftAdapterError.setupUnavailable
            }
            
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
        
        await Task.yield()
        await persistenceRealm.asyncRefresh()
        
        var lastTrackedChangesAtUpdates: [(String, Date)] = []
        for (schema, latestExplicitlyModifiedAt) in currentChangeSet.trackedChangeHighWatermarks {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            guard let syncedEntityType = try await getOrCreateSyncedEntityType(schema) else {
                throw RealmSwiftAdapterError.setupUnavailable
            }
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
        let ownedEntityTypes = ownedEntityTypeNames.sorted()
        let results = realm.objects(SyncedEntity.self).where {
            $0.state.in(pendingStates)
                && $0.entityType.in(ownedEntityTypes)
        }
        // The common idle path only needs to prove that the indexed result is
        // still empty. Avoid recomputing its exact cardinality at every no-op
        // import, acknowledgement, and terminal callback.
        if hasChangesCount == 0, results.first == nil {
            hasChanges = false
            return
        }
        let count = results.count
        let previousCount = hasChangesCount
        hasChangesCount = count
        hasChanges = count > 0
        guard previousCount != count else { return }

        logger.debug(
            "QSCloudKitSynchronizer >> \(count) changed records remaining to upload."
        )
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
            } else {
                // No tracking row does not prove the record was never uploaded:
                // reset/recovery can rebuild the journal before persistence
                // tracking. Preserve an explicit remote tombstone instead of
                // leaving this mutation permanently unforwarded.
                let deletedEntity = SyncedEntity(
                    entityType: entityName,
                    identifier: identifier,
                    state: SyncedEntityState.deletedLocally.rawValue
                )
                deletedEntity.pendingGeneration = pendingGeneration
                persistenceRealm.add(deletedEntity, update: .modified)
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
            await Task.yield()
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
                // These are selection-time cache candidates. A concurrent
                // journal forward may already have created a newer pending
                // tracking row for the same record; preserve that row.
                guard persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: entity.identifier
                ) == nil else {
                    continue
                }
                persistenceRealm.add(entity)
            }
        }
    }
    
    func getObjectIdentifier(for syncedEntity: SyncedEntity) -> Any? {
        getObjectIdentifier(
            recordName: syncedEntity.identifier,
            entityType: syncedEntity.entityType
        )
    }

    func getObjectIdentifier(recordName: String, entityType: String) -> Any? {
        let prefix = entityType + "."
        guard recordName.hasPrefix(prefix) else { return nil }
        let objectIdentifier = String(recordName.dropFirst(prefix.count))
        guard !objectIdentifier.isEmpty else { return nil }
        return getObjectIdentifier(
            stringObjectId: objectIdentifier,
            entityType: entityType
        )
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
        !serverDifferencePropertyNames(record: record, object: object).isEmpty
    }

    func serverDifferencePropertyNames(
        record: CKRecord,
        object: Object
    ) -> [String] {
        let objectProperties = object.objectSchema.properties
        var differences = [String]()
        
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
                func existingCollectionIsEmpty() -> Bool {
                    if let collection = existingValue as? RLMSwiftCollectionBase {
                        return collection._rlmCollection.count == 0
                    }
                    if property.isMap {
                        switch property.type {
                        case .int:
                            return (existingValue as? RealmSwift.Map<String, Int>)?.count == 0
                        case .string:
                            return (existingValue as? RealmSwift.Map<String, String>)?.count == 0
                        case .bool:
                            return (existingValue as? RealmSwift.Map<String, Bool>)?.count == 0
                        case .float:
                            return (existingValue as? RealmSwift.Map<String, Float>)?.count == 0
                        case .double:
                            return (existingValue as? RealmSwift.Map<String, Double>)?.count == 0
                        case .data:
                            return (existingValue as? RealmSwift.Map<String, Data>)?.count == 0
                        case .date:
                            return (existingValue as? RealmSwift.Map<String, Date>)?.count == 0
                        case .UUID:
                            return (existingValue as? RealmSwift.Map<String, UUID>)?.count == 0
                        default:
                            return false
                        }
                    }
                    return false
                }

                // Handle one side being nil first
                guard !(newValue == nil && existingValue == nil) else {
                    return false
                }
                if newValue == nil,
                   (property.isArray || property.isSet || property.isMap),
                   existingCollectionIsEmpty() {
                    // Empty Realm collections are intentionally represented by
                    // an absent CloudKit field.
                    return false
                }
                if (newValue == nil && existingValue != nil) || (newValue != nil && existingValue == nil) {
                    return true
                }

                if property.type == .object,
                   !property.isArray,
                   !property.isSet,
                   let expectedEntityType = property.objectClassName,
                   let existingObject = existingValue as? Object,
                   let primaryKey = type(of: existingObject).primaryKey()
                    ?? existingObject.objectSchema.primaryKeyProperty?.name {
                    let targetIdentifier = Self.getTargetObjectStringIdentifier(
                        for: existingObject,
                        usingPrimaryKey: primaryKey
                    )
                    let expectedRecordName = "\(expectedEntityType).\(targetIdentifier)"
                    if let recordName = newValue as? String {
                        return recordName != expectedRecordName
                    }
                    if let reference = newValue as? CKRecord.Reference {
                        return reference.recordID.zoneID != zoneID
                            || reference.recordID.recordName != expectedRecordName
                    }
                    return true
                }
                
                if let newValue = newValue as? CKRecord.Reference {
                    guard let expectedEntityType = property.objectClassName,
                          let newObjectIdentifier = objectIdentifier(
                            from: newValue,
                            expectedEntityType: expectedEntityType,
                            expectedZoneID: zoneID
                          ) else {
                        // Let the throwing decoder reject the malformed
                        // relationship rather than trapping during this cheap
                        // change-detection pass.
                        return true
                    }
                    
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
                    case .object:
                        guard let newValue = newValue as? [String],
                              let existingValue = existingValue as? RLMSwiftCollectionBase,
                              let expectedEntityType = property.objectClassName else {
                            return true
                        }
                        let existingRecordNames = (0..<existingValue._rlmCollection.count)
                            .compactMap { existingValue._rlmCollection[$0] as? Object }
                            .map { linkedObject -> String in
                                let primaryKey = type(of: linkedObject).primaryKey()
                                    ?? linkedObject.objectSchema.primaryKeyProperty!.name
                                return "\(expectedEntityType).\(Self.getTargetObjectStringIdentifier(for: linkedObject, usingPrimaryKey: primaryKey))"
                            }
                        return Set(newValue) != Set(existingRecordNames)
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
                    case .object:
                        guard let newValue = newValue as? [String],
                              let existingValue = existingValue as? RLMSwiftCollectionBase,
                              let expectedEntityType = property.objectClassName else {
                            return true
                        }
                        let existingRecordNames = (0..<existingValue._rlmCollection.count)
                            .compactMap { existingValue._rlmCollection[$0] as? Object }
                            .map { linkedObject -> String in
                                let primaryKey = type(of: linkedObject).primaryKey()
                                    ?? linkedObject.objectSchema.primaryKeyProperty!.name
                                return "\(expectedEntityType).\(Self.getTargetObjectStringIdentifier(for: linkedObject, usingPrimaryKey: primaryKey))"
                            }
                        return newValue != existingRecordNames
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
                    case .data:
                        guard let newValue = result as? [String: Data], let existingValue = existingValue as? RealmSwift.Map<String, Data> else { return true }
                        return newValue != existingValue.reduce(into: [String: Data]()) { $0[$1.key] = $1.value }
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
                        // Realm and CloudKit persist dates at millisecond
                        // precision even though Foundation exposes a finer
                        // grained value.
                        return abs(newValue.timeIntervalSince(existingValue))
                            >= 0.001
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
                differences.append(key)
            }
        }
        return differences
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
                // Choosing the local object is an authoritative conflict
                // decision. Persist that decision through the same target-
                // Realm journal boundary as an ordinary user edit so a later
                // upload cannot be lost after process death. This path is
                // reached only when the selection snapshot had no pending
                // mutation; journaled local values are fenced before entering
                // `applyChanges`.
                guard let changeMetadata = object as? ChangeMetadataRecordable
                else {
                    throw RealmSwiftAdapterError
                        .missingChangeMetadataConformance(
                            entityType: entityType
                        )
                }
                changeMetadata.refreshChangeMetadata(
                    explicitlyModified: true,
                    at: Date()
                )
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
        func malformed(_ expected: String) -> RealmSwiftRemoteRecordDecodingError {
            .malformedField(
                recordName: record.recordID.recordName,
                propertyName: key,
                expected: expected
            )
        }
        func requireArray<Element>(
            _ type: Element.Type,
            expected: String
        ) throws -> [Element] {
            guard let result = value as? [Element] else {
                throw malformed(expected)
            }
            return result
        }
        if (property.isSet || property.isArray || property.isMap),
           !record.allKeys().contains(key) {
            // Full zone-change fetches use desiredKeys == nil, so an absent
            // collection field is the CloudKit representation of an empty
            // collection. Realm collections cannot be assigned nil.
            if property.type == .object {
                guard property.objectClassName != nil else {
                    throw malformed("a Realm relationship type")
                }
                // Replace any older deferred relationship with an explicit
                // empty intent. Clearing only the target object would leave a
                // prior missing-target request able to replay later.
                appendPendingRelationship(
                    name: property.name,
                    syncedEntityID: syncedEntityIdentifier,
                    targetIdentifiers: [],
                    record: record,
                    to: &pendingRelationships
                )
                return
            }
            clearCollection(property: property, on: object)
            return
        }
        
        // List/Set support forked from IceCream: https://github.com/caiyue1993/IceCream/blob/master/IceCream/Classes/CKRecordRecoverable.swift
        var recordValue: Any?
        if property.isSet {
            switch property.type {
            case .int:
                let value = try requireArray(Int.self, expected: "an array of integers")
                var set = Set<Int>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .string:
                let value = try requireArray(String.self, expected: "an array of strings")
                var set = Set<String>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .bool:
                let value = try requireArray(Bool.self, expected: "an array of booleans")
                var set = Set<Bool>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .float:
                let value = try requireArray(Float.self, expected: "an array of floats")
                var set = Set<Float>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .double:
                let value = try requireArray(Double.self, expected: "an array of doubles")
                var set = Set<Double>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .data:
                let value = try requireArray(Data.self, expected: "an array of data values")
                var set = Set<Data>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .date:
                let value = try requireArray(Date.self, expected: "an array of dates")
                var set = Set<Date>()
                try value.forEach {
                    try Task.checkCancellation()
                    set.insert($0)
                }
                recordValue = set
            case .UUID:
                let stringArray = try requireArray(
                    String.self,
                    expected: "an array of UUID strings"
                )
                var set = Set<UUID>()
                for string in stringArray {
                    try Task.checkCancellation()
                    guard let uuid = UUID(uuidString: string) else {
                        throw malformed("an array of UUID strings")
                    }
                    set.insert(uuid)
                }
                object.setValue(set, forKey: key)
                return
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                guard let expectedEntityType = property.objectClassName else {
                    throw malformed("a Realm relationship type")
                }
                var targetIdentifiers = [String]()
                if let value = value as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: recordName,
                            expectedEntityType: expectedEntityType
                        ) else { throw malformed("an array of CloudKit record names") }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else if let value = value as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            from: reference,
                            expectedEntityType: expectedEntityType,
                            expectedZoneID: zoneID
                        ) else { throw malformed("an array of CloudKit references") }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else {
                    throw malformed("an array of CloudKit references or record names")
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
                throw malformed("a supported Realm set element type")
            }
            try Task.checkCancellation()
            if let recordValue {
                object.setValue(recordValue, forKey: property.name)
            }
        } else if property.isArray {
            switch property.type {
            case .int:
                let value = try requireArray(Int.self, expected: "an array of integers")
                let list = List<Int>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .string:
                let value = try requireArray(String.self, expected: "an array of strings")
                if object[property.name] is List<URL> {
                    let list = List<URL>()
                    for item in value {
                        try Task.checkCancellation()
                        guard let url = URL(string: item) else {
                            throw malformed("an array of URL strings")
                        }
                        list.append(url)
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
                let value = try requireArray(Bool.self, expected: "an array of booleans")
                let list = List<Bool>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .float:
                let value = try requireArray(Float.self, expected: "an array of floats")
                let list = List<Float>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .double:
                let value = try requireArray(Double.self, expected: "an array of doubles")
                let list = List<Double>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .data:
                let value = try requireArray(Data.self, expected: "an array of data values")
                let list = List<Data>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .date:
                let value = try requireArray(Date.self, expected: "an array of dates")
                let list = List<Date>()
                for item in value {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .UUID:
                let value = try requireArray(
                    String.self,
                    expected: "an array of UUID strings"
                )
                let list = List<UUID>()
                let newValues = try value.map {
                    try Task.checkCancellation()
                    guard let uuid = UUID(uuidString: $0) else {
                        throw malformed("an array of UUID strings")
                    }
                    return uuid
                }
                for item in newValues {
                    try Task.checkCancellation()
                    list.append(item)
                }
                recordValue = list
            case .object:
                // Save relationship to be applied after all records have been downloaded and persisted
                // to ensure target of the relationship has already been created
                guard let expectedEntityType = property.objectClassName else {
                    throw malformed("a Realm relationship type")
                }
                var targetIdentifiers = [String]()
                if let value = value as? [String] {
                    for recordName in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            fromCloudKitRecordName: recordName,
                            expectedEntityType: expectedEntityType
                        ) else { throw malformed("an array of CloudKit record names") }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else if let value = value as? [CKRecord.Reference] {
                    for reference in value {
                        try Task.checkCancellation()
                        guard let objectIdentifier = objectIdentifier(
                            from: reference,
                            expectedEntityType: expectedEntityType,
                            expectedZoneID: zoneID
                        ) else { throw malformed("an array of CloudKit references") }
                        targetIdentifiers.append(objectIdentifier)
                    }
                } else {
                    throw malformed("an array of CloudKit references or record names")
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
                throw malformed("a supported Realm list element type")
            }
            try Task.checkCancellation()
            if let recordValue {
                object.setValue(recordValue, forKey: property.name)
            }
        } else if property.isMap {
            guard var result = decodedCloudKitMap(value) else {
                throw malformed("an encoded string-keyed map")
            }
            if property.type == .UUID {
                var converted = [String: UUID]()
                for entry in result {
                    try Task.checkCancellation()
                    guard let string = entry.value as? String,
                          let uuid = UUID(uuidString: string) else {
                        throw malformed("a map of UUID strings")
                    }
                    converted[entry.key] = uuid
                }
                result = converted
            } else {
                let valuesAreValid: Bool
                switch property.type {
                case .int:
                    valuesAreValid = result.values.allSatisfy { $0 is Int }
                case .string:
                    valuesAreValid = result.values.allSatisfy { $0 is String }
                case .bool:
                    valuesAreValid = result.values.allSatisfy { $0 is Bool }
                case .float:
                    valuesAreValid = result.values.allSatisfy { $0 is Float }
                case .double:
                    valuesAreValid = result.values.allSatisfy { $0 is Double }
                case .data:
                    valuesAreValid = result.values.allSatisfy { $0 is Data }
                case .date:
                    valuesAreValid = result.values.allSatisfy { $0 is Date }
                default:
                    valuesAreValid = false
                }
                guard valuesAreValid else {
                    throw malformed("a map matching the Realm property type")
                }
            }
            try Task.checkCancellation()
            object.setValue(result, forKey: property.name)
        } else if let reference = value as? CKRecord.Reference {
            // Save relationship to be applied after all records have been downloaded and persisted
            // to ensure target of the relationship has already been created
            guard let expectedEntityType = property.objectClassName else {
                throw malformed("a Realm relationship type")
            }
            guard let objectIdentifier = objectIdentifier(
                from: reference,
                expectedEntityType: expectedEntityType,
                expectedZoneID: zoneID
            ) else { throw malformed("a CloudKit reference") }
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
            guard let expectedEntityType = property.objectClassName else {
                throw malformed("a Realm relationship type")
            }
            let targetIdentifiers: [String]
            if let recordName = record.value(forKey: property.name) as? String,
               let objectIdentifier = objectIdentifier(
                    fromCloudKitRecordName: recordName,
                    expectedEntityType: expectedEntityType
               ) {
                targetIdentifiers = [objectIdentifier]
            } else if value == nil, property.isOptional {
                targetIdentifiers = []
            } else {
                throw malformed("a CloudKit reference or record name")
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
            } else if value != nil {
                throw malformed("a UUID string")
            }
        } else if let asset = value as? CKAsset {
            if let fileURL = asset.fileURL,
               let data = NSData(contentsOf: fileURL) {
                try Task.checkCancellation()
                object.setValue(data, forKey: key)
            } else {
                throw malformed("a readable CloudKit asset")
            }
        } else if value != nil || property.isOptional == true {
            // If property is not a relationship or value is nil and property is optional.
            // If value is nil and property is non-optional, it is ignored. This is something that could happen
            // when extending an object model with a new non-optional property, when an old record is applied to the object.
            //            let ref = ThreadSafeReference(to: object)
            //            debugPrint("!! applyChange", type(of: object), key, value.debugDescription.prefix(100))
            if let value {
                let isExpectedScalar: Bool
                switch property.type {
                case .int:
                    isExpectedScalar = value is Int
                case .string:
                    isExpectedScalar = value is String
                case .bool:
                    isExpectedScalar = value is Bool
                case .float:
                    isExpectedScalar = value is Float
                case .double:
                    isExpectedScalar = value is Double
                case .data:
                    isExpectedScalar = value is Data
                case .date:
                    isExpectedScalar = value is Date
                default:
                    isExpectedScalar = false
                }
                guard isExpectedScalar else {
                    throw malformed("a scalar matching the Realm property type")
                }
            }
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
                    try persistPendingRelationships(
                        Array(chunk),
                        in: persistenceRealm
                    )
                }
            } catch {
                logger.error("Error during persistPendingRelationships: \(error)")
                throw error
            }
        }
    }

    @BigSyncBackgroundActor
    private func persistPendingRelationships(
        _ requests: [PendingRelationshipRequest],
        in persistenceRealm: Realm
    ) throws {
        for request in requests {
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
            guard let objectIdentifier = getObjectIdentifier(for: syncedEntity) else {
                throw RealmSwiftAdapterError.malformedRecordIdentifier(
                    recordName: syncedEntity.identifier,
                    entityType: syncedEntity.entityType
                )
            }
            guard let targetRealm = realmProvider.targetReaderRealmPerSchemaName[
                originObjectClass.className()
            ] else { continue }
            await targetRealm.asyncRefresh()
            // A cancelled synchronization task can resume after Realm refresh
            // while a newer run is preparing the same adapter. Stop before
            // touching persistence-backed relationship objects from the old
            // run; the pending rows remain durable for retry.
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            guard let originObject = targetRealm.object(
                ofType: originObjectClass,
                forPrimaryKey: objectIdentifier
            ) else { continue }

            let expectedModifiedAt =
                relationships.first?.expectedModifiedAt
            let expectedExplicitlyModifiedAt =
                relationships.first?.expectedExplicitlyModifiedAt
            let expectedRecordChangeTag =
                relationships.first?.sourceRecordChangeTag
            let currentRecordChangeTag = getRecord(for: syncedEntity)?
                .recordChangeTag
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
                let remoteVersionChanged = expectedRecordChangeTag != nil
                    && currentRecordChangeTag != expectedRecordChangeTag
                return hasPendingMutation
                    || syncedEntity.pendingGeneration != nil
                    || remoteVersionChanged
                    || (expectedRecordChangeTag != nil
                        && localMetadataChanged)
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
    func encodedRecord(_ record: CKRecord, onlySystemFields: Bool) throws -> Data {
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
            throw RealmSwiftAdapterError.systemFieldEncodingFailed(
                recordName: record.recordID.recordName
            )
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

    @BigSyncBackgroundActor
    private func hasValidServerRecordProof(_ entity: SyncedEntity) -> Bool {
        guard let record = getRecord(for: entity),
              record.recordID.recordName == entity.identifier,
              record.recordID.zoneID == zoneID,
              record.recordChangeTag != nil else {
            return false
        }
        return true
    }

    /// A tracking row that was previously reconciled with CloudKit remains
    /// server-backed evidence even if its optional cached system fields can no
    /// longer be decoded. Never apply this to local-only `.new` work or a
    /// pending local tombstone: their mutation generation is authoritative.
    @BigSyncBackgroundActor
    private func hadPriorServerMembership(_ entity: SyncedEntity) -> Bool {
        switch entity.entityState {
        case .synced, .changed, .deletedRemotely, .awaitingServerEvidence:
            return true
        case .new, .deletedLocally:
            return false
        }
    }
    
    func nextStateToSync(after state: SyncedEntityState) -> SyncedEntityState {
        return SyncedEntityState(rawValue: state.rawValue + 1)!
    }

    // MARK: - Change-feed tracking rebuild

    @BigSyncBackgroundActor
    public func hasChangeFeedEstablishedServerEvidence() async throws -> Bool {
        // Migration ownership must be established before normal setup can scan
        // target objects. Open only the tracking Realm here; `ensureSetup()` can
        // perform broad initial discovery and is intentionally forbidden at
        // this pre-reset boundary.
        let persistenceRealm = try await Realm(
            configuration: persistenceRealmConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        return persistenceRealm.objects(SyncedEntity.self).contains {
            hasValidServerRecordProof($0)
        }
    }

    /// The synchronizer calls these hooks inside its account- and attempt-
    /// fenced migration. Keeping the marker and provenance in the tracking
    /// Realm makes a killed process resume safely: target data and its durable
    /// mutation journal are never reset.
    @BigSyncBackgroundActor
    public func prepareChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode,
        preservingMutationsChangedAfter cutoff: Date?
    ) async throws {
        let persistenceRealm = try await Realm(
            configuration: persistenceRealmConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        if let state = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        ), state.accountScopeIdentifier == accountScopeIdentifier,
           state.epoch == epoch,
           changeFeedResetMode(for: state) == mode,
           (isCompletedChangeFeedReset(state)
                || (state.isActive && state.phase != "requested")) {
            // The capture-and-clear transaction already completed for this
            // exact migration. This also leaves an adapter which finished
            // before a peer adapter crashed in `finishing` alone: repeating
            // reset after its provenance was discarded would turn established
            // target objects into fresh local uploads.
            return
        }
        try await persistenceRealm.asyncWrite {
            let state = persistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            ) ?? RebuildProvenanceState()
            if state.accountScopeIdentifier != accountScopeIdentifier
                || state.epoch != epoch {
                // A stale interrupted migration belongs to a different
                // account/epoch and must not suppress this run's discovery.
                persistenceRealm.delete(
                    persistenceRealm.objects(RebuildProvenance.self)
                )
            }
            // A different account is never allowed to consume another
            // account's provenance. The following reset will overwrite it
            // from the currently configured tracking rows.
            state.accountScopeIdentifier = accountScopeIdentifier
            state.epoch = epoch
            state.mode = mode.rawValue
            state.isActive = true
            state.serverBootstrapStarted = false
            state.phase = "requested"
            persistenceRealm.add(state, update: .modified)
        }
        if mode == .backupRestore {
            // These rows are BigSync's historical outbox, not user model data.
            // A backup can contain a generation that the original installation
            // acknowledged later, so replaying it can overwrite newer CloudKit
            // truth or resurrect a remotely deleted record. Target objects are
            // deliberately retained and become uploadable again only after a
            // genuine post-restore user mutation creates a fresh generation.
            guard let cutoff else {
                throw CocoaError(.fileReadCorruptFile)
            }
            try await retireRestoredMutationJournal(
                preservingMutationsChangedAfter: cutoff
            )
        }
        try await resetSyncCaches()
    }

    @BigSyncBackgroundActor
    private func retireRestoredMutationJournal(
        preservingMutationsChangedAfter cutoff: Date
    ) async throws {
        var openedRealmIdentities = Set<String>()
        for configuration in targetRealmConfigurations {
            let identity = configuration.inMemoryIdentifier
                ?? configuration.fileURL?.standardizedFileURL.path
                ?? "default"
            guard openedRealmIdentities.insert(identity).inserted else {
                continue
            }
            let targetRealm = try await Realm(
                configuration: configuration,
                actor: BigSyncBackgroundActor.shared
            )
            let restoredMutations = Array(
                targetRealm.objects(BigSyncPendingMutation.self).filter {
                    !BigSyncPendingMutation.wasCreatedInCurrentProcess(
                        $0.generation
                    )
                    && $0.changedAt < cutoff
                }
            )
            guard !restoredMutations.isEmpty else { continue }
            try await targetRealm.asyncWrite {
                targetRealm.delete(restoredMutations)
            }
        }
    }

    @BigSyncBackgroundActor
    public func beginChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        try await ensureSetup()
        guard let persistenceRealm = realmProvider?.persistenceRealm else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        if let state = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        ), state.accountScopeIdentifier == accountScopeIdentifier,
           state.epoch == epoch,
           changeFeedResetMode(for: state) == mode,
           isCompletedChangeFeedReset(state) {
            return
        }
        try await persistenceRealm.asyncWrite {
            guard let state = persistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            ), state.isActive,
              state.accountScopeIdentifier == accountScopeIdentifier,
              state.epoch == epoch,
              changeFeedResetMode(for: state) == mode else {
                throw NSError(
                    domain: "BigSyncKit",
                    code: 1,
                    userInfo: [NSLocalizedDescriptionKey: "Missing or mismatched change-feed rebuild state"]
                )
            }
            state.serverBootstrapStarted = true
            state.phase = "serverBootstrap"
        }
    }

    @BigSyncBackgroundActor
    public func reconcileAfterChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        try await ensureSetup()
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        let state = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        )
        if let state,
           state.accountScopeIdentifier == accountScopeIdentifier,
           state.epoch == epoch,
           changeFeedResetMode(for: state) == mode,
           isCompletedChangeFeedReset(state) {
            return
        }
        guard state?.isActive == true,
              state?.serverBootstrapStarted == true,
              state?.accountScopeIdentifier == accountScopeIdentifier,
              state?.epoch == epoch,
              state.map(changeFeedResetMode(for:)) == mode else {
            throw NSError(
                domain: "BigSyncKit",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "Change-feed server bootstrap was not established"]
            )
        }

        struct Candidate: Sendable {
            let identifier: String
            let entityType: String
            let isDeleted: Bool
            let pendingGeneration: String?
        }
        var candidates = [Candidate]()
        let serverBackedIdentifiers = Set(
            persistenceRealm.objects(SyncedEntity.self).map(\.identifier)
        )
        for targetRealm in realmProvider.targetReaderRealmObjects {
            await targetRealm.asyncRefresh()
            struct TargetCandidate: Sendable {
                let identifier: String
                let entityType: String
                let objectIdentifier: String
                let isDeleted: Bool
            }
            var targetCandidates = [TargetCandidate]()
            for schema in targetRealm.schema.objectSchema
            where !excludedClassNames.contains(schema.className) {
                guard let objectType = realmObjectClass(name: schema.className),
                      let primaryKey = objectType.primaryKey()
                        ?? objectType.sharedSchema()?.primaryKeyProperty?.name else {
                    continue
                }
                var objects = targetRealm.objects(objectType)
                if let eligibilityType = objectType as? CloudKitInitialSyncEligibilityModel.Type {
                    objects = objects.filter(eligibilityType.initialCloudKitSyncEligibilityPredicate)
                }
                for object in objects {
                    let objectIdentifier = Self
                        .getTargetObjectStringIdentifier(
                            for: object,
                            usingPrimaryKey: primaryKey
                        )
                    targetCandidates.append(TargetCandidate(
                        identifier: schema.className + "." + objectIdentifier,
                        entityType: schema.className,
                        objectIdentifier: objectIdentifier,
                        isDeleted: (object as? SoftDeletable)?.isDeleted == true
                    ))
                }
            }

            if mode == .encryptedDataReset {
                // CloudKit's encrypted-data reset is the one recovery mode in
                // which an absent server record does not mean remote deletion.
                // Materialize durable target-Realm journal generations for
                // live local records before rebuilding tracking. Actual target
                // objects and their timestamps are deliberately untouched.
                let changedAt = Date()
                try await targetRealm.asyncWrite {
                    for candidate in targetCandidates
                    where !serverBackedIdentifiers.contains(
                        candidate.identifier
                    ) {
                        let existingMutation = targetRealm.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: candidate.identifier
                        )
                        if candidate.isDeleted {
                            // The reset server has no corresponding record to
                            // delete. Retain the local tombstone but retire its
                            // obsolete sync-only mutation generation.
                            if let existingMutation {
                                targetRealm.delete(existingMutation)
                            }
                            continue
                        }
                        guard existingMutation == nil else { continue }
                        targetRealm.add(BigSyncPendingMutation(
                            recordName: candidate.identifier,
                            entityType: candidate.entityType,
                            objectIdentifier: candidate.objectIdentifier,
                            changedAt: changedAt
                        ))
                    }
                }
            }

            candidates.append(contentsOf: targetCandidates.map { candidate in
                Candidate(
                    identifier: candidate.identifier,
                    entityType: candidate.entityType,
                    isDeleted: candidate.isDeleted,
                    pendingGeneration: targetRealm.object(
                        ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: candidate.identifier
                    )?.generation
                )
            })
        }

        try await persistenceRealm.asyncWrite {
            for candidate in candidates {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                // Remote imports/deletions and journal forwarding already
                // create tracking rows. They are authoritative and win.
                guard persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: candidate.identifier
                ) == nil else { continue }

                if mode == .encryptedDataReset {
                    guard !candidate.isDeleted,
                          let generation = candidate.pendingGeneration else {
                        continue
                    }
                    let record = SyncedEntity(
                        entityType: candidate.entityType,
                        identifier: candidate.identifier,
                        state: SyncedEntityState.new.rawValue
                    )
                    record.pendingGeneration = generation
                    persistenceRealm.add(record, update: .modified)
                    continue
                }

                let provenance = persistenceRealm.object(
                    ofType: RebuildProvenance.self,
                    forPrimaryKey: candidate.identifier
                ).flatMap { provenance in
                    provenance.accountScopeIdentifier == accountScopeIdentifier
                        && provenance.epoch == epoch ? provenance : nil
                }
                if mode != .backupRestore,
                   let generation = provenance?.priorPendingGeneration {
                    // Old pending tracking without a surviving journal is
                    // conservative local intent, never remote absence proof.
                    let record = SyncedEntity(
                        entityType: candidate.entityType,
                        identifier: candidate.identifier,
                        state: candidate.isDeleted
                            ? SyncedEntityState.deletedLocally.rawValue
                            : SyncedEntityState.new.rawValue
                    )
                    record.pendingGeneration = generation
                    persistenceRealm.add(record, update: .modified)
                } else if provenance?.hadValidServerRecord == true {
                    // Do not resurrect a stale target object. The transport
                    // must deliver a live record or deletion before this can
                    // transition; uploads and cleanup deliberately ignore it.
                    persistenceRealm.add(SyncedEntity(
                        entityType: candidate.entityType,
                        identifier: candidate.identifier,
                        state: SyncedEntityState.awaitingServerEvidence.rawValue
                    ), update: .modified)
                } else if mode == .initialImport, !candidate.isDeleted {
                    // No old server proof and no durable mutation means this
                    // is the bounded pre-journal/local-only discovery case.
                    let record = SyncedEntity(
                        entityType: candidate.entityType,
                        identifier: candidate.identifier,
                        state: SyncedEntityState.new.rawValue
                    )
                    record.pendingGeneration = UUID().uuidString
                    persistenceRealm.add(record, update: .modified)
                }
                // A tombstone without journal or server proof is intentionally
                // left untracked. It is neither uploaded nor hard-deleted.
            }
        }
        updateHasChanges(realm: persistenceRealm)
    }

    @BigSyncBackgroundActor
    public func finishChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        let persistenceRealm = if let configuredPersistenceRealm = realmProvider?.persistenceRealm {
            configuredPersistenceRealm
        } else {
            try await Realm(
                configuration: persistenceRealmConfiguration,
                actor: BigSyncBackgroundActor.shared
            )
        }
        if let state = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        ), state.accountScopeIdentifier == accountScopeIdentifier,
           state.epoch == epoch,
           changeFeedResetMode(for: state) == mode,
           isCompletedChangeFeedReset(state) {
            return
        }
        try await persistenceRealm.asyncWrite {
            guard let state = persistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            ), state.isActive,
              state.accountScopeIdentifier == accountScopeIdentifier,
              state.epoch == epoch,
              changeFeedResetMode(for: state) == mode else {
                throw NSError(domain: "BigSyncKit", code: 3)
            }
            // A fully traversed nil-token feed is absence evidence. Keep the
            // target object, but remove its neutral tracking row so it cannot
            // be uploaded or hard-deleted. A future explicit local mutation
            // recreates normal tracking through the durable journal.
            persistenceRealm.delete(
                persistenceRealm.objects(SyncedEntity.self).where {
                    $0.state == SyncedEntityState.awaitingServerEvidence.rawValue
                }
            )
            persistenceRealm.delete(persistenceRealm.objects(RebuildProvenance.self))
            state.isActive = false
            state.serverBootstrapStarted = false
            state.phase = "complete"
#if DEBUG
            try _testBeforeChangeFeedResetCompletionMarkerWrite?()
#endif
            // Completion and the current recovery signature are one durable
            // transition. Otherwise a process death here leaves `complete`
            // suppressing a later signature's broad recovery forever.
            for markerID in mutationJournalRecoveryMarkerIDs() {
                let marker = persistenceRealm.object(
                    ofType: SyncedEntityType.self,
                    forPrimaryKey: markerID
                ) ?? SyncedEntityType(entityType: markerID)
                marker.recoveryVersion = Self.mutationJournalRecoveryVersion
                persistenceRealm.add(marker, update: .modified)
            }
        }
    }

    @BigSyncBackgroundActor
    public func isChangeFeedServerBootstrapActive() async -> Bool {
        guard let persistenceRealm = realmProvider?.persistenceRealm else {
            return false
        }
        let state = persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        )
        return state?.isActive == true && state?.serverBootstrapStarted == true
    }

    /// `finishing` is coordinated by a synchronizer-wide KVS marker while
    /// completion is committed independently by every adapter. A peer can
    /// therefore still be active after this adapter has committed completion.
    /// Keep this predicate deliberately strict: an inactive state from another
    /// account or epoch must never suppress the current migration.
    private func isCompletedChangeFeedReset(
        _ state: RebuildProvenanceState
    ) -> Bool {
        !state.isActive && state.phase == "complete"
    }

    private func changeFeedResetMode(
        for state: RebuildProvenanceState
    ) -> ChangeFeedResetMode {
        ChangeFeedResetMode(rawValue: state.mode)
            ?? .serverReconciliation
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
            guard isOwnedEntityType(entityType) else { continue }
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
#if DEBUG
        // Read the RealmBackgroundActor-owned debug set before creating or
        // enumerating any live Realm query. Record materialization below is
        // deliberately non-suspending.
        let dummyRecordIdentifiers = await dummyRecordIdentifiers
#endif
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
        // Capture only primary keys before materializing any records. The
        // missing-target path in recordToUpload can mark a tracking row as
        // deleted; iterating a live Results while that write occurs can leave
        // Realm's fast enumerator pointing at invalidated storage.
        let candidateIdentifiers = results.map(\.identifier)
        var resultArray = [PreparedRecordUpload]()
        var includedEntityIDs = Set<String>()

        func appendUploadRecords(
            matching include: (String) -> Bool
        ) throws {
            for identifier in candidateIdentifiers {
                guard include(identifier) else { continue }
                if resultArray.count >= limit {
                    return
                }
                guard let candidate = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: identifier
                ) else {
                    continue
                }
                try appendUploadRecords(startingAt: candidate)
            }
        }

        func appendUploadRecords(startingAt syncedEntity: SyncedEntity) throws {
            if let restrictedEntityType, syncedEntity.entityType != restrictedEntityType {
                return
            }
            if resultArray.count >= limit {
                return
            }
            
            if !isOwnedEntityType(syncedEntity.entityType) {
                return
            }
            
            guard syncedEntity.state == state.rawValue,
                  !includedEntityIDs.contains(syncedEntity.identifier) else {
                return
            }
            let entityIdentifier = syncedEntity.identifier
            let generation = syncedEntity.pendingGeneration
#if DEBUG
            let isDummyRecord = dummyRecordIdentifiers.contains(entityIdentifier)
#else
            let isDummyRecord = false
#endif
            guard let record = try recordToUpload(
                syncedEntity: syncedEntity,
                isDummyRecord: isDummyRecord
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
        if dummyRecordIdentifiers.isEmpty {
            try appendUploadRecords { _ in true }
        } else {
            try appendUploadRecords { dummyRecordIdentifiers.contains($0) }
            if resultArray.count < limit {
                try appendUploadRecords { !dummyRecordIdentifiers.contains($0) }
            }
        }
#else
        try appendUploadRecords { _ in true }
#endif
        
        return resultArray
    }
    
    @BigSyncBackgroundActor
    func recordToUpload(
        syncedEntity: SyncedEntity,
        isDummyRecord: Bool
    ) throws -> CKRecord? {
        try Self.validateCloudKitRecordName(syncedEntity.identifier)
        let record = getRecord(for: syncedEntity) ?? CKRecord(recordType: syncedEntity.entityType, recordID: CKRecord.ID(recordName: syncedEntity.identifier, zoneID: zoneID))
        
        guard let objectClass = self.realmObjectClass(name: syncedEntity.entityType) else {
            return nil
        }
        guard let objectIdentifier = getObjectIdentifier(for: syncedEntity) else {
            throw RealmSwiftAdapterError.malformedRecordIdentifier(
                recordName: syncedEntity.identifier,
                entityType: syncedEntity.entityType
            )
        }
        let object = realmProvider?.targetReaderRealmPerSchemaName[objectClass.className()]?.object(ofType: objectClass, forPrimaryKey: objectIdentifier)
        let entityState = syncedEntity.state
        
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
        guard let object else {
            // Object does not exist, but tracking syncedEntity thinks it does.
            // We mark it as deleted so the iCloud record will get deleted too
            try persistenceRealm.write {
                // Resolve at the transaction boundary. A different operation
                // may already have changed or removed this tracking row.
                guard let current = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: syncedEntity.identifier
                ), current.state == entityState else { return }
                current.entityState = .deletedLocally
            }
            return nil
        }
        
        let skippedKeys: Set<String>
        if let skippable = object as? SyncSkippablePropertiesModel {
            skippedKeys = skippable.skipSyncingProperties() ?? []
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
                        // Match Realm's other collection encodings: an empty
                        // collection is represented by an absent CloudKit
                        // field. The comparison and audit paths intentionally
                        // also accept a legacy encoded empty map as equal.
                        record[property.name] = mapValue.isEmpty
                            ? nil
                            : try encodedCloudKitMap(mapValue) as CKRecordValue
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
        if isDummyRecord {
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
            var entitiesToDelete = [SyncedEntity]()
            for recordName in committedRecordNames {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                if let entity = persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: recordName
                ), entity.entityState == .deletedRemotely,
                   entity.pendingGeneration == nil {
                    entitiesToDelete.append(entity)
                }
            }

            let retiredIdentifiers = Set(entitiesToDelete.map(\.identifier))
            let obsoleteRelationships = Array(
                persistenceRealm.objects(PendingRelationship.self)
            ).filter { relationship in
                guard let ownerIdentifier = relationship.forSyncedEntity?
                    .identifier else {
                    return true
                }
                return retiredIdentifiers.contains(ownerIdentifier)
            }
            persistenceRealm.delete(obsoleteRelationships)
            persistenceRealm.delete(entitiesToDelete)
        }
    }
    
    // MARK: - QSModelAdapter
    
    @BigSyncBackgroundActor
    public func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {
        guard let realmProvider = realmProvider else { return }
        guard !records.isEmpty else { return }
        
        //        debugPrint("# To save from icloud:", records.map { $0.recordID.recordName })
        var recordsToSave: [(
            record: CKRecord,
            objectClass: RealmSwift.Object.Type,
            objectIdentifier: Any,
            syncedEntityID: String,
            syncedEntityState: SyncedEntityState,
            entityType: String,
            expectedMutationGeneration: String?,
            expectedModifiedAt: Date?,
            expectedExplicitlyModifiedAt: Date?
        )] = []
        var syncedEntitiesToCreate = [String: SyncedEntity]()
        try Task.checkCancellation()
        
        for chunk in records.chunks(ofCount: 200) {
            var readerRealmsForChunk = [String: Realm]()
            for recordType in Set(chunk.map(\.recordType)) {
                guard let realm = realmProvider
                    .targetReaderRealmPerSchemaName[recordType] else { continue }
                readerRealmsForChunk[
                    BigSyncMutationTrackingRegistry.identity(
                        for: realm.configuration
                    )
                ] = realm
            }
            for targetReaderRealm in readerRealmsForChunk.values {
                await targetReaderRealm.asyncRefresh()
            }
            for record in chunk {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                // Exclusions define ownership in both directions. Records for
                // AppSync-owned models must never be imported merely because a
                // stale or older client left them in this CloudKit zone.
                guard !excludedClassNames.contains(record.recordType) else {
                    continue
                }
                
                guard let persistenceRealm = realmProvider.persistenceRealm else { return }
                var syncedEntity: SyncedEntity? = Self.getSyncedEntity(objectIdentifier: record.recordID.recordName, realm: persistenceRealm)
                if syncedEntity == nil {
                    let newSyncedEntity = SyncedEntity(entityType: record.recordType, identifier: record.recordID.recordName, state: SyncedEntityState.synced.rawValue)
                    syncedEntitiesToCreate[newSyncedEntity.identifier] = newSyncedEntity
                    syncedEntity = newSyncedEntity
                }
                // Redelivery must finish persistence publication even when the
                // target object already matches. CloudKit change tags identify
                // the system-field version; synthetic records without one are
                // conservatively finalized again.
                let cachedChangeTag = (syncedEntity.flatMap {
                    getRecord(for: $0)
                })?.recordChangeTag
                let requiresSystemFieldPersistence =
                    syncedEntity?.encodedRecord == nil
                    || record.recordChangeTag == nil
                    || cachedChangeTag != record.recordChangeTag
                try Task.checkCancellation()
                
                if let syncedEntity {
                    if syncedEntity.entityState != .deletedLocally && syncedEntity.entityState != .deletedRemotely && syncedEntity.entityType != "CKShare" {
                        guard let objectClass = self.realmObjectClass(name: record.recordType) else {
                            continue
                        }
                        guard syncedEntity.entityType == record.recordType,
                              let objectIdentifier = getObjectIdentifier(
                                for: syncedEntity
                              ) else {
                            throw RealmSwiftAdapterError
                                .malformedRecordIdentifier(
                                    recordName: record.recordID.recordName,
                                    entityType: record.recordType
                                )
                        }
                        try Task.checkCancellation()
                        guard !cancelSync else { throw CancellationError() }
                        
                        let targetReaderRealm = realmProvider
                            .targetReaderRealmPerSchemaName[objectClass.className()]
                        let expectedMutationGeneration: String?
                        if targetReaderRealm?.schema.objectSchema.contains(where: {
                            $0.className == BigSyncPendingMutation.className()
                        }) == true {
                            expectedMutationGeneration = targetReaderRealm?.object(
                                ofType: BigSyncPendingMutation.self,
                                forPrimaryKey: syncedEntity.identifier
                            )?.generation
                        } else {
                            expectedMutationGeneration = nil
                        }
                        let existingObject = targetReaderRealm?.object(
                            ofType: objectClass,
                            forPrimaryKey: objectIdentifier
                        )
                        let expectedModifiedAt =
                            (existingObject as? ChangeMetadataRecordable)?
                                .modifiedAt
                        let expectedExplicitlyModifiedAt =
                            (existingObject as? ChangeMetadataRecordable)?
                                .explicitlyModifiedAt
                        let recordToSave = (
                            record,
                            objectClass,
                            objectIdentifier,
                            syncedEntity.identifier,
                            syncedEntity.entityState,
                            syncedEntity.entityType,
                            expectedMutationGeneration,
                            expectedModifiedAt,
                            expectedExplicitlyModifiedAt
                        )
                        guard !cancelSync else { throw CancellationError() }
                        try Task.checkCancellation()
                        
                        guard let object = existingObject else {
                            recordsToSave.append(recordToSave)
                            continue
                        }
                        guard !cancelSync else { throw CancellationError() }
                        try Task.checkCancellation()
                        
                        if forceSave || requiresSystemFieldPersistence || hasChanges(record: record, object: object) {
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
            
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            await Task.yield()
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
        }
        
        if !recordsToSave.isEmpty {
            for chunk in recordsToSave.chunks(ofCount: 100) {
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                
                let fetchedModifiedAts = chunk.compactMap { item -> (String, Date)? in
                    guard let modifiedAt = item.record["modifiedAt"] as? Date else {
                        return nil
                    }
                    return (item.syncedEntityID, modifiedAt)
                }
                for (syncedEntityID, modifiedAt) in fetchedModifiedAts {
                    recentlyFetchedRecordModifiedAts[syncedEntityID] = modifiedAt
                }
                
                let safeChunk = chunk.map {
                    RemoteRecordWriteCandidate(
                        record: $0.record,
                        objectType: $0.objectClass,
                        objectIdentifier: $0.objectIdentifier as any Sendable,
                        syncedEntityID: $0.syncedEntityID,
                        syncedEntityState: $0.syncedEntityState,
                        entityType: $0.entityType,
                        expectedMutationGeneration: $0.expectedMutationGeneration,
                        expectedModifiedAt: $0.expectedModifiedAt,
                        expectedExplicitlyModifiedAt: $0.expectedExplicitlyModifiedAt
                    )
                }
                do {
#if DEBUG
                    try await _testBeforeImportedRecordTargetWrite?()
#endif
                    let relationshipRequests =
                        try await { @RealmBackgroundActor () async throws
                            -> [PendingRelationshipRequest] in
                            var relationshipRequests =
                                [PendingRelationshipRequest]()
                            guard realmProvider.targetWriterRealms != nil else {
                                return []
                            }
                            var candidatesByRealm = [
                                String: (
                                    realm: Realm,
                                    candidates: [RemoteRecordWriteCandidate]
                                )
                            ]()
                            // `Realm` is a value wrapper, so cache by the
                            // schema key that selected this writer Realm.
                            // Each schema maps to one target writer Realm for
                            // the lifetime of this provider.
                            var realmIdentityBySchemaName = [String: String]()
                            for candidate in safeChunk {
                                guard let targetWriterRealm = realmProvider
                                    .targetWriterRealmPerSchemaName[
                                        candidate.objectType.className()
                                    ] else { continue }
                                let schemaName = candidate.objectType.className()
                                let realmIdentity: String
                                if let cachedIdentity = realmIdentityBySchemaName[
                                    schemaName
                                ] {
                                    realmIdentity = cachedIdentity
                                } else {
                                    let resolvedIdentity =
                                        BigSyncMutationTrackingRegistry.identity(
                                            for: targetWriterRealm.configuration
                                        )
                                    realmIdentityBySchemaName[schemaName] =
                                        resolvedIdentity
                                    realmIdentity = resolvedIdentity
                                }
                                if var group = candidatesByRealm[realmIdentity] {
                                    group.candidates.append(candidate)
                                    candidatesByRealm[realmIdentity] = group
                                } else {
                                    candidatesByRealm[realmIdentity] = (
                                        targetWriterRealm,
                                        [candidate]
                                    )
                                }
                            }

                            for group in candidatesByRealm.values {
                                let targetWriterRealm = group.realm
                                try Task.checkCancellation()
                                guard await !cancelSync else { throw CancellationError() }
                                await targetWriterRealm.asyncRefresh()
                                try Task.checkCancellation()

                                try await targetWriterRealm.asyncWrite { [weak self] in
                                    guard let self else { return }
                                    for candidate in group.candidates {
                                        try Task.checkCancellation()

                                        var object = targetWriterRealm.object(
                                            ofType: candidate.objectType,
                                            forPrimaryKey: candidate.objectIdentifier
                                        )
                                        let currentMutationGeneration: String?
                                        if targetWriterRealm.schema.objectSchema.contains(where: {
                                            $0.className == BigSyncPendingMutation.className()
                                        }) {
                                            currentMutationGeneration = targetWriterRealm.object(
                                                ofType: BigSyncPendingMutation.self,
                                                forPrimaryKey: candidate.syncedEntityID
                                            )?.generation
                                        } else {
                                            currentMutationGeneration = nil
                                        }
                                        let currentModifiedAt =
                                            (object as? ChangeMetadataRecordable)?
                                                .modifiedAt
                                        let currentExplicitlyModifiedAt =
                                            (object as? ChangeMetadataRecordable)?
                                                .explicitlyModifiedAt
                                        guard currentMutationGeneration
                                                == candidate.expectedMutationGeneration,
                                              currentModifiedAt == candidate.expectedModifiedAt,
                                              currentExplicitlyModifiedAt
                                                == candidate.expectedExplicitlyModifiedAt else {
                                            logger.info(
                                                "QSCloudKitSynchronizer >> Skipped downloaded record after a newer local mutation: \(candidate.syncedEntityID)"
                                            )
                                            continue
                                        }
                                        if currentMutationGeneration != nil {
                                            // The durable local journal is the
                                            // authority for user intent. Keep
                                            // the target values untouched while
                                            // still allowing the later tracking
                                            // write to retain the server's
                                            // current system fields/change tag;
                                            // the pending generation will then
                                            // upload those local values with an
                                            // if-server-unchanged fence.
                                            logger.info(
                                                "QSCloudKitSynchronizer >> Preserved journaled local values while importing server metadata: \(candidate.syncedEntityID)"
                                            )
                                            continue
                                        }
                                        try Task.checkCancellation()

                                        if object == nil {
                                            object = candidate.objectType.init()
                                            try Task.checkCancellation()

                                            if let object {
                                                object.setValue(
                                                    candidate.objectIdentifier,
                                                    forKey: (
                                                        candidate.objectType.primaryKey()
                                                        ?? candidate.objectType.sharedSchema()?.primaryKeyProperty?.name
                                                    )!
                                                )
                                                targetWriterRealm.add(object, update: .modified)
                                            }
                                        }

                                        try Task.checkCancellation()
                                        if let object {
                                            relationshipRequests.append(
                                                contentsOf: try self.applyChanges(
                                                    in: candidate.record,
                                                    to: object,
                                                    syncedEntityID: candidate.syncedEntityID,
                                                    syncedEntityState: candidate.syncedEntityState,
                                                    entityType: candidate.entityType
                                                )
                                            )
                                        }
                                    }
                                }
                            }
                            return relationshipRequests
                    }()

                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    guard let persistenceRealm = realmProvider.persistenceRealm else {
                        throw RealmSwiftAdapterError.setupUnavailable
                    }
#if DEBUG
                    try await _testBeforeImportedRecordPersistenceWrite?()
#endif
                    let newEntitiesForChunk = chunk.compactMap {
                        syncedEntitiesToCreate[$0.syncedEntityID]
                    }
                    try await persistenceRealm.asyncWrite { [weak self] in
                        guard let self else { return }
                        for entity in newEntitiesForChunk {
                            // A journal forwarder can create or update this
                            // record after selection but before this write.
                            // Never let an old synthetic `.synced` candidate
                            // erase its pending state or generation.
                            guard persistenceRealm.object(
                                ofType: SyncedEntity.self,
                                forPrimaryKey: entity.identifier
                            ) == nil else {
                                continue
                            }
                            persistenceRealm.add(entity)
                        }
                        for item in chunk {
                            try Task.checkCancellation()
                            guard !cancelSync else { throw CancellationError() }
                            guard let syncedEntity = persistenceRealm.object(
                                ofType: SyncedEntity.self,
                                forPrimaryKey: item.syncedEntityID
                            ) else { continue }
                            try save(record: item.record, for: syncedEntity)
                        }
                        try persistPendingRelationships(
                            relationshipRequests,
                            in: persistenceRealm
                        )
                    }
                    for entity in newEntitiesForChunk {
                        syncedEntitiesToCreate.removeValue(
                            forKey: entity.identifier
                        )
                    }
                    // Journal-enabled Realms never use the legacy timestamp
                    // scanner during normal operation. Once persistence
                    // publication succeeds, its temporary inbound suppression
                    // entry can be released immediately. Failed publication
                    // deliberately retains the marker until redelivery.
                    for item in chunk {
                        guard let targetRealm = realmProvider
                            .targetReaderRealmPerSchemaName[item.entityType],
                              targetRealm.schema.objectSchema.contains(where: {
                                  $0.className == BigSyncPendingMutation.className()
                              }) else { continue }
                        recentlyFetchedRecordModifiedAts.removeValue(
                            forKey: item.syncedEntityID
                        )
                    }
                }
                
                try Task.checkCancellation()
                guard !cancelSync else { throw CancellationError() }
                await Task.yield()
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

        if !syncedEntitiesToCreate.isEmpty {
            try await writeSyncedEntities(
                syncedEntities: Array(syncedEntitiesToCreate.values),
                realmProvider: realmProvider
            )
        }
    }
    
    @BigSyncBackgroundActor
    public func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              !recordIDs.isEmpty else { return }

        var deletions = [RemoteDeletionSnapshot]()
        var localWins = [
            (
                deletion: RemoteDeletionSnapshot,
                generation: String?,
                preservesLocalTombstone: Bool
            )
        ]()
        func targetHasSoftTombstone(_ deletion: RemoteDeletionSnapshot) -> Bool {
            guard let targetRealm = realmProvider
                .targetReaderRealmPerSchemaName[deletion.entityType],
                  let objectClass = realmObjectClass(name: deletion.entityType),
                  let objectIdentifier = getObjectIdentifier(
                    stringObjectId: deletion.objectIdentifier,
                    entityType: deletion.entityType
                  ),
                  let object = targetRealm.object(
                    ofType: objectClass,
                    forPrimaryKey: objectIdentifier
                  ) as? SoftDeletable else {
                return false
            }
            return object.isDeleted
        }
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
            let preservesLocalTombstone =
                syncedEntity?.entityState == .deletedLocally
                || targetHasSoftTombstone(deletion)
            if pendingMutation != nil
                || syncedEntity?.entityState == .new
                || syncedEntity?.entityState == .changed {
                localWins.append(
                    (deletion, pendingMutation?.generation, preservesLocalTombstone)
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
                        localWins.append((
                            deletion,
                            mutation.generation,
                            targetHasSoftTombstone(deletion)
                        ))
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
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            if let mutation = targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: deletion.recordName
            ) {
                lateLocalRecordNames.insert(deletion.recordName)
                localWins.append((
                    deletion,
                    mutation.generation,
                    targetHasSoftTombstone(deletion)
                ))
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
                if localWin.preservesLocalTombstone {
                    // A remote deletion acknowledges neither the local soft
                    // tombstone nor its journal generation. Keep it in the
                    // deletion lane so CloudKit receives a delete (where an
                    // unknown-item response is an idempotent acknowledgement),
                    // never a save of `isDeleted == true`.
                    syncedEntity.state = SyncedEntityState.deletedLocally.rawValue
                    syncedEntity.pendingGeneration =
                        localWin.generation ?? syncedEntity.pendingGeneration
                            ?? UUID().uuidString
                } else {
                    syncedEntity.state = SyncedEntityState.new.rawValue
                    syncedEntity.encodedRecord = nil
                    if syncedEntity.pendingGeneration == nil {
                        syncedEntity.pendingGeneration =
                            localWin.generation ?? UUID().uuidString
                    }
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
    public func preparedRecordsToUpload(
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

    /// Prepares upload records together with an opaque snapshot of their local
    /// mutation generations. Pass this batch back when acknowledging successes.
    @BigSyncBackgroundActor
    func prepareUploadBatch(limit: Int) async throws -> RealmSwiftPreparedUploadBatch {
        let prepared = try await preparedRecordsToUpload(
            limit: limit,
            restrictedToEntityType: nil
        )
        return RealmSwiftPreparedUploadBatch(
            records: prepared.map(\.record),
            matchingGenerations: prepared.reduce(into: [:]) { generations, item in
                guard let generation = item.generation else { return }
                generations[item.record.recordID.recordName] = generation
            },
            issuerID: acknowledgementIssuerID
        )
    }

    /// Acknowledges the successful subset of a previously prepared upload.
    @BigSyncBackgroundActor
    func acknowledgeUploadedRecords(
        _ savedRecords: [CKRecord],
        from batch: RealmSwiftPreparedUploadBatch
    ) async throws {
        guard batch.issuerID == acknowledgementIssuerID else {
            throw RealmSwiftAdapterAcknowledgementError.batchBelongsToAnotherAdapter
        }
        let preparedRecordIDs = Set(batch.records.map(\.recordID))
        guard savedRecords.allSatisfy({ preparedRecordIDs.contains($0.recordID) }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        try await didUpload(
            savedRecords: savedRecords,
            matchingGenerations: batch.matchingGenerations
        )
    }
    
    @BigSyncBackgroundActor
    public func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()
        
        for chunk in savedRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            
            //            await persistenceRealm.asyncRefresh()
            try await persistenceRealm.asyncWrite {
                for record in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    
                    guard let syncedEntity = persistenceRealm.object(
                        ofType: SyncedEntity.self,
                        forPrimaryKey: record.recordID.recordName
                    ), let uploadedGeneration = matchingGenerations[record.recordID.recordName],
                       syncedEntity.pendingGeneration == uploadedGeneration else { continue }
                    try Task.checkCancellation()
                    try save(record: record, for: syncedEntity)
                    syncedEntity.state = SyncedEntityState.synced.rawValue
                    syncedEntity.pendingGeneration = nil
                    acknowledgedGenerations[record.recordID.recordName] = uploadedGeneration
                    acknowledgedEntityTypes[record.recordID.recordName] =
                        syncedEntity.entityType
                }
            }
            await Task.yield()
        }

        if !acknowledgedGenerations.isEmpty {
            if realmProvider.targetReaderRealms != nil {
                var generationsByRealm = [
                    String: (realm: Realm, generations: [String: String])
                ]()
                for (recordName, generation) in acknowledgedGenerations {
                    guard let entityType = acknowledgedEntityTypes[recordName],
                          let targetReaderRealm = realmProvider
                            .targetReaderRealmPerSchemaName[entityType],
                          targetReaderRealm.schema.objectSchema.contains(where: {
                              $0.className == BigSyncPendingMutation.className()
                          }) else { continue }
                    let realmIdentity = BigSyncMutationTrackingRegistry.identity(
                        for: targetReaderRealm.configuration
                    )
                    if var group = generationsByRealm[realmIdentity] {
                        group.generations[recordName] = generation
                        generationsByRealm[realmIdentity] = group
                    } else {
                        generationsByRealm[realmIdentity] = (
                            targetReaderRealm,
                            [recordName: generation]
                        )
                    }
                }
                for group in generationsByRealm.values {
                    let targetReaderRealm = group.realm
                    let generations = group.generations
                    try await targetReaderRealm.asyncWrite {
                        for (recordName, generation) in generations {
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
                        for: generations.keys,
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
    public func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] {
        var deletions = [PreparedRecordDeletion]()
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return [] }
        let targetEntityType = restrictedToEntityType ?? prioritizedEntityTypeWithPendingUploadOrDeletion()
        if let targetEntityType, !isOwnedEntityType(targetEntityType) {
            return []
        }
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
            guard isOwnedEntityType(syncedEntity.entityType) else { continue }
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

    /// Prepares deletions together with an opaque snapshot of their local
    /// mutation generations. Pass this batch back when acknowledging successes.
    @BigSyncBackgroundActor
    func prepareDeletionBatch(limit: Int) async throws -> RealmSwiftPreparedDeletionBatch {
        let prepared = try await preparedRecordDeletions(
            limit: limit,
            restrictedToEntityType: nil
        )
        return RealmSwiftPreparedDeletionBatch(
            recordIDs: prepared.map(\.recordID),
            matchingGenerations: prepared.reduce(into: [:]) { generations, item in
                guard let generation = item.generation else { return }
                generations[item.recordID.recordName] = generation
            },
            issuerID: acknowledgementIssuerID
        )
    }

    /// Acknowledges the successful subset of a previously prepared deletion.
    @BigSyncBackgroundActor
    func acknowledgeDeletedRecordIDs(
        _ recordIDs: [CKRecord.ID],
        from batch: RealmSwiftPreparedDeletionBatch
    ) async throws {
        guard batch.issuerID == acknowledgementIssuerID else {
            throw RealmSwiftAdapterAcknowledgementError.batchBelongsToAnotherAdapter
        }
        let preparedRecordIDs = Set(batch.recordIDs)
        guard recordIDs.allSatisfy({ preparedRecordIDs.contains($0) }) else {
            throw RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared
        }
        try await didDelete(
            recordIDs: recordIDs,
            matchingGenerations: batch.matchingGenerations
        )
    }
    
    @BigSyncBackgroundActor
    public func didDelete(
        recordIDs deletedRecordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm else { return }
        var acknowledgedGenerations = [String: String]()
        var acknowledgedEntityTypes = [String: String]()

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
                    acknowledgedEntityTypes[recordID.recordName] =
                        syncedEntity.entityType
                }
            }
        }

        if !acknowledgedGenerations.isEmpty,
           realmProvider.targetReaderRealms != nil {
            var generationsByRealm = [
                String: (realm: Realm, generations: [String: String])
            ]()
            for (recordName, generation) in acknowledgedGenerations {
                guard let entityType = acknowledgedEntityTypes[recordName],
                      let targetReaderRealm = realmProvider
                        .targetReaderRealmPerSchemaName[entityType],
                      targetReaderRealm.schema.objectSchema.contains(where: {
                          $0.className == BigSyncPendingMutation.className()
                      }) else { continue }
                let realmIdentity = BigSyncMutationTrackingRegistry.identity(
                    for: targetReaderRealm.configuration
                )
                if var group = generationsByRealm[realmIdentity] {
                    group.generations[recordName] = generation
                    generationsByRealm[realmIdentity] = group
                } else {
                    generationsByRealm[realmIdentity] = (
                        targetReaderRealm,
                        [recordName: generation]
                    )
                }
            }
            for group in generationsByRealm.values {
                let targetReaderRealm = group.realm
                let generations = group.generations
                try await targetReaderRealm.asyncWrite {
                    for (recordName, generation) in generations {
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
                    for: generations.keys,
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
    public func didFinishImport() async throws {
        try await ensureSetup()
        guard let realmProvider, let persistenceRealm = realmProvider.persistenceRealm else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        
        //        logger.info("QSCloudKitSynchronizer >> Clearing temporary CKAsset files")
        try await updateCreatedAndModified()
        // didFinishImport is reached only after the operation that consumed
        // prepared CKAssets is terminal. Realm data, not these files, owns any
        // still-pending generation, so future retries can safely rematerialize
        // their current values without retaining superseded offline versions.
        persistentAssetManager.clearAssetFiles()
        updateHasChanges(realm: persistenceRealm)
    }

    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        // Do not suspend between refreshing these Realm views and deciding
        // whether a receipt can be issued. A target write committed before this
        // cut remains visible even when its debounced observer has not fired.
        try Task.checkCancellation()
        guard !cancelSync else { throw CancellationError() }
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              let targetReaderRealms = realmProvider.targetReaderRealms else {
            throw RealmSwiftAdapterError.setupUnavailable
        }

        for targetReaderRealm in targetReaderRealms where
            targetReaderRealm.schema.objectSchema.contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
            targetReaderRealm.refresh()
            if targetReaderRealm.objects(BigSyncPendingMutation.self).first != nil {
                return true
            }
        }

        persistenceRealm.refresh()
        updateHasChanges(realm: persistenceRealm)
        return hasChanges
    }
    
    /// Requeues only records whose pending generation is still the generation
    /// sent to CloudKit. A newer local mutation remains pending untouched.
    @BigSyncBackgroundActor
    public func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        guard let persistenceRealm = realmProvider?.persistenceRealm else { return }

        for chunk in recordIDs.chunks(ofCount: 1000) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWrite {
                for recordID in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    let recordName = recordID.recordName
                    guard let preparedGeneration = matchingPreparedGenerations[recordName],
                          let syncedEntity = persistenceRealm.object(
                            ofType: SyncedEntity.self,
                            forPrimaryKey: recordName
                          ),
                          syncedEntity.pendingGeneration == preparedGeneration else {
                        continue
                    }
                    syncedEntity.entityState = .new
                    syncedEntity.encodedRecord = nil
                    // Keep the prepared generation. The matching journal row
                    // remains the authority for retrying this exact mutation.
                }
            }
        }
        updateHasChanges(realm: persistenceRealm)
    }

    /// Updates CloudKit's opaque system fields for a deletion conflict without
    /// touching the target object or changing which journal generation owns
    /// the tombstone. A newer local mutation makes the prepared response stale
    /// and is deliberately ignored.
    @BigSyncBackgroundActor
    public func rebasePendingDeletionMetadata(
        using serverRecords: [CKRecord],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
              let persistenceRealm = realmProvider.persistenceRealm,
              !serverRecords.isEmpty else { return }

        for chunk in serverRecords.chunks(ofCount: 500) {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            try await persistenceRealm.asyncWrite {
                for record in chunk {
                    try Task.checkCancellation()
                    guard !cancelSync,
                          record.recordID.zoneID == zoneID,
                          let preparedGeneration =
                            matchingPreparedGenerations[
                                record.recordID.recordName
                            ],
                          let syncedEntity = persistenceRealm.object(
                            ofType: SyncedEntity.self,
                            forPrimaryKey: record.recordID.recordName
                          ),
                          syncedEntity.entityState == .deletedLocally,
                          syncedEntity.entityType == record.recordType,
                          syncedEntity.pendingGeneration == preparedGeneration,
                          let targetRealm = realmProvider
                            .targetReaderRealmPerSchemaName[
                                syncedEntity.entityType
                            ],
                          let targetMutation = targetRealm.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: syncedEntity.identifier
                          ),
                          targetMutation.generation == preparedGeneration,
                          pendingMutationTargetsDeletedObject(
                            targetMutation,
                            in: targetRealm
                          ) else { continue }
                    try save(record: record, for: syncedEntity)
                    // `save` changes only the opaque system-field archive.
                    // Restate these invariants to make future edits fail safe.
                    syncedEntity.state = SyncedEntityState.deletedLocally.rawValue
                    syncedEntity.pendingGeneration = preparedGeneration
                }
            }
            await Task.yield()
        }
    }
    
    public var recordZoneID: CKRecordZone.ID {
        return zoneID
    }
    
    public var serverChangeToken: RecordZoneChangeCursor? {
        get async {
            return await { @BigSyncBackgroundActor in
                guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
                let serverToken = persistenceRealm.objects(ServerToken.self).first
                return serverToken?.token.map(RecordZoneChangeCursor.init(serializedData:))
            }()
        }
    }
    
    @BigSyncBackgroundActor
    public func saveToken(_ token: RecordZoneChangeCursor?) async throws {
        //        debugPrint("# saveToken", token, recordZoneID)
        // Token migration and zone-token publication require the same completed
        // readiness boundary as imports and uploads. Returning while setup is
        // incomplete would let callers discard their only durable token copy.
        try await ensureSetup()
        guard let persistenceRealm = realmProvider?.persistenceRealm else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        //        await persistenceRealm.asyncRefresh()
        try Task.checkCancellation()
        guard !cancelSync else { throw CancellationError() }
        try await persistenceRealm.asyncWrite {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            // Resolve the managed token at the transaction boundary rather than
            // carrying a Realm object across the async-write suspension.
            let serverToken: ServerToken
            if let existingToken = persistenceRealm.objects(ServerToken.self).first {
                serverToken = existingToken
            } else {
                serverToken = ServerToken()
                persistenceRealm.add(serverToken)
            }
            
            serverToken.token = token?.serializedData
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
