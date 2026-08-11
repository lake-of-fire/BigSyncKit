//
//  CloudKitSynchronizer.swift
//  Pods
//
//  Created by Manuel Entrena on 05/04/2019.
//

import Foundation
import CloudKit
import CryptoKit
import Logging
import RealmSwiftGaps
import SwiftUtilities

/// Bridges callback-only CloudKit APIs without pinning the caller to a checked
/// continuation after its task is cancelled. The callback may still arrive,
/// but `AsyncThrowingStream` safely discards it after termination.
internal func awaitCancellableCloudKitCallback<Value>(
    timeoutNanoseconds: UInt64? = nil,
    _ start: (@escaping (Result<Value, Error>) -> Void) -> Void
) async throws -> Value {
    let (stream, continuation) = AsyncThrowingStream<Value, Error>.makeStream()
    start { result in
        switch result {
        case .success(let value):
            continuation.yield(value)
            continuation.finish()
        case .failure(let error):
            continuation.finish(throwing: error)
        }
    }
    let timeoutTask = timeoutNanoseconds.map { timeoutNanoseconds in
        Task.detached {
            do {
                try await Task.sleep(nanoseconds: timeoutNanoseconds)
            } catch {
                return
            }
            continuation.finish(
                throwing: CKError(
                    .networkFailure,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "CloudKit callback exceeded its deadline"
                    ]
                )
            )
        }
    }
    defer {
        timeoutTask?.cancel()
        continuation.finish()
    }
    var iterator = stream.makeAsyncIterator()
    guard let value = try await iterator.next() else {
        throw CancellationError()
    }
    return value
}

/// Durable, account-scoped progress for the one-time transition to the
/// page-oriented CloudKit change feed.  This deliberately lives beside the
/// synchronizer's other local metadata rather than in a target Realm: a
/// migration can be resumed after a process death before an adapter has opened
/// its tracking Realm.
private struct ChangeFeedMigrationState {
    static let version = 1
    // Epochs are durable adapter identities, not merely counters inside one KVS
    // key. Reserve a disjoint range for each future migration version so a v2
    // reset can never be mistaken for an already-completed v1 reset.
    static let epochRangeSize = 1_000_000_000
    static var initialEpoch: Int {
        (version * epochRangeSize) + 1
    }

    enum Phase: String {
        case requested
        case prepared
        case serverBootstrap
        case reconciled
        case finishing
        case completed
    }

    let key: String
    let accountScopeIdentifier: String
    let epoch: Int
    var mode: ChangeFeedResetMode
    var phase: Phase
}

/// A production migration must never recreate a zone that CloudKit reports as
/// deleted, purged, or reset.  The target Realm and durable journal remain
/// intact so a future, explicitly supported recovery can classify the state.
public enum ChangeFeedMigrationError: LocalizedError {
    case establishedZoneUnavailable(
        CKRecordZone.ID,
        CloudKitZoneDeletionKind
    )

    public var deletionKind: CloudKitZoneDeletionKind {
        switch self {
        case .establishedZoneUnavailable(_, let kind):
            return kind
        }
    }

    public var errorDescription: String? {
        switch self {
        case .establishedZoneUnavailable(let zoneID, let kind):
            return "The established CloudKit zone \(zoneID.zoneName) is unavailable (\(kind.rawValue)); local data was preserved and upload is blocked"
        }
    }
}

public struct CloudKitTerminalZoneState: Sendable, Equatable {
    public let zoneName: String
    public let ownerName: String
    public let deletionKind: CloudKitZoneDeletionKind
    public let observedAt: Date

    public init(
        zoneName: String,
        ownerName: String,
        deletionKind: CloudKitZoneDeletionKind,
        observedAt: Date
    ) {
        self.zoneName = zoneName
        self.ownerName = ownerName
        self.deletionKind = deletionKind
        self.observedAt = observedAt
    }
}

// For Swift
public extension Notification.Name {
    /// Sent when the synchronizer is going to start a sync with CloudKit.
    static let SynchronizerWillSynchronize = Notification.Name("QSCloudKitSynchronizerWillSynchronizeNotification")
    /// Sent when the synchronizer is going to start the fetch stage, where it downloads any new changes from CloudKit.
    static let SynchronizerWillFetchChanges = Notification.Name("QSCloudKitSynchronizerWillFetchChangesNotification")
    /// Sent when the synchronizer is going to start the upload stage, where it sends changes to CloudKit.
    static let SynchronizerWillUploadChanges = Notification.Name("QSCloudKitSynchronizerWillUploadChangesNotification")
    //    /// Sent when the synchronizer finishes syncing.
    static let SynchronizerDidSynchronize = Notification.Name("QSCloudKitSynchronizerDidSynchronizeNotification")
    /// Sent when the synchronizer encounters an error while syncing.
    static let SynchronizerDidFailToSynchronize = Notification.Name("QSCloudKitSynchronizerDidFailToSynchronizeNotification")
    /// Reports remaining changes
    static let SynchronizerChangesRemainingToUpload = Notification.Name("QSCloudKitSynchronizerChangesRemainingToUploadNotification")
}

// For Obj-C
@objc public extension NSNotification {
    /// Sent when the synchronizer is going to start a sync with CloudKit.
    @MainActor
    static let CloudKitSynchronizerWillSynchronizeNotification: NSString = "QSCloudKitSynchronizerWillSynchronizeNotification"
    /// Sent when the synchronizer is going to start the fetch stage, where it downloads any new changes from CloudKit.
    @MainActor
    static let CloudKitSynchronizerWillFetchChangesNotification: NSString = "QSCloudKitSynchronizerWillFetchChangesNotification"
    /// Sent when the synchronizer is going to start the upload stage, where it sends changes to CloudKit.
    @MainActor
    static let CloudKitSynchronizerWillUploadChangesNotification: NSString = "QSCloudKitSynchronizerWillUploadChangesNotification"
    /// Sent when the synchronizer finishes syncing.
    @MainActor
    static let CloudKitSynchronizerDidSynchronizeNotification: NSString = "QSCloudKitSynchronizerDidSynchronizeNotification"
    /// Sent when the synchronizer encounters an error while syncing.
    @MainActor
    static let CloudKitSynchronizerDidFailToSynchronizeNotification: NSString = "QSCloudKitSynchronizerDidFailToSynchronizeNotification"
}

/// Handles destructive CloudKit zone notifications for the synchronizer's
/// explicitly registered model adapters.
public protocol AdapterProvider {
    /// The `CloudKitSynchronizer` informs the provider that a record zone was deleted so it can clean up any associated data.
    /// - Parameters:
    ///   - synchronizer: `QSCloudKitSynchronizer` that found the deleted record zone.
    ///   - zoneID: `CKRecordZoneID` of the record zone that was deleted.
    @BigSyncBackgroundActor
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID) async throws
}

//@objc public protocol CloudKitSynchronizerDelegate: AnyObject {
public protocol CloudKitSynchronizerDelegate: AnyObject {
    func synchronizerWillFetchChanges(_ synchronizer: CloudKitSynchronizer, in recordZone: CKRecordZone.ID)
    func synchronizerWillUploadChanges(_ synchronizer: CloudKitSynchronizer, to recordZone: CKRecordZone.ID)
    func synchronizerDidSync(_ synchronizer: CloudKitSynchronizer)
    func synchronizerDidfailToSync(_ synchronizer: CloudKitSynchronizer, error: Error)
    func synchronizer(_ synchronizer: CloudKitSynchronizer, zoneIDWasDeleted zoneID: CKRecordZone.ID)
}

internal struct ChangeRequest: Sendable {
    let downloadedRecord: CKRecord?
    let deletedRecordID: CKRecord.ID?
    let adapter: ModelAdapter
    let runID: UUID?

    init(
        downloadedRecord: CKRecord?,
        deletedRecordID: CKRecord.ID?,
        adapter: ModelAdapter,
        runID: UUID? = nil
    ) {
        self.downloadedRecord = downloadedRecord
        self.deletedRecordID = deletedRecordID
        self.adapter = adapter
        self.runID = runID
    }
}

public enum OneOffRecordZoneResetError: LocalizedError {
    case cloudKitAccountChanged
    case cloudKitAccountUnavailable
    case disposableClientMustUseExactlyOneRecordZone
    case disposableZoneDeletionNotAllowed

    public var errorDescription: String? {
        switch self {
        case .cloudKitAccountChanged:
            return "The iCloud account changed during the CloudKit reset"
        case .cloudKitAccountUnavailable:
            return "The current iCloud account could not be identified"
        case .disposableClientMustUseExactlyOneRecordZone:
            return "A disposable synchronization client must use exactly one record zone"
        case .disposableZoneDeletionNotAllowed:
            return "This synchronizer is not permitted to delete CloudKit zones"
        }
    }
}

@BigSyncBackgroundActor
internal class ChangeRequestProcessor {
    static let defaultFetchedChangeBatchSize = 300
    
//    internal var logger: Logging.Logger?

    internal var cancelSync = false
    
    init() {}
    
    private var changeRequests = [ChangeRequest]()
    private var localErrors: [Error] = []
    private var activeRunID = UUID()
    private var processingTask: Task<Void, Error>?
    private var processingTaskID: UUID?
    internal var fetchedChangeBatchSize = ChangeRequestProcessor.defaultFetchedChangeBatchSize
    
    internal func addFetchedChangeRequest(_ request: ChangeRequest) {
        guard !cancelSync,
              request.runID == nil || request.runID == activeRunID else { return }
        changeRequests.append(request)
    }

    @discardableResult
    func beginRun() async -> UUID {
        reset()
        await waitForProcessingToStop()
        activeRunID = UUID()
        cancelSync = false
        return activeRunID
    }

    private func entityType(for request: ChangeRequest) -> String? {
        if let recordType = request.downloadedRecord?.recordType {
            return recordType
        }
        guard let recordName = request.deletedRecordID?.recordName else { return nil }
        return recordName.split(separator: ".", maxSplits: 1).first.map(String.init)
    }

    private func dequeueBatch(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?,
        runID: UUID
    ) -> [ChangeRequest] {
        guard activeRunID == runID, !cancelSync else { return [] }
        var batch = [ChangeRequest]()
        var remaining = [ChangeRequest]()
        batch.reserveCapacity(min(fetchedChangeBatchSize, changeRequests.count))
        remaining.reserveCapacity(changeRequests.count)

        for request in changeRequests {
            guard request.runID == nil || request.runID == runID else {
                continue
            }
            let isMatchingAdapter = request.adapter.recordZoneID == adapter.recordZoneID
            let isMatchingRestriction = restrictedEntityType == nil || entityType(for: request) == restrictedEntityType
            if batch.count < fetchedChangeBatchSize &&
                isMatchingAdapter &&
                isMatchingRestriction {
                batch.append(request)
            } else {
                remaining.append(request)
            }
        }
        changeRequests = remaining

        return batch
    }
    
    private func runProcessFetchedChangeRequests(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?
    ) async throws {
        let runID = activeRunID
        let taskID = UUID()
        let task = Task { @BigSyncBackgroundActor [weak self] in
            guard let self else { throw CancellationError() }
            try await processFetchedChangeRequests(
                for: adapter,
                restrictedToEntityType: restrictedEntityType,
                runID: runID
            )
        }
        processingTaskID = taskID
        processingTask = task
        defer {
            if processingTaskID == taskID {
                processingTask = nil
                processingTaskID = nil
            }
        }
        try await task.value
    }
    
    private func processFetchedChangeRequests(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?,
        runID: UUID
    ) async throws {
        try Task.checkCancellation()
        
        while true {
            try Task.checkCancellation()
            guard !cancelSync, activeRunID == runID else {
                throw CancellationError()
            }
            let batch = dequeueBatch(
                for: adapter,
                restrictedToEntityType: restrictedEntityType,
                runID: runID
            )
            guard !batch.isEmpty else { return }
            
            do {
                let downloadedRecords = try batch.compactMap {
                    try Task.checkCancellation()
                    return $0.downloadedRecord
                }
                
                if !downloadedRecords.isEmpty {
                    try await batch.first?.adapter.saveChanges(in: downloadedRecords, forceSave: false)
                    try Task.checkCancellation()
                    guard !cancelSync, activeRunID == runID else {
                        throw CancellationError()
                    }
                }
                
                let deletedRecordIDs = try batch.compactMap {
                    try Task.checkCancellation()
                    return $0.deletedRecordID
                }
                if !deletedRecordIDs.isEmpty {
                    try await batch.first?.adapter.deleteRecords(with: deletedRecordIDs)
                    try Task.checkCancellation()
                    guard !cancelSync, activeRunID == runID else {
                        throw CancellationError()
                    }
                }
            } catch is CancellationError {
                // The page token is committed only after the complete fetched page
                // publishes successfully. Discard this batch and let CloudKit
                // redeliver it; requeueing can leak an old run into a newer one.
                throw CancellationError()
            } catch {
                localErrors.append(error)
                // As above, retaining a failed value batch is unnecessary because
                // its page token has not advanced.
                return
            }
            
            try await Task.sleep(nanoseconds: 500_000)
        }
    }
    
    func getErrors() -> [Error] {
        return localErrors
    }
    
    func clearErrors() {
        localErrors.removeAll()
    }

    func reset() {
        cancelSync = true
        processingTask?.cancel()
        changeRequests.removeAll(keepingCapacity: false)
        localErrors.removeAll(keepingCapacity: false)
    }

    func waitForProcessingToStop() async {
        let task = processingTask
        task?.cancel()
        _ = await task?.result
    }
    
    @BigSyncBackgroundActor
    func hasPendingChangeRequests(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String? = nil
    ) -> Bool {
        changeRequests.contains { request in
            request.adapter.recordZoneID == adapter.recordZoneID &&
            (restrictedEntityType == nil || entityType(for: request) == restrictedEntityType)
        }
    }

    @BigSyncBackgroundActor
    func finishProcessing(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String? = nil
    ) async throws {
        try Task.checkCancellation()
        try await runProcessFetchedChangeRequests(
            for: adapter,
            restrictedToEntityType: restrictedEntityType
        )
    }

    @BigSyncBackgroundActor
    func finishProcessing() async throws {
        while let adapter = changeRequests.first?.adapter {
            try await finishProcessing(for: adapter)
        }
    }
}

let cloudKitSynchronizerDeviceUUIDKey = "QSCloudKitDeviceUUIDKey"
let cloudKitSynchronizerModelCompatibilityVersionKey = "QSCloudKitModelCompatibilityVersionKey"
public let cloudKitSynchronizerErrorDomain = "CloudKitSynchronizerErrorDomain"
public let cloudKitSynchronizerErrorKey = "CloudKitSynchronizerErrorKey"

///  These keys will be added to CKRecords uploaded to CloudKit and are used by SyncKit internally.
public let cloudKitSynchronizerMetadataKeys: [String] = [
    cloudKitSynchronizerDeviceUUIDKey,
    cloudKitSynchronizerModelCompatibilityVersionKey,
]

/**
 A `CloudKitSynchronizer` object takes care of making all the required calls to CloudKit to keep your model synchronized, using the provided
 `ModelAdapter` to interact with it.
 
 `CloudKitSynchronizer` will post notifications at different steps of the synchronization process.
 */
@BigSyncBackgroundActor
public class CloudKitSynchronizer: NSObject {
    public typealias AccountIdentifierProvider = @Sendable () async throws -> String
    /// Optional, actor-isolated lifecycle observation for diagnostics. Production
    /// callers receive a no-op unless they explicitly provide a handler.
    public typealias ProgressHandler = @BigSyncBackgroundActor @Sendable (String) -> Void
    internal struct RunContext: Sendable, Equatable {
        let attemptID: UUID
        let runID: UUID
        let accountIdentifier: String
        let accountScopeIdentifier: String
    }

    public struct SynchronizationReceipt: Sendable, Equatable {
        public let accountScopeIdentifier: String
        public let runID: UUID
        internal let accountIdentifier: String
        internal let issuerID: UUID
        internal let authorizationID: UUID

        internal init(
            context: RunContext,
            issuerID: UUID,
            authorizationID: UUID
        ) {
            accountScopeIdentifier = context.accountScopeIdentifier
            runID = context.runID
            accountIdentifier = context.accountIdentifier
            self.issuerID = issuerID
            self.authorizationID = authorizationID
        }
    }

    public struct SynchronizationResult: Sendable, Equatable {
        public let didImportChanges: Bool
        public let receipt: SynchronizationReceipt?

        public init(
            didImportChanges: Bool,
            receipt: SynchronizationReceipt? = nil
        ) {
            self.didImportChanges = didImportChanges
            self.receipt = receipt
        }
    }

    /// SyncError
    public enum SyncError: Int, Error {
        /**
         *  A synchronizer with a higer `compatibilityVersion` value uploaded changes to CloudKit, so those changes won't be imported here.
         *  This error can be detected to prompt the user to update the app to a newer version.
         */
        case higherModelVersionFound = 1
        /**
         *  A record fot the provided object was not found, so the object cannot be shared on CloudKit.
         */
        //        case recordNotFound = 2
        /**
         *  Synchronization was manually cancelled.
         */
        case cancelled = 3
        /// Synchronization cannot start until an iCloud account is available.
        case notAuthenticated = 4
    }
    
    /// `CloudKitSynchronizer` can be configured to only download changes, never uploading local changes to CloudKit.
    public enum SynchronizeMode: Int {
        /// Download and upload all changes
        case sync
        /// Only download changes
        case downloadOnly
    }
    
    /**
     More than one `CloudKitSynchronizer` may be created in an app.
     The identifier is used to persist some state, so it should always be the same for a synchronizer –if you change your app to use a different identifier state might be lost.
     */
    public let identifier: String
    
    /// iCloud container identifier.
    public let containerIdentifier: String?
    
    /// Adapter wrapping a `CKDatabase`. The synchronizer will run CloudKit operations on the given database.
//    @BigSyncBackgroundActor
    public let database: CloudKitDatabaseAdapter
    
    /// Provides the model adapter to the synchronizer.
    public let adapterProvider: AdapterProvider
    
    /// Required by the synchronizer to persist some state. `UserDefaults` can be used via `UserDefaultsAdapter`.
    public let keyValueStore: KeyValueStore
    internal let accountIdentifierProvider: AccountIdentifierProvider
    private let progressHandler: ProgressHandler
    private var allowsDisposableZoneDeletion: Bool
    /// Page-oriented history transport. A non-default database adapter must
    /// inject this explicitly; callback fetch operations are intentionally not
    /// a fallback.
    @available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
    internal let changeFeed: any CloudKitChangeFeed
    /// Async lifecycle transport. Unlike the callback database façade, this
    /// provides exact-ID lookups and per-subscription mutation results.
    @available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
    internal let subscriptionStore: any CloudKitSubscriptionStore
    internal let zoneStore: any CloudKitZoneStore
    internal let recordStore: any CloudKitRecordStore
    internal let synchronizationReceiptIssuerID = UUID()
    private var cloudKitAccountIdentifierKey: String {
        "\(identifier).BigSyncKitCloudKitAccountIdentifier"
    }
    private var transientRetryStateKey: String {
        "\(identifier).BigSyncKitTransientRetryState.v1"
    }
    private var accountValidationRequired = true
    private var accountChangeObserver: NSObjectProtocol?
    /// Set during construction when the local defaults marker survived a backup
    /// but the excluded-from-backup sentinel did not. The synchronization
    /// bootstrap consumes this before any fetch can advance a token.
    internal private(set) var backupRestoreDetected = false
    private var backupDetectionError: Error?
    
    /// Indicates whether the instance is currently synchronizing data.
    @BigSyncBackgroundActor
    public internal(set) var syncing: Bool = false
    
    /// Indicates whether it failed to synchronize due to the user being unauthenticated. May not auto-recover.
    @BigSyncBackgroundActor
    public internal(set) var cancelledDueToUnauthentication = false

    ///  Number of records that are sent in an upload operation.
    @BigSyncBackgroundActor
    public var batchSize: Int = CloudKitSynchronizer.defaultInitialBatchSize
    
    /**
     *  When set, if the synchronizer finds records uploaded by a different device using a higher compatibility version,
     *   it will end synchronization with a `higherModelVersionFound` error.
     */
    public var compatibilityVersion: Int = 0
    
    /// Whether the synchronizer will only download data or also upload any local changes.
    public var syncMode: SynchronizeMode = .sync
    
//    @BigSyncBackgroundActor
    public var delegate: CloudKitSynchronizerDelegate?
    
    //    internal let dispatchQueue = DispatchQueue(label: "QSCloudKitSynchronizer")
//    @BigSyncBackgroundActor
    internal let operationQueue: OperationQueue = {
        let queue = OperationQueue()
        //        queue.maxConcurrentOperationCount = 1
        return queue
    }()
    internal var modelAdapterDictionary = [CKRecordZone.ID: ModelAdapter]()
    internal var serverChangeToken: DatabaseChangeCursor?
    internal var activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
    @BigSyncBackgroundActor
    internal var cancelSync = false
    @BigSyncBackgroundActor
    internal var retrySleepUntil: Date?
    /// In-memory mirror of the current account-scoped durable retry count.
    /// An explicit cancellation stops the active sleep but deliberately keeps
    /// a server-directed floor durable; account replacement rejects and clears
    /// a state belonging to another account.
    @BigSyncBackgroundActor
    internal var consecutiveTransientCloudKitFailures = 0
    
    internal var uploadRetries = 0
    internal var didNotifyUpload = Set<CKRecordZone.ID>()
    internal var synchronizationTask: Task<Void, Never>?
    internal var synchronizationRequestedWhileRunning = false
    internal var synchronizationDrainIsActive = false
    internal var synchronizationDrainDidImportChanges = false
    private var synchronizationWaiters = [
        UUID: CheckedContinuation<SynchronizationResult, Error>
    ]()
    private var activeRunCallbackCount = 0
#if DEBUG
    internal var _testActiveRunCallbackCount: Int {
        activeRunCallbackCount
    }
#endif
    private var runCallbackWaiters = [CheckedContinuation<Void, Never>]()
    private var attemptCallbackContinuations = [
        UUID: [
            UUID: AsyncThrowingStream<Void, Error>.Continuation
        ]
    ]()

    internal var lastDatabaseChangesEmptyAt: Date?
    internal var lastZoneChangesEmptyAt: Date?
    internal let changeRequestProcessor = ChangeRequestProcessor()
    internal var synchronizationAttemptID = UUID()
    internal var synchronizationRunID = UUID()
    internal var activeRunContext: RunContext?
    internal var activeReceiptAuthorizationID: UUID?
    private var reservedReceiptAuthorizationID: UUID?
    /// Non-nil only for the attempt currently performing the durable
    /// change-feed migration.  Adapter state remains the source of truth for
    /// per-zone provenance; this value merely fences the orchestration.
    private var activeChangeFeedMigration: ChangeFeedMigrationState?
 
    internal let logger: Logging.Logger
    
    /// Default number of records to send in an upload operation.
    public static let defaultInitialBatchSize = 300
    public static let maxBatchSize = 400 // Apple's suggestion is 400
    
    /// Initializes a newly allocated synchronizer.
    /// - Parameters:
    ///   - identifier: Identifier for the `QSCloudKitSynchronizer`.
    ///   - containerIdentifier: Identifier of the iCloud container to be used. The application must have the right entitlements to be able to access this container.
    ///   - database: Private or Shared CloudKit Database
    ///   - adapterProvider: `CloudKitSynchronizerAdapterProvider`
    ///   - keyValueStore: Object conforming to KeyValueStore (`UserDefaultsAdapter`, for example)
    /// - Returns: Initialized synchronizer or `nil` if no iCloud container can be found with the provided identifier.
    public init(
        identifier: String,
        containerIdentifier: String? = nil,
        database: CloudKitDatabaseAdapter,
        adapterProvider: AdapterProvider,
        keyValueStore: KeyValueStore = UserDefaultsAdapter(userDefaults: UserDefaults.standard),
        compatibilityVersion: Int = 0,
        accountIdentifierProvider: AccountIdentifierProvider? = nil,
        progressHandler: ProgressHandler? = nil,
        changeFeed: (any CloudKitChangeFeed)? = nil,
        subscriptionStore: (any CloudKitSubscriptionStore)? = nil,
        zoneStore: (any CloudKitZoneStore)? = nil,
        recordStore: (any CloudKitRecordStore)? = nil,
        logger: Logging.Logger
    ) {
        self.identifier = identifier
        self.containerIdentifier = containerIdentifier
        self.adapterProvider = adapterProvider
        self.database = database
        self.keyValueStore = keyValueStore
        self.compatibilityVersion = compatibilityVersion
        if let accountIdentifierProvider {
            self.accountIdentifierProvider = accountIdentifierProvider
        } else {
            self.accountIdentifierProvider = {
                guard let containerIdentifier else {
                    throw OneOffRecordZoneResetError.cloudKitAccountUnavailable
                }
                let container = CKContainer(identifier: containerIdentifier)
                return try await awaitCancellableCloudKitCallback(
                    timeoutNanoseconds: 60_000_000_000
                ) { completion in
                    container.fetchUserRecordID { recordID, error in
                        if let error {
                            completion(.failure(error))
                        } else if let recordID {
                            completion(.success(recordID.recordName))
                        } else {
                            completion(.failure(
                                OneOffRecordZoneResetError.cloudKitAccountUnavailable
                            ))
                        }
                    }
                }
            }
        }
        self.progressHandler = progressHandler ?? { _ in }
        self.allowsDisposableZoneDeletion = false
        guard #available(iOS 15.0, macOS 12.0, watchOS 8.0, *),
              let resolvedChangeFeed = changeFeed ?? (database as? any CloudKitChangeFeed) else {
            preconditionFailure("CloudKitSynchronizer requires an async CloudKitChangeFeed")
        }
        self.changeFeed = resolvedChangeFeed
        guard let resolvedSubscriptionStore = subscriptionStore
            ?? (database as? any CloudKitSubscriptionStore) else {
            preconditionFailure(
                "CloudKitSynchronizer requires an async CloudKitSubscriptionStore"
            )
        }
        self.subscriptionStore = resolvedSubscriptionStore
        guard let resolvedZoneStore = zoneStore
            ?? (database as? any CloudKitZoneStore) else {
            preconditionFailure(
                "CloudKitSynchronizer requires an async CloudKitZoneStore"
            )
        }
        self.zoneStore = resolvedZoneStore
        guard let resolvedRecordStore = recordStore
            ?? (database as? any CloudKitRecordStore) else {
            preconditionFailure(
                "CloudKitSynchronizer requires an async CloudKitRecordStore"
            )
        }
        self.recordStore = resolvedRecordStore
        self.logger = logger
        super.init()

        accountChangeObserver = NotificationCenter.default.addObserver(
            forName: .CKAccountChanged,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                accountValidationRequired = true
                cancelSynchronization()
            }
        }
        
        do {
            let result = try BackupDetection.run(store: keyValueStore)
            backupRestoreDetected = BackupDetection.restoreResetIsRequired(
                store: keyValueStore
            )
            if result == .restoredFromBackup || backupRestoreDetected {
                clearDeviceIdentifier()
            }
        } catch {
            backupDetectionError = error
            logger.error("QSCloudKitSynchronizer >> Backup detection failed: \(error)")
        }
        
//        Task {
//            ChangeRequestProcessor.shared.logger = logger
//        }
    }

#if DEBUG
    /// Test-only capability for an isolated synchronizer with a disposable
    /// CloudKit zone. Production construction always leaves deletion disabled.
    internal func _enableDisposableZoneDeletionForTesting() {
        allowsDisposableZoneDeletion = true
    }
#endif

    @BigSyncBackgroundActor
    internal func reportProgress(_ checkpoint: String) {
        progressHandler(checkpoint)
    }

    internal var isChangeFeedMigrationActive: Bool {
        activeChangeFeedMigration != nil
    }

    internal var isEncryptedDataResetRecoveryActive: Bool {
        activeChangeFeedMigration?.mode == .encryptedDataReset
    }

    internal func markConfiguredZoneEstablished(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) {
        keyValueStore.set(
            boolValue: true,
            forKey: zoneLifecycleKey(zoneID, accountScopeIdentifier: accountScopeIdentifier, suffix: "established")
        )
    }

    internal func markConfiguredZoneTerminal(
        _ zoneID: CKRecordZone.ID,
        kind: CloudKitZoneDeletionKind,
        accountScopeIdentifier: String,
        observedAt: Date = Date()
    ) {
        // A database-history deletion event is itself durable proof that the
        // zone existed. All deletion kinds are terminal for production sync:
        // preserve the concrete reason and never silently recreate the zone
        // from possibly stale local objects.
        let establishedKey = zoneLifecycleKey(zoneID, accountScopeIdentifier: accountScopeIdentifier, suffix: "established")
        keyValueStore.set(boolValue: true, forKey: establishedKey)
        keyValueStore.set(
            value: kind.rawValue,
            forKey: zoneLifecycleKey(zoneID, accountScopeIdentifier: accountScopeIdentifier, suffix: "terminal")
        )
        keyValueStore.set(
            value: observedAt,
            forKey: zoneLifecycleKey(
                zoneID,
                accountScopeIdentifier: accountScopeIdentifier,
                suffix: "terminalObservedAt"
            )
        )
    }

    internal func configuredZoneIsEstablished(_ zoneID: CKRecordZone.ID) -> Bool {
        guard let context = activeRunContext else { return false }
        return keyValueStore.bool(
            forKey: zoneLifecycleKey(
                zoneID,
                accountScopeIdentifier: context.accountScopeIdentifier,
                suffix: "established"
            )
        )
    }

    internal func configuredZoneIsTerminal(_ zoneID: CKRecordZone.ID) -> Bool {
        configuredZoneTerminalState(zoneID) != nil
    }

    internal func configuredZoneTerminalState(
        _ zoneID: CKRecordZone.ID
    ) -> CloudKitTerminalZoneState? {
        guard let context = activeRunContext,
              let rawKind = keyValueStore.object(
                forKey: zoneLifecycleKey(
                    zoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier,
                    suffix: "terminal"
                )
              ) as? String else {
            return nil
        }
        let deletionKind = CloudKitZoneDeletionKind(rawValue: rawKind)
            ?? .unknown
        let observedAt = keyValueStore.object(
            forKey: zoneLifecycleKey(
                zoneID,
                accountScopeIdentifier: context.accountScopeIdentifier,
                suffix: "terminalObservedAt"
            )
        ) as? Date ?? .distantPast
        return CloudKitTerminalZoneState(
            zoneName: zoneID.zoneName,
            ownerName: zoneID.ownerName,
            deletionKind: deletionKind,
            observedAt: observedAt
        )
    }

    internal func clearConfiguredZoneTerminal(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) {
        for suffix in ["terminal", "terminalObservedAt"] {
            keyValueStore.removeObject(
                forKey: zoneLifecycleKey(
                    zoneID,
                    accountScopeIdentifier: accountScopeIdentifier,
                    suffix: suffix
                )
            )
        }
    }

    private func zoneLifecycleKey(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String,
        suffix: String
    ) -> String {
        "\(identifier).BigSyncKit.ZoneLifecycle.v1.\(accountScopeIdentifier).\(zoneID.ownerName).\(zoneID.zoneName).\(suffix)"
    }

    deinit {
        if let accountChangeObserver {
            NotificationCenter.default.removeObserver(accountChangeObserver)
        }
    }
    
    fileprivate var _deviceIdentifier: String!
    @BigSyncBackgroundActor
    var deviceIdentifier: String {
        if _deviceIdentifier == nil {
            _deviceIdentifier = deviceUUID
            if _deviceIdentifier == nil {
                _deviceIdentifier = UUID().uuidString
                deviceUUID = _deviceIdentifier
            }
        }
        return _deviceIdentifier
    }
    
    internal func clearDeviceIdentifier() {
        deviceUUID = nil
        _deviceIdentifier = nil
    }
    
    /// Resets synchronization metadata after crossing the cancellation barrier.
    ///
    /// This remains internal because a metadata-only reset outside the fenced
    /// change-feed migration can rediscover established local objects as new and
    /// re-upload them. Internal recovery flows call it only after establishing
    /// exclusive attempt ownership.
    @BigSyncBackgroundActor
    internal func resetSyncCaches(
        cancelSynchronization _: Bool,
        includingAdapters: Bool = true
    ) async throws {
        await cancelSynchronizationAndWait()
        try await resetSyncCachesOwnedByCurrentFlow(
            includingAdapters: includingAdapters
        )
    }

    @BigSyncBackgroundActor
    private func resetSyncCachesOwnedByCurrentFlow(
        includingAdapters: Bool
    ) async throws {
        clearDeviceIdentifier()
        resetDatabaseToken()
        resetActiveTokens()
        lastDatabaseChangesEmptyAt = nil

        if includingAdapters {
            for adapter in modelAdapters {
                try await adapter.resetSyncCaches()
                try await adapter.unsetCancellation()
            }
        }
    }

    @BigSyncBackgroundActor
    private func resetRestoredBackupCachesIfNeeded(
        context: RunContext
    ) async throws {
        if backupDetectionError != nil {
            let result = try BackupDetection.run(store: keyValueStore)
            backupDetectionError = nil
            backupRestoreDetected = BackupDetection.restoreResetIsRequired(
                store: keyValueStore
            )
            if result == .restoredFromBackup || backupRestoreDetected {
                clearDeviceIdentifier()
            }
        }
        guard backupRestoreDetected
                || BackupDetection.restoreResetIsRequired(store: keyValueStore) else {
            return
        }

        try await revalidateRunContext(context)
        try await resetSyncCachesOwnedByCurrentFlow(includingAdapters: true)
        try await revalidateRunContext(context)
        BackupDetection.markRestoreResetCompleted(store: keyValueStore)
        backupRestoreDetected = false
    }

    // MARK: - Public

    @BigSyncBackgroundActor
    private func activeTerminalZoneError() -> ChangeFeedMigrationError? {
        guard let context = activeRunContext,
              !hasPendingEncryptedDataResetRecovery(context: context),
              let terminalState = modelAdapters.compactMap({ adapter in
                  configuredZoneTerminalState(adapter.recordZoneID)
              }).first else {
            return nil
        }
        return .establishedZoneUnavailable(
            CKRecordZone.ID(
                zoneName: terminalState.zoneName,
                ownerName: terminalState.ownerName
            ),
            terminalState.deletionKind
        )
    }
    
    /// Synchronize data with CloudKit.
    /// - Parameter onFailure: Block that receives an error if the synchronization stopped due to a failure. Could be a `SyncError`, `CKError`, or any other error found during synchronization.
    @BigSyncBackgroundActor
    @objc public func beginSynchronization() { //onFailure: ((Error) -> ())?) {
        guard !cancelledDueToUnauthentication else { return }
        if activeTerminalZoneError() != nil {
            // A purge or unexplained zone loss intentionally leaves local
            // journal generations pending. Ignore their observer wakeups so
            // they cannot spin terminal synchronization attempts.
            return
        }
        guard !syncing else {
            synchronizationRequestedWhileRunning = true
            return
        }

        logger.info("QSCloudKitSynchronizer >> Begin synchronization...")
        if !synchronizationDrainIsActive {
            synchronizationDrainIsActive = true
            synchronizationDrainDidImportChanges = false
        }
        cancelSync = false
        syncing = true
        retrySleepUntil = nil
        let attemptID = UUID()
        synchronizationAttemptID = attemptID
        activeReceiptAuthorizationID = nil
        reservedReceiptAuthorizationID = nil

        synchronizationTask?.cancel()
        synchronizationTask = Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            do {
                await waitForRunCallbacksToFinish()
                try checkSynchronizationAttempt(attemptID)
                let accountIdentifier = try await validateSynchronizationAccount()
                reportProgress("account-identity-validated")
                let runID = await changeRequestProcessor.beginRun()
                synchronizationRunID = runID
                let context = RunContext(
                    attemptID: attemptID,
                    runID: runID,
                    accountIdentifier: accountIdentifier,
                    accountScopeIdentifier: Self.accountScopeIdentifier(
                        for: accountIdentifier
                    )
                )
                activeRunContext = context
                if let terminalState = modelAdapters.compactMap({
                    self.configuredZoneTerminalState($0.recordZoneID)
                }).first,
                   !hasPendingEncryptedDataResetRecovery(context: context) {
                    throw ChangeFeedMigrationError
                        .establishedZoneUnavailable(
                            CKRecordZone.ID(
                                zoneName: terminalState.zoneName,
                                ownerName: terminalState.ownerName
                            ),
                            terminalState.deletionKind
                        )
                }
                try await waitForPersistedTransientRetryIfNeeded(
                    context: context
                )
                try recordSyncHealth(.syncing, context: context)
                try await resetRestoredBackupCachesIfNeeded(context: context)
                // Subscription identifiers are account-scoped sync metadata.
                // Account validation clears them after an iCloud account
                // change, so ensure the current account has a subscription as
                // part of every attempt. The stored-ID fast path performs no
                // CloudKit request during ordinary synchronization.
                try await subscribeForChangesInDatabase()
                reportProgress("subscription-completed")
                try await revalidateRunContext(context)
                for adapter in modelAdapters {
                    await adapter.waitForCancellation()
                    try await adapter.unsetCancellation()
                    try checkRunContext(context)
                }
                reportProgress("adapters-ready")
                try await beginChangeFeedMigrationIfNeeded(context: context)
                try await revalidateRunContext(context)
                reportProgress("change-feed-migration-ready")
                try Task.checkCancellation()
                await performSynchronization()
            } catch {
                guard synchronizationAttemptID == attemptID else { return }
                await failSynchronization(error: error)
            }
        }
    }

    /// Starts synchronization, coalesces with any in-flight request, and returns
    /// only after the full fetch/import/upload drain has finished.
    @BigSyncBackgroundActor
    public func synchronize() async throws -> SynchronizationResult {
        guard !cancelledDueToUnauthentication else {
            throw SyncError.notAuthenticated
        }
        if let terminalError = activeTerminalZoneError() {
            throw terminalError
        }

        let requestID = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                synchronizationWaiters[requestID] = continuation
                beginSynchronization()
            }
        } onCancel: {
            Task { @BigSyncBackgroundActor [weak self] in
                self?.cancelSynchronizationRequest(requestID)
            }
        }
    }

    private func cancelSynchronizationRequest(_ requestID: UUID) {
        synchronizationWaiters.removeValue(forKey: requestID)?
            .resume(throwing: CancellationError())
    }

    /// Checks that an asynchronous callback still belongs to the active sync.
    ///
    /// CloudKit callbacks and adapter calls can suspend the global actor. A
    /// cancellation or account change may start a newer attempt while they are
    /// suspended, so checking only when a callback first enters is insufficient.
    internal func checkSynchronizationAttempt(_ attemptID: UUID) throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID, !cancelSync else {
            throw CancellationError()
        }
    }

    internal func checkRunContext(_ context: RunContext) throws {
        try Task.checkCancellation()
        guard activeRunContext == context,
              synchronizationAttemptID == context.attemptID,
              synchronizationRunID == context.runID,
              !cancelSync else {
            throw CancellationError()
        }
    }

    internal func revalidateRunContext(_ context: RunContext) async throws {
        try checkRunContext(context)
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkRunContext(context)
        guard currentAccountIdentifier == context.accountIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }

    internal func revalidateActiveRunContext(
        for attemptID: UUID
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        guard let context = activeRunContext else { return }
        try await revalidateRunContext(context)
    }

    /// Registers an asynchronous CloudKit callback as work owned by its
    /// synchronization attempt. A newer run does not clear adapter cancellation
    /// until every callback that entered the old attempt has left.
    @discardableResult
    internal func beginRunCallback(for attemptID: UUID) -> Bool {
        guard synchronizationAttemptID == attemptID else { return false }
        activeRunCallbackCount += 1
        return true
    }

    internal func endRunCallback() {
        precondition(activeRunCallbackCount > 0)
        activeRunCallbackCount -= 1
        guard activeRunCallbackCount == 0 else { return }
        let waiters = runCallbackWaiters
        runCallbackWaiters.removeAll(keepingCapacity: false)
        waiters.forEach { $0.resume() }
    }

    internal func waitForRunCallbacksToFinish() async {
        guard activeRunCallbackCount > 0 else { return }
        await withCheckedContinuation { continuation in
            runCallbackWaiters.append(continuation)
        }
    }

    /// Bridges callback work that belongs to a synchronization attempt.
    /// Cancelling that attempt finishes the waiter even when an underlying
    /// callback API fails to invoke its completion handler.
    internal func awaitAttemptCallback(
        for attemptID: UUID,
        _ start: (@escaping @Sendable (Result<Void, Error>) -> Void) -> Void
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        let callbackID = UUID()
        let (stream, continuation) =
            AsyncThrowingStream<Void, Error>.makeStream()
        attemptCallbackContinuations[attemptID, default: [:]][callbackID] =
            continuation
        start { [weak self] result in
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else {
                    continuation.finish(throwing: CancellationError())
                    return
                }
                guard let registered =
                    attemptCallbackContinuations[attemptID]?
                        .removeValue(forKey: callbackID) else { return }
                if attemptCallbackContinuations[attemptID]?.isEmpty == true {
                    attemptCallbackContinuations.removeValue(forKey: attemptID)
                }
                switch result {
                case .success:
                    registered.yield(())
                    registered.finish()
                case .failure(let error):
                    registered.finish(throwing: error)
                }
            }
        }
        defer {
            attemptCallbackContinuations[attemptID]?
                .removeValue(forKey: callbackID)
            if attemptCallbackContinuations[attemptID]?.isEmpty == true {
                attemptCallbackContinuations.removeValue(forKey: attemptID)
            }
            continuation.finish()
        }
        var iterator = stream.makeAsyncIterator()
        guard try await iterator.next() != nil else {
            throw CancellationError()
        }
        try checkSynchronizationAttempt(attemptID)
    }

    private func cancelAttemptCallbacks(for attemptID: UUID) {
        guard let continuations = attemptCallbackContinuations
            .removeValue(forKey: attemptID) else { return }
        for continuation in continuations.values {
            continuation.finish(throwing: CancellationError())
        }
    }

    private func checkAccountValidationAttempt(_ attemptID: UUID) throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID else {
            throw CancellationError()
        }
    }

    internal func finishSynchronizationDrain(
        with result: Result<SynchronizationResult, Error>
    ) {
        synchronizationDrainIsActive = false
        synchronizationRequestedWhileRunning = false
        let waiters = synchronizationWaiters.values
        synchronizationWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters {
            waiter.resume(with: result)
        }
    }

    @BigSyncBackgroundActor
    private func validateSynchronizationAccount() async throws -> String {
        let validationAttemptID = synchronizationAttemptID
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
        var confirmedAccountIdentifier = currentAccountIdentifier
        if let previousAccountIdentifier =
            keyValueStore.object(forKey: cloudKitAccountIdentifierKey) as? String,
           previousAccountIdentifier != currentAccountIdentifier {
            logger.info(
                "QSCloudKitSynchronizer >> CloudKit account changed; rebuilding local sync metadata for the new account"
            )
            changeRequestProcessor.reset()
            resetDatabaseToken()
            resetActiveTokens()
            clearAllStoredSubscriptionIDs()
            clearPersistedTransientRetryState()
            for adapter in modelAdapters {
                adapter.cancelSynchronization()
                await adapter.waitForCancellation()
                try await adapter.resetSyncCaches()
                try await adapter.unsetCancellation()
                try checkAccountValidationAttempt(validationAttemptID)
            }
            // Confirm the provider still reports the account whose metadata was
            // prepared. This catches account changes that occur while an
            // adapter cache reset is suspended. Normal validation has no
            // suspension after the first lookup and avoids a second request.
            confirmedAccountIdentifier = try await accountIdentifierProvider()
            try checkAccountValidationAttempt(validationAttemptID)
        }
        guard confirmedAccountIdentifier == currentAccountIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        keyValueStore.set(
            value: confirmedAccountIdentifier,
            forKey: cloudKitAccountIdentifierKey
        )
        accountValidationRequired = false
        cancelSync = false
        cancelledDueToUnauthentication = false
        return confirmedAccountIdentifier
    }

    private func persistedTransientRetryState() -> (
        accountScopeIdentifier: String,
        notBefore: Date,
        consecutiveFailures: Int
    )? {
        guard let value = keyValueStore.object(
            forKey: transientRetryStateKey
        ) as? [String: Any],
        let accountScopeIdentifier = value["accountScopeIdentifier"] as? String,
        let notBefore = value["notBefore"] as? Date,
        let consecutiveFailures = value["consecutiveFailures"] as? Int,
        consecutiveFailures > 0 else {
            return nil
        }
        return (accountScopeIdentifier, notBefore, consecutiveFailures)
    }

    internal func persistTransientRetryState(
        context: RunContext,
        notBefore: Date,
        consecutiveFailures: Int
    ) {
        keyValueStore.set(
            value: [
                "accountScopeIdentifier": context.accountScopeIdentifier,
                "notBefore": notBefore,
                "consecutiveFailures": max(1, consecutiveFailures),
            ],
            forKey: transientRetryStateKey
        )
    }

    internal func clearPersistedTransientRetryState() {
        keyValueStore.removeObject(forKey: transientRetryStateKey)
    }

    internal func waitForPersistedTransientRetryIfNeeded(
        context: RunContext
    ) async throws {
        guard let state = persistedTransientRetryState() else { return }
        guard state.accountScopeIdentifier == context.accountScopeIdentifier else {
            clearPersistedTransientRetryState()
            return
        }
        consecutiveTransientCloudKitFailures = state.consecutiveFailures
        let delay = state.notBefore.timeIntervalSinceNow
        guard delay > 0 else {
            clearPersistedTransientRetryState()
            return
        }
        retrySleepUntil = state.notBefore
        do {
            try await Task.sleep(
                nanoseconds: UInt64(delay * 1_000_000_000)
            )
            try await revalidateRunContext(context)
            retrySleepUntil = nil
            clearPersistedTransientRetryState()
        } catch {
            retrySleepUntil = nil
            throw error
        }
    }

#if DEBUG
    internal func _test_persistedTransientRetryNotBefore(
        accountScopeIdentifier: String
    ) -> Date? {
        guard let state = persistedTransientRetryState(),
              state.accountScopeIdentifier == accountScopeIdentifier else {
            return nil
        }
        return state.notBefore
    }
#endif

    /// Stable, non-reversible identifier for account-scoped safety markers.
    @BigSyncBackgroundActor
    public func cloudKitAccountScopeIdentifier() async throws -> String {
        let accountIdentifier = try await accountIdentifierProvider()
        return Self.accountScopeIdentifier(for: accountIdentifier)
    }

    internal static func accountScopeIdentifier(
        for accountIdentifier: String
    ) -> String {
        SHA256.hash(data: Data(accountIdentifier.utf8))
            .map { String(format: "%02x", $0) }
            .joined()
    }

    // MARK: - Change-feed migration

    @BigSyncBackgroundActor
    private func changeFeedMigrationStateKey(
        for context: RunContext
    ) -> String {
        let zoneComponent = modelAdapters
            .map {
                "\($0.recordZoneID.ownerName)/\($0.recordZoneID.zoneName)"
            }
            .sorted()
            .joined(separator: "|")
        let containerComponent = containerIdentifier ?? "default-container"
        return "\(identifier).BigSyncKit.ChangeFeedMigration.v\(ChangeFeedMigrationState.version).\(containerComponent).\(context.accountScopeIdentifier).\(zoneComponent)"
    }

    @BigSyncBackgroundActor
    private func changeFeedMigrationEpoch(
        for stateKey: String
    ) -> Int {
        let epochKey = "\(stateKey).epoch"
        return (keyValueStore.object(forKey: epochKey) as? NSNumber)?.intValue
            ?? (keyValueStore.object(forKey: epochKey) as? Int)
            ?? ChangeFeedMigrationState.initialEpoch
    }

    @BigSyncBackgroundActor
    private func changeFeedMigrationMode(
        for stateKey: String
    ) -> ChangeFeedResetMode {
        guard let rawMode = keyValueStore.object(
            forKey: "\(stateKey).mode"
        ) as? String else {
            return .serverReconciliation
        }
        return ChangeFeedResetMode(rawValue: rawMode)
            ?? .serverReconciliation
    }

    @BigSyncBackgroundActor
    internal func hasPendingEncryptedDataResetRecovery(
        context: RunContext
    ) -> Bool {
        let stateKey = changeFeedMigrationStateKey(for: context)
        let phase = (keyValueStore.object(forKey: stateKey) as? String)
            .flatMap(ChangeFeedMigrationState.Phase.init(rawValue:))
        return phase != nil
            && phase != .completed
            && changeFeedMigrationMode(for: stateKey)
                == .encryptedDataReset
    }

    /// Requests a new server-first rebuild after opaque CloudKit history can no
    /// longer continue. Target Realms and their durable mutation journals remain
    /// untouched; only adapter tracking/provenance is rebuilt on the next run.
    @BigSyncBackgroundActor
    internal func requestChangeFeedRecovery(
        context: RunContext,
        mode: ChangeFeedResetMode = .serverReconciliation
    ) {
        let stateKey = changeFeedMigrationStateKey(for: context)
        let currentPhase = (keyValueStore.object(forKey: stateKey) as? String)
            .flatMap(ChangeFeedMigrationState.Phase.init(rawValue:))
        let currentMode = changeFeedMigrationMode(for: stateKey)

        // A migration already in progress owns valid provenance for this exact
        // epoch. Restarting its nil-token bootstrap is idempotent; incrementing
        // here would discard evidence captured before the interrupted fetch.
        if currentPhase != .completed,
           currentPhase != nil,
           currentMode == mode {
            return
        }

        // An encrypted-data reset supersedes a conservative server
        // reconciliation already in flight. Its next epoch must rebuild all
        // live local records rather than interpret the empty server as remote
        // deletion. A conservative recovery never downgrades an encrypted
        // reset already in progress.
        if currentPhase != .completed,
           currentPhase != nil,
           currentMode == .encryptedDataReset,
           mode == .serverReconciliation {
            return
        }

        let epochKey = "\(stateKey).epoch"
        let nextEpoch = changeFeedMigrationEpoch(for: stateKey) + 1
        keyValueStore.set(value: nextEpoch, forKey: epochKey)
        keyValueStore.set(value: mode.rawValue, forKey: "\(stateKey).mode")
        keyValueStore.set(
            value: ChangeFeedMigrationState.Phase.requested.rawValue,
            forKey: stateKey
        )
        activeChangeFeedMigration = nil
    }

    /// A fresh install can discover an encrypted-data reset while its normal
    /// server-first migration is already active. Promote that same durable
    /// epoch instead of clearing Realm tracking twice; no server pages or
    /// uploads have been published yet, and each adapter changes only its
    /// recovery marker.
    @BigSyncBackgroundActor
    internal func promoteActiveChangeFeedMigrationToEncryptedDataReset(
        context: RunContext
    ) async throws {
        guard var migration = activeChangeFeedMigration else { return }
        guard migration.mode != .encryptedDataReset else { return }
        for adapter in modelAdapters {
            guard let migrating = adapter as? any ChangeFeedResetMigrating
            else { continue }
            try await migrating
                .promoteChangeFeedResetToEncryptedDataReset(
                    accountScopeIdentifier:
                        migration.accountScopeIdentifier,
                    epoch: migration.epoch
                )
            try await revalidateRunContext(context)
        }
        migration.mode = .encryptedDataReset
        activeChangeFeedMigration = migration
        keyValueStore.set(
            value: ChangeFeedResetMode.encryptedDataReset.rawValue,
            forKey: "\(migration.key).mode"
        )
    }

    /// Starts (or resumes) the production migration before any normal token
    /// can advance.  Every suspension is followed by a run-context validation;
    /// a changed account or superseding attempt therefore leaves the durable
    /// requested state for the next valid attempt instead of publishing stale
    /// progress.
    @BigSyncBackgroundActor
    internal func beginChangeFeedMigrationIfNeeded(
        context: RunContext
    ) async throws {
        let migratingAdapters = modelAdapters.compactMap { $0 as? any ChangeFeedResetMigrating }
        guard !migratingAdapters.isEmpty else { return }

        let stateKey = changeFeedMigrationStateKey(for: context)
        var previousPhase = (keyValueStore.object(forKey: stateKey) as? String)
            .flatMap(ChangeFeedMigrationState.Phase.init(rawValue:))
            ?? .requested
        guard previousPhase != .completed else { return }
        let epochKey = "\(stateKey).epoch"
        let epoch = changeFeedMigrationEpoch(for: stateKey)
        let mode = changeFeedMigrationMode(for: stateKey)
        keyValueStore.set(value: epoch, forKey: epochKey)
        // `finishing` is the only cross-store ambiguous window: all adapters
        // may have durably finished before the local KVS marker was written.
        // In that case observing no active bootstrap is sufficient to publish
        // completion.  If any adapter is still active, restart from requested;
        // the adapter's provenance makes that restart idempotent.
        if previousPhase == .finishing {
            var anyBootstrapActive = false
            for adapter in migratingAdapters {
                let isBootstrapActive = await adapter.isChangeFeedServerBootstrapActive()
                anyBootstrapActive = anyBootstrapActive || isBootstrapActive
                try await revalidateRunContext(context)
            }
            if !anyBootstrapActive {
                if mode == .encryptedDataReset {
                    for adapter in modelAdapters {
                        clearConfiguredZoneTerminal(
                            adapter.recordZoneID,
                            accountScopeIdentifier:
                                context.accountScopeIdentifier
                        )
                    }
                }
                keyValueStore.set(
                    value: ChangeFeedMigrationState.Phase.completed.rawValue,
                    forKey: stateKey
                )
                return
            }
            previousPhase = .requested
        }
        keyValueStore.set(value: previousPhase.rawValue, forKey: stateKey)
        let migration = ChangeFeedMigrationState(
            key: stateKey,
            accountScopeIdentifier: context.accountScopeIdentifier,
            epoch: epoch,
            mode: mode,
            phase: .requested
        )
        activeChangeFeedMigration = migration

        // A database cursor is not per-zone evidence. With multiple adapters it
        // may predate a newly configured zone, so only an adapter's valid server
        // record proof or a previously persisted zone marker may establish it.
        for adapter in modelAdapters {
            guard let migrating = adapter as? any ChangeFeedResetMigrating else { continue }
            if try await migrating.hasChangeFeedEstablishedServerEvidence() {
                markConfiguredZoneEstablished(
                    adapter.recordZoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            }
            try await revalidateRunContext(context)
        }

        for adapter in migratingAdapters {
            try await adapter.prepareChangeFeedReset(
                accountScopeIdentifier: context.accountScopeIdentifier,
                epoch: epoch,
                mode: mode
            )
            try await revalidateRunContext(context)
        }
        keyValueStore.set(
            value: ChangeFeedMigrationState.Phase.prepared.rawValue,
            forKey: stateKey
        )
        for adapter in migratingAdapters {
            try await adapter.beginChangeFeedServerBootstrap(
                accountScopeIdentifier: context.accountScopeIdentifier,
                epoch: epoch,
                mode: mode
            )
            try await revalidateRunContext(context)
        }

        // A nil database and zone token is the explicit full-server bootstrap
        // contract.  Do not reuse a token left by the legacy transport.
        resetDatabaseToken()
        resetActiveTokens()
        for adapter in modelAdapters {
            try await adapter.saveToken(nil)
            try await revalidateRunContext(context)
        }
        activeChangeFeedMigration?.phase = .serverBootstrap
        keyValueStore.set(
            value: ChangeFeedMigrationState.Phase.serverBootstrap.rawValue,
            forKey: stateKey
        )
    }

    /// Runs exactly after the nil-token feed has consumed its pages and before
    /// upload discovery.  Remote tracking rows and the target Realm's durable
    /// mutation journal win over captured provenance during reconciliation.
    @BigSyncBackgroundActor
    internal func reconcileChangeFeedMigrationIfNeeded(
        context: RunContext
    ) async throws {
        guard var migration = activeChangeFeedMigration,
              migration.phase == .serverBootstrap else { return }
        for adapter in modelAdapters {
            guard let migrating = adapter as? any ChangeFeedResetMigrating else { continue }
            try await migrating.reconcileAfterChangeFeedServerBootstrap(
                accountScopeIdentifier: migration.accountScopeIdentifier,
                epoch: migration.epoch,
                mode: migration.mode
            )
            try await revalidateRunContext(context)
        }
        migration.phase = .reconciled
        activeChangeFeedMigration = migration
        keyValueStore.set(value: ChangeFeedMigrationState.Phase.reconciled.rawValue, forKey: migration.key)
    }

    /// Called only at the normal terminal receipt, after the post-upload
    /// refetch establishes quiescence.  Interrupted migrations intentionally
    /// retain their non-complete state and restart safely with nil tokens.
    @BigSyncBackgroundActor
    internal func finishChangeFeedMigrationIfNeeded(
        context: RunContext
    ) async throws {
        guard var migration = activeChangeFeedMigration,
              migration.phase == .reconciled else { return }
        keyValueStore.set(
            value: ChangeFeedMigrationState.Phase.finishing.rawValue,
            forKey: migration.key
        )
        migration.phase = .finishing
        activeChangeFeedMigration = migration
        for adapter in modelAdapters {
            guard let migrating = adapter as? any ChangeFeedResetMigrating else { continue }
            try await migrating.finishChangeFeedReset(
                accountScopeIdentifier: migration.accountScopeIdentifier,
                epoch: migration.epoch,
                mode: migration.mode
            )
            try await revalidateRunContext(context)
        }
        if migration.mode == .encryptedDataReset {
            // Clear the terminal fence only after every adapter has completed
            // its journal-backed re-upload and the normal terminal drain has
            // proven quiescence. Clearing before the durable `completed`
            // marker makes the crash window safely resumable.
            for adapter in modelAdapters {
                clearConfiguredZoneTerminal(
                    adapter.recordZoneID,
                    accountScopeIdentifier:
                        migration.accountScopeIdentifier
                )
            }
        }
        migration.phase = .completed
        keyValueStore.set(value: ChangeFeedMigrationState.Phase.completed.rawValue, forKey: migration.key)
        activeChangeFeedMigration = nil
    }

#if DEBUG
    @BigSyncBackgroundActor
    func _test_validateSynchronizationAccount() async throws {
        _ = try await validateSynchronizationAccount()
    }
#endif
    
    /// Cancel synchronization. It will cause a current synchronization to end with a `cancelled` error.
    @BigSyncBackgroundActor
    @objc public func cancelSynchronization() {
        //        guard syncing, !cancelSync else { return }
        let cancelledAttemptID = synchronizationAttemptID
        synchronizationAttemptID = UUID()
        cancelAttemptCallbacks(for: cancelledAttemptID)
        activeRunContext = nil
        activeReceiptAuthorizationID = nil
        reservedReceiptAuthorizationID = nil
        changeRequestProcessor.reset()
        synchronizationTask?.cancel()
        synchronizationTask = nil
        if !cancelSync {
            logger.info("QSCloudKitSynchronizer >> Cancelling synchronization...")
        }
        cancelSync = true
        syncing = false
        retrySleepUntil = nil
        consecutiveTransientCloudKitFailures = 0
        
        for adapter in modelAdapters {
            adapter.cancelSynchronization()
        }
        finishSynchronizationDrain(
            with: .failure(CancellationError())
        )
    }

    /// Establishes a logical cancellation barrier before destructive metadata
    /// or zone changes. Operation callbacks are fenced by attempt ID, so the
    /// barrier does not need a second lock or an unbounded operation poll.
    @BigSyncBackgroundActor
    public func cancelSynchronizationAndWait() async {
        cancelSynchronization()
        // Do not await the orchestration task itself. An async CloudKit request
        // is allowed to ignore cooperative task cancellation and can therefore
        // remain suspended indefinitely. Attempt/run-context fencing prevents
        // that task from publishing anything if it eventually resumes; the
        // barriers below cover the callback and adapter work that can mutate
        // durable state.
        await changeRequestProcessor.waitForProcessingToStop()
        await waitForRunCallbacksToFinish()
        for adapter in modelAdapters {
            await adapter.waitForCancellation()
        }
    }
    
    /**
     *  Deletes saved database token, so next synchronization will include changes in all record zones in the database.
     * This does not reset tokens stored by model adapters.
     */
    @BigSyncBackgroundActor
    @objc internal func resetDatabaseToken() {
        storedDatabaseToken = nil
    }
    
    internal func activeZoneToken(zoneID: CKRecordZone.ID) -> RecordZoneChangeCursor? {
        return activeZoneTokens[zoneID]
    }
    
    /// Deletes the one active custom zone of a disposable synchronizer client.
    ///
    /// The caller must discard the synchronizer and all of its adapters after
    /// this succeeds: their local tracking state now refers to a deleted zone.
    /// This is intentionally limited to a client whose every adapter targets
    /// one identical zone, such as an isolated end-to-end test client.
    @BigSyncBackgroundActor
    @discardableResult
    public func deleteActiveRecordZoneForDisposableClient(
        using receipt: SynchronizationReceipt
    ) async throws -> Bool {
        let activeZoneIDs = Set(modelAdapters.map(\.recordZoneID))
        guard activeZoneIDs.count == 1,
              let activeZoneID = activeZoneIDs.first else {
            throw OneOffRecordZoneResetError.disposableClientMustUseExactlyOneRecordZone
        }
        return try await deleteDisposableRecordZoneIfPresent(
            activeZoneID,
            using: receipt
        )
    }

    private func deleteDisposableRecordZoneIfPresent(
        _ zoneID: CKRecordZone.ID,
        using receipt: SynchronizationReceipt
    ) async throws -> Bool {
        guard allowsDisposableZoneDeletion else {
            throw OneOffRecordZoneResetError.disposableZoneDeletionNotAllowed
        }
        guard receipt.issuerID == synchronizationReceiptIssuerID else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        guard activeReceiptAuthorizationID == receipt.authorizationID else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        guard reservedReceiptAuthorizationID == nil else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        // Reserve before suspending so a concurrent/replayed call cannot use
        // this authorization. Consume only after a terminal delete outcome;
        // transient CloudKit failures may safely retry the same fenced receipt.
        reservedReceiptAuthorizationID = receipt.authorizationID
        var shouldConsumeAuthorization = false
        func validateReservedAuthorization() throws {
            guard activeReceiptAuthorizationID == receipt.authorizationID,
                  reservedReceiptAuthorizationID == receipt.authorizationID else {
                throw OneOffRecordZoneResetError.cloudKitAccountChanged
            }
        }
        defer {
            if reservedReceiptAuthorizationID == receipt.authorizationID {
                reservedReceiptAuthorizationID = nil
            }
            if shouldConsumeAuthorization,
               activeReceiptAuthorizationID == receipt.authorizationID {
                activeReceiptAuthorizationID = nil
            }
        }
        do {
            try await ensureCurrentAccount(receipt.accountIdentifier)
            try validateReservedAuthorization()
            let deleted: Bool
            do {
                try await deleteRecordZone(zoneID)
                deleted = true
            } catch {
                guard isMissingRecordZoneError(error) else { throw error }
                deleted = false
            }
            try validateReservedAuthorization()
            try await ensureCurrentAccount(receipt.accountIdentifier)
            try validateReservedAuthorization()
            shouldConsumeAuthorization = true
            return deleted
        } catch {
            throw error
        }
    }

    @BigSyncBackgroundActor
    private func ensureCurrentAccount(_ expectedIdentifier: String) async throws {
        guard try await accountIdentifierProvider() == expectedIdentifier else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }

    @BigSyncBackgroundActor
    private func deleteRecordZone(_ zoneID: CKRecordZone.ID) async throws {
        try await zoneStore.deleteRecordZone(withID: zoneID)
    }

    private func isMissingRecordZoneError(_ error: Error) -> Bool {
        let nsError = error as NSError
        return nsError.domain == CKErrorDomain &&
            [
                CKError.zoneNotFound.rawValue,
                CKError.userDeletedZone.rawValue,
            ].contains(nsError.code)
    }

    /// Model adapters in use by this synchronizer
    public var modelAdapters: [ModelAdapter] {
        return Array(modelAdapterDictionary.values)
    }

    /// Returns whether `adapter` can be registered without sharing a Realm
    /// tracking store with a different CloudKit zone. The rebuild provenance is
    /// deliberately adapter-local, so sharing it would allow one zone's reset
    /// or completion to erase another zone's server evidence.
    internal func canAddModelAdapter(_ adapter: ModelAdapter) -> Bool {
        if let existing = modelAdapterDictionary[adapter.recordZoneID] {
            // Registration is idempotent for the same object, but silently
            // replacing a live adapter would leave the old instance able to
            // call this synchronizer through its delegate.
            return existing === adapter
        }
        guard let realmAdapter = adapter as? RealmSwiftAdapter else {
            return true
        }
        return !modelAdapters.contains { existingAdapter in
            guard existingAdapter.recordZoneID != realmAdapter.recordZoneID,
                  let existingRealmAdapter = existingAdapter as? RealmSwiftAdapter else {
                return false
            }
            return existingRealmAdapter.persistenceRealmIdentity
                == realmAdapter.persistenceRealmIdentity
        }
    }
    
    /// Adds a new model adapter to be synchronized with CloudKit.
    /// - Parameter adapter: The adapter to be managed by this synchronizer.
    public func addModelAdapter(_ adapter: ModelAdapter) {
        precondition(
            canAddModelAdapter(adapter),
            "RealmSwiftAdapters for different CloudKit zones must use distinct persistence Realm identities"
        )
        modelAdapterDictionary[adapter.recordZoneID] = adapter
        adapter.modelAdapterDelegate = self
    }
    
}

extension CloudKitSynchronizer: ModelAdapterDelegate {
    public func needsInitialSetup() async throws {
        try await resetSyncCachesOwnedByCurrentFlow(
            includingAdapters: false
        )
    }
    
    public func hasChangesToUpload() async {
        beginSynchronization()
    }
}
