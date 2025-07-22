//
//  CloudKitSynchronizer.swift
//  Pods
//
//  Created by Manuel Entrena on 05/04/2019.
//

import Foundation
import CloudKit
import Logging
import Combine
import RealmSwiftGaps

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

/// An `AdapterProvider` gets requested for new model adapters when a `CloudKitSynchronizer` encounters a new `CKRecordZone` that does not already correspond to an existing model adapter.
//@objc public protocol AdapterProvider {
public protocol AdapterProvider {
    
    /// The `CloudKitSynchronizer` requests a new model adapter for the given record zone.
    /// - Parameters:
    ///   - synchronizer: `QSCloudKitSynchronizer` asking for the adapter.
    ///   - zoneID: `CKRecordZoneID` that the model adapter will be used for.
    /// - Returns: `ModelAdapter` correctly configured to sync changes in the given record zone.
    @BigSyncBackgroundActor
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, modelAdapterForRecordZoneID zoneID: CKRecordZone.ID) -> ModelAdapter?
    
    /// The `CloudKitSynchronizer` informs the provider that a record zone was deleted so it can clean up any associated data.
    /// - Parameters:
    ///   - synchronizer: `QSCloudKitSynchronizer` that found the deleted record zone.
    ///   - zoneID: `CKRecordZoneID` of the record zone that was deleted.
    @BigSyncBackgroundActor
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID) async
}

//@objc public protocol CloudKitSynchronizerDelegate: AnyObject {
public protocol CloudKitSynchronizerDelegate: AnyObject {
    func synchronizerWillFetchChanges(_ synchronizer: CloudKitSynchronizer, in recordZone: CKRecordZone.ID)
    func synchronizerWillUploadChanges(_ synchronizer: CloudKitSynchronizer, to recordZone: CKRecordZone.ID)
    func synchronizerDidSync(_ synchronizer: CloudKitSynchronizer)
    func synchronizerDidfailToSync(_ synchronizer: CloudKitSynchronizer, error: Error)
    func synchronizer(_ synchronizer: CloudKitSynchronizer, didAddAdapter adapter: ModelAdapter, forRecordZoneID zoneID: CKRecordZone.ID)
    func synchronizer(_ synchronizer: CloudKitSynchronizer, zoneIDWasDeleted zoneID: CKRecordZone.ID)
}

internal struct ChangeRequest: Sendable {
    let downloadedRecord: CKRecord?
    let deletedRecordID: CKRecord.ID?
    let adapter: ModelAdapter
}

@BigSyncBackgroundActor
internal class ChangeRequestProcessor {
    static let shared = ChangeRequestProcessor()
    
//    internal var logger: Logging.Logger?

    internal var cancelSync = false {
        didSet {
            if !cancelSync && oldValue {
                Task { @BigSyncBackgroundActor [weak self] in
                    try await self?.runProcessFetchedChangeRequests()
                }
            }
        }
    }
    
    init() {
        changeSubject
            .debounce(for: .seconds(3), scheduler: DispatchQueue.global())
            .sink { @Sendable [weak self] _ in
                guard let self else { return }
                Task { @BigSyncBackgroundActor [weak self] in
                    guard let self else { return }
                    guard !cancelSync else { return }
                    try await runProcessFetchedChangeRequests()
                }
            }
            .store(in: &cancellables)
    }
    
    private var changeRequests = [ChangeRequest]()
    private var changeSubject = PassthroughSubject<Void, Never>()
    private var cancellables = Set<AnyCancellable>()
    private var localErrors: [Error] = []
    internal var processTask: Task<Void, Error>? = nil
    private let batchSize = 100
    
    internal func addFetchedChangeRequest(_ request: ChangeRequest) {
        //        debugPrint("# addChangeReq", request.downloadedRecord?.recordID.recordName)
        changeRequests.append(request)
        changeSubject.send()
    }
    
    private func runProcessFetchedChangeRequests() async throws {
        processTask?.cancel()
        _ = try? await processTask?.value
        processTask = Task { @BigSyncBackgroundActor [weak self] in
            try await self?.processFetchedChangeRequests()
            self?.processTask = nil
        }
        try await processTask?.value
    }
    
    private func processFetchedChangeRequests() async throws {
//        debugPrint("# processFetchedChangeRequests() inner")
        try Task.checkCancellation()
        
        while !changeRequests.isEmpty {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            let batch = changeRequests.prefix(batchSize)
            changeRequests.removeFirst(batch.count)
            
            do {
//                logger?.info("QSCloudKitSynchronizer >> Processing \(batch.count) remote records for local merge: \(batch.compactMap { $0.downloadedRecord?.recordID.recordName } .joined(separator: " ")) (\(changeRequests.count) more remaining)")
                
                let downloadedRecords = try batch.compactMap {
                    try Task.checkCancellation()
                    return $0.downloadedRecord
                }
                
                try await batch.first?.adapter.saveChanges(in: downloadedRecords, forceSave: false)
                try Task.checkCancellation()
                
                let deletedRecordIDs = try batch.compactMap {
                    try Task.checkCancellation()
                    return $0.deletedRecordID
                }
                if !deletedRecordIDs.isEmpty {
                    try await batch.first?.adapter.deleteRecords(with: deletedRecordIDs)
                }
            } catch is CancellationError {
                changeRequests.insert(contentsOf: batch, at: 0)
                return
            } catch {
                localErrors.append(error)
                changeRequests.insert(contentsOf: batch, at: 0)
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
    
    @BigSyncBackgroundActor
    func finishProcessing() async throws {
        try Task.checkCancellation()
        try await runProcessFetchedChangeRequests()
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
    internal var serverChangeToken: CKServerChangeToken?
    internal var activeZoneTokens = [CKRecordZone.ID: CKServerChangeToken]()
    @BigSyncBackgroundActor
    internal var cancelSync = false
    @BigSyncBackgroundActor
    internal var retrySleepUntil: Date?
    
    internal var currentOperations = [Operation]()
    internal var uploadRetries = 0
    internal var didNotifyUpload = Set<CKRecordZone.ID>()
    internal var synchronizationTask: Task<Void, Never>?
    internal var modifyRecordsTask: Task<Void, Error>?
    internal var fetchDatabaseChangesTask: Task<Void, Error>?
    internal var fetchZoneChangesTask: Task<Void, Error>?
    internal var mergeChangesTask: Task<Void, Error>?
    internal var fetchZoneChangesCompletionTask: Task<Void, Error>? = nil

    internal var lastDatabaseChangesEmptyAt: Date?
    internal var lastZoneChangesEmptyAt: Date?
 
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
        logger: Logging.Logger
    ) {
        self.identifier = identifier
        self.containerIdentifier = containerIdentifier
        self.adapterProvider = adapterProvider
        self.database = database
        self.keyValueStore = keyValueStore
        self.compatibilityVersion = compatibilityVersion
        self.logger = logger
        super.init()
        
        BackupDetection.runBackupDetection { [weak self] (result, error) in
            guard let self else { return }
            if result == .restoredFromBackup {
                clearDeviceIdentifier()
            }
        }
        
//        Task {
//            ChangeRequestProcessor.shared.logger = logger
//        }
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
    }
    
    @BigSyncBackgroundActor
    public func resetSyncCaches(cancelSynchronization: Bool, includingAdapters: Bool = true) async throws {
        if cancelSynchronization {
            self.cancelSynchronization()
        }
        
        clearDeviceIdentifier()
        resetDatabaseToken()
        resetActiveTokens()
        lastDatabaseChangesEmptyAt = nil
        
        //        try? await Task.sleep(nanoseconds: 300_000_000) // Allow cancellations to catch up...
        if includingAdapters {
            for adapter in modelAdapters {
                try? await adapter.unsetCancellation()
                try? await adapter.resetSyncCaches()
            }
        }
    }
    
    // MARK: - Public
    
    /// Synchronize data with CloudKit.
    /// - Parameter onFailure: Block that receives an error if the synchronization stopped due to a failure. Could be a `SyncError`, `CKError`, or any other error found during synchronization.
    @BigSyncBackgroundActor
    @objc public func beginSynchronization() { //onFailure: ((Error) -> ())?) {
        guard !cancelledDueToUnauthentication else { return }
        
        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            guard !syncing else {
                return
            }
            
            logger.info("QSCloudKitSynchronizer >> Begin synchronization...")
   
            //        debugPrint("CloudKitSynchronizer >> Initiating synchronization", identifier, containerIdentifier)
            cancelSync = false
            ChangeRequestProcessor.shared.cancelSync = false
            syncing = true
            //        self.onFailure = onFailure
            
            for adapter in modelAdapters {
                try await adapter.unsetCancellation()
            }
            
            await performSynchronization()
        }
    }
    
    /// Cancel synchronization. It will cause a current synchronization to end with a `cancelled` error.
    @BigSyncBackgroundActor
    @objc public func cancelSynchronization() {
        //        guard syncing, !cancelSync else { return }
        
        ChangeRequestProcessor.shared.cancelSync = true
        ChangeRequestProcessor.shared.processTask?.cancel()
        synchronizationTask?.cancel()
        modifyRecordsTask?.cancel()
        fetchDatabaseChangesTask?.cancel()
        fetchZoneChangesTask?.cancel()
        mergeChangesTask?.cancel()
        fetchZoneChangesCompletionTask?.cancel()

        guard !cancelSync else { return }
        logger.info("QSCloudKitSynchronizer >> Cancelling synchronization...")
        
        cancelSync = true
        syncing = false // TODO: This might be buggy to set eagerly?!
        currentOperations.forEach { $0.cancel() }
        
        for adapter in modelAdapters {
            adapter.cancelSynchronization()
        }
    }
    
    /**
     *  Deletes saved database token, so next synchronization will include changes in all record zones in the database.
     * This does not reset tokens stored by model adapters.
     */
    @BigSyncBackgroundActor
    @objc public func resetDatabaseToken() {
        storedDatabaseToken = nil
    }
    
    internal func activeZoneToken(zoneID: CKRecordZone.ID) -> CKServerChangeToken? {
        return activeZoneTokens[zoneID]
    }
    
    //    /**
    //    * Deletes saved database token and all local metadata used to track changes in models.
    //    * The synchronizer should not be used after calling this function, create a new synchronizer instead if you need it.
    //    */
    //    @BigSyncBackgroundActor
    //    @objc public func eraseLocalMetadata(removeModelAdapters: Bool) {
    //        cancelSynchronization()
    //
    ////        dispatchQueue.async {
    //        Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
    //            guard let self = self else { return }
    //            storedDatabaseToken = nil
    //            clearAllStoredSubscriptionIDs()
    //            deviceUUID = nil
    //            for modelAdapter in modelAdapters {
    //                await modelAdapter.deleteChangeTracking()
    //                if removeModelAdapters {
    //                    removeModelAdapter(modelAdapter)
    ////                } else {
    ////                    await modelAdapter.saveToken(nil)
    //                }
    //            }
    //        }
    //    }
    
    /// Deletes the corresponding record zone on CloudKit, along with any data in it.
    /// - Parameters:
    ///   - adapter: Model adapter whose corresponding record zone should be deleted
    ///   - completion: Completion block.
    @BigSyncBackgroundActor
    public func deleteRecordZone(for adapter: ModelAdapter, completion: ((Error?) -> ())?) {
        database.delete(withRecordZoneID: adapter.recordZoneID) { (zoneID, error) in
            Task(priority: .background) { @BigSyncBackgroundActor [weak self] in
                await adapter.saveToken(nil)
                if let error = error {
                    //                    debugPrint("CloudKitSynchronizer >> Error: \(error)")
                    self?.logger.error("CloudKitSynchronizer >> Error: \(error)")
                } else {
                    //                    debugPrint("CloudKitSynchronizer >> Deleted zone: \(zoneID?.debugDescription ?? "")")
                    self?.logger.error("CloudKitSynchronizer >> Deleted zone: \(zoneID?.debugDescription ?? "")")
                }
                completion?(error)
            }
        }
    }
    
    /// Model adapters in use by this synchronizer
    public var modelAdapters: [ModelAdapter] {
        return Array(modelAdapterDictionary.values)
    }
    
    /// Adds a new model adapter to be synchronized with CloudKit.
    /// - Parameter adapter: The adapter to be managed by this synchronizer.
    public func addModelAdapter(_ adapter: ModelAdapter) {
        modelAdapterDictionary[adapter.recordZoneID] = adapter
        adapter.modelAdapterDelegate = self
    }
    
    /// Removes the model adapter so data managed by it won't be synced with CloudKit any more.
    /// - Parameter adapter: Adapter to be removed from the synchronizer
    public func removeModelAdapter(_ adapter: ModelAdapter) {
        modelAdapterDictionary.removeValue(forKey: adapter.recordZoneID)
    }
}

extension CloudKitSynchronizer: ModelAdapterDelegate {
    public func needsInitialSetup() async throws {
        try await resetSyncCaches(cancelSynchronization: false, includingAdapters: false)
    }
    
    public func hasChangesToUpload() async {
        await beginSynchronization()
    }
}
