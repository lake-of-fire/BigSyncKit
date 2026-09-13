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

/// Ordinary deletion and purge fence an established zone. Only the explicit
/// encrypted-data-reset recovery may recreate it and re-upload retained data;
/// its durable recovery obligation must precede destructive metadata changes.
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

internal struct InboundProcessingOutcomes: Sendable {
    var liveResults = [InboundLiveResult]()
    var deletionResults = [InboundDeletionResult]()

    mutating func append(_ other: Self) {
        liveResults.append(contentsOf: other.liveResults)
        deletionResults.append(contentsOf: other.deletionResults)
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
    private var processingTask: Task<InboundProcessingOutcomes, Error>?
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
    ) async throws -> InboundProcessingOutcomes {
        let runID = activeRunID
        let taskID = UUID()
        let task = Task { @BigSyncBackgroundActor [weak self] in
            guard let self else { throw CancellationError() }
            return try await processFetchedChangeRequests(
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
        return try await task.value
    }
    
    private func processFetchedChangeRequests(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String?,
        runID: UUID
    ) async throws -> InboundProcessingOutcomes {
        try Task.checkCancellation()
        var outcomes = InboundProcessingOutcomes()
        
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
            guard !batch.isEmpty else { return outcomes }
            
            do {
                let downloadedRecords = try batch.compactMap {
                    try Task.checkCancellation()
                    return $0.downloadedRecord
                }
                
                if !downloadedRecords.isEmpty {
                    let results = try await batch.first?.adapter.saveChanges(
                        in: downloadedRecords,
                        forceSave: false
                    ) ?? []
                    try Self.validateInboundLiveResults(
                        results,
                        records: downloadedRecords
                    )
                    outcomes.liveResults.append(contentsOf: results)
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
                    let results = try await batch.first?.adapter.deleteRecords(
                        with: deletedRecordIDs
                    ) ?? []
                    try Self.validateInboundDeletionResults(
                        results,
                        recordIDs: deletedRecordIDs
                    )
                    outcomes.deletionResults.append(contentsOf: results)
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
                return outcomes
            }
            
            try await Task.sleep(nanoseconds: 500_000)
        }
    }

    static func validateInboundLiveResults(
        _ results: [InboundLiveResult],
        records: [CKRecord]
    ) throws {
        guard results.count == records.count else {
            throw InboundDispositionValidationError.cardinality(
                expected: records.count,
                actual: results.count
            )
        }
        for (ordinal, pair) in zip(records.indices, zip(records, results)) {
            guard pair.1.event.matches(
                ordinal: ordinal,
                entityType: pair.0.recordType,
                recordID: pair.0.recordID
            ) else {
                throw InboundDispositionValidationError.identityMismatch(
                    ordinal: ordinal,
                    expectedRecordName: pair.0.recordID.recordName
                )
            }
        }
    }

    static func validateInboundDeletionResults(
        _ results: [InboundDeletionResult],
        recordIDs: [CKRecord.ID]
    ) throws {
        guard results.count == recordIDs.count else {
            throw InboundDispositionValidationError.cardinality(
                expected: recordIDs.count,
                actual: results.count
            )
        }
        for (ordinal, pair) in zip(
            recordIDs.indices,
            zip(recordIDs, results)
        ) {
            guard pair.1.event.matches(
                ordinal: ordinal,
                recordID: pair.0
            ) else {
                throw InboundDispositionValidationError.identityMismatch(
                    ordinal: ordinal,
                    expectedRecordName: pair.0.recordName
                )
            }
        }
    }

    /// Reject malformed transport pages before an adapter can publish any of
    /// their target or tracking state. Commit-time receipt validation remains
    /// as a second fence around cursor advancement.
    static func validateInboundPageIdentities(
        records: [CKRecord],
        deletedRecordIDs: [CKRecord.ID],
        expectedZoneID: CKRecordZone.ID
    ) throws {
        var representedRecordIDs = Set<CKRecord.ID>()
        for recordID in records.map(\.recordID) + deletedRecordIDs {
            guard recordID.zoneID == expectedZoneID else {
                throw InboundDispositionValidationError
                    .eventOutsideExpectedZone(
                        recordName: recordID.recordName
                    )
            }
            guard representedRecordIDs.insert(recordID).inserted else {
                throw InboundDispositionValidationError
                    .duplicateInboundEvent(recordName: recordID.recordName)
            }
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
    @discardableResult
    func finishProcessing(
        for adapter: ModelAdapter,
        restrictedToEntityType restrictedEntityType: String? = nil
    ) async throws -> InboundProcessingOutcomes {
        try Task.checkCancellation()
        return try await runProcessFetchedChangeRequests(
            for: adapter,
            restrictedToEntityType: restrictedEntityType
        )
    }

    @BigSyncBackgroundActor
    @discardableResult
    func finishProcessing() async throws -> InboundProcessingOutcomes {
        var outcomes = InboundProcessingOutcomes()
        while let adapter = changeRequests.first?.adapter {
            outcomes.append(try await finishProcessing(for: adapter))
        }
        return outcomes
    }
}

let cloudKitSynchronizerDeviceUUIDKey = "QSCloudKitDeviceUUIDKey"
let cloudKitSynchronizerModelCompatibilityVersionKey = "QSCloudKitModelCompatibilityVersionKey"
public let cloudKitSynchronizerErrorDomain = "CloudKitSynchronizerErrorDomain"
public let cloudKitSynchronizerErrorKey = "CloudKitSynchronizerErrorKey"

/// Revokes account authority immediately while actor-isolated durable
/// invalidation catches up.
final class AccountScopeAuthorityFence: @unchecked Sendable {
    private let lock = NSLock()
    private var isPoisoned = true
    private var rotatesGeneration = false

    func poison(requiresGenerationRotation: Bool = true) {
        lock.lock()
        isPoisoned = true
        rotatesGeneration = rotatesGeneration || requiresGenerationRotation
        lock.unlock()
    }

    func clear() {
        lock.lock()
        isPoisoned = false
        rotatesGeneration = false
        lock.unlock()
    }

    var rejectsAuthority: Bool {
        lock.lock()
        defer { lock.unlock() }
        return isPoisoned
    }

    var requiresGenerationRotation: Bool {
        lock.lock()
        defer { lock.unlock() }
        return rotatesGeneration
    }
}

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
    public typealias AccountStatusProvider = @Sendable () async throws
        -> CKAccountStatus
    /// Optional, actor-isolated lifecycle observation for diagnostics. Production
    /// callers receive a no-op unless they explicitly provide a handler.
    /// `terminal-receipt` is a post-completion diagnostic, not a veto or
    /// capability: the old drain has released its shared state before delivery.
    /// Use the explicit DEBUG process-kill checkpoint for pre-delivery tests.
    public typealias ProgressHandler = @BigSyncBackgroundActor @Sendable (String) -> Void
    public typealias SynchronizationWillConsumeServerChangesHandler =
        @BigSyncBackgroundActor @Sendable (
            SynchronizationBoundaryContext
        ) async throws -> Void
    public typealias DomainPrepublicationHandler =
        @BigSyncBackgroundActor @Sendable (
            PrepublicationBoundaryContext
        ) async throws -> [DomainBlocker]
    public typealias PublicationFetchDeferralHandler =
        @BigSyncBackgroundActor @Sendable (BigSyncDurablePublicationEvidence) async throws -> Bool
    public typealias DomainPublicationScopeIdentifierProvider =
        @BigSyncBackgroundActor @Sendable () async throws -> String?

    public struct DomainBlocker: Sendable, Equatable, Hashable {
        public let code: String
        public let detail: String?

        public init(code: String, detail: String? = nil) {
            precondition(!code.isEmpty)
            self.code = code
            self.detail = detail
        }
    }

    /// Fenced transport identity exposed immediately before a synchronization
    /// run may make downloaded target rows visible.
    public struct SynchronizationBoundaryContext: Sendable, Equatable {
        public let accountScopeIdentifier: String
        public let replicaBindingGenerationIdentifier: String?
        public let runID: UUID

        internal init(context: RunContext) {
            accountScopeIdentifier = context.accountScopeIdentifier
            replicaBindingGenerationIdentifier =
                context.replicaBindingGenerationIdentifier
            runID = context.runID
        }

        internal init(receipt: SynchronizationReceipt) {
            accountScopeIdentifier = receipt.accountScopeIdentifier
            replicaBindingGenerationIdentifier = receipt.replicaBindingGenerationIdentifier
            runID = receipt.runID
        }
    }

    internal struct RunContext: Sendable, Equatable {
        let attemptID: UUID
        let runID: UUID
        let accountIdentifier: String
        let accountScopeIdentifier: String
        let replicaBindingGenerationIdentifier: String?
        /// Captured before the run's first suspension after account validation.
        /// Nil is reserved for non-upload recovery contexts/older test fixtures.
        let accountInvalidationGeneration: Int64?

        init(
            attemptID: UUID,
            runID: UUID,
            accountIdentifier: String,
            accountScopeIdentifier: String,
            replicaBindingGenerationIdentifier: String? = nil,
            accountInvalidationGeneration: Int64? = nil
        ) {
            self.accountInvalidationGeneration = accountInvalidationGeneration
            self.attemptID = attemptID
            self.runID = runID
            self.accountIdentifier = accountIdentifier
            self.accountScopeIdentifier = accountScopeIdentifier
            self.replicaBindingGenerationIdentifier =
                replicaBindingGenerationIdentifier
        }
    }

    public struct SynchronizationReceipt: Sendable, Equatable {
        public let accountScopeIdentifier: String
        public let replicaBindingGenerationIdentifier: String?
        public let runID: UUID
        /// Latest record-zone cursor durably consumed by the terminal drain.
        /// It is distinct from any domain activation/floor boundary.
        public let consumedServerBoundaryIdentifier: String?
        /// Exact domain scope used for this terminal candidate and its
        /// durable evidence. Nil remains valid for a non-domain synchronizer.
        public let domainPublicationScopeIdentifier: String?
        internal let accountIdentifier: String
        internal let issuerID: UUID
        internal let authorizationID: UUID

        internal init(
            context: RunContext,
            issuerID: UUID,
            authorizationID: UUID,
            consumedServerBoundaryIdentifier: String? = nil,
            domainPublicationScopeIdentifier: String? = nil
        ) {
            accountScopeIdentifier = context.accountScopeIdentifier
            replicaBindingGenerationIdentifier =
                context.replicaBindingGenerationIdentifier
            runID = context.runID
            self.consumedServerBoundaryIdentifier =
                consumedServerBoundaryIdentifier
            self.domainPublicationScopeIdentifier = domainPublicationScopeIdentifier
            accountIdentifier = context.accountIdentifier
            self.issuerID = issuerID
            self.authorizationID = authorizationID
        }
    }

    public struct SynchronizationResult: Sendable, Equatable {
        public enum PublicationState: Sendable, Equatable {
            case complete
            case blocked([DomainBlocker])
        }

        public let didImportChanges: Bool
        public let receipt: SynchronizationReceipt?
        public let publicationState: PublicationState
        /// The attempted boundary exists even when semantic blockers prohibit
        /// a success receipt. Domain callbacks must not apply a delayed block
        /// to whichever unrelated run happens to be current on delivery.
        public let boundary: SynchronizationBoundaryContext?

        public init(
            didImportChanges: Bool,
            receipt: SynchronizationReceipt? = nil,
            publicationState: PublicationState = .complete,
            boundary: SynchronizationBoundaryContext? = nil
        ) {
            precondition(
                publicationState == .complete || receipt == nil,
                "A semantically blocked synchronization cannot publish a terminal receipt"
            )
            self.didImportChanges = didImportChanges
            let receiptBoundary = receipt.map { SynchronizationBoundaryContext(receipt: $0) }
            precondition(
                boundary == nil || receiptBoundary == nil || boundary == receiptBoundary,
                "A result and its receipt must name the same synchronization boundary"
            )
            self.receipt = receipt
            self.publicationState = publicationState
            self.boundary = receiptBoundary ?? boundary
        }
    }

    /// Immutable causal boundary available to application reconciliation
    /// before BigSync authorizes any terminal receipt.
    public struct PrepublicationBoundaryContext: Sendable, Equatable {
        public let accountScopeIdentifier: String
        public let replicaBindingGenerationIdentifier: String?
        public let runID: UUID
        public let consumedServerBoundaryIdentifier: String?
        public let didImportChanges: Bool
        public let committedInboundIdentities: [CommittedInboundIdentity]

        internal init(
            context: RunContext,
            consumedServerBoundaryIdentifier: String?,
            didImportChanges: Bool,
            committedInboundIdentities: [CommittedInboundIdentity] = []
        ) {
            accountScopeIdentifier = context.accountScopeIdentifier
            replicaBindingGenerationIdentifier =
                context.replicaBindingGenerationIdentifier
            runID = context.runID
            self.consumedServerBoundaryIdentifier =
                consumedServerBoundaryIdentifier
            self.didImportChanges = didImportChanges
            self.committedInboundIdentities = committedInboundIdentities
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
    public let containerIdentifier: String

    /// The sole private custom record zone owned by this synchronizer.
    ///
    /// Binding the zone at construction time makes it part of the durable
    /// client identity before backup detection, cursors, subscriptions, or
    /// migration state can be read or written.
    public private(set) var recordZoneID: CKRecordZone.ID
    
    /// Adapter wrapping a `CKDatabase`. The synchronizer will run CloudKit operations on the given database.
//    @BigSyncBackgroundActor
    public let database: CloudKitDatabaseAdapter
    
    /// Required by the synchronizer to persist some state. `UserDefaults` can be used via `UserDefaultsAdapter`.
    public let keyValueStore: KeyValueStore
    internal let accountIdentifierProvider: AccountIdentifierProvider
    internal let accountStatusProvider: AccountStatusProvider
    private let progressHandler: ProgressHandler
    internal var synchronizationWillConsumeServerChangesHandler:
        SynchronizationWillConsumeServerChangesHandler?
    internal var domainPrepublicationHandler: DomainPrepublicationHandler?
    internal var publicationFetchDeferralHandler: PublicationFetchDeferralHandler?
    internal var publicationConsumptionHandler: SynchronizationWillConsumeServerChangesHandler?
    internal var publicationConsumptionPending = false
    internal var publicationFetchDeferralEligible = false
    internal var domainPublicationScopeIdentifierProvider:
        DomainPublicationScopeIdentifierProvider?
    internal let backupDetectionBaseURL: URL?
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
    /// Every durable synchronizer value belongs to exactly one CloudKit
    /// container and database scope. By default this namespace includes the
    /// active private transport zone; callers replacing that zone may provide
    /// a stable durable-state zone identity instead.
    internal private(set) var durableStateNamespace: String
    @BigSyncBackgroundActor
    var accountScopeInvalidationHandler:
        BigSyncBackgroundWorkerConfiguration.AccountScopeInvalidationHandler?
    private var pendingAccountScopeInvalidation: (
        id: UUID,
        reason: BigSyncAccountScopeInvalidationReason
    )?
#if DEBUG
    private var allowsRecordZoneRebindingForTesting = false
#endif

    nonisolated internal static func makeDurableStateNamespace(
        identifier: String,
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope,
        recordZoneID: CKRecordZone.ID
    ) -> String {
        let fields = [
            containerIdentifier,
            identifier,
            String(databaseScope.rawValue),
            recordZoneID.ownerName,
            recordZoneID.zoneName,
        ]
        let framedIdentity = fields.map { field in
            "\(field.utf8.count):\(field)"
        }.joined(separator: "|")
        let digest = SHA256.hash(data: Data(framedIdentity.utf8))
            .map { String(format: "%02x", $0) }
            .joined()
        return "BigSyncKit.v3.\(digest)"
    }

    /// Canonical identity of one fully consumed record-zone cursor. The
    /// cursor bytes are meaningful only inside their container, database,
    /// account, zone, and replica-binding namespace, so all of those fields
    /// participate in the digest.
    nonisolated internal static func makeConsumedServerBoundaryIdentifier(
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope,
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?,
        recordZoneID: CKRecordZone.ID,
        changeFeedEpoch: Int = 0,
        cursorData: Data
    ) -> String? {
        guard !containerIdentifier.isEmpty,
              !accountScopeIdentifier.isEmpty,
              !recordZoneID.ownerName.isEmpty,
              !recordZoneID.zoneName.isEmpty,
              changeFeedEpoch >= 0,
              !cursorData.isEmpty,
              replicaBindingGenerationIdentifier?.isEmpty != true else {
            return nil
        }

        var bytes = Data()
        func append(_ value: Data) {
            var length = UInt64(value.count).bigEndian
            withUnsafeBytes(of: &length) {
                bytes.append(contentsOf: $0)
            }
            bytes.append(value)
        }
        func append(_ value: String) {
            append(Data(value.utf8))
        }

        append("BigSyncServerBoundary.v3")
        append(containerIdentifier)
        append(String(databaseScope.rawValue))
        append(accountScopeIdentifier)
        append(recordZoneID.ownerName)
        append(recordZoneID.zoneName)
        append(String(changeFeedEpoch))
        if let replicaBindingGenerationIdentifier {
            bytes.append(1)
            append(replicaBindingGenerationIdentifier)
        } else {
            bytes.append(0)
        }
        append(cursorData)
        return SHA256.hash(data: bytes).map {
            String(format: "%02x", $0)
        }.joined()
    }

    internal func durableStateKey(_ suffix: String) -> String {
        "\(durableStateNamespace).\(suffix)"
    }

    private var cloudKitAccountIdentifierKey: String {
        durableStateKey("CloudKitAccountIdentifier")
    }

    private var accountScopeLeaseKey: String {
        durableStateKey("AccountScopeLease.v1")
    }

    internal var replicaBindingStateKey: String {
        durableStateKey("ReplicaBinding.v1")
    }

    private var transientRetryStateKey: String {
        durableStateKey("TransientRetryState.v2")
    }
    internal var accountValidationRequired = true
    let accountScopeAuthorityFence = AccountScopeAuthorityFence()
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
    private var portActivationRequiresWorkerRestart = false
    private var synchronizationWaiters = [
        UUID: CheckedContinuation<SynchronizationResult, Error>
    ]()
    private var activeRunCallbackCount = 0
#if DEBUG
    /// Test-only compatibility seam for older focused fixtures that construct
    /// the transport before their fake adapter. Production clients bind their
    /// one zone in the initializer and can never rebind it.
    internal func _allowRecordZoneRebindingForTesting() {
        precondition(modelAdapterDictionary.isEmpty)
        allowsRecordZoneRebindingForTesting = true
    }

    internal var _testActiveRunCallbackCount: Int {
        activeRunCallbackCount
    }

    internal var _testSynchronizationWaiterCount: Int {
        synchronizationWaiters.count
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
    internal var outboundRecoveryID: UUID?
    /// Non-nil only for the attempt currently performing the durable
    /// change-feed migration.  Adapter state remains the source of truth for
    /// per-zone provenance; this value merely fences the orchestration.
    private var activeChangeFeedMigration: ChangeFeedMigrationState?
 
    internal let logger: Logging.Logger
    public let accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy
    internal let initialReplicaBindingAdmissionHandler:
        BigSyncBackgroundWorkerConfiguration
            .InitialReplicaBindingAdmissionHandler?
    
    /// Default number of records to send in an upload operation.
    public nonisolated static let defaultInitialBatchSize = 300
    public nonisolated static let maxBatchSize = 400 // Apple's suggestion is 400
    
    /// Initializes a newly allocated synchronizer.
    /// - Parameters:
    ///   - identifier: Identifier for the `QSCloudKitSynchronizer`.
    ///   - containerIdentifier: Identifier of the iCloud container to be used. The application must have the right entitlements to be able to access this container.
    ///   - database: Private CloudKit Database. BigSyncKit supports one
    ///     private-zone adapter per synchronizer.
    ///   - keyValueStore: Object conforming to KeyValueStore (`UserDefaultsAdapter`, for example)
    /// - Returns: Initialized synchronizer or `nil` if no iCloud container can be found with the provided identifier.
    public init(
        identifier: String,
        containerIdentifier: String,
        database: CloudKitDatabaseAdapter,
        recordZoneID: CKRecordZone.ID,
        durableStateRecordZoneID: CKRecordZone.ID? = nil,
        keyValueStore: KeyValueStore = UserDefaultsAdapter(userDefaults: UserDefaults.standard),
        compatibilityVersion: Int = 0,
        accountIdentifierProvider: AccountIdentifierProvider? = nil,
        accountStatusProvider: AccountStatusProvider? = nil,
        progressHandler: ProgressHandler? = nil,
        changeFeed: (any CloudKitChangeFeed)? = nil,
        subscriptionStore: (any CloudKitSubscriptionStore)? = nil,
        zoneStore: (any CloudKitZoneStore)? = nil,
        recordStore: (any CloudKitRecordStore)? = nil,
        backupDetectionBaseURL: URL? = nil,
        initialReplicaBindingAdmissionHandler:
            BigSyncBackgroundWorkerConfiguration
                .InitialReplicaBindingAdmissionHandler? = nil,
        accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy =
            .serverReconciliation,
        logger: Logging.Logger
    ) {
        precondition(
            database.databaseScope == .private,
            "BigSyncKit supports exactly one adapter in one private CloudKit record zone"
        )
        precondition(
            !containerIdentifier.isEmpty,
            "BigSyncKit requires an explicit CloudKit container identifier so durable state cannot collide across containers"
        )
        self.identifier = identifier
        self.containerIdentifier = containerIdentifier
        self.recordZoneID = recordZoneID
        self.database = database
        let durableStateZoneID = durableStateRecordZoneID ?? recordZoneID
        self.durableStateNamespace = Self.makeDurableStateNamespace(
            identifier: identifier,
            containerIdentifier: containerIdentifier,
            databaseScope: database.databaseScope,
            recordZoneID: durableStateZoneID
        )
        self.keyValueStore = keyValueStore
        self.compatibilityVersion = compatibilityVersion
        if let accountIdentifierProvider {
            self.accountIdentifierProvider = accountIdentifierProvider
        } else {
            self.accountIdentifierProvider = {
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
        if let accountStatusProvider {
            self.accountStatusProvider = accountStatusProvider
        } else {
            self.accountStatusProvider = {
                let configuration = CKOperation.Configuration()
                configuration.timeoutIntervalForRequest = 15
                configuration.timeoutIntervalForResource = 20
                let container = CKContainer(identifier: containerIdentifier)
                return try await container.configuredWith(
                    configuration: configuration
                ) { configuredContainer in
                    try await configuredContainer.accountStatus()
                }
            }
        }
        self.progressHandler = progressHandler ?? { _ in }
        self.backupDetectionBaseURL = backupDetectionBaseURL
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
        self.initialReplicaBindingAdmissionHandler =
            initialReplicaBindingAdmissionHandler
        self.accountReplacementPolicy = accountReplacementPolicy
        self.logger = logger
        super.init()

        accountChangeObserver = NotificationCenter.default.addObserver(
            forName: .CKAccountChanged,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            // The notification can arrive while the background actor is busy.
            // Revoke leases before queuing the durable invalidation work.
            self?.accountScopeAuthorityFence.poison()
            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                // Revoke run ownership before application invalidation can
                // suspend and allow actor reentrancy.
                accountValidationRequired = true
                cancelSynchronization()
                queueAccountScopeInvalidation(.accountChanged)
                do {
                    try await performPendingAccountScopeInvalidation()
                } catch {
                    logger.error(
                        "QSCloudKitSynchronizer >> Failed to invalidate account-scoped authority: \(error)"
                    )
                    return
                }
                // CloudKit's account-change contract requires a fresh account
                // validation. This entry point performs no record/zone work
                // until the new account identity has been established.
                if modelAdapterDictionary.count == 1 {
                    beginSynchronization()
                }
            }
        }
        
        do {
            let result = try BackupDetection.run(
                store: keyValueStore,
                namespace: durableStateNamespace,
                sharedSentinelBaseURL: backupDetectionBaseURL
            )
            refreshBackupRestoreRequirement()
            if result == .restoredFromBackup || backupRestoreDetected {
                accountScopeAuthorityFence.poison()
                clearDeviceIdentifier()
                try invalidateAccountScopeLeaseDurably()
                queueAccountScopeInvalidation(.restoreDetected)
            }
        } catch {
            accountScopeAuthorityFence.poison()
            backupDetectionError = error
            logger.error("QSCloudKitSynchronizer >> Backup detection failed: \(error)")
        }
        
//        Task {
//            ChangeRequestProcessor.shared.logger = logger
//        }
    }

#if DEBUG
    @BigSyncBackgroundActor
    internal var processKillCheckpointHandler:
        BigSyncBackgroundWorkerConfiguration.ProcessKillCheckpointHandler?

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
    ) throws {
        var state = storedZoneLifecycleState(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        ) ?? baseZoneLifecycleState(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
        state["established"] = true
        try persistZoneLifecycleState(
            state,
            zoneID: zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
    }

    internal func markConfiguredZoneTerminal(
        _ zoneID: CKRecordZone.ID,
        kind: CloudKitZoneDeletionKind,
        accountScopeIdentifier: String,
        observedAt: Date = Date()
    ) throws {
        // A database-history deletion event is itself durable proof that the
        // zone existed. All deletion kinds are terminal for production sync:
        // preserve the concrete reason and never silently recreate the zone
        // from possibly stale local objects.
        var state = baseZoneLifecycleState(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
        state["established"] = true
        state["terminal"] = kind.rawValue
        state["terminalObservedAt"] = observedAt
        try persistZoneLifecycleState(
            state,
            zoneID: zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
    }

    internal func configuredZoneIsEstablished(_ zoneID: CKRecordZone.ID) -> Bool {
        guard let context = activeRunContext else { return false }
        return storedZoneLifecycleState(
            zoneID,
            accountScopeIdentifier: context.accountScopeIdentifier
        )?["established"] as? Bool == true
    }

    internal func configuredZoneIsTerminal(_ zoneID: CKRecordZone.ID) -> Bool {
        configuredZoneTerminalState(zoneID) != nil
    }

    internal func configuredZoneTerminalState(
        _ zoneID: CKRecordZone.ID
    ) -> CloudKitTerminalZoneState? {
        guard let context = activeRunContext,
              let state = storedZoneLifecycleState(
                zoneID,
                accountScopeIdentifier: context.accountScopeIdentifier
              ),
              let rawKind = state["terminal"] as? String else {
            return nil
        }
        let deletionKind = CloudKitZoneDeletionKind(rawValue: rawKind)
            ?? .unknown
        let observedAt = state["terminalObservedAt"] as? Date ?? .distantPast
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
    ) throws {
        guard var state = storedZoneLifecycleState(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        ) else { return }
        state.removeValue(forKey: "terminal")
        state.removeValue(forKey: "terminalObservedAt")
        try persistZoneLifecycleState(
            state,
            zoneID: zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
    }

    private func zoneLifecycleStateKey(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) -> String {
        durableStateKey(
            "ZoneLifecycle.v3.\(accountScopeIdentifier).\(zoneID.ownerName).\(zoneID.zoneName)"
        )
    }

    private func baseZoneLifecycleState(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) -> [String: Any] {
        [
            "version": 3,
            "accountScopeIdentifier": accountScopeIdentifier,
            "zoneOwnerName": zoneID.ownerName,
            "zoneName": zoneID.zoneName,
        ]
    }

    private func storedZoneLifecycleState(
        _ zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) -> [String: Any]? {
        guard let state = keyValueStore.object(forKey: zoneLifecycleStateKey(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )) as? [String: Any],
        (state["version"] as? NSNumber)?.intValue == 3,
        state["accountScopeIdentifier"] as? String == accountScopeIdentifier,
        state["zoneOwnerName"] as? String == zoneID.ownerName,
        state["zoneName"] as? String == zoneID.zoneName else {
            return nil
        }
        return state
    }

    private func persistZoneLifecycleState(
        _ state: [String: Any],
        zoneID: CKRecordZone.ID,
        accountScopeIdentifier: String
    ) throws {
        let key = zoneLifecycleStateKey(
            zoneID,
            accountScopeIdentifier: accountScopeIdentifier
        )
        let previousValue = keyValueStore.object(forKey: key)
        keyValueStore.set(value: state, forKey: key)
        guard keyValueStore.synchronize?() == true,
              let persisted = storedZoneLifecycleState(
                zoneID,
                accountScopeIdentifier: accountScopeIdentifier
              ),
              NSDictionary(dictionary: persisted).isEqual(to: state) else {
            if let previousValue {
                keyValueStore.set(value: previousValue, forKey: key)
            } else {
                keyValueStore.removeObject(forKey: key)
            }
            _ = keyValueStore.synchronize?()
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
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
            // This is an optimization tag, not durable client identity. Keeping
            // it across launches lets a restored backup suppress newer records
            // written by the original installation while still advancing its
            // cursor. A process-scoped value filters only the current process's
            // immediate post-upload echo.
            _deviceIdentifier = UUID().uuidString
        }
        return _deviceIdentifier
    }
    
    internal func clearDeviceIdentifier() {
        // The modern transport's echo tag is deliberately process-scoped.
        // Do not mutate the obsolete persisted key: it is never read, and a
        // best-effort deletion must not introduce a durable-state failure into
        // an otherwise unrelated recovery transaction.
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
        try resetDatabaseToken()
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
    private func refreshBackupRestoreRequirement() {
        backupRestoreDetected = BackupDetection.restoreResetIsRequired(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: backupDetectionBaseURL
        )
    }

    @BigSyncBackgroundActor
    private func prepareRestoredBackupRecoveryIfNeeded(
        context: RunContext
    ) async throws {
        refreshBackupRestoreRequirement()
        guard backupRestoreDetected else {
            return
        }
        guard let restoreEventIdentifier = BackupDetection
            .restoreResetEventIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: backupDetectionBaseURL
        ) else {
            // An unreadable event remains restore evidence, but cannot safely
            // identify an idempotent recovery epoch.
            throw CocoaError(.fileReadCorruptFile)
        }

        try await revalidateRunContext(context)
        if let completed = try requireStoredChangeFeedMigrationState(for: context),
           completed.phase == .completed,
           completed.backupRestoreEventIdentifier == restoreEventIdentifier {
            try completeRestoredBackupRecoveryIfNeeded(
                expectedEventIdentifier: restoreEventIdentifier
            )
            return
        }
        try requestChangeFeedRecovery(
            context: context,
            mode: .backupRestore,
            backupRestoreEventIdentifier: restoreEventIdentifier
        )
        guard let recovery = try requireStoredChangeFeedMigrationState(for: context),
              recovery.phase != .completed,
              recovery.backupRestoreEventIdentifier == restoreEventIdentifier,
              recovery.mode == .backupRestore || recovery.mode == .encryptedDataReset else {
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
        // The exact event and its recovery envelope are durable before clearing
        // checkpoints. Adapter preparation owns copied-journal retirement.
        clearDeviceIdentifier()
        try resetDatabaseToken()
        resetActiveTokens()
        try clearAllStoredSubscriptionIDs()
        clearPersistedTransientRetryState()
        lastDatabaseChangesEmptyAt = nil
        if recovery.mode == .backupRestore {
            // A copied terminal marker is not fresh server evidence. A stronger
            // encrypted reset observed by this restore, however, retains its
            // fence until its journal-backed re-upload actually completes.
            try clearConfiguredZoneTerminal(
                recordZoneID,
                accountScopeIdentifier: context.accountScopeIdentifier
            )
        }
        try await revalidateRunContext(context)
    }

    /// Retries construction-time backup detection before this attempt obtains
    /// an account lease or creates a run context. A restored process must first
    /// revoke the copied lease and let the application fence its copied domain
    /// authority; only a subsequent fresh account validation may continue.
    @BigSyncBackgroundActor
    private func retryBackupDetectionBeforeAccountValidationIfNeeded(
        attemptID: UUID
    ) async throws {
        guard backupDetectionError != nil else { return }
        let result = try BackupDetection.run(
            store: keyValueStore,
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: backupDetectionBaseURL
        )
        refreshBackupRestoreRequirement()
        guard result == .restoredFromBackup || backupRestoreDetected else {
            backupDetectionError = nil
            return
        }

        clearDeviceIdentifier()
        try invalidateAccountScopeLeaseDurably()
        accountValidationRequired = true
        queueAccountScopeInvalidation(.restoreDetected)
        try await performPendingAccountScopeInvalidation()
        try checkSynchronizationAttempt(attemptID)
        // Clear only after both authority barriers completed. A failed durable
        // invalidation or cancelled handler is retried idempotently next time.
        backupDetectionError = nil
    }

    @BigSyncBackgroundActor
    private func completeRestoredBackupRecoveryIfNeeded(
        expectedEventIdentifier: String? = nil
    ) throws {
        refreshBackupRestoreRequirement()
        guard backupRestoreDetected else { return }
        guard let expectedEventIdentifier = try expectedEventIdentifier
            ?? activeChangeFeedMigration?.backupRestoreEventIdentifier
            ?? activeRunContext.flatMap({ context in
                try requireStoredChangeFeedMigrationState(for: context)?
                    .backupRestoreEventIdentifier
            }) else {
            throw CocoaError(.fileReadCorruptFile)
        }
        try BackupDetection.markRestoreResetCompleted(
            namespace: durableStateNamespace,
            expectedEventIdentifier: expectedEventIdentifier,
            sharedSentinelBaseURL: backupDetectionBaseURL
        )
        refreshBackupRestoreRequirement()
        guard !backupRestoreDetected else {
            throw CocoaError(.fileWriteUnknown)
        }
    }

    // MARK: - Public

    /// Synchronize data with CloudKit.
    /// - Parameter onFailure: Block that receives an error if the synchronization stopped due to a failure. Could be a `SyncError`, `CKError`, or any other error found during synchronization.
    @BigSyncBackgroundActor
    @objc public func beginSynchronization() { //onFailure: ((Error) -> ())?) {
        precondition(
            modelAdapterDictionary.count == 1,
            "BigSyncKit requires exactly one model adapter before synchronization"
        )
        guard !portActivationRequiresWorkerRestart else { return }
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
        activeRunContext = nil
        publicationConsumptionPending = false
        publicationFetchDeferralEligible = false
        activeReceiptAuthorizationID = nil
        reservedReceiptAuthorizationID = nil

        synchronizationTask?.cancel()
        // Synchronization is deferrable user-data work, but it must still make
        // progress under sustained system load. `.background` cooperative
        // tasks can be starved indefinitely; utility QoS preserves energy
        // intent without making CloudKit work user-interactive.
        synchronizationTask = Task(priority: .utility) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            do {
                await waitForRunCallbacksToFinish()
                try checkSynchronizationAttempt(attemptID)
                // Fail before the first CloudKit/account request if durable
                // local state is corrupt, unreadable, or carries an
                // uncommitted mutation from an earlier attempt. Treating that
                // state as empty could advance a fresh cursor over an old
                // namespace.
                if let durableStore = keyValueStore as? any DurableKeyValueStore {
                    try durableStore.validateDurability()
                }
                try await retryBackupDetectionBeforeAccountValidationIfNeeded(
                    attemptID: attemptID
                )
                try await performPendingAccountScopeInvalidation()
                try await validateAccountAvailabilityIfNeeded(
                    attemptID: attemptID
                )
                let accountIdentifier = try await validateSynchronizationAccount()
                reportProgress("account-identity-validated")
                let replicaBindingGenerationIdentifier = try
                    activeReplicaBindingGenerationIdentifierForRun(
                    accountScopeIdentifier: Self.accountScopeIdentifier(
                        for: accountIdentifier
                    )
                )
                guard let validatedLease = try readAccountScopeLeaseDurably().lease,
                      validatedLease.accountScopeIdentifier == Self.accountScopeIdentifier(for: accountIdentifier) else {
                    throw BigSyncAccountScopeLeaseError.unavailable
                }
                let runID = await changeRequestProcessor.beginRun()
                synchronizationRunID = runID
                let context = RunContext(
                    attemptID: attemptID,
                    runID: runID,
                    accountIdentifier: accountIdentifier,
                    accountScopeIdentifier: Self.accountScopeIdentifier(
                        for: accountIdentifier
                    ),
                    replicaBindingGenerationIdentifier:
                        replicaBindingGenerationIdentifier,
                    accountInvalidationGeneration: validatedLease.invalidationGeneration
                )
                activeRunContext = context
                for adapter in modelAdapters {
                    try await adapter.activateTransportNamespace(
                        containerIdentifier: containerIdentifier,
                        databaseScope: database.databaseScope
                    )
                    try checkRunContext(context)
                    try await adapter.activateReplicaBinding(
                        accountScopeIdentifier:
                            context.accountScopeIdentifier,
                        replicaBindingGenerationIdentifier:
                            context.replicaBindingGenerationIdentifier
                    )
                    try checkRunContext(context)
                }
                // A verified restored-backup event invalidates copied sync-only
                // lifecycle markers. Establish its durable reconciliation
                // request before consulting those stale markers. A deletion
                // observed by the fresh nil-token feed will fence the zone
                // again under current server truth.
                try await prepareRestoredBackupRecoveryIfNeeded(context: context)
                let isRestoredBackupRecovery = backupRestoreDetected
                if !isRestoredBackupRecovery,
                   let terminalState = configuredZoneTerminalState(recordZoneID),
                   try !hasPendingEncryptedDataResetRecovery(context: context) {
                    let terminalZoneID = CKRecordZone.ID(
                        zoneName: terminalState.zoneName,
                        ownerName: terminalState.ownerName
                    )
                    if terminalState.deletionKind == .encryptedDataReset {
                        // Repair a process death or durability loss between
                        // observing CloudKit's encrypted reset and publishing
                        // its recovery envelope. User data and journals remain
                        // untouched; the reset begins with a fresh epoch.
                        try requestChangeFeedRecovery(
                            context: context,
                            mode: .encryptedDataReset
                        )
                    } else {
                        throw ChangeFeedMigrationError
                            .establishedZoneUnavailable(
                                terminalZoneID,
                                terminalState.deletionKind
                            )
                    }
                }
                try await waitForPersistedTransientRetryIfNeeded(
                    context: context
                )
                try recordSyncHealth(.syncing, context: context)
                // Subscription identifiers are account-scoped sync metadata.
                // Account validation clears them after an iCloud account
                // change, so ensure the current account has a subscription as
                // part of every attempt. The stored-ID fast path performs no
                // CloudKit request during ordinary synchronization.
                try await subscribeForChangesInDatabase()
                reportProgress("subscription-completed")
                try await revalidateRunContext(context)
                try await beginChangeFeedMigrationIfNeeded(context: context)
                try await revalidateRunContext(context)
                reportProgress("change-feed-migration-ready")
                // Migration preparation must precede setup. In recovery modes it
                // activates Realm provenance (and, for backup restore, retires
                // the copied outbox) before setup can scan target objects.
                for adapter in modelAdapters {
                    await adapter.waitForCancellation()
                    try await adapter.unsetCancellation()
                    try checkRunContext(context)
                }
                reportProgress("adapters-ready")
                // Legacy integrations keep their eager invalidation contract.
                if publicationFetchDeferralHandler == nil || publicationConsumptionHandler == nil {
                    try clearDurablePublicationEvidence()
                }
                publicationConsumptionPending = publicationConsumptionHandler != nil
                if let handler =
                    synchronizationWillConsumeServerChangesHandler {
                    try await handler(
                        SynchronizationBoundaryContext(context: context)
                    )
                    try await revalidateRunContext(context)
                }
                if publicationConsumptionHandler != nil {
                    var canDefer = false
                    if let handler = publicationFetchDeferralHandler,
                       try requireStoredChangeFeedMigrationState(for: context)?.phase == .completed,
                       let evidence = try publicationEvidenceForUnconsumedFetch(context: context) {
                        var blockers = [DomainBlocker]()
                        for adapter in modelAdapters {
                            blockers.append(contentsOf: try await adapter.semanticPublicationBlockers())
                            try await revalidateRunContext(context)
                        }
                        if blockers.isEmpty { canDefer = try await handler(evidence) }
                        try await revalidateRunContext(context)
                    }
                    publicationFetchDeferralEligible = canDefer
                    if !canDefer { try await beginPublicationConsumptionIfNeeded() }
                }
                try Task.checkCancellation()
                await performSynchronization()
            } catch {
                guard synchronizationAttemptID == attemptID else { return }
                await failSynchronization(error: error)
            }
        }
    }

    /// Runs before relevant target pages, lifecycle loss, malformed results,
    /// or semantic/non-network failure. Eligible irrelevant drains retain their
    /// candidate, but only the terminal checks can mint a new receipt.
    internal func beginPublicationConsumptionIfNeeded() async throws {
        guard publicationConsumptionPending, let context = activeRunContext else { return }
        try checkRunContext(context)
        publicationConsumptionPending = false
        publicationFetchDeferralEligible = false
        if let handler = publicationConsumptionHandler {
            try await handler(SynchronizationBoundaryContext(context: context))
            try await revalidateRunContext(context)
        }
        try clearDurablePublicationEvidence()
    }

    /// Starts synchronization, coalesces with any in-flight request, and returns
    /// only after the full fetch/import/upload drain has finished.
    @BigSyncBackgroundActor
    public func synchronize() async throws -> SynchronizationResult {
        guard !portActivationRequiresWorkerRestart else {
            throw BigSyncCloudAccountPortError.workerRestartRequired
        }
        precondition(
            modelAdapterDictionary.count == 1,
            "BigSyncKit requires exactly one model adapter before synchronization"
        )
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
              !accountScopeAuthorityFence.requiresGenerationRotation,
              !cancelSync else {
            throw CancellationError()
        }
        if let expectedGeneration = context.accountInvalidationGeneration {
            guard let lease = try readAccountScopeLeaseDurably().lease,
                  lease.accountScopeIdentifier == context.accountScopeIdentifier,
                  lease.invalidationGeneration == expectedGeneration else {
                throw CancellationError()
            }
        }
        if let expectedBinding =
            context.replicaBindingGenerationIdentifier {
            guard let binding = try BigSyncReplicaBindingStateStore.load(
                store: keyValueStore,
                key: replicaBindingStateKey
            ), binding.pendingPort == nil,
            binding.activeGenerationIdentifier == expectedBinding,
            binding.activeAccountScopeIdentifier
                == context.accountScopeIdentifier else {
                throw CancellationError()
            }
        }
    }

    /// Revalidates application work performed from the pre-inbound callback
    /// against this synchronizer's exact active run and durable replica
    /// binding. This intentionally performs no network request: the enclosing
    /// synchronization revalidates the CloudKit account after the callback,
    /// while callers use this check on both sides of their own suspension.
    internal func validateBoundaryContext(
        _ context: SynchronizationBoundaryContext
    ) throws {
        guard let activeRunContext,
              activeRunContext.runID == context.runID,
              activeRunContext.accountScopeIdentifier
                == context.accountScopeIdentifier,
              activeRunContext.replicaBindingGenerationIdentifier
                == context.replicaBindingGenerationIdentifier else {
            throw CancellationError()
        }
        try checkRunContext(activeRunContext)
    }

    /// Revalidates application work performed from the terminal
    /// prepublication callback. See the synchronization-boundary overload for
    /// the account and suspension contract.
    internal func validateBoundaryContext(
        _ context: PrepublicationBoundaryContext
    ) throws {
        guard let activeRunContext,
              activeRunContext.runID == context.runID,
              activeRunContext.accountScopeIdentifier
                == context.accountScopeIdentifier,
              activeRunContext.replicaBindingGenerationIdentifier
                == context.replicaBindingGenerationIdentifier else {
            throw CancellationError()
        }
        try checkRunContext(activeRunContext)
    }

    /// Revalidates the exact CloudKit account as well as the active run and
    /// durable replica binding. Application callbacks use this immediately
    /// before an irreversible local mutation after suspended CloudKit work.
    internal func revalidateBoundaryContext(
        _ context: SynchronizationBoundaryContext
    ) async throws {
        try validateBoundaryContext(context)
        guard let activeRunContext else {
            throw CancellationError()
        }
        try await revalidateRunContext(activeRunContext)
        try validateBoundaryContext(context)
    }

    /// Terminal-prepublication counterpart of the synchronization-boundary
    /// exact-account revalidation.
    internal func revalidateBoundaryContext(
        _ context: PrepublicationBoundaryContext
    ) async throws {
        try validateBoundaryContext(context)
        guard let activeRunContext else {
            throw CancellationError()
        }
        try await revalidateRunContext(activeRunContext)
        try validateBoundaryContext(context)
    }

    /// Revalidates the account-validation attempt that owns initial dataset
    /// admission. A context manually constructed outside BigSync lacks this
    /// private authority and is rejected.
    internal func revalidateInitialReplicaBindingContext(
        _ context: BigSyncInitialReplicaBindingContext
    ) async throws {
        guard let expectedAccountIdentifier = context.accountIdentifier,
              let validationAttemptID = context.validationAttemptID else {
            throw CancellationError()
        }
        try checkAccountValidationAttempt(validationAttemptID)
        try validatePendingReplicaBinding(context)
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
        try validatePendingReplicaBinding(context)
        guard currentAccountIdentifier == expectedAccountIdentifier,
              Self.accountScopeIdentifier(for: currentAccountIdentifier)
                == context.accountScopeIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }

    private func validatePendingReplicaBinding(
        _ context: BigSyncInitialReplicaBindingContext
    ) throws {
        guard let binding = try BigSyncReplicaBindingStateStore.load(
            store: keyValueStore,
            key: replicaBindingStateKey
        ), binding.mutationGenerationIdentifier
            == context.replicaBindingGenerationIdentifier else {
            throw CancellationError()
        }
        if let pending = binding.pendingPort {
            guard pending.bindingGenerationIdentifier
                    == context.replicaBindingGenerationIdentifier,
                  pending.destinationAccountScopeIdentifier
                    == context.accountScopeIdentifier else {
                throw CancellationError()
            }
        } else if let activeAccountScopeIdentifier =
                    binding.activeAccountScopeIdentifier {
            guard activeAccountScopeIdentifier
                    == context.accountScopeIdentifier else {
                throw CancellationError()
            }
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

    @BigSyncBackgroundActor
    private func validateAccountAvailabilityIfNeeded(
        attemptID: UUID
    ) async throws {
        guard accountValidationRequired
                || cancelledDueToUnauthentication else { return }
        let status = try await accountStatusProvider()
        try checkAccountValidationAttempt(attemptID)
        switch status {
        case .available:
            return
        case .noAccount, .restricted:
            throw CKError(.notAuthenticated)
        case .couldNotDetermine, .temporarilyUnavailable:
            throw CKError(.accountTemporarilyUnavailable)
        @unknown default:
            throw CKError(.accountTemporarilyUnavailable)
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
        // Every replacement policy must distinguish absent account history
        // from an unreadable or malformed stored marker before account I/O.
        let previousAccountIdentifier = try durableAccountIdentifier()
        let preparedReplicaBinding: BigSyncReplicaBindingSnapshot?
        if accountReplacementPolicy.usesDatasetReplicaBinding {
            var binding = try prepareReplicaBindingState()
            // An existing installation can have an account marker but no
            // replica binding yet. Record that account as the dataset owner
            // before asking which account is signed in now. Otherwise an
            // account switch could silently claim the existing local dataset.
            if binding.datasetOwnerAccountScopeIdentifier == nil,
               let previousAccountIdentifier {
                binding = try BigSyncReplicaBindingStateStore
                    .bindInitialAccount(
                        Self.accountScopeIdentifier(
                            for: previousAccountIdentifier
                        ),
                        store: keyValueStore,
                        key: replicaBindingStateKey
                    )
            }
            preparedReplicaBinding = binding
        } else {
            preparedReplicaBinding = nil
        }
        let validationAttemptID = synchronizationAttemptID
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
        var confirmedAccountIdentifier = currentAccountIdentifier
        let currentAccountScopeIdentifier = Self.accountScopeIdentifier(
            for: currentAccountIdentifier
        )
        var didReplaceAccount: Bool
        if let preparedReplicaBinding {
            didReplaceAccount = preparedReplicaBinding
                .datasetOwnerAccountScopeIdentifier
                .map { $0 != currentAccountScopeIdentifier }
                ?? false
        } else {
            didReplaceAccount = previousAccountIdentifier
                .map { $0 != currentAccountIdentifier }
                ?? false
        }
        var requiresLocalDatasetRebootstrap = false
        if didReplaceAccount {
            if accountReplacementPolicy == .requireExplicitDatasetPort {
                logger.info(
                    "QSCloudKitSynchronizer >> CloudKit account changed; an explicit local-dataset port is required"
                )
            } else {
                logger.info(
                    "QSCloudKitSynchronizer >> CloudKit account changed; rebuilding local sync metadata for the new account"
                )
            }
            // Confirm the provider still reports the same replacement account
            // before either publishing a port gate or preparing reconciliation.
            confirmedAccountIdentifier = try await accountIdentifierProvider()
            try checkAccountValidationAttempt(validationAttemptID)
        }
        guard confirmedAccountIdentifier == currentAccountIdentifier else {
            accountValidationRequired = true
            accountScopeAuthorityFence.poison()
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        if accountReplacementPolicy == .requireExplicitDatasetPort {
            let currentScope = currentAccountScopeIdentifier
            if let pendingPortRequirement = try
                pendingCloudAccountPortRequirement() {
                accountValidationRequired = true
                try await invalidateAccountScope(.accountReplaced)
                throw BigSyncCloudAccountPortError.required(
                    pendingPortRequirement
                )
            }
            if didReplaceAccount {
                guard let sourceAccountScopeIdentifier =
                        preparedReplicaBinding?
                            .datasetOwnerAccountScopeIdentifier else {
                    throw BigSyncReplicaBindingError.corrupt
                }
                let requirement = try BigSyncReplicaBindingStateStore
                    .requirePort(
                        sourceAccountScopeIdentifier:
                            sourceAccountScopeIdentifier,
                        destinationAccountScopeIdentifier: currentScope,
                        store: keyValueStore,
                        key: replicaBindingStateKey
                    )
                accountValidationRequired = true
                try await invalidateAccountScope(.accountReplaced)
                throw BigSyncCloudAccountPortError.required(requirement)
            }
            try await admitInitialReplicaBindingIfNeeded(
                accountIdentifier: confirmedAccountIdentifier,
                accountScopeIdentifier: currentScope,
                validationAttemptID: validationAttemptID
            )
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount(
                currentScope,
                store: keyValueStore,
                key: replicaBindingStateKey
            )
        } else if accountReplacementPolicy == .localDatasetRebootstrap {
            let currentScope = currentAccountScopeIdentifier
            guard var binding = try BigSyncReplicaBindingStateStore.load(
                store: keyValueStore,
                key: replicaBindingStateKey
            ) else {
                throw BigSyncReplicaBindingError.corrupt
            }

            if let pending = binding.pendingPort {
                if currentScope == pending.sourceAccountScopeIdentifier {
                    // Persist the exact retiring generation before cancelPort
                    // erases it. Pending-row rebasing resumes from this envelope.
                    try requestReplicaJournalHandoff(
                        from: pending.bindingGenerationIdentifier,
                        retiringAccountScopeIdentifier:
                            pending.destinationAccountScopeIdentifier,
                        to: binding.activeGenerationIdentifier,
                        accountIdentifier: confirmedAccountIdentifier,
                        accountScopeIdentifier: currentScope,
                        validationAttemptID: validationAttemptID
                    )
                    binding = try BigSyncReplicaBindingStateStore.cancelPort(
                        pending,
                        store: keyValueStore,
                        key: replicaBindingStateKey
                    )
                    requiresLocalDatasetRebootstrap = true
                } else if currentScope
                            == pending.destinationAccountScopeIdentifier {
                    try requestReplicaJournalHandoff(
                        from: binding.activeGenerationIdentifier,
                        retiringAccountScopeIdentifier:
                            pending.sourceAccountScopeIdentifier,
                        to: pending.bindingGenerationIdentifier,
                        accountIdentifier: confirmedAccountIdentifier,
                        accountScopeIdentifier: currentScope,
                        validationAttemptID: validationAttemptID
                    )
                    try await admitReplicaBinding(
                        accountIdentifier: confirmedAccountIdentifier,
                        accountScopeIdentifier: currentScope,
                        generationIdentifier:
                            pending.bindingGenerationIdentifier,
                        expectedBinding: binding,
                        validationAttemptID: validationAttemptID
                    )
                    binding = try BigSyncReplicaBindingStateStore.activatePort(
                        pending,
                        store: keyValueStore,
                        key: replicaBindingStateKey
                    )
                    requiresLocalDatasetRebootstrap = true
                } else {
                    throw BigSyncCloudAccountPortError.required(pending)
                }
            }

            if let sourceScope = binding.datasetOwnerAccountScopeIdentifier,
               sourceScope != currentScope {
                guard didReplaceAccount else {
                    throw BigSyncReplicaBindingError.accountMismatch
                }
                let pending = try BigSyncReplicaBindingStateStore.requirePort(
                    sourceAccountScopeIdentifier: sourceScope,
                    destinationAccountScopeIdentifier: currentScope,
                    store: keyValueStore,
                    key: replicaBindingStateKey
                )
                binding = try BigSyncReplicaBindingStateStore.load(
                    store: keyValueStore,
                    key: replicaBindingStateKey
                ) ?? binding
                try requestReplicaJournalHandoff(
                    from: binding.activeGenerationIdentifier,
                    retiringAccountScopeIdentifier:
                        pending.sourceAccountScopeIdentifier,
                    to: pending.bindingGenerationIdentifier,
                    accountIdentifier: confirmedAccountIdentifier,
                    accountScopeIdentifier: currentScope,
                    validationAttemptID: validationAttemptID
                )
                try await admitReplicaBinding(
                    accountIdentifier: confirmedAccountIdentifier,
                    accountScopeIdentifier: currentScope,
                    generationIdentifier:
                        pending.bindingGenerationIdentifier,
                    expectedBinding: binding,
                    validationAttemptID: validationAttemptID
                )
                binding = try BigSyncReplicaBindingStateStore.activatePort(
                    pending,
                    store: keyValueStore,
                    key: replicaBindingStateKey
                )
                requiresLocalDatasetRebootstrap = true
            } else if binding.activeAccountScopeIdentifier == nil {
                try await admitInitialReplicaBindingIfNeeded(
                    accountIdentifier: confirmedAccountIdentifier,
                    accountScopeIdentifier: currentScope,
                    validationAttemptID: validationAttemptID
                )
                binding = try BigSyncReplicaBindingStateStore
                    .bindInitialAccount(
                        currentScope,
                        store: keyValueStore,
                        key: replicaBindingStateKey
                    )
            } else if previousAccountIdentifier.map({
                $0 != confirmedAccountIdentifier
            }) == true {
                // Resume a process death after binding activation but before
                // the destination recovery envelope was published.
                requiresLocalDatasetRebootstrap = true
            }
            guard binding.activeAccountScopeIdentifier == currentScope,
                  binding.pendingPort == nil else {
                throw BigSyncReplicaBindingError.accountMismatch
            }
        }
        if didReplaceAccount || requiresLocalDatasetRebootstrap {
            accountValidationRequired = true
            try await invalidateAccountScope(.accountReplaced)
            try checkAccountValidationAttempt(validationAttemptID)
            let currentScope = currentAccountScopeIdentifier
            let activeBindingGenerationIdentifier = try
                activeReplicaBindingGenerationIdentifierForRun(
                accountScopeIdentifier: currentScope
            )
            let recoveryContext = RunContext(
                attemptID: validationAttemptID,
                runID: synchronizationRunID,
                accountIdentifier: confirmedAccountIdentifier,
                accountScopeIdentifier: currentScope,
                replicaBindingGenerationIdentifier:
                    activeBindingGenerationIdentifier
            )
            // The new account's recovery envelope is the durable hand-off.
            // Do not discard the old account's cursors, subscriptions, or
            // retry state until that hand-off is readable after a restart.
            try requestChangeFeedRecovery(
                context: recoveryContext,
                mode: requiresLocalDatasetRebootstrap
                    ? .localDatasetRebootstrap
                    : .serverReconciliation
            )
            changeRequestProcessor.reset()
            try resetDatabaseToken()
            resetActiveTokens()
            try clearAllStoredSubscriptionIDs()
            clearPersistedTransientRetryState()
        }
        try establishAccountScopeLeaseDurably(
            accountIdentifier: confirmedAccountIdentifier,
            forceInvalidation:
                didReplaceAccount || requiresLocalDatasetRebootstrap
                    || accountScopeAuthorityFence.requiresGenerationRotation
        )
        // Publish the new account identity only after its server-first rebuild
        // still sees the old account and idempotently requests the same reset;
        // it can never observe a new account paired with an old completed
        // migration and rediscover retained user data as fresh uploads.
        try persistAccountIdentifier(confirmedAccountIdentifier)
        accountValidationRequired = false
        cancelSync = false
        cancelledDueToUnauthentication = false
        accountScopeAuthorityFence.clear()
        return confirmedAccountIdentifier
    }

    /// Resolves application-specific dataset identity before BigSync claims an
    /// initial account. The callback may suspend for Realm and CloudKit work,
    /// so the exact attempt, account, and binding generation are all sampled
    /// again before account publication can continue.
    private func admitInitialReplicaBindingIfNeeded(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID
    ) async throws {
        guard let expectedBinding = try BigSyncReplicaBindingStateStore.load(
            store: keyValueStore,
            key: replicaBindingStateKey
        ) else {
            throw BigSyncReplicaBindingError.corrupt
        }
        guard expectedBinding.pendingPort == nil else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        guard expectedBinding.activeAccountScopeIdentifier == nil else {
            return
        }
        try await admitReplicaBinding(
            accountIdentifier: accountIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            generationIdentifier:
                expectedBinding.activeGenerationIdentifier,
            expectedBinding: expectedBinding,
            validationAttemptID: validationAttemptID
        )
    }

    private func admitReplicaBinding(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        generationIdentifier: String,
        expectedBinding: BigSyncReplicaBindingSnapshot,
        validationAttemptID: UUID
    ) async throws {
        guard let handler = initialReplicaBindingAdmissionHandler else {
            throw BigSyncCloudAccountPortError
                .initialDatasetAdmissionUnavailable
        }

        try await handler(BigSyncInitialReplicaBindingContext(
            accountIdentifier: accountIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                generationIdentifier,
            validationAttemptID: validationAttemptID
        ))
        try checkAccountValidationAttempt(validationAttemptID)
        let revalidatedAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
        guard revalidatedAccountIdentifier == accountIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        guard try BigSyncReplicaBindingStateStore.load(
            store: keyValueStore,
            key: replicaBindingStateKey
        ) == expectedBinding else {
            throw CancellationError()
        }
    }

    private func durableAccountIdentifier() throws -> String? {
        guard let value = try keyValueStore.bigSyncDurableObject(
            forKey: cloudKitAccountIdentifierKey
        ) else {
            return nil
        }
        guard let value = value as? String, !value.isEmpty else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        return value
    }

    private func persistAccountIdentifier(_ identifier: String) throws {
        guard !identifier.isEmpty else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        // Routine validation of the same readable marker needs no durable
        // rewrite. Re-read here rather than trusting the pre-await snapshot.
        guard try durableAccountIdentifier() != identifier else { return }
        try keyValueStore.bigSyncSetDurably(
            value: identifier,
            forKey: cloudKitAccountIdentifierKey
        )
        guard try durableAccountIdentifier() == identifier else {
            throw DurableKeyValueStoreError.mutationNotDurable
        }
    }

    /// Returns the durable account-port gate, if one is active.
    @BigSyncBackgroundActor
    public func pendingCloudAccountPortRequirement() throws
        -> BigSyncCloudAccountPortRequirement? {
        try BigSyncReplicaBindingStateStore.load(
            store: keyValueStore,
            key: replicaBindingStateKey
        )?.pendingPort
    }

    /// Publishes an exact destination binding for clients that implement the
    /// optional explicit-transfer policy outside an ordinary sync run.
    @BigSyncBackgroundActor
    public func activateCloudAccountPort(
        _ expected: BigSyncCloudAccountPortRequirement
    ) async throws {
        guard accountReplacementPolicy == .requireExplicitDatasetPort,
              !syncing,
              !synchronizationDrainIsActive,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let attemptID = synchronizationAttemptID
        let accountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(attemptID)
        guard Self.accountScopeIdentifier(for: accountIdentifier)
                == expected.destinationAccountScopeIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let confirmedAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(attemptID)
        guard confirmedAccountIdentifier == accountIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }

        // Activation may commit before the durable store reports failure.
        // Fence this worker before the first fallible publication, not only
        // after success. Retry/reconstruction must reload the actual binding;
        // an uncertain write must never resume the old configured worker.
        accountValidationRequired = true
        cancelSync = true
        portActivationRequiresWorkerRestart = true
        _ = try BigSyncReplicaBindingStateStore.activatePort(
            expected,
            store: keyValueStore,
            key: replicaBindingStateKey
        )
        try persistAccountIdentifier(confirmedAccountIdentifier)
    }

    /// Stops an ordinary journal wakeup at the durable replica-binding gate.
    /// Account-change notifications deliberately call `beginSynchronization`
    /// directly so they can revalidate the current account while preserving
    /// the first pending transition. Local writes use this helper through the
    /// model-adapter delegate and never perform doomed account or CloudKit I/O.
    @discardableResult
    internal func reportPendingCloudAccountPortIfNeeded() -> Bool {
        guard accountReplacementPolicy == .requireExplicitDatasetPort else {
            return false
        }
        do {
            guard let requirement = try
                    pendingCloudAccountPortRequirement() else {
                return false
            }
            postNotification(
                .SynchronizerDidFailToSynchronize,
                userInfo: [
                    cloudKitSynchronizerErrorKey:
                        BigSyncCloudAccountPortError.required(requirement),
                ]
            )
            logger.info(
                "QSCloudKitSynchronizer >> Synchronization remains paused for the pending local-dataset port"
            )
            return true
        } catch {
            let reportedError: Error =
                error is BigSyncReplicaBindingError
                    ? BigSyncCloudAccountPortError.corruptRequirement
                    : error
            postNotification(
                .SynchronizerDidFailToSynchronize,
                userInfo: [
                    cloudKitSynchronizerErrorKey: reportedError,
                ]
            )
            logger.error(
                "QSCloudKitSynchronizer >> Pending account-port state is unreadable: \(error)"
            )
            return true
        }
    }

    @BigSyncBackgroundActor
    private func requestReplicaJournalHandoff(
        from retiringBinding: String,
        retiringAccountScopeIdentifier: String,
        to destinationBinding: String,
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID
    ) throws {
        guard let installation = BackupDetection.installationIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: backupDetectionBaseURL
        ) else { throw BigSyncReplicaBindingError.corrupt }
        let context = RunContext(
            attemptID: validationAttemptID,
            runID: synchronizationRunID,
            accountIdentifier: accountIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier: destinationBinding
        )
        var handoff = try BigSyncReplicaJournalHandoff(
            installationIdentifier: installation,
            retiringBindingGenerationIdentifiers: [retiringBinding],
            destinationBindingGenerationIdentifier: destinationBinding
        )
        let retiringContext = RunContext(
            attemptID: validationAttemptID,
            runID: synchronizationRunID,
            accountIdentifier: accountIdentifier,
            accountScopeIdentifier: retiringAccountScopeIdentifier,
            replicaBindingGenerationIdentifier: retiringBinding
        )
        if let previous = try requireStoredChangeFeedMigrationState(for: retiringContext),
           previous.phase != .completed,
           let inherited = previous.replicaJournalHandoff {
            guard inherited.destinationBindingGenerationIdentifier == retiringBinding else {
                throw BigSyncReplicaJournalHandoffError.unexpectedBinding
            }
            handoff = try handoff.retainingUnfinished(inherited)
        }
        try requestChangeFeedRecovery(
            context: context,
            mode: .localDatasetRebootstrap,
            replicaJournalHandoff: handoff
        )
        guard let recorded = try requireStoredChangeFeedMigrationState(for: context),
              recorded.phase != .completed,
              let handoff = recorded.replicaJournalHandoff,
              handoff.installationIdentifier == installation,
              handoff.destinationBindingGenerationIdentifier == destinationBinding,
              handoff.retiringBindingGenerationIdentifiers.contains(retiringBinding) else {
            // Backup/encrypted reset may own a stronger recovery. Do not erase
            // pending port identity unless this exact obligation was recorded.
            throw BigSyncReplicaJournalHandoffError.invalidIdentity
        }
    }

    private func prepareReplicaBindingState() throws
        -> BigSyncReplicaBindingSnapshot {
        guard let installationIdentifier = BackupDetection
            .installationIdentifier(
                namespace: durableStateNamespace,
                sharedSentinelBaseURL: backupDetectionBaseURL
            ) else {
            throw BigSyncReplicaBindingError.corrupt
        }
        return try BigSyncReplicaBindingStateStore.prepare(
            store: keyValueStore,
            key: replicaBindingStateKey,
            installationIdentifier: installationIdentifier
        )
    }

    internal func activeReplicaBindingGenerationIdentifierForRun(
        accountScopeIdentifier: String
    ) throws -> String? {
        guard accountReplacementPolicy.usesDatasetReplicaBinding else {
            return nil
        }
        guard let binding = try BigSyncReplicaBindingStateStore.load(
            store: keyValueStore,
            key: replicaBindingStateKey
        ), binding.pendingPort == nil,
        binding.activeAccountScopeIdentifier == accountScopeIdentifier else {
            throw BigSyncReplicaBindingError.accountMismatch
        }
        return binding.activeGenerationIdentifier
    }

    /// Returns the last durably validated CloudKit account lease. Temporary
    /// network or account-status failures do not invalidate this value;
    /// `.CKAccountChanged`, restore, and a proved account replacement do.
    @BigSyncBackgroundActor
    public func accountScopeLease() throws -> BigSyncAccountScopeLease? {
        guard !accountScopeAuthorityFence.rejectsAuthority,
              !accountValidationRequired,
              backupDetectionError == nil,
              !backupRestoreDetected else {
            return nil
        }
        return try readAccountScopeLeaseDurably().lease
    }

    /// A normal, validated transport run may finish server-first restore
    /// reconciliation while the public domain-writer lease is still withheld.
    /// This never grants a cutoff/writer capability and does not bypass account,
    /// installation, binding, invalidation-epoch or durable-state validation.
    internal func outboundAccountScopeLease(for context: RunContext) throws -> BigSyncAccountScopeLease {
        try checkRunContext(context)
        guard !accountScopeAuthorityFence.rejectsAuthority, !accountValidationRequired,
              backupDetectionError == nil,
              let lease = try readAccountScopeLeaseDurably().lease,
              lease.accountScopeIdentifier == context.accountScopeIdentifier else {
            throw BigSyncAccountScopeLeaseError.unavailable
        }
        return lease
    }

    /// Revalidates a captured lease after suspension. Domain writers should
    /// also mirror this epoch into their local Realm and compare it inside the
    /// final non-suspending write transaction.
    @BigSyncBackgroundActor
    public func validateAccountScopeLease(
        _ expected: BigSyncAccountScopeLease
    ) throws {
        guard let current = try accountScopeLease() else {
            throw BigSyncAccountScopeLeaseError.unavailable
        }
        guard current.accountScopeIdentifier
                == expected.accountScopeIdentifier,
              current.invalidationGeneration
                == expected.invalidationGeneration else {
            throw BigSyncAccountScopeLeaseError.stale
        }
    }

    /// Read-only startup ownership. This is NOT a live account lease or writer
    /// permission: a fresh synchronizer deliberately still requires validation.
    internal struct PublicationRestorationAuthority: Equatable {
        let attemptID: UUID
        let installationIdentifier: String
        let accountState: BigSyncAccountScopeLeaseState
        let binding: BigSyncReplicaBindingSnapshot?
    }

    internal func publicationRestorationAuthority() throws -> PublicationRestorationAuthority {
        try Task.checkCancellation()
        guard !syncing, !synchronizationDrainIsActive, activeRunContext == nil,
              !cancelSync, !accountScopeAuthorityFence.requiresGenerationRotation,
              pendingAccountScopeInvalidation == nil,
              backupDetectionError == nil, !backupRestoreDetected else {
            throw CancellationError()
        }
        try keyValueStore.bigSyncValidateDurability()
        guard let installation = BackupDetection.installationIdentifier(
            namespace: durableStateNamespace, sharedSentinelBaseURL: backupDetectionBaseURL
        ) else { throw BigSyncReplicaBindingError.corrupt }
        let binding = accountReplacementPolicy.usesDatasetReplicaBinding
            ? try BigSyncReplicaBindingStateStore.load(store: keyValueStore, key: replicaBindingStateKey)
            : nil
        if accountReplacementPolicy.usesDatasetReplicaBinding {
            guard let binding, binding.pendingPort == nil,
                  binding.installationIdentityDigest == BigSyncReplicaBindingStateStore.installationIdentityDigest(for: installation) else {
                throw CancellationError()
            }
        }
        return PublicationRestorationAuthority(
            attemptID: synchronizationAttemptID, installationIdentifier: installation,
            accountState: try readAccountScopeLeaseDurably(), binding: binding
        )
    }

    private func readAccountScopeLeaseDurably() throws
        -> BigSyncAccountScopeLeaseState {
        try BigSyncAccountScopeLeaseState.load(
            store: keyValueStore,
            key: accountScopeLeaseKey
        )
    }

    private func persistAccountScopeLease(
        generation: Int64,
        accountScopeIdentifier: String?,
        validatedAt: Date?
    ) throws {
        let state = try BigSyncAccountScopeLeaseState(
            generation: generation,
            accountScopeIdentifier: accountScopeIdentifier,
            validatedAt: validatedAt
        )
        try state.persist(store: keyValueStore, key: accountScopeLeaseKey)
    }

    private func invalidateAccountScopeLeaseDurably() throws {
        let persisted = try readAccountScopeLeaseDurably()
        guard persisted.lease != nil else { return }
        guard persisted.generation < Int64.max else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        try persistAccountScopeLease(
            generation: persisted.generation + 1,
            accountScopeIdentifier: nil,
            validatedAt: nil
        )
    }

    private func queueAccountScopeInvalidation(
        _ reason: BigSyncAccountScopeInvalidationReason
    ) {
        pendingAccountScopeInvalidation = (UUID(), reason)
    }

    private func invalidateAccountScope(
        _ reason: BigSyncAccountScopeInvalidationReason
    ) async throws {
        queueAccountScopeInvalidation(reason)
        try await performPendingAccountScopeInvalidation()
    }

    private func performPendingAccountScopeInvalidation() async throws {
        guard let pending = pendingAccountScopeInvalidation else { return }
        try invalidateAccountScopeLeaseDurably()
        try await accountScopeInvalidationHandler?(pending.reason)
        if pendingAccountScopeInvalidation?.id == pending.id {
            pendingAccountScopeInvalidation = nil
        }
    }

    private func establishAccountScopeLeaseDurably(
        accountIdentifier: String,
        forceInvalidation: Bool
    ) throws {
        let scope = Self.accountScopeIdentifier(for: accountIdentifier)
        var persisted = try readAccountScopeLeaseDurably()
        if forceInvalidation || (
            persisted.lease != nil
                && persisted.lease?.accountScopeIdentifier != scope
        ) {
            try invalidateAccountScopeLeaseDurably()
            persisted = try readAccountScopeLeaseDurably()
        }
        if persisted.lease?.accountScopeIdentifier == scope {
            return
        }
        try persistAccountScopeLease(
            generation: persisted.generation,
            accountScopeIdentifier: scope,
            validatedAt: Date()
        )
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
        return durableStateKey(
            "ChangeFeedMigration.v\(ChangeFeedMigrationState.version).\(context.accountScopeIdentifier).\(recordZoneID.ownerName).\(recordZoneID.zoneName)"
        )
    }

    @BigSyncBackgroundActor
    private func requireStoredChangeFeedMigrationState(
        for context: RunContext
    ) throws -> ChangeFeedMigrationState? {
        let key = changeFeedMigrationStateKey(for: context)
        guard let raw = try keyValueStore.bigSyncDurableObject(forKey: key) else {
            return nil
        }
        guard let propertyList = raw as? [String: Any],
              let state = ChangeFeedMigrationState(
                key: key, propertyList: propertyList,
                accountScopeIdentifier: context.accountScopeIdentifier,
                zoneName: recordZoneID.zoneName,
                zoneOwnerName: recordZoneID.ownerName
              ) else {
            // Decode the snapshot just read. Unknown/corrupt history is never
            // permission to rediscover retained target rows as initial intent.
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
        return state
    }

    @BigSyncBackgroundActor
    private func persistChangeFeedMigrationState(
        _ state: ChangeFeedMigrationState
    ) throws {
        // The durable store owns atomic replacement and error propagation. A
        // failed/uncertain write must not be followed by a best-effort write of
        // an old envelope which could erase a newer recovery obligation.
        try keyValueStore.bigSyncSetDurably(value: state.propertyList, forKey: state.key)
        guard let raw = try keyValueStore.bigSyncDurableObject(forKey: state.key),
              let fields = raw as? [String: Any],
              let persisted = ChangeFeedMigrationState(
                key: state.key, propertyList: fields,
                accountScopeIdentifier: state.accountScopeIdentifier,
                zoneName: state.zoneName, zoneOwnerName: state.zoneOwnerName
              ), persisted == state else {
            throw ChangeFeedMigrationPersistenceError.stateNotDurable
        }
    }

    @BigSyncBackgroundActor
    internal func hasPendingEncryptedDataResetRecovery(
        context: RunContext
    ) throws -> Bool {
        guard let state = try requireStoredChangeFeedMigrationState(for: context) else {
            return false
        }
        return state.phase != .completed && state.mode == .encryptedDataReset
    }

    /// Requests a new server-first rebuild after opaque CloudKit history can no
    /// longer continue. Target Realms and their durable mutation journals remain
    /// untouched; only adapter tracking/provenance is rebuilt on the next run.
    @BigSyncBackgroundActor
    internal func requestChangeFeedRecovery(
        context: RunContext,
        mode: ChangeFeedResetMode = .serverReconciliation,
        backupRestoreEventIdentifier: String? = nil,
        replicaJournalHandoff: BigSyncReplicaJournalHandoff? = nil
    ) throws {
        let current = try requireStoredChangeFeedMigrationState(for: context)
        let requested = try ChangeFeedMigrationState.requesting(
            current: current,
            key: changeFeedMigrationStateKey(for: context),
            accountScopeIdentifier: context.accountScopeIdentifier,
            zoneName: recordZoneID.zoneName,
            zoneOwnerName: recordZoneID.ownerName,
            mode: mode,
            backupRestoreEventIdentifier: backupRestoreEventIdentifier,
            replicaJournalHandoff: replicaJournalHandoff
        )
        guard requested != current else { return }
        try persistChangeFeedMigrationState(requested)
        activeChangeFeedMigration = nil
    }

    /// Old recovery envelopes did not preserve retiring identities. Do not
    /// infer them from arbitrary pending rows: a bound retained-data recovery
    /// without history may proceed only when every portable row is already
    /// owned by its destination. This also fences old interrupted migrations.
    private func journalHandoffExpectation(
        for migration: ChangeFeedMigrationState,
        context: RunContext
    ) throws -> BigSyncReplicaJournalHandoff? {
        if let handoff = migration.replicaJournalHandoff { return handoff }
        guard migration.mode.reuploadsRetainedLocalData,
              let binding = context.replicaBindingGenerationIdentifier else { return nil }
        guard let installation = BackupDetection.installationIdentifier(
            namespace: durableStateNamespace,
            sharedSentinelBaseURL: backupDetectionBaseURL
        ) else { throw BigSyncReplicaBindingError.corrupt }
        return try BigSyncReplicaJournalHandoff(
            installationIdentifier: installation,
            retiringBindingGenerationIdentifiers: [],
            destinationBindingGenerationIdentifier: binding
        )
    }

    /// Run identity alone does not identify a recovery operation. A stronger
    /// recovery can be requested while an adapter or account check suspends,
    /// without changing the run. Never overwrite that newer durable envelope
    /// with an older phase or consume its restore event on return.
    @BigSyncBackgroundActor
    private func revalidateChangeFeedMigration(
        _ expected: ChangeFeedMigrationState,
        context: RunContext
    ) async throws {
        try await revalidateRunContext(context)
        guard try requireStoredChangeFeedMigrationState(for: context) == expected else {
            throw ChangeFeedMigrationPersistenceError.stateSuperseded
        }
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
        guard let modelAdapter = modelAdapters.first else { return }
        let stateKey = changeFeedMigrationStateKey(for: context)
        var stored = try requireStoredChangeFeedMigrationState(for: context)
        guard let migratingAdapter = modelAdapter as? any ChangeFeedResetMigrating else {
            if let stored, stored.phase != .completed,
               stored.replicaJournalHandoff != nil {
                throw BigSyncReplicaJournalHandoffError.unsupportedAdapter
            }
            return
        }
        if stored == nil {
            let initial = ChangeFeedMigrationState(
                key: stateKey,
                accountScopeIdentifier: context.accountScopeIdentifier,
                zoneName: modelAdapter.recordZoneID.zoneName,
                zoneOwnerName: modelAdapter.recordZoneID.ownerName,
                epoch: ChangeFeedMigrationState.initialEpoch,
                mode: .initialImport,
                phase: .requested
            )
            try persistChangeFeedMigrationState(initial)
            stored = initial
        }
        guard var migration = stored, migration.phase != .completed else {
            try completeRestoredBackupRecoveryIfNeeded(
                expectedEventIdentifier: stored?
                    .backupRestoreEventIdentifier
            )
            return
        }
        // A cold or unavailable adapter is not completion evidence. Only the
        // exact durable terminal marker can close the finishing crash window.
        if migration.phase == .finishing {
            let completionIsDurable = try await migratingAdapter
                .changeFeedResetCompletionIsDurable(
                    accountScopeIdentifier: migration.accountScopeIdentifier,
                    epoch: migration.epoch,
                    mode: migration.mode
                )
            try await revalidateChangeFeedMigration(migration, context: context)
            if completionIsDurable {
                if let handoff = try journalHandoffExpectation(for: migration, context: context) {
                    try await migratingAdapter.reconcileReplicaJournalHandoff(
                        handoff,
                        accountScopeIdentifier: migration.accountScopeIdentifier,
                        epoch: migration.epoch,
                        verifyOnly: true
                    )
                    try await revalidateChangeFeedMigration(migration, context: context)
                }
                if migration.mode == .encryptedDataReset {
                    try clearConfiguredZoneTerminal(
                        modelAdapter.recordZoneID,
                        accountScopeIdentifier: context.accountScopeIdentifier
                    )
                }
                migration.phase = .completed
                try persistChangeFeedMigrationState(migration)
                activeChangeFeedMigration = nil
                try completeRestoredBackupRecoveryIfNeeded(
                    expectedEventIdentifier: migration
                        .backupRestoreEventIdentifier
                )
                return
            }
            migration.phase = .requested
        }
        migration.phase = .requested
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = migration

        // A database cursor is not record-zone evidence. It may predate this
        // configured zone, so only the adapter's valid server record proof or
        // a previously persisted lifecycle marker may establish the zone.
        let hasServerEvidence = try await migratingAdapter.hasChangeFeedEstablishedServerEvidence()
        try await revalidateChangeFeedMigration(migration, context: context)
        if hasServerEvidence {
            try markConfiguredZoneEstablished(
                modelAdapter.recordZoneID,
                accountScopeIdentifier: context.accountScopeIdentifier
            )
        }

        try await migratingAdapter.prepareChangeFeedReset(
            accountScopeIdentifier: context.accountScopeIdentifier,
            epoch: migration.epoch,
            mode: migration.mode
        )
        try await revalidateChangeFeedMigration(migration, context: context)
        if let handoff = try journalHandoffExpectation(for: migration, context: context) {
            guard context.replicaBindingGenerationIdentifier
                    == handoff.destinationBindingGenerationIdentifier else {
                throw BigSyncReplicaJournalHandoffError.unexpectedBinding
            }
            try await migratingAdapter.reconcileReplicaJournalHandoff(
                handoff,
                accountScopeIdentifier: migration.accountScopeIdentifier,
                epoch: migration.epoch,
                verifyOnly: false
            )
            try await revalidateChangeFeedMigration(migration, context: context)
        }
        migration.phase = .prepared
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = migration
        try await migratingAdapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: context.accountScopeIdentifier,
            epoch: migration.epoch,
            mode: migration.mode
        )
        try await revalidateChangeFeedMigration(migration, context: context)

        // A nil database and zone token is the explicit full-server bootstrap
        // contract. Do not reuse a token from the pre-change-feed transport.
        try resetDatabaseToken()
        resetActiveTokens()
        try await modelAdapter.saveToken(nil)
        try await revalidateChangeFeedMigration(migration, context: context)
        migration.phase = .serverBootstrap
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = migration
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
        guard let adapter = modelAdapters.first
            as? any ChangeFeedResetMigrating else { return }
        try await revalidateChangeFeedMigration(migration, context: context)
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: migration.accountScopeIdentifier,
            epoch: migration.epoch,
            mode: migration.mode
        )
        try await revalidateChangeFeedMigration(migration, context: context)
        migration.phase = .reconciled
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = migration
    }

    /// Called only at the normal terminal receipt, after the post-upload
    /// refetch establishes quiescence.  Interrupted migrations intentionally
    /// retain their non-complete state and restart safely with nil tokens.
    @BigSyncBackgroundActor
    internal func finishChangeFeedMigrationIfNeeded(
        context: RunContext
    ) async throws {
        guard var migration = activeChangeFeedMigration,
              migration.phase == .reconciled else {
            if let stored = try requireStoredChangeFeedMigrationState(for: context),
               stored.phase != .completed {
                throw ChangeFeedMigrationPersistenceError.stateSuperseded
            }
            return
        }
        try await revalidateChangeFeedMigration(migration, context: context)
        if let handoff = try journalHandoffExpectation(for: migration, context: context) {
            guard let adapter = modelAdapters.first as? any ChangeFeedResetMigrating
            else { throw BigSyncReplicaJournalHandoffError.unsupportedAdapter }
            try await adapter.reconcileReplicaJournalHandoff(
                handoff,
                accountScopeIdentifier: migration.accountScopeIdentifier,
                epoch: migration.epoch,
                verifyOnly: true
            )
            try await revalidateChangeFeedMigration(migration, context: context)
        }
        migration.phase = .finishing
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = migration
        guard let modelAdapter = modelAdapters.first,
              let migratingAdapter = modelAdapter
                as? any ChangeFeedResetMigrating else { return }
        try await migratingAdapter.finishChangeFeedReset(
            accountScopeIdentifier: migration.accountScopeIdentifier,
            epoch: migration.epoch,
            mode: migration.mode
        )
        try await revalidateChangeFeedMigration(migration, context: context)
        if migration.mode == .encryptedDataReset {
            // Clear the terminal fence only after every adapter has completed
            // its journal-backed re-upload and the normal terminal drain has
            // proven quiescence. Clearing before the durable `completed`
            // marker makes the crash window safely resumable.
            try clearConfiguredZoneTerminal(
                modelAdapter.recordZoneID,
                accountScopeIdentifier: context.accountScopeIdentifier
            )
        }
        migration.phase = .completed
        try persistChangeFeedMigrationState(migration)
        activeChangeFeedMigration = nil
        try completeRestoredBackupRecoveryIfNeeded(
            expectedEventIdentifier: migration.backupRestoreEventIdentifier
        )
    }

#if DEBUG
    @BigSyncBackgroundActor
    func _test_validateSynchronizationAccount() async throws {
        _ = try await validateSynchronizationAccount()
    }

    @BigSyncBackgroundActor
    func _test_requireBackupDetectionRetry(_ error: Error) {
        backupDetectionError = error
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
        publicationConsumptionPending = false
        publicationFetchDeferralEligible = false
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
    @objc internal func resetDatabaseToken() throws {
        try persistDatabaseToken(nil)
    }
    
    internal func activeZoneToken(zoneID: CKRecordZone.ID) -> RecordZoneChangeCursor? {
        return activeZoneTokens[zoneID]
    }
    
    /// Ensures that the one active custom zone of a disposable synchronizer
    /// client no longer exists.
    ///
    /// The caller must discard the synchronizer and all of its adapters after
    /// this succeeds: their local tracking state now refers to an absent zone.
    /// This is intentionally limited to a client whose every adapter targets
    /// one identical zone, such as an isolated end-to-end test client.
    ///
    /// CloudKit reporting the zone already absent is also success. That makes
    /// a crash after remote delete but before the caller's local receipt
    /// durably resumable.
    @BigSyncBackgroundActor
    public func deleteActiveRecordZoneForDisposableClient(
        using receipt: SynchronizationReceipt
    ) async throws {
        let activeZoneIDs = Set(modelAdapters.map(\.recordZoneID))
        guard activeZoneIDs.count == 1,
              let activeZoneID = activeZoneIDs.first else {
            throw OneOffRecordZoneResetError.disposableClientMustUseExactlyOneRecordZone
        }
        try await deleteDisposableRecordZoneIfPresent(
            activeZoneID,
            using: receipt
        )
    }

    private func deleteDisposableRecordZoneIfPresent(
        _ zoneID: CKRecordZone.ID,
        using receipt: SynchronizationReceipt
    ) async throws {
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
            do {
                try await deleteRecordZone(zoneID)
            } catch {
                guard isMissingRecordZoneError(error) else { throw error }
            }
            try validateReservedAuthorization()
            try await ensureCurrentAccount(receipt.accountIdentifier)
            try validateReservedAuthorization()
            shouldConsumeAuthorization = true
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

    /// Returns whether `adapter` is the synchronizer's sole private-zone
    /// adapter. Re-registering the same instance is idempotent; every other
    /// topology is rejected at configuration time.
    internal func canAddModelAdapter(_ adapter: ModelAdapter) -> Bool {
        if let existing = modelAdapterDictionary[adapter.recordZoneID] {
            return existing === adapter
        }
        guard modelAdapterDictionary.isEmpty,
              adapter.recordZoneID.ownerName == CKCurrentUserDefaultName,
              adapter.recordZoneID != CKRecordZone.default().zoneID else {
            return false
        }
#if DEBUG
        if allowsRecordZoneRebindingForTesting {
            return true
        }
#endif
        guard adapter.recordZoneID == recordZoneID,
              adapter is any ChangeFeedResetMigrating else { return false }
        return true
    }
    
    /// Adds a new model adapter to be synchronized with CloudKit.
    /// - Parameter adapter: The adapter to be managed by this synchronizer.
    public func addModelAdapter(_ adapter: ModelAdapter) {
        precondition(
            canAddModelAdapter(adapter),
            "BigSyncKit supports exactly one adapter in one private custom record zone"
        )
#if DEBUG
        if allowsRecordZoneRebindingForTesting,
           modelAdapterDictionary.isEmpty,
           adapter.recordZoneID != recordZoneID {
            recordZoneID = adapter.recordZoneID
            durableStateNamespace = Self.makeDurableStateNamespace(
                identifier: identifier,
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope,
                recordZoneID: adapter.recordZoneID
            )
            // The initializer created backup/installation state for the original
            // zone. This legacy DEBUG fixture hook must initialize the rebound
            // namespace too; ordinary outbound admission still requires its
            // real installation sentinel and must never bypass that proof.
            do {
                let result = try BackupDetection.run(
                    store: keyValueStore,
                    namespace: durableStateNamespace,
                    sharedSentinelBaseURL: backupDetectionBaseURL
                )
                refreshBackupRestoreRequirement()
                if result == .restoredFromBackup || backupRestoreDetected {
                    accountScopeAuthorityFence.poison()
                    clearDeviceIdentifier()
                    try invalidateAccountScopeLeaseDurably()
                    queueAccountScopeInvalidation(.restoreDetected)
                }
            } catch {
                accountScopeAuthorityFence.poison()
                backupDetectionError = error
                logger.error("QSCloudKitSynchronizer >> Rebound fixture backup detection failed: \(error)")
            }
        }
        allowsRecordZoneRebindingForTesting = false
#endif
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
        guard !reportPendingCloudAccountPortIfNeeded() else { return }
        beginSynchronization()
    }
}
