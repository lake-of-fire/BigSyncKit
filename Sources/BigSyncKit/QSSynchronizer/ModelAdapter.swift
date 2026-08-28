//
//  ModelAdapter.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation
import CloudKit
import RealmSwift

/// The merge policy to resolve change conflicts. Default value is `server`
@objc public enum MergePolicy: Int, Sendable {
    /// Downloaded changes have preference.
    case server
    /// Delegate can resolve changes manually.
    case custom
}

//public protocol ModelAdapter: AnyObject {
//    /// Tells the model adapter that these records were uploaded successfully to CloudKit.
//    /// - Parameter savedRecords: Records that were saved.
//    func didUpload(savedRecords: [CKRecord])
//}

public protocol ModelAdapterDelegate: AnyObject {
    func needsInitialSetup() async throws
    func hasChangesToUpload() async
}

/// Optional hooks for the one-time change-feed tracking migration. The
/// synchronizer owns account fencing and phase ordering; adapters own durable
/// tracking/provenance storage.
public enum ChangeFeedResetMode: String, Sendable {
    /// The one bounded import of objects that predate BigSyncKit's durable
    /// mutation journal. This is the only mode allowed to discover an
    /// untracked, unjournaled target object as new upload work.
    case initialImport
    /// Reconcile a full server bootstrap conservatively. A previously
    /// server-backed record that is now absent must not be resurrected.
    case serverReconciliation
    /// A device/app backup contains a historical snapshot of the local outbox.
    /// Keep target Realm user objects, but do not replay copied mutation
    /// generations or rediscover untracked objects as current local intent.
    case backupRestore
    /// CloudKit explicitly reset the account's encrypted data. The direct
    /// database API documents that locally retained live data may be
    /// re-uploaded, so rebuild durable upload generations without changing the
    /// target objects themselves.
    case encryptedDataReset
    /// The authenticated account changed while the application retained one
    /// admitted local dataset. Rebuild the destination replica from local
    /// rows without copying an old CloudKit zone.
    case localDatasetRebootstrap

    var reuploadsRetainedLocalData: Bool {
        self == .encryptedDataReset || self == .localDatasetRebootstrap
    }
}

public protocol ChangeFeedResetMigrating: AnyObject {
    /// Evidence captured before reset that this zone has previously held a
    /// valid server record.  It protects an established zone from accidental
    /// recreation after a deletion lifecycle event.
    func hasChangeFeedEstablishedServerEvidence() async throws -> Bool
    func prepareChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws
    func isChangeFeedServerBootstrapActive() async -> Bool
    /// Proves that this exact reset reached its durable terminal state.
    /// Failure to open the persistence store must throw, not look complete.
    func changeFeedResetCompletionIsDurable(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws -> Bool
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws
}

public extension ChangeFeedResetMigrating {
    func hasChangeFeedEstablishedServerEvidence() async throws -> Bool { false }

    func prepareChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int
    ) async throws {
        try await prepareChangeFeedReset(
            accountScopeIdentifier: accountScopeIdentifier,
            epoch: epoch,
            mode: .serverReconciliation
        )
    }

    func beginChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int
    ) async throws {
        try await beginChangeFeedServerBootstrap(
            accountScopeIdentifier: accountScopeIdentifier,
            epoch: epoch,
            mode: .serverReconciliation
        )
    }

    func reconcileAfterChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int
    ) async throws {
        try await reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: accountScopeIdentifier,
            epoch: epoch,
            mode: .serverReconciliation
        )
    }

    func finishChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int
    ) async throws {
        try await finishChangeFeedReset(
            accountScopeIdentifier: accountScopeIdentifier,
            epoch: epoch,
            mode: .serverReconciliation
        )
    }
}

public struct InboundEventIdentity: Sendable, Equatable, Hashable {
    public let ordinal: Int
    public let entityType: String
    public let recordName: String
    public let zoneName: String
    public let zoneOwnerName: String

    public init(
        ordinal: Int,
        entityType: String,
        recordID: CKRecord.ID
    ) {
        self.ordinal = ordinal
        self.entityType = entityType
        recordName = recordID.recordName
        zoneName = recordID.zoneID.zoneName
        zoneOwnerName = recordID.zoneID.ownerName
    }

    public func matches(
        ordinal: Int,
        entityType: String,
        recordID: CKRecord.ID
    ) -> Bool {
        self == InboundEventIdentity(
            ordinal: ordinal,
            entityType: entityType,
            recordID: recordID
        )
    }

    public func matches(ordinal: Int, recordID: CKRecord.ID) -> Bool {
        self.ordinal == ordinal
            && recordName == recordID.recordName
            && zoneName == recordID.zoneID.zoneName
            && zoneOwnerName == recordID.zoneID.ownerName
    }
}

public enum InboundLiveDisposition: Sendable, Equatable {
    case applied
    case unchanged
    /// The server change is the authoritative echo of this replica's upload.
    /// Its semantic payload was validated without applying it back to the local
    /// model, so it can prove supersession of older quarantined evidence.
    case validatedAuthoritativeOwnUpload
    case preservedPendingLocal(generation: String)
    case preservedImmutable
    case deferred(relationshipCount: Int)
    case quarantined(lineageID: String)
    case ignoredExplicitAuthority
}

public enum InboundDeletionDisposition: Sendable, Equatable {
    case appliedTombstone
    case alreadyDeleted
    case preservedNewerLive(generation: String)
    case quarantined(lineageID: String)
    case ignoredExplicitAuthority
}

public struct InboundLiveResult: Sendable, Equatable {
    public let event: InboundEventIdentity
    public let disposition: InboundLiveDisposition

    public init(
        event: InboundEventIdentity,
        disposition: InboundLiveDisposition
    ) {
        self.event = event
        self.disposition = disposition
    }
}

public struct InboundDeletionResult: Sendable, Equatable {
    public let event: InboundEventIdentity
    public let disposition: InboundDeletionDisposition

    public init(
        event: InboundEventIdentity,
        disposition: InboundDeletionDisposition
    ) {
        self.event = event
        self.disposition = disposition
    }
}

/// Complete durable outcome of one CloudKit record-zone page. The adapter
/// commits this receipt together with the page cursor so cursor advancement
/// can never become detached from the exact accepted-event postimage.
public struct InboundPageCommit: Sendable, Equatable {
    public let previousCursor: RecordZoneChangeCursor?
    public let nextCursor: RecordZoneChangeCursor
    public let liveResults: [InboundLiveResult]
    public let deletionResults: [InboundDeletionResult]

    public init(
        previousCursor: RecordZoneChangeCursor?,
        nextCursor: RecordZoneChangeCursor,
        liveResults: [InboundLiveResult],
        deletionResults: [InboundDeletionResult]
    ) {
        self.previousCursor = previousCursor
        self.nextCursor = nextCursor
        self.liveResults = liveResults
        self.deletionResults = deletionResults
    }
}

public enum InboundDispositionValidationError: Error, Equatable {
    case cardinality(expected: Int, actual: Int)
    case identityMismatch(ordinal: Int, expectedRecordName: String)
    case eventOutsideExpectedZone(recordName: String)
    case duplicateInboundEvent(recordName: String)
}

/// An object conforming to `ModelAdapter` will track the local model, provide changes to upload to CloudKit and import downloaded changes.
//@objc public protocol ModelAdapter: AnyObject {
public protocol ModelAdapter: AnyObject, Sendable {
    /// Whether the model has any changes
    var hasChanges: Bool { get }

    /// Entity types that should be synced ahead of the default order.
    var priorityEntityTypeNames: [String] { get }
    
    var modelAdapterDelegate: ModelAdapterDelegate? { get set }

    /// Supplies the exact CloudKit transport namespace used by inbound
    /// semantic lineage. Adapters must receive this before accepting events.
    @BigSyncBackgroundActor
    func activateTransportNamespace(
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope
    ) async throws

    /// Durable adapter-owned conditions that make transport quiescent but
    /// semantic publication incomplete.
    @BigSyncBackgroundActor
    func semanticPublicationBlockers() async throws
        -> [CloudKitSynchronizer.DomainBlocker]

    /// Binds adapter discovery, preparation, inbound validation, and
    /// acknowledgement to the CloudKit account already validated for this
    /// synchronization run. Implementations that do not own account-scoped
    /// models may use the default no-op.
    @BigSyncBackgroundActor
    func activateAccountScope(_ accountScopeIdentifier: String) async throws

    /// Activates the exact local transport generation authorized for this
    /// synchronization run. Implementations without a durable mutation journal
    /// may use the default account-only activation.
    @BigSyncBackgroundActor
    func activateReplicaBinding(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?
    ) async throws
    
    func cleanUp() async throws
    
    func resetSyncCaches() async throws
    
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool
    
    /// Apply changes in the provided record to the local model objects and save the records.
    /// - Parameter records: Array of `CKRecord` that were obtained from CloudKit.
    /// - Parameter forceSave: Use especially for saving conflicted CKRecords which may have a newer record change tag from the server regardless of whether they have changes.
    func saveChanges(
        in records: [CKRecord],
        forceSave: Bool
    ) async throws -> [InboundLiveResult]

    /// Semantically validates server-authoritative echoes of this replica's
    /// uploads without applying their fields back to the local model. The
    /// returned outcomes remain part of the exact inbound page receipt.
    func validateAuthoritativeOwnUploadRecords(
        _ records: [CKRecord]
    ) async throws -> [InboundLiveResult]
    
    /// Delete the local model objects corresponding to the given record IDs.
    /// - Parameter recordIDs: Array of identifiers of records that were deleted on CloudKit.
    func deleteRecords(
        with recordIDs: [CKRecord.ID]
    ) async throws -> [InboundDeletionResult]
    
    /// Tells the model adapter to persist all downloaded changes in the current import operation.
    func persistImportedChanges() async throws

    /// Prepares upload records with the exact durable mutation generation that
    /// authorized each record. Restricting by entity type supports Manabi's
    /// deterministic priority phases.
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload]

    /// Acknowledges only generations returned by the matching preparation.
    @BigSyncBackgroundActor
    func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws

    /// Prepares deletions with their exact durable mutation generations.
    @BigSyncBackgroundActor
    func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion]

    /// Acknowledges only deletion generations returned by preparation.
    @BigSyncBackgroundActor
    func didDelete(
        recordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws

    /// Requeues an upload rejected as missing on the server without
    /// overwriting a newer mutation that arrived after preparation.
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws

    /// Rebases only the cached CloudKit system fields for local deletions that
    /// lost a server-record conflict. Implementations must preserve both the
    /// local tombstone and the exact prepared mutation generation; this is not
    /// an inbound model-value merge.
    @BigSyncBackgroundActor
    func rebasePendingDeletionMetadata(
        using serverRecords: [CKRecord],
        matchingPreparedGenerations: [String: String]
    ) async throws

    /// Asks the model adapter whether it has a local object for the given record identifier.
    /// - Parameter recordID: Record identifier.
    /// - Returns: Whether there is a corresponding object for this identifier.
//    func hasRecordID(_ recordID: CKRecord.ID) -> Bool
    
    /// Tells the model adapter that the current import operation finished.
    func didFinishImport() async throws
    
    /// Record zone ID managed by this adapter
    var recordZoneID: CKRecordZone.ID { get }
    
    /// Latest record-zone cursor stored by this adapter, or `nil` if one does not exist.
    var serverChangeToken: RecordZoneChangeCursor? { get async }
    
    /// Save given token for future use by this adapter.
    /// - Parameter token: opaque record-zone history cursor.
    func saveToken(_ token: RecordZoneChangeCursor?) async throws

    /// Atomically publishes one accepted page's exact outcome receipt and its
    /// next cursor. Adapters without page-receipt storage retain the token-last
    /// behavior through the default implementation.
    func commitInboundPage(_ page: InboundPageCommit) async throws

    /// Stable identifier for the latest record-zone cursor durably consumed by
    /// this adapter for the validated account. A terminal synchronization
    /// receipt uses this to distinguish a complete server snapshot from target
    /// rows that became visible while a page was still being imported.
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?,
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope
    ) throws -> String?

    /// Durable reset/rebuild epoch that namespaces the consumed cursor.
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int?
    
    /// Merge policy in case of conflicts. Default is `server`.
    var mergePolicy: MergePolicy { get set }
    
    func cancelSynchronization()
    /// Waits until adapter-owned work from the cancelled synchronization can no
    /// longer publish tracking metadata. Implementations without background
    /// work may use the default no-op implementation.
    func waitForCancellation() async
    func unsetCancellation() async throws
        
    /// Returns corresponding `CKRecord` for the given model object.
    /// - Parameter object: Model object.
//    func record(for object: AnyObject) -> CKRecord?

    /// Returns CKShare for the given model object, if one exists.
    /// - Parameter object: Model object.
    //    @available(iOS 10.0, OSX 10.12, *) func share(for object: AnyObject) -> CKShare?
    
    /// Store CKShare for given model object.
    /// - Parameters:
    ///   - share: `CKShare` object to save.
    ///   - object: Model object.
//    @available(iOS 10.0, OSX 10.12, *) func save(share: CKShare, for object: AnyObject)
    
    /// Delete existing `CKShare` for given model object.
    /// - Parameter object: Model object.
    //    @available(iOS 10.0, OSX 10.12, *) func deleteShare(for object: AnyObject)
}

public extension ModelAdapter {
    func commitInboundPage(_ page: InboundPageCommit) async throws {
        try await saveToken(page.nextCursor)
    }

    func validateAuthoritativeOwnUploadRecords(
        _ records: [CKRecord]
    ) async throws -> [InboundLiveResult] {
        records.enumerated().map { ordinal, record in
            InboundLiveResult(
                event: InboundEventIdentity(
                    ordinal: ordinal,
                    entityType: record.recordType,
                    recordID: record.recordID
                ),
                disposition: .ignoredExplicitAuthority
            )
        }
    }

    @BigSyncBackgroundActor
    func activateTransportNamespace(
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope
    ) async throws {
        _ = containerIdentifier
        _ = databaseScope
    }

    @BigSyncBackgroundActor
    func semanticPublicationBlockers() async throws
        -> [CloudKitSynchronizer.DomainBlocker] { [] }

    var priorityEntityTypeNames: [String] { [] }

    func waitForCancellation() async {}

    @BigSyncBackgroundActor
    func activateAccountScope(_ accountScopeIdentifier: String) async throws {
        _ = accountScopeIdentifier
    }

    /// Activates the exact local transport generation authorized for this
    /// synchronization run. Implementations without a durable mutation
    /// journal may continue using account-only activation.
    @BigSyncBackgroundActor
    func activateReplicaBinding(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?
    ) async throws {
        _ = replicaBindingGenerationIdentifier
        try await activateAccountScope(accountScopeIdentifier)
    }

    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?,
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope
    ) throws -> String? {
        _ = accountScopeIdentifier
        _ = replicaBindingGenerationIdentifier
        _ = containerIdentifier
        _ = databaseScope
        return nil
    }

    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? { nil }

    @BigSyncBackgroundActor
    func rebasePendingDeletionMetadata(
        using serverRecords: [CKRecord],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        // There is no safe generic implementation: applying an inbound server
        // record can overwrite the local tombstone this conflict is supposed
        // to preserve. Fail closed; the bounded mutation drain returns the
        // conflict and leaves the exact deletion generation pending.
        _ = serverRecords
        _ = matchingPreparedGenerations
        throw ModelAdapterDeletionConflictError
            .tombstonePreservingRebaseNotImplemented
    }
}

public enum ModelAdapterDeletionConflictError: Error, Equatable {
    case tombstonePreservingRebaseNotImplemented
}

public struct PreparedRecordUpload: @unchecked Sendable {
    public let record: CKRecord
    public let generation: String?

    public init(record: CKRecord, generation: String?) {
        self.record = record
        self.generation = generation
    }
}

public struct PreparedRecordDeletion: Sendable {
    public let recordID: CKRecord.ID
    public let generation: String?

    public init(recordID: CKRecord.ID, generation: String?) {
        self.recordID = recordID
        self.generation = generation
    }
}

// Terminal receipts need a synchronous view of adapter-owned durable work after
// the final suspending import and cleanup boundary.
internal protocol TerminalSynchronizationStateModelAdapter: ModelAdapter {
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool
}
