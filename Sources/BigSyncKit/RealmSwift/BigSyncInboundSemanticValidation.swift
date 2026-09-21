import CloudKit
import Foundation
import RealmSwift

/// Implemented by synchronized Realm model classes whose CloudKit payload has
/// semantic invariants that must hold before it is admitted to the target
/// Realm. Validation is intentionally performed on the record, before Realm's
/// generic property decoder can publish a partial or malformed object.
public protocol BigSyncInboundSemanticRecordValidating {
    static func validateInboundSemanticRecord(_ record: CKRecord) throws

    /// Best-effort domain scope used to block only the affected activation or
    /// authority. Returning nil is deliberately conservative: malformed or
    /// unscoped records then block every scope queried for this model/account.
    static func inboundSemanticQuarantineScopeIdentifier(
        _ record: CKRecord
    ) -> String?
}

public extension BigSyncInboundSemanticRecordValidating {
    static func inboundSemanticQuarantineScopeIdentifier(
        _ record: CKRecord
    ) -> String? {
        nil
    }
}

/// Optional second admission fence for models whose primary key denotes one
/// immutable semantic fact. It runs after target re-resolution and before the
/// generic merge policy can replace an existing object.
public enum BigSyncInboundSemanticReplacementDisposition: Sendable, Equatable {
    /// Apply the received fields to the target object normally.
    case applyIncomingRecord

    /// Keep the existing target object byte-for-byte while still accepting
    /// the received CKRecord as current tracking/system-field evidence.
    case preserveExistingObject

    /// Select the complete incoming value independently of timestamp order.
    /// Replacing pending work mints a new journal generation so an old upload
    /// acknowledgement cannot clear the selected value.
    case preferIncomingRecord

    /// Select the complete existing value and retain or enqueue its upload
    /// without manufacturing a new modification timestamp.
    case preferExistingObject
}

public protocol BigSyncInboundSemanticReplacementValidating {
    static func validateInboundSemanticReplacement(
        _ record: CKRecord,
        existingObject: Object?
    ) throws

    static func inboundSemanticReplacementDisposition(
        _ record: CKRecord,
        existingObject: Object?
    ) throws -> BigSyncInboundSemanticReplacementDisposition
}

public extension BigSyncInboundSemanticReplacementValidating {
    static func inboundSemanticReplacementDisposition(
        _ record: CKRecord,
        existingObject: Object?
    ) throws -> BigSyncInboundSemanticReplacementDisposition {
        try validateInboundSemanticReplacement(
            record,
            existingObject: existingObject
        )
        return .applyIncomingRecord
    }
}

/// Missing local authority is retryable, not proof of corrupt remote data.
public struct BigSyncSemanticAdmissionUnavailable: Error, Equatable, Sendable {
    public let entityType: String
    public init(entityType: String) { self.entityType = entityType }
}

/// Validate the actual managed value before preparing an upload.
public protocol BigSyncOutboundSemanticObjectValidating {
    func validateOutboundSemanticObject(in realm: Realm) throws
}

/// Structural selectors must retain their identity through acknowledgement,
/// cleanup and destination reseeding. This is a type-wide lifecycle contract;
/// the value must be constant for every instance, including a default instance.
public protocol BigSyncRetainsSyncedTombstone {
    var retainsSyncedTombstone: Bool { get }
}

enum BigSyncInboundValidationErrors {
    static func rethrowNonSemantic(_ error: any Error) throws {
        try Task.checkCancellation()
        if error is CancellationError || error is BigSyncSemanticAdmissionUnavailable {
            throw error
        }
    }
}

/// Opt-in for models whose unjournaled local value is a replicated snapshot,
/// not independent local authoring authority. Standalone/replacement validation
/// still runs, and an exact pending local generation still wins the write fence.
/// This does not restrict forwarding an unchanged source during account recovery.
public protocol BigSyncAuthoritativeServerSnapshotModel {}

/// Narrow model-owned exception to replacement validation. A valid received
/// record may be a predecessor of a pending local semantic extension rather
/// than an attempted rollback of accepted state (for example unbound -> bound).
/// Implementations must validate immutable identity and the exact permitted
/// predecessor relationship; the presence of a journal is not permission to
/// bypass validation. The adapter never applies these fields over that journal.
public protocol BigSyncInboundPendingSemanticReplacementValidating {
    static func validateInboundSemanticPredecessorOfPendingMutation(
        _ record: CKRecord,
        existingObject: Object
    ) throws
}

/// Admission fence for CloudKit record deletions whose absence would violate
/// a model's semantic authority. CloudKit deletion callbacks carry only a
/// record identifier, so authorization must be provable from that identifier
/// and the current local object. A thrown error quarantines the deletion and
/// leaves the target object unchanged.
public protocol BigSyncInboundSemanticDeletionValidating {
    static func validateInboundSemanticDeletion(
        _ recordID: CKRecord.ID,
        existingObject: Object?
    ) throws

    /// Best-effort domain scope used to block only the affected authority.
    /// Unknown or malformed local state should return nil, which
    /// conservatively blocks every scope queried for this model/account.
    static func inboundSemanticDeletionQuarantineScopeIdentifier(
        _ recordID: CKRecord.ID,
        existingObject: Object?
    ) -> String?
}

public extension BigSyncInboundSemanticDeletionValidating {
    static func inboundSemanticDeletionQuarantineScopeIdentifier(
        _ recordID: CKRecord.ID,
        existingObject: Object?
    ) -> String? {
        nil
    }
}

/// A stable, non-sensitive classification persisted with a quarantined
/// inbound record. The detailed error remains diagnostic-only in logs.
public protocol BigSyncInboundSemanticValidationFailure: Error {
    var bigSyncValidationCode: String { get }
}

/// Local recovery evidence for one received record that failed model-level
/// semantic validation. This is deliberately stored in BigSync's tracking
/// Realm: it is neither application state nor a second upload journal.
final class BigSyncInboundSemanticQuarantine: Object {
    @Persisted(primaryKey: true) var lineageID = ""
    @Persisted(indexed: true) var recordName = ""
    @Persisted(indexed: true) var entityType = ""
    @Persisted(indexed: true) var accountScopeIdentifier: String?
    @Persisted(indexed: true) var semanticScopeIdentifier: String?
    @Persisted var containerIdentifier = ""
    @Persisted var databaseScopeRawValue = 0
    @Persisted var zoneOwnerName = ""
    @Persisted var zoneName = ""
    @Persisted var eventKind = ""
    @Persisted var compatibilityGeneration = 0
    @Persisted var recordChangeTag: String?
    @Persisted var deletionFeedLineage = ""
    @Persisted var replicaActivationIdentifier = ""
    @Persisted var changeFeedEpoch = 0
    @Persisted var validationCode = ""
    @Persisted var receivedRecordDigestHex = ""
    @Persisted var importRunIdentifier = ""
    /// Zero denotes crash-prefix evidence that was never bound to a committed
    /// page receipt and therefore cannot be retired by ordering.
    @Persisted var committedPageSequence: Int64 = 0
    @Persisted var committedPageReceiptID = ""
    @Persisted var committedPageOutcomeDigestHex = ""
    @Persisted var detectedAt = Date()
}

/// Receipt for a fully committed record-zone page in one adapter tracking
/// Realm. The canonical row is the current page head; semantic-evidence rows
/// are retained only while a live quarantine refers to them. This is not an
/// event ledger or upload-work authority.
final class BigSyncInboundPageReceipt: Object {
    static let canonicalID = "record-zone-page-head-v1"

    @Persisted(primaryKey: true) var id = ""
    @Persisted var isHead = false
    @Persisted var accountScopeIdentifier = ""
    @Persisted var containerIdentifier = ""
    @Persisted var databaseScopeRawValue = 0
    @Persisted var zoneOwnerName = ""
    @Persisted var zoneName = ""
    @Persisted var replicaActivationIdentifier = ""
    @Persisted var changeFeedEpoch = 0
    @Persisted var pageSequence: Int64 = 0
    @Persisted var previousCursorDigestHex = ""
    @Persisted var nextCursorDigestHex = ""
    @Persisted var outcomeDigestHex = ""
    @Persisted var acceptedEventCount = 0
    @Persisted var supersededLineageIDs = List<String>()
    @Persisted var committedAt = Date()
}

/// Unacknowledged application repair input. It is committed with the zone
/// cursor and removed only after the fenced application callback succeeds.
final class BigSyncPendingInboundIdentityDelivery: Object {
    static let canonicalID = "pending-inbound-identity-delivery-v1"

    @Persisted(primaryKey: true) var id = canonicalID
    @Persisted var deliveryID = ""
    /// Compatibility payload written by persistence schemas through v19.
    /// When present, it precedes every page batch below.
    @Persisted var encodedIdentities = Data()
    /// Independently encoded committed pages, retained in cursor order. Page
    /// commit appends only its own identities; terminal delivery performs the
    /// one required last-disposition-wins consolidation.
    @Persisted var encodedIdentityPageBatches = List<Data>()
}
