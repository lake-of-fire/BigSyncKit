import CloudKit
import Foundation
import RealmSwift

/// Implemented by synchronized Realm model classes whose CloudKit payload has
/// semantic invariants that must hold before it is admitted to the target
/// Realm. Validation is intentionally performed on the record, before Realm's
/// generic property decoder can publish a partial or malformed object.
public protocol BigSyncInboundSemanticRecordValidating {
    static func validateInboundSemanticRecord(_ record: CKRecord) throws

    /// Returns the record to consume after successful validation. The default
    /// preserves the existing validator behavior. Models with bounded assets
    /// can return a detached copy with those assets materialized as Data so
    /// change detection, replacement validation, delegates and target decoding
    /// all consume the same bytes rather than reopening a mutable file.
    ///
    /// Do not mutate the received record. Preserve all semantic values, record
    /// identity and system fields. This is a representation boundary, not a
    /// merge or repair hook. The adapter preserves record ID/type/change tag.
    /// Throw BigSyncInboundResourceUnavailable for unavailable local assets,
    /// not a semantic failure which would quarantine otherwise unknown bytes.
    static func validatedInboundRecord(_ record: CKRecord) throws -> CKRecord

    /// Best-effort domain scope used to block only the affected activation or
    /// authority. Returning nil is deliberately conservative: malformed or
    /// unscoped records then block every scope queried for this model/account.
    static func inboundSemanticQuarantineScopeIdentifier(
        _ record: CKRecord
    ) -> String?
}

public extension BigSyncInboundSemanticRecordValidating {
    static func validatedInboundRecord(_ record: CKRecord) throws -> CKRecord {
        try validateInboundSemanticRecord(record)
        return record
    }

    static func inboundSemanticQuarantineScopeIdentifier(
        _ record: CKRecord
    ) -> String? {
        nil
    }
}

/// Admission which depends on authority stored in the target Realm, rather
/// than on a pre-existing target object. It runs for new records too. The
/// adapter checks it before protocol-specific intrinsic validation during
/// selection, and again inside the actual target write before creating an
/// object or choosing a replacement winner. Own-upload validation uses the
/// same admission-before-decoding order.
///
/// Implementations must only read this Realm, must not suspend, and must not
/// infer a protocol from `existingObject == nil`. Throw a semantic validation
/// failure for incompatible received data. Throw
/// `BigSyncSemanticAdmissionUnavailable` when local authority is not ready;
/// that aborts the attempt without quarantining the record or consuming it.
public protocol BigSyncRealmSemanticRecordAdmitting {
    static func validateSemanticAdmission(
        of record: CKRecord,
        in realm: Realm
    ) throws
}

/// Missing local authority is not evidence that a received record is corrupt.
/// The same error also fences outbound preparation until admission is ready.
/// It deliberately does not conform to BigSyncInboundSemanticValidationFailure.
public struct BigSyncSemanticAdmissionUnavailable: Error, Equatable, Sendable {
    public let entityType: String

    public init(entityType: String) {
        self.entityType = entityType
    }
}

/// A local resource needed to inspect received bytes is unavailable or changed
/// while being read. Abort this attempt without consuming/quarantining the
/// event; the ordinary cursor/journal recovery path can redeliver it. This is
/// not a semantic verdict and contains no local filesystem path.
public struct BigSyncInboundResourceUnavailable: Error, Equatable, Sendable {
    public let entityType: String
    public let fieldName: String

    public init(entityType: String, fieldName: String) {
        self.entityType = entityType
        self.fieldName = fieldName
    }
}

/// Keep the error boundary identical for record, replacement, echo, and
/// deletion validators. An unavailable local prerequisite cannot establish
/// corruption of the received event. Unknown validator errors retain the
/// existing conservative semantic-quarantine behavior.
enum BigSyncInboundValidationErrors {
    static func rethrowNonSemantic(_ error: any Error) throws {
        try Task.checkCancellation()
        if error is CancellationError
            || error is BigSyncSemanticAdmissionUnavailable
            || error is BigSyncInboundResourceUnavailable
            || error is BigSyncInboundSemanticContextUnavailable {
            throw error
        }
    }
}

/// Model-level replacement admission and conflict selection. Normal imports
/// evaluate it against the current target inside the target write, before any
/// fields are changed. Authoritative own-upload echoes may also use it as a
/// read-only compatibility check. Implementations must not mutate or suspend.
/// CancellationError aborts processing rather than creating quarantine evidence.
public enum BigSyncInboundSemanticReplacementDisposition: Sendable, Equatable {
    /// Apply the received fields to the target object normally.
    case applyIncomingRecord

    /// Keep the existing target object byte-for-byte while still accepting
    /// the received CKRecord as current tracking/system-field evidence.
    case preserveExistingObject

    /// A model-level whole-record conflict rule selects the received value.
    /// Apply it even when ordinary timestamps or pending local work prefer the
    /// target. Replacing pending work must mint a fresh generation so an older
    /// acknowledgement cannot erase the selected value. Keep the selected
    /// record's change timestamps; retransmission is not a new user edit.
    case preferIncomingRecord

    /// A model-level whole-record conflict rule selects the existing value.
    /// Preserve any current journal, or create fresh upload work when none
    /// exists, without advancing the selected record's change timestamps.
    /// Unlike immutable audit preservation, the server needs this value.
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

/// Authority-dependent replacement selection against the actual target Realm.
/// Runs for unseen objects too, after namespace admission and before mutation.
/// Implementations must not suspend or mutate.
public protocol BigSyncInboundSemanticTargetValidating {
    static func inboundSemanticTargetDisposition(
        _ record: CKRecord, existingObject: Object?, in realm: Realm
    ) throws -> BigSyncInboundSemanticReplacementDisposition
}

/// A missing local authority is retryable context, never remote corruption.
public struct BigSyncInboundSemanticContextUnavailable: Error, Sendable {
    public init() {}
}

/// Checks the actual local target before serializing it for upload.
public protocol BigSyncOutboundSemanticObjectValidating {
    func validateOutboundSemanticObject(in realm: Realm) throws
}

/// Opts structural tombstones into live CloudKit upserts, preserving their
/// ordered fields across acknowledgement, cleanup and destination reseeding.
/// `isDeleted` remains the domain's local visibility flag; it must not become
/// a record deletion that erases the ordering evidence.
public protocol BigSyncRetainsSyncedTombstone {
    var retainsSyncedTombstone: Bool { get }
}

/// Admission fence for CloudKit record deletions whose absence would violate
/// a model's semantic authority. CloudKit deletion callbacks carry only a
/// record identifier, so authorization must be provable from that identifier
/// and the current local object. Deletion is revalidated inside the target
/// write. A semantic error quarantines it without changing the target;
/// Cancellation, unavailable local admission, and unavailable local resources
/// instead abort processing without consuming/quarantining the event. A failure
/// at the target write rolls back that physical transaction, not preceding
/// transactions in another Realm. Implementations must not mutate or suspend.
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

/// Optional, model-owned invalidation of application publication inside the
/// physical target Realm write. Runs when a selected record enters target
/// application (including custom delegates), on an applied deferred relationship,
/// and on actual soft/hard deletion. It may conservatively invalidate an
/// unchanged application, so this is NOT an exact semantic-delta/revision hook.
/// Skipped candidates, quarantine, immutable preservation and upload bookkeeping
/// do not call it. A thrown error rolls back this target transaction and aborts
/// processing; it is not evidence that the remote record is corrupt.
///
/// Implementations may update local-only state in this Realm, but must not
/// suspend, start another transaction, change received data or enqueue uploads.
public protocol BigSyncInboundTargetMutationObserving {
    static func invalidatePublicationForInboundTargetWrite(in realm: Realm) throws
}

/// Optional identity-aware form for value-only models. Called for each target
/// before generic application/deletion, with both the old object and incoming
/// fields. It is deliberately bypassed for arbitrary custom merge delegates.
/// It may only invalidate local control state, never authorize received data.
public protocol BigSyncInboundTargetIdentityObserving: BigSyncInboundTargetMutationObserving {
    static func invalidatePublicationForInboundTargetWrite(
        in realm: Realm, existingObject: Object?, incomingRecord: CKRecord?
    ) throws
}

/// Transaction-local call coalescing, not mutation history or certification.
/// Different models may own different application invalidators; invoke each
/// participating model once, without assuming they share a domain callback.
struct BigSyncInboundPublicationInvalidations {
    private var notifiedTypes = Set<String>()

    mutating func record(
        _ model: Object.Type, in realm: Realm,
        existingObject: Object? = nil, incomingRecord: CKRecord? = nil,
        allowTargeting: Bool = false
    ) throws {
        guard realm.isInWriteTransaction else {
            throw BigSyncSemanticAdmissionUnavailable(entityType: model.className())
        }
        if allowTargeting, let observer = model as? BigSyncInboundTargetIdentityObserving.Type {
            try observer.invalidatePublicationForInboundTargetWrite(
                in: realm, existingObject: existingObject, incomingRecord: incomingRecord)
            return
        }
        guard let observer = model as? BigSyncInboundTargetMutationObserving.Type,
              !notifiedTypes.contains(model.className()) else { return }
        guard realm.isInWriteTransaction else {
            throw BigSyncSemanticAdmissionUnavailable(entityType: model.className())
        }
        try observer.invalidatePublicationForInboundTargetWrite(in: realm)
        notifiedTypes.insert(model.className())
    }
}
