import CloudKit
import Foundation
import RealmSwift

/// Implemented by synchronized Realm model classes whose CloudKit payload has
/// semantic invariants that must hold before it is admitted to the target
/// Realm. Validation is intentionally performed on the record, before Realm's
/// generic property decoder can publish a partial or malformed object.
public protocol BigSyncInboundSemanticRecordValidating {
    static func validateInboundSemanticRecord(_ record: CKRecord) throws
}

/// Optional second admission fence for models whose primary key denotes one
/// immutable semantic fact. It runs after target re-resolution and before the
/// generic merge policy can replace an existing object.
public protocol BigSyncInboundSemanticReplacementValidating {
    static func validateInboundSemanticReplacement(
        _ record: CKRecord,
        existingObject: Object?
    ) throws
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
    @Persisted(primaryKey: true) var recordName = ""
    @Persisted(indexed: true) var entityType = ""
    @Persisted(indexed: true) var accountScopeIdentifier: String?
    @Persisted var recordChangeTag: String?
    @Persisted var validationCode = ""
    @Persisted var receivedRecordDigestHex = ""
    @Persisted var importRunIdentifier = ""
    @Persisted var detectedAt = Date()
}
