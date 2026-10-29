import Foundation
import CloudKit
import RealmSwift

// Detached evidence used by the adapter's existing preparation and atomic
// acknowledgement boundaries. Mutation and lifecycle fences stay private in
// RealmSwiftAdapter.swift; this support file carries no independent writer.
extension RealmSwiftAdapter {
    static func isRetainedPhysicalDeletionQuarantine(
        _ quarantine: BigSyncInboundSemanticQuarantine
    ) -> Bool {
        quarantine.eventKind == "deletion"
            && quarantine.validationCode == "retained-record-physically-deleted"
            && quarantine.semanticScopeIdentifier
                == "retained-physical-deletion:" + quarantine.recordName
    }

    /// Re-observation or page rebinding is not the same cleanup input, even
    /// when the deterministic lineage ID is reused.
    struct RetainedDeletionQuarantineObservation: Equatable, Sendable {
        let lineageID: String
        let recordName: String
        let entityType: String
        let importRunIdentifier: String
        let receivedRecordDigestHex: String
        let changeFeedEpoch: Int
        let committedPageSequence: Int64
        let committedPageReceiptID: String
        let committedPageOutcomeDigestHex: String
        let detectedAt: Date

        init(_ quarantine: BigSyncInboundSemanticQuarantine) {
            lineageID = quarantine.lineageID
            recordName = quarantine.recordName
            entityType = quarantine.entityType
            importRunIdentifier = quarantine.importRunIdentifier
            receivedRecordDigestHex = quarantine.receivedRecordDigestHex
            changeFeedEpoch = quarantine.changeFeedEpoch
            committedPageSequence = quarantine.committedPageSequence
            committedPageReceiptID = quarantine.committedPageReceiptID
            committedPageOutcomeDigestHex = quarantine.committedPageOutcomeDigestHex
            detectedAt = quarantine.detectedAt
        }
    }

    struct RetainedDeletionUploadReceipt {
        let recordID: CKRecord.ID
        let recordType: String
        let changeTag: String?
    }

    /// Local to one acknowledgement, never persisted as another recovery queue.
    struct RetainedDeletionQuarantineCleanup {
        let context: BigSyncRecordRebaseContext
        let cancellationGeneration: UInt64
        let provider: RealmProvider
        let receipts: [String: RetainedDeletionUploadReceipt]
        let candidates: [RetainedDeletionQuarantineObservation]

        func matches(_ quarantine: BigSyncInboundSemanticQuarantine) -> Bool {
            Self.matches(quarantine, receipts: receipts)
        }

        static func matches(
            _ quarantine: BigSyncInboundSemanticQuarantine,
            receipts: [String: RetainedDeletionUploadReceipt]
        ) -> Bool {
            guard RealmSwiftAdapter.isRetainedPhysicalDeletionQuarantine(quarantine),
                  let saved = receipts[quarantine.recordName],
                  saved.recordType == quarantine.entityType,
                  let tag = saved.changeTag, !tag.isEmpty else { return false }
            return true
        }
    }

    /// An observation made before transport submission. No live Realm, mutable
    /// CKRecord, persisted marker or renewal is retained.
    struct RetainedDeletionQuarantineEvidence: Sendable {
        let issuerID: UUID
        let providerID: ObjectIdentifier
        let context: BigSyncRecordRebaseContext
        let cancellationGeneration: UInt64
        let recordID: CKRecord.ID
        let recordType: String
        let generation: String
        let submissionIdentity: String?
        let candidates: [RetainedDeletionQuarantineObservation]
    }

    struct RetainedDeletionQuarantineNeedsFreshPreparation: Error, Sendable {
        let recordNames: [String]
    }
}
