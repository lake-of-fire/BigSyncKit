import CloudKit
import RealmSwift
import XCTest

@testable import BigSyncKit

/// A deliberately minimal non-Realm adapter. It relies on the protocol's
/// default deletion-conflict hook so this test protects custom adapters from
/// accidentally falling back to an inbound model-value merge.
private final class DefaultDeletionConflictAdapter: NSObject, ModelAdapter, @unchecked Sendable {
    let recordZoneID = CKRecordZone.ID(zoneName: "default-delete-conflict-zone")
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server

    private(set) var localTombstoneGeneration = "local-delete-generation"
    private(set) var localTombstoneIsDeleted = true
    private(set) var inboundSaveCount = 0
    private(set) var deletionAcknowledgementCount = 0

    var hasChanges: Bool { true }

    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: Object) -> Bool { true }

    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {
        inboundSaveCount += records.count
        localTombstoneIsDeleted = false
        localTombstoneGeneration = "server-overwrote-local-generation"
    }

    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {}
    func persistImportedChanges() async throws {}

    @BigSyncBackgroundActor
    func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] {
        []
    }

    @BigSyncBackgroundActor
    func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {}

    @BigSyncBackgroundActor
    func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] {
        []
    }

    @BigSyncBackgroundActor
    func didDelete(
        recordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {
        deletionAcknowledgementCount += recordIDs.count
    }

    @BigSyncBackgroundActor
    func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws {}

    var serverChangeToken: RecordZoneChangeCursor? {
        get async { nil }
    }

    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func didFinishImport() async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

final class ModelAdapterDeletionConflictSafetyTests: XCTestCase {
    @BigSyncBackgroundActor
    func testDefaultDeletionConflictRebaseFailsWithoutMutatingLocalTombstone() async throws {
        let adapter = DefaultDeletionConflictAdapter()
        let recordID = CKRecord.ID(recordName: "locally-deleted", zoneID: adapter.recordZoneID)
        let serverRecord = CKRecord(recordType: "Bookmark", recordID: recordID)

        do {
            try await adapter.rebasePendingDeletionMetadata(
                using: [serverRecord],
                matchingPreparedGenerations: [recordID.recordName: "local-delete-generation"]
            )
            XCTFail("The default hook must reject a delete-side server-record conflict")
        } catch {
            XCTAssertEqual(
                error as? ModelAdapterDeletionConflictError,
                .tombstonePreservingRebaseNotImplemented
            )
        }

        XCTAssertTrue(adapter.localTombstoneIsDeleted)
        XCTAssertEqual(adapter.localTombstoneGeneration, "local-delete-generation")
        XCTAssertEqual(adapter.inboundSaveCount, 0)
        XCTAssertEqual(adapter.deletionAcknowledgementCount, 0)
    }
}
