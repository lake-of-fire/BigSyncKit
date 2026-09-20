import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testPreparedDeleteRetiresStagedV1WithoutConsumingRecreatedV2() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        _ = try await edit(object, text: "V1 staged", time: 30, realm: realm, adapter: adapter)
        let save = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let oldCandidate = try XCTUnwrap(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity)
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.preparedRecordDeletions(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(deletion.count, 1)
        let fence = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        try realm.write {
            object.isDeleted = false
            object.text = "V2 recreated before delete reply"
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 50))
        }
        let v2 = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        XCTAssertNotEqual(v2, deletion.first?.generation)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, oldCandidate)
        try await adapter.didDelete(recordIDs: [incoming.recordID], matchingPreparedDeletions: deletion)
        XCTAssertEqual(object.text, "V2 recreated before delete reply")
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 50))
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, v2)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        XCTAssertEqual(base.revision, fence)
        XCTAssertTrue(base.isComparisonInvalidated)
        XCTAssertTrue(base.fields.count == 0)
        XCTAssertNil(base.acceptedSystemFields)
        XCTAssertNil(base.serverChangeTag)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first)
        XCTAssertEqual(tracking.entityState, .new)
        XCTAssertEqual(tracking.pendingGeneration, v2)
        XCTAssertNil(tracking.encodedRecord)
        try await adapter.didUpload(savedRecords: save.map(\.record), matchingPreparedUploads: save)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, v2)
        XCTAssertEqual(base.revision, fence)
        let (restarted, reopened) = try await restart(adapter)
        let records = try await drain(restarted, realm: reopened, expectsFreshTemplate: true)
        XCTAssertEqual(records.first?["text"] as? String, "V2 recreated before delete reply")
        // Once V2 is accepted, even the previously legitimate delete evidence
        // cannot invalidate its new baseline or schedule another deletion.
        let acceptedRevision = reopened.objects(BigSyncRecordBaseline.self).first?.revision
        try await restarted.didDelete(recordIDs: [incoming.recordID], matchingPreparedDeletions: deletion)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, acceptedRevision)
        try await quiet(restarted, realm: reopened)
    }

    @BigSyncBackgroundActor
    func testDeleteTargetFirstCrashFinishesTrackingWithoutAnotherServerDelete() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        _ = try await edit(object, text: "unresolved save", time: 30, realm: realm, adapter: adapter)
        let save = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.preparedRecordDeletions(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(deletion.count, 1)
        adapter._testAfterDisappearanceTargetWrite = { throw W1InjectedFailure.afterTarget }
        do {
            try await adapter.didDelete(recordIDs: [incoming.recordID], matchingPreparedDeletions: deletion)
            XCTFail("Expected target-first interruption")
        } catch W1InjectedFailure.afterTarget { }
        adapter._testAfterDisappearanceTargetWrite = nil
        XCTAssertTrue(object.isDeleted)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.isComparisonInvalidated, true)
        XCTAssertEqual(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first?.entityState, .deletedLocally)
        let (restarted, reopened) = try await restart(adapter)
        let retried = try await restarted.preparedRecordDeletions(limit: 50, restrictedToEntityType: nil)
        XCTAssertTrue(retried.isEmpty, "The committed target disposition only needs cache completion")
        try await restarted.didUpload(savedRecords: save.map(\.record), matchingPreparedUploads: save)
        try await restarted.cleanUp()
        XCTAssertNil(reopened.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let audit = try await restarted.auditSynchronizationState(serverRecords: [])
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        XCTAssertEqual(audit.unresolvedSubmissionCount, 0)
        try await quiet(restarted, realm: reopened)
    }

    @BigSyncBackgroundActor
    func testAdoptedDeleteCannotAcknowledgeUsingOnlyAGenerationMap() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.preparedRecordDeletions(limit: 50, restrictedToEntityType: nil)
        let generation = try XCTUnwrap(deletion.first?.generation)
        do {
            try await adapter.didDelete(recordIDs: [incoming.recordID],
                matchingGenerations: [incoming.recordID.recordName: generation])
            XCTFail("Generation-only receipts have no comparison/submission authority")
        } catch RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared { }
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        try await adapter.didDelete(recordIDs: [incoming.recordID], matchingPreparedDeletions: deletion)
        try await adapter.cleanUp()
        try await quiet(adapter, realm: realm)
    }
}
