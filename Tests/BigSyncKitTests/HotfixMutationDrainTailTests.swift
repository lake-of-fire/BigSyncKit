import CloudKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(HotfixMutationDrainTailObject)
private final class HotfixMutationDrainTailObject: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var payload = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class HotfixMutationDrainTailTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "mutation-tail-tracking-" + nonce
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "mutation-tail-target-" + nonce
        target.objectTypes = [
            HotfixMutationDrainTailObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(
                zoneName: "mutation-tail",
                ownerName: CKCurrentUserDefaultName
            ),
            logger: Logger(label: "HotfixMutationDrainTailTests"),
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("mutation-tail-assets-" + nonce)
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        return (adapter, realm)
    }

    @BigSyncBackgroundActor
    private func authoredObject(
        id: String,
        payload: String,
        adapter: RealmSwiftAdapter,
        realm: Realm
    ) async throws -> HotfixMutationDrainTailObject {
        let object = HotfixMutationDrainTailObject()
        object.id = id
        object.payload = payload
        try await realm.asyncWrite {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        return object
    }

    func testDrainContinuationRequiresRealReason() {
        XCTAssertFalse(bigSyncMutationDrainShouldContinue(
            handledFailures: 0,
            completedCount: 1,
            requestedBatchSize: 10,
            adapterHasChanges: false
        ))
        XCTAssertTrue(bigSyncMutationDrainShouldContinue(
            handledFailures: 0,
            completedCount: 1,
            requestedBatchSize: 10,
            adapterHasChanges: true
        ))
        XCTAssertTrue(bigSyncMutationDrainShouldContinue(
            handledFailures: 0,
            completedCount: 10,
            requestedBatchSize: 10,
            adapterHasChanges: false
        ))
        XCTAssertTrue(bigSyncMutationDrainShouldContinue(
            handledFailures: 1,
            completedCount: 1,
            requestedBatchSize: 10,
            adapterHasChanges: false
        ))
    }

    @BigSyncBackgroundActor
    func testUploadAcknowledgementLeavesNewerGenerationVisibleToDrain() async throws {
        let (adapter, realm) = try await fixture()
        let object = try await authoredObject(
            id: "upload-tail",
            payload: "first",
            adapter: adapter,
            realm: realm
        )
        let firstBatch = try await adapter.prepareUploadBatch(limit: 10)
        let firstRecord = try XCTUnwrap(firstBatch.records.first)
        let recordName = firstRecord.recordID.recordName
        let firstGeneration = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation)

        try await realm.asyncWrite {
            object.payload = "second"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let secondGeneration = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation)
        XCTAssertNotEqual(secondGeneration, firstGeneration)
        // Mirror the normal journal wakeup that may race the CloudKit save.
        try await adapter.didFinishImport()

        try await adapter.acknowledgeUploadedRecords(
            firstBatch.records,
            from: firstBatch
        )
        realm.refresh()
        XCTAssertEqual(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation, secondGeneration)
        XCTAssertTrue(adapter.hasChanges)
        XCTAssertTrue(bigSyncMutationDrainShouldContinue(
            handledFailures: 0,
            completedCount: firstBatch.records.count,
            requestedBatchSize: 10,
            adapterHasChanges: adapter.hasChanges
        ))

        let secondBatch = try await adapter.prepareUploadBatch(limit: 10)
        let secondRecord = try XCTUnwrap(secondBatch.records.first)
        XCTAssertEqual(secondRecord["payload"] as? String, "second")
        try await adapter.acknowledgeUploadedRecords(
            secondBatch.records,
            from: secondBatch
        )
        realm.refresh()
        XCTAssertNil(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))
    }

    @BigSyncBackgroundActor
    func testDeletionAcknowledgementLeavesNewerTombstoneGenerationVisibleToDrain() async throws {
        let (adapter, realm) = try await fixture()
        let object = try await authoredObject(
            id: "deletion-tail",
            payload: "value",
            adapter: adapter,
            realm: realm
        )
        let initial = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(initial.records, from: initial)
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)

        try await realm.asyncWrite {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let firstDeletion = try await adapter.prepareDeletionBatch(limit: 10)
        let firstRecordID = try XCTUnwrap(firstDeletion.recordIDs.first)
        let recordName = firstRecordID.recordName
        let firstGeneration = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation)

        // Model a genuine newer local mutation while the first tombstone is
        // in flight. It remains a deletion but must receive a fresh generation.
        try await realm.asyncWrite {
            object.payload = "changed-while-deleted"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let secondGeneration = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation)
        XCTAssertNotEqual(secondGeneration, firstGeneration)
        try await adapter.didFinishImport()

        try await adapter.acknowledgeDeletedRecordIDs(
            firstDeletion.recordIDs,
            from: firstDeletion
        )
        realm.refresh()
        XCTAssertEqual(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        )?.generation, secondGeneration)
        XCTAssertTrue(adapter.hasChanges)
        XCTAssertTrue(bigSyncMutationDrainShouldContinue(
            handledFailures: 0,
            completedCount: firstDeletion.recordIDs.count,
            requestedBatchSize: 10,
            adapterHasChanges: adapter.hasChanges
        ))

        let secondDeletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(secondDeletion.recordIDs, [firstRecordID])
        try await adapter.acknowledgeDeletedRecordIDs(
            secondDeletion.recordIDs,
            from: secondDeletion
        )
        realm.refresh()
        XCTAssertNil(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))
    }
}
