import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(W1UploadSnapshotRow)
final class W1UploadSnapshotRow: Object, ChangeMetadataRecordable {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "upload-snapshot"
    @Persisted var title = "committed"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@BigSyncBackgroundActor
private final class W1MissingUploadTargetSignal {
    private(set) var reached = false
    private var released = false
    private var waiter: CheckedContinuation<Void, Never>?

    func reach() {
        reached = true
        release()
    }

    func release() {
        released = true
        waiter?.resume()
        waiter = nil
    }

    func wait() async {
        guard !released else { return }
        await withCheckedContinuation { waiter = $0 }
    }
}

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    private func uploadSnapshotFixture() async throws
        -> (RealmSwiftAdapter, Realm, W1UploadSnapshotRow, String, String) {
        let (adapter, realm) = try await fixture()
        let row = W1UploadSnapshotRow()
        try realm.write {
            realm.add(row)
            row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let name = W1UploadSnapshotRow.className() + "." + row.id
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                   forPrimaryKey: name)?.generation)
        return (adapter, realm, row, name, generation)
    }

    @BigSyncBackgroundActor
    func testUncontractedUploadIgnoresProvisionalTargetPayloadAndDeletion() async throws {
        for removesTarget in [false, true] {
            for commits in [false, true] {
                let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
                realm.beginWrite()
                defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
                if removesTarget { realm.delete(row) }
                else {
                    row.title = "provisional"
                    row.refreshChangeMetadata(explicitlyModified: true,
                        at: Date(timeIntervalSinceReferenceDate: 20))
                }
                let selected = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(selected.count, 1)
                XCTAssertEqual(selected.first?.record["title"] as? String, "committed")
                XCTAssertEqual(selected.first?.generation, generation)
                XCTAssertTrue(realm.isInWriteTransaction, "Selection must preserve the independent owner")
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(
                    ofType: SyncedEntity.self, forPrimaryKey: name))
                XCTAssertEqual(tracking.entityState, .new,
                    "Provisional absence must not become a CloudKit deletion")
                if commits { try realm.commitWrite() } else { realm.cancelWrite() }
                let after = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                if removesTarget && commits {
                    XCTAssertTrue(after.isEmpty)
                    XCTAssertEqual(tracking.entityState, .deletedLocally)
                } else {
                    XCTAssertEqual(after.first?.record["title"] as? String,
                        commits ? "provisional" : "committed")
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testUncontractedUploadIgnoresProvisionalTrackingGenerationAndRemoval() async throws {
        for removesTracking in [false, true] {
            for commits in [false, true] {
                let (adapter, _, _, name, generation) = try await uploadSnapshotFixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let row = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if removesTracking { tracking.delete(row) }
                else { row.pendingGeneration = "provisional-generation" }
                let selected = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(selected.count, 1)
                XCTAssertEqual(selected.first?.generation, generation)
                XCTAssertEqual(selected.first?.record["title"] as? String, "committed")
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                let after = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                if removesTracking && commits { XCTAssertTrue(after.isEmpty) }
                else {
                    XCTAssertEqual(after.first?.generation,
                        commits ? "provisional-generation" : generation)
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testMissingUploadTargetSerializerDoesNotWriteTracking() async throws {
        let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
        try realm.write { realm.delete(row) }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        XCTAssertNil(try adapter.recordToUpload(syncedEntity: entity, isDummyRecord: false))
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertEqual(entity.entityState, .new)
        XCTAssertEqual(entity.pendingGeneration, generation)
        tracking.cancelWrite()
        let selected = try await adapter.preparedRecordsToUpload(limit: 10,
            restrictedToEntityType: W1UploadSnapshotRow.className())
        XCTAssertTrue(selected.isEmpty)
        XCTAssertEqual(entity.entityState, .deletedLocally)
        XCTAssertEqual(entity.pendingGeneration, generation)
    }
    @BigSyncBackgroundActor
    func testMissingUploadTargetRechecksReappearanceAfterTrackingOwnershipWait() async throws {
        let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
        try realm.write { realm.delete(row) }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        let signal = W1MissingUploadTargetSignal()
        adapter._testBeforeMissingUploadTargetTrackingWrite = { signal.reach() }
        defer { adapter._testBeforeMissingUploadTargetTrackingWrite = nil }
        let preparation = Task { @BigSyncBackgroundActor in
            defer { signal.release() }
            return try await adapter.preparedRecordsToUpload(limit: 10,
                restrictedToEntityType: W1UploadSnapshotRow.className())
        }
        await signal.wait()
        XCTAssertTrue(signal.reached, "The serializer must observe durable absence before the wait")
        XCTAssertTrue(tracking.isInWriteTransaction)
        let restored = W1UploadSnapshotRow()
        restored.title = "restored while tracking waited"
        try realm.write { realm.add(restored) }
        try tracking.commitWrite()
        let selected = try await preparation.value
        XCTAssertTrue(selected.isEmpty, "This selection began with an absent target")
        XCTAssertEqual(entity.entityState, .new, "A reappeared target must not become a deletion")
        XCTAssertEqual(entity.pendingGeneration, generation)
        let retry = try await adapter.preparedRecordsToUpload(limit: 10,
            restrictedToEntityType: W1UploadSnapshotRow.className())
        XCTAssertEqual(retry.first?.record["title"] as? String, restored.title)
    }

}
