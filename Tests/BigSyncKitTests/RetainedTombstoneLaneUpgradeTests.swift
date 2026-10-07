import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

private enum LaneUpgradePolicy {
    // Changing only the model-owned deletion declaration reproduces an app
    // upgrade without altering a journal or tracking row by hand.
    @TaskLocal static var retainsTombstones = false
}

@objc(RetainedTombstoneLaneUpgradeRow)
private final class RetainedTombstoneLaneUpgradeRow: Object,
    ChangeMetadataRecordable, BigSyncRetainsSyncedTombstone {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    var retainsSyncedTombstone: Bool { LaneUpgradePolicy.retainsTombstones }
    @Persisted(primaryKey: true) var id = "edge"
    @Persisted var payload = "association"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class RetainedTombstoneLaneUpgradeTests: XCTestCase {
    @BigSyncBackgroundActor
    private lazy var fixtureOwner = RealmAdapterFixtureOwner(testCase: self)

    @BigSyncBackgroundActor
    private func adapter(target: Realm.Configuration, tracking: Realm.Configuration,
                         assets: URL) async throws -> RealmSwiftAdapter {
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: [],
            recordZoneID: .init(zoneName: "retained-lane-upgrade"),
            logger: Logger(label: "RetainedLaneUpgrade"), startSetupTask: false,
            assetDirectoryURL: assets)
        fixtureOwner.own(adapter)
        try await adapter.didFinishImport()
        adapter.invalidateTokens()
        return adapter
    }

    @BigSyncBackgroundActor
    private func fixture() async throws
        -> (RealmSwiftAdapter, Realm.Configuration, Realm.Configuration, URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("retained-lane-upgrade-" + UUID().uuidString,
                                    isDirectory: true)
        try FileManager.default.createDirectory(at: directory,
                                               withIntermediateDirectories: true)
        fixtureOwner.ownDirectory(directory)
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [RetainedTombstoneLaneUpgradeRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target])
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent("tracking.realm")
        let assets = directory.appendingPathComponent("assets", isDirectory: true)
        return (try await adapter(target: target, tracking: tracking, assets: assets),
                target, tracking, assets)
    }

    private func response(_ record: CKRecord) throws -> CKRecord {
        // Preserve the actual serialized record; no synthetic Apple change tag
        // or manual metadata row stands in for generation acknowledgment.
        let bytes = try NSKeyedArchiver.archivedData(withRootObject: record,
                                                   requiringSecureCoding: true)
        return try XCTUnwrap(NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self,
                                                              from: bytes))
    }

    @BigSyncBackgroundActor
    private func queuedPhysicalTombstone(_ adapter: RealmSwiftAdapter) async throws -> String {
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        let object = RetainedTombstoneLaneUpgradeRow()
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let live = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        XCTAssertEqual(live.count, 1)
        try await adapter.didUpload(savedRecords: try live.map { try response($0.record) },
                                    matchingPreparedUploads: live)
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.preparedRecordDeletions(limit: 10, restrictedToEntityType: nil)
        XCTAssertEqual(deletion.count, 1)
        let generation = try XCTUnwrap(deletion.first?.generation)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertEqual(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first?.entityState,
                       .deletedLocally)
        return generation
    }

    @BigSyncBackgroundActor
    private func verifyRetainedDrain(_ adapter: RealmSwiftAdapter, generation: String) async throws {
        try await adapter.didFinishImport()
        let deletes = try await adapter.preparedRecordDeletions(limit: 10, restrictedToEntityType: nil)
        XCTAssertTrue(deletes.isEmpty)
        let saved = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        XCTAssertEqual(saved.count, 1)
        let item = try XCTUnwrap(saved.first)
        XCTAssertEqual(item.generation, generation,
                       "A transport-lane upgrade must retain the original user mutation generation")
        XCTAssertEqual((item.record["isDeleted"] as? NSNumber)?.boolValue, true)
        try await adapter.didUpload(savedRecords: [try response(item.record)], matchingPreparedUploads: saved)
        try await adapter.cleanUp()
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        XCTAssertEqual(realm.object(ofType: RetainedTombstoneLaneUpgradeRow.self,
                                   forPrimaryKey: "edge")?.isDeleted, true)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testRetainedAdoptionRemapsAlreadyForwardedDeletionWithoutReplacingGeneration() async throws {
        let (adapter, _, _, _) = try await fixture()
        let generation = try await queuedPhysicalTombstone(adapter)
        try await LaneUpgradePolicy.$retainsTombstones.withValue(true) {
            try await verifyRetainedDrain(adapter, generation: generation)
        }
    }

    @BigSyncBackgroundActor
    func testPhysicalDeletionReceiptPreparedBeforeRetainedAdoptionCannotConsumeTombstone() async throws {
        let (adapter, _, _, _) = try await fixture()
        let generation = try await queuedPhysicalTombstone(adapter)
        let oldDeletions = try await adapter.preparedRecordDeletions(limit: 10, restrictedToEntityType: nil)
        let oldID = try XCTUnwrap(oldDeletions.first?.recordID)
        try await LaneUpgradePolicy.$retainsTombstones.withValue(true) {
            do {
                try await adapter.didDelete(recordIDs: [oldID], matchingPreparedDeletions: oldDeletions)
                XCTFail("The obsolete physical deletion receipt has no authority over a retained tombstone")
            } catch BigSyncRecordContractError.unexpectedPhysicalDeletion(let name) {
                XCTAssertEqual(name, oldID.recordName)
            }
            let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
            XCTAssertEqual(realm.object(ofType: RetainedTombstoneLaneUpgradeRow.self,
                                       forPrimaryKey: "edge")?.isDeleted, true)
            try await verifyRetainedDrain(adapter, generation: generation)
        }
    }

    @BigSyncBackgroundActor
    func testRestartWithRetainedAdoptionRecoversDurablePhysicalDeletionLane() async throws {
        let (original, target, tracking, assets) = try await fixture()
        let generation = try await queuedPhysicalTombstone(original)
        original.cancelSynchronization()
        await original.waitForCancellation()
        original.invalidateTokens()
        try await LaneUpgradePolicy.$retainsTombstones.withValue(true) {
            let restarted = try await adapter(target: target, tracking: tracking, assets: assets)
            try await verifyRetainedDrain(restarted, generation: generation)
        }
    }
}
