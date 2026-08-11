import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

final class ChangeFeedMigrationResumeTests: XCTestCase {
    @BigSyncBackgroundActor
    func testCompletedAdapterRemainsNoOpWhenPeerResumesFinishingMigration()
    async throws {
        let account = "migration-account"
        let epoch = 17
        let completed = try makeAdapter(label: "completed")
        let unfinished = try makeAdapter(label: "unfinished")

        // Model a process death in the synchronizer-wide `.finishing` phase:
        // the first adapter has committed completion while its peer has only
        // reached the server bootstrap.
        try await completed.prepareChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await completed.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await completed.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )

        let completedPersistence = try await Realm(
            configuration: completed.persistenceRealmConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let retainedRecordName = "MigrationPeerObject.retained-tracking"
        try await completedPersistence.asyncWrite {
            completedPersistence.add(SyncedEntity(
                entityType: "MigrationPeerObject",
                identifier: retainedRecordName,
                state: SyncedEntityState.synced.rawValue
            ), update: .modified)
        }
        try await completed.finishChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )

        try await unfinished.prepareChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await unfinished.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        let unfinishedWasActive = await unfinished.isChangeFeedServerBootstrapActive()
        XCTAssertTrue(unfinishedWasActive)

        // A resumed synchronizer starts all hooks again. The completed adapter
        // must not clear its tracking Realm or reopen a finished migration.
        try await completed.prepareChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await completed.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await completed.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await completed.finishChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )

        completedPersistence.refresh()
        XCTAssertEqual(
            completedPersistence.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: retainedRecordName
            )?.entityState,
            .synced
        )
        let completedState = try XCTUnwrap(completedPersistence.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        ))
        XCTAssertFalse(completedState.isActive)
        XCTAssertEqual(completedState.phase, "complete")

        // The unfinished peer remains able to complete the original epoch.
        try await unfinished.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await unfinished.finishChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        let unfinishedIsInactive = await unfinished.isChangeFeedServerBootstrapActive()
        XCTAssertFalse(unfinishedIsInactive)
    }

    @BigSyncBackgroundActor
    private func makeAdapter(label: String) throws -> RealmSwiftAdapter {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "change-feed-resume-\(label)-persistence-\(nonce)"
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "change-feed-resume-\(label)-target-\(nonce)"
        target.objectTypes = [MigrationPeerObject.self, BigSyncPendingMutation.self]
        return RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "change-feed-resume-\(label)-\(nonce)"),
            logger: Logger(label: "ChangeFeedMigrationResumeTests"),
            startSetupTask: false
        )
    }
}

@objc(MigrationPeerObject)
final class MigrationPeerObject: Object, ChangeMetadataRecordable {
    @objc dynamic var id = ""
    @objc dynamic var createdAt = Date()
    @objc dynamic var modifiedAt = Date()
    @objc dynamic var explicitlyModifiedAt: Date?
    @objc dynamic var isDeleted = false

    override static func primaryKey() -> String? { "id" }
}

extension MigrationPeerObject: @unchecked Sendable { }
