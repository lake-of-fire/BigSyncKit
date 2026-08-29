import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

final class ChangeFeedMigrationResumeTests: XCTestCase {
    @BigSyncBackgroundActor
    func testCompletedResetPrunesOnlyQuarantineAbsentFromItsNamespace()
    async throws {
        let account = "quarantine-account"
        let epoch = 41
        let binding = "current-binding"
        let container = "iCloud.test.quarantine"
        let adapter = try makeAdapter(label: "quarantine-retention")
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: account,
            replicaBindingGenerationIdentifier: binding
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: container,
            databaseScope: .private
        )
        try await prepareForCompletion(
            adapter,
            account: account,
            epoch: epoch
        )
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let zone = adapter.recordZoneID
        try await realm.asyncWrite {
            addQuarantine(
                id: "current",
                account: account,
                container: container,
                zone: zone,
                binding: binding,
                epoch: epoch,
                withReceipt: true,
                to: realm
            )
            addQuarantine(
                id: "old-epoch",
                account: account,
                container: container,
                zone: zone,
                binding: binding,
                epoch: epoch - 1,
                withReceipt: true,
                to: realm
            )
            addQuarantine(
                id: "old-binding",
                account: account,
                container: container,
                zone: zone,
                binding: "old-binding",
                epoch: epoch,
                withReceipt: true,
                to: realm
            )
            addQuarantine(
                id: "uncommitted-current",
                account: account,
                container: container,
                zone: zone,
                binding: binding,
                epoch: epoch,
                withReceipt: false,
                to: realm
            )
            addQuarantine(
                id: "other-account",
                account: "other-account",
                container: container,
                zone: zone,
                binding: binding,
                epoch: epoch,
                withReceipt: false,
                to: realm
            )
            addQuarantine(
                id: "other-zone",
                account: account,
                container: container,
                zone: CKRecordZone.ID(
                    zoneName: "other-zone",
                    ownerName: zone.ownerName
                ),
                binding: binding,
                epoch: epoch,
                withReceipt: false,
                to: realm
            )
        }

        adapter._testBeforeChangeFeedResetCompletionMarkerWrite = {
            throw NSError(domain: "rollback", code: 1)
        }
        do {
            try await adapter.finishChangeFeedReset(
                accountScopeIdentifier: account,
                epoch: epoch
            )
            XCTFail("Expected the completion transaction to roll back")
        } catch {
            // Expected injected failure.
        }
        realm.refresh()
        XCTAssertEqual(
            Set(realm.objects(BigSyncInboundSemanticQuarantine.self)
                .map(\.lineageID)),
            [
                "current", "old-epoch", "old-binding",
                "uncommitted-current", "other-account", "other-zone",
            ]
        )

        adapter._testBeforeChangeFeedResetCompletionMarkerWrite = nil
        try await adapter.finishChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        realm.refresh()
        XCTAssertEqual(
            Set(realm.objects(BigSyncInboundSemanticQuarantine.self)
                .map(\.lineageID)),
            ["current", "other-account", "other-zone"]
        )
        XCTAssertNotNil(realm.object(
            ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: "receipt-current"
        ))
        for retiredID in ["old-epoch", "old-binding"] {
            XCTAssertNil(realm.object(
                ofType: BigSyncInboundPageReceipt.self,
                forPrimaryKey: "receipt-\(retiredID)"
            ))
        }
    }

    @BigSyncBackgroundActor
    func testDurableCompletionRequiresExactTerminalProvenance() async throws {
        let account = "durable-completion-account"
        let epoch = 29
        let adapter = try makeAdapter(label: "durable-completion")

        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )
        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )
        let incomplete = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )
        XCTAssertFalse(incomplete)

        try await adapter.finishChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )

        let completed = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .encryptedDataReset
        )
        let wrongAccount = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: "another-account",
            epoch: epoch,
            mode: .encryptedDataReset
        )
        let wrongEpoch = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account,
            epoch: epoch + 1,
            mode: .encryptedDataReset
        )
        let wrongMode = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account,
            epoch: epoch,
            mode: .backupRestore
        )
        XCTAssertTrue(completed)
        XCTAssertFalse(wrongAccount)
        XCTAssertFalse(wrongEpoch)
        XCTAssertFalse(wrongMode)
    }

    @BigSyncBackgroundActor
    func testDurableCompletionThrowsWhenPersistenceRealmCannotOpen() async throws {
        let nonce = UUID().uuidString
        let persistenceDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "change-feed-unopenable-persistence-\(nonce)",
                isDirectory: true
            )
        try FileManager.default.createDirectory(
            at: persistenceDirectory,
            withIntermediateDirectories: true
        )
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.fileURL = persistenceDirectory
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "change-feed-unopenable-target-\(nonce)"
        target.objectTypes = [MigrationPeerObject.self, BigSyncPendingMutation.self]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(
                zoneName: "change-feed-unopenable-\(nonce)"
            ),
            logger: Logger(label: "ChangeFeedMigrationResumeTests"),
            startSetupTask: false
        )

        do {
            _ = try await adapter.changeFeedResetCompletionIsDurable(
                accountScopeIdentifier: "account",
                epoch: 1,
                mode: .initialImport
            )
            XCTFail("An unavailable persistence Realm cannot prove completion")
        } catch {
            XCTAssertFalse(error is CancellationError)
        }
    }

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

    @BigSyncBackgroundActor
    private func prepareForCompletion(
        _ adapter: RealmSwiftAdapter,
        account: String,
        epoch: Int
    ) async throws {
        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: account,
            epoch: epoch
        )
    }

    private func addQuarantine(
        id: String,
        account: String,
        container: String,
        zone: CKRecordZone.ID,
        binding: String,
        epoch: Int,
        withReceipt: Bool,
        to realm: Realm
    ) {
        precondition(realm.isInWriteTransaction)
        let quarantine = BigSyncInboundSemanticQuarantine()
        quarantine.lineageID = id
        quarantine.recordName = "record-\(id)"
        quarantine.entityType = MigrationPeerObject.className()
        quarantine.accountScopeIdentifier = account
        quarantine.containerIdentifier = container
        quarantine.databaseScopeRawValue = CKDatabase.Scope.private.rawValue
        quarantine.zoneOwnerName = zone.ownerName
        quarantine.zoneName = zone.zoneName
        quarantine.eventKind = "live"
        quarantine.replicaActivationIdentifier = binding
        quarantine.changeFeedEpoch = epoch
        quarantine.validationCode = "test"
        if withReceipt {
            let receiptID = "receipt-\(id)"
            let outcomeDigest = String(repeating: "a", count: 64)
            quarantine.committedPageSequence = 1
            quarantine.committedPageReceiptID = receiptID
            quarantine.committedPageOutcomeDigestHex = outcomeDigest

            let receipt = BigSyncInboundPageReceipt()
            receipt.id = receiptID
            receipt.accountScopeIdentifier = account
            receipt.containerIdentifier = container
            receipt.databaseScopeRawValue = CKDatabase.Scope.private.rawValue
            receipt.zoneOwnerName = zone.ownerName
            receipt.zoneName = zone.zoneName
            receipt.replicaActivationIdentifier = binding
            receipt.changeFeedEpoch = epoch
            receipt.pageSequence = 1
            receipt.outcomeDigestHex = outcomeDigest
            realm.add(receipt)
        }
        realm.add(quarantine)
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
