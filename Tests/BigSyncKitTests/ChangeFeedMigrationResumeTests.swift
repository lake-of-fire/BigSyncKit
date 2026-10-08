import CloudKit
import Foundation
import Logging
import RealmSwift
@testable import RealmSwiftGaps
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
        try await activateChangeFeedNamespace(adapter, account: account)

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
        try await activateChangeFeedNamespace(completed, account: account)
        try await activateChangeFeedNamespace(unfinished, account: account)

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
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "change-feed-resume-\(label)-\(nonce)"),
            logger: Logger(label: "ChangeFeedMigrationResumeTests"),
            startSetupTask: false
        )
        let fixtureTargetConfiguration = target
        addTeardownBlock {
            await Task { @BigSyncBackgroundActor in
                adapter.cancelSynchronization()
                await adapter.waitForCancellation()
                adapter.invalidateTokens()
                adapter.realmProvider = nil
            }.value
            await Task { @RealmBackgroundActor in
                _ = await RealmBackgroundActor.shared.removeCachedRealm(for: fixtureTargetConfiguration)
            }.value
        }
        return adapter
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

    @BigSyncBackgroundActor
    private func activateChangeFeedNamespace(
        _ adapter: RealmSwiftAdapter,
        account: String
    ) async throws {
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test",
            databaseScope: .private
        )
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: account,
            replicaBindingGenerationIdentifier: nil
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

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testDurableCompletionExcludesProvisionalTerminalMarkerUntilCommit() async throws {
        let account = "completion-snapshot-account"
        let epoch = 85
        let adapter = try makeAdapter(label: "completion-snapshot")
        try await activateChangeFeedNamespace(adapter, account: account)
        try await prepareForCompletion(adapter, account: account, epoch: epoch)
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = try XCTUnwrap(realm.object(ofType: RebuildProvenanceState.self,
                                             forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        state.isActive = false
        state.phase = "complete"
        let provisional = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account, epoch: epoch, mode: .serverReconciliation)
        XCTAssertFalse(provisional, "A provisional terminal marker cannot finish the global migration")
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertEqual(state.phase, "complete")
        realm.cancelWrite()
        let rolledBack = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account, epoch: epoch, mode: .serverReconciliation)
        XCTAssertFalse(rolledBack)
        try realm.write {
            state.isActive = false
            state.phase = "complete"
        }
        let committed = try await adapter.changeFeedResetCompletionIsDurable(
            accountScopeIdentifier: account, epoch: epoch, mode: .serverReconciliation)
        XCTAssertTrue(committed)
    }

    @BigSyncBackgroundActor
    func testBackupRestoreRetiresCommittedJournalBehindRolledBackCurrentMutation() async throws {
        try await exerciseRestoreJournalSnapshot(commitSuccessor: false)
    }

    @BigSyncBackgroundActor
    func testBackupRestorePreservesCurrentMutationCommittedAfterSnapshot() async throws {
        try await exerciseRestoreJournalSnapshot(commitSuccessor: true)
    }

    @BigSyncBackgroundActor
    private func exerciseRestoreJournalSnapshot(commitSuccessor: Bool) async throws {
        let adapter = try makeAdapter(label: "restore-journal-snapshot")
        let configuration = try XCTUnwrap(adapter.targetRealmConfigurations.first)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [configuration], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "restore-current-installation",
                      replicaBindingGenerationIdentifier: "restore-current-binding")
            })
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.restore-snapshot", databaseScope: .private)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "restore-account",
                                                replicaBindingGenerationIdentifier: "restore-current-binding")
        let realm = try await Realm(configuration: configuration, actor: BigSyncBackgroundActor.shared)
        let object = MigrationPeerObject()
        object.id = "backup-object"
        let recordName = MigrationPeerObject.className() + "." + object.id
        let backupTimestamp = Date(timeIntervalSinceReferenceDate: 100)
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true, at: backupTimestamp)
            // Emulate the historical outbox copied from another installation's backup.
            let mutation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                     forPrimaryKey: recordName))
            mutation.generation = "installation:backup-installation:binding:backup-binding:historical"
            mutation.replicaBindingGenerationIdentifier = "backup-binding"
        }
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 200))
        let currentGeneration = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                          forPrimaryKey: recordName)).generation
        let reachedSnapshot = expectation(description: "restore candidate snapshot completed")
        adapter._testAfterRestoredMutationJournalSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction, "Inspection must preserve the independent owner")
            if commitSuccessor { try realm.commitWrite() } else { realm.cancelWrite() }
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterRestoredMutationJournalSnapshot = nil }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: "restore-account",
                                                epoch: 81, mode: .backupRestore)
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        XCTAssertFalse(realm.isInWriteTransaction)
        let remaining = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName)
        if commitSuccessor {
            XCTAssertEqual(remaining?.generation, currentGeneration)
            XCTAssertEqual(remaining?.replicaBindingGenerationIdentifier, "restore-current-binding")
            XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 200))
        } else {
            XCTAssertNil(remaining, "A rolled-back current edit cannot hide committed backup debt")
            XCTAssertEqual(object.modifiedAt, backupTimestamp)
        }
        XCTAssertNotNil(realm.object(ofType: MigrationPeerObject.self, forPrimaryKey: object.id))
    }

    @BigSyncBackgroundActor
    func testResetPreparationRejectsProvisionalPreparedMarkerAfterRollback() async throws {
        let adapter = try makeAdapter(label: "preparation-snapshot")
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await activateChangeFeedNamespace(adapter, account: "preparation-account")
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = RebuildProvenanceState()
        try realm.write {
            state.accountScopeIdentifier = "preparation-account"
            state.epoch = 83
            state.mode = ChangeFeedResetMode.backupRestore.rawValue
            state.isActive = true
            state.phase = "requested"
            realm.add(state)
        }
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        state.phase = "trackingReset"
        let reachedSnapshot = expectation(description: "preparation marker snapshot completed")
        adapter._testAfterChangeFeedResetPreparationSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            realm.cancelWrite()
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPreparationSnapshot = nil }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: "preparation-account",
                                                epoch: 83, mode: .backupRestore)
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: RebuildProvenanceState.self,
                                   forPrimaryKey: RebuildProvenanceState.primaryKeyValue)?.phase,
                       "trackingReset", "Rolled-back preparation cannot satisfy the durable reset boundary")
    }

    @BigSyncBackgroundActor
    func testBootstrapCannotSkipItsWriteForProvisionalCompletion() async throws {
        let account = "bootstrap-snapshot-account"
        let epoch = 87
        let adapter = try makeAdapter(label: "bootstrap-snapshot")
        try await activateChangeFeedNamespace(adapter, account: account)
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = try XCTUnwrap(realm.object(ofType: RebuildProvenanceState.self,
                                             forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        state.isActive = false
        state.phase = "complete"
        let reachedSnapshot = expectation(description: "bootstrap phase snapshot completed")
        adapter._testAfterChangeFeedResetPhaseSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            realm.cancelWrite()
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPhaseSnapshot = nil }
        try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        XCTAssertTrue(state.isActive)
        XCTAssertTrue(state.serverBootstrapStarted)
        XCTAssertEqual(state.phase, "serverBootstrap")
    }

    @BigSyncBackgroundActor
    func testFinishCannotSkipItsWriteForProvisionalCompletion() async throws {
        let account = "finish-snapshot-account"
        let epoch = 89
        let adapter = try makeAdapter(label: "finish-snapshot")
        try await activateChangeFeedNamespace(adapter, account: account)
        try await prepareForCompletion(adapter, account: account, epoch: epoch)
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = try XCTUnwrap(realm.object(ofType: RebuildProvenanceState.self,
                                             forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        state.isActive = false
        state.phase = "complete"
        let reachedSnapshot = expectation(description: "finish phase snapshot completed")
        adapter._testAfterChangeFeedResetPhaseSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            realm.cancelWrite()
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPhaseSnapshot = nil }
        try await adapter.finishChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        XCTAssertFalse(state.isActive)
        XCTAssertEqual(state.phase, "complete")
    }
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testReconciliationCannotAcceptProvisionalCompletionWithoutBootstrap() async throws {
        let account = "reconciliation-snapshot-account"
        let epoch = 91
        let adapter = try makeAdapter(label: "reconciliation-snapshot")
        try await activateChangeFeedNamespace(adapter, account: account)
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = try XCTUnwrap(realm.object(ofType: RebuildProvenanceState.self,
                                             forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        state.isActive = false
        state.phase = "complete"
        let reachedSnapshot = expectation(description: "reconciliation phase snapshot completed")
        adapter._testAfterChangeFeedResetPhaseSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            realm.cancelWrite()
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPhaseSnapshot = nil }
        do {
            try await adapter.reconcileAfterChangeFeedServerBootstrap(
                accountScopeIdentifier: account, epoch: epoch)
            XCTFail("A rolled-back completion cannot authorize reconciliation before bootstrap")
        } catch {
            XCTAssertEqual((error as NSError).domain, "BigSyncKit")
            XCTAssertEqual((error as NSError).code, 2)
        }
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        XCTAssertTrue(state.isActive)
        XCTAssertFalse(state.serverBootstrapStarted)
        XCTAssertEqual(state.phase, "trackingReset")
    }
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    private func retainedResetFixture(label: String) async throws -> (RealmSwiftAdapter, Realm, MigrationPeerObject) {
        let adapter = try makeAdapter(label: label)
        let configuration = try XCTUnwrap(adapter.targetRealmConfigurations.first)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [configuration], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "reset-current-installation",
                      replicaBindingGenerationIdentifier: "reset-current-binding")
            })
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.reset-snapshot",
                                                     databaseScope: .private)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "reset-account",
                                                replicaBindingGenerationIdentifier: "reset-current-binding")
        let realm = try await Realm(configuration: configuration, actor: BigSyncBackgroundActor.shared)
        let object = MigrationPeerObject()
        object.id = "retained-object"
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 100))
            let mutation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: MigrationPeerObject.className() + "." + object.id))
            mutation.generation = "installation:reset-old-installation:binding:reset-old-binding:historical"
            mutation.replicaBindingGenerationIdentifier = "reset-old-binding"
        }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: "reset-account",
                                                epoch: 93, mode: .encryptedDataReset)
        try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: "reset-account",
                                                        epoch: 93, mode: .encryptedDataReset)
        adapter.invalidateTokens()
        return (adapter, realm, object)
    }

    @BigSyncBackgroundActor
    func testEncryptedResetReuploadsRetainedLiveObjectBehindRolledBackDeletion() async throws {
        try await exerciseRetainedResetCandidate(commitDeletion: false)
    }

    @BigSyncBackgroundActor
    func testEncryptedResetPreservesDeletionCommittedAfterRetainedCandidateSnapshot() async throws {
        try await exerciseRetainedResetCandidate(commitDeletion: true)
    }

    @BigSyncBackgroundActor
    private func exerciseRetainedResetCandidate(commitDeletion: Bool) async throws {
        let (adapter, realm, object) = try await retainedResetFixture(label: "retained-candidate")
        let recordName = MigrationPeerObject.className() + "." + object.id
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.isDeleted = true
        object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 200))
        let deletionGeneration = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                          forPrimaryKey: recordName)).generation
        let reachedSnapshot = expectation(description: "retained target candidate snapshot completed")
        adapter._testAfterChangeFeedReconciliationTargetSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            if commitDeletion { try realm.commitWrite() } else { realm.cancelWrite() }
            reachedSnapshot.fulfill()
        }
        defer { adapter._testAfterChangeFeedReconciliationTargetSnapshot = nil }
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "reset-account",
                                                                  epoch: 93, mode: .encryptedDataReset)
        await fulfillment(of: [reachedSnapshot], timeout: 1)
        let mutation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName))
        XCTAssertEqual(mutation.replicaBindingGenerationIdentifier, "reset-current-binding")
        if commitDeletion {
            XCTAssertTrue(object.isDeleted)
            XCTAssertEqual(mutation.generation, deletionGeneration, "A committed successor retains its generation")
            XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 200))
        } else {
            XCTAssertFalse(object.isDeleted)
            XCTAssertNotEqual(mutation.generation, deletionGeneration)
            XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 100),
                           "Retained reupload must not manufacture a later user clock")
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                                           forPrimaryKey: recordName)?.pendingGeneration,
                           mutation.generation)
        }
    }

    @BigSyncBackgroundActor
    func testEstablishedServerEvidenceExcludesProvisionalMembershipUntilCommit() async throws {
        let adapter = try makeAdapter(label: "server-membership-snapshot")
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        let realm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = SyncedEntity(entityType: MigrationPeerObject.className(),
                                 identifier: MigrationPeerObject.className() + ".one",
                                 state: SyncedEntityState.new.rawValue)
        try realm.write { realm.add(entity) }
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        entity.entityState = .synced
        let provisional = try await adapter.hasChangeFeedEstablishedServerEvidence()
        XCTAssertFalse(provisional)
        XCTAssertTrue(realm.isInWriteTransaction)
        realm.cancelWrite()
        let rolledBack = try await adapter.hasChangeFeedEstablishedServerEvidence()
        XCTAssertFalse(rolledBack)
        try realm.write { entity.entityState = .synced }
        let committed = try await adapter.hasChangeFeedEstablishedServerEvidence()
        XCTAssertTrue(committed)
        realm.beginWrite()
        realm.delete(entity)
        let hiddenCommitted = try await adapter.hasChangeFeedEstablishedServerEvidence()
        XCTAssertTrue(hiddenCommitted, "Provisional removal cannot hide committed server membership")
        XCTAssertTrue(realm.isInWriteTransaction)
        realm.cancelWrite()
    }
}

@BigSyncBackgroundActor
private final class MigrationSnapshotGeneration {
    var committed: String?
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testResetTrackingPublicationUsesCommittedJournalBehindRolledBackSuccessor() async throws {
        let (adapter, realm, object) = try await retainedResetFixture(label: "tracking-journal-snapshot")
        let recordName = MigrationPeerObject.className() + "." + object.id
        let captured = MigrationSnapshotGeneration()
        let beforeTracking = expectation(description: "retained journal durable before tracking phase")
        let afterTargetSnapshot = expectation(description: "tracking phase sampled target committed version")
        adapter._testBeforeChangeFeedReconciliationTrackingWrite = {
            captured.committed = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                           forPrimaryKey: recordName)).generation
            realm.beginWrite()
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 300))
            beforeTracking.fulfill()
        }
        adapter._testAfterChangeFeedReconciliationTrackingTargetSnapshot = {
            XCTAssertTrue(realm.isInWriteTransaction)
            afterTargetSnapshot.fulfill()
        }
        defer {
            adapter._testBeforeChangeFeedReconciliationTrackingWrite = nil
            adapter._testAfterChangeFeedReconciliationTrackingTargetSnapshot = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "reset-account",
                                                                  epoch: 93, mode: .encryptedDataReset)
        await fulfillment(of: [beforeTracking, afterTargetSnapshot], timeout: 1)
        XCTAssertTrue(realm.isInWriteTransaction, "Tracking publication must preserve the independent target owner")
        realm.cancelWrite()
        let committedGeneration = try XCTUnwrap(captured.committed)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                                    forPrimaryKey: recordName)?.generation, committedGeneration)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                                       forPrimaryKey: recordName)?.pendingGeneration, committedGeneration)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 100))
    }
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testQueuedPreparationPreservesCommittedPreparedSuccessorAndProvenance() async throws {
        try await exerciseQueuedPreparationSuccessor(completed: false)
    }

    @BigSyncBackgroundActor
    func testQueuedPreparationPreservesCommittedCompleteSuccessorAndProvenance() async throws {
        try await exerciseQueuedPreparationSuccessor(completed: true)
    }

    @BigSyncBackgroundActor
    private func exerciseQueuedPreparationSuccessor(completed: Bool) async throws {
        let account = "queued-preparation-account"
        let epoch = 95
        let adapter = try makeAdapter(label: "queued-preparation-successor")
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await activateChangeFeedNamespace(adapter, account: account)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let target = try await Realm(configuration: XCTUnwrap(adapter.targetRealmConfigurations.first),
                                     actor: BigSyncBackgroundActor.shared)
        let object = MigrationPeerObject()
        object.id = "retained-backup-command"
        let recordName = MigrationPeerObject.className() + "." + object.id
        try target.write {
            target.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
            let mutation = try XCTUnwrap(target.object(ofType: BigSyncPendingMutation.self,
                                                      forPrimaryKey: recordName))
            mutation.generation = "historical-backup-generation"
        }
        let state = RebuildProvenanceState()
        try tracking.write {
            state.accountScopeIdentifier = account
            state.epoch = epoch
            state.mode = ChangeFeedResetMode.backupRestore.rawValue
            state.isActive = true
            state.phase = "requested"
            tracking.add(state)
        }
        let observed = expectation(description: "committed preparation successor arrives before admission")
        adapter._testAfterChangeFeedResetPreparationSnapshot = {
            // Model another owner committing after the read cut and before this
            // request's own write. No adapter cancellation generation changes.
            try tracking.write {
                state.isActive = !completed
                state.phase = completed ? "complete" : "trackingReset"
                Self.addQueuedMigrationSentinels(account: account, epoch: epoch, to: tracking)
            }
            observed.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPreparationSnapshot = nil }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account, epoch: epoch, mode: .backupRestore)
        await fulfillment(of: [observed], timeout: 1)
        XCTAssertEqual(state.phase, completed ? "complete" : "trackingReset")
        XCTAssertEqual(state.isActive, !completed)
        assertQueuedMigrationSentinels(in: tracking)
        XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                                     forPrimaryKey: recordName)?.generation,
                       "historical-backup-generation", "An admitted preparation no-op cannot retire target work again")
        XCTAssertFalse(tracking.isInWriteTransaction)
        XCTAssertFalse(target.isInWriteTransaction)
    }

    @BigSyncBackgroundActor
    func testQueuedBootstrapTreatsCommittedCompletionAsNoOpWithoutRetiringProof() async throws {
        try await exerciseQueuedPhaseCompletion(finishing: false)
    }

    @BigSyncBackgroundActor
    func testQueuedFinishTreatsCommittedCompletionAsNoOpWithoutRetiringProof() async throws {
        try await exerciseQueuedPhaseCompletion(finishing: true)
    }

    @BigSyncBackgroundActor
    private func exerciseQueuedPhaseCompletion(finishing: Bool) async throws {
        let account = "queued-phase-completion-account"
        let epoch = 97
        let adapter = try makeAdapter(label: "queued-phase-completion")
        try await activateChangeFeedNamespace(adapter, account: account)
        if finishing {
            try await prepareForCompletion(adapter, account: account, epoch: epoch)
        } else {
            try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let state = try XCTUnwrap(tracking.object(ofType: RebuildProvenanceState.self,
                                                 forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        let observed = expectation(description: "committed terminal successor arrives before phase admission")
        adapter._testAfterChangeFeedResetPhaseSnapshot = {
            try tracking.write {
                state.isActive = false
                state.serverBootstrapStarted = false
                state.phase = "complete"
                Self.addQueuedMigrationSentinels(account: account, epoch: epoch, to: tracking)
            }
            observed.fulfill()
        }
        defer { adapter._testAfterChangeFeedResetPhaseSnapshot = nil }
        if finishing {
            try await adapter.finishChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        } else {
            try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
        }
        await fulfillment(of: [observed], timeout: 1)
        XCTAssertEqual(state.phase, "complete")
        XCTAssertFalse(state.isActive)
        XCTAssertFalse(state.serverBootstrapStarted)
        assertQueuedMigrationSentinels(in: tracking)
        XCTAssertFalse(tracking.isInWriteTransaction)
    }

    @BigSyncBackgroundActor
    private static func addQueuedMigrationSentinels(account: String, epoch: Int, to realm: Realm) {
        precondition(realm.isInWriteTransaction)
        let provenance = RebuildProvenance()
        provenance.identifier = "queued-retained-proof"
        provenance.entityType = MigrationPeerObject.className()
        provenance.accountScopeIdentifier = account
        provenance.epoch = epoch
        provenance.hadValidServerRecord = true
        provenance.priorState = SyncedEntityState.synced.rawValue
        realm.add(provenance)
        let entity = SyncedEntity(entityType: MigrationPeerObject.className(),
                                  identifier: MigrationPeerObject.className() + ".queued-retained-tracking",
                                  state: SyncedEntityState.awaitingServerEvidence.rawValue)
        realm.add(entity)
    }

    @BigSyncBackgroundActor
    private func assertQueuedMigrationSentinels(in realm: Realm) {
        let provenance = realm.object(ofType: RebuildProvenance.self, forPrimaryKey: "queued-retained-proof")
        XCTAssertEqual(provenance?.hadValidServerRecord, true,
                       "An already-admitted successor's server proof cannot be recaptured from cleared tracking")
        XCTAssertEqual(provenance?.priorState, SyncedEntityState.synced.rawValue)
        XCTAssertEqual(realm.object(ofType: SyncedEntity.self,
                                   forPrimaryKey: MigrationPeerObject.className() + ".queued-retained-tracking")?.entityState,
                       .awaitingServerEvidence, "Idempotent phase completion cannot prune unrelated retained tracking")
    }
}

private enum MigrationCommitSettlementPhase { case bootstrap, reconcile, finish }

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testBootstrapRejectsCancellationAfterCommitSubmissionAndKeepsDurableMarker() async throws {
        try await exercisePhaseCancellationAfterCommitSubmission(.bootstrap)
    }

    @BigSyncBackgroundActor
    func testReconciliationRejectsCancellationAfterTrackingCommitSubmission() async throws {
        try await exercisePhaseCancellationAfterCommitSubmission(.reconcile)
    }

    @BigSyncBackgroundActor
    func testFinishRejectsCancellationAfterCommitSubmissionAndKeepsDurableMarker() async throws {
        try await exercisePhaseCancellationAfterCommitSubmission(.finish)
    }

    @BigSyncBackgroundActor
    private func exercisePhaseCancellationAfterCommitSubmission(_ phase: MigrationCommitSettlementPhase) async throws {
        let account = "phase-commit-settlement-account"
        let epoch = 101
        let adapter = try makeAdapter(label: "phase-commit-settlement")
        try await activateChangeFeedNamespace(adapter, account: account)
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
        if phase != .bootstrap {
            try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
        }
        if phase == .finish {
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
        }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let submitted = expectation(description: "phase commit was submitted before cancellation")
        let caller = Task { @BigSyncBackgroundActor in
            try await RealmWriteCommitObservation.$didSubmit.withValue({
                withUnsafeCurrentTask { $0?.cancel() }
                submitted.fulfill()
            }) {
                switch phase {
                case .bootstrap:
                    try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
                case .reconcile:
                    try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: account, epoch: epoch)
                case .finish:
                    try await adapter.finishChangeFeedReset(accountScopeIdentifier: account, epoch: epoch)
                }
            }
        }
        do {
            try await caller.value
            XCTFail("A durable old phase commit cannot report a current successful continuation after cancellation")
        } catch is CancellationError {}
        await fulfillment(of: [submitted], timeout: 1)
        XCTAssertTrue(caller.isCancelled)
        tracking.refresh()
        let state = try XCTUnwrap(tracking.object(ofType: RebuildProvenanceState.self,
                                                 forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        XCTAssertEqual(state.accountScopeIdentifier, account)
        XCTAssertEqual(state.epoch, epoch)
        XCTAssertEqual(state.phase, phase == .finish ? "complete" : "serverBootstrap")
        XCTAssertEqual(state.isActive, phase != .finish)
        XCTAssertEqual(state.serverBootstrapStarted, phase != .finish)
        XCTAssertFalse(tracking.isInWriteTransaction, "Rejected continuation cannot roll back a submitted durable commit")
    }
}

private final class MigrationCommitSubmissionCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0
    func next() -> Int {
        lock.withLock {
            count += 1
            return count
        }
    }
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testFencedResetCancellationAfterTrackingCommitPreservesProviderAndDurableReset() async throws {
        let account = "reset-commit-settlement-account"
        let epoch = 103
        let adapter = try makeAdapter(label: "reset-commit-settlement")
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await activateChangeFeedNamespace(adapter, account: account)
        let originalProvider = try XCTUnwrap(adapter.realmProvider)
        let tracking = try XCTUnwrap(originalProvider.persistenceRealm)
        let submissions = MigrationCommitSubmissionCounter()
        let resetSubmitted = expectation(description: "fenced tracking reset submitted before cancellation")
        let caller = Task { @BigSyncBackgroundActor in
            try await RealmWriteCommitObservation.$didSubmit.withValue({
                // The first commit requests migration; the second durably
                // captures/clears tracking in resetSyncCaches itself.
                if submissions.next() == 2 {
                    withUnsafeCurrentTask { $0?.cancel() }
                    resetSubmitted.fulfill()
                }
            }) {
                try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account,
                                                        epoch: epoch, mode: .backupRestore)
            }
        }
        do {
            try await caller.value
            XCTFail("An obsolete reset continuation cannot clear provider state or report successful preparation")
        } catch is CancellationError {}
        await fulfillment(of: [resetSubmitted], timeout: 1)
        XCTAssertTrue(caller.isCancelled)
        XCTAssertTrue(adapter.realmProvider === originalProvider,
                      "After durable submission, cancellation must fence in-memory cleanup of a newer setup owner")
        tracking.refresh()
        let state = try XCTUnwrap(tracking.object(ofType: RebuildProvenanceState.self,
                                                 forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        XCTAssertEqual(state.accountScopeIdentifier, account)
        XCTAssertEqual(state.epoch, epoch)
        XCTAssertEqual(state.phase, "trackingReset")
        XCTAssertTrue(state.isActive)
        XCTAssertFalse(state.serverBootstrapStarted)
        XCTAssertFalse(tracking.isInWriteTransaction, "The submitted reset remains durable despite rejected continuation")
    }
}

extension ChangeFeedMigrationResumeTests {
    @BigSyncBackgroundActor
    func testFencedResetPreservesPreparedSuccessorAtOwnedResetAdmission() async throws {
        try await exerciseFencedResetSuccessorAtAdmission(completed: false)
    }

    @BigSyncBackgroundActor
    func testFencedResetPreservesCompleteSuccessorAtOwnedResetAdmission() async throws {
        try await exerciseFencedResetSuccessorAtAdmission(completed: true)
    }

    @BigSyncBackgroundActor
    private func exerciseFencedResetSuccessorAtAdmission(completed: Bool) async throws {
        let account = "reset-admission-successor-account"
        let epoch = 107
        let adapter = try makeAdapter(label: "reset-admission-successor")
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await activateChangeFeedNamespace(adapter, account: account)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let target = try await Realm(configuration: XCTUnwrap(adapter.targetRealmConfigurations.first),
                                     actor: BigSyncBackgroundActor.shared)
        // Neutral retained server data is already tracked by the successor.
        // No new local mutation is introduced by this fixture row.
        let retained = MigrationPeerObject()
        retained.id = "queued-retained-tracking"
        try target.write { target.add(retained) }
        let observed = expectation(description: "successor commits before reset-owned tracking admission")
        adapter._testBeforeFencedResetTrackingWrite = {
            let state = try XCTUnwrap(tracking.object(ofType: RebuildProvenanceState.self,
                                                     forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
            XCTAssertEqual(state.phase, "requested")
            try tracking.write {
                state.isActive = !completed
                state.phase = completed ? "complete" : "trackingReset"
                Self.addQueuedMigrationSentinels(account: account, epoch: epoch, to: tracking)
            }
            observed.fulfill()
        }
        defer { adapter._testBeforeFencedResetTrackingWrite = nil }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: account,
                                                epoch: epoch, mode: .backupRestore)
        await fulfillment(of: [observed], timeout: 1)
        tracking.refresh()
        let state = try XCTUnwrap(tracking.object(ofType: RebuildProvenanceState.self,
                                                 forPrimaryKey: RebuildProvenanceState.primaryKeyValue))
        XCTAssertEqual(state.phase, completed ? "complete" : "trackingReset")
        XCTAssertEqual(state.isActive, !completed)
        assertQueuedMigrationSentinels(in: tracking)
        XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty,
                      "Successor reset proof cannot turn retained server data into fresh local upload work")
        XCTAssertFalse(tracking.isInWriteTransaction)
        XCTAssertFalse(target.isInWriteTransaction)
    }
}
