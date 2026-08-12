import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

private final class AccountFencingStore: NSObject, KeyValueStore {
    private var values = [String: Any]()
    var synchronizesDurably = true

    func object(forKey defaultName: String) -> Any? { values[defaultName] }
    func bool(forKey defaultName: String) -> Bool {
        values[defaultName] as? Bool ?? false
    }
    func set(value: Any?, forKey defaultName: String) {
        values[defaultName] = value
    }
    func set(boolValue: Bool, forKey defaultName: String) {
        values[defaultName] = boolValue
    }
    func removeObject(forKey defaultName: String) {
        values.removeValue(forKey: defaultName)
    }
    func synchronize() -> Bool { synchronizesDurably }

    override func value(forKey key: String) -> Any? { values[key] }

    func valuesWithPrefix(_ prefix: String) -> [String: Any] {
        values.filter { $0.key.hasPrefix(prefix) }
    }
}

private final class AccountFencingTransport:
    NSObject,
    CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore,
    CloudKitZoneStore,
    CloudKitRecordStore,
    CloudKitChangeFeed,
    @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    var nextDatabaseChangesError: Error?
    private(set) var subscriptionFetchCount = 0
    private(set) var subscriptionSaveCount = 0
    private(set) var zoneFetchCount = 0
    private(set) var zoneSaveCount = 0
    private(set) var recordMutationCount = 0
    private(set) var databaseChangeFetchCount = 0
    private(set) var zoneChangeFetchCount = 0

    var operationCount: Int {
        subscriptionFetchCount + subscriptionSaveCount + zoneFetchCount +
        zoneSaveCount + recordMutationCount + databaseChangeFetchCount +
        zoneChangeFetchCount
    }

    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        subscriptionFetchCount += 1
        return nil
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        subscriptionSaveCount += 1
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {}

    func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        zoneFetchCount += 1
        return CKRecordZone(zoneID: identifier)
    }

    func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        zoneSaveCount += 1
        return recordZone
    }

    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {}

    func modifyRecords(
        saving recordsToSave: [CKRecord],
        deleting recordIDsToDelete: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        recordMutationCount += 1
        return .init(saveResults: [:], deleteResults: [:])
    }

    func databaseChanges(
        since cursor: DatabaseChangeCursor?,
        resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        databaseChangeFetchCount += 1
        if let nextDatabaseChangesError {
            self.nextDatabaseChangesError = nil
            throw nextDatabaseChangesError
        }
        return .init(
            cursor: DatabaseChangeCursor(serializedData: Data("account-fencing-db".utf8)),
            changedZoneIDs: [],
            deletions: [],
            moreComing: false
        )
    }

    func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        zoneChangeFetchCount += 1
        return .init(
            cursor: RecordZoneChangeCursor(serializedData: Data("account-fencing-zone".utf8)),
            records: [],
            deletedRecordIDs: [],
            moreComing: false
        )
    }
}

private final class AccountFencingModelAdapter:
    NSObject,
    ModelAdapter,
    ChangeFeedResetMigrating,
    TerminalSynchronizationStateModelAdapter,
    @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    private(set) var resetSyncCachesCount = 0
    var requestsOneUploadWakeupOnFinish = false
    private var rebuildIsActive = false

    init(zoneID: CKRecordZone.ID) {
        recordZoneID = zoneID
    }

    var hasChanges: Bool { false }
    func cleanUp() async throws {}
    func resetSyncCaches() async throws { resetSyncCachesCount += 1 }
    func prepareChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        _ = accountScopeIdentifier
        _ = epoch
        _ = mode
        rebuildIsActive = true
        try await resetSyncCaches()
    }
    func beginChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        _ = accountScopeIdentifier
        _ = epoch
        _ = mode
    }
    func isChangeFeedServerBootstrapActive() async -> Bool {
        rebuildIsActive
    }
    func reconcileAfterChangeFeedServerBootstrap(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        _ = accountScopeIdentifier
        _ = epoch
        _ = mode
    }
    func finishChangeFeedReset(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws {
        _ = accountScopeIdentifier
        _ = epoch
        _ = mode
        rebuildIsActive = false
    }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {}
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {}
    func persistImportedChanges() async throws {}
    func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] { [] }
    func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {}
    func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] { [] }
    func didDelete(
        recordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {}
    func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func didFinishImport() async throws {
        if requestsOneUploadWakeupOnFinish {
            requestsOneUploadWakeupOnFinish = false
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool { false }
}

private actor AccountFencingStatusSequence {
    private var values: [CKAccountStatus]

    init(_ values: [CKAccountStatus]) {
        self.values = values
    }

    func next() -> CKAccountStatus {
        guard values.count > 1 else { return values[0] }
        return values.removeFirst()
    }
}

final class CloudKitSynchronizerAccountFencingTests: XCTestCase {
    @BigSyncBackgroundActor
    func testUnavailableAccountStatusPreventsEveryCloudKitOperation() async {
        let transport = AccountFencingTransport()
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountStatusProvider: { .temporarilyUnavailable }
        )
        synchronizer.addModelAdapter(
            AccountFencingModelAdapter(zoneID: makeZoneID())
        )

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected unavailable account to stop synchronization")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .accountTemporarilyUnavailable)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAvailableDifferentAccountResetsMetadataBeforeIgnoringOldTerminal()
    async throws {
        let identifier = "account-fencing-\(UUID().uuidString)"
        let store = AccountFencingStore()
        let transport = AccountFencingTransport()
        let statuses = AccountFencingStatusSequence([
            .temporarilyUnavailable,
            .available,
        ])
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            accountIdentifierProvider: { "account-b" },
            accountStatusProvider: { await statuses.next() }
        )
        let adapter = AccountFencingModelAdapter(zoneID: zoneID)
        synchronizer.addModelAdapter(adapter)

        let oldScope = CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        store.set(value: "account-a", forKey: synchronizer.durableStateKey("CloudKitAccountIdentifier"))
        try synchronizer.markConfiguredZoneTerminal(
            zoneID,
            kind: .purged,
            accountScopeIdentifier: oldScope
        )

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected first unavailable account attempt to fail")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .accountTemporarilyUnavailable)
        }
        XCTAssertEqual(transport.operationCount, 0)

        _ = try await synchronizer.synchronize()

        XCTAssertEqual(adapter.resetSyncCachesCount, 1)
        XCTAssertGreaterThan(transport.subscriptionSaveCount, 0)
        XCTAssertGreaterThan(transport.databaseChangeFetchCount, 0)
        XCTAssertFalse(synchronizer.cancelledDueToUnauthentication)
    }

    @BigSyncBackgroundActor
    func testTemporaryAccountErrorDoesNotScheduleGenericTransportRetry()
    async throws {
        let transport = AccountFencingTransport()
        transport.nextDatabaseChangesError = CKError(.accountTemporarilyUnavailable)
        let synchronizer = makeSynchronizer(transport: transport)
        let adapter = AccountFencingModelAdapter(zoneID: makeZoneID())
        synchronizer.addModelAdapter(adapter)
        adapter.requestsOneUploadWakeupOnFinish = true

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected temporary account failure")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .accountTemporarilyUnavailable)
        }
        XCTAssertEqual(transport.databaseChangeFetchCount, 1)

        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.databaseChangeFetchCount, 1)

        _ = try await synchronizer.synchronize()
        // The explicit successful attempt performs its normal fetch plus the
        // post-upload quiescence fetch. Neither came from a timer retry.
        XCTAssertEqual(transport.databaseChangeFetchCount, 3)
    }

    @BigSyncBackgroundActor
    func testDurableNamespaceSeparatesContainerAndZoneIdentity() async {
        let firstZone = CKRecordZone.ID(
            zoneName: "zone-a",
            ownerName: CKCurrentUserDefaultName
        )
        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            identifier: "shared-identifier",
            containerIdentifier: "iCloud.one",
            recordZoneID: firstZone
        )
        let second = makeSynchronizer(
            transport: AccountFencingTransport(),
            identifier: "shared-identifier",
            containerIdentifier: "iCloud.two",
            recordZoneID: firstZone
        )
        let third = makeSynchronizer(
            transport: AccountFencingTransport(),
            identifier: "shared-identifier",
            containerIdentifier: "iCloud.one",
            recordZoneID: CKRecordZone.ID(
                zoneName: "zone-b",
                ownerName: CKCurrentUserDefaultName
            )
        )

        XCTAssertNotEqual(first.durableStateNamespace, second.durableStateNamespace)
        XCTAssertNotEqual(first.durableStateNamespace, third.durableStateNamespace)
        XCTAssertTrue(first.durableStateNamespace.hasPrefix("BigSyncKit.v3."))
    }

    @BigSyncBackgroundActor
    func testProductionSynchronizerAcceptsOnlyItsConstructorBoundZone() {
        let transport = AccountFencingTransport()
        let configuredZone = makeZoneID()
        let otherZone = makeZoneID()
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.account-fencing",
            database: transport,
            recordZoneID: configuredZone,
            keyValueStore: AccountFencingStore(),
            accountIdentifierProvider: { "account-a" },
            accountStatusProvider: { .available },
            changeFeed: transport,
            subscriptionStore: transport,
            zoneStore: transport,
            recordStore: transport,
            logger: Logger(label: "BigSyncKitTests")
        )

        XCTAssertTrue(synchronizer.canAddModelAdapter(
            AccountFencingModelAdapter(zoneID: configuredZone)
        ))
        XCTAssertFalse(synchronizer.canAddModelAdapter(
            AccountFencingModelAdapter(zoneID: otherZone)
        ))
    }

    @BigSyncBackgroundActor
    func testZoneBoundClientsDoNotShareDurableCursorRetryHealthOrLifecycle()
    async throws {
        let store = AccountFencingStore()
        let identifier = "shared-identifier"
        let zoneA = CKRecordZone.ID(
            zoneName: "zone-a",
            ownerName: CKCurrentUserDefaultName
        )
        let zoneB = CKRecordZone.ID(
            zoneName: "zone-b",
            ownerName: CKCurrentUserDefaultName
        )
        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneA
        )
        let second = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneB
        )
        let scope = CloudKitSynchronizer.accountScopeIdentifier(
            for: "account-a"
        )
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: "account-a",
            accountScopeIdentifier: scope
        )

        first.storedDatabaseToken = DatabaseChangeCursor(
            serializedData: Data("zone-a-cursor".utf8)
        )
        first.persistTransientRetryState(
            context: context,
            notBefore: Date().addingTimeInterval(600),
            consecutiveFailures: 2
        )
        try first._test_recordSyncHealth(
            .failed,
            accountIdentifier: "account-a",
            now: Date()
        )
        try first.markConfiguredZoneEstablished(
            zoneA,
            accountScopeIdentifier: scope
        )
        try first.requestChangeFeedRecovery(context: context)

        XCTAssertNil(second.storedDatabaseToken)
        XCTAssertNil(second._test_persistedTransientRetryNotBefore(
            accountScopeIdentifier: scope
        ))
        let secondHealth = try await second.syncHealthSnapshot()
        XCTAssertNil(secondHealth)
        XCTAssertNil(store.value(forKey: second.durableStateKey(
            "ZoneLifecycle.v3.\(scope).\(zoneB.ownerName).\(zoneB.zoneName)"
        )))
        XCTAssertNil(store.value(forKey: second.durableStateKey(
            "ChangeFeedMigration.v2.\(scope).\(zoneB.ownerName).\(zoneB.zoneName)"
        )))

        XCTAssertNotNil(first.storedDatabaseToken)
        XCTAssertNotNil(first._test_persistedTransientRetryNotBefore(
            accountScopeIdentifier: scope
        ))
        let firstHealth = try await first.syncHealthSnapshot()
        XCTAssertNotNil(firstHealth)
    }

    @BigSyncBackgroundActor
    func testAmbiguousLegacyStateIsIgnoredByZoneBoundNamespace() async throws {
        let store = AccountFencingStore()
        let transport = AccountFencingTransport()
        let zoneID = makeZoneID()
        let identifier = "legacy-ignored-\(UUID().uuidString)"
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { "account-a" }
        )
        let adapter = AccountFencingModelAdapter(zoneID: zoneID)
        synchronizer.addModelAdapter(adapter)
        let scope = CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        store.set(
            value: "account-a",
            forKey: "\(identifier).BigSyncKitCloudKitAccountIdentifier"
        )
        let legacyPrefix = "\(identifier).BigSyncKit.ZoneLifecycle.v1.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        store.set(boolValue: true, forKey: "\(legacyPrefix).established")
        store.set(
            value: CloudKitZoneDeletionKind.purged.rawValue,
            forKey: "\(legacyPrefix).terminal"
        )
        store.set(
            value: ["version": 1, "phase": "prepared", "epoch": 7],
            forKey: "\(identifier).BigSyncKit.ChangeFeedMigration.v1.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        )

        try await synchronizer._test_validateSynchronizationAccount()

        XCTAssertEqual(adapter.resetSyncCachesCount, 0)
        XCTAssertEqual(transport.operationCount, 0)
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "ZoneLifecycle.v3.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        )))
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "ChangeFeedMigration.v2.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        )))
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
        XCTAssertFalse(store.bool(forKey:
            "\(identifier).BigSyncKit.LegacyStateImport.v1.\(scope).\(zoneID.ownerName).\(zoneID.zoneName).consumed"
        ))
    }

    @BigSyncBackgroundActor
    func testAccountReplacementDoesNotPublishNewAccountWhenRecoveryEnvelopeIsNotDurable()
    async throws {
        let store = AccountFencingStore()
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            recordZoneID: zoneID,
            accountIdentifierProvider: { "account-b" }
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )
        let oldScope = CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        synchronizer.storedDatabaseToken = DatabaseChangeCursor(
            serializedData: Data("old-account-cursor".utf8)
        )
        synchronizer.databaseSubscriptionID = "old-account-subscription"
        synchronizer.persistTransientRetryState(
            context: .init(
                attemptID: synchronizer.synchronizationAttemptID,
                runID: synchronizer.synchronizationRunID,
                accountIdentifier: "account-a",
                accountScopeIdentifier: oldScope
            ),
            notBefore: Date().addingTimeInterval(60),
            consecutiveFailures: 1
        )
        store.synchronizesDurably = false

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the migration durability fence to fail")
        } catch let error as ChangeFeedMigrationPersistenceError {
            XCTAssertEqual(error, .stateNotDurable)
        }

        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
        XCTAssertTrue(store.valuesWithPrefix(
            synchronizer.durableStateKey("ChangeFeedMigration.v2")
        ).isEmpty)
        XCTAssertEqual(
            synchronizer.storedDatabaseToken?.serializedData,
            Data("old-account-cursor".utf8)
        )
        XCTAssertEqual(
            synchronizer.databaseSubscriptionID,
            "old-account-subscription"
        )
        XCTAssertNotNil(synchronizer._test_persistedTransientRetryNotBefore(
            accountScopeIdentifier: oldScope
        ))
    }

    @BigSyncBackgroundActor
    func testUndurableZoneLifecycleWriteRollsBackInProcessState() throws {
        let store = AccountFencingStore()
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            recordZoneID: zoneID
        )
        let scope = CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        synchronizer.activeRunContext = .init(
            attemptID: synchronizer.synchronizationAttemptID,
            runID: synchronizer.synchronizationRunID,
            accountIdentifier: "account-a",
            accountScopeIdentifier: scope
        )
        store.synchronizesDurably = false

        XCTAssertThrowsError(try synchronizer.markConfiguredZoneEstablished(
            zoneID,
            accountScopeIdentifier: scope
        ))
        XCTAssertFalse(synchronizer.configuredZoneIsEstablished(zoneID))
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "ZoneLifecycle.v3.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        )))
    }

    @BigSyncBackgroundActor
    private func makeSynchronizer(
        transport: AccountFencingTransport,
        store: KeyValueStore = AccountFencingStore(),
        identifier: String = UUID().uuidString,
        containerIdentifier: String = "iCloud.account-fencing",
        recordZoneID: CKRecordZone.ID = CKRecordZone.ID(
            zoneName: "BigSyncKit",
            ownerName: CKCurrentUserDefaultName
        ),
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
            "account-a"
        },
        accountStatusProvider: @escaping CloudKitSynchronizer.AccountStatusProvider = {
            .available
        }
    ) -> CloudKitSynchronizer {
        let synchronizer = CloudKitSynchronizer(
            identifier: identifier,
            containerIdentifier: containerIdentifier,
            database: transport,
            recordZoneID: recordZoneID,
            keyValueStore: store,
            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: accountStatusProvider,
            changeFeed: transport,
            subscriptionStore: transport,
            zoneStore: transport,
            recordStore: transport,
            logger: Logger(label: "BigSyncKitTests")
        )
#if DEBUG
        synchronizer._allowRecordZoneRebindingForTesting()
#endif
        return synchronizer
    }

    private func makeZoneID() -> CKRecordZone.ID {
        CKRecordZone.ID(
            zoneName: "account-fencing-\(UUID().uuidString)",
            ownerName: CKCurrentUserDefaultName
        )
    }
}
