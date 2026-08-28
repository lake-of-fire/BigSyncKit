import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

private final class AccountFencingStore:
    NSObject,
    KeyValueStore,
    @unchecked Sendable {
    private var values = [String: Any]()
    var synchronizesDurably = true
    var undurableKeySubstring: String?
    private var lastMutatedKey: String?

    func object(forKey defaultName: String) -> Any? { values[defaultName] }
    func bool(forKey defaultName: String) -> Bool {
        values[defaultName] as? Bool ?? false
    }
    func set(value: Any?, forKey defaultName: String) {
        lastMutatedKey = defaultName
        values[defaultName] = value
    }
    func set(boolValue: Bool, forKey defaultName: String) {
        lastMutatedKey = defaultName
        values[defaultName] = boolValue
    }
    func removeObject(forKey defaultName: String) {
        lastMutatedKey = defaultName
        values.removeValue(forKey: defaultName)
    }
    func synchronize() -> Bool {
        synchronizesDurably && !(
            undurableKeySubstring.map {
                lastMutatedKey?.contains($0) == true
            } ?? false
        )
    }

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
    private(set) var preparedResetModes = [ChangeFeedResetMode]()
    var requestsOneUploadWakeupOnFinish = false
    var hasPendingTerminalChanges = false
    private var rebuildIsActive = false
    var consumedBoundaryIdentifier: String?
    var feedEpoch: Int?

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
        preparedResetModes.append(mode)
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
    func changeFeedResetCompletionIsDurable(
        accountScopeIdentifier: String,
        epoch: Int,
        mode: ChangeFeedResetMode
    ) async throws -> Bool {
        _ = accountScopeIdentifier
        _ = epoch
        _ = mode
        return !rebuildIsActive
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
    func saveChanges(
        in records: [CKRecord],
        forceSave: Bool
    ) async throws -> [InboundLiveResult] {
        records.enumerated().map {
            .init(
                event: .init(
                    ordinal: $0.offset,
                    entityType: $0.element.recordType,
                    recordID: $0.element.recordID
                ),
                disposition: .applied
            )
        }
    }
    func deleteRecords(
        with recordIDs: [CKRecord.ID]
    ) async throws -> [InboundDeletionResult] {
        recordIDs.enumerated().map {
            .init(
                event: .init(
                    ordinal: $0.offset,
                    entityType: "AccountFencingObject",
                    recordID: $0.element
                ),
                disposition: .appliedTombstone
            )
        }
    }
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
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(
        accountScopeIdentifier: String,
        replicaBindingGenerationIdentifier: String?,
        containerIdentifier: String,
        databaseScope: CKDatabase.Scope
    ) throws -> String? {
        _ = accountScopeIdentifier
        _ = replicaBindingGenerationIdentifier
        _ = containerIdentifier
        _ = databaseScope
        return consumedBoundaryIdentifier
    }
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? { feedEpoch }
    func didFinishImport() async throws {
        if requestsOneUploadWakeupOnFinish {
            requestsOneUploadWakeupOnFinish = false
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        hasPendingTerminalChanges
    }
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

private actor AccountScopeInvalidationRecorder {
    private(set) var reasons = [BigSyncAccountScopeInvalidationReason]()

    func record(_ reason: BigSyncAccountScopeInvalidationReason) {
        reasons.append(reason)
    }
}

private enum AccountScopeInvalidationTestError: Error {
    case rejected
}

private actor FailOnceAccountScopeInvalidation {
    private var shouldFail = true
    private(set) var attempts = 0

    func run() throws {
        attempts += 1
        if shouldFail {
            shouldFail = false
            throw AccountScopeInvalidationTestError.rejected
        }
    }
}

private actor AccountFencingAccountIdentity {
    private var identifier: String
    private var requestCount = 0

    init(_ identifier: String) {
        self.identifier = identifier
    }

    func current() -> String {
        requestCount += 1
        return identifier
    }

    func requests() -> Int { requestCount }

    func replace(with identifier: String) {
        self.identifier = identifier
    }
}

private actor InitialBindingAdmissionRecorder {
    private(set) var contexts = [BigSyncInitialReplicaBindingContext]()

    func record(_ context: BigSyncInitialReplicaBindingContext) {
        contexts.append(context)
    }
}

private final class InitialBindingStateMutation: @unchecked Sendable {
    let store: AccountFencingStore
    var bindingKey = ""

    init(store: AccountFencingStore) {
        self.store = store
    }

    @BigSyncBackgroundActor
    func rotateBinding() throws {
        precondition(!bindingKey.isEmpty)
        _ = try BigSyncReplicaBindingStateStore.prepare(
            store: store,
            key: bindingKey,
            installationIdentifier: "replacement-installation"
        )
    }
}

private enum InitialBindingAdmissionTestError: Error, Equatable {
    case rejected
}

final class CloudKitSynchronizerAccountFencingTests: XCTestCase {
    @BigSyncBackgroundActor
    func testExplicitDatasetPortFailsClosedWithoutAdmissionHandler()
    async throws {
        let store = AccountFencingStore()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            accountReplacementPolicy: .requireExplicitDatasetPort
        )

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected missing dataset admission to fail closed")
        } catch let error as BigSyncCloudAccountPortError {
            XCTAssertEqual(error, .initialDatasetAdmissionUnavailable)
        }

        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "CloudKitAccountIdentifier"
        )))
    }

    @BigSyncBackgroundActor
    func testInitialBindingAdmissionRunsOnceBeforeBindingPublication()
    async throws {
        let recorder = InitialBindingAdmissionRecorder()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { context in
                await recorder.record(context)
            }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        let firstLease = try XCTUnwrap(synchronizer.accountScopeLease())
        try await synchronizer._test_validateSynchronizationAccount()

        let contexts = await recorder.contexts
        XCTAssertEqual(contexts.count, 1)
        XCTAssertEqual(
            contexts.first?.accountScopeIdentifier,
            firstLease.accountScopeIdentifier
        )
        XCTAssertEqual(
            contexts.first?.replicaBindingGenerationIdentifier.utf8.count,
            64
        )
    }

    @BigSyncBackgroundActor
    func testAlreadyAdmittedBindingDoesNotRequireAdmissionHandler()
    async throws {
        let store = AccountFencingStore()
        let identifier = UUID().uuidString
        let containerIdentifier = "iCloud.already-admitted"
        let zoneID = makeZoneID()
        let admitting = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            containerIdentifier: containerIdentifier,
            recordZoneID: zoneID,
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await admitting._test_validateSynchronizationAccount()

        let resumed = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            containerIdentifier: containerIdentifier,
            recordZoneID: zoneID,
            accountReplacementPolicy: .requireExplicitDatasetPort
        )
        try await resumed._test_validateSynchronizationAccount()
        XCTAssertNotNil(try resumed.accountScopeLease())
    }

    @BigSyncBackgroundActor
    func testRejectedInitialBindingCannotActivateAdapterOrTouchCloudKit()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            recordZoneID: zoneID,
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in
                throw InitialBindingAdmissionTestError.rejected
            }
        )
        synchronizer.addModelAdapter(
            AccountFencingModelAdapter(zoneID: zoneID)
        )

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected initial binding admission to reject")
        } catch let error as InitialBindingAdmissionTestError {
            XCTAssertEqual(error, .rejected)
        }

        XCTAssertEqual(transport.operationCount, 0)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "CloudKitAccountIdentifier"
        )))
    }

    @BigSyncBackgroundActor
    func testInitialBindingRevalidatesAccountAfterAdmissionSuspends()
    async throws {
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in
                await identity.replace(with: "account-b")
            }
        )

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected changed account to invalidate admission")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
            // Expected: the callback's account sample no longer authorizes
            // publication of the initial replica binding.
        }

        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertTrue(synchronizer.accountValidationRequired)
    }

    @BigSyncBackgroundActor
    func testInitialBindingRevalidatesBindingGenerationAfterAdmissionSuspends()
    async throws {
        let store = AccountFencingStore()
        let mutation = InitialBindingStateMutation(store: store)
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in
                try mutation.rotateBinding()
            }
        )
        mutation.bindingKey = synchronizer.durableStateKey(
            "ReplicaBinding.v1"
        )

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected changed binding generation to invalidate admission")
        } catch is CancellationError {
            // Expected: admission cannot publish an account against a binding
            // generation that changed while the callback was suspended.
        }

        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertNil(store.value(forKey: synchronizer.durableStateKey(
            "CloudKitAccountIdentifier"
        )))
    }

    @BigSyncBackgroundActor
    func testExplicitDatasetPortAllowsSameAccountRevalidation()
    async throws {
        let transport = AccountFencingTransport()
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        let firstLease = try XCTUnwrap(synchronizer.accountScopeLease())
        try await synchronizer._test_validateSynchronizationAccount()

        XCTAssertEqual(try synchronizer.accountScopeLease(), firstLease)
        XCTAssertNil(try synchronizer.pendingCloudAccountPortRequirement())
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExplicitDatasetPortBlocksReplacementBeforeCloudKitAccess()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        synchronizer.addModelAdapter(
            AccountFencingModelAdapter(zoneID: zoneID)
        )

        try await synchronizer._test_validateSynchronizationAccount()
        let originalLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )
        await identity.replace(with: "account-b")

        let firstRequirement: BigSyncCloudAccountPortRequirement
        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected account replacement to require a dataset port")
            return
        } catch BigSyncCloudAccountPortError.required(let requirement) {
            firstRequirement = requirement
        }

        XCTAssertEqual(transport.operationCount, 0)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(
            firstRequirement.sourceAccountScopeIdentifier,
            originalLease.accountScopeIdentifier
        )
        XCTAssertEqual(
            firstRequirement.destinationAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        )
        XCTAssertEqual(
            firstRequirement.bindingGenerationIdentifier.utf8.count,
            64
        )
        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            firstRequirement
        )
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
        XCTAssertTrue(
            store.valuesWithPrefix(synchronizer.durableStateKey(
                "ChangeFeedMigration"
            )).isEmpty
        )

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the durable port gate to remain active")
        } catch BigSyncCloudAccountPortError.required(let requirement) {
            XCTAssertEqual(requirement, firstRequirement)
        }
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExistingAccountMarkerRequiresPortBeforeReplacement()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-b")
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )

        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the installed account marker to fence replacement")
            return
        } catch BigSyncCloudAccountPortError.required(let pending) {
            requirement = pending
        }
        XCTAssertEqual(
            requirement.sourceAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertEqual(
            requirement.destinationAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        )
        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExistingAccountMarkerBindsSameInstalledAccount()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let admissions = InitialBindingAdmissionRecorder()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { "account-a" },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: {
                await admissions.record($0)
            }
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )

        try await synchronizer._test_validateSynchronizationAccount()

        let binding = try XCTUnwrap(
            BigSyncReplicaBindingStateStore.load(
                store: store,
                key: synchronizer.durableStateKey("ReplicaBinding.v1")
            )
        )
        XCTAssertEqual(
            binding.activeAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertEqual(
            binding.datasetOwnerAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertNil(binding.pendingPort)
        XCTAssertNil(try synchronizer.pendingCloudAccountPortRequirement())
        XCTAssertEqual(
            try synchronizer.accountScopeLease()?.accountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        let admissionContexts = await admissions.contexts
        XCTAssertTrue(admissionContexts.isEmpty)
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testRestoredBindingRetainsDatasetOwnerForExplicitPort()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { "account-b" },
            accountReplacementPolicy: .requireExplicitDatasetPort
        )
        let bindingKey = synchronizer.durableStateKey("ReplicaBinding.v1")
        _ = try BigSyncReplicaBindingStateStore.prepare(
            store: store,
            key: bindingKey,
            installationIdentifier: "installation-before-restore"
        )
        _ = try BigSyncReplicaBindingStateStore.bindInitialAccount(
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a"),
            store: store,
            key: bindingKey
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )

        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the restored dataset to require an explicit port")
            return
        } catch BigSyncCloudAccountPortError.required(let pending) {
            requirement = pending
        }

        XCTAssertEqual(
            requirement.sourceAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertEqual(
            requirement.destinationAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        )
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testRestoredBindingReadmitsSameAccountWithoutPort() async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let admissions = InitialBindingAdmissionRecorder()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { "account-a" },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: {
                await admissions.record($0)
            }
        )
        let bindingKey = synchronizer.durableStateKey("ReplicaBinding.v1")
        _ = try BigSyncReplicaBindingStateStore.prepare(
            store: store,
            key: bindingKey,
            installationIdentifier: "installation-before-restore"
        )
        _ = try BigSyncReplicaBindingStateStore.bindInitialAccount(
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a"),
            store: store,
            key: bindingKey
        )

        try await synchronizer._test_validateSynchronizationAccount()

        let binding = try XCTUnwrap(
            BigSyncReplicaBindingStateStore.load(
                store: store,
                key: bindingKey
            )
        )
        XCTAssertEqual(
            binding.activeAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertNil(binding.restoredDatasetOwnerAccountScopeIdentifier)
        XCTAssertNil(binding.pendingPort)
        let admissionContexts = await admissions.contexts
        XCTAssertEqual(admissionContexts.count, 1)
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testJournalAndWorkerShortCircuitAnExistingPortRequirement()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        synchronizer.addModelAdapter(
            AccountFencingModelAdapter(zoneID: zoneID)
        )

        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected account replacement to require a dataset port")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }
        let accountRequestsAtGate = await identity.requests()

        let worker = BigSyncBackgroundActor()
        worker._test_installSynchronizer(
            synchronizer,
            performsAccountAvailabilityPreflight: false
        )
        let surfaced = expectation(
            forNotification: .SynchronizerDidFailToSynchronize,
            object: synchronizer
        ) { notification in
            guard let error = notification.userInfo?[
                cloudKitSynchronizerErrorKey
            ] as? BigSyncCloudAccountPortError else {
                return false
            }
            return error == .required(requirement)
        }

        await synchronizer.hasChangesToUpload()
        await fulfillment(of: [surfaced], timeout: 1)
        await synchronizer.hasChangesToUpload()
        await synchronizer.hasChangesToUpload()
        XCTAssertFalse(synchronizer.syncing)

        let explicitResult = await worker.synchronizeCloudKit()
        XCTAssertNil(explicitResult)

        let accountRequestsAfterWakeups = await identity.requests()
        XCTAssertEqual(accountRequestsAfterWakeups, accountRequestsAtGate)
        XCTAssertEqual(transport.operationCount, 0)
        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
    }

    @BigSyncBackgroundActor
    func testReturningToFormerAccountPreservesFirstPendingDatasetPort()
    async throws {
        let transport = AccountFencingTransport()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected account replacement to require a dataset port")
        } catch BigSyncCloudAccountPortError.required(_) {
        }
        let firstRequirement = try XCTUnwrap(
            try synchronizer.pendingCloudAccountPortRequirement()
        )

        await identity.replace(with: "account-a")
        let returnRequirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the current dataset to require a fresh port")
            return
        } catch BigSyncCloudAccountPortError.required(let requirement) {
            returnRequirement = requirement
        }

        XCTAssertEqual(
            returnRequirement.transitionID,
            firstRequirement.transitionID
        )
        XCTAssertEqual(
            returnRequirement.bindingGenerationIdentifier,
            firstRequirement.bindingGenerationIdentifier
        )
        XCTAssertEqual(
            returnRequirement.sourceAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        XCTAssertEqual(
            returnRequirement.destinationAccountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        )
        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            returnRequirement
        )
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testThirdAccountCannotSupersedeFirstPendingDatasetPort()
    async throws {
        let transport = AccountFencingTransport()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected account replacement to require a dataset port")
        } catch BigSyncCloudAccountPortError.required(_) {
        }
        let firstRequirement = try XCTUnwrap(
            try synchronizer.pendingCloudAccountPortRequirement()
        )

        await identity.replace(with: "account-c")
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the first pending dataset port to remain active")
        } catch BigSyncCloudAccountPortError.required(let requirement) {
            XCTAssertEqual(requirement, firstRequirement)
        }

        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            firstRequirement
        )
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountChangeNotifiesDomainInvalidationAfterLeaseIsDurable()
    async throws {
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountIdentifierProvider: { "account-a" }
        )
        let recorder = AccountScopeInvalidationRecorder()
        synchronizer.accountScopeInvalidationHandler = { reason in
            let lease: BigSyncAccountScopeLease?
            do {
                lease = try synchronizer.accountScopeLease()
            } catch {
                XCTFail("Could not read invalidated account lease: \(error)")
                return
            }
            XCTAssertNil(lease)
            await recorder.record(reason)
        }
        try await synchronizer._test_validateSynchronizationAccount()
        let establishedLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        // Delivery revokes authority synchronously, before the actor task can
        // persist invalidation or invoke the domain callback.
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertThrowsError(
            try synchronizer.validateAccountScopeLease(establishedLease)
        ) { error in
            XCTAssertEqual(
                error as? BigSyncAccountScopeLeaseError,
                .unavailable
            )
        }
        for _ in 0..<100 {
            if !(await recorder.reasons).isEmpty { break }
            await Task.yield()
        }

        let reasons = await recorder.reasons
        XCTAssertEqual(reasons, [.accountChanged])
        XCTAssertNil(try synchronizer.accountScopeLease())
    }

    @BigSyncBackgroundActor
    func testAccountChangeInvalidationFailurePreventsCloudKitAndRetries()
    async throws {
        let transport = AccountFencingTransport()
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: transport,
            recordZoneID: zoneID,
            accountIdentifierProvider: { "account-a" }
        )
        synchronizer.addModelAdapter(
            AccountFencingModelAdapter(zoneID: zoneID)
        )
        try await synchronizer._test_validateSynchronizationAccount()

        let invalidation = FailOnceAccountScopeInvalidation()
        synchronizer.accountScopeInvalidationHandler = { _ in
            try await invalidation.run()
        }

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        for _ in 0..<100 {
            if await invalidation.attempts > 0 { break }
            await Task.yield()
        }
        for _ in 0..<10 { await Task.yield() }

        let attemptsAfterFailure = await invalidation.attempts
        XCTAssertEqual(attemptsAfterFailure, 1)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        let attemptsAfterRetry = await invalidation.attempts
        XCTAssertEqual(attemptsAfterRetry, 2)
        XCTAssertNotNil(try synchronizer.accountScopeLease())
        XCTAssertGreaterThan(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountReplacementInvalidationFailureDoesNotPublishAndRetries()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { await identity.current() }
        )
        try await synchronizer._test_validateSynchronizationAccount()

        let invalidation = FailOnceAccountScopeInvalidation()
        synchronizer.accountScopeInvalidationHandler = { _ in
            try await invalidation.run()
        }
        await identity.replace(with: "account-b")

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected account-domain invalidation to fail closed")
        } catch AccountScopeInvalidationTestError.rejected {
        }

        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
        XCTAssertEqual(transport.operationCount, 0)

        try await synchronizer._test_validateSynchronizationAccount()

        let attempts = await invalidation.attempts
        let lease = try XCTUnwrap(synchronizer.accountScopeLease())
        XCTAssertEqual(attempts, 2)
        XCTAssertEqual(
            lease.accountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        )
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-b"
        )
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountScopeLeaseIsStableUntilDurableInvalidation()
    async throws {
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountIdentifierProvider: { "account-a" }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        let first = try XCTUnwrap(synchronizer.accountScopeLease())
        XCTAssertEqual(
            first.accountScopeIdentifier,
            CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )

        try await synchronizer._test_validateSynchronizationAccount()
        let routineRevalidation = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )
        XCTAssertEqual(
            routineRevalidation.invalidationGeneration,
            first.invalidationGeneration
        )

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        XCTAssertNil(try synchronizer.accountScopeLease())
        for _ in 0..<100 where !synchronizer.accountValidationRequired {
            await Task.yield()
        }
        XCTAssertTrue(synchronizer.accountValidationRequired)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertThrowsError(
            try synchronizer.validateAccountScopeLease(first)
        ) { error in
            XCTAssertEqual(
                error as? BigSyncAccountScopeLeaseError,
                .unavailable
            )
        }

        try await synchronizer._test_validateSynchronizationAccount()
        let restored = try XCTUnwrap(synchronizer.accountScopeLease())
        XCTAssertEqual(
            restored.accountScopeIdentifier,
            first.accountScopeIdentifier
        )
        XCTAssertEqual(
            restored.invalidationGeneration,
            first.invalidationGeneration + 1
        )
        XCTAssertThrowsError(
            try synchronizer.validateAccountScopeLease(first)
        ) { error in
            XCTAssertEqual(
                error as? BigSyncAccountScopeLeaseError,
                .stale
            )
        }
        XCTAssertNoThrow(
            try synchronizer.validateAccountScopeLease(restored)
        )
    }

    @BigSyncBackgroundActor
    func testUnavailableAccountStatusPreventsEveryCloudKitOperation() async {
        let transport = AccountFencingTransport()
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountStatusProvider: { .temporarilyUnavailable },
            accountReplacementPolicy: .requireExplicitDatasetPort
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
        XCTAssertNil(try? synchronizer.pendingCloudAccountPortRequirement())
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
        try await synchronizer._test_validateSynchronizationAccount()
        let establishedLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected temporary account failure")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .accountTemporarilyUnavailable)
        }
        XCTAssertEqual(transport.databaseChangeFetchCount, 1)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertThrowsError(
            try synchronizer.validateAccountScopeLease(establishedLease)
        ) { error in
            XCTAssertEqual(
                error as? BigSyncAccountScopeLeaseError,
                .unavailable
            )
        }

        try await Task.sleep(nanoseconds: 150_000_000)
        XCTAssertEqual(transport.databaseChangeFetchCount, 1)

        try await synchronizer._test_validateSynchronizationAccount()
        let revalidatedLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )
        XCTAssertEqual(
            revalidatedLease.invalidationGeneration,
            establishedLease.invalidationGeneration
        )
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
    func testStableDurableStateZoneKeepsNamespaceWhileTransportZoneChanges() async {
        let stableStateZone = CKRecordZone.ID(
            zoneName: "durable-state-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let firstTransportZone = CKRecordZone.ID(
            zoneName: "transport-zone-a",
            ownerName: CKCurrentUserDefaultName
        )
        let secondTransportZone = CKRecordZone.ID(
            zoneName: "transport-zone-b",
            ownerName: CKCurrentUserDefaultName
        )
        let identifier = "stable-durable-state-identifier"

        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            identifier: identifier,
            recordZoneID: firstTransportZone,
            durableStateRecordZoneID: stableStateZone
        )
        let second = makeSynchronizer(
            transport: AccountFencingTransport(),
            identifier: identifier,
            recordZoneID: secondTransportZone,
            durableStateRecordZoneID: stableStateZone
        )

        XCTAssertEqual(first.durableStateNamespace, second.durableStateNamespace)
        XCTAssertEqual(first.recordZoneID, firstTransportZone)
        XCTAssertEqual(second.recordZoneID, secondTransportZone)
        XCTAssertNotEqual(first.recordZoneID, second.recordZoneID)
    }

    @BigSyncBackgroundActor
    func testVerifiedPortActivatesExactPendingBindingAndRequiresWorkerRestart()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            store: store,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected a port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }

        try await synchronizer.activateCloudAccountPort(requirement)

        XCTAssertNil(try synchronizer.pendingCloudAccountPortRequirement())
        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected the old-zone worker to require a restart")
        } catch let error as BigSyncCloudAccountPortError {
            XCTAssertEqual(error, .workerRestartRequired)
        }
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testPortActivationRejectsTheWrongDestinationAccount() async throws {
        let transport = AccountFencingTransport()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected a port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }
        await identity.replace(with: "account-c")

        do {
            try await synchronizer.activateCloudAccountPort(requirement)
            XCTFail("Expected the wrong account to be rejected")
        } catch let error as BigSyncCloudAccountPortError {
            XCTAssertEqual(error, .corruptRequirement)
        }
        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testTerminalPublicationEvidenceRestoresOnlyForExactTransportBoundary()
    async throws {
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "publication-evidence-\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        let firstAdapter = AccountFencingModelAdapter(zoneID: zoneID)
        firstAdapter.consumedBoundaryIdentifier = "boundary-a"
        firstAdapter.feedEpoch = 7
        first.addModelAdapter(firstAdapter)
        first.domainPublicationScopeIdentifierProvider = { "dataset-a" }

        let firstResult = try await first.synchronize()
        XCTAssertNotNil(firstResult.receipt)

        func reopened(
            accountIdentifierProvider:
                @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
                    "account-a"
                },
            recordZoneID: CKRecordZone.ID? = nil,
            boundary: String = "boundary-a",
            feedEpoch: Int = 7,
            hasPendingChanges: Bool = false
        ) -> CloudKitSynchronizer {
            let synchronizer = makeSynchronizer(
                transport: AccountFencingTransport(),
                store: store,
                identifier: identifier,
                recordZoneID: recordZoneID ?? zoneID,
                durableStateRecordZoneID: zoneID,
                accountIdentifierProvider: accountIdentifierProvider,
                accountReplacementPolicy: .localDatasetRebootstrap
            )
            let adapter = AccountFencingModelAdapter(
                zoneID: recordZoneID ?? zoneID
            )
            adapter.consumedBoundaryIdentifier = boundary
            adapter.feedEpoch = feedEpoch
            adapter.hasPendingTerminalChanges = hasPendingChanges
            synchronizer.addModelAdapter(adapter)
            return synchronizer
        }

        let matching = try await reopened()
            .restoredDurablePublicationEvidence()
        XCTAssertEqual(matching?.domainScopeIdentifier, "dataset-a")
        XCTAssertEqual(matching?.consumedServerBoundaryIdentifier, "boundary-a")
        XCTAssertEqual(matching?.changeFeedEpoch, 7)

        let wrongBoundary = try await reopened(boundary: "boundary-b")
            .restoredDurablePublicationEvidence()
        let wrongEpoch = try await reopened(feedEpoch: 8)
            .restoredDurablePublicationEvidence()
        let pendingChanges = try await reopened(hasPendingChanges: true)
            .restoredDurablePublicationEvidence()
        let wrongAccount = try await reopened(
            accountIdentifierProvider: { "account-b" }
        ).restoredDurablePublicationEvidence()
        let wrongZone = try await reopened(
            recordZoneID: makeZoneID()
        ).restoredDurablePublicationEvidence()
        XCTAssertNil(wrongBoundary)
        XCTAssertNil(wrongEpoch)
        XCTAssertNil(pendingChanges)
        XCTAssertNil(wrongAccount)
        XCTAssertNil(wrongZone)

        let evidenceKey = first.durableStateKey("TerminalPublication.v1")
        var alteredEvidence = try XCTUnwrap(
            store.value(forKey: evidenceKey) as? [String: Any]
        )
        alteredEvidence["replicaBindingGenerationIdentifier"] =
            String(repeating: "f", count: 64)
        store.set(value: alteredEvidence, forKey: evidenceKey)
        let wrongBinding = try await reopened()
            .restoredDurablePublicationEvidence()
        XCTAssertNil(wrongBinding)
    }

    @BigSyncBackgroundActor
    func testNewDrainClearsPriorPublicationEvidenceBeforeInboundWork()
    async throws {
        let store = AccountFencingStore()
        let identifier = "publication-clear-\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        let adapter = AccountFencingModelAdapter(zoneID: zoneID)
        adapter.consumedBoundaryIdentifier = "boundary-a"
        adapter.feedEpoch = 3
        synchronizer.addModelAdapter(adapter)
        synchronizer.domainPublicationScopeIdentifierProvider = { "dataset-a" }
        let firstResult = try await synchronizer.synchronize()
        XCTAssertNotNil(firstResult.receipt)

        let evidenceKey = synchronizer.durableStateKey(
            "TerminalPublication.v1"
        )
        XCTAssertNotNil(store.value(forKey: evidenceKey))
        let observedKey = "test.publication-evidence-was-cleared"
        synchronizer.synchronizationWillConsumeServerChangesHandler = { _ in
            XCTAssertNil(store.value(forKey: evidenceKey))
            store.set(boolValue: true, forKey: observedKey)
        }

        let secondResult = try await synchronizer.synchronize()
        XCTAssertNotNil(secondResult.receipt)
        XCTAssertTrue(store.bool(forKey: observedKey))
        XCTAssertNotNil(store.value(forKey: evidenceKey))
    }

    @BigSyncBackgroundActor
    func testLocalDatasetRebootstrapActivatesDestinationAndCompletesRecovery()
    async throws {
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let admissions = InitialBindingAdmissionRecorder()
        let identifier = "local-rebootstrap-\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: {
                await admissions.record($0)
            }
        )
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        try await synchronizer._test_validateSynchronizationAccount()

        let destinationScope = CloudKitSynchronizer
            .accountScopeIdentifier(for: "account-b")
        let binding = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(
            store: store,
            key: synchronizer.durableStateKey("ReplicaBinding.v1")
        ))
        XCTAssertEqual(binding.activeAccountScopeIdentifier, destinationScope)
        XCTAssertNil(binding.pendingPort)
        let admissionContexts = await admissions.contexts
        XCTAssertEqual(admissionContexts.count, 2)
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-b"
        )

        let adapter = AccountFencingModelAdapter(zoneID: zoneID)
        synchronizer.addModelAdapter(adapter)
        let synchronizationResult = try await synchronizer.synchronize()
        XCTAssertNotNil(synchronizationResult.receipt)
        XCTAssertEqual(adapter.preparedResetModes, [.localDatasetRebootstrap])
        let envelope = try XCTUnwrap(store.valuesWithPrefix(
            synchronizer.durableStateKey("ChangeFeedMigration.v3")
        ).values.first as? [String: Any])
        XCTAssertEqual(envelope["mode"] as? String, "localDatasetRebootstrap")
        XCTAssertEqual(envelope["phase"] as? String, "completed")
    }

    @BigSyncBackgroundActor
    func testLocalDatasetRebootstrapResumesPendingBindingAfterRelaunch()
    async throws {
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "pending-rebootstrap-\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { context in
                if context.accountScopeIdentifier
                    == CloudKitSynchronizer.accountScopeIdentifier(
                        for: "account-b"
                    ) {
                    throw InitialBindingAdmissionTestError.rejected
                }
            }
        )
        try await first._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        do {
            try await first._test_validateSynchronizationAccount()
            XCTFail("Expected destination admission to fail")
        } catch InitialBindingAdmissionTestError.rejected {}
        XCTAssertNotNil(try first.pendingCloudAccountPortRequirement())

        let reopened = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await reopened._test_validateSynchronizationAccount()
        XCTAssertNil(try reopened.pendingCloudAccountPortRequirement())
        XCTAssertEqual(
            store.value(forKey: reopened.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-b"
        )
    }

    @BigSyncBackgroundActor
    func testLocalDatasetRebootstrapRecoversActivationBeforeAccountPublication()
    async throws {
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "activated-rebootstrap-\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let first = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { context in
                if context.accountScopeIdentifier
                    == CloudKitSynchronizer.accountScopeIdentifier(
                        for: "account-b"
                    ) {
                    throw InitialBindingAdmissionTestError.rejected
                }
            }
        )
        try await first._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        do {
            try await first._test_validateSynchronizationAccount()
            XCTFail("Expected destination admission to fail")
        } catch InitialBindingAdmissionTestError.rejected {}
        let pending = try XCTUnwrap(
            first.pendingCloudAccountPortRequirement()
        )
        _ = try BigSyncReplicaBindingStateStore.activatePort(
            pending,
            store: store,
            key: first.durableStateKey("ReplicaBinding.v1")
        )

        let reopened = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await reopened._test_validateSynchronizationAccount()
        XCTAssertNil(try reopened.pendingCloudAccountPortRequirement())
        XCTAssertEqual(
            store.value(forKey: reopened.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-b"
        )
        let envelope = try XCTUnwrap(store.valuesWithPrefix(
            reopened.durableStateKey("ChangeFeedMigration.v3")
        ).values.first as? [String: Any])
        XCTAssertEqual(envelope["mode"] as? String, "localDatasetRebootstrap")
        XCTAssertEqual(envelope["phase"] as? String, "requested")
    }

    @BigSyncBackgroundActor
    func testReturningToSourceCancelsPendingDestinationAndRebuildsJournal()
    async throws {
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "return-source-rebootstrap-\(UUID().uuidString)"
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            store: store,
            identifier: identifier,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { context in
                if context.accountScopeIdentifier
                    == CloudKitSynchronizer.accountScopeIdentifier(
                        for: "account-b"
                    ) {
                    throw InitialBindingAdmissionTestError.rejected
                }
            }
        )
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected destination admission to fail")
        } catch InitialBindingAdmissionTestError.rejected {}
        XCTAssertNotNil(try synchronizer.pendingCloudAccountPortRequirement())

        await identity.replace(with: "account-a")
        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertNil(try synchronizer.pendingCloudAccountPortRequirement())
        let envelope = try XCTUnwrap(store.valuesWithPrefix(
            synchronizer.durableStateKey("ChangeFeedMigration.v3")
        ).values.first as? [String: Any])
        XCTAssertEqual(envelope["mode"] as? String, "localDatasetRebootstrap")
        XCTAssertEqual(envelope["phase"] as? String, "requested")
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
            "ChangeFeedMigration.v3.\(scope).\(zoneB.ownerName).\(zoneB.zoneName)"
        )))

        XCTAssertNotNil(first.storedDatabaseToken)
        XCTAssertNotNil(first._test_persistedTransientRetryNotBefore(
            accountScopeIdentifier: scope
        ))
        let firstHealth = try await first.syncHealthSnapshot()
        XCTAssertNotNil(firstHealth)
    }

    @BigSyncBackgroundActor
    func testUnqualifiedObsoleteStateIsIgnoredByZoneBoundNamespace() async throws {
        let store = AccountFencingStore()
        let transport = AccountFencingTransport()
        let zoneID = makeZoneID()
        let identifier = "obsolete-ignored-\(UUID().uuidString)"
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
        let obsoletePrefix = "\(identifier).BigSyncKit.ZoneLifecycle.v1.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        store.set(boolValue: true, forKey: "\(obsoletePrefix).established")
        store.set(
            value: CloudKitZoneDeletionKind.purged.rawValue,
            forKey: "\(obsoletePrefix).terminal"
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
            "ChangeFeedMigration.v3.\(scope).\(zoneID.ownerName).\(zoneID.zoneName)"
        )))
        XCTAssertEqual(
            store.value(forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )) as? String,
            "account-a"
        )
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
        store.undurableKeySubstring = "ChangeFeedMigration.v3"

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
            synchronizer.durableStateKey("ChangeFeedMigration.v3")
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

    @MainActor
    func testSyncStatusSurfacesDurableAccountPortRequirement() {
        var configuration = Realm.Configuration()
        configuration.inMemoryIdentifier = UUID().uuidString
        let viewModel = SyncStatusViewModel(
            realmConfiguration: configuration
        )
        let requirement = BigSyncCloudAccountPortRequirement(
            transitionID: UUID(),
            bindingGenerationIdentifier: String(repeating: "a", count: 64),
            sourceAccountScopeIdentifier: "account-a",
            destinationAccountScopeIdentifier: "account-b",
            detectedAt: Date(timeIntervalSince1970: 1_000)
        )

        viewModel.applySynchronizationFailure(
            BigSyncCloudAccountPortError.required(requirement)
        )

        XCTAssertEqual(viewModel.cloudAccountPortRequirement, requirement)
        XCTAssertTrue(viewModel.syncFailed)
        XCTAssertEqual(
            viewModel.syncStatus,
            "iCloud Account Change Requires Data Transfer"
        )
        XCTAssertEqual(
            viewModel.cloudKitSyncHealthText,
            "Your iCloud account changed; Manabi data must be moved before sync can resume"
        )
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
        durableStateRecordZoneID: CKRecordZone.ID? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
            "account-a"
        },
        accountStatusProvider: @escaping CloudKitSynchronizer.AccountStatusProvider = {
            .available
        },
        accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy =
            .serverReconciliation,
        initialReplicaBindingAdmissionHandler:
            BigSyncBackgroundWorkerConfiguration
                .InitialReplicaBindingAdmissionHandler? = nil
    ) -> CloudKitSynchronizer {
        let synchronizer = CloudKitSynchronizer(
            identifier: identifier,
            containerIdentifier: containerIdentifier,
            database: transport,
            recordZoneID: recordZoneID,
            durableStateRecordZoneID: durableStateRecordZoneID,
            keyValueStore: store,
            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: accountStatusProvider,
            changeFeed: transport,
            subscriptionStore: transport,
            zoneStore: transport,
            recordStore: transport,
            initialReplicaBindingAdmissionHandler:
                initialReplicaBindingAdmissionHandler,
            accountReplacementPolicy: accountReplacementPolicy,
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
