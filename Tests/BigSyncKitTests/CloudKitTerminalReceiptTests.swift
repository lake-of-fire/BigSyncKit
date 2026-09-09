import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Real synchronizer drains with injected transport/account providers. These
/// tests do not access a CloudKit account, manufacture journals, or arm a cutoff.
final class CloudKitTerminalReceiptTests: XCTestCase {
    @BigSyncBackgroundActor
    func testOrdinaryReceiptCanBeRevalidatedRepeatedlyWithoutCloudMutations() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        let operations = fixture.transport.operations
        let writes = fixture.store.writes
        for _ in 0..<3 {
            try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
            try fixture.synchronizer.validateTerminalReceipt(receipt)
        }
        XCTAssertEqual(fixture.transport.operations, operations)
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertFalse(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testNextCompletedDrainInvalidatesPreviousReceipt() async throws {
        let fixture = Fixture()
        let first = try await fixture.drain()
        let second = try await fixture.drain()
        XCTAssertNotEqual(first.runID, second.runID)
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
        try await fixture.synchronizer.revalidateTerminalReceipt(second)
    }

    @BigSyncBackgroundActor
    func testOtherSynchronizerCannotValidateReceipt() async throws {
        let first = Fixture()
        let second = Fixture()
        let receipt = try await first.drain()
        _ = try await second.drain()
        await assertRejected { try await second.synchronizer.revalidateTerminalReceipt(receipt) }
        try await first.synchronizer.revalidateTerminalReceipt(receipt)
    }

    @BigSyncBackgroundActor
    func testPendingEditAfterDrainIsRejectedWithoutAcknowledgement() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        fixture.adapter.hasPendingTerminalChanges = true
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testChangedOrMissingConsumedBoundaryRejectsReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        for boundary: String? in ["new-boundary", nil] {
            fixture.adapter.boundary = boundary
            await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testFinalSynchronousCheckRejectsChangesAfterAccountValidation() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
        fixture.adapter.hasPendingTerminalChanges = true
        XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testUnavailableDurabilityRejectsPreviouslyCompletedReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        fixture.store.durable = false
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
    }

    @BigSyncBackgroundActor
    func testAccountReplacementDuringValidationRejectsOriginalReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        await fixture.account.replace("replacement-account")
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testNewRunDuringAccountAwaitCannotReturnOldReceiptAsCurrent() async throws {
        let fixture = Fixture()
        let first = try await fixture.drain()
        await fixture.account.onNextRead {
            _ = try await fixture.synchronizer.synchronize()
        }
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
        let current = try await fixture.drain()
        try await fixture.synchronizer.revalidateTerminalReceipt(current)
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementDuringValidationRejectsCompletion() async throws {
        let first = Fixture()
        let second = Fixture()
        let worker = BigSyncBackgroundActor()
        let receipt = try await first.drain()
        await worker._test_installSynchronizer(first.synchronizer)
        await first.account.onNextRead {
            await worker._test_installSynchronizer(second.synchronizer)
        }
        await assertRejected { try await worker.revalidateTerminalReceipt(receipt) }
    }

    @BigSyncBackgroundActor
    func testCancellationRejectsReceiptWithoutStartingAnotherDrain() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        let operations = fixture.transport.operations
        let task = Task { @BigSyncBackgroundActor in
            try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
        }
        task.cancel()
        await assertRejected { try await task.value }
        XCTAssertEqual(fixture.transport.operations, operations)
    }

    @BigSyncBackgroundActor
    private func assertRejected(
        file: StaticString = #filePath, line: UInt = #line,
        _ operation: () async throws -> Void
    ) async {
        do {
            try await operation()
            XCTFail("Expected obsolete or incomplete terminal authority to be rejected", file: file, line: line)
        } catch { }
    }

    @BigSyncBackgroundActor
    private final class Fixture {
        let store = ReceiptStore()
        let transport = ReceiptTransport()
        let account = ReceiptAccount()
        let adapter: ReceiptAdapter
        let synchronizer: CloudKitSynchronizer

        init() {
            let zone = CKRecordZone.ID(zoneName: "receipt-fixture-\(UUID().uuidString)",
                ownerName: CKCurrentUserDefaultName)
            adapter = ReceiptAdapter(zoneID: zone)
            let account = account
            synchronizer = CloudKitSynchronizer(identifier: UUID().uuidString,
                containerIdentifier: "iCloud.receipt-fixture", database: transport,
                recordZoneID: zone, keyValueStore: store,
                accountIdentifierProvider: { try await account.read() },
                accountStatusProvider: { .available }, changeFeed: transport,
                subscriptionStore: transport, zoneStore: transport, recordStore: transport,
                logger: Logger(label: "TerminalReceiptTests"))
            synchronizer._allowRecordZoneRebindingForTesting()
            synchronizer.addModelAdapter(adapter)
        }

        func drain() async throws -> CloudKitSynchronizer.SynchronizationReceipt {
            let result = try await synchronizer.synchronize()
            XCTAssertEqual(result.publicationState, .complete)
            return try XCTUnwrap(result.receipt)
        }
    }
}

private actor ReceiptAccount {
    private var identifier = "original-account"
    private var nextRead: (@Sendable () async throws -> Void)?
    func replace(_ identifier: String) { self.identifier = identifier }
    func onNextRead(_ operation: @escaping @Sendable () async throws -> Void) { nextRead = operation }
    func read() async throws -> String {
        let operation = nextRead
        nextRead = nil
        try await operation?()
        return identifier
    }
}

// These synchronous test doubles are used only by the serial BigSync actor.
private final class ReceiptStore: NSObject, KeyValueStore, @unchecked Sendable {
    private var values: [String: Any] = [:]
    private(set) var writes = 0
    var durable = true
    func object(forKey key: String) -> Any? { values[key] }
    override func value(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value; writes += 1 }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { values.removeValue(forKey: key); writes += 1 }
    func synchronize() -> Bool { durable }
}

private final class ReceiptTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private(set) var operations = 0
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? {
        operations += 1; return nil
    }
    func save(subscription: CKSubscription) async throws -> CKSubscription { operations += 1; return subscription }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws { operations += 1 }
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        operations += 1; return CKRecordZone(zoneID: identifier)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { operations += 1; return recordZone }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws { operations += 1 }
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        operations += 1; return .init(saveResults: [:], deleteResults: [:])
    }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        operations += 1
        return .init(cursor: .init(serializedData: Data("db-boundary".utf8)),
            changedZoneIDs: [], deletions: [], moreComing: false)
    }
    func recordZoneChanges(in zoneID: CKRecordZone.ID, since: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        operations += 1
        return .init(cursor: .init(serializedData: Data("zone-boundary".utf8)),
            records: [], deletedRecordIDs: [], moreComing: false)
    }
}

private final class ReceiptAdapter: NSObject, ModelAdapter, ChangeFeedResetMigrating,
    TerminalSynchronizationStateModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    var hasChanges: Bool { false }
    var hasPendingTerminalChanges = false
    var boundary: String? = "consumed-boundary"
    private var bootstrapActive = false
    private(set) var acknowledgements = 0
    init(zoneID: CKRecordZone.ID) { recordZoneID = zoneID }
    func cleanUp() async throws { }
    func resetSyncCaches() async throws { }
    func reconcileReplicaJournalHandoff(_ handoff: BigSyncReplicaJournalHandoff,
        accountScopeIdentifier: String, epoch: Int, verifyOnly: Bool) async throws { }
    func prepareChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws {
        bootstrapActive = true
    }
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { }
    func isChangeFeedServerBootstrapActive() async -> Bool { bootstrapActive }
    func changeFeedResetCompletionIsDurable(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws -> Bool {
        !bootstrapActive
    }
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { }
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws {
        bootstrapActive = false
    }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] { [] }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    func persistImportedChanges() async throws { }
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] { [] }
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws { acknowledgements += 1 }
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws { acknowledgements += 1 }
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws { }
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws { }
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(accountScopeIdentifier: String, replicaBindingGenerationIdentifier: String?,
        containerIdentifier: String, databaseScope: CKDatabase.Scope) throws -> String? { boundary }
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? { 7 }
    func didFinishImport() async throws { }
    func cancelSynchronization() { }
    func unsetCancellation() async throws { }
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool { hasPendingTerminalChanges }
}
