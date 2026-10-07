import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

/// Reads actual diagnostic serialization and account-read boundaries. Every
/// account and transport dependency is injected; no CloudKit operation is sent.
final class SyncHealthSnapshotOwnershipTests: XCTestCase, @unchecked Sendable {
    @BigSyncBackgroundActor
    private struct Fixture {
        let sync: CloudKitSynchronizer
        let account: HealthSnapshotAccount
        let store: HealthSnapshotStore
        let transport: HealthSnapshotTransport
        let recordedAt = Date(timeIntervalSince1970: 1_700_000_000)
    }

    @BigSyncBackgroundActor
    private func fixture() throws -> Fixture {
        let account = HealthSnapshotAccount()
        let store = HealthSnapshotStore()
        let transport = HealthSnapshotTransport()
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("health-read-" + UUID().uuidString)
        let sync = CloudKitSynchronizer(
            identifier: UUID().uuidString, containerIdentifier: "iCloud.test.health-read",
            database: transport,
            recordZoneID: CKRecordZone.ID(zoneName: UUID().uuidString, ownerName: CKCurrentUserDefaultName),
            keyValueStore: store, accountIdentifierProvider: { try await account.read() },
            accountStatusProvider: { .available }, changeFeed: transport,
            subscriptionStore: transport, zoneStore: transport, recordStore: transport,
            backupDetectionBaseURL: directory, logger: Logger(label: "HealthSnapshotOwnership")
        )
        let result = Fixture(sync: sync, account: account, store: store, transport: transport)
        // This fixture explicitly seeds a completed, validated account, not an
        // account login. Read tests below must not reopen authority themselves.
        sync.accountScopeAuthorityFence.clear()
        let context = CloudKitSynchronizer.RunContext(
            attemptID: sync.synchronizationAttemptID, runID: sync.synchronizationRunID,
            accountIdentifier: "health-a",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "health-a")
        )
        sync.activeRunContext = context
        try sync.recordSyncHealth(.succeeded, context: context, now: result.recordedAt)
        sync.activeRunContext = nil
        store.resetObservations()
        addTeardownBlock { @BigSyncBackgroundActor in
            account.handler = nil
            store.onRead = nil
            await sync.cancelSynchronizationAndWait()
            try? FileManager.default.removeItem(at: directory)
        }
        return result
    }

    private func cancellation(
        _ result: Result<CloudKitSyncHealthSnapshot?, Error>,
        file: StaticString = #filePath, line: UInt = #line
    ) {
        guard case .failure(let error) = result else {
            return XCTFail("Retired health read returned a snapshot or successful nil", file: file, line: line)
        }
        XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)", file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testPrecancelledHealthReadStartsNoProviderOrStoreRead() async throws {
        let f = try fixture()
        let request = Task { @BigSyncBackgroundActor in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await f.sync.syncHealthSnapshot()
        }
        cancellation(await request.result)
        XCTAssertEqual(f.account.calls, 0)
        XCTAssertEqual(f.store.readCount, 0)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    private func heldRead(_ f: Fixture, entered: XCTestExpectation, gate: HealthSnapshotGate)
        -> Task<CloudKitSyncHealthSnapshot?, Error> {
        f.account.handler = { entered.fulfill(); await gate.wait(); return "health-a" }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        addTeardownBlock { request.cancel(); await gate.open(); _ = await request.result }
        return request
    }

    @BigSyncBackgroundActor
    func testAccountInvalidationDuringIdentityReadRejectsOldSnapshot() async throws {
        let f = try fixture()
        let entered = expectation(description: "identity read held")
        let gate = HealthSnapshotGate()
        let request = heldRead(f, entered: entered, gate: gate)
        await fulfillment(of: [entered], timeout: 2)
        f.sync.accountScopeAuthorityFence.poison()
        await gate.open()
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 0)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testPoisonThenClearCannotReviveAnOlderIdentityRead() async throws {
        let f = try fixture()
        let entered = expectation(description: "old same-account read held")
        let gate = HealthSnapshotGate()
        let request = heldRead(f, entered: entered, gate: gate)
        await fulfillment(of: [entered], timeout: 2)
        f.sync.accountScopeAuthorityFence.poison()
        f.sync.accountScopeAuthorityFence.clear()
        await gate.open()
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 0)
    }

    @BigSyncBackgroundActor
    func testCallerCancellationDuringIdentityReadDoesNotReadPersistedState() async throws {
        let f = try fixture()
        let entered = expectation(description: "cancelled identity read held")
        let gate = HealthSnapshotGate()
        let request = heldRead(f, entered: entered, gate: gate)
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        await gate.open()
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 0)
        XCTAssertEqual(f.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testStoreReadInvalidationCannotDeliverItsOldValue() async throws {
        let f = try fixture()
        let fence = f.sync.accountScopeAuthorityFence
        f.store.onRead = { fence.poison() }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 1)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testCancellationDuringStoreReadCannotDeliverBufferedSnapshot() async throws {
        let f = try fixture()
        f.store.onRead = { withUnsafeCurrentTask { $0?.cancel() } }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 1)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testLiveProviderErrorRetainsOriginalIdentity() async throws {
        let f = try fixture()
        let original = NSError(domain: "HealthIdentityFailure", code: 41)
        f.account.handler = { throw original }
        do { _ = try await f.sync.syncHealthSnapshot(); XCTFail("Expected original provider error") }
        catch { XCTAssertTrue((error as NSError) === original) }
        XCTAssertEqual(f.store.readCount, 0)
    }

    @BigSyncBackgroundActor
    func testProviderErrorDoesNotMaskCallerCancellation() async throws {
        let f = try fixture()
        f.account.handler = {
            withUnsafeCurrentTask { $0?.cancel() }
            throw NSError(domain: "CancelledHealthProvider", code: 42)
        }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 0)
    }

    @BigSyncBackgroundActor
    func testProviderErrorDoesNotMaskAccountInvalidation() async throws {
        let f = try fixture()
        let fence = f.sync.accountScopeAuthorityFence
        f.account.handler = {
            fence.poison()
            throw NSError(domain: "RetiredHealthProvider", code: 43)
        }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await request.result)
        XCTAssertEqual(f.store.readCount, 0)
    }

    @BigSyncBackgroundActor
    func testCurrentHealthPreservesStoredFieldsAndFreshReads() async throws {
        let f = try fixture()
        for _ in 0..<2 {
            let result = try await f.sync.syncHealthSnapshot()
            let value = try XCTUnwrap(result)
            XCTAssertEqual(value.category, .succeeded)
            XCTAssertEqual(value.accountScopeIdentifier, CloudKitSynchronizer.accountScopeIdentifier(for: "health-a"))
            XCTAssertEqual(value.lastSuccessAt, f.recordedAt)
            XCTAssertEqual(value.updatedAt, f.recordedAt)
            XCTAssertNil(value.lastFailureAt)
            XCTAssertNil(value.retryNotBefore)
        }
        XCTAssertEqual(f.account.calls, 2, "A health read must not cache account identity")
        XCTAssertEqual(f.store.readCount, 2)
        XCTAssertEqual(f.store.writeCount, 0)
        XCTAssertEqual(f.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testStableStoppedAccountCanReadHealthWithoutGrantingWriteAuthority() async throws {
        let f = try fixture()
        f.sync.accountScopeAuthorityFence.poison()
        let generation = f.sync.accountScopeAuthorityFence.invalidationGenerationSnapshot
        let value = try await f.sync.syncHealthSnapshot()
        XCTAssertEqual(value?.lastSuccessAt, f.recordedAt)
        XCTAssertTrue(f.sync.accountScopeAuthorityFence.rejectsAuthority)
        XCTAssertEqual(f.sync.accountScopeAuthorityFence.invalidationGenerationSnapshot, generation)
        XCTAssertNil(f.sync.activeRunContext)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testIndependentSameAccountAttemptChangeDoesNotInvalidateDiagnosticRead() async throws {
        let f = try fixture()
        let entered = expectation(description: "diagnostic read across ordinary retry")
        let gate = HealthSnapshotGate()
        let request = heldRead(f, entered: entered, gate: gate)
        await fulfillment(of: [entered], timeout: 2)
        f.sync.synchronizationAttemptID = UUID()
        f.sync.synchronizationRunID = UUID()
        await gate.open()
        let result = try await request.value
        XCTAssertEqual(result?.lastSuccessAt, f.recordedAt)
        XCTAssertEqual(f.account.calls, 1)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testDifferentAccountDoesNotReturnOrEraseSavedHealth() async throws {
        let f = try fixture()
        f.account.handler = { "health-b" }
        let different = try await f.sync.syncHealthSnapshot()
        XCTAssertNil(different)
        f.account.handler = nil
        let original = try await f.sync.syncHealthSnapshot()
        XCTAssertEqual(original?.lastSuccessAt, f.recordedAt)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testRejectedReadDoesNotPoisonNextFreshRead() async throws {
        let f = try fixture()
        let fence = f.sync.accountScopeAuthorityFence
        f.account.handler = { fence.poison(); return "health-a" }
        let old = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await old.result)
        f.account.handler = nil
        let fresh = try await f.sync.syncHealthSnapshot()
        XCTAssertEqual(fresh?.lastSuccessAt, f.recordedAt)
        XCTAssertTrue(fence.rejectsAuthority, "Diagnostic success is not fresh write authority")
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testMissingAndMalformedSnapshotsRemainReadOnly() async throws {
        for malformed in [false, true] {
            let f = try fixture()
            let key = f.sync.durableStateKey("CloudKitSyncHealth.v2")
            f.store.set(value: malformed ? ["category": "not-a-health-category"] : nil, forKey: key)
            f.store.resetObservations()
            let result = try await f.sync.syncHealthSnapshot()
            XCTAssertNil(result)
            XCTAssertEqual(f.account.calls, 1)
            XCTAssertEqual(f.store.readCount, 1)
            XCTAssertEqual(f.store.writeCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testAbsentSnapshotCannotTurnAnInvalidatedReadIntoSuccessfulNil() async throws {
        let f = try fixture()
        f.store.removeObject(forKey: f.sync.durableStateKey("CloudKitSyncHealth.v2"))
        f.store.resetObservations()
        let fence = f.sync.accountScopeAuthorityFence
        f.store.onRead = { fence.poison() }
        let request = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        cancellation(await request.result)
        XCTAssertEqual(f.store.writeCount, 0)
    }

    @BigSyncBackgroundActor
    func testCancellingOneHealthReadDoesNotCancelItsIndependentPeer() async throws {
        let f = try fixture()
        let entered = expectation(description: "both health reads held")
        entered.expectedFulfillmentCount = 2
        let gate = HealthSnapshotGate()
        f.account.handler = { entered.fulfill(); await gate.wait(); return "health-a" }
        let cancelled = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        let live = Task { @BigSyncBackgroundActor in try await f.sync.syncHealthSnapshot() }
        addTeardownBlock {
            cancelled.cancel(); live.cancel(); await gate.open()
            _ = await cancelled.result; _ = await live.result
        }
        await fulfillment(of: [entered], timeout: 2)
        cancelled.cancel()
        await gate.open()
        cancellation(await cancelled.result)
        let result = try await live.value
        XCTAssertEqual(result?.lastSuccessAt, f.recordedAt)
        XCTAssertEqual(f.account.calls, 2)
        XCTAssertEqual(f.store.readCount, 1)
        XCTAssertEqual(f.store.writeCount, 0)
        XCTAssertFalse(f.sync.accountScopeAuthorityFence.rejectsAuthority)
    }

}

@BigSyncBackgroundActor
private final class HealthSnapshotAccount {
    var calls = 0
    var handler: (@Sendable @BigSyncBackgroundActor () async throws -> String)?
    func read() async throws -> String {
        calls += 1
        return try await handler?() ?? "health-a"
    }
}
private actor HealthSnapshotGate {
    private var opened = false
    private var waiting = [CheckedContinuation<Void, Never>]()
    func wait() async {
        if opened { return }
        await withCheckedContinuation { waiting.append($0) }
    }
    func open() {
        opened = true
        let old = waiting; waiting.removeAll()
        old.forEach { $0.resume() }
    }
}
private final class HealthSnapshotStore: NSObject, KeyValueStore, @unchecked Sendable {
    private let lock = NSLock()
    private var values = [String: Any]()
    private var reads = 0
    private var writes = 0
    private var hook: (@Sendable () -> Void)?
    var readCount: Int { lock.withLock { reads } }
    var writeCount: Int { lock.withLock { writes } }
    var onRead: (@Sendable () -> Void)? {
        get { lock.withLock { hook } }
        set { lock.withLock { hook = newValue } }
    }
    func resetObservations() { lock.withLock { reads = 0; writes = 0 } }
    func object(forKey key: String) -> Any? {
        let (value, callback) = lock.withLock {
            reads += 1
            return (values[key], hook)
        }
        callback?() // Never hold the test store's lock across reentry.
        return value
    }
    func bool(forKey key: String) -> Bool { object(forKey: key) as? Bool ?? false }
    func set(value: Any?, forKey key: String) { lock.withLock { writes += 1; values[key] = value } }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { lock.withLock { writes += 1; _ = values.removeValue(forKey: key) } }
    func synchronize() -> Bool { true }
#if canImport(ObjectiveC)
    override func value(forKey key: String) -> Any? { object(forKey: key) }
#endif
}
private final class HealthSnapshotTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private let lock = NSLock()
    private var calls = 0
    var operationCount: Int { lock.withLock { calls } }
    private func reject() -> Error {
        lock.withLock { calls += 1 }
        return NSError(domain: "UnexpectedHealthSnapshotCloudKitOperation", code: 1)
    }
    func subscription(withID: CKSubscription.ID) async throws -> CKSubscription? { throw reject() }
    func save(subscription: CKSubscription) async throws -> CKSubscription { throw reject() }
    func deleteSubscription(withID: CKSubscription.ID) async throws { throw reject() }
    func recordZone(withID: CKRecordZone.ID) async throws -> CKRecordZone { throw reject() }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { throw reject() }
    func deleteRecordZone(withID: CKRecordZone.ID) async throws { throw reject() }
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults { throw reject() }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage { throw reject() }
    func recordZoneChanges(in: CKRecordZone.ID, since: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage { throw reject() }
}
