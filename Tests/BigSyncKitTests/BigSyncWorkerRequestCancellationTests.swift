import CloudKit
import Foundation
import Logging
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

final class BigSyncWorkerRequestCancellationTests: XCTestCase {
    @BigSyncBackgroundActor
    func testAlreadyCancelledRequestPreservesScheduledStartup() async {
        await assertCancelledRequestPreservesStartup(withDeadline: false)
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledDeadlineRequestPreservesScheduledStartup() async {
        await assertCancelledRequestPreservesStartup(withDeadline: true)
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledRequestPreservesLiveRetry() async {
        await assertCancelledRequestPreservesRetry(withDeadline: false)
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledDeadlineRequestPreservesLiveRetry() async {
        await assertCancelledRequestPreservesRetry(withDeadline: true)
    }

    @BigSyncBackgroundActor
    func testE2EDeadlineOutcomeReportsAlreadyCancelledCaller() async {
        let fixture = makeFixture()
        let request = Task { @BigSyncBackgroundActor in
            withUnsafeCurrentTask { $0?.cancel() }
            return await fixture.worker.cloudKitE2ESynchronizeCloudKit(
                deadlineNanoseconds: 5_000_000_000
            )
        }
        let outcome = await request.value
        guard case .cancelled = outcome else {
            return XCTFail("Expected request-scoped cancellation, got \(outcome)")
        }
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testE2EDeadlineOutcomeDistinguishesCompletedNilFromTimeout() async {
        let fixture = makeFixture()
        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(
            deadlineNanoseconds: 5_000_000_000
        )
        guard case .completed(let result) = outcome else {
            return XCTFail("Expected completed result, got \(outcome)")
        }
        XCTAssertNil(result)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 1)
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testE2EDeadlineTimeoutDoesNotCancelSharedRestorationTask() async {
        let fixture = makeFixture()
        let release = WorkerRestorationRelease()
        let restoration = installRestoration(on: fixture.worker, release: release)

        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(
            deadlineNanoseconds: 1_000_000
        )
        guard case .timedOut = outcome else {
            await release.open()
            await restoration.value
            return XCTFail("Expected deadline outcome, got \(outcome)")
        }
        XCTAssertFalse(
            restoration.isCancelled,
            "Request timeout must not cancel shared restoration authority"
        )
        await release.open()
        await restoration.value
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testLiveExplicitRequestStillSupersedesScheduledStartup() async {
        let fixture = makeFixture()
        fixture.worker._test_scheduleDormantInitialSynchronization()
        let result = await fixture.worker.synchronizeCloudKit()
        XCTAssertNil(result)
        XCTAssertFalse(fixture.worker._test_hasScheduledInitialSynchronization)
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 1)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testCancelledRestorationWaiterDoesNotEnterPreflight() async {
        let fixture = makeFixture()
        let release = WorkerRestorationRelease()
        let restoration = installRestoration(on: fixture.worker, release: release)
        let entered = expectation(description: "request reaches restoration barrier")
        let originalValidation = fixture.synchronizer.accountValidationRequired
        let originalUnauthentication = fixture.synchronizer.cancelledDueToUnauthentication
        let request = Task { @BigSyncBackgroundActor in
            entered.fulfill()
            return await fixture.worker.synchronizeCloudKit()
        }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        await release.open()
        let result = await request.value
        await restoration.value

        XCTAssertNil(result)
        XCTAssertFalse(restoration.isCancelled)
        XCTAssertFalse(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
        XCTAssertEqual(fixture.synchronizer.accountValidationRequired, originalValidation)
        XCTAssertEqual(fixture.synchronizer.cancelledDueToUnauthentication, originalUnauthentication)
    }

    @BigSyncBackgroundActor
    func testCancellingOneWaiterPreservesRestorationAndLiveWaiter() async {
        let fixture = makeFixture()
        let release = WorkerRestorationRelease()
        let restoration = installRestoration(on: fixture.worker, release: release)
        let firstEntered = expectation(description: "first request reaches restoration")
        let liveEntered = expectation(description: "live request reaches restoration")
        let cancelled = Task { @BigSyncBackgroundActor in
            firstEntered.fulfill()
            return await fixture.worker.synchronizeCloudKit()
        }
        let live = Task { @BigSyncBackgroundActor in
            liveEntered.fulfill()
            return await fixture.worker.synchronizeCloudKit()
        }
        await fulfillment(of: [firstEntered, liveEntered], timeout: 2)
        cancelled.cancel()
        XCTAssertFalse(restoration.isCancelled)
        let before = await fixture.availability.count
        XCTAssertEqual(before, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)

        await release.open()
        let cancelledResult = await cancelled.value
        let liveResult = await live.value
        await restoration.value
        XCTAssertNil(cancelledResult)
        XCTAssertNil(liveResult) // The live request receives the deliberate preflight failure.
        XCTAssertFalse(restoration.isCancelled)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 1, "Only the live waiter may enter account preflight")
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    private func assertCancelledRequestPreservesStartup(withDeadline: Bool) async {
        let fixture = makeFixture()
        fixture.worker._test_scheduleDormantInitialSynchronization()
        let result = await runAlreadyCancelledRequest(fixture.worker, withDeadline: withDeadline)
        XCTAssertNil(result)
        XCTAssertTrue(fixture.worker._test_hasScheduledInitialSynchronization)
        XCTAssertFalse(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    private func assertCancelledRequestPreservesRetry(withDeadline: Bool) async {
        let fixture = makeFixture()
        _ = await fixture.worker.synchronizeCloudKit()
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let before = await fixture.availability.count
        XCTAssertEqual(before, 1)

        let result = await runAlreadyCancelledRequest(fixture.worker, withDeadline: withDeadline)
        XCTAssertNil(result)
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let after = await fixture.availability.count
        XCTAssertEqual(after, before)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    private func runAlreadyCancelledRequest(
        _ worker: BigSyncBackgroundActor,
        withDeadline: Bool
    ) async -> CloudKitSynchronizer.SynchronizationResult? {
        let request = Task { @BigSyncBackgroundActor in
            if withDeadline {
                return await worker.synchronizeCloudKit(deadlineNanoseconds: 5_000_000_000)
            }
            return await worker.synchronizeCloudKit()
        }
        // Both closures run on the same global actor. There is no suspension
        // between creation and cancellation, so admission sees cancellation.
        request.cancel()
        return await request.value
    }

    @BigSyncBackgroundActor
    private func makeFixture() -> WorkerPreflightFixture {
        let fixture = WorkerPreflightFixture()
        addTeardownBlock { @BigSyncBackgroundActor in
            await fixture.worker.cancelSynchronization()
            await fixture.synchronizer.cancelSynchronizationAndWait()
        }
        return fixture
    }

    @BigSyncBackgroundActor
    private func installRestoration(
        on worker: BigSyncBackgroundActor,
        release: WorkerRestorationRelease
    ) -> Task<Void, Never> {
        let restoration = Task { @BigSyncBackgroundActor in
            await release.wait()
        }
        worker._test_installPublicationRestoration(restoration)
        addTeardownBlock {
            await release.open()
            await restoration.value
        }
        return restoration
    }
}

private actor WorkerAvailabilityCalls {
    private(set) var count = 0
    func record() { count += 1 }
}

private actor WorkerRestorationRelease {
    private var isOpen = false
    private var waiters = [CheckedContinuation<Void, Never>]()
    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func open() {
        isOpen = true
        let current = waiters
        waiters.removeAll()
        for waiter in current { waiter.resume() }
    }
}

@BigSyncBackgroundActor
private final class WorkerPreflightFixture {
    let availability = WorkerAvailabilityCalls()
    let transport = WorkerUnexpectedTransport()
    let worker: BigSyncBackgroundActor
    let synchronizer: CloudKitSynchronizer

    init() {
        let calls = availability
        worker = BigSyncBackgroundActor(accountAvailabilityGate: CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                await calls.record()
                return .failed
            },
            deadlineNanoseconds: 5_000_000_000
        ))
        let zone = CKRecordZone.ID(zoneName: UUID().uuidString, ownerName: CKCurrentUserDefaultName)
        synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.worker-cancellation-tests",
            database: transport,
            recordZoneID: zone,
            keyValueStore: WorkerMemoryStore(),
            accountIdentifierProvider: { "test-account" },
            accountStatusProvider: { .available },
            changeFeed: transport,
            subscriptionStore: transport,
            zoneStore: transport,
            recordStore: transport,
            logger: Logger(label: "WorkerCancellationTests")
        )
        worker._test_installSynchronizer(synchronizer)
    }
}

private final class WorkerMemoryStore: NSObject, KeyValueStore, @unchecked Sendable {
    private let lock = NSLock()
    private var values = [String: Any]()
    func object(forKey key: String) -> Any? { lock.withLock { values[key] } }
    func bool(forKey key: String) -> Bool { object(forKey: key) as? Bool ?? false }
    func set(value: Any?, forKey key: String) { lock.withLock { values[key] = value } }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { lock.withLock { _ = values.removeValue(forKey: key) } }
    func synchronize() -> Bool { true }
    override func value(forKey key: String) -> Any? { object(forKey: key) }
}

// Every transport is injected; an accidentally admitted drain cannot touch
// CloudKit. Counting the calls is stronger than inspecting an empty adapter list.
private final class WorkerUnexpectedTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    private enum Failure: Error { case unexpectedOperation }
    private let lock = NSLock()
    private var calls = 0
    var databaseScope: CKDatabase.Scope { .private }
    var operationCount: Int { lock.withLock { calls } }
    private func reject() -> Failure {
        lock.withLock { calls += 1 }
        return .unexpectedOperation
    }
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? { throw reject() }
    func save(subscription: CKSubscription) async throws -> CKSubscription { throw reject() }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws { throw reject() }
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone { throw reject() }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { throw reject() }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws { throw reject() }
    func modifyRecords(
        saving recordsToSave: [CKRecord], deleting recordIDsToDelete: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool
    ) async throws -> CloudKitRecordMutationResults { throw reject() }
    func databaseChanges(
        since cursor: DatabaseChangeCursor?, resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage { throw reject() }
    func recordZoneChanges(
        in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage { throw reject() }
}
