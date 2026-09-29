import Foundation
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

// Only the dedicated dependency-free deadline runner sets this flag. Normal
// Apple package and assembled Reader tests always compile the real worker tests.
#if !BIGSYNC_WORKER_DEADLINE_PORTABLE
import CloudKit
import Logging

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
    func testZeroE2EDeadlineDoesNotRetireStartupOrEnterPreflight() async {
        let fixture = makeFixture()
        fixture.worker._test_scheduleDormantInitialSynchronization()
        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(deadlineNanoseconds: 0)
        guard case .timedOut = outcome else {
            return XCTFail("Zero budget must time out without admission, got \(outcome)")
        }
        XCTAssertTrue(fixture.worker._test_hasScheduledInitialSynchronization)
        XCTAssertFalse(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testZeroPublicDeadlinePreservesLiveRetry() async {
        let fixture = makeFixture()
        _ = await fixture.worker.synchronizeCloudKit()
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let before = await fixture.availability.count
        let result = await fixture.worker.synchronizeCloudKit(deadlineNanoseconds: 0)
        XCTAssertNil(result)
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let after = await fixture.availability.count
        XCTAssertEqual(after, before)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExpiredCallerCutoffPreservesStartupWithoutPreflight() async {
        let fixture = makeFixture()
        fixture.worker._test_scheduleDormantInitialSynchronization()
        // An absolute cutoff in the past is NOT a new positive duration.
        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(untilUptimeNanoseconds: 1)
        guard case .timedOut = outcome else { return XCTFail("Expired caller budget was renewed") }
        XCTAssertTrue(fixture.worker._test_hasScheduledInitialSynchronization)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExpiredCallerCutoffPreservesScheduledRetry() async {
        let fixture = makeFixture()
        _ = await fixture.worker.synchronizeCloudKit()
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let before = await fixture.availability.count
        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(untilUptimeNanoseconds: 1)
        guard case .timedOut = outcome else { return XCTFail("Expected expired caller cutoff") }
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
        let after = await fixture.availability.count
        XCTAssertEqual(after, before)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testCancelledAbsoluteCutoffCallerDoesNotBecomeTimeout() async {
        let fixture = makeFixture()
        fixture.worker._test_scheduleDormantInitialSynchronization()
        let request = Task { @BigSyncBackgroundActor in
            withUnsafeCurrentTask { $0?.cancel() }
            return await fixture.worker.cloudKitE2ESynchronizeCloudKit(untilUptimeNanoseconds: 0)
        }
        let outcome = await request.value
        guard case .cancelled = outcome else { return XCTFail("Cancellation became timeout") }
        XCTAssertTrue(fixture.worker._test_hasScheduledInitialSynchronization)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(fixture.transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testLiveAbsoluteCutoffCanReturnCompletedNil() async {
        let fixture = makeFixture()
        let outcome = await fixture.worker.cloudKitE2ESynchronizeCloudKit(untilUptimeNanoseconds: UInt64.max)
        guard case .completed(let result) = outcome else { return XCTFail("Valid cutoff lost completed-nil") }
        XCTAssertNil(result)
        let count = await fixture.availability.count
        XCTAssertEqual(count, 1)
        XCTAssertTrue(fixture.worker._test_hasScheduledAccountAvailabilityRetry)
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
        XCTAssertTrue(fixture.worker._test_hasPublicationRestorationTask,
                      "A cancelled waiter must not clear shared restoration state")
        _ = await fixture.worker.synchronizeCloudKit()
        XCTAssertFalse(fixture.worker._test_hasPublicationRestorationTask,
                       "The next live waiter may retire the completed barrier")
    }

    @BigSyncBackgroundActor
    func testOldRestorationWaiterDoesNotClearReplacementBarrier() async {
        let original = makeFixture()
        let replacement = makeFixture()
        let oldRelease = WorkerRestorationRelease()
        let oldRestoration = installRestoration(on: original.worker, release: oldRelease)
        let entered = expectation(description: "original request entered restoration")
        let request = Task { @BigSyncBackgroundActor in
            entered.fulfill()
            return await original.worker.synchronizeCloudKit()
        }
        await fulfillment(of: [entered], timeout: 2)

        // Replacement is test-controlled; production configuration is one-shot.
        original.worker._test_installSynchronizer(replacement.synchronizer)
        let newRelease = WorkerRestorationRelease()
        let newRestoration = installRestoration(on: original.worker, release: newRelease)
        await oldRelease.open()
        let result = await request.value
        await oldRestoration.value

        XCTAssertNil(result)
        XCTAssertTrue(original.worker._test_hasPublicationRestorationTask,
                      "An obsolete waiter cleared the replacement's barrier")
        XCTAssertFalse(newRestoration.isCancelled)
        let count = await original.availability.count
        XCTAssertEqual(count, 0)
        XCTAssertEqual(original.transport.operationCount, 0)
        XCTAssertEqual(replacement.transport.operationCount, 0)
        await newRelease.open()
        await newRestoration.value
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

#endif // !BIGSYNC_WORKER_DEADLINE_PORTABLE

/// Exercises the same deadline owner used by the real worker, without CloudKit.
/// These tests also run in the already-registered native test source file.
final class BigSyncDeadlineRaceTests: XCTestCase, @unchecked Sendable {
    private typealias Race = BigSyncDeadlineRace<Int>
    private enum Failure: Error { case stopTimer }

    func testCompletionAfterDeadlineCannotBeatUnscheduledTimer() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(151)
        await race.resolve(.completed(7)) // No timer task has executed.
        guard case .timedOut = await race.value() else {
            return XCTFail("Late completion won before the timer was scheduled")
        }
    }

    func testCompletionAtDeadlineIsExpired() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(150)
        await race.resolve(.completed(7))
        guard case .timedOut = await race.value() else { return XCTFail("Boundary was renewed") }
    }

    func testOnTimeSettlementSurvivesDelayedWaiter() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(149)
        await race.resolve(.completed(7))
        clock.set(900)
        await race.resolve(.timedOut)
        guard case .completed(7) = await race.value() else {
            return XCTFail("Already accepted result was revoked by delayed delivery")
        }
    }

    func testDelayedTimerSleepsOnlyRemainingBudget() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(140) // Timer only starts now.
        await race.waitUntilDeadline { remaining in
            XCTAssertEqual(remaining, 10)
            clock.set(150)
        }
        guard case .timedOut = await race.value() else { return XCTFail("Timer did not expire") }
    }

    func testTimerStartingAfterExpiryDoesNotSleepAgain() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(200)
        await race.waitUntilDeadline { _ in XCTFail("Expired timer restarted its budget") }
        guard case .timedOut = await race.value() else { return XCTFail("Expired timer did not settle") }
    }

    func testEarlyWakeRecomputesRemainingBudget() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(125)
        await race.waitUntilDeadline { remaining in
            if clock.read() == 125 {
                XCTAssertEqual(remaining, 25)
                clock.set(140)
            } else {
                XCTAssertEqual(remaining, 10)
                clock.set(150)
            }
        }
        guard case .timedOut = await race.value() else { return XCTFail("Early wake extended budget") }
    }

    func testEarlyTimeoutSignalDoesNotDefeatOnTimeCompletion() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        let accepted = await race.resolve(.timedOut)
        XCTAssertFalse(accepted)
        await race.resolve(.completed(9))
        guard case .completed(9) = await race.value() else { return XCTFail("Early timer won") }
    }

    func testZeroBudgetCannotAcceptQuickCompletedNil() async {
        let clock = DeadlineTestClock(100)
        let race = BigSyncDeadlineRace<Int?>(durationNanoseconds: 0, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 0)
        await race.resolve(.completed(nil))
        guard case .timedOut = await race.value() else { return XCTFail("Zero budget admitted completion") }
    }

    func testOnTimeCompletedNilRemainsDistinctFromTimeout() async {
        let clock = DeadlineTestClock(100)
        let race = BigSyncDeadlineRace<Int?>(durationNanoseconds: 50, now: { clock.read() })
        await race.resolve(.completed(nil))
        guard case .completed(let value) = await race.value() else { return XCTFail("Nil became timeout") }
        XCTAssertNil(value)
    }

    func testCancelledTimerDoesNotManufactureTimeout() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        let timer = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            await race.waitUntilDeadline { _ in XCTFail("Cancelled timer slept") }
        }
        await timer.value
        let accepted = await race.resolve(.completed(11))
        XCTAssertTrue(accepted)
        guard case .completed(11) = await race.value() else { return XCTFail("Timer cancellation expired request") }
    }

    func testFailedSleepDoesNotManufactureTimeout() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        await race.waitUntilDeadline { _ in throw Failure.stopTimer }
        let accepted = await race.resolve(.completed(12))
        XCTAssertTrue(accepted)
        guard case .completed(12) = await race.value() else { return XCTFail("Failed sleep became timeout") }
    }

    func testCancellationSettlementRemainsDistinctAfterExpiry() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        clock.set(200)
        await race.resolve(.cancelled)
        await race.resolve(.completed(1))
        guard case .cancelled = await race.value() else { return XCTFail("Cancellation became timeout") }
    }

    func testExpiredRequestCannotChangeSuccessorOutcome() async {
        let clock = DeadlineTestClock(100)
        let old = Race(durationNanoseconds: 10, now: { clock.read() })
        let successor = Race(durationNanoseconds: 100, now: { clock.read() })
        clock.set(111)
        await old.resolve(.completed(1))
        await successor.resolve(.completed(2))
        await old.resolve(.cancelled)
        guard case .timedOut = await old.value() else { return XCTFail("Old request revived") }
        guard case .completed(2) = await successor.value() else { return XCTFail("Successor was affected") }
    }

    func testOverflowSaturatesInsteadOfExpiringImmediately() async {
        let clock = DeadlineTestClock(UInt64.max - 20)
        let race = Race(durationNanoseconds: 100, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 20)
        clock.set(UInt64.max - 1)
        XCTAssertEqual(race.remainingNanoseconds, 1)
        await race.resolve(.completed(7))
        guard case .completed(7) = await race.value() else { return XCTFail("Overflow wrapped deadline") }
    }

    func testConcurrentSettlementHasExactlyOneWinner() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        let winners = await withTaskGroup(of: Bool.self) { group in
            for value in 0..<32 { group.addTask { await race.resolve(.completed(value)) } }
            var count = 0
            for await accepted in group where accepted { count += 1 }
            return count
        }
        XCTAssertEqual(winners, 1)
        guard case .completed(let value) = await race.value() else { return XCTFail("No completed result") }
        XCTAssertTrue((0..<32).contains(value))
    }

    func testAbsoluteCutoffDoesNotRenewAfterCallerDelay() async {
        let clock = DeadlineTestClock(500)
        let race = Race(untilUptimeNanoseconds: 150, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 0)
        await race.resolve(.completed(7))
        guard case .timedOut = await race.value() else { return XCTFail("Past cutoff became new duration") }
    }

    func testAbsoluteCutoffSleepsOnlyUnspentCallerBudget() async {
        let clock = DeadlineTestClock(140)
        let race = Race(untilUptimeNanoseconds: 150, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 10)
        await race.waitUntilDeadline { remaining in
            XCTAssertEqual(remaining, 10)
            clock.set(150)
        }
        guard case .timedOut = await race.value() else { return XCTFail("Caller deadline was extended") }
    }

    func testOriginalCutoffIsSharedAcrossSeparatelyConstructedAttempts() async {
        let clock = DeadlineTestClock(100)
        let first = BigSyncDeadlineRace<Int?>(untilUptimeNanoseconds: 150, now: { clock.read() })
        clock.set(120)
        await first.resolve(.completed(nil))
        guard case .completed(nil) = await first.value() else { return XCTFail("First result was not accepted") }
        // Logging/preparation between attempts consumes, rather than renews,
        // the caller's budget even though a new race object is constructed.
        clock.set(151)
        let second = Race(untilUptimeNanoseconds: 150, now: { clock.read() })
        await second.resolve(.completed(8))
        guard case .timedOut = await second.value() else { return XCTFail("Second attempt reset the cutoff") }
    }

    func testAbsoluteOnTimeResultSurvivesLaterDelivery() async {
        let clock = DeadlineTestClock(149)
        let race = Race(untilUptimeNanoseconds: 150, now: { clock.read() })
        await race.resolve(.completed(9))
        clock.set(900)
        await race.resolve(.timedOut)
        guard case .completed(9) = await race.value() else { return XCTFail("Accepted result was revoked") }
    }

    func testAbsoluteExpiredCutoffKeepsCancellationDistinct() async {
        let clock = DeadlineTestClock(500)
        let race = Race(untilUptimeNanoseconds: 150, now: { clock.read() })
        await race.resolve(.cancelled)
        await race.resolve(.completed(8))
        guard case .cancelled = await race.value() else { return XCTFail("Cancellation was relabeled") }
    }

    func testZeroAbsoluteCutoffDoesNotMeanUnlimited() async {
        let clock = DeadlineTestClock(0)
        let race = Race(untilUptimeNanoseconds: 0, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 0)
        await race.resolve(.completed(8))
        guard case .timedOut = await race.value() else { return XCTFail("Zero cutoff admitted work") }
    }

    func testAbsoluteCutoffDoesNotAddUptimeOrOverflow() async {
        let clock = DeadlineTestClock(UInt64.max - 20)
        let race = Race(untilUptimeNanoseconds: UInt64.max - 5, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 15)
        clock.set(UInt64.max - 5)
        await race.resolve(.completed(8))
        guard case .timedOut = await race.value() else { return XCTFail("Cutoff arithmetic added time") }
    }

    func testRelativeHandoffRenewsBudgetButAbsoluteHandoffDoesNot() async {
        let clock = DeadlineTestClock(100)
        let callerCutoff: UInt64 = 150
        let cachedRemaining = callerCutoff - clock.read()
        // Reproduce the inspected Core call sequence: remaining time is
        // calculated before logging and the worker-actor scheduling gap.
        clock.set(160)
        let durationHandoff = Race(durationNanoseconds: cachedRemaining, now: { clock.read() })
        let cutoffHandoff = Race(untilUptimeNanoseconds: callerCutoff, now: { clock.read() })
        await durationHandoff.resolve(.completed(1))
        await cutoffHandoff.resolve(.completed(1))
        guard case .completed(1) = await durationHandoff.value() else {
            return XCTFail("Duration compatibility semantics unexpectedly changed")
        }
        guard case .timedOut = await cutoffHandoff.value() else {
            return XCTFail("Absolute handoff still renewed the caller's budget")
        }
    }

    func testProductionTimerResumesRegisteredWaiter() async {
        let race = Race(durationNanoseconds: 1_000_000)
        let timer = Task { await race.waitUntilDeadline() }
        guard case .timedOut = await race.value() else {
            timer.cancel()
            return XCTFail("Real timer did not settle the waiter")
        }
        await timer.value
    }
}

private final class DeadlineTestClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant: UInt64
    init(_ instant: UInt64) { self.instant = instant }
    func read() -> UInt64 { lock.withLock { instant } }
    func set(_ instant: UInt64) { lock.withLock { self.instant = instant } }
}
