import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

final class CloudKitAccountAvailabilityCancellationTests: XCTestCase {
    func testAlreadyCancelledRequestDoesNotInvokeStatusProvider() async {
        let invoked = expectation(description: "cancelled request starts no provider")
        invoked.isInverted = true
        let gate = CloudKitAccountAvailabilityGate(statusProvider: { _ in
            invoked.fulfill()
            return .available
        })

        let request = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return await gate.availability(for: "iCloud.cancelled")
        }
        let result = await request.value
        XCTAssertEqual(result, .failed)
        await fulfillment(of: [invoked], timeout: 0.1)
    }

    func testCancellationReturnsBeforeNonCooperativeProviderFinishes() async {
        let entered = expectation(description: "provider entered")
        let returned = expectation(description: "cancelled request returned")
        let providerFinished = expectation(description: "late provider finished")
        let release = AvailabilityCancellationLatch()
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                entered.fulfill()
                await release.wait()
                providerFinished.fulfill()
                return .available
            },
            deadlineNanoseconds: 60_000_000_000
        )
        addTeardownBlock { await release.open() }
        let request = Task {
            let result = await gate.availability(for: "iCloud.cancelled")
            returned.fulfill()
            return result
        }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        // A structured join of the provider would not return until release.
        await fulfillment(of: [returned], timeout: 2)
        await release.open()
        let result = await request.value
        XCTAssertEqual(result, .failed)
        await fulfillment(of: [providerFinished], timeout: 2)
    }

    func testCancellingOneRequestDoesNotCancelAnotherRequest() async {
        let entered = expectation(description: "both providers entered")
        entered.expectedFulfillmentCount = 2
        let cancelledReturned = expectation(description: "cancelled request returned")
        let providersFinished = expectation(description: "both providers finished")
        providersFinished.expectedFulfillmentCount = 2
        let release = AvailabilityCancellationLatch()
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { identifier in
                entered.fulfill()
                await release.wait()
                if identifier == "iCloud.live" {
                    XCTAssertFalse(Task.isCancelled)
                }
                providersFinished.fulfill()
                return .unavailable(.noAccount)
            },
            deadlineNanoseconds: 60_000_000_000
        )
        addTeardownBlock { await release.open() }
        let cancelled = Task {
            let result = await gate.availability(for: "iCloud.cancelled")
            cancelledReturned.fulfill()
            return result
        }
        let live = Task { await gate.availability(for: "iCloud.live") }
        await fulfillment(of: [entered], timeout: 2)
        cancelled.cancel()
        await fulfillment(of: [cancelledReturned], timeout: 2)
        await release.open()
        let cancelledResult = await cancelled.value
        let liveResult = await live.value
        XCTAssertEqual(cancelledResult, .failed)
        XCTAssertEqual(liveResult, .unavailable(.noAccount))
        await fulfillment(of: [providersFinished], timeout: 2)
    }

    func testCancelledRequestDoesNotPoisonSubsequentUseOfGate() async {
        let provider = AvailabilityCancellationCallCounter()
        let gate = CloudKitAccountAvailabilityGate(statusProvider: { _ in
            await provider.recordCall()
            return .unavailable(.noAccount)
        })
        let cancelled = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return await gate.availability(for: "iCloud.example")
        }
        let cancelledResult = await cancelled.value
        let nextResult = await gate.availability(for: "iCloud.example")
        XCTAssertEqual(cancelledResult, .failed)
        XCTAssertEqual(nextResult, .unavailable(.noAccount))
        let callCount = await provider.count
        XCTAssertEqual(callCount, 1, "Only the live caller may start a provider")
    }
}

/// Deliberately ignores cancellation, like a callback that has already been
/// submitted. Every test opens the latch and drains its late provider replies.
private actor AvailabilityCancellationLatch {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

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

private actor AvailabilityCancellationCallCounter {
    private(set) var count = 0
    func recordCall() { count += 1 }
}


final class CloudKitAccountAvailabilityDeadlineTests: XCTestCase {
    func testZeroBudgetDoesNotStartProvider() async {
        let invoked = expectation(description: "zero budget starts no account read")
        invoked.isInverted = true
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                invoked.fulfill()
                return .available
            },
            deadlineNanoseconds: 0
        )
        let result = await gate.availability(for: "iCloud.expired")
        XCTAssertEqual(result, .failed)
        await fulfillment(of: [invoked], timeout: 0.1)
    }

    func testLateAvailableCannotBeatDelayedTimer() async {
        let result = await completionWithHeldTimer(.available, at: 151)
        XCTAssertEqual(result, .failed)
    }

    func testExactDeadlineCannotPublishUnavailableStatus() async {
        let result = await completionWithHeldTimer(.unavailable(.noAccount), at: 150)
        XCTAssertEqual(result, .failed)
    }

    func testOnTimeStatusSurvivesClockAdvanceDuringDelivery() async {
        let result = await completionWithHeldTimer(
            .unavailable(.temporarilyUnavailable), at: 149, advanceAfterSample: 200
        )
        XCTAssertEqual(result, .unavailable(.temporarilyUnavailable))
    }

    func testDelayedTimerUsesOnlyRemainingBudget() async {
        let clock = AvailabilityDeadlineClock(140)
        let providerRelease = AvailabilityCancellationLatch()
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                await providerRelease.wait()
                return .available
            },
            deadlineNanoseconds: 50,
            now: { clock.read(first: 100) },
            sleep: { remaining in
                XCTAssertEqual(remaining, 10, "Timer must not restart the original 50ns budget")
                clock.set(150)
            }
        )
        addTeardownBlock { await providerRelease.open() }
        let result = await gate.availability(for: "iCloud.delayed")
        XCTAssertEqual(result, .failed)
        await providerRelease.open()
    }

    func testExpiredProviderAdmissionStartsNoAccountRead() async {
        let clock = AvailabilityDeadlineClock(150)
        let invoked = expectation(description: "queued provider is now expired")
        invoked.isInverted = true
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in invoked.fulfill(); return .available },
            deadlineNanoseconds: 50,
            // Capture and caller preflight are on time. Both subsequently
            // scheduled tasks observe expiry, independent of their order.
            now: { clock.read(first: 100, count: 2) },
            sleep: { _ in XCTFail("Already expired timer should not sleep") }
        )
        let result = await gate.availability(for: "iCloud.queued")
        XCTAssertEqual(result, .failed)
        await fulfillment(of: [invoked], timeout: 0.1)
    }

    func testCancellationAtSettlementCannotDeliverAvailable() async {
        let clock = AvailabilityDeadlineClock(100)
        let timerEntered = AvailabilityCancellationLatch()
        let releaseTimer = AvailabilityCancellationLatch()
        let startProvider = AvailabilityCancellationLatch()
        let returned = expectation(description: "cancelled result delivered")
        let requestBox = AvailabilityDeadlineRequestBox()
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                await startProvider.wait()
                await timerEntered.wait()
                // The next clock read occurs inside the single-winner race.
                // Cancel the caller there: completion still settles first on
                // that actor, but caller delivery must observe cancellation.
                clock.onNextRead { requestBox.cancel() }
                return .available
            },
            deadlineNanoseconds: 50,
            now: { clock.read() },
            sleep: { _ in
                await timerEntered.open()
                await releaseTimer.wait()
            }
        )
        addTeardownBlock { await startProvider.open(); await releaseTimer.open() }
        let request = Task {
            let result = await gate.availability(for: "iCloud.delivery")
            returned.fulfill()
            return result
        }
        requestBox.store(request)
        await startProvider.open()
        await fulfillment(of: [returned], timeout: 2)
        await releaseTimer.open()
        let result = await request.value
        XCTAssertTrue(request.isCancelled)
        XCTAssertEqual(result, .failed)
    }

    func testOnTimeStatusesArePreservedWithoutAccountCaching() async {
        for expected in [CloudKitAccountAvailability.available, .failed,
                         .unavailable(.noAccount), .unavailable(.restricted),
                         .unavailable(.couldNotDetermine), .unavailable(.temporarilyUnavailable)] {
            let calls = AvailabilityCancellationCallCounter()
            let gate = CloudKitAccountAvailabilityGate(
                statusProvider: { _ in await calls.recordCall(); return expected },
                deadlineNanoseconds: 50,
                now: { 100 },
                sleep: { _ in try await Task.sleep(nanoseconds: 60_000_000_000) }
            )
            let first = await gate.availability(for: "iCloud.status")
            let second = await gate.availability(for: "iCloud.status")
            XCTAssertEqual(first, expected)
            XCTAssertEqual(second, expected)
            let count = await calls.count
            XCTAssertEqual(count, 2, "Reuse of the gate must perform a fresh read")
        }
    }

    func testExpiredCallDoesNotReuseItsDeadlineForNextCall() async {
        let clock = AvailabilityDeadlineClock(100)
        let calls = AvailabilityCancellationCallCounter()
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                await calls.recordCall()
                if await calls.count == 1 { clock.set(150) }
                return .available
            },
            deadlineNanoseconds: 50,
            now: { clock.read() },
            sleep: { _ in try await Task.sleep(nanoseconds: 60_000_000_000) }
        )
        let expired = await gate.availability(for: "iCloud.original")
        XCTAssertEqual(expired, .failed)
        clock.set(151)
        let next = await gate.availability(for: "iCloud.successor")
        XCTAssertEqual(next, .available)
        let count = await calls.count
        XCTAssertEqual(count, 2)
    }

    private func completionWithHeldTimer(
        _ completion: CloudKitAccountAvailability, at completionTime: UInt64,
        advanceAfterSample: UInt64? = nil
    ) async -> CloudKitAccountAvailability {
        let clock = AvailabilityDeadlineClock(100)
        let timerEntered = AvailabilityCancellationLatch()
        let releaseTimer = AvailabilityCancellationLatch()
        let returned = expectation(description: "provider settles independently of timer delivery")
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                await timerEntered.wait()
                clock.set(completionTime)
                if let advanceAfterSample {
                    clock.onNextRead { clock.set(advanceAfterSample) }
                }
                return completion
            },
            deadlineNanoseconds: 50,
            now: { clock.read() },
            sleep: { remaining in
                XCTAssertEqual(remaining, 50)
                await timerEntered.open()
                await releaseTimer.wait()
            }
        )
        addTeardownBlock { await timerEntered.open(); await releaseTimer.open() }
        let request = Task {
            let result = await gate.availability(for: "iCloud.deadline")
            returned.fulfill()
            return result
        }
        await fulfillment(of: [returned], timeout: 2)
        await releaseTimer.open()
        return await request.value
    }
}

/// Test clock only; callbacks execute outside its lock.
private final class AvailabilityDeadlineClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant: UInt64
    private var reads = 0
    private var nextRead: (@Sendable () -> Void)?
    init(_ instant: UInt64) { self.instant = instant }
    func set(_ instant: UInt64) { lock.withLock { self.instant = instant } }
    func onNextRead(_ callback: @escaping @Sendable () -> Void) {
        lock.withLock { nextRead = callback }
    }
    func read(first initial: UInt64? = nil, count: Int = 1) -> UInt64 {
        let (instant, callback) = lock.withLock {
            reads += 1
            let value = reads <= count ? initial ?? self.instant : self.instant
            let callback = nextRead
            nextRead = nil
            return (value, callback)
        }
        callback?()
        return instant
    }
}

private final class AvailabilityDeadlineRequestBox: @unchecked Sendable {
    private let lock = NSLock()
    private var request: Task<CloudKitAccountAvailability, Never>?
    func store(_ task: Task<CloudKitAccountAvailability, Never>) {
        lock.withLock { request = task }
    }
    func cancel() { lock.withLock { request }?.cancel() }
}
