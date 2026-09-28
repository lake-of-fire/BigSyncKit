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
