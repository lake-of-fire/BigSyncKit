import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

final class CloudKitAccountAvailabilityGateTests: XCTestCase {
    func testInjectableAsyncStatusProviderIsUsed() async {
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { identifier in
                XCTAssertEqual(identifier, "iCloud.example")
                return .unavailable(.noAccount)
            }
        )
        let availability = await gate.availability(for: "iCloud.example")
        XCTAssertEqual(availability, .unavailable(.noAccount))
    }

    func testAvailabilityGateReturnsFailedAtItsHardDeadline() async {
        let gate = CloudKitAccountAvailabilityGate(
            statusProvider: { _ in
                try? await Task.sleep(nanoseconds: 60_000_000_000)
                return .available
            },
            deadlineNanoseconds: 1_000_000
        )

        let startedAt = ContinuousClock.now
        let availability = await gate.availability(for: "iCloud.example")

        XCTAssertEqual(availability, .failed)
        XCTAssertLessThan(
            startedAt.duration(to: .now),
            .seconds(1)
        )
    }

    func testCallbackBridgeHasAHardDeadlineAndIgnoresLateCompletion()
    async {
        var lateCompletion: ((Result<String, Error>) -> Void)?
        do {
            let _: String = try await awaitCancellableCloudKitCallback(
                timeoutNanoseconds: 1_000_000
            ) { completion in
                lateCompletion = completion
            }
            XCTFail("Expected callback deadline")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .networkFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        // Finishing an already-timed-out stream is deliberately harmless.
        lateCompletion?(.success("late"))
    }
}

/// Actual callback bridge with a controlled clock; no CloudKit request is sent.
final class CloudKitCallbackAdmissionTests: XCTestCase {
    private enum Failure: Error, Equatable { case original }

    func testPrecancelledCallerDoesNotRegisterCallback() async {
        let calls = CallbackAdmissionCounter()
        let request = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            do {
                let _: String = try await awaitCancellableCloudKitCallback { done in
                    calls.record(); done(.success("account"))
                }
                XCTFail("Cancelled caller received a buffered value")
            } catch { XCTAssertTrue(error is CancellationError) }
        }
        await request.value
        XCTAssertEqual(calls.count, 0)
    }

    func testZeroBudgetDoesNotRegisterCallback() async {
        let calls = CallbackAdmissionCounter()
        do {
            let _: String = try await awaitCancellableCloudKitCallback(
                timeoutNanoseconds: 0, now: { 100 }
            ) { done in calls.record(); done(.success("account")) }
            XCTFail("Zero budget accepted a result")
        } catch let error as CKError { XCTAssertEqual(error.code, .networkFailure) }
        catch { XCTFail("Unexpected error: \(error)") }
        XCTAssertEqual(calls.count, 0)
    }

    func testRegistrationConsumesBudgetBeforeSynchronousCompletion() async {
        let clock = CallbackAdmissionClock(100)
        do {
            let _: String = try await awaitCancellableCloudKitCallback(
                timeoutNanoseconds: 50, now: { clock.now }
            ) { done in clock.set(150); done(.success("too-late")) }
            XCTFail("Registration renewed the budget")
        } catch let error as CKError { XCTAssertEqual(error.code, .networkFailure) }
        catch { XCTFail("Unexpected error: \(error)") }
    }

    func testLateCallbackErrorBecomesDeadlineFailure() async {
        let clock = CallbackAdmissionClock(100)
        do {
            let _: String = try await awaitCancellableCloudKitCallback(
                timeoutNanoseconds: 50, now: { clock.now }
            ) { done in clock.set(151); done(.failure(Failure.original)) }
            XCTFail("Late error was accepted")
        } catch let error as CKError { XCTAssertEqual(error.code, .networkFailure) }
        catch { XCTFail("Late error escaped the deadline: \(error)") }
    }

    func testOnTimeBufferedValueSurvivesDelayedRegistrationReturn() async throws {
        let clock = CallbackAdmissionClock(100)
        let value: String = try await awaitCancellableCloudKitCallback(
            timeoutNanoseconds: 50, now: { clock.now }
        ) { done in
            clock.set(149); done(.success("accepted")); clock.set(500)
        }
        XCTAssertEqual(value, "accepted")
    }

    func testOnTimeBufferedErrorSurvivesDelayedRegistrationReturn() async {
        let clock = CallbackAdmissionClock(100)
        do {
            let _: String = try await awaitCancellableCloudKitCallback(
                timeoutNanoseconds: 50, now: { clock.now }
            ) { done in
                clock.set(149); done(.failure(Failure.original)); clock.set(500)
            }
            XCTFail("Expected original error")
        } catch { XCTAssertEqual(error as? Failure, .original) }
    }

    func testCancellationDuringRegistrationDefeatsBufferedSuccess() async {
        let request = Task {
            do {
                let _: String = try await awaitCancellableCloudKitCallback { done in
                    done(.success("accepted"))
                    withUnsafeCurrentTask { $0?.cancel() }
                }
                XCTFail("Cancelled caller received a buffered value")
            } catch { XCTAssertTrue(error is CancellationError) }
        }
        await request.value
    }

    func testCancellationDuringRegistrationDefeatsBufferedError() async {
        let request = Task {
            do {
                let _: String = try await awaitCancellableCloudKitCallback { done in
                    done(.failure(Failure.original))
                    withUnsafeCurrentTask { $0?.cancel() }
                }
                XCTFail("Expected cancellation")
            } catch { XCTAssertTrue(error is CancellationError, "Original error replaced cancellation: \(error)") }
        }
        await request.value
    }

    func testUnboundedCallDoesNotConsultClockAndKeepsFirstResult() async throws {
        let value: String = try await awaitCancellableCloudKitCallback(
            now: { XCTFail("Unbounded callback consulted deadline clock"); return 0 }
        ) { done in
            done(.success("first")); done(.failure(Failure.original)); done(.success("second"))
        }
        XCTAssertEqual(value, "first")
    }

    func testOverflowSaturatesWithoutRejectingOnTimeCallback() async throws {
        let clock = CallbackAdmissionClock(UInt64.max - 20)
        let value: String = try await awaitCancellableCloudKitCallback(
            timeoutNanoseconds: 100, now: { clock.now }
        ) { done in clock.set(UInt64.max - 1); done(.success("accepted")) }
        XCTAssertEqual(value, "accepted")
    }

    func testCancellationWhileCallbackNeverArrivesReturnsAndIgnoresLateReply() async {
        let registered = expectation(description: "callback registered")
        let returned = expectation(description: "caller cancelled independently")
        let box = CallbackAdmissionCompletion()
        let request = Task {
            defer { returned.fulfill() }
            do {
                let _: String = try await awaitCancellableCloudKitCallback { done in
                    box.store(done); registered.fulfill()
                }
                XCTFail("Cancelled waiter succeeded")
            } catch { XCTAssertTrue(error is CancellationError) }
        }
        await fulfillment(of: [registered], timeout: 2)
        request.cancel()
        await fulfillment(of: [returned], timeout: 2)
        box.deliver(.success("late"))
        await request.value
    }
}

private final class CallbackAdmissionClock: @unchecked Sendable {
    private let lock = NSLock()
    private var value: UInt64
    init(_ value: UInt64) { self.value = value }
    var now: UInt64 { lock.withLock { value } }
    func set(_ value: UInt64) { lock.withLock { self.value = value } }
}
private final class CallbackAdmissionCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    var count: Int { lock.withLock { value } }
    func record() { lock.withLock { value += 1 } }
}
private final class CallbackAdmissionCompletion: @unchecked Sendable {
    private let lock = NSLock()
    private var completion: ((Result<String, Error>) -> Void)?
    func store(_ completion: @escaping (Result<String, Error>) -> Void) {
        lock.withLock { self.completion = completion }
    }
    func deliver(_ result: Result<String, Error>) { lock.withLock { completion }?(result) }
}
