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
