import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

final class CloudKitAccountAvailabilityGateTests: XCTestCase {
    func testCancellationReturnsWithoutWaitingForAccountStatusCallback() async {
        let callbacks = AccountStatusCallbackStore()
        let gate = CloudKitAccountAvailabilityGate(
            accountStatusProvider: { _, completion in
                callbacks.install(completion)
            }
        )
        let task = Task {
            await gate.availability(for: "iCloud.example")
        }

        await fulfillment(of: [callbacks.didInstall], timeout: 1)
        task.cancel()

        let result = await task.value
        XCTAssertEqual(result, .failed)

        callbacks.complete(status: .available, error: nil)
        let resultAfterLateCallback = await task.value
        XCTAssertEqual(resultAfterLateCallback, .failed)
    }
}

private final class AccountStatusCallbackStore: @unchecked Sendable {
    let didInstall = XCTestExpectation(description: "account status callback installed")
    private let lock = NSLock()
    private var callback: (@Sendable (CKAccountStatus, Error?) -> Void)?

    func install(_ callback: @escaping @Sendable (CKAccountStatus, Error?) -> Void) {
        lock.lock()
        self.callback = callback
        lock.unlock()
        didInstall.fulfill()
    }

    func complete(status: CKAccountStatus, error: Error?) {
        lock.lock()
        let callback = self.callback
        lock.unlock()
        callback?(status, error)
    }
}
