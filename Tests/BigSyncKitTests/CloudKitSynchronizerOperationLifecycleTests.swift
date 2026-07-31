import XCTest
@testable import BigSyncKit

final class CloudKitSynchronizerOperationLifecycleTests: XCTestCase {
    func testTerminalCompletionInvokesFinishedHandlerExactlyOnce() {
        let operation = CloudKitSynchronizerOperation()
        var finishedCount = 0
        operation.finishedHandler = { _ in
            finishedCount += 1
        }

        operation.finish(error: nil)
        operation.finish(error: nil)

        XCTAssertTrue(operation.isFinished)
        XCTAssertEqual(finishedCount, 1)
    }
}
