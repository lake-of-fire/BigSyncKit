import XCTest
@testable import BigSyncKit

final class BigSyncManualRebuildTests: XCTestCase {
    func testManualRebuildIsDeferredBeforeWorkerConfiguration() async {
        let worker = BigSyncBackgroundActor(
            accountAvailabilityGate: CloudKitAccountAvailabilityGate(
                statusProvider: { _ in .available }
            )
        )

        let outcome = await worker.rebuildAndReuploadCloudKitData()

        XCTAssertEqual(outcome, .deferred)
    }
}
