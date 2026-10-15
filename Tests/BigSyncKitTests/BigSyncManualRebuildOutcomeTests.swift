import Foundation
import XCTest
@testable import BigSyncKit

final class BigSyncManualRebuildOutcomeTests: XCTestCase {
    func test_unconfiguredWorkerDefersRebuild() async {
        let worker = BigSyncBackgroundActor()

        let outcome = await worker.rebuildAndReuploadCloudKitData()

        XCTAssertEqual(outcome, .deferred)
    }

    func test_rebuildRequiresFullSuccessReceipt() async {
        let incomplete = CloudKitSynchronizer.SynchronizationResult(
            didImportChanges: false
        )
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: "account",
            accountScopeIdentifier: "scope"
        )
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: context,
            issuerID: UUID(),
            authorizationID: UUID()
        )
        let completed = CloudKitSynchronizer.SynchronizationResult(
            didImportChanges: false,
            receipt: receipt
        )

        let incompleteOutcome = await BigSyncBackgroundActor.manualRebuildOutcome(
            for: incomplete, expectedAccountScopeIdentifier: "scope"
        )
        let completedOutcome = await BigSyncBackgroundActor.manualRebuildOutcome(
            for: completed, expectedAccountScopeIdentifier: "scope"
        )
        let wrongAccountOutcome = await BigSyncBackgroundActor.manualRebuildOutcome(
            for: completed, expectedAccountScopeIdentifier: "replacement-scope"
        )

        XCTAssertEqual(incompleteOutcome, .deferred)
        XCTAssertEqual(completedOutcome, .succeeded(accountScopeIdentifier: "scope"))
        XCTAssertEqual(wrongAccountOutcome, .deferred)
    }
}
