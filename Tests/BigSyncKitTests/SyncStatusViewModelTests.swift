import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private actor HealthSnapshotContinuationSequence {
    private var continuations = [
        CheckedContinuation<CloudKitSyncHealthSnapshot?, Never>
    ]()

    var pendingCount: Int { continuations.count }

    func next() async -> CloudKitSyncHealthSnapshot? {
        await withCheckedContinuation { continuation in
            continuations.append(continuation)
        }
    }

    func resumeFirst(with snapshot: CloudKitSyncHealthSnapshot?) {
        continuations.removeFirst().resume(returning: snapshot)
    }

    func resumeLast(with snapshot: CloudKitSyncHealthSnapshot?) {
        continuations.removeLast().resume(returning: snapshot)
    }
}

final class SyncStatusViewModelTests: XCTestCase {
    @MainActor
    func testHealthNotificationPayloadIsOnlyAWakeup() async {
        let expected = CloudKitSyncHealthSnapshot(
            category: .succeeded,
            accountScopeIdentifier: "current-account",
            updatedAt: Date(timeIntervalSince1970: 2)
        )
        let untrusted = CloudKitSyncHealthSnapshot(
            category: .failed,
            accountScopeIdentifier: "replaced-account",
            updatedAt: Date(timeIntervalSince1970: 1)
        )
        let viewModel = SyncStatusViewModel(
            realmConfiguration: Realm.Configuration(
                inMemoryIdentifier: UUID().uuidString
            )
        )
        viewModel._testCloudKitSyncHealthSnapshotProvider = { expected }

        NotificationCenter.default.post(
            name: .SynchronizerSyncHealthDidChange,
            object: nil,
            userInfo: [
                cloudKitSynchronizerSyncHealthSnapshotKey: untrusted,
            ]
        )
        for _ in 0..<100 where viewModel.cloudKitSyncHealth == nil {
            await Task.yield()
        }

        XCTAssertEqual(viewModel.cloudKitSyncHealth, expected)
        XCTAssertNotEqual(viewModel.cloudKitSyncHealth, untrusted)
    }

    @MainActor
    func testOlderHealthReloadCannotOverwriteNewerResult() async {
        let sequence = HealthSnapshotContinuationSequence()
        let stale = CloudKitSyncHealthSnapshot(
            category: .failed,
            accountScopeIdentifier: "old-account",
            updatedAt: Date(timeIntervalSince1970: 1)
        )
        let current = CloudKitSyncHealthSnapshot(
            category: .succeeded,
            accountScopeIdentifier: "current-account",
            updatedAt: Date(timeIntervalSince1970: 2)
        )
        let viewModel = SyncStatusViewModel(
            realmConfiguration: Realm.Configuration(
                inMemoryIdentifier: UUID().uuidString
            )
        )
        viewModel._testCloudKitSyncHealthSnapshotProvider = {
            await sequence.next()
        }

        viewModel.restoreCloudKitSyncHealth()
        for _ in 0..<100 where await sequence.pendingCount < 1 {
            await Task.yield()
        }
        viewModel.restoreCloudKitSyncHealth()
        for _ in 0..<100 where await sequence.pendingCount < 2 {
            await Task.yield()
        }

        await sequence.resumeLast(with: current)
        for _ in 0..<100 where viewModel.cloudKitSyncHealth != current {
            await Task.yield()
        }
        await sequence.resumeFirst(with: stale)
        for _ in 0..<10 { await Task.yield() }

        XCTAssertEqual(viewModel.cloudKitSyncHealth, current)
    }
}
