import Foundation
import RealmSwift
import RealmSwiftGaps
import XCTest
#if os(iOS)
import UIKit
#endif
@testable import BigSyncKit

@objc(BigSyncDurableExecutionTestRow)
private final class DurableExecutionRow: Object {
    @Persisted(primaryKey: true) var id = ""
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
}

@MainActor
final class BigSyncDurableWriteExecutionTests: XCTestCase {
    private final class LeaseLifetime: @unchecked Sendable {
        let state = BigSyncWriteExecutionState()
        private let lock = NSLock()
        private var releaseCount = 0

        init() {
            state.installReleaseHandler { [weak self] in
                guard let self else { return }
                self.lock.withLock { self.releaseCount += 1 }
            }
        }

        var releases: Int { lock.withLock { releaseCount } }
    }

    func testExecutionExpirationCancelsQueuedWriteWithoutTouchingItsPeer() async throws {
        let realm = try await makeRealm()
        try realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        realm.add(row("peer"))
        let execution = LeaseLifetime()
        let acquired = expectation(description: "Queued writer acquired its own execution lease")
        let writer = Task { @MainActor in
            try await RealmWriteExecutionProtection.$synchronousAcquisition.withValue({
                acquired.fulfill()
                return execution.state.lease
            }) {
                try await realm.asyncWritePreservingOwnership {
                    realm.add(self.row("retired"))
                    XCTFail("An expired queued writer must never enter its mutation body")
                }
            }
        }
        await fulfillment(of: [acquired], timeout: 3)
        // Both tasks use this Realm's actor. Its writer queues the SDK ticket
        // before suspending, so this actor turn interrupts a genuinely queued
        // request, not an unrelated task or a borrowed peer transaction.
        XCTAssertEqual(execution.releases, 0)
        execution.state.interrupt()
        do {
            try await writer.value
            XCTFail("Execution expiry must settle the queued writer as cancelled")
        } catch is CancellationError { }
        XCTAssertEqual(execution.releases, 1)
        XCTAssertTrue(realm.isInWriteTransaction)
        XCTAssertNotNil(realm.object(ofType: DurableExecutionRow.self, forPrimaryKey: "peer"))
        XCTAssertNil(realm.object(ofType: DurableExecutionRow.self, forPrimaryKey: "retired"))
        try realm.commitWrite()
    }

    func testExecutionExpirationRollsBackOwnedMutationAndAllowsFreshLease() async throws {
        let realm = try await makeRealm()
        let expired = LeaseLifetime()
        do {
            try await RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ expired.state.lease }) {
                try await realm.asyncWritePreservingOwnership {
                    realm.add(row("uncommitted"))
                    expired.state.interrupt()
                    XCTAssertEqual(expired.releases, 0, "Expiry must not release the operation's assertion")
                }
            }
            XCTFail("Expired uncommitted data must roll back")
        } catch is CancellationError { }
        XCTAssertFalse(realm.isInWriteTransaction)
        XCTAssertNil(realm.object(ofType: DurableExecutionRow.self, forPrimaryKey: "uncommitted"))
        XCTAssertEqual(expired.releases, 1)

        let replacement = LeaseLifetime()
        try await RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ replacement.state.lease }) {
            try await realm.asyncWritePreservingOwnership {
                realm.add(row("committed"))
                XCTAssertEqual(replacement.releases, 0)
            }
        }
        XCTAssertFalse(realm.isInWriteTransaction)
        XCTAssertNotNil(realm.object(ofType: DurableExecutionRow.self, forPrimaryKey: "committed"))
        XCTAssertEqual(replacement.releases, 1)
        expired.state.release()
        XCTAssertEqual(expired.releases, 1, "Late cleanup must not release twice or retire the successor")
        XCTAssertEqual(replacement.releases, 1)
    }

#if os(iOS)
    func testInstalledApplicationPolicySurvivesPreparationAndProtectsDetachedWrites() async throws {
        // Use the real current application policy, not a global test provider
        // that would escape this test and poison unrelated storage operations.
        BigSyncDurableWriteExecution.installApplicationPolicy(application: UIApplication.shared)
        let parentLease = try XCTUnwrap(RealmWriteExecutionProtection.acquireSynchronousLease())
        defer { parentLease.release() }
        try parentLease.checkpoint()
        parentLease.release()
        BigSyncDurableWriteExecution.prepareApplicationPolicy()

        let committed = try await RealmWriteExecutionProtection.$synchronousAcquisition.withValue({
            throw CancellationError()
        }) {
            XCTAssertThrowsError(try RealmWriteExecutionProtection.acquireSynchronousLease()) {
                XCTAssertTrue($0 is CancellationError)
            }
            // Detached callbacks do not inherit the orchestration task's
            // override. They must still use the application-installed default,
            // acquiring fresh assertions after the parent's lease was released.
            return try await Task.detached { @RealmBackgroundActor in
                XCTAssertNil(RealmWriteExecutionProtection.synchronousAcquisition)
                var configuration = Realm.Configuration(inMemoryIdentifier: UUID().uuidString)
                configuration.objectTypes = [DurableExecutionRow.self]
                let realm = try await RealmWriteExecutionProtection.openRealm(
                    configuration: configuration, actor: RealmBackgroundActor.shared
                )
                try await realm.asyncWritePreservingOwnership {
                    let object = DurableExecutionRow()
                    object.id = "detached"
                    realm.add(object)
                }
                return realm.object(ofType: DurableExecutionRow.self, forPrimaryKey: "detached") != nil
            }.value
        }
        XCTAssertTrue(committed)
    }
#endif

    private func makeRealm() async throws -> Realm {
        var configuration = Realm.Configuration(inMemoryIdentifier: UUID().uuidString)
        configuration.objectTypes = [DurableExecutionRow.self]
        return try await Realm(configuration: configuration, actor: MainActor.shared)
    }

    private func row(_ identifier: String) -> DurableExecutionRow {
        let value = DurableExecutionRow()
        value.id = identifier
        return value
    }
}
