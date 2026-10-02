import CloudKit
import Darwin
import Foundation
import XCTest
@testable import BigSyncKit

final class InjectedBindingStoreIdentityTests: XCTestCase {
    func testInjectedStoreIsTheOnlyPreparedBindingStore() throws {
        try withResources { identity, store, root in
            let installation = try identity.prepareInstallation()
            let binding = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let snapshot = try XCTUnwrap(identity.currentMutationJournalIdentity(store: store))
            XCTAssertEqual(snapshot.installationIdentifier, installation)
            XCTAssertEqual(snapshot.replicaBindingGenerationIdentifier, binding)
            XCTAssertNil(identity.currentMutationJournalIdentity())
            XCTAssertTrue(FileManager.default.fileExists(atPath: root.appendingPathComponent("bigsync-state.plist").path))
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: store), binding)
        }
    }

    func testExistingProviderObservesPendingReplacementAndActivation() throws {
        try withResources { identity, store, _ in
            let original = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let provider = identity.makeMutationJournalIdentityProvider(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount("account-a", store: store, key: key)
            let pending = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "account-a", destinationAccountScopeIdentifier: "account-b",
                store: store, key: key
            )
            XCTAssertNotEqual(pending.bindingGenerationIdentifier, original)
            XCTAssertEqual(provider()?.replicaBindingGenerationIdentifier, pending.bindingGenerationIdentifier)
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: store), pending.bindingGenerationIdentifier)
            _ = try BigSyncReplicaBindingStateStore.activatePort(pending, store: store, key: key)
            XCTAssertEqual(provider()?.replicaBindingGenerationIdentifier, pending.bindingGenerationIdentifier)
        }
    }

    func testFreshStoreAndProviderResumeTheSameDurableBinding() throws {
        try withResources { identity, store, root in
            let binding = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let reopened = FileKeyValueStore(fileURL: root.appendingPathComponent("bigsync-state.plist"), writesAtomically: true)
            try reopened.prepareForUse()
            XCTAssertEqual(identity.currentMutationJournalIdentity(store: reopened)?.replicaBindingGenerationIdentifier, binding)
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: reopened), binding)
        }
    }

    func testMalformedBindingFailsClosedWithoutPreparingAnotherStore() throws {
        try withResources { identity, store, _ in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            try store.bigSyncSetDurably(value: ["version": 999], forKey: key)
            XCTAssertNil(identity.currentMutationJournalIdentity(store: store))
            XCTAssertThrowsError(try identity.prepareReplicaBindingGenerationIdentifier(store: store))
            XCTAssertNil(identity.currentMutationJournalIdentity())
        }
    }

    private func withResources(
        _ body: (BigSyncClientIdentity, FileKeyValueStore, URL) throws -> Void
    ) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("InjectedBindingStoreIdentityTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let identity = BigSyncClientIdentity(
            synchronizerName: "isolated-client",
            containerName: "iCloud.example.injected-store-tests",
            recordZoneID: CKRecordZone.ID(zoneName: "isolated-zone", ownerName: CKCurrentUserDefaultName),
            sharedStateBaseURL: root.appendingPathComponent("identity", isDirectory: true)
        )
        let store = FileKeyValueStore(fileURL: root.appendingPathComponent("bigsync-state.plist"), writesAtomically: true)
        try body(identity, store, root)
    }
}

/// A second independently opened descriptor exercises the actual advisory
/// lock, not just the registry's Mode value. All files are test-owned.
final class BigSyncClientIdentityLeaseRegressionTests: XCTestCase {
    private enum TestFailure: Error { case replacement }

    func testSharedRetentionPreservesExclusiveOwner() throws {
        try withLease { url, competitor in
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testNestedExclusiveIsRejectedBeforeCallbackWithoutDowngrade() throws {
        try withLease { url, competitor in
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                var nestedCallbackRan = false
                XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                    nestedCallbackRan = true
                }) { error in
                    XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
                }
                XCTAssertFalse(nestedCallbackRan)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
        }
    }

    func testThrowingOuterOwnerRestoresSharedLeaseAndPermitsRetry() throws {
        try withLease { url, competitor in
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                throw TestFailure.replacement
            }) { error in
                XCTAssertTrue(error is TestFailure)
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
        }
    }

    func testDifferentClientExclusiveOwnersRemainIndependent() throws {
        try withLease { firstURL, firstCompetitor in
            try withLease { secondURL, secondCompetitor in
                try BigSyncClientIdentityLeaseRegistry.withExclusive(at: firstURL) {
                    try BigSyncClientIdentityLeaseRegistry.withExclusive(at: secondURL) {
                        XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: firstCompetitor))
                        XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: secondCompetitor))
                    }
                    XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: secondCompetitor))
                    XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: firstCompetitor))
                }
            }
        }
    }

    func testRejectedUpgradeRestoresProcessSharedLease() throws {
        try withLease { url, competitor in
            guard bigSyncFlock(competitor, LOCK_SH | LOCK_NB) == 0 else {
                throw POSIXError(.init(rawValue: errno) ?? .EIO)
            }
            defer { _ = bigSyncFlock(competitor, LOCK_UN) }
            var callbackRan = false
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                callbackRan = true
            }) { error in
                XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
            }
            XCTAssertFalse(callbackRan)
            XCTAssertEqual(bigSyncFlock(competitor, LOCK_UN), 0)
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
        }
    }

    func testReadOnlyCacheProbeDoesNotChangeExclusiveOwnership() throws {
        try withLease { url, competitor in
            BigSyncClientIdentityLeaseRegistry.publishInstallationIdentifier("old", at: url)
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertNil(BigSyncClientIdentityLeaseRegistry.cachedInstallationIdentifier(at: url))
                BigSyncClientIdentityLeaseRegistry.invalidateInstallationIdentifier(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
        }
    }

    private func withLease(_ body: (URL, Int32) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncLease-\(UUID().uuidString)", isDirectory: true)
        let url = root.appendingPathComponent("client.lease")
        try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
        defer { try? FileManager.default.removeItem(at: root) }
        let competitor = Darwin.open(url.path, O_RDWR)
        guard competitor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(competitor) }
        try body(url, competitor)
    }

    private func canAcquire(_ operation: Int32, descriptor: Int32) throws -> Bool {
        if bigSyncFlock(descriptor, operation | LOCK_NB) == 0 {
            guard bigSyncFlock(descriptor, LOCK_UN) == 0 else {
                throw POSIXError(.init(rawValue: errno) ?? .EIO)
            }
            return true
        }
        let failure = errno
        guard failure == EWOULDBLOCK || failure == EAGAIN else {
            throw POSIXError(.init(rawValue: failure) ?? .EIO)
        }
        return false
    }
}
