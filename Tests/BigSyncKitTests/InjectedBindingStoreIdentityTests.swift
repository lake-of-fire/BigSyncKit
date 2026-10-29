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

    func testRestoreReopensReplacedCoordinationFileBeforeCheckingPeers() throws {
        try withLease { url, _ in
            try FileManager.default.moveItem(at: url, to: url.appendingPathExtension("retired"))
            let peer = Darwin.open(url.path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
            guard peer >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
            defer { Darwin.close(peer) }
            XCTAssertEqual(bigSyncFlock(peer, LOCK_SH | LOCK_NB), 0)
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTFail("A peer registering on the current path must exclude replacement")
            }) {
                XCTAssertEqual($0 as? BigSyncClientIdentityLeaseError, .restoreInProgress)
            }
            XCTAssertEqual(bigSyncFlock(peer, LOCK_UN), 0)
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: peer), "Restore must fence the current inode")
            }
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: peer))
        }
    }

    func testColdRestoreDoesNotWaitForAnotherExclusiveHolderToRegister() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncColdRestore-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let url = root.appendingPathComponent("client.lease")
        let peer = Darwin.open(url.path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard peer >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(peer) }
        XCTAssertEqual(bigSyncFlock(peer, LOCK_EX | LOCK_NB), 0)
        XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
            XCTFail("A cold restore must reject the existing holder before replacement")
        }) {
            XCTAssertEqual($0 as? BigSyncClientIdentityLeaseError, .restoreInProgress)
        }
        XCTAssertEqual(bigSyncFlock(peer, LOCK_UN), 0)
        XCTAssertNoThrow(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {})
    }

    func testLiveProcessPresenceVetoesRestoreWithoutRetainingAFileLock() throws {
        try withLease { url, competitor in
            let peer = try writePeer(at: url, pid: getpid())
            BigSyncClientIdentityLeaseRegistry.publishInstallationIdentifier("current", at: url)
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
            var replacementRan = false
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                replacementRan = true
            }) {
                XCTAssertEqual($0 as? BigSyncClientIdentityLeaseError, .restoreInProgress)
            }
            XCTAssertFalse(replacementRan)
            XCTAssertTrue(FileManager.default.fileExists(atPath: peer.path))
            XCTAssertEqual(BigSyncClientIdentityLeaseRegistry.cachedInstallationIdentifier(at: url), "current")
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testExitedProcessPresenceIsCollectedBeforeRestoreAndRetry() throws {
        try withLease { url, competitor in
            // Darwin PIDs cannot reach Int32.max; kill(pid, 0) reports ESRCH.
            let peer = try writePeer(at: url, pid: Int32.max)
            var replacements = 0
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                replacements += 1
                XCTAssertFalse(FileManager.default.fileExists(atPath: peer.path))
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) { replacements += 1 }
            XCTAssertEqual(replacements, 2)
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testMalformedProcessPresenceFailsClosedWithoutLeakingAFileLock() throws {
        try withLease { url, competitor in
            let peer = BigSyncClientIdentityLeaseRegistry.ownerDirectory(at: url)
                .appendingPathComponent(UUID().uuidString + ".plist")
            try Data("incomplete owner".utf8).write(to: peer, options: .atomic)
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTFail("Unreadable presence cannot authorize replacement")
            }) {
                XCTAssertEqual($0 as? BigSyncClientIdentityLeaseError, .restoreInProgress)
            }
            XCTAssertTrue(FileManager.default.fileExists(atPath: peer.path))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testProcessPresenceNeverExpiresOrAssumesAnUnknownBootExited() {
        let boot = UUID()
        let nextBoot = UUID()
        let owner = BigSyncClientIdentityLeaseRegistry.ProcessOwner(
            identifier: UUID(), pid: 42, bootSession: boot
        )
        XCTAssertFalse(owner.isDefinitelyExited(currentBootSession: boot, isProcessAlive: { _ in true }))
        XCTAssertFalse(owner.isDefinitelyExited(currentBootSession: nil, isProcessAlive: { _ in true }))
        XCTAssertTrue(owner.isDefinitelyExited(currentBootSession: nextBoot, isProcessAlive: { _ in
            XCTFail("A different boot already proves this process exited")
            return true
        }))
        XCTAssertTrue(owner.isDefinitelyExited(currentBootSession: nil, isProcessAlive: { _ in false }))
        let unknownBoot = BigSyncClientIdentityLeaseRegistry.ProcessOwner(
            identifier: UUID(), pid: 42, bootSession: nil
        )
        XCTAssertFalse(unknownBoot.isDefinitelyExited(currentBootSession: nextBoot, isProcessAlive: { _ in true }))
    }

    func testProcessPresenceIsExcludedFromBackup() throws {
        try withLease { url, _ in
            let directory = BigSyncClientIdentityLeaseRegistry.ownerDirectory(at: url)
            XCTAssertEqual(try directory.resourceValues(forKeys: [.isExcludedFromBackupKey]).isExcludedFromBackup, true)
        }
    }

    func testProcessRegistrationPreservesExclusiveOwner() throws {
        try withLease { url, competitor in
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                try BigSyncClientIdentityLeaseRegistry.retainProcessPresence(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor), "An idle client must not retain a kernel file lock")
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

    func testThrowingRestoreReleasesKernelLockAndPermitsRetry() throws {
        try withLease { url, competitor in
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                try BigSyncClientIdentityLeaseRegistry.retainProcessPresence(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                throw TestFailure.replacement
            }) { error in
                XCTAssertTrue(error is TestFailure)
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor), "An idle client must not retain a kernel file lock")
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

    func testRejectedRestoreLeavesNoKernelLock() throws {
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
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor), "An idle client must not retain a kernel file lock")
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

    func testPublicRestoreReplacementReentryKeepsExclusiveLeaseUntilIdentityPublication() throws {
        try withIdentityLease { identity, competitor in
            let original = try identity.prepareInstallation()
            let transaction = UUID()
            var replacements = 0
            let receipt = try identity.withManualBackupRestore(transactionIdentifier: transaction) {
                replacements += 1
                try checkPublicRestoreReentry(identity, transaction: transaction, competitor: competitor)
            }
            XCTAssertEqual(replacements, 1)
            XCTAssertEqual(receipt.oldInstallationIdentifier, original)
            XCTAssertNotEqual(receipt.newInstallationIdentifier, original)
            XCTAssertEqual(try identity.prepareInstallation(), receipt.newInstallationIdentifier)
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor), "An idle client must not retain a kernel file lock")
            let resumed = try identity.withManualBackupRestore(transactionIdentifier: transaction) {
                XCTFail("A completed transaction must not replace again")
            }
            XCTAssertEqual(resumed, receipt)
        }
    }

    func testPublicRestoreRollbackReentryKeepsFenceUntilIntentCancellation() throws {
        try withIdentityLease { identity, competitor in
            let original = try identity.prepareInstallation()
            let transaction = UUID()
            var rollbacks = 0
            XCTAssertThrowsError(try identity.withManualBackupRestore(transactionIdentifier: transaction, {
                try checkPublicRestoreReentry(identity, transaction: transaction, competitor: competitor)
                throw TestFailure.replacement
            }, rollback: {
                rollbacks += 1
                try checkPublicRestoreReentry(identity, transaction: transaction, competitor: competitor)
            })) { error in
                XCTAssertTrue(error is TestFailure)
            }
            XCTAssertEqual(rollbacks, 1)
            XCTAssertEqual(try identity.prepareInstallation(), original)
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            let retried = try identity.withManualBackupRestore(transactionIdentifier: transaction) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertEqual(retried.oldInstallationIdentifier, original)
            XCTAssertEqual(try identity.prepareInstallation(), retried.newInstallationIdentifier)
        }
    }

    func testRestoreValidationRunsUnderExclusiveLeaseBeforeReplacement() throws {
        try withIdentityLease { identity, competitor in
            let original = try identity.prepareInstallation()
            let transaction = UUID()
            var replacements = 0
            XCTAssertThrowsError(try identity.withManualBackupRestore(
                transactionIdentifier: transaction,
                validation: {
                    XCTAssertFalse(try self.canAcquire(LOCK_SH, descriptor: competitor))
                    throw TestFailure.replacement
                }, {
                    replacements += 1
                }
            )) { XCTAssertTrue($0 is TestFailure) }
            XCTAssertEqual(replacements, 0)
            XCTAssertEqual(try identity.prepareInstallation(), original)
            let receipt = try identity.withManualBackupRestore(transactionIdentifier: transaction) {
                replacements += 1
            }
            XCTAssertEqual(replacements, 1)
            XCTAssertEqual(receipt.oldInstallationIdentifier, original)
        }
    }

    func testCompletedRestoreStillValidatesCapturedBoundary() throws {
        try withIdentityLease { identity, competitor in
            let transaction = UUID()
            let receipt = try identity.withManualBackupRestore(transactionIdentifier: transaction) {}
            XCTAssertThrowsError(try identity.withManualBackupRestore(
                transactionIdentifier: transaction,
                validation: {
                    XCTAssertFalse(try self.canAcquire(LOCK_SH, descriptor: competitor))
                    throw TestFailure.replacement
                }, { XCTFail("Completed restore must never replace again") }
            )) { XCTAssertTrue($0 is TestFailure) }
            XCTAssertEqual(try identity.prepareInstallation(), receipt.newInstallationIdentifier)
            XCTAssertEqual(try identity.withManualBackupRestore(transactionIdentifier: transaction) {
                XCTFail("Completed restore must never replace again")
            }, receipt)
        }
    }

    private func checkPublicRestoreReentry(
        _ identity: BigSyncClientIdentity, transaction: UUID, competitor: Int32
    ) throws {
        XCTAssertNil(identity.currentInstallationIdentifier())
        XCTAssertThrowsError(try identity.prepareInstallation()) { error in
            XCTAssertEqual(error as? BigSyncManualBackupRestoreError, .stateAmbiguous)
        }
        XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor), "Setup cannot downgrade the outer restore")
        XCTAssertThrowsError(try identity.withManualBackupRestore(transactionIdentifier: transaction) {
            XCTFail("A nested replacement must never execute")
        }) { error in
            XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
        }
        XCTAssertThrowsError(try identity.beginManualBackupRestore(transactionIdentifier: transaction)) { error in
            XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
        }
        XCTAssertThrowsError(try identity.cancelManualBackupRestoreIntent(transactionIdentifier: transaction)) { error in
            XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
        }
        XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor), "Nested APIs cannot release the outer fence")
    }

    private func withIdentityLease(_ body: (BigSyncClientIdentity, Int32) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncPublicRestore-\(UUID().uuidString)", isDirectory: true)
        let identity = BigSyncClientIdentity(synchronizerName: "public-restore", containerName: "iCloud.test.restore",
            recordZoneID: .init(zoneName: "restore-zone"), sharedStateBaseURL: root)
        _ = try identity.prepareInstallation()
        defer { try? FileManager.default.removeItem(at: root) }
        let url = BackupDetection.defaultSentinelURL(namespace: identity.durableStateNamespace,
            sharedBaseURL: root).appendingPathExtension("lease")
        let competitor = Darwin.open(url.path, O_RDWR)
        guard competitor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(competitor) }
        try body(identity, competitor)
    }

    private func withLease(_ body: (URL, Int32) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncLease-\(UUID().uuidString)", isDirectory: true)
        let url = root.appendingPathComponent("client.lease")
        try BigSyncClientIdentityLeaseRegistry.retainProcessPresence(at: url)
        defer { try? FileManager.default.removeItem(at: root) }
        let competitor = Darwin.open(url.path, O_RDWR)
        guard competitor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(competitor) }
        try body(url, competitor)
    }

    private func writePeer(at url: URL, pid: Int32) throws -> URL {
        let owner = BigSyncClientIdentityLeaseRegistry.ProcessOwner(
            identifier: UUID(), pid: pid, bootSession: nil
        )
        let file = BigSyncClientIdentityLeaseRegistry.ownerDirectory(at: url)
            .appendingPathComponent(owner.identifier.uuidString + ".plist")
        try PropertyListEncoder().encode(owner).write(to: file, options: .atomic)
        return file
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
