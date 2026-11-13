import CloudKit
import Darwin
import Foundation
import RealmSwift
import RealmSwiftGaps
import XCTest
@testable import BigSyncKit

@objc(BigSyncMetadataExecutionMutation)
private final class MetadataExecutionMutation: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = "value"
    @Persisted var value = "original"
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
}

final class BigSyncMetadataExecutionTests: XCTestCase {
    private final class State: @unchecked Sendable {
        let lock = NSLock()
        var expired = false
        var released = false
        var checks = 0
        func expire() { lock.withLock { expired = true } }
        var lease: RealmWriteExecutionLease {
            .init(checkpoint: { [self] in
                try lock.withLock {
                    checks += 1
                    if expired { throw CancellationError() }
                }
            }, installInterruptionHandler: { _ in }, release: { [self] in
                lock.withLock { released = true }
            })
        }
    }

    private func withRoot(_ body: (URL) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncMetadataExecution-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try body(root)
    }

    func testDeniedFileStoreReadAndWriteAreRetryableAndDoNotRetainLocks() throws {
        try withRoot { root in
            let url = root.appendingPathComponent("state.plist")
            let store = FileKeyValueStore(fileURL: url)
            try store.setDurably(value: "old", forKey: "value")
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ throw CancellationError() }) {
                XCTAssertThrowsError(try store.durableObject(forKey: "value")) { XCTAssertTrue($0 is CancellationError) }
                XCTAssertThrowsError(try store.setDurably(value: "denied", forKey: "value")) { XCTAssertTrue($0 is CancellationError) }
            }
            XCTAssertEqual(try store.durableObject(forKey: "value") as? String, "old")
            try store.setDurably(value: "retry", forKey: "value")
            XCTAssertEqual(try store.durableObject(forKey: "value") as? String, "retry")
            try store.validateDurability()
        }
    }

    func testExpirationBeforeAtomicPublicationPreservesOldValueAndAllowsRetry() throws {
        try withRoot { root in
            let url = root.appendingPathComponent("state.plist")
            try FileKeyValueStore(fileURL: url).setDurably(value: "old", forKey: "value")
            let state = State()
            let store = FileKeyValueStore(fileURL: url, beforeAtomicReplace: { state.expire() })
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ state.lease }) {
                XCTAssertThrowsError(try store.setDurably(value: "cancelled", forKey: "value")) {
                    XCTAssertTrue($0 is CancellationError)
                }
            }
            XCTAssertTrue(state.lock.withLock { state.released })
            XCTAssertEqual(try store.durableObject(forKey: "value") as? String, "old")
            try store.setDurably(value: "retry", forKey: "value")
            XCTAssertEqual(try store.durableObject(forKey: "value") as? String, "retry")
            XCTAssertFalse(try FileManager.default.contentsOfDirectory(atPath: root.path).contains { $0.hasSuffix(".tmp") })
        }
    }

    func testStoreReleasesExecutionOnlyAfterFileLockIsReleased() throws {
        try withRoot { root in
            let url = root.appendingPathComponent("state.plist")
            let store = FileKeyValueStore(fileURL: url, beforeAtomicReplace: {})
            let peer = Darwin.open(url.appendingPathExtension("lock").path, O_RDWR)
            guard peer >= 0 else { throw POSIXError(.EIO) }
            defer { Darwin.close(peer) }
            let state = State()
            let lease = RealmWriteExecutionLease(checkpoint: state.lease.checkpoint,
                installInterruptionHandler: { _ in }, release: {
                    XCTAssertEqual(bigSyncFlock(peer, LOCK_EX | LOCK_NB), 0)
                    _ = bigSyncFlock(peer, LOCK_UN)
                    state.lease.release()
                })
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ lease }) {
                try store.setDurably(value: "saved", forKey: "value")
            }
            XCTAssertTrue(state.lock.withLock { state.released })
        }
    }

    func testContentionCanBeInterruptedWithoutUnlockingPeerOrPoisoningStore() throws {
        try withRoot { root in
            let url = root.appendingPathComponent("state.plist")
            let store = FileKeyValueStore(fileURL: url)
            try store.setDurably(value: "old", forKey: "value")
            let peer = Darwin.open(url.appendingPathExtension("lock").path, O_RDWR)
            guard peer >= 0 else { throw POSIXError(.EIO) }
            defer { Darwin.close(peer) }
            XCTAssertEqual(bigSyncFlock(peer, LOCK_EX | LOCK_NB), 0)
            let state = State()
            let lease = RealmWriteExecutionLease(checkpoint: {
                try state.lock.withLock {
                    state.checks += 1
                    if state.checks >= 3 { throw CancellationError() }
                }
            }, installInterruptionHandler: { _ in }, release: state.lease.release)
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ lease }) {
                XCTAssertThrowsError(try store.setDurably(value: "blocked", forKey: "value")) {
                    XCTAssertTrue($0 is CancellationError)
                }
            }
            XCTAssertTrue(state.lock.withLock { state.released })
            let probe = Darwin.open(url.appendingPathExtension("lock").path, O_RDWR)
            guard probe >= 0 else { throw POSIXError(.EIO) }
            defer { Darwin.close(probe) }
            XCTAssertEqual(bigSyncFlock(probe, LOCK_EX | LOCK_NB), -1)
            XCTAssertTrue(errno == EWOULDBLOCK || errno == EAGAIN)
            XCTAssertEqual(bigSyncFlock(peer, LOCK_UN), 0)
            try store.setDurably(value: "retry", forKey: "value")
            XCTAssertEqual(try store.durableObject(forKey: "value") as? String, "retry")
        }
    }

    func testDeniedBackupDetectionDoesNotPublishStateAndAllowsRetry() throws {
        try withRoot { root in
            let sentinel = root.appendingPathComponent("sentinel")
            let store = FileKeyValueStore(fileURL: root.appendingPathComponent("state.plist"))
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ throw CancellationError() }) {
                XCTAssertThrowsError(try BackupDetection.run(store: store, namespace: "metadata-test", sentinelURL: sentinel)) {
                    XCTAssertTrue($0 is CancellationError)
                }
            }
            XCTAssertFalse(FileManager.default.fileExists(atPath: sentinel.path))
            XCTAssertEqual(try BackupDetection.run(store: store, namespace: "metadata-test", sentinelURL: sentinel), .firstRun)
        }
    }

    func testDeniedMutationIdentityReadRollsBackDomainAndJournalAndAllowsRetry() throws {
        try withRoot { root in
            let identity = BigSyncClientIdentity(synchronizerName: "metadata-rollback", containerName: "iCloud.test",
                recordZoneID: .init(zoneName: "isolated"), sharedStateBaseURL: root)
            _ = try identity.prepareReplicaBindingGenerationIdentifier()
            let provider = identity.makeThrowingMutationJournalIdentityProvider()
            var configuration = Realm.Configuration()
            configuration.inMemoryIdentifier = UUID().uuidString
            configuration.objectTypes = [MetadataExecutionMutation.self, BigSyncPendingMutation.self]
            BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [configuration], mutationJournalIdentityProvider: provider)
            let realm = try Realm(configuration: configuration)
            try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({ throw CancellationError() }) {
                XCTAssertThrowsError(try realm.write {
                    let object = MetadataExecutionMutation()
                    realm.add(object)
                    object.value = "denied"
                    try object.refreshChangeMetadataThrowing(explicitlyModified: true)
                }) { XCTAssertTrue($0 is CancellationError) }
            }
            XCTAssertEqual(realm.objects(MetadataExecutionMutation.self).count, 0)
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 0)
            try realm.write {
                let object = MetadataExecutionMutation()
                realm.add(object)
                object.value = "saved"
                try object.refreshChangeMetadataThrowing(explicitlyModified: true)
            }
            XCTAssertEqual(realm.objects(MetadataExecutionMutation.self).first?.value, "saved")
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
        }
    }
}

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

    func testIdlePreparedClientDoesNotRetainAFileLock() throws {
        try withIdentityLease { identity, competitor in
            XCTAssertNotNil(identity.currentInstallationIdentifier())
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor),
                "An idle configured client must retain restore fencing without a process-lifetime file lock")
        }
    }

    func testLiveClientPresenceBlocksPublicRestoreUntilReleased() throws {
        try withIdentityLease { identity, competitor in
            let leaseURL = BackupDetection.defaultSentinelURL(
                namespace: identity.durableStateNamespace,
                sharedBaseURL: identity.sharedStateBaseURL
            ).appendingPathExtension("lease")
            var peer: BigSyncClientProcessPresence? = try .init(leaseURL: leaseURL)
            var replacements = 0
            try withExtendedLifetime(peer) {
                XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
                XCTAssertThrowsError(try identity.withManualBackupRestore(transactionIdentifier: UUID()) {
                    replacements += 1
                }) { error in
                    XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
                }
            }
            XCTAssertEqual(replacements, 0)
            peer = nil
            _ = try identity.withManualBackupRestore(transactionIdentifier: UUID()) { replacements += 1 }
            XCTAssertEqual(replacements, 1)
        }
    }

    func testDeniedRestoreExecutionDoesNotEnterReplacementOrRetainFileLock() throws {
        try withIdentityLease { identity, competitor in
            XCTAssertThrowsError(try RealmWriteExecutionProtection.$synchronousAcquisition.withValue({
                throw CancellationError()
            }) {
                try identity.withManualBackupRestore(transactionIdentifier: UUID()) {
                    XCTFail("Denied execution must not replace files")
                }
            }) { error in
                XCTAssertTrue(error is CancellationError)
            }
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testDesktopClientPreservesLegacyFenceAfterRejectedAndCompletedRestore() throws {
        try withLease(retainsSharedLock: true) { url, competitor in
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
            XCTAssertEqual(bigSyncFlock(competitor, LOCK_SH | LOCK_NB), 0)
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTFail("A legacy client must exclude restore")
            })
            XCTAssertEqual(bigSyncFlock(competitor, LOCK_UN), 0)
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
        }
    }

    func testRegistrationRejectsActiveLegacyRestoreOnBothPlatforms() throws {
        for retainsSharedLock in [false, true] {
            try BigSyncClientIdentityLeaseRegistry.$retainsSharedLockForTesting.withValue(retainsSharedLock) {
                let root = FileManager.default.temporaryDirectory
                    .appendingPathComponent("BigSyncLegacyRestore-" + UUID().uuidString, isDirectory: true)
                try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
                defer { try? FileManager.default.removeItem(at: root) }
                let url = root.appendingPathComponent("client.lease")
                let descriptor = Darwin.open(url.path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
                guard descriptor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
                defer { Darwin.close(descriptor) }
                XCTAssertEqual(bigSyncFlock(descriptor, LOCK_EX | LOCK_NB), 0)
                XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)) { error in
                    XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
                }
                XCTAssertEqual(bigSyncFlock(descriptor, LOCK_UN), 0)
                try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
                XCTAssertEqual(try canAcquire(LOCK_EX, descriptor: descriptor), !retainsSharedLock)
            }
        }
    }

    func testValidationRejectsBeforeFirstIdentityUnderExclusiveLeaseAndAllowsStartup() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncInitialValidation-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let identity = BigSyncClientIdentity(
            synchronizerName: "initial-validation", containerName: "iCloud.test.restore",
            recordZoneID: .init(zoneName: "restore-zone"), sharedStateBaseURL: root
        )
        let leaseURL = BackupDetection.defaultSentinelURL(
            namespace: identity.durableStateNamespace, sharedBaseURL: root
        ).appendingPathExtension("lease")
        var validations = 0
        var replacements = 0
        var rollbacks = 0
        let transaction = UUID()
        for _ in 0..<2 {
            XCTAssertThrowsError(try identity.withManualBackupRestore(
                transactionIdentifier: transaction,
                validation: {
                    validations += 1
                    let competitor = Darwin.open(leaseURL.path, O_RDWR)
                    guard competitor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
                    defer { Darwin.close(competitor) }
                    XCTAssertFalse(try self.canAcquire(LOCK_SH, descriptor: competitor))
                    throw TestFailure.replacement
                },
                { replacements += 1 },
                rollback: { rollbacks += 1 }
            )) { error in
                XCTAssertTrue(error is TestFailure)
            }
            XCTAssertNil(identity.currentInstallationIdentifier())
        }
        XCTAssertEqual(validations, 2)
        XCTAssertEqual(replacements, 0)
        XCTAssertEqual(rollbacks, 0)
        let installation = try identity.prepareInstallation()
        XCTAssertEqual(identity.currentInstallationIdentifier(), installation)
    }

    func testSharedRetentionPreservesExclusiveOwner() throws {
        try withLease { url, competitor in
            try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
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

    func testThrowingOuterOwnerReleasesFileLockAndPermitsRetry() throws {
        try withLease { url, competitor in
            XCTAssertThrowsError(try BigSyncClientIdentityLeaseRegistry.withExclusive(at: url) {
                try BigSyncClientIdentityLeaseRegistry.retainShared(at: url)
                XCTAssertFalse(try canAcquire(LOCK_SH, descriptor: competitor))
                throw TestFailure.replacement
            }) { error in
                XCTAssertTrue(error is TestFailure)
            }
            XCTAssertTrue(try canAcquire(LOCK_SH, descriptor: competitor))
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
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

    func testRejectedUpgradeLeavesNoIdleFileLock() throws {
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
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
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
            XCTAssertTrue(try canAcquire(LOCK_EX, descriptor: competitor))
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
        try BigSyncClientIdentityLeaseRegistry.$retainsSharedLockForTesting.withValue(false) {
            try withSuspensionSafeIdentityLease(body)
        }
    }

    private func withSuspensionSafeIdentityLease(_ body: (BigSyncClientIdentity, Int32) throws -> Void) throws {
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

    private func withLease(retainsSharedLock: Bool = false, _ body: (URL, Int32) throws -> Void) throws {
        try BigSyncClientIdentityLeaseRegistry.$retainsSharedLockForTesting.withValue(retainsSharedLock) {
            try withConfiguredLease(body)
        }
    }

    private func withConfiguredLease(_ body: (URL, Int32) throws -> Void) throws {
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

final class BigSyncClientProcessPresenceTests: XCTestCase {
    func testRedactedOrInvalidBirthIsUnknown() {
        XCTAssertNil(BigSyncClientProcessPresence.Birth.observed(seconds: 0, microseconds: 0))
        XCTAssertNil(BigSyncClientProcessPresence.Birth.observed(seconds: -1, microseconds: 0))
        XCTAssertNil(BigSyncClientProcessPresence.Birth.observed(seconds: 1, microseconds: -1))
        XCTAssertNil(BigSyncClientProcessPresence.Birth.observed(seconds: 1, microseconds: 1_000_000))
        XCTAssertEqual(BigSyncClientProcessPresence.Birth.observed(seconds: 1, microseconds: 999_999),
            .init(seconds: 1, microseconds: 999_999))
    }

    private func withNamespace(_ body: (URL) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncProcessPresence-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try body(root.appendingPathComponent("client.lease"))
    }

    func testRestoreGateRejectsRegistrationAndAllowsItAfterSettlement() throws {
        try withNamespace { url in
            let restorer = try BigSyncClientProcessPresence(leaseURL: url)
            try restorer.beginExclusive()
            XCTAssertThrowsError(try BigSyncClientProcessPresence(leaseURL: url)) { error in
                XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
            }
            let remaining = try FileManager.default.contentsOfDirectory(
                at: restorer.directoryURL, includingPropertiesForKeys: nil
            ).filter { $0.pathExtension == "json" }
            XCTAssertEqual(remaining.map { $0.resolvingSymlinksInPath() },
                [restorer.recordURL.resolvingSymlinksInPath()])
            restorer.endExclusive()
            let client = try BigSyncClientProcessPresence(leaseURL: url)
            try withExtendedLifetime(client) {
                XCTAssertThrowsError(try restorer.beginExclusive()) { error in
                    XCTAssertEqual(error as? BigSyncClientIdentityLeaseError, .restoreInProgress)
                }
            }
        }
    }

    func testDeadProcessRecordsAndGateDoNotBlockRecovery() throws {
        try withNamespace { url in
            let deadOwner = BigSyncClientProcessPresence.Owner(pid: 1234, birth: nil, nonce: UUID())
            let dead = try BigSyncClientProcessPresence(leaseURL: url, owner: deadOwner, isAlive: { _ in false })
            try dead.beginExclusive()
            let current = try BigSyncClientProcessPresence(leaseURL: url, isAlive: { $0 != deadOwner })
            try withExtendedLifetime(dead) {
                try current.beginExclusive()
                XCTAssertFalse(FileManager.default.fileExists(atPath: dead.recordURL.path))
                current.endExclusive()
            }
            XCTAssertFalse(FileManager.default.fileExists(atPath: current.gateURL.path))
        }
    }

    func testRegistrationPrunesOnlyDeadClientRecords() throws {
        try withNamespace { url in
            let departedOwner = BigSyncClientProcessPresence.Owner(pid: 1234, birth: nil, nonce: UUID())
            let departed = try BigSyncClientProcessPresence(leaseURL: url, owner: departedOwner)
            let live = try BigSyncClientProcessPresence(leaseURL: url, isAlive: { _ in true })
            let damaged = live.directoryURL.appendingPathComponent("damaged.json")
            try Data("unreadable".utf8).write(to: damaged)
            XCTAssertTrue(FileManager.default.fileExists(atPath: departed.recordURL.path))
            let current = try BigSyncClientProcessPresence(leaseURL: url, isAlive: { $0 != departedOwner })
            withExtendedLifetime((departed, live, current)) {
                XCTAssertFalse(FileManager.default.fileExists(atPath: departed.recordURL.path))
                XCTAssertTrue(FileManager.default.fileExists(atPath: live.recordURL.path))
                XCTAssertTrue(FileManager.default.fileExists(atPath: damaged.path))
            }
        }
    }

    func testUnreadablePeerRecordFailsClosedAndWithdrawsRestoreGate() throws {
        try withNamespace { url in
            let current = try BigSyncClientProcessPresence(leaseURL: url)
            let damaged = current.directoryURL.appendingPathComponent("damaged.json")
            try Data("not a client record".utf8).write(to: damaged)
            XCTAssertThrowsError(try current.beginExclusive())
            XCTAssertFalse(FileManager.default.fileExists(atPath: current.gateURL.path))
            XCTAssertTrue(FileManager.default.fileExists(atPath: current.recordURL.path))
            try FileManager.default.removeItem(at: damaged)
            try current.beginExclusive()
            current.endExclusive()
        }
    }

    func testNativeProcessProbeRecognizesThisProcessAndRejectsReusedPIDBirth() throws {
        try withNamespace { url in
            let current = try BigSyncClientProcessPresence(leaseURL: url)
            XCTAssertTrue(BigSyncClientProcessPresence.processIsAlive(current.owner))
            if let birth = current.owner.birth {
                let previous = BigSyncClientProcessPresence.Owner(
                    pid: current.owner.pid,
                    birth: .init(seconds: birth.seconds - 1, microseconds: birth.microseconds),
                    nonce: UUID()
                )
                XCTAssertFalse(BigSyncClientProcessPresence.processIsAlive(previous))
            }
        }
    }
}
