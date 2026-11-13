import CloudKit
import Darwin
import Foundation
import XCTest
@testable import BigSyncKit

final class InjectedBindingStoreIdentityTests: XCTestCase {
    func testCanceledPortDoesNotRevivePublicationAcrossPeerStoreABA() throws {
        try withResources { identity, store, root in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount("a", store: store, key: key)
            let predecessor = try identity.capturePublicationAdmission(store: store)
            let peer = FileKeyValueStore(fileURL: root.appendingPathComponent("bigsync-state.plist"))
            let port = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "a", destinationAccountScopeIdentifier: "b", store: peer, key: key)
            _ = try BigSyncReplicaBindingStateStore.cancelPort(port, store: peer, key: key)
            let successor = try identity.capturePublicationAdmission(store: peer)
            XCTAssertEqual(successor.installationIdentifier, predecessor.installationIdentifier)
            XCTAssertEqual(successor.replicaBindingGenerationIdentifier, predecessor.replicaBindingGenerationIdentifier,
                           "Publication retirement must not change journal attribution")
            XCTAssertThrowsError(try predecessor.withCurrent {}) { error in
                XCTAssertEqual(error as? BigSyncReplicaPublicationError, .superseded)
            }
            try successor.withCurrent {
                XCTAssertNoThrow(try successor.validateHeldPublication())
                XCTAssertThrowsError(try predecessor.validateHeldPublication())
            }
        }
    }

    func testDifferentClientNestedPublicationAndSetupRejectBeforeStoreReentry() throws {
        try withResources { identity, store, root in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let other = BigSyncClientIdentity(synchronizerName: "other-client",
                containerName: "iCloud.example.injected-store-tests", recordZoneID: .init(zoneName: "other-zone"),
                sharedStateBaseURL: root.appendingPathComponent("other-identity", isDirectory: true))
            _ = try other.prepareReplicaBindingGenerationIdentifier(store: store)
            let first = try identity.capturePublicationAdmission(store: store)
            let second = try other.capturePublicationAdmission(store: store)
            let otherProvider = other.makeMutationJournalIdentityProvider(store: store)
            try first.withCurrent {
                XCTAssertThrowsError(try second.withCurrent { XCTFail("Nested publication entered") }) { error in
                    XCTAssertEqual(error as? BigSyncReplicaPublicationError, .reentrantPublication)
                }
                XCTAssertThrowsError(try other.capturePublicationAdmission(store: store)) { error in
                    XCTAssertEqual(error as? BigSyncReplicaPublicationError, .reentrantPublication)
                }
                XCTAssertThrowsError(try other.prepareReplicaBindingGenerationIdentifier(store: store)) { error in
                    XCTAssertEqual(error as? BigSyncReplicaPublicationError, .reentrantPublication)
                }
                XCTAssertThrowsError(try other.cancelManualBackupRestoreIntent(transactionIdentifier: UUID())) { error in
                    XCTAssertEqual(error as? BigSyncReplicaPublicationError, .reentrantPublication)
                }
                XCTAssertNil(otherProvider(), "Another client must not borrow this publication's identity")
                XCTAssertNil(other.currentMutationJournalIdentity(storeURL: store.fileURL))
            }
            XCTAssertEqual(try second.withCurrent { 7 }, 7)
        }
    }

    func testThrowingPublicationReleasesBorrowAndRealFileGuard() throws {
        enum Failure: Error { case callback }
        try withResources { identity, store, root in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let admission = try identity.capturePublicationAdmission(store: store)
            let descriptor = Darwin.open(root.appendingPathComponent("bigsync-state.plist.lock").path, O_RDWR)
            guard descriptor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
            defer { Darwin.close(descriptor) }
            XCTAssertThrowsError(try admission.withCurrent { () -> Void in throw Failure.callback }) { error in
                XCTAssertTrue(error is Failure)
            }
            XCTAssertFalse(BigSyncReplicaPublicationReadScope.isActive)
            XCTAssertEqual(bigSyncFlock(descriptor, LOCK_EX | LOCK_NB), 0)
            XCTAssertEqual(bigSyncFlock(descriptor, LOCK_UN), 0)
            XCTAssertEqual(try admission.withCurrent { 9 }, 9)
        }
    }

    func testExistingV1BindingInitializesEpochWithoutChangingItsJournalOrOtherState() throws {
        try withResources { identity, store, _ in
            let generation = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            var legacy = try XCTUnwrap(store.durableObject(forKey: key) as? [String: Any])
            legacy.removeValue(forKey: "publicationEpoch")
            try store.setDurably(value: legacy, forKey: key)
            try store.setDurably(value: Data([7, 8]), forKey: "unrelated-current-state")
            XCTAssertThrowsError(try identity.capturePublicationAdmission(store: store)) { error in
                XCTAssertEqual(error as? BigSyncReplicaPublicationError, .unavailable)
            }
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: store), generation)
            let current = try identity.capturePublicationAdmission(store: store)
            XCTAssertEqual(current.replicaBindingGenerationIdentifier, generation)
            var upgraded = try XCTUnwrap(store.durableObject(forKey: key) as? [String: Any])
            XCTAssertNotNil(try BigSyncReplicaBindingStateStore.publicationEpoch(from: upgraded))
            upgraded.removeValue(forKey: "publicationEpoch")
            XCTAssertTrue(NSDictionary(dictionary: legacy).isEqual(to: upgraded))
            XCTAssertEqual(try store.durableObject(forKey: "unrelated-current-state") as? Data, Data([7, 8]))
            XCTAssertEqual(try current.withCurrent { 3 }, 3)
        }
    }

    func testMalformedEpochCannotAuthorizeCaptureOrPreparation() throws {
        try withResources { identity, store, _ in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            var malformed = try XCTUnwrap(store.durableObject(forKey: key) as? [String: Any])
            malformed["publicationEpoch"] = "not-a-uuid"
            try store.setDurably(value: malformed, forKey: key)
            XCTAssertThrowsError(try identity.capturePublicationAdmission(store: store)) { error in
                XCTAssertEqual(error as? BigSyncReplicaBindingError, .corrupt)
            }
            XCTAssertThrowsError(try identity.prepareReplicaBindingGenerationIdentifier(store: store))
            let retained = try XCTUnwrap(store.durableObject(forKey: key) as? [String: Any])
            XCTAssertTrue(NSDictionary(dictionary: malformed).isEqual(to: retained))
        }
    }

    func testManualRestoreRetiresPublicationUntilReplacementBindingIsPrepared() throws {
        try withResources { identity, store, _ in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let original = try identity.capturePublicationAdmission(store: store)
            let receipt = try identity.withManualBackupRestore(transactionIdentifier: UUID()) {}
            XCTAssertNotEqual(receipt.newInstallationIdentifier, original.installationIdentifier)
            XCTAssertThrowsError(try original.withCurrent {})
            XCTAssertThrowsError(try identity.capturePublicationAdmission(store: store))
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let replacement = try identity.capturePublicationAdmission(store: store)
            XCTAssertEqual(replacement.installationIdentifier, receipt.newInstallationIdentifier)
            XCTAssertNotEqual(replacement.replicaBindingGenerationIdentifier, original.replicaBindingGenerationIdentifier)
            XCTAssertEqual(try replacement.withCurrent { 4 }, 4)
        }
    }

    func testPublicationHoldsRealBindingFileGuardAndBorrowsJournalIdentity() throws {
        try withResources { identity, store, root in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let admission = try identity.capturePublicationAdmission(store: store)
            let provider = identity.makeMutationJournalIdentityProvider(store: store)
            let descriptor = Darwin.open(root.appendingPathComponent("bigsync-state.plist.lock").path, O_RDWR)
            XCTAssertGreaterThanOrEqual(descriptor, 0)
            guard descriptor >= 0 else { return }
            defer { Darwin.close(descriptor) }
            try admission.withCurrent {
                XCTAssertNotEqual(flock(descriptor, LOCK_EX | LOCK_NB), 0,
                    "A separately opened writer cannot replace binding state during COMMIT")
                XCTAssertEqual(provider()?.installationIdentifier, admission.installationIdentifier)
                XCTAssertEqual(provider()?.replicaBindingGenerationIdentifier, admission.replicaBindingGenerationIdentifier)
                XCTAssertEqual(identity.currentMutationJournalIdentity(storeURL: store.fileURL), provider())
                XCTAssertThrowsError(try admission.withCurrent {})
                XCTAssertThrowsError(try identity.withManualBackupRestore(transactionIdentifier: UUID()) {
                    XCTFail("Nested restore entered a held publication")
                })
            }
            XCTAssertEqual(flock(descriptor, LOCK_EX | LOCK_NB), 0)
            _ = flock(descriptor, LOCK_UN)
        }
    }

    func testBindingLifecycleRejectsPredecessorBeforeNotificationDeliveryAndStablePrepareDoesNotRestart() throws {
        try withResources { identity, store, _ in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount("a", store: store, key: key)
            let old = try identity.capturePublicationAdmission(store: store)
            let notification = expectation(description: "Changed binding notified after releasing its file guard")
            let observer = NotificationCenter.default.addObserver(forName: .BigSyncReplicaPublicationAuthorityDidChange,
                object: nil, queue: nil) { _ in
                do {
                    try old.withCurrent {}
                    XCTFail("Old publication admitted")
                } catch {
                    // Rejection is the expected predecessor outcome. The
                    // notification callback itself must remain nonthrowing.
                }
                notification.fulfill()
            }
            defer { NotificationCenter.default.removeObserver(observer) }
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            XCTAssertEqual(try old.withCurrent { 1 }, 1)
            let port = try BigSyncReplicaBindingStateStore.requirePort(sourceAccountScopeIdentifier: "a",
                destinationAccountScopeIdentifier: "b", store: store, key: key)
            wait(for: [notification], timeout: 1)
            NotificationCenter.default.removeObserver(observer)
            XCTAssertThrowsError(try identity.capturePublicationAdmission(store: store), "Pending port is not publication authority")
            _ = try BigSyncReplicaBindingStateStore.activatePort(port, store: store, key: key)
            let successor = try identity.capturePublicationAdmission(store: store)
            XCTAssertNotEqual(successor.replicaBindingGenerationIdentifier, old.replicaBindingGenerationIdentifier)
            XCTAssertEqual(try successor.withCurrent { 2 }, 2)
            XCTAssertThrowsError(try old.withCurrent {})
        }
    }

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
            XCTAssertFalse(try canAcquire(LOCK_EX, descriptor: competitor))
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
