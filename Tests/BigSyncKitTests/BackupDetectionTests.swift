import CloudKit
import Darwin
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(InstallationIdentityMutationObject)
private final class InstallationIdentityMutationObject: Object,
    ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
}

final class BackupDetectionTests: XCTestCase {
    private final class Store: NSObject, KeyValueStore {
        func object(forKey key: String) -> Any? { nil }
        func bool(forKey key: String) -> Bool { false }
        func set(value: Any?, forKey key: String) {}
        func set(boolValue: Bool, forKey key: String) {}
        func removeObject(forKey key: String) {}
        func synchronize() -> Bool { true }
    }

    private let store = Store()

    private func temporaryRoot() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
    }

    private func simulateRestoredMarker(from installed: URL, to restored: URL) throws {
        let source = BackupDetection.markerURL(sentinelURL: installed)
        let destination = BackupDetection.markerURL(sentinelURL: restored)
        try FileManager.default.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(at: source, to: destination)
    }

    func testFirstRunRegularLaunchAndRestoredBackup() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")

        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: installed),
            .firstRun
        )
        XCTAssertTrue(FileManager.default.fileExists(atPath: installed.path))
        XCTAssertTrue(FileManager.default.fileExists(
            atPath: BackupDetection.markerURL(sentinelURL: installed).path
        ))
        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: installed),
            .regularLaunch
        )

        try simulateRestoredMarker(from: installed, to: restored)
        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: restored),
            .restoredFromBackup
        )
        XCTAssertNotNil(BackupDetection.restoreResetEventIdentifier(sentinelURL: restored))
        XCTAssertTrue(FileManager.default.fileExists(atPath: restored.path))
    }

    func testInstallationIdentityIsStableUntilRestoreAndThenRotates() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: installed
        )
        let installedIdentifier = try XCTUnwrap(
            BackupDetection.installationIdentifier(sentinelURL: installed)
        )
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: installed
        )
        XCTAssertEqual(
            BackupDetection.installationIdentifier(sentinelURL: installed),
            installedIdentifier
        )

        try simulateRestoredMarker(from: installed, to: restored)
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: restored
        )
        let restoredIdentifier = try XCTUnwrap(
            BackupDetection.installationIdentifier(sentinelURL: restored)
        )
        XCTAssertNotEqual(restoredIdentifier, installedIdentifier)
    }

    func testManualRestorePublishesEventAndRotatesInstallationIdentity() throws {
        let namespace = "container.private.owner.zone"
        let base = temporaryRoot()
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sharedSentinelBaseURL: base
        )
        let oldIdentifier = try XCTUnwrap(
            BackupDetection.installationIdentifier(
                namespace: namespace,
                sharedSentinelBaseURL: base
            )
        )

        let newIdentifier = try BackupDetection.beginManualRestore(
            namespace: namespace,
            sharedSentinelBaseURL: base
        )

        XCTAssertNotEqual(newIdentifier, oldIdentifier)
        XCTAssertEqual(
            BackupDetection.installationIdentifier(
                namespace: namespace,
                sharedSentinelBaseURL: base
            ),
            newIdentifier
        )
        XCTAssertTrue(BackupDetection.restoreResetIsRequired(
            namespace: namespace,
            sharedSentinelBaseURL: base
        ))
    }

    func testManualRestoreDefersWhileAnotherClientHoldsSharedLease() throws {
        let namespace = "container.private.owner.zone"
        let base = temporaryRoot()
        let identity = BigSyncClientIdentity(
            synchronizerName: "lease-test",
            containerName: "container",
            recordZoneID: CKRecordZone.ID(
                zoneName: namespace,
                ownerName: CKCurrentUserDefaultName
            ),
            sharedStateBaseURL: base
        )
        _ = try identity.prepareInstallation()
        let leaseURL = BackupDetection.defaultSentinelURL(
            namespace: identity.durableStateNamespace,
            sharedBaseURL: base
        ).appendingPathExtension("lease")
        let peerDescriptor = Darwin.open(leaseURL.path, O_RDWR)
        XCTAssertGreaterThanOrEqual(peerDescriptor, 0)
        defer { Darwin.close(peerDescriptor) }
        XCTAssertEqual(flock(peerDescriptor, LOCK_SH), 0)

        var replacementRan = false
        XCTAssertThrowsError(try identity.withManualBackupRestore {
            replacementRan = true
        }) { error in
            XCTAssertEqual(
                error as? BigSyncClientIdentityLeaseError,
                .restoreInProgress
            )
        }
        XCTAssertFalse(replacementRan)

        XCTAssertEqual(flock(peerDescriptor, LOCK_UN), 0)
        let exclusiveProbeDescriptor = Darwin.open(leaseURL.path, O_RDWR)
        XCTAssertGreaterThanOrEqual(exclusiveProbeDescriptor, 0)
        XCTAssertEqual(
            flock(exclusiveProbeDescriptor, LOCK_EX | LOCK_NB),
            -1,
            "A failed upgrade must immediately restore this process's shared lease"
        )
        Darwin.close(exclusiveProbeDescriptor)

        let replacementInstallation = try identity.withManualBackupRestore {
            replacementRan = true
        }
        XCTAssertTrue(replacementRan)
        XCTAssertEqual(
            identity.currentInstallationIdentifier(),
            replacementInstallation
        )
    }

    func testFailedReplacementRollsBackBeforeExclusiveLeaseIsReleased() throws {
        struct ExpectedFailure: Error {}
        let base = temporaryRoot()
        let identity = BigSyncClientIdentity(
            synchronizerName: "rollback-lease-test",
            containerName: "container",
            recordZoneID: CKRecordZone.ID(
                zoneName: "zone",
                ownerName: CKCurrentUserDefaultName
            ),
            sharedStateBaseURL: base
        )
        _ = try identity.prepareInstallation()
        let originalInstallationIdentifier = try XCTUnwrap(
            identity.currentInstallationIdentifier()
        )
        let leaseURL = BackupDetection.defaultSentinelURL(
            namespace: identity.durableStateNamespace,
            sharedBaseURL: base
        ).appendingPathExtension("lease")
        var rollbackRanUnderExclusiveLease = false
        var rollbackCount = 0

        XCTAssertThrowsError(try identity.withManualBackupRestore({
            throw ExpectedFailure()
        }, rollback: {
            rollbackCount += 1
            let peerDescriptor = Darwin.open(leaseURL.path, O_RDWR)
            XCTAssertGreaterThanOrEqual(peerDescriptor, 0)
            defer { Darwin.close(peerDescriptor) }
            rollbackRanUnderExclusiveLease = flock(
                peerDescriptor,
                LOCK_SH | LOCK_NB
            ) == -1
        })) { error in
            XCTAssertTrue(error is ExpectedFailure)
        }

        XCTAssertTrue(rollbackRanUnderExclusiveLease)
        XCTAssertEqual(rollbackCount, 1)
        XCTAssertEqual(
            identity.currentInstallationIdentifier(),
            originalInstallationIdentifier
        )
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: identity.durableStateNamespace,
            sharedSentinelBaseURL: base
        ))
        let peerDescriptor = Darwin.open(leaseURL.path, O_RDWR)
        XCTAssertGreaterThanOrEqual(peerDescriptor, 0)
        defer { Darwin.close(peerDescriptor) }
        XCTAssertEqual(flock(peerDescriptor, LOCK_SH | LOCK_NB), 0)
    }

    func testRollbackFailurePropagatesWhileExclusiveLeaseIsHeld() throws {
        struct ReplacementFailure: Error {}
        struct RollbackFailure: Error {}
        let base = temporaryRoot()
        let identity = BigSyncClientIdentity(
            synchronizerName: "throwing-rollback-lease-test",
            containerName: "container",
            recordZoneID: CKRecordZone.ID(
                zoneName: "zone",
                ownerName: CKCurrentUserDefaultName
            ),
            sharedStateBaseURL: base
        )
        _ = try identity.prepareInstallation()
        let leaseURL = BackupDetection.defaultSentinelURL(
            namespace: identity.durableStateNamespace,
            sharedBaseURL: base
        ).appendingPathExtension("lease")
        var rollbackHeldExclusiveLease = false

        XCTAssertThrowsError(try identity.withManualBackupRestore({
            throw ReplacementFailure()
        }, rollback: {
            let peerDescriptor = Darwin.open(leaseURL.path, O_RDWR)
            XCTAssertGreaterThanOrEqual(peerDescriptor, 0)
            defer { Darwin.close(peerDescriptor) }
            rollbackHeldExclusiveLease = flock(
                peerDescriptor,
                LOCK_SH | LOCK_NB
            ) == -1
            throw RollbackFailure()
        })) { error in
            XCTAssertTrue(error is RollbackFailure)
        }

        XCTAssertTrue(rollbackHeldExclusiveLease)
        let peerDescriptor = Darwin.open(leaseURL.path, O_RDWR)
        XCTAssertGreaterThanOrEqual(peerDescriptor, 0)
        defer { Darwin.close(peerDescriptor) }
        XCTAssertEqual(flock(peerDescriptor, LOCK_SH | LOCK_NB), 0)
    }

    func testMutationGenerationObservesInstallationRotationAcrossProcesses()
    throws {
        let namespace = "container.private.owner.zone"
        let base = temporaryRoot()
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sharedSentinelBaseURL: base
        )
        var configuration = Realm.Configuration()
        configuration.inMemoryIdentifier = UUID().uuidString
        configuration.objectTypes = [
            InstallationIdentityMutationObject.self,
            BigSyncPendingMutation.self,
        ]
        BigSyncMutationTracking.install(
            configurations: [configuration],
            excludedClassNames: [],
            installationIdentifierProvider: {
                BackupDetection.installationIdentifier(
                    namespace: namespace,
                    sharedSentinelBaseURL: base
                )
            }
        )
        let realm = try Realm(configuration: configuration)
        let object = InstallationIdentityMutationObject()
        object.id = "object"
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let firstGeneration = try XCTUnwrap(
            realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: InstallationIdentityMutationObject.className()
                    + ".object"
            )?.generation
        )
        let firstInstallation = try XCTUnwrap(
            BackupDetection.installationIdentifier(
                namespace: namespace,
                sharedSentinelBaseURL: base
            )
        )
        XCTAssertTrue(BigSyncPendingMutation.wasCreatedInInstallation(
            firstGeneration,
            installationIdentifier: firstInstallation
        ))

        let secondInstallation = try BackupDetection.beginManualRestore(
            namespace: namespace,
            sharedSentinelBaseURL: base
        )
        try realm.write {
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let secondGeneration = try XCTUnwrap(
            realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: InstallationIdentityMutationObject.className()
                    + ".object"
            )?.generation
        )

        XCTAssertNotEqual(secondInstallation, firstInstallation)
        XCTAssertTrue(BigSyncPendingMutation.wasCreatedInInstallation(
            secondGeneration,
            installationIdentifier: secondInstallation
        ))
        XCTAssertFalse(BigSyncPendingMutation.wasCreatedInInstallation(
            secondGeneration,
            installationIdentifier: firstInstallation
        ))
    }

    func testFailedManualRestoreRestoresExcludedInstallationBeforeClearingEvent() throws {
        let namespace = "container.private.owner.zone"
        let base = temporaryRoot()
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sharedSentinelBaseURL: base
        )
        let sentinelURL = BackupDetection.defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: base
        )
        let oldIdentifier = try XCTUnwrap(
            BackupDetection.installationIdentifier(sentinelURL: sentinelURL)
        )

        XCTAssertThrowsError(try BackupDetection.beginManualRestore(
            namespace: namespace,
            sharedSentinelBaseURL: base,
            sentinelPublisher: { _, _ in
                throw CocoaError(.fileWriteUnknown)
            }
        ))

        XCTAssertEqual(
            BackupDetection.installationIdentifier(sentinelURL: sentinelURL),
            oldIdentifier
        )
        XCTAssertEqual(
            try sentinelURL.resourceValues(
                forKeys: [.isExcludedFromBackupKey]
            ).isExcludedFromBackup,
            true
        )
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: namespace,
            sharedSentinelBaseURL: base
        ))
        XCTAssertEqual(
            try BackupDetection.run(
                store: store,
                namespace: namespace,
                sharedSentinelBaseURL: base
            ),
            .regularLaunch
        )
    }

    func testRestoreEventPersistsUntilThatNamespaceAcknowledgesIt() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")
        _ = try BackupDetection.run(store: store, namespace: namespace, sentinelURL: installed)
        try simulateRestoredMarker(from: installed, to: restored)

        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: restored),
            .restoredFromBackup
        )
        let event = try XCTUnwrap(BackupDetection.restoreResetEventIdentifier(sentinelURL: restored))

        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: restored),
            .regularLaunch
        )
        XCTAssertEqual(BackupDetection.restoreResetEventIdentifier(sentinelURL: restored), event)

        try BackupDetection.markRestoreResetCompleted(sentinelURL: restored)
        XCTAssertNil(BackupDetection.restoreResetEventIdentifier(sentinelURL: restored))
    }

    func testASecondRestoreReplacesTheCopiedRecoveryEvent() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let firstRestore = temporaryRoot().appendingPathComponent("restored-1")
        let secondRestore = temporaryRoot().appendingPathComponent("restored-2")
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: installed
        )
        try simulateRestoredMarker(from: installed, to: firstRestore)
        XCTAssertEqual(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: firstRestore
        ), .restoredFromBackup)
        let firstEvent = try XCTUnwrap(
            BackupDetection.restoreResetEventIdentifier(
                sentinelURL: firstRestore
            )
        )

        let secondMarker = BackupDetection.markerURL(
            sentinelURL: secondRestore
        )
        try FileManager.default.createDirectory(
            at: secondMarker.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: firstRestore),
            to: secondMarker
        )
        try FileManager.default.copyItem(
            at: BackupDetection.restoreEventURL(sentinelURL: firstRestore),
            to: BackupDetection.restoreEventURL(sentinelURL: secondRestore)
        )

        XCTAssertEqual(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: secondRestore
        ), .restoredFromBackup)
        XCTAssertNotEqual(
            BackupDetection.restoreResetEventIdentifier(
                sentinelURL: secondRestore
            ),
            firstEvent
        )
    }

    func testFailedRestoreEventAcknowledgementRemainsRequired() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: installed
        )
        try simulateRestoredMarker(from: installed, to: restored)
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: restored
        )
        let event = try XCTUnwrap(
            BackupDetection.restoreResetEventIdentifier(sentinelURL: restored)
        )

        XCTAssertThrowsError(try BackupDetection.markRestoreResetCompleted(
            sentinelURL: restored,
            completionSynchronizer: { _ in throw CocoaError(.fileWriteUnknown) }
        )) { error in
            XCTAssertEqual(
                error as? BackupDetection.Error,
                .restoreEventAcknowledgementVerificationFailed
            )
        }
        XCTAssertEqual(
            BackupDetection.restoreResetEventIdentifier(sentinelURL: restored),
            event
        )
    }

    func testMalformedRestoreEventFailsClosedAsRecoveryRequired() throws {
        let namespace = "container.private.owner.zone"
        let base = temporaryRoot()
        let sentinel = BackupDetection.defaultSentinelURL(
            namespace: namespace,
            sharedBaseURL: base
        )
        let eventURL = BackupDetection.restoreEventURL(sentinelURL: sentinel)
        try FileManager.default.createDirectory(
            at: eventURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data([0xFF, 0x00]).write(to: eventURL, options: .atomic)

        XCTAssertNil(BackupDetection.restoreResetEventIdentifier(
            sentinelURL: sentinel
        ))
        XCTAssertTrue(BackupDetection.restoreResetIsRequired(
            namespace: namespace,
            sharedSentinelBaseURL: base
        ))
    }

    func testNamespacesSharingBaseRemainIndependent() throws {
        let sharedBase = temporaryRoot()
        let restoredBase = temporaryRoot()
        let first = "container.private.owner.first"
        let second = "container.private.owner.second"
        let firstInstalled = BackupDetection.defaultSentinelURL(namespace: first, sharedBaseURL: sharedBase)
        let firstRestored = BackupDetection.defaultSentinelURL(namespace: first, sharedBaseURL: restoredBase)

        _ = try BackupDetection.run(store: store, namespace: first, sharedSentinelBaseURL: sharedBase)
        try simulateRestoredMarker(from: firstInstalled, to: firstRestored)
        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: first, sharedSentinelBaseURL: restoredBase),
            .restoredFromBackup
        )
        try BackupDetection.markRestoreResetCompleted(
            namespace: first,
            sharedSentinelBaseURL: restoredBase
        )

        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: second, sharedSentinelBaseURL: restoredBase),
            .firstRun
        )
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: second,
            sharedSentinelBaseURL: restoredBase
        ))
    }

    func testLaterClientReceivesItsOwnRestoreEventAfterFirstClientAcknowledges() throws {
        let first = "container.private.owner.first"
        let second = "container.private.owner.second"
        let installedBase = temporaryRoot()
        let restoredBase = temporaryRoot()
        let firstInstalled = BackupDetection.defaultSentinelURL(namespace: first, sharedBaseURL: installedBase)
        let secondInstalled = BackupDetection.defaultSentinelURL(namespace: second, sharedBaseURL: installedBase)
        let firstRestored = BackupDetection.defaultSentinelURL(namespace: first, sharedBaseURL: restoredBase)
        let secondRestored = BackupDetection.defaultSentinelURL(namespace: second, sharedBaseURL: restoredBase)

        _ = try BackupDetection.run(store: store, namespace: first, sharedSentinelBaseURL: installedBase)
        _ = try BackupDetection.run(store: store, namespace: second, sharedSentinelBaseURL: installedBase)
        try simulateRestoredMarker(from: firstInstalled, to: firstRestored)
        try simulateRestoredMarker(from: secondInstalled, to: secondRestored)

        XCTAssertEqual(try BackupDetection.run(
            store: store, namespace: first, sharedSentinelBaseURL: restoredBase
        ), .restoredFromBackup)
        try BackupDetection.markRestoreResetCompleted(
            namespace: first,
            sharedSentinelBaseURL: restoredBase
        )

        XCTAssertEqual(try BackupDetection.run(
            store: store, namespace: second, sharedSentinelBaseURL: restoredBase
        ), .restoredFromBackup)
        XCTAssertNil(BackupDetection.restoreResetEventIdentifier(
            namespace: first, sharedSentinelBaseURL: restoredBase
        ))
        XCTAssertNotNil(BackupDetection.restoreResetEventIdentifier(
            namespace: second, sharedSentinelBaseURL: restoredBase
        ))
    }

    func testFailedRestoreEventPersistenceLeavesSentinelAbsent() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")
        _ = try BackupDetection.run(store: store, namespace: namespace, sentinelURL: installed)
        try simulateRestoredMarker(from: installed, to: restored)

        XCTAssertThrowsError(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: restored,
            eventWriter: { _, _ in }
        )) { error in
            XCTAssertEqual(error as? BackupDetection.Error, .restoreEventPersistenceVerificationFailed)
        }
        XCTAssertFalse(FileManager.default.fileExists(atPath: restored.path))
    }

    func testFailedMarkerWriteCannotCreateFalseRestoreState() throws {
        let namespace = "container.private.owner.zone"
        let sentinel = temporaryRoot().appendingPathComponent("installed")
        let marker = BackupDetection.markerURL(sentinelURL: sentinel)

        XCTAssertThrowsError(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: sentinel,
            markerWriter: { url, _ in
                try Data("partial marker".utf8).write(to: url, options: .atomic)
            }
        )) { error in
            XCTAssertEqual(error as? BackupDetection.Error, .markerPersistenceVerificationFailed)
        }
        XCTAssertTrue(FileManager.default.fileExists(atPath: sentinel.path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: marker.path))

        let restoredSentinel = temporaryRoot().appendingPathComponent("restored")
        XCTAssertEqual(
            try BackupDetection.run(store: store, namespace: namespace, sentinelURL: restoredSentinel),
            .firstRun
        )
        XCTAssertNil(BackupDetection.restoreResetEventIdentifier(sentinelURL: restoredSentinel))
    }

    func testMarkerIsPublishedOnlyAfterFinalSentinelIsBackupExcluded() throws {
        let namespace = "container.private.owner.zone"
        let sentinel = temporaryRoot().appendingPathComponent("installed")
        var markerObservedExcludedSentinel = false

        XCTAssertEqual(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: sentinel,
            markerWriter: { url, data in
                markerObservedExcludedSentinel = try sentinel.resourceValues(
                    forKeys: [.isExcludedFromBackupKey]
                ).isExcludedFromBackup == true
                try data.write(to: url, options: .atomic)
            }
        ), .firstRun)

        XCTAssertTrue(markerObservedExcludedSentinel)
        XCTAssertEqual(try sentinel.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup, true)
    }

    func testLegacyUnexcludedSentinelWithMarkerFailsClosedAsRestore() throws {
        let namespace = "container.private.owner.zone"
        let installed = temporaryRoot().appendingPathComponent("installed")
        let restored = temporaryRoot().appendingPathComponent("restored")
        _ = try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: installed
        )
        try simulateRestoredMarker(from: installed, to: restored)
        try Data("legacy unsafe sentinel".utf8).write(
            to: restored,
            options: .atomic
        )
        XCTAssertNotEqual(try restored.resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup, true)

        XCTAssertEqual(try BackupDetection.run(
            store: store,
            namespace: namespace,
            sentinelURL: restored
        ), .restoredFromBackup)
        XCTAssertNotNil(BackupDetection.restoreResetEventIdentifier(
            sentinelURL: restored
        ))
        XCTAssertEqual(try URL(fileURLWithPath: restored.path).resourceValues(
            forKeys: [.isExcludedFromBackupKey]
        ).isExcludedFromBackup, true)
    }
}
