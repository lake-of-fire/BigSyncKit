#if DEBUG
import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(BigSyncSplitOwnerRow)
private final class SplitOwnerRow: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var text = "local"
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
}

@objc(BigSyncSplitOwnerContractRow)
private final class SplitOwnerContractRow: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    static let bigSyncRecordContract = BigSyncRecordContract(policy: .independentFields,
        expectedFields: ["text", "isDeleted"])
    @Persisted(primaryKey: true) var id = "contract"
    @Persisted var text = "local"
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
}

@objc(BigSyncSplitOwnerChild)
private final class SplitOwnerChild: Object {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "child"
}

@objc(BigSyncSplitOwnerParent)
private final class SplitOwnerParent: Object, ChangeMetadataRecordable {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "parent"
    @Persisted var children: List<SplitOwnerChild>
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
}


@objc(BigSyncSplitOwnerInvalidEcho)
private final class SplitOwnerInvalidEcho: Object, ChangeMetadataRecordable,
    BigSyncInboundSemanticRecordValidating {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "invalid-echo"
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?

    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        throw CocoaError(.coderInvalidValue)
    }
}

@objc(BigSyncSplitOwnerSemanticEcho)
private final class SplitOwnerSemanticEcho: Object, ChangeMetadataRecordable,
    BigSyncInboundSemanticReplacementValidating {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "semantic-echo"
    @Persisted var text = "committed predecessor"
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?

    static func validateInboundSemanticReplacement(
        _ record: CKRecord, existingObject: Object?
    ) throws {
        guard let existing = existingObject as? Self,
              record["text"] as? String == existing.text else {
            throw CocoaError(.coderInvalidValue)
        }
    }
}

// The semaphore joins the native writer before the refresh callback continues.
// No Realm handle or managed value leaves its owning thread.
private final class SplitRefreshWriterResult: @unchecked Sendable {
    var result: Result<String, Error>?
}

@BigSyncBackgroundActor
private final class SplitRefreshCapture {
    var stagedGeneration: String?
    var error: Error?
    var refreshRetirementCount = 0
    var sawOwningWrite = false
    var forwardedCount = 0
}

/// Runs real target/tracking commits through the cancellation-generation ABA:
/// a successor makes cancelSync false but cannot authorize the old continuation.
@BigSyncBackgroundActor
private final class SplitForwardDelegate: ModelAdapterDelegate {
    private(set) var uploadWakeupCount = 0
    func needsInitialSetup() async throws {}
    func hasChangesToUpload() async { uploadWakeupCount += 1 }
}

final class SyncSplitOperationOwnershipTests: XCTestCase {
    @BigSyncBackgroundActor
    private lazy var fixtureOwner = RealmAdapterFixtureOwner(testCase: self)

    @BigSyncBackgroundActor
    private func fixture(
        comparison: Bool = false, contract: Bool = false,
        defersSetup: Bool = false, initializeThroughEcho: Bool = false
    ) async throws
        -> (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm) {
        let nonce = UUID().uuidString
        var config = Realm.Configuration()
        config.fileURL = nil
        config.inMemoryIdentifier = "split-owner-target-" + nonce
        config.objectTypes = [SplitOwnerRow.self, SplitOwnerChild.self,
                              SplitOwnerParent.self, SplitOwnerInvalidEcho.self,
                              SplitOwnerSemanticEcho.self,
                              BigSyncPendingMutation.self]
        if contract { config.objectTypes?.append(SplitOwnerContractRow.self) }
        if comparison || contract { BigSyncMutationPolicy.enableRecordRebasing(in: &config) }
        BigSyncMutationPolicy(excludedClassNames: [SplitOwnerChild.className()])
            .install(configurations: [config], mutationJournalIdentityProvider: {
                .init(installationIdentifier: "split-owner-installation",
                      replicaBindingGenerationIdentifier: "split-owner-binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = nil
        tracking.inMemoryIdentifier = "split-owner-tracking-" + nonce
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("split-owner-assets-" + nonce)
        fixtureOwner.ownDirectory(directory)
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [config], excludedClassNames: [SplitOwnerChild.className()],
            recordZoneID: .init(zoneName: "split-owner-zone"),
            logger: Logger(label: "SplitOperationOwnershipTests"),
            startSetupTask: false, assetDirectoryURL: directory)
        fixtureOwner.own(adapter)
        adapter.forceDataTypeInsteadOfAsset = true
        if !defersSetup && !initializeThroughEcho {
            try await adapter.resetSyncCaches()
            adapter.invalidateTokens()
        }
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "split-owner-account",
            replicaBindingGenerationIdentifier: "split-owner-binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.split-owner",
            databaseScope: .private)
        if initializeThroughEcho {
            let outcomes = try await adapter.validateAuthoritativeOwnUploadRecords([])
            XCTAssertTrue(outcomes.isEmpty)
        }
        if defersSetup && !initializeThroughEcho {
            return (adapter,
                try await Realm(configuration: config, actor: BigSyncBackgroundActor.shared),
                try await Realm(configuration: tracking, actor: BigSyncBackgroundActor.shared))
        }
        return (adapter,
                try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first),
                try XCTUnwrap(adapter.realmProvider?.persistenceRealm))
    }

    @BigSyncBackgroundActor
    private func pendingRow(_ fixture: (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm))
        async throws -> (SplitOwnerRow, String, String) {
        let row = SplitOwnerRow()
        try fixture.target.write {
            fixture.target.add(row)
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let name = SplitOwnerRow.className() + ".row"
        let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        return (row, name, generation)
    }

    @BigSyncBackgroundActor
    private func pendingInboundDelivery(in tracking: Realm) throws
        -> (delivery: BigSyncPendingInboundIdentityDelivery, identity: CommittedInboundIdentity) {
        let identity = CommittedInboundIdentity(entityType: SplitOwnerRow.className(),
            recordName: SplitOwnerRow.className() + ".row", disposition: .upsert)
        let delivery = BigSyncPendingInboundIdentityDelivery()
        delivery.deliveryID = "committed-delivery"
        delivery.encodedIdentityPageBatches.append(try JSONEncoder().encode([identity]))
        try tracking.write { tracking.add(delivery) }
        return (delivery, identity)
    }

    @BigSyncBackgroundActor
    func testInboundIdentityInspectionIgnoresProvisionalReplacementAndRemoval() async throws {
        for removesDelivery in [false, true] {
            for commits in [false, true] {
                let fixture = try await fixture()
                let (delivery, identity) = try pendingInboundDelivery(in: fixture.tracking)
                let successor = CommittedInboundIdentity(entityType: SplitOwnerRow.className(),
                    recordName: SplitOwnerRow.className() + ".successor", disposition: .delete)
                fixture.tracking.beginWrite()
                defer { if fixture.tracking.isInWriteTransaction { fixture.tracking.cancelWrite() } }
                if removesDelivery {
                    fixture.tracking.delete(delivery)
                } else {
                    delivery.deliveryID = "provisional-successor"
                    delivery.encodedIdentityPageBatches.removeAll()
                    delivery.encodedIdentityPageBatches.append(try JSONEncoder().encode([successor]))
                }
                let batch = try XCTUnwrap(fixture.adapter.pendingCommittedInboundIdentityBatch())
                XCTAssertEqual(batch.deliveryID, "committed-delivery")
                XCTAssertEqual(batch.identities, [identity])
                XCTAssertTrue(fixture.tracking.isInWriteTransaction,
                    "Inspection must leave the independent writer's transaction open")

                if commits { try fixture.tracking.commitWrite() }
                else { fixture.tracking.cancelWrite() }
                let settled = try fixture.adapter.pendingCommittedInboundIdentityBatch()
                if !commits {
                    XCTAssertEqual(settled?.deliveryID, batch.deliveryID)
                    XCTAssertEqual(settled?.identities, [identity])
                } else if removesDelivery {
                    XCTAssertNil(settled)
                } else {
                    XCTAssertEqual(settled?.deliveryID, "provisional-successor")
                    XCTAssertEqual(settled?.identities, [successor])
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testInboundIdentityAcknowledgementRejectsAccountAndTransportReplacement() async throws {
        for replacesTransport in [false, true] {
            let fixture = try await fixture()
            let (delivery, identity) = try pendingInboundDelivery(in: fixture.tracking)
            let deliveryID = delivery.deliveryID
            fixture.adapter._testBeforeInboundIdentityAcknowledgementTrackingWrite = {
                if replacesTransport {
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.inbound-successor", databaseScope: .public
                    )
                } else {
                    try await fixture.adapter.activateAccountScope("inbound-successor-account")
                }
            }
            defer { fixture.adapter._testBeforeInboundIdentityAcknowledgementTrackingWrite = nil }
            do {
                try await fixture.adapter.acknowledgeCommittedInboundIdentityBatch(deliveryID: deliveryID)
                XCTFail("The prior domain callback consumed repair input after ownership replacement")
            } catch is CancellationError { }
            let pending = try XCTUnwrap(fixture.adapter.pendingCommittedInboundIdentityBatch())
            XCTAssertEqual(pending.deliveryID, deliveryID)
            XCTAssertEqual(pending.identities, [identity])

            fixture.adapter._testBeforeInboundIdentityAcknowledgementTrackingWrite = nil
            try await fixture.adapter.acknowledgeCommittedInboundIdentityBatch(deliveryID: deliveryID)
            XCTAssertNil(try fixture.adapter.pendingCommittedInboundIdentityBatch())
        }
    }

    @BigSyncBackgroundActor
    func testPublicImportRejectsCancellationResetFromProgressBeforeForwarding() async throws {
        for checkpoint in ["adapter-import-setup-started", "adapter-import-forwarding-started"] {
            let fixture = try await fixture()
            let row = SplitOwnerRow()
            try fixture.target.write {
                fixture.target.add(row)
                row.refreshChangeMetadata(explicitlyModified: true)
            }
            let name = SplitOwnerRow.className() + ".row"
            let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation)
            do {
                try await fixture.adapter.didFinishImport { current in
                    guard current == checkpoint else { return }
                    fixture.adapter.cancelSynchronization()
                    do { try fixture.adapter.prepareForFencedMigrationAfterCancellation() }
                    catch { XCTFail("Unable to prepare the successor: \(error)") }
                }
                XCTFail("The public import adopted ownership replaced by its progress callback")
            } catch is CancellationError { }
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.didFinishImport()
            XCTAssertEqual(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: name)?.pendingGeneration, generation)
        }
    }

    @BigSyncBackgroundActor
    func testPublicImportMayInitializeItsProviderWithoutReplacingOperationOwnership() async throws {
        let fixture = try await fixture(defersSetup: true)
        XCTAssertNil(fixture.adapter.realmProvider)
        let row = SplitOwnerRow()
        try fixture.target.write {
            fixture.target.add(row)
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        let name = SplitOwnerRow.className() + ".row"
        let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        try await fixture.adapter.didFinishImport()
        let tracking = try XCTUnwrap(fixture.adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.pendingGeneration, generation)
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testJournalForwardingRejectsCancellationResetBeforeTrackingAdmission() async throws {
        let fixture = try await fixture()
        let row = SplitOwnerRow()
        try fixture.target.write {
            fixture.target.add(row)
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        let name = SplitOwnerRow.className() + ".row"
        let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        fixture.adapter._testBeforePendingMutationTrackingWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testBeforePendingMutationTrackingWrite = nil }
        do {
            _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
            XCTFail("The retired journal forwarder published tracking for a successor")
        } catch is CancellationError { }
        XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)

        fixture.adapter._testBeforePendingMutationTrackingWrite = nil
        try await fixture.adapter.unsetCancellation()
        _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
        XCTAssertEqual(fixture.tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.pendingGeneration, generation)
        let retry = try await fixture.adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(retry.records.first?["text"] as? String, row.text)
        try await fixture.adapter.acknowledgeUploadedRecords(retry.records, from: retry)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testJournalForwardingRejectsAccountAndTransportReplacementBeforeTrackingAdmission() async throws {
        for replacesTransport in [false, true] {
            let fixture = try await fixture()
            let row = SplitOwnerRow()
            try fixture.target.write {
                fixture.target.add(row)
                row.refreshChangeMetadata(explicitlyModified: true)
            }
            let name = SplitOwnerRow.className() + ".row"
            let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation)
            fixture.adapter._testBeforePendingMutationTrackingWrite = {
                if replacesTransport {
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.journal-successor", databaseScope: .public
                    )
                } else {
                    try await fixture.adapter.activateAccountScope("journal-successor-account")
                }
            }
            defer { fixture.adapter._testBeforePendingMutationTrackingWrite = nil }
            do {
                _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
                XCTFail("The old journal forwarder crossed the active operation's identity")
            } catch is CancellationError { }
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
        }
    }

    @BigSyncBackgroundActor
    func testCancelledJournalForwardingCannotPublishToSuccessorTracking() async throws {
        for revokesAfterTrackingCommit in [false, true] {
            let fixture = try await fixture()
            let delegate = SplitForwardDelegate()
            fixture.adapter.modelAdapterDelegate = delegate
            let row = SplitOwnerRow()
            try fixture.target.write {
                fixture.target.add(row)
                row.refreshChangeMetadata(explicitlyModified: true)
            }
            let name = SplitOwnerRow.className() + ".row"
            let generation = try XCTUnwrap(fixture.target.object(
                ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
            let revoke: @BigSyncBackgroundActor @Sendable () async throws -> Void = {
                fixture.adapter.cancelSynchronization()
                try fixture.adapter.prepareForFencedMigrationAfterCancellation()
            }
            if revokesAfterTrackingCommit {
                fixture.adapter._testAfterPendingMutationTrackingWrite = revoke
            } else {
                fixture.adapter._testBeforePendingMutationTrackingWrite = revoke
            }
            defer {
                fixture.adapter._testBeforePendingMutationTrackingWrite = nil
                fixture.adapter._testAfterPendingMutationTrackingWrite = nil
            }
            do {
                _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
                XCTFail("The obsolete journal drain published completion to its successor")
            } catch is CancellationError {}
            XCTAssertEqual(delegate.uploadWakeupCount, 0,
                "An obsolete drain cannot wake the successor's synchronizer")
            XCTAssertEqual(fixture.adapter.hasChangesCount, 0,
                "The revoked drain cannot publish a remaining-count update")
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
            let tracked = fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)
            if revokesAfterTrackingCommit {
                XCTAssertEqual(tracked?.pendingGeneration, generation,
                    "A committed first phase stays recoverable after owner revocation")
            } else {
                XCTAssertNil(tracked, "The revoked owner cannot begin the tracking phase")
            }
            fixture.adapter._testBeforePendingMutationTrackingWrite = nil
            fixture.adapter._testAfterPendingMutationTrackingWrite = nil
            try await fixture.adapter.unsetCancellation()
            _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
            let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(batch.records.map { $0.recordID.recordName }, [name])
            try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name), "The successor drains the same durable generation")
        }
    }

    @BigSyncBackgroundActor
    func testImportProgressCannotReacquireSuccessorJournalOwnership() async throws {
        for checkpoint in ["adapter-import-setup-started",
                           "adapter-import-setup-completed",
                           "adapter-import-forwarding-started",
                           "adapter-import-target-0-started",
                           "adapter-import-journal-snapshot-started",
                           "adapter-import-journal-snapshot-completed",
                           "adapter-import-journal-page-tracking-started",
                           "adapter-import-delegate-started"] {
            let fixture = try await fixture()
            let delegate = SplitForwardDelegate()
            fixture.adapter.modelAdapterDelegate = delegate
            let row = SplitOwnerRow()
            try fixture.target.write {
                fixture.target.add(row)
                row.refreshChangeMetadata(explicitlyModified: true)
            }
            let name = SplitOwnerRow.className() + ".row"
            let generation = try XCTUnwrap(fixture.target.object(
                ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
            do {
                try await fixture.adapter.didFinishImport(progress: { step in
                    guard step == checkpoint else { return }
                    fixture.adapter.cancelSynchronization()
                    do { try fixture.adapter.prepareForFencedMigrationAfterCancellation() }
                    catch { XCTFail("Failed to admit test successor: \(error)") }
                })
                XCTFail("A progress callout let the old import acquire its successor's owner")
            } catch is CancellationError {}
            XCTAssertEqual(delegate.uploadWakeupCount, 0)
            XCTAssertEqual(fixture.adapter.hasChangesCount, 0)
            let tracked = fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)
            if checkpoint == "adapter-import-delegate-started" {
                XCTAssertEqual(tracked?.pendingGeneration, generation,
                    "The already committed first phase remains retryable")
            } else {
                XCTAssertNil(tracked, "The progress callback revoked tracking admission")
            }
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.didFinishImport()
            let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(batch.records.map { $0.recordID.recordName }, [name])
        }
    }

    @BigSyncBackgroundActor
    func testCancelledImportCannotClearSuccessorAssetsAfterProgressCallout() async throws {
        for checkpoint in ["adapter-import-forwarding-completed",
                           "adapter-import-quarantine-started",
                           "adapter-import-quarantine-completed",
                           "adapter-import-assets-started"] {
            let fixture = try await fixture()
            _ = try await pendingRow(fixture)
            let delegate = SplitForwardDelegate()
            fixture.adapter.modelAdapterDelegate = delegate
            let asset = try fixture.adapter.persistentAssetManager.store(
                data: Data("successor upload materialization".utf8),
                forRecordID: "successor", propertyName: "text")
            do {
                try await fixture.adapter.didFinishImport(progress: { step in
                    guard step == checkpoint else { return }
                    fixture.adapter.cancelSynchronization()
                    do { try fixture.adapter.prepareForFencedMigrationAfterCancellation() }
                    catch { XCTFail("Failed to admit test successor: \(error)") }
                })
                XCTFail("The old import cleared assets after progress revoked its owner")
            } catch is CancellationError {}
            XCTAssertTrue(FileManager.default.fileExists(atPath: asset.path))
            XCTAssertEqual(delegate.uploadWakeupCount, 0)
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.didFinishImport()
            XCTAssertFalse(FileManager.default.fileExists(atPath: asset.path),
                "Only the resumed owner performs terminal asset cleanup")
        }
    }

    @BigSyncBackgroundActor
    func testCancelledQueuedRemainingCountDoesNotNotifySuccessor() async throws {
        let fixture = try await fixture()
        let name = SplitOwnerRow.className() + ".row"
        try fixture.tracking.write {
            let entity = SyncedEntity(entityType: SplitOwnerRow.className(),
                identifier: name, state: SyncedEntityState.new.rawValue)
            entity.setPendingMutation(generation: "queued-status-generation",
                replicaBindingGenerationIdentifier: "split-owner-binding")
            fixture.tracking.add(entity)
        }
        let staleStatus = expectation(description: "An obsolete queued status was delivered")
        staleStatus.isInverted = true
        let successorStatus = expectation(description: "The successor's remaining count was delivered")
        let observer = NotificationCenter.default.addObserver(
            forName: .SynchronizerChangesRemainingToUpload, object: nil, queue: nil
        ) { notification in
            switch notification.userInfo?["CloudKitSynchronizerChangesRemainingToUploadKey"] as? Int {
            case 1: staleStatus.fulfill()
            case 2: successorStatus.fulfill()
            default: break
            }
        }
        defer { NotificationCenter.default.removeObserver(observer) }
        fixture.adapter.updateHasChanges(realm: fixture.tracking)
        XCTAssertEqual(fixture.adapter.hasChangesCount, 1)
        fixture.adapter.cancelSynchronization()
        try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        try fixture.tracking.write {
            let entity = SyncedEntity(entityType: SplitOwnerRow.className(),
                identifier: SplitOwnerRow.className() + ".successor",
                state: SyncedEntityState.new.rawValue)
            entity.setPendingMutation(generation: "successor-status-generation",
                replicaBindingGenerationIdentifier: "split-owner-binding")
            fixture.tracking.add(entity)
        }
        // There is no actor suspension between queuing the obsolete count,
        // revoking its generation and queuing the successor's current count.
        // Delivering two proves the queue ran rather than hiding both events.
        fixture.adapter.updateHasChanges(realm: fixture.tracking)
        XCTAssertEqual(fixture.adapter.hasChangesCount, 2)
        await fulfillment(of: [successorStatus, staleStatus], timeout: 1)
    }

    @BigSyncBackgroundActor
    func testJournalForwardingRejectsTransportReplacementBeforeTrackingAdmission() async throws {
        let fixture = try await fixture()
        let row = SplitOwnerRow()
        try fixture.target.write {
            fixture.target.add(row)
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        let name = SplitOwnerRow.className() + ".row"
        let generation = try XCTUnwrap(fixture.target.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
        fixture.adapter._testBeforePendingMutationTrackingWrite = {
            try await fixture.adapter.activateTransportNamespace(
                containerIdentifier: "iCloud.test.forwarding-successor", databaseScope: .public)
        }
        defer { fixture.adapter._testBeforePendingMutationTrackingWrite = nil }
        do {
            _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
            XCTFail("The old drain copied work into a replacement transport namespace")
        } catch is CancellationError {}
        XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
        fixture.adapter._testBeforePendingMutationTrackingWrite = nil
        _ = try await fixture.adapter._test_forwardPendingMutations(in: fixture.target)
        XCTAssertEqual(fixture.tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.pendingGeneration, generation)
    }

    @BigSyncBackgroundActor
    func testCancelledRelationshipCleanupRetainsCommittedIntentForSuccessor() async throws {
        let fixture = try await fixture()
        let parent = SplitOwnerParent(), child = SplitOwnerChild()
        try fixture.target.write { fixture.target.add([parent, child]) }
        let name = SplitOwnerParent.className() + ".parent"
        try fixture.tracking.write {
            let entity = SyncedEntity(entityType: SplitOwnerParent.className(),
                identifier: name, state: SyncedEntityState.synced.rawValue)
            fixture.tracking.add(entity)
            let edge = PendingRelationship()
            edge.forSyncedEntity = entity
            edge.relationshipName = "children"
            edge.targetIdentifier = child.id
            edge.expectedModifiedAt = parent.modifiedAt
            edge.expectedExplicitlyModifiedAt = parent.explicitlyModifiedAt
            fixture.tracking.add(edge)
        }
        fixture.adapter._testAfterPendingRelationshipTargetWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testAfterPendingRelationshipTargetWrite = nil }
        do {
            try await fixture.adapter.persistImportedChanges()
            XCTFail("The obsolete import continued its tracking cleanup")
        } catch is CancellationError {}
        XCTAssertEqual(parent.children.map(\.id), [child.id], "The first target phase committed")
        XCTAssertEqual(fixture.tracking.objects(PendingRelationship.self).count, 1)
        fixture.adapter._testAfterPendingRelationshipTargetWrite = nil
        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter.persistImportedChanges()
        XCTAssertEqual(parent.children.map(\.id), [child.id])
        XCTAssertTrue(fixture.tracking.objects(PendingRelationship.self).isEmpty)
    }

    @BigSyncBackgroundActor
    private func changedLegacyUpload(
        _ fixture: (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm)
    ) async throws -> (prepared: [PreparedRecordUpload], generation: String, encodedRecord: Data) {
        let (row, name, _) = try await pendingRow(fixture)
        let initial = try await fixture.adapter.prepareUploadBatch(limit: 10)
        try await fixture.adapter.acknowledgeUploadedRecords(initial.records, from: initial)
        try fixture.target.write {
            row.text = "changed after accepted upload"
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let prepared = try await fixture.adapter.preparedRecordsToUpload(
            limit: 10, restrictedToEntityType: nil
        )
        XCTAssertEqual(prepared.count, 1)
        let generation = try XCTUnwrap(prepared.first?.generation)
        let entity = try XCTUnwrap(fixture.tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: name
        ))
        XCTAssertEqual(entity.entityState, .changed)
        return (prepared, generation, try XCTUnwrap(entity.encodedRecord))
    }

    @BigSyncBackgroundActor
    func testMissingServerRetryRejectsCancellationResetBeforeTrackingAdmission() async throws {
        for usesPreparedEnvelope in [false, true] {
            let fixture = try await fixture()
            let upload = try await changedLegacyUpload(fixture)
            let recordID = try XCTUnwrap(upload.prepared.first?.record.recordID)
            let entity = try XCTUnwrap(fixture.tracking.object(
                ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName
            ))
            fixture.adapter._testBeforeMissingServerTrackingWrite = {
                fixture.adapter.cancelSynchronization()
                try fixture.adapter.prepareForFencedMigrationAfterCancellation()
            }
            defer { fixture.adapter._testBeforeMissingServerTrackingWrite = nil }
            do {
                if usesPreparedEnvelope {
                    try await fixture.adapter.requeueMissingServerRecords(
                        [recordID], matchingPreparedUploads: upload.prepared
                    )
                } else {
                    try await fixture.adapter.requeueMissingServerRecords(
                        [recordID], matchingPreparedGenerations: [recordID.recordName: upload.generation]
                    )
                }
                XCTFail("The old missing-record response reset a successor's tracking value")
            } catch is CancellationError { }
            XCTAssertEqual(entity.entityState, .changed)
            XCTAssertEqual(entity.encodedRecord, upload.encodedRecord)
            XCTAssertEqual(entity.pendingGeneration, upload.generation)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordID.recordName)?.generation, upload.generation)

            fixture.adapter._testBeforeMissingServerTrackingWrite = nil
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.requeueMissingServerRecords(
                [recordID], matchingPreparedUploads: upload.prepared
            )
            XCTAssertEqual(entity.entityState, .new)
            XCTAssertNil(entity.encodedRecord)
            XCTAssertEqual(entity.pendingGeneration, upload.generation)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordID.recordName)?.generation, upload.generation)
        }
    }

    @BigSyncBackgroundActor
    func testMissingServerRetryRejectsAccountAndTransportReplacementBeforeTrackingAdmission() async throws {
        for replacesTransport in [false, true] {
            let fixture = try await fixture()
            let upload = try await changedLegacyUpload(fixture)
            let recordID = try XCTUnwrap(upload.prepared.first?.record.recordID)
            let entity = try XCTUnwrap(fixture.tracking.object(
                ofType: SyncedEntity.self, forPrimaryKey: recordID.recordName
            ))
            fixture.adapter._testBeforeMissingServerTrackingWrite = {
                if replacesTransport {
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.missing-response-successor", databaseScope: .public
                    )
                } else {
                    try await fixture.adapter.activateAccountScope("missing-response-successor-account")
                }
            }
            defer { fixture.adapter._testBeforeMissingServerTrackingWrite = nil }
            do {
                try await fixture.adapter.requeueMissingServerRecords(
                    [recordID], matchingPreparedUploads: upload.prepared
                )
                XCTFail("The old missing-record response crossed the active operation's identity")
            } catch is CancellationError { }
            XCTAssertEqual(entity.entityState, .changed)
            XCTAssertEqual(entity.encodedRecord, upload.encodedRecord)
            XCTAssertEqual(entity.pendingGeneration, upload.generation)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordID.recordName)?.generation, upload.generation)
        }
    }

    @BigSyncBackgroundActor
    func testRelationshipCleanupRejectsAccountBindingAndTransportReplacement() async throws {
        for replacement in 0..<3 {
            let fixture = try await fixture()
            let parent = SplitOwnerParent(), child = SplitOwnerChild()
            try fixture.target.write { fixture.target.add([parent, child]) }
            let name = SplitOwnerParent.className() + ".parent"
            try fixture.tracking.write {
                let entity = SyncedEntity(entityType: SplitOwnerParent.className(),
                    identifier: name, state: SyncedEntityState.synced.rawValue)
                fixture.tracking.add(entity)
                let edge = PendingRelationship()
                edge.forSyncedEntity = entity
                edge.relationshipName = "children"
                edge.targetIdentifier = child.id
                edge.expectedModifiedAt = parent.modifiedAt
                edge.expectedExplicitlyModifiedAt = parent.explicitlyModifiedAt
                fixture.tracking.add(edge)
            }
            fixture.adapter._testAfterPendingRelationshipTargetWrite = {
                switch replacement {
                case 0:
                    try await fixture.adapter.activateAccountScope("replacement-account")
                case 1:
                    try await fixture.adapter.activateReplicaBinding(
                        accountScopeIdentifier: "split-owner-account",
                        replicaBindingGenerationIdentifier: "replacement-binding")
                default:
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.replacement", databaseScope: .public)
                }
            }
            defer { fixture.adapter._testAfterPendingRelationshipTargetWrite = nil }
            do {
                try await fixture.adapter.persistImportedChanges()
                XCTFail("The previous identity continued deferred tracking cleanup")
            } catch is CancellationError {}
            XCTAssertEqual(fixture.tracking.objects(PendingRelationship.self).count, 1)
            XCTAssertEqual(parent.children.map(\.id), [child.id])
        }
    }

    @BigSyncBackgroundActor
    func testCancelledComparisonAcknowledgementRetainsTrackingAndJournalAfterBaseCommit() async throws {
        let fixture = try await fixture(comparison: true)
        let (_, name, generation) = try await pendingRow(fixture)
        let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        fixture.adapter._testAfterUploadComparisonWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testAfterUploadComparisonWrite = nil }
        do {
            try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
            XCTFail("The obsolete receipt continued after its target base committed")
        } catch is CancellationError {}
        XCTAssertNotNil(fixture.target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name))
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
        XCTAssertEqual(fixture.tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.pendingGeneration, generation)
        fixture.adapter._testAfterUploadComparisonWrite = nil
        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testUploadCandidateStagingRejectsIdentityCallbackCancellationReset() async throws {
        let fixture = try await fixture(contract: true)
        let row = SplitOwnerContractRow()
        try fixture.target.write {
            fixture.target.add(row)
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let name = SplitOwnerContractRow.className() + ".contract"
        let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        fixture.adapter._testAfterUploadPreparationIdentityValidation = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testAfterUploadPreparationIdentityValidation = nil }
        do {
            _ = try await fixture.adapter.prepareUploadBatch(limit: 10)
            XCTFail("The cancelled identity callback committed an obsolete staged candidate")
        } catch is CancellationError {}
        XCTAssertTrue(fixture.target.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
        fixture.adapter._testAfterUploadPreparationIdentityValidation = nil
        try await fixture.adapter.unsetCancellation()
        let retry = try await fixture.adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(retry.records.count, 1)
        XCTAssertEqual(fixture.target.objects(BigSyncRecordSubmission.self).count, 1)
        try await fixture.adapter.acknowledgeUploadedRecords(retry.records, from: retry)
        XCTAssertTrue(fixture.target.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testCancelledUploadJournalCleanupRetainsSameAndNewerGenerations() async throws {
        for editsDuringHandoff in [false, true] {
            let fixture = try await fixture()
            let (row, name, generation) = try await pendingRow(fixture)
            let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
            fixture.adapter._testAfterUploadTrackingWrite = {
                fixture.adapter.cancelSynchronization()
                try fixture.adapter.prepareForFencedMigrationAfterCancellation()
                if editsDuringHandoff {
                    try fixture.target.write {
                        row.text = "successor edit"
                        row.refreshChangeMetadata(explicitlyModified: true)
                    }
                }
            }
            defer { fixture.adapter._testAfterUploadTrackingWrite = nil }
            do {
                try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
                XCTFail("The obsolete upload continued its target journal cleanup")
            } catch is CancellationError {}
            let pending = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name))
            if editsDuringHandoff { XCTAssertNotEqual(pending.generation, generation) }
            else { XCTAssertEqual(pending.generation, generation) }
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: name)?.pendingGeneration, "The first tracking phase committed")
            fixture.adapter._testAfterUploadTrackingWrite = nil
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.didFinishImport()
            let retry = try await fixture.adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(retry.records.first?["text"] as? String, row.text)
            try await fixture.adapter.acknowledgeUploadedRecords(retry.records, from: retry)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        }
    }

    @BigSyncBackgroundActor
    private func syncedDeletionCandidate(
        _ fixture: (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm)
    ) async throws -> (row: SplitOwnerRow, recordID: CKRecord.ID, encodedRecord: Data) {
        let (row, name, _) = try await pendingRow(fixture)
        let batch = try await fixture.adapter.prepareUploadBatch(limit: 0)
        try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        let entity = try XCTUnwrap(fixture.tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: name
        ))
        XCTAssertEqual(entity.entityState, .synced)
        XCTAssertNil(entity.pendingGeneration)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name))
        return (row, try XCTUnwrap(batch.records.first?.recordID),
                try XCTUnwrap(entity.encodedRecord))
    }

    @BigSyncBackgroundActor
    private static func replaceInboundOwner(
        _ adapter: RealmSwiftAdapter, schedule: Int
    ) async throws {
        switch schedule {
        case 0:
            adapter.cancelSynchronization()
            try adapter.prepareForFencedMigrationAfterCancellation()
        case 1:
            try await adapter.activateAccountScope("replacement-account")
        case 2:
            try await adapter.activateReplicaBinding(
                accountScopeIdentifier: "split-owner-account",
                replicaBindingGenerationIdentifier: "replacement-binding"
            )
        default:
            try await adapter.activateTransportNamespace(
                containerIdentifier: "iCloud.test.replacement", databaseScope: .public
            )
        }
    }

    @BigSyncBackgroundActor
    private static func restoreInboundOwner(_ adapter: RealmSwiftAdapter) async throws {
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "split-owner-account",
            replicaBindingGenerationIdentifier: "split-owner-binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.split-owner", databaseScope: .private
        )
        try await adapter.unsetCancellation()
    }

    @BigSyncBackgroundActor
    private func inboundReplacement(
        for candidate: (row: SplitOwnerRow, recordID: CKRecord.ID, encodedRecord: Data)
    ) -> CKRecord {
        let record = CKRecord(recordType: SplitOwnerRow.className(), recordID: candidate.recordID)
        record["text"] = "incoming" as NSString
        record["isDeleted"] = false as NSNumber
        record["createdAt"] = candidate.row.createdAt as NSDate
        let remoteDate = candidate.row.modifiedAt.addingTimeInterval(60)
        record["modifiedAt"] = remoteDate as NSDate
        record["explicitlyModifiedAt"] = remoteDate as NSDate
        let setter = NSSelectorFromString("setRecordChangeTag:")
        guard record.responds(to: setter) else {
            XCTFail("CloudKit SDK cannot construct tagged system-field fixture")
            return record
        }
        _ = record.perform(setter, with: "inbound-live-owner-accepted" as NSString)
        XCTAssertEqual(record.recordChangeTag, "inbound-live-owner-accepted")
        return record
    }

    @BigSyncBackgroundActor
    func testInboundLiveRejectsCancellationResetAccountBindingAndTransportReplacementBeforeTarget() async throws {
        for schedule in 0..<4 {
            let fixture = try await fixture()
            let candidate = try await syncedDeletionCandidate(fixture)
            let record = inboundReplacement(for: candidate)
            let originalText = candidate.row.text
            let originalModifiedAt = candidate.row.modifiedAt
            fixture.adapter._testBeforeImportedRecordTargetWrite = {
                try await Self.replaceInboundOwner(fixture.adapter, schedule: schedule)
            }
            defer { fixture.adapter._testBeforeImportedRecordTargetWrite = nil }
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("A replaced owner admitted the old live response, schedule \(schedule)")
            } catch is CancellationError { }
            XCTAssertEqual(candidate.row.text, originalText)
            XCTAssertEqual(candidate.row.modifiedAt, originalModifiedAt)
            let entity = try XCTUnwrap(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: candidate.recordID.recordName))
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertEqual(entity.encodedRecord, candidate.encodedRecord)
            XCTAssertNil(entity.pendingGeneration)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: candidate.recordID.recordName))

            fixture.adapter._testBeforeImportedRecordTargetWrite = nil
            try await Self.restoreInboundOwner(fixture.adapter)
            let fresh = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTAssertEqual(fresh.first?.disposition, .applied)
            XCTAssertEqual(candidate.row.text, "incoming")
            XCTAssertEqual(fixture.adapter.getRecord(for: entity)?.recordChangeTag,
                "inbound-live-owner-accepted")
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertNil(entity.pendingGeneration)
        }
    }

    @BigSyncBackgroundActor
    func testInboundLiveRetainsCommittedTargetAfterOwnerReplacementAndFreshRetry() async throws {
        for schedule in 0..<4 {
            let fixture = try await fixture()
            let candidate = try await syncedDeletionCandidate(fixture)
            let record = inboundReplacement(for: candidate)
            fixture.adapter._testBeforeImportedRecordPersistenceWrite = {
                XCTAssertEqual(candidate.row.text, "incoming")
                try await Self.replaceInboundOwner(fixture.adapter, schedule: schedule)
            }
            defer { fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil }
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("A retired live response published tracking, schedule \(schedule)")
            } catch is CancellationError { }
            XCTAssertEqual(candidate.row.text, "incoming")
            let entity = try XCTUnwrap(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: candidate.recordID.recordName))
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertEqual(entity.encodedRecord, candidate.encodedRecord)
            XCTAssertNil(entity.pendingGeneration)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: candidate.recordID.recordName))

            fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil
            try await Self.restoreInboundOwner(fixture.adapter)
            let fresh = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTAssertEqual(fresh.first?.disposition, .applied)
            XCTAssertEqual(candidate.row.text, "incoming")
            XCTAssertEqual(fixture.adapter.getRecord(for: entity)?.recordChangeTag,
                "inbound-live-owner-accepted")
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertNil(entity.pendingGeneration)
            try fixture.target.write {
                candidate.row.text = "later local edit"
                candidate.row.refreshChangeMetadata(explicitlyModified: true)
            }
            let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: candidate.recordID.recordName)?.generation)
            try await fixture.adapter.didFinishImport()
            let replay = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTAssertEqual(replay.first?.disposition, .preservedPendingLocal(generation: generation))
            XCTAssertEqual(candidate.row.text, "later local edit")
            XCTAssertEqual(entity.pendingGeneration, generation)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: candidate.recordID.recordName)?.generation, generation)
        }
    }

    @BigSyncBackgroundActor
    func testInboundDeletionRejectsCancellationResetAccountBindingAndTransportReplacement() async throws {
        for replacement in 0..<4 {
            let fixture = try await fixture()
            let (row, name, _) = try await pendingRow(fixture)
            let accepted = try await fixture.adapter.prepareUploadBatch(limit: 10)
            try await fixture.adapter.acknowledgeUploadedRecords(accepted.records, from: accepted)
            let entity = try XCTUnwrap(fixture.tracking.object(
                ofType: SyncedEntity.self, forPrimaryKey: name
            ))
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
            let recordID = CKRecord.ID(recordName: name, zoneID: fixture.adapter.recordZoneID)
            fixture.adapter._testBeforeRemoteDeletionTargetWrite = {
                switch replacement {
                case 0:
                    fixture.adapter.cancelSynchronization()
                    try fixture.adapter.prepareForFencedMigrationAfterCancellation()
                case 1:
                    try await fixture.adapter.activateAccountScope("inbound-deletion-successor-account")
                case 2:
                    try await fixture.adapter.activateReplicaBinding(
                        accountScopeIdentifier: "split-owner-account",
                        replicaBindingGenerationIdentifier: "inbound-deletion-successor-binding"
                    )
                default:
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.inbound-deletion-successor", databaseScope: .public
                    )
                }
            }
            defer { fixture.adapter._testBeforeRemoteDeletionTargetWrite = nil }
            do {
                _ = try await fixture.adapter.deleteRecords(with: [recordID])
                XCTFail("An obsolete inbound deletion crossed the operation's original identity")
            } catch is CancellationError { }
            XCTAssertFalse(row.isDeleted, "Rejected deletion must leave the live target unchanged")
            XCTAssertEqual(row.text, "local")
            XCTAssertEqual(entity.entityState, .synced, "Rejected deletion must leave tracking unchanged")
            XCTAssertNil(entity.pendingGeneration)
            XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))

            if replacement == 0 {
                fixture.adapter._testBeforeRemoteDeletionTargetWrite = nil
                try await fixture.adapter.unsetCancellation()
                let retry = try await fixture.adapter.deleteRecords(with: [recordID])
                XCTAssertEqual(retry.count, 1)
                XCTAssertTrue(row.isDeleted, "A fresh operation may apply the same server deletion")
                XCTAssertEqual(entity.entityState, .deletedRemotely)
                XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name),
                             "Inbound tombstones must not manufacture a local edit")
            }
        }
    }

    @BigSyncBackgroundActor
    func testInboundDeletionRetainsCommittedTombstoneAfterOwnerRetirementAndFreshRetry() async throws {
        let fixture = try await fixture()
        let candidate = try await syncedDeletionCandidate(fixture)
        let originalModifiedAt = candidate.row.modifiedAt
        fixture.adapter._testAfterRemoteDeletionTargetWrite = {
            XCTAssertTrue(candidate.row.isDeleted)
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testAfterRemoteDeletionTargetWrite = nil }
        do {
            _ = try await fixture.adapter.deleteRecords(with: [candidate.recordID])
            XCTFail("The old deletion published tracking after its target transaction retired the owner")
        } catch is CancellationError { }
        XCTAssertTrue(candidate.row.isDeleted)
        XCTAssertEqual(candidate.row.modifiedAt, originalModifiedAt)
        let entity = try XCTUnwrap(fixture.tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: candidate.recordID.recordName))
        XCTAssertEqual(entity.entityState, .synced)
        XCTAssertEqual(entity.encodedRecord, candidate.encodedRecord)
        XCTAssertNil(entity.pendingGeneration)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: candidate.recordID.recordName))

        fixture.adapter._testAfterRemoteDeletionTargetWrite = nil
        try await fixture.adapter.unsetCancellation()
        let results = try await fixture.adapter.deleteRecords(with: [candidate.recordID])
        XCTAssertEqual(results.first?.disposition, .appliedTombstone)
        XCTAssertTrue(candidate.row.isDeleted)
        XCTAssertEqual(candidate.row.modifiedAt, originalModifiedAt)
        XCTAssertEqual(entity.entityState, .deletedRemotely)
        XCTAssertNil(entity.pendingGeneration)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: candidate.recordID.recordName))

        // Redelivery must also preserve a later local Unmark and its exact
        // durable generation after the interrupted deletion has recovered.
        try fixture.target.write {
            candidate.row.isDeleted = false
            candidate.row.refreshChangeMetadata(explicitlyModified: true)
        }
        let successorGeneration = try XCTUnwrap(fixture.target.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: candidate.recordID.recordName)?.generation)
        try await fixture.adapter.didFinishImport()
        let replay = try await fixture.adapter.deleteRecords(with: [candidate.recordID])
        XCTAssertEqual(replay.first?.disposition,
            .preservedNewerLive(generation: successorGeneration))
        XCTAssertFalse(candidate.row.isDeleted)
        XCTAssertEqual(entity.entityState, .new)
        XCTAssertNil(entity.encodedRecord)
        XCTAssertEqual(entity.pendingGeneration, successorGeneration)
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: candidate.recordID.recordName)?.generation, successorGeneration)
    }

    @BigSyncBackgroundActor
    func testCancelledPhysicalDeleteJournalCleanupRetainsTombstoneForSuccessor() async throws {
        let fixture = try await fixture()
        let (row, name, _) = try await pendingRow(fixture)
        let upload = try await fixture.adapter.prepareUploadBatch(limit: 10)
        try await fixture.adapter.acknowledgeUploadedRecords(upload.records, from: upload)
        try fixture.target.write {
            row.isDeleted = true
            row.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.didFinishImport()
        let batch = try await fixture.adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(batch.recordIDs.count, 1)
        let generation = try XCTUnwrap(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        fixture.adapter._testAfterDeletionTrackingWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testAfterDeletionTrackingWrite = nil }
        do {
            try await fixture.adapter.acknowledgeDeletedRecordIDs(batch.recordIDs, from: batch)
            XCTFail("The obsolete deletion continued its target journal cleanup")
        } catch is CancellationError {}
        XCTAssertTrue(row.isDeleted)
        XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation, generation)
        XCTAssertEqual(fixture.tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: name)?.entityState, .deletedRemotely)
        fixture.adapter._testAfterDeletionTrackingWrite = nil
        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter.didFinishImport()
        let retry = try await fixture.adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(retry.recordIDs, batch.recordIDs)
        try await fixture.adapter.acknowledgeDeletedRecordIDs(retry.recordIDs, from: retry)
        XCTAssertNil(fixture.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testCleanupIgnoresProvisionalTrackingDeletionAndPreservesOtherOwner() async throws {
        let fixture = try await fixture()
        let row = SplitOwnerRow()
        row.isDeleted = true
        try fixture.target.write { fixture.target.add(row) }
        let name = SplitOwnerRow.className() + ".row"
        let entity = SyncedEntity(entityType: SplitOwnerRow.className(), identifier: name,
            state: SyncedEntityState.synced.rawValue)
        try fixture.tracking.write { fixture.tracking.add(entity) }
        fixture.tracking.beginWrite()
        defer { if fixture.tracking.isInWriteTransaction { fixture.tracking.cancelWrite() } }
        entity.entityState = .deletedRemotely
        fixture.adapter._testBeforeCleanupTrackingWrite = {
            XCTAssertTrue(fixture.tracking.isInWriteTransaction,
                "Cleanup must not commit the independent owner's provisional deletion")
            fixture.tracking.cancelWrite()
        }
        defer { fixture.adapter._testBeforeCleanupTrackingWrite = nil }
        try await fixture.adapter.cleanUp()
        XCTAssertEqual(entity.entityState, .synced)
        XCTAssertNotNil(fixture.target.object(ofType: SplitOwnerRow.self, forPrimaryKey: "row"))
    }

    @BigSyncBackgroundActor
    func testCancelledCleanupCannotRetireTrackingAfterTargetCommit() async throws {
        let fixture = try await fixture()
        let row = SplitOwnerRow()
        row.isDeleted = true
        try fixture.target.write { fixture.target.add(row) }
        let name = SplitOwnerRow.className() + ".row"
        let entity = SyncedEntity(entityType: SplitOwnerRow.className(), identifier: name,
            state: SyncedEntityState.deletedRemotely.rawValue)
        try fixture.tracking.write { fixture.tracking.add(entity) }
        fixture.adapter._testBeforeCleanupTrackingWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testBeforeCleanupTrackingWrite = nil }
        do {
            try await fixture.adapter.cleanUp()
            XCTFail("The obsolete cleanup retired tracking owned by its successor")
        } catch is CancellationError {}
        XCTAssertNil(fixture.target.object(ofType: SplitOwnerRow.self, forPrimaryKey: "row"))
        XCTAssertNotNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        fixture.adapter._testBeforeCleanupTrackingWrite = nil
        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter.cleanUp()
        XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    private func incomingOwnerRecord(
        _ fixture: (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm),
        contract: Bool
    ) -> CKRecord {
        let type: Object.Type = contract ? SplitOwnerContractRow.self : SplitOwnerRow.self
        let record = CKRecord(recordType: type.className(), recordID: .init(
            recordName: type.className() + (contract ? ".contract" : ".row"),
            zoneID: fixture.adapter.recordZoneID))
        record["text"] = "admitted incoming" as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 20) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 20) as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    func testCancelledIncomingImportCannotApplyTargetAfterSuccessorResumes() async throws {
        for contract in [false, true] {
            let fixture = try await fixture(contract: contract)
            let record = incomingOwnerRecord(fixture, contract: contract)
            fixture.adapter._testBeforeImportedRecordTargetWrite = {
                fixture.adapter.cancelSynchronization()
                try fixture.adapter.prepareForFencedMigrationAfterCancellation()
            }
            defer { fixture.adapter._testBeforeImportedRecordTargetWrite = nil }
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("An obsolete import applied its target in the successor attempt")
            } catch is CancellationError {}
            fixture.target.refresh()
            let type: Object.Type = contract ? SplitOwnerContractRow.self : SplitOwnerRow.self
            XCTAssertTrue(fixture.target.objects(type).isEmpty)
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertTrue(fixture.tracking.objects(SyncedEntity.self)
                .filter("entityType == %@", type.className()).isEmpty)
            if contract {
                XCTAssertNil(fixture.target.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: record.recordID.recordName))
            }

            fixture.adapter._testBeforeImportedRecordTargetWrite = nil
            try await fixture.adapter.unsetCancellation()
            let retry = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTAssertEqual(retry.count, 1)
            fixture.target.refresh()
            XCTAssertEqual(fixture.target.objects(type).first?["text"] as? String, "admitted incoming")
            XCTAssertNotNil(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName)?.encodedRecord)
        }
    }

    @BigSyncBackgroundActor
    func testCancelledIncomingImportRetainsTargetCommitWithoutPublishingTracking() async throws {
        for contract in [false, true] {
            let fixture = try await fixture(contract: contract)
            let record = incomingOwnerRecord(fixture, contract: contract)
            fixture.adapter._testBeforeImportedRecordPersistenceWrite = {
                fixture.adapter.cancelSynchronization()
                try fixture.adapter.prepareForFencedMigrationAfterCancellation()
            }
            defer { fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil }
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("An obsolete import published tracking after its target phase")
            } catch is CancellationError {}
            fixture.target.refresh()
            let type: Object.Type = contract ? SplitOwnerContractRow.self : SplitOwnerRow.self
            XCTAssertEqual(fixture.target.objects(type).first?["text"] as? String, "admitted incoming",
                "The target phase was already durable before ownership changed")
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName))
            let revision: String?
            if contract {
                revision = try XCTUnwrap(fixture.target.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: record.recordID.recordName)).revision
            } else { revision = nil }

            fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil
            try await fixture.adapter.unsetCancellation()
            _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTAssertNotNil(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName)?.encodedRecord)
            if contract {
                XCTAssertEqual(fixture.target.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: record.recordID.recordName)?.revision, revision,
                    "Redelivery completes tracking without manufacturing a new accepted ancestor")
            }
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testIncomingImportRejectsAccountAndTransportReplacementBeforeTargetAdmission() async throws {
        for replacesTransport in [false, true] {
            let fixture = try await fixture()
            let record = incomingOwnerRecord(fixture, contract: false)
            fixture.adapter._testBeforeImportedRecordTargetWrite = {
                if replacesTransport {
                    try await fixture.adapter.activateTransportNamespace(
                        containerIdentifier: "iCloud.test.incoming-successor", databaseScope: .public)
                } else {
                    try await fixture.adapter.activateAccountScope("incoming-successor-account")
                }
            }
            defer { fixture.adapter._testBeforeImportedRecordTargetWrite = nil }
            do {
                _ = try await fixture.adapter.saveChanges(in: [record], forceSave: true)
                XCTFail("An obsolete incoming payload crossed its account or transport namespace")
            } catch is CancellationError {}
            fixture.target.refresh()
            XCTAssertTrue(fixture.target.objects(SplitOwnerRow.self).isEmpty)
            XCTAssertTrue(fixture.target.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName))
        }
    }


    @BigSyncBackgroundActor
    func testAuthoritativeOwnEchoIgnoresForeignProvisionalPredecessorAndJournal() async throws {
        for hasCommittedJournal in [false, true] {
            let fixture = try await fixture()
            let row = SplitOwnerSemanticEcho()
            let name = SplitOwnerSemanticEcho.className() + "." + row.id
            // This seeds server-known state; only the optional local edit is authoritative.
            try fixture.target.write {
                fixture.target.add(row)
                if hasCommittedJournal {
                    row.refreshChangeMetadata(explicitlyModified: true,
                        at: Date(timeIntervalSinceReferenceDate: 10))
                }
            }
            let committedGeneration = fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation
            let committedModifiedAt = row.modifiedAt
            let record = CKRecord(recordType: SplitOwnerSemanticEcho.className(),
                recordID: .init(recordName: name, zoneID: fixture.adapter.recordZoneID))
            record["text"] = row.text as CKRecordValue
            fixture.target.beginWrite()
            defer { if fixture.target.isInWriteTransaction { fixture.target.cancelWrite() } }
            row.text = "foreign provisional predecessor"
            row.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 100))
            let provisionalGeneration = try XCTUnwrap(fixture.target.object(
                ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
            XCTAssertNotEqual(provisionalGeneration, committedGeneration)

            let outcomes = try await fixture.adapter.validateAuthoritativeOwnUploadRecords([record])
            XCTAssertEqual(outcomes.count, 1)
            XCTAssertEqual(outcomes.first?.disposition, .validatedAuthoritativeOwnUpload,
                "Semantic validation must use the committed predecessor behind the foreign write")
            XCTAssertTrue(fixture.target.isInWriteTransaction)
            XCTAssertEqual(row.text, "foreign provisional predecessor")
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, provisionalGeneration)
            XCTAssertTrue(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
            XCTAssertNil(fixture.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))

            fixture.target.cancelWrite()
            XCTAssertEqual(row.text, "committed predecessor")
            XCTAssertEqual(row.modifiedAt, committedModifiedAt)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, committedGeneration)
            let retry = try await fixture.adapter.validateAuthoritativeOwnUploadRecords([record])
            XCTAssertEqual(retry.first?.disposition, .validatedAuthoritativeOwnUpload)
            XCTAssertTrue(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testUploadAcknowledgementRejectsOwnerRetiredBySynchronousJournalRefresh() async throws {
        for replacesProvider in [false, true] {
            let fixture = try await fixture()
            let (row, name, sentGeneration) = try await pendingRow(fixture)
            let batch = try await fixture.adapter.prepareUploadBatch(limit: 0)
            let originalProvider = try XCTUnwrap(fixture.adapter.realmProvider)
            let capture = SplitRefreshCapture()
            let configuration = fixture.target.configuration
            fixture.target.autorefresh = false
            fixture.adapter._testBeforePendingMutationTrackingWrite = { capture.forwardedCount += 1 }

            let onChange: @BigSyncBackgroundActor @Sendable () -> Void = {
                guard capture.error == nil, capture.refreshRetirementCount == 0 else { return }
                if capture.stagedGeneration == nil {
                    // The acknowledgement has committed its target journal retirement.
                    // Commit a successor on a separate native Realm thread while this
                    // reader remains pinned to that retirement version.
                    guard fixture.target.object(ofType: BigSyncPendingMutation.self,
                        forPrimaryKey: name) == nil else { return }
                    let joined = DispatchSemaphore(value: 0)
                    let result = SplitRefreshWriterResult()
                    Thread.detachNewThread {
                        defer { joined.signal() }
                        result.result = Result {
                            let writer = try Realm(configuration: configuration)
                            let current = try XCTUnwrap(writer.object(ofType: SplitOwnerRow.self,
                                forPrimaryKey: "row"))
                            try writer.write {
                                current.text = "committed refresh successor"
                                current.refreshChangeMetadata(explicitlyModified: true,
                                    at: Date(timeIntervalSinceReferenceDate: 300))
                            }
                            return try XCTUnwrap(writer.object(ofType: BigSyncPendingMutation.self,
                                forPrimaryKey: name)?.generation)
                        }
                    }
                    guard joined.wait(timeout: .now() + 5) == .success else {
                        capture.error = NSError(
                            domain: "SplitOwnerJournalRefreshHistory", code: 1,
                            userInfo: [NSLocalizedDescriptionKey:
                                "Native successor writer did not finish within the bounded refresh history"]
                        )
                        return
                    }
                    do { capture.stagedGeneration = try result.result?.get() }
                    catch { capture.error = error }
                    return
                }
                guard fixture.target.object(ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: name)?.generation == capture.stagedGeneration else { return }
                capture.refreshRetirementCount += 1
                capture.sawOwningWrite = fixture.target.isInWriteTransaction
                if replacesProvider { fixture.adapter.realmProvider = nil }
                else {
                    fixture.adapter.cancelSynchronization()
                    do { try fixture.adapter.prepareForFencedMigrationAfterCancellation() }
                    catch { capture.error = error }
                }
            }
            let token = fixture.target.observe { notification, _ in
                guard notification == .didChange else { return }
                // Realm invokes this notification synchronously on the target actor.
                BigSyncBackgroundActor.shared.assumeIsolated { _ in
                    let callback = unsafeBitCast(onChange, to: (@Sendable () -> Void).self)
                    callback()
                }
            }
            defer {
                token.invalidate()
                fixture.target.autorefresh = true
                fixture.adapter.realmProvider = originalProvider
                fixture.adapter._testBeforePendingMutationTrackingWrite = nil
            }
            do {
                try await fixture.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
                XCTFail("The original acknowledgement borrowed the refresh successor's owner")
            } catch is CancellationError {}
            token.invalidate()
            if let error = capture.error { throw error }
            XCTAssertEqual(capture.refreshRetirementCount, 1,
                "The real committed journal refresh must synchronously retire the original owner")
            XCTAssertFalse(capture.sawOwningWrite,
                "Retirement belongs to read refresh after target commit, not the owned write")
            XCTAssertEqual(capture.forwardedCount, 0,
                "The original owner must be checked before forwarding can capture a successor")
            let successor = try XCTUnwrap(capture.stagedGeneration)
            XCTAssertNotEqual(successor, sentGeneration)
            XCTAssertEqual(row.text, "committed refresh successor")
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, successor)
            let tracked = try XCTUnwrap(fixture.tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: name))
            XCTAssertEqual(tracked.entityState, .synced)
            XCTAssertNil(tracked.pendingGeneration,
                "Only the already committed acknowledgement may publish tracking")

            fixture.adapter.realmProvider = originalProvider
            fixture.adapter._testBeforePendingMutationTrackingWrite = nil
            fixture.target.autorefresh = true
            try await fixture.adapter.unsetCancellation()
            try await fixture.adapter.didFinishImport()
            XCTAssertEqual(tracked.pendingGeneration, successor)
            XCTAssertEqual(fixture.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, successor)
        }
    }

    @BigSyncBackgroundActor
    func testAuthoritativeOwnEchoAllowsInitialProviderSetup() async throws {
        let fixture = try await fixture(initializeThroughEcho: true)
        XCTAssertNotNil(fixture.adapter.realmProvider)
        XCTAssertTrue(fixture.target.objects(SplitOwnerRow.self).isEmpty)
        XCTAssertTrue(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testCancelledAuthoritativeOwnEchoCannotPublishSuccessorQuarantine() async throws {
        let fixture = try await fixture()
        let name = SplitOwnerInvalidEcho.className() + ".invalid-echo"
        let record = CKRecord(recordType: SplitOwnerInvalidEcho.className(),
            recordID: .init(recordName: name, zoneID: fixture.adapter.recordZoneID))
        fixture.adapter._testBeforeAuthoritativeOwnUploadQuarantineWrite = {
            fixture.adapter.cancelSynchronization()
            try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        }
        defer { fixture.adapter._testBeforeAuthoritativeOwnUploadQuarantineWrite = nil }
        do {
            _ = try await fixture.adapter.validateAuthoritativeOwnUploadRecords([record])
            XCTFail("An obsolete own echo published quarantine into the successor attempt")
        } catch is CancellationError {}
        XCTAssertTrue(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        XCTAssertTrue(fixture.target.objects(SplitOwnerInvalidEcho.self).isEmpty)

        fixture.adapter._testBeforeAuthoritativeOwnUploadQuarantineWrite = nil
        try await fixture.adapter.unsetCancellation()
        let retry = try await fixture.adapter.validateAuthoritativeOwnUploadRecords([record])
        XCTAssertEqual(retry.count, 1)
        XCTAssertEqual(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
        XCTAssertEqual(fixture.tracking.objects(BigSyncInboundSemanticQuarantine.self).first?.recordName, name)
        XCTAssertTrue(fixture.target.objects(SplitOwnerInvalidEcho.self).isEmpty)
    }

}
#endif
