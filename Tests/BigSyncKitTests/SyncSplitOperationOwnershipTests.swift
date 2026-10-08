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
    private func fixture(comparison: Bool = false, contract: Bool = false) async throws
        -> (adapter: RealmSwiftAdapter, target: Realm, tracking: Realm) {
        let nonce = UUID().uuidString
        var config = Realm.Configuration()
        config.fileURL = nil
        config.inMemoryIdentifier = "split-owner-target-" + nonce
        config.objectTypes = [SplitOwnerRow.self, SplitOwnerChild.self,
                              SplitOwnerParent.self, BigSyncPendingMutation.self]
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
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "split-owner-account",
            replicaBindingGenerationIdentifier: "split-owner-binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.split-owner",
            databaseScope: .private)
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
}
#endif
