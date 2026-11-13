import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Production adapter operations against the existing disk-backed W1 fixture.
/// The evidence tables deliberately remain in the schema: retentionOnly must
/// suppress comparison work because of policy, rather than missing tables.
extension SyncUndoCloseoutW1Tests {
    private func assertNoComparisonRows(
        in realm: Realm, file: StaticString = #filePath, line: UInt = #line
    ) {
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).count, 0, file: file, line: line)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 0, file: file, line: line)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 0, file: file, line: line)
    }

    private func retentionPending(_ name: String, in realm: Realm) throws -> BigSyncPendingMutation {
        try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyLiveUploadKeepsNewGenerationAfterOlderAcknowledgement() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1UploadSnapshotRow()
        let name = W1UploadSnapshotRow.className() + "." + object.id
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 10))
        }
        let firstGeneration = try retentionPending(name, in: realm).generation
        XCTAssertEqual(try retentionPending(name, in: realm).replicaBindingGenerationIdentifier, "w1-binding")
        assertNoComparisonRows(in: realm)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let first = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(first.records.count, 1)
        XCTAssertEqual(first.prepared.first?.generation, firstGeneration)
        XCTAssertNil(first.prepared.first?.comparisonBase)
        XCTAssertEqual(first.records.first?["title"] as? String, "committed")
        assertNoComparisonRows(in: realm)

        try realm.write {
            object.title = "new typing"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 20))
        }
        let currentGeneration = try retentionPending(name, in: realm).generation
        XCTAssertNotEqual(currentGeneration, firstGeneration)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        try await adapter.acknowledgeUploadedRecords(first.records, from: first)
        XCTAssertEqual(try retentionPending(name, in: realm).generation, currentGeneration)
        XCTAssertEqual(object.title, "new typing")
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertEqual(entity.pendingGeneration, currentGeneration)
        assertNoComparisonRows(in: realm)

        let current = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(current.records.count, 1)
        XCTAssertEqual(current.prepared.first?.generation, currentGeneration)
        XCTAssertEqual(current.records.first?["title"] as? String, "new typing")
        XCTAssertNil(current.prepared.first?.comparisonBase)
        try await adapter.acknowledgeUploadedRecords(current.records, from: current)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        XCTAssertNil(entity.pendingGeneration)
        XCTAssertEqual(entity.entityState, .synced)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyBatchCannotAcknowledgeAnotherAdaptersJournal() async throws {
        let (owner, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let (other, otherRealm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1UploadSnapshotRow()
        let name = W1UploadSnapshotRow.className() + "." + object.id
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try retentionPending(name, in: realm).generation
        _ = try await owner._test_forwardPendingMutations(in: realm)
        let batch = try await owner.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        do {
            try await other.acknowledgeUploadedRecords(batch.records, from: batch)
            XCTFail("Only the issuing adapter may consume its prepared generations")
        } catch RealmSwiftAdapterAcknowledgementError.batchBelongsToAnotherAdapter { }
        XCTAssertEqual(try retentionPending(name, in: realm).generation, generation)
        XCTAssertTrue(otherRealm.objects(BigSyncPendingMutation.self).isEmpty)
        assertNoComparisonRows(in: realm)
        assertNoComparisonRows(in: otherRealm)
        try await owner.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyDeclaredLiveContractUploadsWithoutComparisonEvidence() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1ContractNote()
        object.id = noteID
        let name = W1ContractNote.className() + "." + noteID.uuidString
        try realm.write {
            realm.add(object)
            object.text = "local contract value"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try retentionPending(name, in: realm).generation
        assertNoComparisonRows(in: realm)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        XCTAssertEqual(batch.prepared.first?.generation, generation)
        XCTAssertNil(batch.prepared.first?.comparisonBase)
        XCTAssertEqual(batch.records.first?["text"] as? String, "local contract value")
        assertNoComparisonRows(in: realm)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyInboundDeclaredLiveContractCreatesNoComparisonRows() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        _ = try await deliver([note(adapter)], to: adapter)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        XCTAssertEqual(object.text, "server-text")
        XCTAssertEqual(object.number, 9)
        XCTAssertFalse(object.isDeleted)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        assertNoComparisonRows(in: realm)
        _ = try await deliver([note(adapter, time: 20)], to: adapter)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyInboundCollisionPreservesJournalWithoutConflictCopies() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1ContractNote()
        object.id = noteID
        let name = W1ContractNote.className() + "." + noteID.uuidString
        try realm.write {
            realm.add(object)
            object.text = "unsent local value"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try retentionPending(name, in: realm).generation
        // With legacyAutomatic this unbased collision preserves a conflict.
        // Retention-only keeps ordinary whole-record journal ownership instead.
        _ = try await deliver([note(adapter, time: 40)], to: adapter)
        XCTAssertEqual(object.text, "unsent local value")
        XCTAssertEqual(try retentionPending(name, in: realm).generation, generation)
        XCTAssertEqual(realm.objects(W1ContractNote.self).count, 1)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    private func verifyRetentionOnlyRetainedTombstone<T: Object & ChangeMetadataRecordable>(
        _ object: T
    ) async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        try realm.write {
            realm.add(object)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let pending = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first)
        let name = pending.recordName
        let generation = pending.generation
        assertNoComparisonRows(in: realm)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let deletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertTrue(deletion.recordIDs.isEmpty, "A retained tombstone travels as a value")
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        XCTAssertEqual(batch.prepared.first?.generation, generation)
        XCTAssertNil(batch.prepared.first?.comparisonBase)
        XCTAssertEqual(batch.records.first?["isDeleted"] as? NSNumber, NSNumber(value: true))
        assertNoComparisonRows(in: realm)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await adapter.cleanUp()
        XCTAssertFalse(object.isInvalidated)
        XCTAssertTrue(object.isDeleted)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyLegacyRetainedTombstoneAcknowledgementKeepsTarget() async throws {
        try await verifyRetentionOnlyRetainedTombstone(W1LegacyRetainedArticle())
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyDeclaredRetainedTombstoneAcknowledgementKeepsTarget() async throws {
        let object = W1RetainedArticle()
        object.epoch = "E0"
        try await verifyRetentionOnlyRetainedTombstone(object)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyPhysicalDeletionInvalidatesNoBaselineAndConsumesExactGeneration() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1UploadSnapshotRow()
        let name = W1UploadSnapshotRow.className() + "." + object.id
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 10))
        }
        let liveGeneration = try retentionPending(name, in: realm).generation
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 20))
        }
        let deletionGeneration = try retentionPending(name, in: realm).generation
        XCTAssertNotEqual(deletionGeneration, liveGeneration)
        assertNoComparisonRows(in: realm)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let deletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.map(\.recordName), [name])
        XCTAssertEqual(deletion.prepared.first?.generation, deletionGeneration)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        try await adapter.cleanUp()
        XCTAssertNil(realm.object(ofType: W1UploadSnapshotRow.self, forPrimaryKey: "upload-snapshot"))
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyOldPhysicalDeletionCannotConsumeResurrectionGeneration() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let object = W1UploadSnapshotRow()
        let name = W1UploadSnapshotRow.className() + "." + object.id
        try realm.write {
            realm.add(object)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 10))
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let deletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.map(\.recordName), [name])
        let deletedGeneration = try retentionPending(name, in: realm).generation
        try realm.write {
            object.isDeleted = false
            object.title = "resurrected"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 20))
        }
        let currentGeneration = try retentionPending(name, in: realm).generation
        XCTAssertNotEqual(currentGeneration, deletedGeneration)
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await adapter.cleanUp()
        XCTAssertFalse(object.isInvalidated)
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(try retentionPending(name, in: realm).generation, currentGeneration)
        assertNoComparisonRows(in: realm)
        let upload = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(upload.prepared.first?.generation, currentGeneration)
        XCTAssertEqual(upload.records.first?["title"] as? String, "resurrected")
        try await adapter.acknowledgeUploadedRecords(upload.records, from: upload)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testDefaultPolicyStillInstallsBaselineAndStagesContractSubmission() async throws {
        XCTAssertEqual(BigSyncMutationPolicy(excludedClassNames: []).recordEvidenceUse, .legacyAutomatic)
        let (adapter, realm) = try await fixture()
        _ = try await deliver([note(adapter)], to: adapter)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        XCTAssertEqual(baseline.fieldDigests, try BigSyncRecordFingerprint.fields(of: object))
        let oldRevision = baseline.revision
        try realm.write {
            object.text = "new local text"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        XCTAssertNotNil(batch.prepared.first?.comparisonBase?.submissionIdentity)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 0)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertNotEqual(baseline.revision, oldRevision)
        XCTAssertEqual(baseline.fieldDigests, try BigSyncRecordFingerprint.fields(of: object))
    }

    @BigSyncBackgroundActor
    func testDefaultPolicyStillPreservesUnbasedContractCollision() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        let name = W1ContractNote.className() + "." + noteID.uuidString
        try realm.write {
            realm.add(object)
            object.text = "unsent local value"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try retentionPending(name, in: realm).generation
        _ = try await deliver([note(adapter, time: 40)], to: adapter)
        XCTAssertEqual(object.text, "unsent local value")
        XCTAssertEqual(try retentionPending(name, in: realm).generation, generation)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().count, 1)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyInboundRetainedTombstoneKeepsValueWithoutComparisonRows() async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let record = CKRecord(recordType: W1RetainedArticle.className(), recordID: .init(
            recordName: W1RetainedArticle.className() + ".article", zoneID: adapter.recordZoneID))
        record["epoch"] = "E0" as CKRecordValue
        record["title"] = "remote tombstone" as CKRecordValue
        record["number"] = 0 as CKRecordValue
        record["isDeleted"] = true as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 10) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 10) as CKRecordValue
        _ = try await deliver([record], to: adapter)
        let object = try XCTUnwrap(realm.object(ofType: W1RetainedArticle.self, forPrimaryKey: "article"))
        XCTAssertTrue(object.isDeleted)
        XCTAssertEqual(object.title, "remote tombstone")
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        try await adapter.cleanUp()
        XCTAssertFalse(object.isInvalidated)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyFreshRestoringUploadCanRetirePhysicalDeletionQuarantine() async throws {
        try await verifyRetentionOnlyRestoringUpload(W1LegacyRetainedArticle())
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyDeclaredRestoringUploadRetiresOnlyItsObservedDeletionQuarantine() async throws {
        try await verifyRetentionOnlyRestoringUpload(W1RetainedArticle())
    }

    @BigSyncBackgroundActor
    private func verifyRetentionOnlyRestoringUpload<T: Object & ChangeMetadataRecordable>(
        _ object: T
    ) async throws {
        let (adapter, realm) = try await fixture(recordEvidenceUse: .retentionOnly)
        let name = T.className() + "." + (try XCTUnwrap(object["id"] as? String))
        try realm.write {
            realm.add(object)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let generation = try retentionPending(name, in: realm).generation
        let results = try await adapter.deleteRecords(with: [.init(recordName: name, zoneID: adapter.recordZoneID)])
        guard case let .quarantined(lineage) = try XCTUnwrap(results.first).disposition else {
            return XCTFail("Physical deletion of a retained record must be quarantined")
        }
        let previous = await adapter.serverChangeToken
        try await adapter.commitInboundPage(.init(
            previousCursor: previous,
            nextCursor: RecordZoneChangeCursor(serializedData: Data(UUID().uuidString.utf8)),
            liveResults: [], deletionResults: results))
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let quarantine = try XCTUnwrap(tracking.object(
            ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        let pageReceiptID = quarantine.committedPageReceiptID
        XCTAssertFalse(pageReceiptID.isEmpty)
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        XCTAssertEqual(batch.prepared.first?.generation, generation)
        XCTAssertNil(batch.prepared.first?.comparisonBase)
        assertNoComparisonRows(in: realm)
        // A fresh preparation observed this exact deletion page. A simulated
        // saved response has a genuine system archive/tag, just as the existing
        // production acknowledgement contract requires for quarantine cleanup.
        let saved = try batch.records.map { try tagged($0, "retention-restoring-response") }
        try await adapter.acknowledgeUploadedRecords(saved, from: batch)
        XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertNil(tracking.object(ofType: BigSyncInboundPageReceipt.self, forPrimaryKey: pageReceiptID))
        XCTAssertTrue(object.isDeleted)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    private func verifyRetentionOnlyRecoveryDrains(
        priorPending: Bool, mode: ChangeFeedResetMode
    ) async throws {
        let (adapter, realm) = try await initialImportNote(priorPending: priorPending,
            mode: mode, recordEvidenceUse: .retentionOnly)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let createdAt = object.createdAt
        let modifiedAt = object.modifiedAt
        let explicitlyModifiedAt = object.explicitlyModifiedAt
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        let mutation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first)
        let generation = mutation.generation
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1,
            "Legacy discovery must publish target-owned intent, not only tracking state")
        XCTAssertEqual(mutation.replicaBindingGenerationIdentifier, "w1-binding")
        if priorPending {
            XCTAssertEqual(generation, "legacy-tracking-only")
        } else {
            XCTAssertTrue(BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(generation,
                identity: .init(installationIdentifier: "w1-local",
                    replicaBindingGenerationIdentifier: "w1-binding")))
        }
        XCTAssertEqual(object.text, "legacy-local-note")
        XCTAssertEqual(object.createdAt, createdAt)
        XCTAssertEqual(object.modifiedAt, modifiedAt)
        XCTAssertEqual(object.explicitlyModifiedAt, explicitlyModifiedAt)
        assertNoComparisonRows(in: realm)
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: mutation.recordName))
        XCTAssertEqual(entity.pendingGeneration, generation)
        let batch = try await adapter.prepareUploadBatch(limit: 50)
        XCTAssertEqual(batch.records.count, 1)
        XCTAssertEqual(batch.prepared.first?.generation, generation)
        XCTAssertEqual(batch.records.first?["text"] as? String, "legacy-local-note")
        XCTAssertNil(batch.prepared.first?.comparisonBase)
        assertNoComparisonRows(in: realm)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertNil(entity.pendingGeneration)
        XCTAssertEqual(entity.entityState, .synced)
        assertNoComparisonRows(in: realm)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyInitialImportJournalsLegacyValueWithoutReauthoring() async throws {
        try await verifyRetentionOnlyRecoveryDrains(priorPending: false, mode: .initialImport)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyInitialImportRecoversExactPriorTrackingGeneration() async throws {
        try await verifyRetentionOnlyRecoveryDrains(priorPending: true, mode: .initialImport)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyServerReconciliationRecoversExactPriorTrackingGeneration() async throws {
        try await verifyRetentionOnlyRecoveryDrains(priorPending: true, mode: .serverReconciliation)
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyRecoveryKeepsAlreadyJournaledSuccessorGeneration() async throws {
        for mode in [ChangeFeedResetMode.initialImport, .serverReconciliation] {
            let (adapter, realm) = try await initialImportNote(priorPending: true,
                mode: mode, recordEvidenceUse: .retentionOnly)
            let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
            try realm.write {
                object.text = "newer journaled user edit"
                object.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: 40))
            }
            let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
            XCTAssertNotEqual(generation, "legacy-tracking-only")
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: mode)
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
            let batch = try await adapter.prepareUploadBatch(limit: 50)
            XCTAssertEqual(batch.records.count, 1)
            XCTAssertEqual(batch.prepared.first?.generation, generation)
            XCTAssertEqual(batch.records.first?["text"] as? String, "newer journaled user edit")
            XCTAssertEqual(object.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 40))
            assertNoComparisonRows(in: realm)
        }
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyRecoveryDoesNotRebindObsoleteProvenance() async throws {
        for mode in [ChangeFeedResetMode.initialImport, .serverReconciliation] {
            let (adapter, realm) = try await initialImportNote(priorPending: true,
                mode: mode, priorBinding: "obsolete-binding", recordEvidenceUse: .retentionOnly)
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: mode)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            XCTAssertTrue(tracking.objects(SyncedEntity.self).filter { $0.pendingGeneration != nil }.isEmpty,
                "Rejected provenance must not reappear through tracking-only fallback")
            let batch = try await adapter.prepareUploadBatch(limit: 50)
            XCTAssertTrue(batch.records.isEmpty)
            assertNoComparisonRows(in: realm)
        }
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyRecoveryRejectsMalformedProvenanceBeforeTrackingFallback() async throws {
        for mode in [ChangeFeedResetMode.initialImport, .serverReconciliation] {
            for mismatchedType in [false, true] {
                let (adapter, realm) = try await initialImportNote(priorPending: true,
                    mode: mode, recordEvidenceUse: .retentionOnly)
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let name = W1ContractNote.className() + "." + noteID.uuidString
                try tracking.write {
                    let row = try XCTUnwrap(tracking.object(ofType: RebuildProvenance.self, forPrimaryKey: name))
                    if mismatchedType {
                        row.entityType = W1RetainedArticle.className()
                    } else {
                        row.priorState = SyncedEntityState.synced.rawValue
                    }
                }
                try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                    epoch: 1, mode: mode)
                XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
                XCTAssertTrue(tracking.objects(SyncedEntity.self).filter { $0.pendingGeneration != nil }.isEmpty)
                let batch = try await adapter.prepareUploadBatch(limit: 50)
                XCTAssertTrue(batch.records.isEmpty)
                XCTAssertEqual(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)?.text,
                    "legacy-local-note")
                assertNoComparisonRows(in: realm)
            }
        }
    }

    @BigSyncBackgroundActor
    func testRetentionOnlyRecoveryTargetJournalSurvivesForwardingInterruption() async throws {
        for mode in [ChangeFeedResetMode.initialImport, .serverReconciliation] {
            let (adapter, realm) = try await initialImportNote(priorPending: true,
                mode: mode, recordEvidenceUse: .retentionOnly)
            adapter._testBeforePendingMutationTrackingWrite = { throw W1InjectedFailure.afterTarget }
            defer { adapter._testBeforePendingMutationTrackingWrite = nil }
            do {
                try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                    epoch: 1, mode: mode)
                XCTFail("Expected interruption after committing target-owned recovery intent")
            } catch W1InjectedFailure.afterTarget { }
            let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
            XCTAssertEqual(generation, "legacy-tracking-only")
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            XCTAssertTrue(tracking.objects(SyncedEntity.self).isEmpty)
            assertNoComparisonRows(in: realm)
            adapter._testBeforePendingMutationTrackingWrite = nil
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: mode)
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
            let batch = try await adapter.prepareUploadBatch(limit: 50)
            XCTAssertEqual(batch.records.count, 1)
            XCTAssertEqual(batch.prepared.first?.generation, generation)
            try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            assertNoComparisonRows(in: realm)
        }
    }
}
