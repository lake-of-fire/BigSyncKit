import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(W1ContractNote)
final class W1ContractNote: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .independentFields, preserveConflictingFields: ["text"],
        incomingRepresentation: .init(identity: "w1-note-released", fields: [
            "number": .compatibilityDefault(.integer(0)),
            "flag": .compatibilityDefault(.boolean(false)),
            "isDeleted": .compatibilityDefault(.boolean(false)),
        ]))
    @Persisted(primaryKey: true) var id = UUID()
    @Persisted var text = "constructor-text"
    // Deliberately different from the declared released omission semantics.
    @Persisted var number = 37
    @Persisted var flag = true
    @Persisted var optional: String? = "constructor-optional"
    @Persisted var list: List<Int>
    @Persisted var members: MutableSet<String>
    @Persisted var map: Map<String, Int>
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(W1RetainedArticle)
final class W1RetainedArticle: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"]),
        deletion: .retained, semanticMetadataFields: ["createdAt"],
        incomingRepresentation: .init(identity: "w1-retained-article", fields: [
            "number": .compatibilityDefault(.integer(0)),
            "isDeleted": .compatibilityDefault(.boolean(false)),
        ]))
    @Persisted(primaryKey: true) var id = "article"
    @Persisted var epoch: String?
    @Persisted var title = "initial"
    @Persisted var number = 17
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// Real target/tracking Realms and production adapter entry points. Synthetic
/// CloudKit records are adapter inputs, not evidence of signed cloud delivery.
final class SyncUndoCloseoutW1Tests: XCTestCase {
    @BigSyncBackgroundActor
    func testDeletionRebaseIgnoresProvisionalResurrectionAndKeepsOriginalJournal() async throws {
        try await verifyDeletionRebaseDuringForeignWrite(removesJournal: false)
    }

    @BigSyncBackgroundActor
    func testDeletionRebaseRetainsCommittedDebtDuringProvisionalJournalRemoval() async throws {
        try await verifyDeletionRebaseDuringForeignWrite(removesJournal: true)
    }

    @BigSyncBackgroundActor
    private func verifyDeletionRebaseDuringForeignWrite(removesJournal: Bool) async throws {
        let (adapter, realm) = try await fixture()
        let value = W1ContractNote()
        value.id = noteID
        try realm.write {
            realm.add(value)
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let deletion = try XCTUnwrap(try await adapter.preparedRecordDeletions(
            limit: 1, restrictedToEntityType: nil).first)
        let generation = try XCTUnwrap(deletion.generation)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let tracked = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: deletion.recordID.recordName))
        try tracking.write { tracked.encodedRecord = nil }
        let journal = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: deletion.recordID.recordName))
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        if removesJournal {
            realm.delete(journal)
        } else {
            value.isDeleted = false
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let server = note(adapter)
        try await adapter.rebasePendingDeletionMetadata(using: [server],
            matchingPreparedGenerations: [deletion.recordID.recordName: generation])
        XCTAssertTrue(realm.isInWriteTransaction, "Rebase cannot settle the target's independent owner")
        XCTAssertNotNil(tracked.encodedRecord, "The original committed deletion still owns its system fields")
        XCTAssertEqual(tracked.entityState, .deletedLocally)
        XCTAssertEqual(tracked.pendingGeneration, generation)
        if removesJournal {
            XCTAssertNil(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: deletion.recordID.recordName))
        } else {
            XCTAssertFalse(value.isDeleted)
        }
        realm.cancelWrite()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: deletion.recordID.recordName)?.generation, generation)
    }

    @BigSyncBackgroundActor
    lazy var realmFixtureOwner = RealmAdapterFixtureOwner(testCase: self)

    let noteID = UUID(uuidString: "A0000000-0000-0000-0000-000000000001")!

    @BigSyncBackgroundActor
    func fixture(enableRecordRebasing: Bool = true) async throws -> (RealmSwiftAdapter, Realm) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("w1-realms-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        realmFixtureOwner.ownDirectory(directory)
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [W1ContractNote.self, W1RetainedArticle.self, BigSyncPendingMutation.self]
        if enableRecordRebasing {
            BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        }
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "w1-local", replicaBindingGenerationIdentifier: "w1-binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent("tracking.realm")
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: [],
            recordZoneID: .init(zoneName: "w1-closeout"),
            logger: Logger(label: "W1Closeout"), startSetupTask: false)
        realmFixtureOwner.own(adapter)
        adapter.mergePolicy = .custom
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "w1-account",
            replicaBindingGenerationIdentifier: "w1-binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.w1-closeout",
            databaseScope: .private)
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    func note(_ adapter: RealmSwiftAdapter, time: Double = 10) -> CKRecord {
        let record = CKRecord(recordType: W1ContractNote.className(), recordID: .init(
            recordName: W1ContractNote.className() + "." + noteID.uuidString, zoneID: adapter.recordZoneID))
        record["text"] = "server-text" as CKRecordValue
        record["number"] = 9 as CKRecordValue
        record["flag"] = true as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    func deliver(_ records: [CKRecord], to adapter: RealmSwiftAdapter) async throws -> [InboundLiveResult] {
        let result = try await adapter.saveChanges(in: records, forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
        return result
    }

    @BigSyncBackgroundActor
    func quiet(_ adapter: RealmSwiftAdapter, realm: Realm) async throws {
        try await adapter.didFinishImport()
        let saves = try await adapter.prepareUploadBatch(limit: 50)
        let deletes = try await adapter.prepareDeletionBatch(limit: 50)
        XCTAssertTrue(saves.records.isEmpty)
        XCTAssertTrue(deletes.recordIDs.isEmpty)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).filter {
            $0.namespace == adapter.recordRebaseContext?.namespace
        }.isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testTerminalBoundaryIgnoresBusyIdleRealmAndFindsNewGeneration()
    async throws {
        let (adapter, realm) = try await fixture()
        let context = try XCTUnwrap(adapter.recordRebaseContext)
        // Exercise the same indexed journal and comparison-evidence queries
        // used by the production terminal cut. Older-binding rows remain
        // durable, but cannot authorize this transport's upload.
        try realm.write {
            for index in 0..<1_024 {
                realm.add(BigSyncPendingMutation(
                    recordName: "W1ContractNote.stale-\(index)",
                    entityType: W1ContractNote.className(),
                    objectIdentifier: "stale-\(index)",
                    replicaBindingGenerationIdentifier: "older-binding"
                ))
                let receipt = BigSyncRecordConflict()
                receipt.id = "resolved-\(index)"
                receipt.recordName = "W1ContractNote.resolved-\(index)"
                receipt.entityType = W1ContractNote.className()
                receipt.namespace = context.namespace
                receipt.isResolved = true
                receipt.isPreservationReceipt = true
                realm.add(receipt)
            }
        }

        let idleStart = Date()
        for _ in 0..<3 {
            XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
        }
        print("terminal-busy-idle-three-cuts-seconds=\(Date().timeIntervalSince(idleStart))")

        let authored = W1ContractNote()
        authored.id = noteID
        try realm.write {
            realm.add(authored)
            authored.refreshChangeMetadata(explicitlyModified: true)
        }
        let pending = try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: W1ContractNote.className() + "." + noteID.uuidString
        ))
        XCTAssertEqual(pending.replicaBindingGenerationIdentifier,
                       context.binding)
        XCTAssertTrue(try adapter.hasPendingChangesAtTerminalBoundary())
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count,
                       1_025)
    }

    @BigSyncBackgroundActor
    func testTrackingPendingQueryIgnoresLargeSyncedPopulation() async throws {
        let (adapter, _) = try await fixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        try tracking.write {
            for index in 0..<4_096 {
                tracking.add(SyncedEntity(
                    entityType: W1ContractNote.className(),
                    identifier: "W1ContractNote.synced-\(index)",
                    state: SyncedEntityState.synced.rawValue
                ))
            }
        }
        let idleStart = Date()
        for _ in 0..<3 {
            adapter.updateHasChanges(realm: tracking)
            XCTAssertFalse(adapter.hasChanges)
            XCTAssertEqual(adapter.hasChangesCount, 0)
        }
        print("tracking-busy-idle-three-checks-seconds=\(Date().timeIntervalSince(idleStart))")

        let changed = SyncedEntity(
            entityType: W1ContractNote.className(),
            identifier: "W1ContractNote.changed",
            state: SyncedEntityState.changed.rawValue
        )
        changed.setPendingMutation(
            generation: "fresh-generation",
            replicaBindingGenerationIdentifier: "w1-binding"
        )
        try tracking.write { tracking.add(changed) }
        adapter.updateHasChanges(realm: tracking)
        XCTAssertTrue(adapter.hasChanges)
        XCTAssertEqual(adapter.hasChangesCount, 1)
    }

    @BigSyncBackgroundActor
    func testOmittedScalarsApplyDeclaredDefaultsAndAgreeWithBaseline() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([note(adapter)], to: adapter)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let incoming = note(adapter, time: 20)
        incoming["number"] = nil
        incoming["flag"] = nil
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(object.number, 0)
        XCTAssertFalse(object.flag)
        XCTAssertNil(object.optional)
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        XCTAssertEqual(baseline.fieldDigests, try BigSyncRecordFingerprint.fields(of: object))
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        let revision = baseline.revision
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(baseline.revision, revision)
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testRetainedClearIsAcceptedByTheTerminalAudit() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1RetainedArticle()
        try realm.write {
            realm.add(object)
            object.epoch = "E0"
            object.number = 0
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 50)
        XCTAssertEqual(batch.records.count, 1)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await adapter.cleanUp()
        XCTAssertTrue(object.isDeleted)
        let audit = try await adapter.auditSynchronizationState(serverRecords: batch.records)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedTombstoneRetiresOnlyItsPhysicalDeletionQuarantine() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1RetainedArticle()
        try realm.write {
            realm.add(object)
            object.epoch = "E0"
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(
            limit: 50,
            restrictedToEntityType: nil
        )
        let record = try XCTUnwrap(prepared.first?.record)
        let deletion = try await adapter.deleteRecords(with: [record.recordID])
        guard case let .quarantined(lineageID) = try XCTUnwrap(
            deletion.first
        ).disposition else {
            return XCTFail("Expected retained physical deletion quarantine")
        }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertNotNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        let unrelated = try XCTUnwrap(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        let unrelatedLineageID = "unrelated-retained-deletion"
        let unrelatedCopy = BigSyncInboundSemanticQuarantine()
        unrelatedCopy.lineageID = unrelatedLineageID
        unrelatedCopy.recordName = "W1RetainedArticle.unrelated"
        unrelatedCopy.entityType = unrelated.entityType
        unrelatedCopy.accountScopeIdentifier = unrelated.accountScopeIdentifier
        unrelatedCopy.semanticScopeIdentifier =
            "retained-physical-deletion:" + unrelatedCopy.recordName
        unrelatedCopy.containerIdentifier = unrelated.containerIdentifier
        unrelatedCopy.databaseScopeRawValue = unrelated.databaseScopeRawValue
        unrelatedCopy.zoneOwnerName = unrelated.zoneOwnerName
        unrelatedCopy.zoneName = unrelated.zoneName
        unrelatedCopy.eventKind = unrelated.eventKind
        unrelatedCopy.validationCode = unrelated.validationCode
        unrelatedCopy.replicaActivationIdentifier =
            unrelated.replicaActivationIdentifier
        unrelatedCopy.changeFeedEpoch = unrelated.changeFeedEpoch
        try tracking.write { tracking.add(unrelatedCopy) }

        // The prepared retained tombstone is the exact server-restoring
        // disposition. Its acknowledgement may retire only this record's
        // quarantine; unrelated evidence must remain an audit blocker.
        try await adapter.didUpload(
            savedRecords: prepared.map(\.record),
            matchingPreparedUploads: prepared
        )
        try await adapter.cleanUp()
        XCTAssertNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        XCTAssertNotNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: unrelatedLineageID
            )
        )
        let audit = try await adapter.auditSynchronizationState(
            serverRecords: prepared.map(\.record)
        )
        XCTAssertFalse(audit.isClean)
        XCTAssertTrue(
            audit.issues.contains {
                $0.contains("W1RetainedArticle.unrelated")
            },
            audit.issues.joined(separator: ",")
        )
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testTerminalLocalDeleteRetiresItsSupersededStagedSave() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let save = try await adapter.prepareUploadBatch(limit: 50)
        XCTAssertEqual(save.records.count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 1)
        try realm.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let revision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first).revision
        try await adapter.didFinishImport()
        let deletion = try await adapter.prepareDeletionBatch(limit: 50)
        XCTAssertEqual(deletion.recordIDs.count, 1)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await adapter.cleanUp()
        XCTAssertNil(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.isComparisonInvalidated, true)
        try await adapter.acknowledgeUploadedRecords(save.records, from: save)
        XCTAssertNil(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func initialImportNote(priorPending: Bool,
        mode: ChangeFeedResetMode = .initialImport,
        priorBinding: String = "w1-binding",
        enableRecordRebasing: Bool = true) async throws -> (RealmSwiftAdapter, Realm) {
        let (adapter, realm) = try await fixture(enableRecordRebasing: enableRecordRebasing)
        let object = W1ContractNote()
        object.id = noteID
        object.text = "legacy-local-note"
        object.createdAt = Date(timeIntervalSinceReferenceDate: 1)
        object.modifiedAt = Date(timeIntervalSinceReferenceDate: 2)
        object.explicitlyModifiedAt = Date(timeIntervalSinceReferenceDate: 2)
        let deleted = W1ContractNote()
        deleted.isDeleted = true
        try realm.write {
            // Historical local-only values, before target journaling existed.
            realm.add(object)
            realm.add(deleted)
        }
        if priorPending {
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            try tracking.write {
                let entity = SyncedEntity(entityType: W1ContractNote.className(),
                    identifier: W1ContractNote.className() + "." + noteID.uuidString,
                    state: SyncedEntityState.changed.rawValue)
                entity.setPendingMutation(generation: "legacy-tracking-only",
                    replicaBindingGenerationIdentifier: priorBinding)
                tracking.add(entity)
            }
        }
        try await adapter.prepareChangeFeedReset(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        try await adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        adapter.invalidateTokens()
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    @BigSyncBackgroundActor
    func assertInitialImportNoteDrains(priorPending: Bool,
        mode: ChangeFeedResetMode = .initialImport) async throws {
        let (adapter, realm) = try await initialImportNote(priorPending: priorPending, mode: mode)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        let fields = try BigSyncRecordFingerprint.fields(of: object)
        let createdAt = object.createdAt
        let modifiedAt = object.modifiedAt
        let explicitlyModifiedAt = object.explicitlyModifiedAt
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        realm.refresh()
        let mutation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first)
        let generation = mutation.generation
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)
        XCTAssertEqual(mutation.replicaBindingGenerationIdentifier, "w1-binding")
        if priorPending {
            XCTAssertEqual(generation, "legacy-tracking-only")
        } else {
            XCTAssertTrue(BigSyncPendingMutation.wasCreatedInMutationJournalIdentity(generation,
                identity: .init(installationIdentifier: "w1-local",
                    replicaBindingGenerationIdentifier: "w1-binding")))
        }
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: object), fields)
        XCTAssertEqual(object.createdAt, createdAt)
        XCTAssertEqual(object.modifiedAt, modifiedAt)
        XCTAssertEqual(object.explicitlyModifiedAt, explicitlyModifiedAt)
        XCTAssertEqual(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first?.pendingGeneration,
            generation)
        // A repeated reconciliation must preserve the target-owned generation.
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: mode)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(prepared.count, 1)
        let upload = try XCTUnwrap(prepared.first)
        XCTAssertEqual(upload.generation, generation)
        XCTAssertEqual(upload.record["text"] as? String, "legacy-local-note")
        XCTAssertNotNil(upload.comparisonBase?.submissionIdentity)
        let saved = try tagged(upload.record, "initial-import-accepted")
        try await adapter.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.fieldDigests, fields)
        XCTAssertEqual(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first?.entityState, .synced)
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testInitialImportJournalsUntrackedAdoptedNoteWithoutReauthoring() async throws {
        try await assertInitialImportNoteDrains(priorPending: false)
    }

    @BigSyncBackgroundActor
    func testInitialImportJournalsAdoptedNoteWithOnlyPriorTrackingIntent() async throws {
        try await assertInitialImportNoteDrains(priorPending: true)
    }

    @BigSyncBackgroundActor
    func testInitialImportReusesExistingEligibleJournalGeneration() async throws {
        let (adapter, realm) = try await initialImportNote(priorPending: true)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        try realm.write {
            object.text = "already-journaled-local-edit"
            object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: .initialImport)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertEqual(object.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 40))
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(prepared.count, 1)
        XCTAssertEqual(prepared.first?.generation, generation)
        XCTAssertEqual(prepared.first?.record["text"] as? String, "already-journaled-local-edit")
    }

    @BigSyncBackgroundActor
    func testServerReconciliationRestoresExactPriorTrackingJournal() async throws {
        try await assertInitialImportNoteDrains(priorPending: true, mode: .serverReconciliation)
    }

    @BigSyncBackgroundActor
    func testInitialImportPreservesLegacyTrackingWhenRebasingIsNotEnabled() async throws {
        for priorPending in [false, true] {
            let (adapter, realm) = try await initialImportNote(priorPending: priorPending,
                enableRecordRebasing: false)
            XCTAssertFalse(BigSyncRecordBaseline.isEnabled(in: realm))
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: .initialImport)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            let entity = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first)
            XCTAssertEqual(entity.entityState, .new)
            XCTAssertEqual(entity.pendingReplicaBindingGenerationIdentifier, "w1-binding")
            XCTAssertFalse(try XCTUnwrap(entity.pendingGeneration).isEmpty)
            if priorPending { XCTAssertEqual(entity.pendingGeneration, "legacy-tracking-only") }
            XCTAssertEqual(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)?.text, "legacy-local-note")
        }
    }

    @BigSyncBackgroundActor
    func testInitialImportDoesNotRebindPriorIntentFromAnotherBinding() async throws {
        let (adapter, realm) = try await initialImportNote(priorPending: true, priorBinding: "obsolete-binding")
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: .initialImport)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertTrue(prepared.isEmpty)
        XCTAssertEqual(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)?.text, "legacy-local-note")
    }

    @BigSyncBackgroundActor
    func testContractRecoveryLeavesServerBackedAndBackupOnlyValuesUnjournaled() async throws {
        for mode in [ChangeFeedResetMode.initialImport, .serverReconciliation, .backupRestore] {
            let (adapter, realm) = try await initialImportNote(priorPending: false, mode: mode)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let name = W1ContractNote.className() + "." + noteID.uuidString
            try tracking.write {
                let provenance = RebuildProvenance()
                provenance.identifier = name
                provenance.entityType = W1ContractNote.className()
                provenance.hadValidServerRecord = true
                provenance.priorState = SyncedEntityState.synced.rawValue
                provenance.accountScopeIdentifier = "w1-account"
                provenance.epoch = 1
                tracking.add(provenance, update: .modified)
            }
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: mode)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
            XCTAssertTrue(prepared.isEmpty)
            XCTAssertEqual(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)?.text, "legacy-local-note")
        }
    }

    @BigSyncBackgroundActor
    func testContractRecoveryRejectsMalformedPriorTrackingProvenance() async throws {
        for mismatchedType in [false, true] {
            let (adapter, realm) = try await initialImportNote(priorPending: true)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let name = W1ContractNote.className() + "." + noteID.uuidString
            try tracking.write {
                let provenance = try XCTUnwrap(tracking.object(ofType: RebuildProvenance.self, forPrimaryKey: name))
                if mismatchedType {
                    provenance.entityType = W1RetainedArticle.className()
                } else {
                    provenance.priorState = SyncedEntityState.synced.rawValue
                }
            }
            try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: .initialImport)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
            XCTAssertTrue(prepared.isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testInitialImportTargetJournalSurvivesInterruptionAndRestart() async throws {
        for priorPending in [false, true] {
            let (adapter, realm) = try await initialImportNote(priorPending: priorPending)
            adapter._testBeforePendingMutationTrackingWrite = { throw W1InjectedFailure.afterTarget }
            do {
                try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                    epoch: 1, mode: .initialImport)
                XCTFail("Expected interruption after the durable target journal")
            } catch W1InjectedFailure.afterTarget { }
            realm.refresh()
            let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
            if priorPending { XCTAssertEqual(generation, "legacy-tracking-only") }
            XCTAssertTrue(try XCTUnwrap(adapter.realmProvider?.persistenceRealm).objects(SyncedEntity.self).isEmpty)
            adapter._testBeforePendingMutationTrackingWrite = nil
            let (restarted, reopened) = try await restart(adapter)
            try await restarted.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
                epoch: 1, mode: .initialImport)
            XCTAssertEqual(reopened.objects(BigSyncPendingMutation.self).first?.generation, generation)
            let prepared = try await restarted.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
            XCTAssertEqual(prepared.count, 1)
            XCTAssertEqual(prepared.first?.generation, generation)
            XCTAssertEqual(prepared.first?.record["text"] as? String, "legacy-local-note")
            let saved = try tagged(XCTUnwrap(prepared.first).record, "restarted-initial-import-accepted")
            try await restarted.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
            try await quiet(restarted, realm: reopened)
        }
    }

    @BigSyncBackgroundActor
    func testInitialImportForwardingKeepsNewerLocalMutation() async throws {
        let (adapter, realm) = try await initialImportNote(priorPending: true)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        adapter._testBeforePendingMutationTrackingWrite = {
            try realm.write {
                object.text = "newer-user-edit"
                object.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: 50))
            }
        }
        try await adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: "w1-account",
            epoch: 1, mode: .initialImport)
        adapter._testBeforePendingMutationTrackingWrite = nil
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        XCTAssertEqual(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first?.pendingGeneration,
            generation)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(prepared.count, 1)
        XCTAssertEqual(prepared.first?.generation, generation)
        XCTAssertEqual(prepared.first?.record["text"] as? String, "newer-user-edit")
    }
}

enum W1InjectedFailure: Error { case afterTarget }


extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testPendingMutationInventoryReadsCommittedJournalDuringProvisionalOwnerWrite() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.text = "committed"
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30)
            )
        }
        let committed = try XCTUnwrap(
            realm.objects(BigSyncPendingMutation.self).first
        )
        let committedGeneration = committed.generation
        let committedChangedAt = committed.changedAt

        realm.beginWrite()
        defer {
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        object.text = "provisional"
        object.isDeleted = true
        object.refreshChangeMetadata(
            explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40)
        )
        XCTAssertNotEqual(
            realm.objects(BigSyncPendingMutation.self).first?.generation,
            committedGeneration
        )

        let inventory = try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]
        )
        XCTAssertEqual(inventory.count, 1)
        XCTAssertEqual(inventory.first?.recordName,
            W1ContractNote.className() + "." + noteID.uuidString)
        XCTAssertEqual(inventory.first?.changedAt, committedChangedAt)
        XCTAssertFalse(try XCTUnwrap(inventory.first).isDeletion)
    }

    @BigSyncBackgroundActor
    func testJournalForwardingStartsFromCommittedVersionDuringProvisionalOwnerWrite() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.text = "committed"
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30)
            )
        }
        let committedGeneration = try XCTUnwrap(
            realm.objects(BigSyncPendingMutation.self).first?.generation
        )

        realm.beginWrite()
        object.text = "provisional"
        object.refreshChangeMetadata(
            explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40)
        )
        let provisionalGeneration = try XCTUnwrap(
            realm.objects(BigSyncPendingMutation.self).first?.generation
        )
        XCTAssertNotEqual(provisionalGeneration, committedGeneration)
        defer {
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }

        try await adapter.didFinishImport()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(
            tracking.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: W1ContractNote.className() + "." + noteID.uuidString
            )?.pendingGeneration,
            committedGeneration
        )
    }

    @BigSyncBackgroundActor
    func testTrackingAdmissionDoesNotBorrowProvisionalSuccessorGeneration() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.text = "committed"
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30)
            )
        }
        let committedGeneration = try XCTUnwrap(
            realm.objects(BigSyncPendingMutation.self).first?.generation
        )
        adapter._testBeforePendingMutationTrackingWrite = {
            realm.beginWrite()
            object.text = "provisional"
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40)
            )
        }
        defer {
            adapter._testBeforePendingMutationTrackingWrite = nil
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }

        try await adapter.didFinishImport()
        XCTAssertNotEqual(
            realm.objects(BigSyncPendingMutation.self).first?.generation,
            committedGeneration
        )
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(
            tracking.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: W1ContractNote.className() + "." + noteID.uuidString
            )?.pendingGeneration,
            committedGeneration
        )
    }
}


extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testTerminalBoundaryCannotBorrowProvisionalTargetJournalRemoval() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30)
            )
        }
        let name = W1ContractNote.className() + "." + noteID.uuidString
        XCTAssertNotNil(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name
        ))

        realm.beginWrite()
        defer {
            if realm.isInWriteTransaction { realm.cancelWrite() }
        }
        realm.delete(try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name
        )))
        XCTAssertNil(realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name
        ))
        XCTAssertTrue(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testTerminalBoundaryCannotBorrowProvisionalTrackingAcknowledgement() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30)
            )
        }
        try await adapter.didFinishImport()
        let name = W1ContractNote.className() + "." + noteID.uuidString
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let tracked = try XCTUnwrap(tracking.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: name
        ))
        XCTAssertNotNil(tracked.pendingGeneration)

        // Leave only committed tracking debt so this specifically exercises
        // the persistence-Realm side of the terminal cutoff.
        try realm.write {
            realm.delete(try XCTUnwrap(realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name
            )))
        }
        tracking.beginWrite()
        defer {
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
        }
        tracked.state = SyncedEntityState.synced.rawValue
        tracked.clearPendingMutation()
        XCTAssertNil(tracked.pendingGeneration)
        XCTAssertTrue(try adapter.hasPendingChangesAtTerminalBoundary())
    }
}


extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testPublicationBoundaryAndEpochIgnoreProvisionalTrackingTransition() async throws {
        let (adapter, _) = try await fixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let committedToken = Data("committed-token".utf8)
        try tracking.write {
            let token = ServerToken()
            token.token = committedToken
            tracking.add(token)
            let rebuild = RebuildProvenanceState()
            rebuild.epoch = 7
            tracking.add(rebuild, update: .modified)
        }
        let committedBoundary = try XCTUnwrap(
            adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: "w1-account",
                replicaBindingGenerationIdentifier: "w1-binding",
                containerIdentifier: "iCloud.test.w1-closeout",
                databaseScope: .private
            )
        )
        XCTAssertEqual(try adapter.changeFeedEpoch(), 7)

        tracking.beginWrite()
        defer {
            if tracking.isInWriteTransaction { tracking.cancelWrite() }
        }
        tracking.objects(ServerToken.self).first?.token =
            Data("provisional-token".utf8)
        tracking.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        )?.epoch = 8

        XCTAssertEqual(
            try adapter.consumedServerBoundaryIdentifier(
                accountScopeIdentifier: "w1-account",
                replicaBindingGenerationIdentifier: "w1-binding",
                containerIdentifier: "iCloud.test.w1-closeout",
                databaseScope: .private
            ),
            committedBoundary
        )
        XCTAssertEqual(try adapter.changeFeedEpoch(), 7)
    }
}
