import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(RetainedContractRow)
private final class RetainedContractRow: Object, ChangeMetadataRecordable, BigSyncRecordContractProviding {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"]),
        deletion: .retained, semanticMetadataFields: ["createdAt"],
        expectedFields: ["epoch", "title", "count", "isDeleted", "createdAt", "bytes", "members", "counts"])
    @Persisted(primaryKey: true) var id = "article"
    @Persisted var epoch = "E0"
    @Persisted var title = "original"
    @Persisted var count = 0
    @Persisted var bytes = Data()
    @Persisted var members: MutableSet<String>
    @Persisted var counts: Map<String, Int>
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(ContractRecoveryNote)
private final class ContractRecoveryNote: Object, ChangeMetadataRecordable, BigSyncRecordContractProviding {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .independentFields, preserveConflictingFields: ["text"])
    @Persisted(primaryKey: true) var id = UUID()
    @Persisted var text = "original"
    @Persisted var isDone: Bool?
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(UnadoptedContractRow)
private final class UnadoptedContractRow: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = "plain"
    @Persisted var text = "local"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(BoundContractControl)
private final class BoundContractControl: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding, BigSyncInboundSemanticReplacementValidating,
    BigSyncInboundPendingSemanticReplacementValidating {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: []), deletion: .retained)
    @Persisted(primaryKey: true) var id = "control"
    @Persisted var epoch = "E0"
    @Persisted var digest = ""
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    static func validateInboundSemanticReplacement(_ record: CKRecord, existingObject: Object?) throws {
        guard let local = existingObject as? BoundContractControl else { return }
        if record["epoch"] as? String == local.epoch, !local.digest.isEmpty,
           record["digest"] as? String != local.digest { throw CocoaError(.coderReadCorrupt) }
    }
    static func validateInboundSemanticPredecessorOfPendingMutation(
        _ record: CKRecord, existingObject: Object
    ) throws {
        guard let local = existingObject as? BoundContractControl,
              record["epoch"] as? String == local.epoch,
              record["digest"] as? String == "", !local.digest.isEmpty else {
            throw CocoaError(.coderReadCorrupt)
        }
    }
}

final class SyncRetainedRecordContractTests: XCTestCase {
    private let nonce = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!

    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        var config = Realm.Configuration()
        config.inMemoryIdentifier = "retained-target-" + UUID().uuidString
        config.objectTypes = [RetainedContractRow.self, BoundContractControl.self, ContractRecoveryNote.self,
                              UnadoptedContractRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &config)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [config],
            mutationJournalIdentityProvider: { .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding") })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "retained-tracking-" + UUID().uuidString
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [config], excludedClassNames: [],
            recordZoneID: .init(zoneName: "retained-contract"), logger: Logger(label: "RetainedContractTests"), startSetupTask: false)
        adapter.mergePolicy = .custom
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.retained-contract", databaseScope: .private)
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(_ adapter: RealmSwiftAdapter, epoch: String = "E0", title: String = "original",
                        count: Int = 0, deleted: Bool = false, time: Double = 10, created: Double = 1) -> CKRecord {
        let record = CKRecord(recordType: RetainedContractRow.className(),
            recordID: .init(recordName: RetainedContractRow.className() + ".article", zoneID: adapter.recordZoneID))
        record["epoch"] = epoch as CKRecordValue
        record["title"] = title as CKRecordValue
        record["count"] = count as CKRecordValue
        record["bytes"] = Data() as CKRecordValue
        record["isDeleted"] = deleted as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: created) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func deliver(_ records: [CKRecord], to adapter: RealmSwiftAdapter) async throws -> [InboundLiveResult] {
        let results = try await adapter.saveChanges(in: records, forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
        return results
    }

    private func value(_ realm: Realm) throws -> RetainedContractRow {
        try XCTUnwrap(realm.object(ofType: RetainedContractRow.self, forPrimaryKey: "article"))
    }

    @BigSyncBackgroundActor
    private func requireQuiet(_ adapter: RealmSwiftAdapter) async throws {
        try await adapter.didFinishImport()
        let saves = try await adapter.prepareUploadBatch(limit: 50)
        let deletes = try await adapter.prepareDeletionBatch(limit: 50)
        XCTAssertTrue(saves.records.isEmpty)
        XCTAssertTrue(deletes.recordIDs.isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testClearIsASaveAndItsAcknowledgementRetainsTheRow() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([record(adapter, count: 7)], to: adapter)
        let object = try value(realm)
        let oldRevision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first).revision
        try realm.write {
            object.epoch = try BigSyncLifetimeID.next(after: object.epoch, nonce: nonce)
            object.count = 0
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, oldRevision)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.isComparisonInvalidated, false)
        try await adapter.didFinishImport()
        let deletes = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertTrue(deletes.recordIDs.isEmpty)
        let saves = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(saves.records.count, 1)
        XCTAssertEqual(saves.records.first?["isDeleted"] as? Bool, true)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 1)
        try await adapter.acknowledgeUploadedRecords(saves.records, from: saves)
        try await adapter.cleanUp()
        XCTAssertTrue(try value(realm).isDeleted)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        let revision = realm.objects(BigSyncRecordBaseline.self).first?.revision
        try await requireQuiet(adapter)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
    }

    @BigSyncBackgroundActor
    func testLateSavedClearDoesNotAcknowledgeReopenedLifetime() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([record(adapter)], to: adapter)
        let object = try value(realm)
        try realm.write {
            object.epoch = try BigSyncLifetimeID.next(after: object.epoch, nonce: nonce)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let clear = try await adapter.prepareUploadBatch(limit: 10)
        try realm.write {
            object.epoch = try BigSyncLifetimeID.next(after: object.epoch, nonce: nonce)
            object.isDeleted = false
            object.count = 3
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let successor = object.epoch
        try await adapter.acknowledgeUploadedRecords(clear.records, from: clear)
        try await adapter.didFinishImport()
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(object.epoch, successor)
        let next = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(next.records.count, 1)
        XCTAssertEqual(next.records.first?["count"] as? Int, 3)
        try await adapter.acknowledgeUploadedRecords(next.records, from: next)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testReceivedClearThenReopenCanEachReachQuietState() async throws {
        let (adapter, realm) = try await fixture()
        let first = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
        let second = try BigSyncLifetimeID.next(after: first, nonce: nonce)
        _ = try await deliver([record(adapter, epoch: first, deleted: true)], to: adapter)
        XCTAssertTrue(try value(realm).isDeleted)
        XCTAssertFalse(try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first).isComparisonInvalidated)
        try await adapter.cleanUp()
        _ = try value(realm)
        _ = try await deliver([record(adapter, epoch: second)], to: adapter)
        XCTAssertFalse(try value(realm).isDeleted)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testIndependentTitleDoesNotKeepPredecessorCreatedAt() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([record(adapter, count: 7, created: 1)], to: adapter)
        let object = try value(realm)
        try realm.write { object.title = "mine"; object.refreshChangeMetadata(explicitlyModified: true) }
        let next = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
        _ = try await deliver([record(adapter, epoch: next, created: 99)], to: adapter)
        XCTAssertEqual(object.title, "mine")
        XCTAssertEqual(object.createdAt, Date(timeIntervalSinceReferenceDate: 99))
        XCTAssertEqual(object.count, 0)
        XCTAssertEqual(object.epoch, next)
    }

    @BigSyncBackgroundActor
    func testOlderClearCannotUndoAcknowledgedReopen() async throws {
        let (adapter, realm) = try await fixture()
        let clear = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
        let live = try BigSyncLifetimeID.next(after: clear, nonce: nonce)
        _ = try await deliver([record(adapter, epoch: live, count: 3)], to: adapter)
        _ = try await deliver([record(adapter, epoch: clear, deleted: true, time: 9_999)], to: adapter)
        XCTAssertFalse(try value(realm).isDeleted)
        XCTAssertEqual(try value(realm).epoch, live)
        let save = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(save.records.first?["epoch"] as? String, live)
        try await adapter.acknowledgeUploadedRecords(save.records, from: save)
        try await requireQuiet(adapter)
    }

    // Missing comparison evidence is not permission to reverse the reset's
    // own order. These simulate absent, invalidated and unusable local evidence
    // without inventing a server baseline or changing a user mutation journal.
    @BigSyncBackgroundActor
    private func makeComparisonEvidenceUnusable(_ mode: Int, in realm: Realm) throws {
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        try realm.write {
            switch mode {
            case 0: realm.delete(baseline)
            case 1: baseline.isComparisonInvalidated = true
            case 2: baseline.namespace = "previous-transport-namespace"
            default: baseline.schemaSignature = "previous-contract-signature"
            }
        }
    }

    @BigSyncBackgroundActor
    func testMissingComparisonEvidenceCannotRollBackAcknowledgedReopen() async throws {
        for mode in 0..<4 {
            let (adapter, realm) = try await fixture()
            let clear = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
            let live = try BigSyncLifetimeID.next(after: clear, nonce: nonce)
            _ = try await deliver([record(adapter, epoch: live, count: 3, created: 99)], to: adapter)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            try makeComparisonEvidenceUnusable(mode, in: realm)
            _ = try await deliver([record(adapter, epoch: clear, title: "incoming title",
                deleted: true, time: 9_999)], to: adapter)
            let object = try value(realm)
            XCTAssertEqual(object.epoch, live, "evidence mode \(mode)")
            XCTAssertFalse(object.isDeleted)
            XCTAssertEqual(object.count, 3)
            XCTAssertEqual(object.createdAt, Date(timeIntervalSinceReferenceDate: 99))
            XCTAssertEqual(object.title, "incoming title")
            XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
            let save = try await adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(save.records.count, 1)
            XCTAssertEqual(save.records.first?["epoch"] as? String, live)
            try await adapter.acknowledgeUploadedRecords(save.records, from: save)
            let revision = realm.objects(BigSyncRecordBaseline.self).first?.revision
            try await requireQuiet(adapter)
            XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        }
    }

    @BigSyncBackgroundActor
    func testMissingComparisonEvidenceCannotResurrectNewerClear() async throws {
        for mode in 0..<4 {
            let (adapter, realm) = try await fixture()
            let live = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
            let clear = try BigSyncLifetimeID.next(after: live, nonce: nonce)
            _ = try await deliver([record(adapter, epoch: clear, deleted: true, created: 99)], to: adapter)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            try makeComparisonEvidenceUnusable(mode, in: realm)
            _ = try await deliver([record(adapter, epoch: live, count: 7, time: 9_999)], to: adapter)
            let object = try value(realm)
            XCTAssertEqual(object.epoch, clear, "evidence mode \(mode)")
            XCTAssertTrue(object.isDeleted)
            XCTAssertEqual(object.count, 0)
            XCTAssertEqual(object.createdAt, Date(timeIntervalSinceReferenceDate: 99))
            let deletes = try await adapter.prepareDeletionBatch(limit: 10)
            XCTAssertTrue(deletes.recordIDs.isEmpty)
            let save = try await adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(save.records.count, 1)
            XCTAssertEqual(save.records.first?["epoch"] as? String, clear)
            try await adapter.acknowledgeUploadedRecords(save.records, from: save)
            try await requireQuiet(adapter)
        }
    }

    @BigSyncBackgroundActor
    func testMissingComparisonEvidenceStillAcceptsGenuineSuccessor() async throws {
        let (adapter, realm) = try await fixture()
        let live = try BigSyncLifetimeID.next(after: "E0", nonce: nonce)
        let clear = try BigSyncLifetimeID.next(after: live, nonce: nonce)
        _ = try await deliver([record(adapter, epoch: live, count: 7)], to: adapter)
        try makeComparisonEvidenceUnusable(0, in: realm)
        _ = try await deliver([record(adapter, epoch: clear, deleted: true)], to: adapter)
        XCTAssertEqual(try value(realm).epoch, clear)
        XCTAssertTrue(try value(realm).isDeleted)
        XCTAssertEqual(try value(realm).count, 0)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testPhysicalDeletionIsQuarantinedWithoutInventingLifecycle() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = record(adapter, count: 3)
        _ = try await deliver([incoming], to: adapter)
        let revision = realm.objects(BigSyncRecordBaseline.self).first?.revision
        let outcomes = try await adapter.deleteRecords(with: [incoming.recordID])
        guard case let .quarantined(lineage) = outcomes.first?.disposition else { return XCTFail("Expected durable quarantine") }
        XCTAssertNotNil(adapter.realmProvider?.persistenceRealm?.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertFalse(try value(realm).isDeleted)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        try await adapter.cleanUp()
        XCTAssertEqual(try value(realm).count, 3)
    }

    @BigSyncBackgroundActor
    func testLostFirstUploadReplyPreservesLaterTypingWhenServerValueReturns() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "V1"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let sent = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(sent.records.count, 1)
        // Capture a separate server-owned copy before upload temporary files
        // are retired. A fetched CloudKit asset has independent file storage.
        let accepted = try BigSyncRecordPayload.encode(try XCTUnwrap(sent.records.first))
        try realm.write { object.title = "V2"; object.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let retry = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(retry.records.first?["title"] as? String, "V1")
        let serverCopy = try BigSyncRecordPayload.decode(accepted)
        _ = try await deliver([serverCopy], to: adapter)
        XCTAssertEqual(object.title, "V2")
        XCTAssertTrue(realm.objects(BigSyncRecordConflict.self).isEmpty)
        let latest = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(latest.records.first?["title"] as? String, "V2")
        try await adapter.acknowledgeUploadedRecords(latest.records, from: latest)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testSubmittedAssetsAreIndependentOfTemporaryAssetFiles() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        let data = Data(repeating: 42, count: 1024)
        try realm.write { realm.add(object); object.bytes = data; object.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let first = try await adapter.prepareUploadBatch(limit: 10)
        let firstAsset = try XCTUnwrap(first.records.first?["bytes"] as? CKAsset)
        if let url = firstAsset.fileURL { try FileManager.default.removeItem(at: url) }
        // didFinishImport clears the manager's path cache as it does after an
        // interrupted upload. The durable candidate must rematerialize bytes.
        try await adapter.didFinishImport()
        let retry = try await adapter.prepareUploadBatch(limit: 10)
        let asset = try XCTUnwrap(retry.records.first?["bytes"] as? CKAsset)
        XCTAssertEqual(try Data(contentsOf: XCTUnwrap(asset.fileURL)), data)
    }

    @BigSyncBackgroundActor
    func testUnbasedConflictPreservesBothValuesAndHasAResolutionPath() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write { realm.add(object); object.title = "mine"; object.refreshChangeMetadata(explicitlyModified: true) }
        let remote = record(adapter, title: "theirs")
        let first = try await deliver([remote], to: adapter)
        guard case let .quarantined(lineage) = first.first?.disposition else { return XCTFail("Expected preserved conflict") }
        XCTAssertNotNil(adapter.realmProvider?.persistenceRealm?.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertEqual(object.title, "mine")
        _ = try await deliver([remote], to: adapter)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1, "Redelivery must not grow preservation history")
        let snapshot = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        let blocked = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(blocked.records.isEmpty)
        XCTAssertThrowsError(try adapter.hasPendingChangesAtTerminalBoundary())
        try await adapter.resolveRecordConflict(id: snapshot.id, expectedGeneration: snapshot.generation, choice: .keepLocal)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
        XCTAssertEqual(object.title, "mine")
        let saves = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(saves.records.count, 1)
        try await adapter.acknowledgeUploadedRecords(saves.records, from: saves)
        try await requireQuiet(adapter)
        XCTAssertTrue(adapter.realmProvider?.persistenceRealm?.objects(BigSyncInboundSemanticQuarantine.self).isEmpty == true)
    }

    @BigSyncBackgroundActor
    func testStaleConflictDecisionCannotDiscardNewTyping() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write { realm.add(object); object.title = "mine"; object.refreshChangeMetadata(explicitlyModified: true) }
        _ = try await deliver([record(adapter, title: "theirs")], to: adapter)
        let old = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        try realm.write { object.title = "new typing"; object.refreshChangeMetadata(explicitlyModified: true) }
        do {
            try await adapter.resolveRecordConflict(id: old.id, expectedGeneration: old.generation, choice: .useIncoming)
            XCTFail("A stale UI choice cannot overwrite new typing")
        } catch BigSyncRecordContractError.staleConflict { }
        XCTAssertEqual(object.title, "new typing")
        try await adapter.refreshRecordConflict(old.id)
        let current = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertNotEqual(current.generation, old.generation)
        try await adapter.resolveRecordConflict(id: current.id, expectedGeneration: current.generation, choice: .useIncoming)
        XCTAssertEqual(object.title, "theirs")
    }

    @BigSyncBackgroundActor
    func testSchemaMismatchPreservesInsteadOfReinterpretingBaseline() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([record(adapter)], to: adapter)
        let object = try value(realm)
        try realm.write {
            realm.objects(BigSyncRecordBaseline.self).first?.schemaSignature = "old-codec"
            object.title = "mine"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "remote")], to: adapter)
        XCTAssertEqual(object.title, "mine")
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().count, 1)
    }

    @BigSyncBackgroundActor
    func testIndependentNoteTextAndTaskDoNotCreateRecoveryCopy() async throws {
        let (adapter, realm) = try await fixture()
        let note = ContractRecoveryNote()
        try realm.write { realm.add(note); note.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let baseline = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(baseline.records, from: baseline)
        try realm.write { note.text = "mine"; note.refreshChangeMetadata(explicitlyModified: true) }
        let incoming = try XCTUnwrap(baseline.records.first?.copy() as? CKRecord)
        incoming["isDone"] = true as CKRecordValue
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(note.text, "mine")
        XCTAssertEqual(note.isDone, true)
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testSameTextCollisionPreservesLosingVersionOnce() async throws {
        let (adapter, realm) = try await fixture()
        let note = ContractRecoveryNote()
        try realm.write { realm.add(note); note.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 10)) }
        try await adapter.didFinishImport()
        let baseline = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(baseline.records, from: baseline)
        try realm.write { note.text = "mine"; note.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20)) }
        let incoming = try XCTUnwrap(baseline.records.first?.copy() as? CKRecord)
        incoming["text"] = "theirs" as CKRecordValue
        incoming["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 30) as CKRecordValue
        incoming["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 30) as CKRecordValue
        _ = try await deliver([incoming], to: adapter)
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(Set(realm.objects(ContractRecoveryNote.self).map(\.text)), ["mine", "theirs"])
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 2)
    }

    @BigSyncBackgroundActor
    func testEvidenceTableDoesNotAdoptOrdinaryModels() async throws {
        let (adapter, realm) = try await fixture()
        let object = UnadoptedContractRow()
        try realm.write { realm.add(object); object.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let saves = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(saves.records.count, 1)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        try await adapter.acknowledgeUploadedRecords(saves.records, from: saves)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    func testAtomicGroupsCannotProduceAMixedSnapshot() throws {
        func fields(_ a: String, _ b: String) -> [String: Data] { ["a": Data(a.utf8), "b": Data(b.utf8)] }
        let contract = BigSyncRecordContract(policy: .independentFields, atomicFieldGroups: [["a", "b"]])
        let result = try BigSyncRecordReconciliationPlanner.plan(base: fields("0", "0"),
            local: fields("1", "0"), remote: fields("0", "2"), policy: contract.policy,
            contract: contract, pending: true, existing: true, localDeleted: false, remoteDeleted: false,
            localLifetime: nil, remoteLifetime: nil, preferRemote: true)
        guard case let .commit(transition) = result else { return XCTFail("Expected complete group") }
        XCTAssertEqual(transition.incomingFields, ["a", "b"])
    }
    @BigSyncBackgroundActor
    func testAdoptedInboundCannotSilentlyUseWrongMergePolicy() async throws {
        let (adapter, realm) = try await fixture()
        adapter.mergePolicy = .server
        do {
            _ = try await deliver([record(adapter)], to: adapter)
            XCTFail("An adopted model cannot silently use legacy merging")
        } catch BigSyncRecordContractError.invalidDeclaration { }
        XCTAssertTrue(realm.objects(RetainedContractRow.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testCanonicalContractValidationRequiresEveryEvidenceTable() async throws {
        let (_, realm) = try await fixture()
        try BigSyncRecordContract.validate(configuration: realm.configuration)
        var partial = realm.configuration
        partial.objectTypes = partial.objectTypes?.filter { $0.className() != BigSyncRecordSubmission.className() }
        XCTAssertThrowsError(try BigSyncRecordContract.validate(configuration: partial))
    }

    @BigSyncBackgroundActor
    func testBoundPendingPredecessorAcceptsOnlyBaselineAndRetiresUncertainty() async throws {
        let (adapter, realm) = try await fixture()
        let control = BoundContractControl()
        try realm.write { realm.add(control); control.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let first = try await adapter.prepareUploadBatch(limit: 10)
        let unbound = try BigSyncRecordPayload.encode(try XCTUnwrap(first.records.first))
        try realm.write { control.digest = "bound"; control.refreshChangeMetadata(explicitlyModified: true) }
        _ = try await deliver([BigSyncRecordPayload.decode(unbound)], to: adapter)
        XCTAssertEqual(control.digest, "bound")
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        let bound = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(bound.records.first?["digest"] as? String, "bound")
        try await adapter.acknowledgeUploadedRecords(bound.records, from: bound)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testRecoveryCopyRedeliveryPreservesUserEditsToTheCopy() async throws {
        let (adapter, realm) = try await fixture()
        let note = ContractRecoveryNote()
        try realm.write { realm.add(note); note.text = "losing"; note.refreshChangeMetadata(explicitlyModified: true) }
        let source = CKRecord(recordType: ContractRecoveryNote.className(),
            recordID: .init(recordName: ContractRecoveryNote.className() + "." + note.id.uuidString,
                            zoneID: adapter.recordZoneID))
        let context = BigSyncRecordRebaseContext(namespace: "test", account: "account", binding: "binding")
        try realm.write {
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        let copy = try XCTUnwrap(realm.objects(ContractRecoveryNote.self).first { $0.id != note.id })
        try realm.write {
            copy.text = "user refined recovery copy"
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        XCTAssertEqual(copy.text, "user refined recovery copy")
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 2)
    }

    @BigSyncBackgroundActor
    func testAcceptedCASSystemFieldsCommitWithTheBaseline() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = record(adapter)
        _ = try await deliver([incoming], to: adapter)
        let row = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let archived = try XCTUnwrap(row.acceptedSystemFields)
        let restored = try BigSyncRecordPayload.record(systemFields: archived)
        XCTAssertEqual(restored.recordID, incoming.recordID)
        XCTAssertEqual(restored.recordType, incoming.recordType)
        XCTAssertEqual(restored.recordChangeTag, row.serverChangeTag)
        XCTAssertTrue(restored.allKeys().isEmpty, "Accepted CAS evidence must not retain payload history")
        let revision = row.revision
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(row.revision, revision)
    }

    @BigSyncBackgroundActor
    func testConflictArchivePruningCannotDiscardUnresolvedWork() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "private local work"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "remote work")], to: adapter)
        let conflict = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        let archive = try adapter.exportPreservedRecordConflicts()
        XCTAssertFalse(archive.isEmpty)
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().count, 1)
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
        XCTAssertEqual(object.title, "private local work")
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertTrue(realm.objects(BigSyncRecordConflict.self).isEmpty)
        XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testUnbasedRecoveryCannotReverseAnOrderedLifetime() async throws {
        let (adapter, realm) = try await fixture()
        let initial = RetainedContractRow()
        let successor = try BigSyncLifetimeID.next(after: initial.epoch)
        try realm.write {
            realm.add(initial)
            initial.title = "local title without an accepted base"
            initial.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, epoch: successor, deleted: true)], to: adapter)
        let conflict = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertEqual(conflict.requiredChoice, .useIncoming)
        XCTAssertEqual(conflict.incomingLifetime, successor)
        XCTAssertTrue(conflict.incomingIsDeleted)
        do {
            try await adapter.resolveRecordConflict(id: conflict.id,
                expectedGeneration: conflict.generation, choice: .keepLocal)
            XCTFail("A record choice cannot roll back the shared lifecycle order")
        } catch BigSyncRecordContractError.staleConflict { }
        XCTAssertEqual(initial.epoch, "E0")
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .useIncoming)
        XCTAssertEqual(initial.epoch, successor)
        XCTAssertTrue(initial.isDeleted)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
        XCTAssertFalse(try adapter.exportPreservedRecordConflicts().isEmpty)
    }

    @BigSyncBackgroundActor
    func testDeletedRecoveryNoteCannotBeRecreatedAfterPhysicalCleanup() async throws {
        let (adapter, realm) = try await fixture()
        let note = ContractRecoveryNote()
        try realm.write { realm.add(note); note.text = "losing"; note.refreshChangeMetadata(explicitlyModified: true) }
        let source = CKRecord(recordType: ContractRecoveryNote.className(),
            recordID: .init(recordName: ContractRecoveryNote.className() + "." + note.id.uuidString,
                            zoneID: adapter.recordZoneID))
        let context = BigSyncRecordRebaseContext(namespace: "test", account: "account", binding: "binding")
        try realm.write {
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        let copy = try XCTUnwrap(realm.objects(ContractRecoveryNote.self).first { $0.id != note.id })
        let receipt = try XCTUnwrap(realm.objects(BigSyncRecordConflict.self).where { $0.isPreservationReceipt }.first)
        let receiptID = receipt.id
        // The source row remains live. Only the user's recovery copy is gone,
        // as it would be after a successful ordinary note-deletion cleanup.
        try realm.write { realm.delete(copy) }
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNotNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: receiptID))
        try realm.write {
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).where { $0.isPreservationReceipt }.count, 1)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
    }


    @BigSyncBackgroundActor
    private final class RecoveryAuthority {
        enum Failure: Error { case revoked }
        var calls = 0
        let revokeAtTransaction: Bool
        let revokeOnValidation: Int
        init(revokeAtTransaction: Bool = true, revokeOnValidation: Int = 2) {
            self.revokeAtTransaction = revokeAtTransaction
            self.revokeOnValidation = revokeOnValidation
        }
        func validate() throws {
            calls += 1
            if revokeAtTransaction && calls >= revokeOnValidation { throw Failure.revoked }
        }
    }

    @BigSyncBackgroundActor
    private func unbasedRecoveryFixture() async throws -> (RealmSwiftAdapter, Realm, RetainedContractRow, BigSyncRecordConflictSnapshot) {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "mine"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "theirs")], to: adapter)
        return (adapter, realm, object, try XCTUnwrap(try adapter.unresolvedRecordConflicts().first))
    }

    @BigSyncBackgroundActor
    func testRevokedResolutionAuthorityCannotCommitEitherChoice() async throws {
        for choice: BigSyncRecordConflictChoice in [.keepLocal, .useIncoming] {
            let (adapter, realm, object, conflict) = try await unbasedRecoveryFixture()
            let authority = RecoveryAuthority()
            let pending = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first).generation
            do {
                try await adapter.resolveRecordConflict(id: conflict.id,
                    expectedGeneration: conflict.generation, choice: choice,
                    validateAuthority: { try authority.validate() })
                XCTFail("Authority was revoked after entry and before the target transaction")
            } catch RecoveryAuthority.Failure.revoked { }
            XCTAssertEqual(authority.calls, 2)
            XCTAssertEqual(object.title, "mine")
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
            XCTAssertEqual(try adapter.unresolvedRecordConflicts().map(\.id), [conflict.id])
            XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testRevokedRefreshAuthorityCannotRetireOrReplaceEvidence() async throws {
        let (adapter, realm, object, conflict) = try await unbasedRecoveryFixture()
        try realm.write {
            object.title = "new typing"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let pending = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first).generation
        let authority = RecoveryAuthority()
        do {
            try await adapter.refreshRecordConflict(conflict.id,
                validateAuthority: { try authority.validate() })
            XCTFail("Refreshing evidence must revalidate authority inside its transaction")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertEqual(authority.calls, 2)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().first?.id, conflict.id)
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().first?.generation, conflict.generation)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
        XCTAssertEqual(object.title, "new typing")
    }

    @BigSyncBackgroundActor
    func testRevokedPruneAuthorityCannotDeleteRetainedArchives() async throws {
        let (adapter, realm, _, conflict) = try await unbasedRecoveryFixture()
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal)
        let before = try XCTUnwrap(realm.objects(BigSyncRecordConflict.self).first).localPayload
        let authority = RecoveryAuthority()
        do {
            try await adapter.discardResolvedRecordConflictArchives(
                validateAuthority: { try authority.validate() })
            XCTFail("Archive cleanup must revalidate authority inside its transaction")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertEqual(authority.calls, 2)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).first?.localPayload, before)
        XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testCurrentRecoveryAuthorityCanResolveAndDrain() async throws {
        let (adapter, _, object, conflict) = try await unbasedRecoveryFixture()
        let authority = RecoveryAuthority(revokeAtTransaction: false)
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal,
            validateAuthority: { try authority.validate() })
        XCTAssertGreaterThanOrEqual(authority.calls, 2)
        XCTAssertEqual(object.title, "mine")
        let batch = try await adapter.prepareUploadBatch(limit: 20)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testRevokedPruneCannotRetireCrashPrefixQuarantine() async throws {
        let (adapter, realm, _, conflict) = try await unbasedRecoveryFixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let original = try XCTUnwrap(tracking.objects(BigSyncInboundSemanticQuarantine.self).first)
        let residual = BigSyncInboundSemanticQuarantine(value: original)
        let lineage = residual.lineageID
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal)
        // Reproduce the durable target / unretired tracking crash prefix.
        // This is persisted model state, not a patched implementation.
        try tracking.write { tracking.add(residual, update: .modified) }
        let pending = realm.objects(BigSyncPendingMutation.self).first?.generation
        let authority = RecoveryAuthority()
        do {
            try await adapter.discardResolvedRecordConflictArchives(
                validateAuthority: { try authority.validate() })
            XCTFail("Revoked archive cleanup must not consume quarantine evidence")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
        // A fresh authorized retry must converge, not merely block forever.
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertTrue(realm.objects(BigSyncRecordConflict.self).isEmpty)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
    }

    @BigSyncBackgroundActor
    func testRecoveryAuthorityRevokedAfterTargetCommitPreservesQuarantineForRetry() async throws {
        let (adapter, realm, object, conflict) = try await unbasedRecoveryFixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let lineage = try XCTUnwrap(tracking.objects(BigSyncInboundSemanticQuarantine.self).first).lineageID
        let authority = RecoveryAuthority(revokeOnValidation: 3)
        do {
            try await adapter.resolveRecordConflict(id: conflict.id,
                expectedGeneration: conflict.generation, choice: .keepLocal,
                validateAuthority: { try authority.validate() })
            XCTFail("A later tracking write must not reuse the target write's authority check")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertEqual(object.title, "mine")
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).first?.isResolved, true)
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await requireQuiet(adapter)
    }
}
