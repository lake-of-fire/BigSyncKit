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
    @BigSyncBackgroundActor
    private lazy var realmFixtureOwner = RealmAdapterFixtureOwner(testCase: self)

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
        realmFixtureOwner.own(adapter)
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
        let boundGeneration = try XCTUnwrap(
            realm.objects(BigSyncPendingMutation.self).first?.generation
        )
        let accepted = try await deliver([BigSyncRecordPayload.decode(unbound)], to: adapter)
        XCTAssertEqual(
            accepted.map(\.disposition),
            [.preservedPendingLocal(generation: boundGeneration)]
        )
        XCTAssertEqual(control.digest, "bound")
        XCTAssertEqual(
            realm.objects(BigSyncPendingMutation.self).first?.generation,
            boundGeneration,
            "Accepting server baseline evidence must not relabel the pending semantic extension"
        )
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
    private func unbasedRecoveryFixture(commitPage: Bool = false) async throws -> (RealmSwiftAdapter, Realm, RetainedContractRow, BigSyncRecordConflictSnapshot) {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "mine"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let results = try await deliver([record(adapter, title: "theirs")], to: adapter)
        if commitPage {
            let first = RecordZoneChangeCursor(serializedData: Data("conflict-page".utf8))
            try await adapter.commitInboundPage(.init(previousCursor: nil, nextCursor: first,
                liveResults: results, deletionResults: []))
            // Advance the head so the quarantine's exact receipt is collectible
            // after retirement, rather than protected as the current feed head.
            try await adapter.commitInboundPage(.init(previousCursor: first,
                nextCursor: .init(serializedData: Data("successor-page".utf8)),
                liveResults: [], deletionResults: []))
        }
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

    @BigSyncBackgroundActor
    func testConflictRefreshTracksAcceptedRevisionAfterLateUploadAcknowledgement() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "first submitted version"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let firstUpload = try await adapter.prepareUploadBatch(limit: 10)
        let sent = try XCTUnwrap(firstUpload.records.first)
        // A server response owns its asset bytes independently of temporary
        // upload files retired by an intervening import.
        let saved = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(sent))
        try realm.write {
            object.title = "later local edit"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "independent remote creation")], to: adapter)
        let old = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertNil(realm.objects(BigSyncRecordBaseline.self).first)
        let generation = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first).generation

        // The earlier V1 save is now acknowledged, without acknowledging V2.
        try await adapter.acknowledgeUploadedRecords([saved], from: firstUpload)
        let accepted = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let acceptedRevision = accepted.revision
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertEqual(object.title, "later local edit")
        do {
            try await adapter.resolveRecordConflict(id: old.id,
                expectedGeneration: old.generation, choice: .keepLocal)
            XCTFail("The old displayed comparison revision must remain stale")
        } catch BigSyncRecordContractError.staleConflict { }

        try await adapter.refreshRecordConflict(old.id)
        let refreshed = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertNotEqual(refreshed.id, old.id, "Changed accepted evidence requires a fresh immutable review identity")
        XCTAssertEqual(refreshed.generation, generation, "An acknowledgement need not change newer local intent")
        XCTAssertEqual(realm.object(ofType: BigSyncRecordConflict.self,
            forPrimaryKey: refreshed.id)?.comparisonRevision, acceptedRevision)
        // A second refresh with unchanged evidence must not grow the archive.
        let count = realm.objects(BigSyncRecordConflict.self).count
        try await adapter.refreshRecordConflict(refreshed.id)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, count)
        do {
            try await adapter.resolveRecordConflict(id: refreshed.id,
                expectedGeneration: refreshed.generation, choice: .keepLocal)
        } catch {
            XCTFail("Fresh review must resolve after the accepted baseline changes: \(error)")
            return
        }
        XCTAssertEqual(object.title, "later local edit")
        let final = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(final.records, from: final)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    private final class ResolutionDuringPruning {
        let realm: Realm
        let resolved: BigSyncRecordConflict
        let accepted: BigSyncRecordBaseline
        let pending: BigSyncPendingMutation
        var didCommitResolution = false
        var calls = 0
        init(realm: Realm, resolved: BigSyncRecordConflict,
             accepted: BigSyncRecordBaseline, pending: BigSyncPendingMutation) {
            self.realm = realm
            self.resolved = resolved
            self.accepted = accepted
            self.pending = pending
        }
        func validate() throws {
            calls += 1
            guard calls == 2 else { return }
            // The pruning operation has already selected resolved A and is
            // entering the tracking write on a different Realm. Commit B's
            // complete target resolution now. These rows were recorded from
            // an actual keep-local resolution, not hand-invented evidence.
            // This phase can commit during pruning's asynchronous wait.
            try realm.write {
                realm.add(resolved, update: .modified)
                realm.add(accepted, update: .modified)
                realm.add(pending, update: .modified)
            }
            didCommitResolution = true
        }
    }

    @BigSyncBackgroundActor
    func testArchivePruningCannotConsumeResolutionOutsideItsRetiredSnapshot() async throws {
        let (adapter, realm, _, firstConflict) = try await unbasedRecoveryFixture()
        try await adapter.resolveRecordConflict(id: firstConflict.id,
            expectedGeneration: firstConflict.generation, choice: .keepLocal)
        let second = RetainedContractRow()
        second.id = "second-article"
        let remote = CKRecord(recordType: RetainedContractRow.className(),
            recordID: .init(recordName: RetainedContractRow.className() + "." + second.id,
                            zoneID: adapter.recordZoneID))
        let payload = record(adapter, title: "second incoming")
        for key in payload.allKeys() { remote[key] = payload[key] }
        try realm.write {
            realm.add(second)
            second.title = "second local"
            second.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([remote], to: adapter)
        let secondConflict = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertNotEqual(firstConflict.id, secondConflict.id)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let scope = "record-conflict:" + secondConflict.id
        let quarantine = BigSyncInboundSemanticQuarantine(value: try XCTUnwrap(
            tracking.objects(BigSyncInboundSemanticQuarantine.self)
                .filter("semanticScopeIdentifier == %@", scope).first))
        let lineage = quarantine.lineageID
        let beforeConflict = BigSyncRecordConflict(value: try XCTUnwrap(realm.object(
            ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id)))
        let beforePending = BigSyncPendingMutation(value: try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: secondConflict.recordName)))
        // Record the real target state produced by resolving B. Restore the
        // preceding snapshot, then replay that committed state at the event
        // boundary. The only injected component is event ordering.
        try await adapter.resolveRecordConflict(id: secondConflict.id,
            expectedGeneration: secondConflict.generation, choice: .keepLocal)
        let resolved = BigSyncRecordConflict(value: try XCTUnwrap(realm.object(
            ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id)))
        let accepted = BigSyncRecordBaseline(value: try XCTUnwrap(realm.object(
            ofType: BigSyncRecordBaseline.self, forPrimaryKey: secondConflict.recordName)))
        let pending = BigSyncPendingMutation(value: try XCTUnwrap(realm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: secondConflict.recordName)))
        let generation = pending.generation
        try realm.write {
            realm.delete(try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                                                  forPrimaryKey: secondConflict.recordName)))
            realm.add(beforeConflict, update: .modified)
            realm.add(beforePending, update: .modified)
        }
        try tracking.write { tracking.add(quarantine, update: .modified) }
        let event = ResolutionDuringPruning(realm: realm, resolved: resolved,
                                           accepted: accepted, pending: pending)
        try await adapter.discardResolvedRecordConflictArchives(validateAuthority: { try event.validate() })
        XCTAssertTrue(event.didCommitResolution)
        XCTAssertNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: firstConflict.id))
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage))
        XCTAssertNotNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id),
                        "The resolution is still the only durable authority for retiring its quarantine")
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: secondConflict.recordName)?.generation, generation)
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage),
                     "An authorized retry must retire B before discarding its resolution evidence")
        XCTAssertNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id))
        XCTAssertEqual(second.title, "second local")
    }

}

// Real Realm notification reentry at the public archive-cleanup boundary.
// The signal write changes only fixture-owned local archive metadata; it does
// not fabricate a submitted/accepted record or mutate a pending journal.
private final class CleanupRefreshAuthority: @unchecked Sendable {
    enum Mode: Sendable { case live, revokeLease, cancelTask, cancelAdapterGeneration, replaceAccount }
    enum Failure: Error { case revoked, holdTrackingCleanup }
    let mode: Mode
    private let lock = NSLock()
    private var seeded = false
    private var observed = false
    private var armed = false

    init(_ mode: Mode) { self.mode = mode }
    func armOnce() -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard !seeded else { return false }
        seeded = true; armed = true
        return true
    }
    func receiveChange() {
        lock.lock()
        let shouldObserve = armed && !observed
        if shouldObserve { observed = true }
        lock.unlock()
        if shouldObserve, mode == .cancelTask {
            withUnsafeCurrentTask { $0?.cancel() }
        }
    }
    var didObserve: Bool {
        lock.lock(); defer { lock.unlock() }
        return observed
    }
    func validate() throws {
        if mode == .revokeLease && didObserve { throw Failure.revoked }
    }
}

extension SyncRetainedRecordContractTests {
    @BigSyncBackgroundActor
    private struct CleanupRefreshFixture {
        let adapter: RealmSwiftAdapter
        let target: Realm
        let tracking: Realm
        let conflictID: String
        let lineageID: String
        let pageReceiptID: String
        let originalTitle: String
        let pendingGeneration: String?
    }

    @BigSyncBackgroundActor
    private func cleanupRefreshFixture() async throws -> CleanupRefreshFixture {
        let (adapter, target, object, conflict) = try await unbasedRecoveryFixture(commitPage: true)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let quarantine = try XCTUnwrap(tracking.objects(BigSyncInboundSemanticQuarantine.self).first)
        let lineage = quarantine.lineageID
        let pageReceiptID = quarantine.committedPageReceiptID
        XCTAssertFalse(pageReceiptID.isEmpty)
        XCTAssertEqual(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: pageReceiptID)?.isHead, false)
        do {
            try await adapter.resolveRecordConflict(
                id: conflict.id, expectedGeneration: conflict.generation,
                choice: .keepLocal, validateAuthority: {
                    // Preserve the actual committed target / unretired tracking
                    // prefix. This predicate is tied to state, not call count.
                    if !target.isInWriteTransaction,
                       target.object(ofType: BigSyncRecordConflict.self,
                                     forPrimaryKey: conflict.id)?.isResolved == true {
                        throw CleanupRefreshAuthority.Failure.holdTrackingCleanup
                    }
                }
            )
            XCTFail("Expected the tracking phase to remain pending")
        } catch CleanupRefreshAuthority.Failure.holdTrackingCleanup { }
        XCTAssertTrue(try XCTUnwrap(target.object(
            ofType: BigSyncRecordConflict.self, forPrimaryKey: conflict.id)).isResolved)
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                       forPrimaryKey: lineage))
        return .init(adapter: adapter, target: target, tracking: tracking,
                     conflictID: conflict.id, lineageID: lineage, pageReceiptID: pageReceiptID,
                     originalTitle: object.title,
                     pendingGeneration: target.objects(BigSyncPendingMutation.self).first?.generation)
    }

    @BigSyncBackgroundActor
    private func exerciseCleanupRefresh(
        mode: CleanupRefreshAuthority.Mode, retry: Bool = false
    ) async throws {
        let fixture = try await cleanupRefreshFixture()
        let authority = CleanupRefreshAuthority(mode)
        let writerQueue = DispatchQueue(label: "test.cleanup-refresh." + UUID().uuidString)
        let configuration = fixture.target.configuration
        let conflictID = fixture.conflictID
        let priorAutorefresh = fixture.target.autorefresh
        fixture.target.autorefresh = false
        let observation = fixture.target.observe { notification, _ in
            if case .didChange = notification {
                let previouslyObserved = authority.didObserve
                authority.receiveChange()
                guard !previouslyObserved, authority.didObserve else { return }
                if mode == .cancelAdapterGeneration {
                    fixture.adapter.cancelSynchronization()
                    // Restore the Boolean immediately: rejection must depend
                    // on the captured generation, not merely cancelSync.
                    do { try fixture.adapter.prepareForFencedMigrationAfterCancellation() }
                    catch { XCTFail("Could not restore adapter cancellation flag: \(error)") }
                } else if mode == .replaceAccount {
                    fixture.adapter.activeAccountScopeIdentifier = "replacement-account"
                }
            }
        }
        defer {
            observation.invalidate()
            fixture.target.autorefresh = priorAutorefresh
        }
        let request = Task { @BigSyncBackgroundActor in
            try await fixture.adapter.discardResolvedRecordConflictArchives(validateAuthority: {
                try authority.validate()
                guard fixture.tracking.isInWriteTransaction, authority.armOnce() else { return }
                // The private cleanup has selected its initial candidates and
                // holds the *tracking* writer. A separate scheduler commits the
                // target metadata; the ensuing target refresh delivers the real
                // notification which revokes the caller or cancels this task.
                try writerQueue.sync {
                    let writer = try Realm(configuration: configuration, queue: writerQueue)
                    try writer.write {
                        let row = try XCTUnwrap(writer.object(
                            ofType: BigSyncRecordConflict.self, forPrimaryKey: conflictID))
                        row.createdAt = row.createdAt.addingTimeInterval(1)
                    }
                }
            })
        }
        addTeardownBlock { request.cancel(); _ = await request.result }
        let outcome = await request.result
        XCTAssertTrue(authority.didObserve, "Must exercise real target refresh notification delivery")
        if mode == .live {
            try outcome.get()
            XCTAssertNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                 forPrimaryKey: fixture.lineageID))
            XCTAssertNil(fixture.target.object(ofType: BigSyncRecordConflict.self,
                                               forPrimaryKey: fixture.conflictID))
        } else {
            switch outcome {
            case .success: XCTFail("Revoked cleanup must not publish success")
            case .failure(let error):
                if mode == .cancelTask { XCTAssertTrue(error is CancellationError) }
                else if mode == .revokeLease { XCTAssertTrue(error is CleanupRefreshAuthority.Failure) }
                else { XCTAssertTrue(error is CancellationError) }
            }
            XCTAssertEqual(request.isCancelled, mode == .cancelTask)
            XCTAssertNotNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                    forPrimaryKey: fixture.lineageID))
            XCTAssertNotNil(fixture.tracking.object(ofType: BigSyncInboundPageReceipt.self,
                forPrimaryKey: fixture.pageReceiptID), "Rejected cleanup must retain its exact committed page proof")
            XCTAssertTrue(try XCTUnwrap(fixture.target.object(
                ofType: BigSyncRecordConflict.self, forPrimaryKey: fixture.conflictID)).isResolved,
                "Reject cleanup, not the previously committed target decision")
            if retry {
                observation.invalidate()
                fixture.adapter.activeAccountScopeIdentifier = "account"
                try await fixture.adapter.unsetCancellation()
                try await fixture.adapter.discardResolvedRecordConflictArchives()
                XCTAssertNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                     forPrimaryKey: fixture.lineageID))
                XCTAssertNil(fixture.target.object(ofType: BigSyncRecordConflict.self,
                                                   forPrimaryKey: fixture.conflictID))
            }
        }
        if mode == .live || retry {
            XCTAssertNil(fixture.tracking.object(ofType: BigSyncInboundPageReceipt.self,
                forPrimaryKey: fixture.pageReceiptID))
        }
        XCTAssertNotNil(fixture.tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: BigSyncInboundPageReceipt.canonicalID), "Current feed head remains protected")
        XCTAssertEqual(try value(fixture.target).title, fixture.originalTitle)
        XCTAssertEqual(fixture.target.objects(BigSyncPendingMutation.self).first?.generation,
                       fixture.pendingGeneration)
    }

    @BigSyncBackgroundActor
    func testRefreshRevocationCannotRetireResolvedQuarantineOrArchive() async throws {
        try await exerciseCleanupRefresh(mode: .revokeLease)
    }

    @BigSyncBackgroundActor
    func testRefreshTaskCancellationCannotRetireResolvedQuarantineOrArchive() async throws {
        try await exerciseCleanupRefresh(mode: .cancelTask)
    }

    @BigSyncBackgroundActor
    func testCurrentRefreshCleanupStillRetiresQuarantineAndArchive() async throws {
        try await exerciseCleanupRefresh(mode: .live)
    }

    @BigSyncBackgroundActor
    func testRefreshAdapterGenerationRevocationPreservesPageReceiptAndRetries() async throws {
        try await exerciseCleanupRefresh(mode: .cancelAdapterGeneration, retry: true)
    }

    @BigSyncBackgroundActor
    func testRefreshAccountContextRevocationPreservesPageReceiptAndRetries() async throws {
        try await exerciseCleanupRefresh(mode: .replaceAccount, retry: true)
    }

    @BigSyncBackgroundActor
    func testRefreshRejectedCleanupCanRetryWithoutChangingCommittedDecision() async throws {
        try await exerciseCleanupRefresh(mode: .revokeLease, retry: true)
    }
}

extension SyncRetainedRecordContractTests {
    @BigSyncBackgroundActor
    private func exportedConflictValues(_ adapter: RealmSwiftAdapter) throws -> [[String: Any]] {
        let bytes = try adapter.exportPreservedRecordConflicts()
        let archive = try XCTUnwrap(PropertyListSerialization.propertyList(
            from: bytes, format: nil) as? [String: Any])
        XCTAssertEqual(archive["format"] as? String, "BigSyncPreservedConflicts-v1")
        return try XCTUnwrap(archive["records"] as? [[String: Any]])
    }

    @BigSyncBackgroundActor
    func testConflictPreviewIgnoresAnotherOwnersProvisionalResolution() async throws {
        for commits in [false, true] {
            let (adapter, realm, object, original) = try await unbasedRecoveryFixture()
            let pending = realm.objects(BigSyncPendingMutation.self).first?.generation
            let archived = try XCTUnwrap(realm.object(
                ofType: BigSyncRecordConflict.self, forPrimaryKey: original.id))
            try realm.beginWrite()
            defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
            // This local fixture transition models another owner's pending
            // archive update, not an acknowledged conflict-resolution command.
            archived.isResolved = true
            let during = try adapter.unresolvedRecordConflicts()
            XCTAssertEqual(during.map(\.id), [original.id])
            XCTAssertEqual(during.first?.localTitle, original.localTitle)
            XCTAssertEqual(during.first?.incomingTitle, original.incomingTitle)
            XCTAssertEqual(during.first?.generation, original.generation)
            XCTAssertTrue(realm.isInWriteTransaction)
            if commits { try realm.commitWrite() } else { realm.cancelWrite() }
            let after = try adapter.unresolvedRecordConflicts()
            XCTAssertEqual(after.map(\.id), commits ? [] : [original.id])
            XCTAssertEqual(object.title, "mine")
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
        }
    }

    @BigSyncBackgroundActor
    func testConflictExportExcludesProvisionalPayloadAndRemoval() async throws {
        for removesRow in [false, true] {
            for commits in [false, true] {
                let (adapter, realm, object, original) = try await unbasedRecoveryFixture()
                let before = try XCTUnwrap(try exportedConflictValues(adapter).first)
                let pending = realm.objects(BigSyncPendingMutation.self).first?.generation
                let archived = try XCTUnwrap(realm.object(
                    ofType: BigSyncRecordConflict.self, forPrimaryKey: original.id))
                let provisional = Data("private provisional archive bytes".utf8)
                try realm.beginWrite()
                defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
                if removesRow { realm.delete(archived) }
                else { archived.localPayload = provisional }
                let during = try exportedConflictValues(adapter)
                XCTAssertEqual(during.count, 1)
                XCTAssertEqual(NSDictionary(dictionary: try XCTUnwrap(during.first)),
                               NSDictionary(dictionary: before))
                XCTAssertTrue(realm.isInWriteTransaction, "Export must not settle the held owner")
                if commits { try realm.commitWrite() } else { realm.cancelWrite() }
                let after = try exportedConflictValues(adapter)
                if removesRow && commits {
                    XCTAssertTrue(after.isEmpty)
                } else {
                    XCTAssertEqual(after.first?["localPayload"] as? Data,
                                   commits ? provisional : before["localPayload"] as? Data)
                }
                XCTAssertEqual(object.title, "mine")
                XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
            }
        }
    }
}


extension SyncRetainedRecordContractTests {
    private enum RetainedAcknowledgementRetryGuard { case wrongTag, wrongContext, newerGeneration }

    @BigSyncBackgroundActor
    private func exerciseRetainedAcknowledgementRefresh(
        mode: CleanupRefreshAuthority.Mode,
        retryGuard: RetainedAcknowledgementRetryGuard? = nil
    ) async throws {
        let (adapter, target) = try await fixture()
        _ = try await deliver([record(adapter)], to: adapter)
        let object = try value(target)
        try target.write {
            object.epoch = try BigSyncLifetimeID.next(after: object.epoch, nonce: nonce)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let initialPrepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let recordForDeletion = try XCTUnwrap(initialPrepared.first?.record)
        let results = try await adapter.deleteRecords(with: [recordForDeletion.recordID])
        guard case .quarantined(let lineage) = try XCTUnwrap(results.first).disposition else {
            return XCTFail("A real retained physical deletion must create quarantine evidence")
        }
        let cursor = RecordZoneChangeCursor(serializedData: Data("retained-deletion-page".utf8))
        try await adapter.commitInboundPage(.init(previousCursor: nil, nextCursor: cursor,
            liveResults: [], deletionResults: results))
        try await adapter.commitInboundPage(.init(previousCursor: cursor,
            nextCursor: .init(serializedData: Data("retained-successor-page".utf8)),
            liveResults: [], deletionResults: []))
        // Bind the exact already-committed page before supplying its restoring
        // response. A received old tag alone cannot order later deletions.
        // Server-response bytes must outlive the prepared upload files retired
        // by an intervening didFinishImport in the newer-generation retry.
        func acceptedResponse(_ record: CKRecord, tag: String) throws -> CKRecord {
            let copy = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(record))
            guard copy.responds(to: NSSelectorFromString("setRecordChangeTag:")) else {
                throw CocoaError(.coderValueNotFound)
            }
            _ = copy.perform(NSSelectorFromString("setRecordChangeTag:"), with: tag as NSString)
            let result = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(copy))
            XCTAssertEqual(result.recordChangeTag, tag)
            return result
        }
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let saved = try acceptedResponse(XCTUnwrap(prepared.first).record, tag: "accepted-after-deletion")
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let originalGeneration = try XCTUnwrap(target.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: saved.recordID.recordName)?.generation)
        let originalTrackingState = try XCTUnwrap(tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: saved.recordID.recordName)).entityState
        let receiptID = try XCTUnwrap(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
            forPrimaryKey: lineage)).committedPageReceiptID
        XCTAssertFalse(receiptID.isEmpty)
        XCTAssertEqual(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: receiptID)?.isHead, false)
        var expectedTitle = object.title
        let epoch = object.epoch
        let authority = CleanupRefreshAuthority(mode)
        let writerQueue = DispatchQueue(label: "test.retained-ack-refresh." + UUID().uuidString)
        let configuration = target.configuration
        let priorAutorefresh = target.autorefresh
        target.autorefresh = false
        let observation = target.observe { notification, _ in
            guard case .didChange = notification, !authority.didObserve else { return }
            authority.receiveChange()
            guard authority.didObserve else { return }
            if mode == .cancelAdapterGeneration {
                adapter.cancelSynchronization()
                do { try adapter.prepareForFencedMigrationAfterCancellation() }
                catch { XCTFail("Could not restore cancellation flag: \(error)") }
            } else if mode == .replaceAccount {
                adapter.activeAccountScopeIdentifier = "replacement-account"
            }
        }
        adapter._testAfterAcceptedRetainedDeletionTrackingAdmission = {
            XCTAssertTrue(tracking.isInWriteTransaction)
            XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: saved.recordID.recordName)?.generation, originalGeneration)
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
            guard authority.armOnce() else { return }
            // Tracking acknowledgement is provisional in this transaction.
            // The target journal remains intact until acknowledgement and
            // quarantine retirement commit together. Deliver real refresh
            // notification revocation before that tracking commit.
            try writerQueue.sync {
                let writer = try Realm(configuration: configuration, queue: writerQueue)
                try writer.write {
                    let row = try XCTUnwrap(writer.object(ofType: RetainedContractRow.self,
                        forPrimaryKey: "article"))
                    row.modifiedAt = row.modifiedAt.addingTimeInterval(1)
                }
            }
        }
        defer {
            adapter._testAfterAcceptedRetainedDeletionTrackingAdmission = nil
            observation.invalidate()
            target.autorefresh = priorAutorefresh
        }
        let request = Task { @BigSyncBackgroundActor in
            try await adapter.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
        }
        addTeardownBlock { request.cancel(); _ = await request.result }
        let outcome = await request.result
        switch outcome {
        case .success:
            XCTAssertEqual(mode, .live, "Revoked accepted-deletion cleanup must reject")
        case .failure(let error):
            XCTAssertNotEqual(mode, .live)
            XCTAssertTrue(error is CancellationError)
        }
        XCTAssertEqual(request.isCancelled, mode == .cancelTask)
        XCTAssertTrue(authority.didObserve, "Must deliver a real refresh before tracking settlement")
        if mode == .live {
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
            XCTAssertNil(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.pendingGeneration)
            XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
        } else {
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, originalTrackingState)
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.pendingGeneration, originalGeneration)
            XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: saved.recordID.recordName)?.generation, originalGeneration)
        }
        if mode != .live {
            XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineage))
            XCTAssertNotNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
                forPrimaryKey: receiptID))
            observation.invalidate()
            adapter._testAfterAcceptedRetainedDeletionTrackingAdmission = nil
            adapter.activeAccountScopeIdentifier = "account"
            try await adapter.unsetCancellation()
            // These variants exercise revoked preparation under different
            // reply/local inputs. Current-owner guards are covered separately
            // by the W1 preparation tests; cancellation is the authority here.
            if let retryGuard {
                switch retryGuard {
                case .wrongTag:
                    let mismatched = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(saved))
                    guard mismatched.responds(to: NSSelectorFromString("setRecordChangeTag:")) else {
                        return XCTFail("CloudKit SDK cannot construct tagged system-field fixture")
                    }
                    _ = mismatched.perform(NSSelectorFromString("setRecordChangeTag:"),
                        with: "different-accepted-tag" as NSString)
                    let wrongTag = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(mismatched))
                    XCTAssertEqual(wrongTag.recordChangeTag, "different-accepted-tag")
                    do {
                        try await adapter.didUpload(savedRecords: [wrongTag], matchingPreparedUploads: prepared)
                        XCTFail("A revoked prepared cleanup must not be revived")
                    } catch { XCTAssertTrue(error is CancellationError) }
                case .wrongContext:
                    adapter.activeAccountScopeIdentifier = "replacement-account"
                    do {
                        try await adapter.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
                        XCTFail("A revoked prepared cleanup must not be revived")
                    } catch { XCTAssertTrue(error is CancellationError) }
                    adapter.activeAccountScopeIdentifier = "account"
                case .newerGeneration:
                    try target.write {
                        object.title = "newer retained intent"
                        object.refreshChangeMetadata(explicitlyModified: true)
                    }
                    try await adapter.didFinishImport()
                    let generation = try XCTUnwrap(target.objects(BigSyncPendingMutation.self).first?.generation)
                    do {
                        try await adapter.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
                        XCTFail("A revoked prepared cleanup must not be revived")
                    } catch { XCTAssertTrue(error is CancellationError) }
                    XCTAssertEqual(target.objects(BigSyncPendingMutation.self).first?.generation, generation)
                    XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                        forPrimaryKey: saved.recordID.recordName)?.pendingGeneration, generation)
                    expectedTitle = object.title
                }
                XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                    forPrimaryKey: lineage), "An ineligible retry must preserve quarantine evidence")
                XCTAssertNotNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
                    forPrimaryKey: receiptID))
            }
            // A replaced/cancelled attempt does not renew an old prepared
            // cleanup. Retained journal input permits a fresh preparation and
            // a new simulated server-restoring response in the healthy attempt.
            let current = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
            XCTAssertFalse(current.isEmpty)
            let freshResponses = try current.map {
                try acceptedResponse($0.record, tag: "accepted-current-retry")
            }
            try await adapter.didUpload(savedRecords: freshResponses, matchingPreparedUploads: current)
        }
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
            forPrimaryKey: lineage))
        XCTAssertNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: receiptID))
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: BigSyncInboundPageReceipt.canonicalID))
        XCTAssertTrue(try value(target).isDeleted)
        XCTAssertEqual(try value(target).epoch, epoch)
        XCTAssertEqual(try value(target).title, expectedTitle)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRefreshGenerationRevocationPreservesReceiptsAndRetries() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .cancelAdapterGeneration)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRefreshTaskCancellationPreservesReceiptsAndRetries() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .cancelTask)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRefreshAccountRevocationPreservesReceiptsAndRetries() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .replaceAccount)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRetryRejectsWrongChangeTag() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .cancelAdapterGeneration, retryGuard: .wrongTag)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRetryRejectsWrongContext() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .cancelAdapterGeneration, retryGuard: .wrongContext)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRetryPreservesNewerPendingGeneration() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .cancelAdapterGeneration, retryGuard: .newerGeneration)
    }

    @BigSyncBackgroundActor
    func testCurrentAcceptedRetainedDeletionRefreshRetiresOnlyHistoricalReceipt() async throws {
        try await exerciseRetainedAcknowledgementRefresh(mode: .live)
    }
}

extension SyncRetainedRecordContractTests {
    @BigSyncBackgroundActor
    private func exerciseConcurrentLegacyLifetimeBundles(
        winningEpoch: String, losingEpoch: String
    ) async throws {
        let (leftAdapter, leftRealm) = try await fixture()
        let (rightAdapter, rightRealm) = try await fixture()
        // Both real adapters establish the same comparison ancestor before
        // either authors its independent legacy lifetime transition.
        _ = try await deliver([record(leftAdapter, epoch: "shared-base", count: 7)], to: leftAdapter)
        _ = try await deliver([record(rightAdapter, epoch: "shared-base", count: 7)], to: rightAdapter)
        let left = try value(leftRealm), right = try value(rightRealm)
        let timestamp = Date(timeIntervalSinceReferenceDate: 50)
        try leftRealm.write {
            left.epoch = winningEpoch
            left.count = 0
            left.isDeleted = true
            left.refreshChangeMetadata(explicitlyModified: true, at: timestamp)
        }
        try rightRealm.write {
            right.epoch = losingEpoch
            right.count = 9
            right.isDeleted = false
            right.refreshChangeMetadata(explicitlyModified: true, at: timestamp)
        }
        try await leftAdapter.didFinishImport()
        try await rightAdapter.didFinishImport()
        let leftPrepared = try await leftAdapter.prepareUploadBatch(limit: 10)
        let rightPrepared = try await rightAdapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(leftPrepared.records.count, 1)
        XCTAssertEqual(rightPrepared.records.count, 1)
        func retained(_ record: CKRecord) throws -> CKRecord {
            let bytes = try NSKeyedArchiver.archivedData(withRootObject: record,
                                                       requiringSecureCoding: true)
            let copy = try XCTUnwrap(NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self,
                                                                      from: bytes))
            for key in copy.allKeys() {
                if let asset = copy[key] as? CKAsset {
                    copy[key] = try Data(contentsOf: XCTUnwrap(asset.fileURL)) as NSData
                }
            }
            return copy
        }
        // Preserve exact adapter payloads before the subsequent imports retire
        // their operation-owned asset files. No tags or evidence rows are made.
        let leftIncoming = try retained(XCTUnwrap(leftPrepared.records.first))
        let rightIncoming = try retained(XCTUnwrap(rightPrepared.records.first))
        _ = try await deliver([rightIncoming], to: leftAdapter)
        _ = try await deliver([leftIncoming], to: rightAdapter)
        for object in [left, right] {
            XCTAssertEqual(Data(object.epoch.utf8), Data(winningEpoch.utf8),
                           "Both replicas must select the same literal legacy lifetime")
            XCTAssertTrue(object.isDeleted)
            XCTAssertEqual(object.count, 0, "Deletion and its read-count bundle cannot split across lifetimes")
        }
        let leftCurrent = try await leftAdapter.prepareUploadBatch(limit: 10)
        let rightCurrent = try await rightAdapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(leftCurrent.records.count, 1,
                       "The winning local bundle must still be retransmitted over the accepted losing base")
        XCTAssertLessThanOrEqual(rightCurrent.records.count, 1)
        for batch in [leftCurrent, rightCurrent] {
            for record in batch.records {
                XCTAssertEqual(Data(try XCTUnwrap(record["epoch"] as? String).utf8), Data(winningEpoch.utf8))
                XCTAssertEqual((record["isDeleted"] as? NSNumber)?.boolValue, true)
            }
        }
        try await leftAdapter.acknowledgeUploadedRecords(leftCurrent.records, from: leftCurrent)
        try await rightAdapter.acknowledgeUploadedRecords(rightCurrent.records, from: rightCurrent)
        try await leftAdapter.cleanUp()
        try await rightAdapter.cleanUp()
        try await requireQuiet(leftAdapter)
        try await requireQuiet(rightAdapter)
        XCTAssertTrue(try value(leftRealm).isDeleted)
        XCTAssertTrue(try value(rightRealm).isDeleted)
    }

    @BigSyncBackgroundActor
    func testConcurrentCanonicallyEquivalentLegacyLifetimesConvergeOnOneByteExactDeletionBundle()
    async throws {
        let winning = "legacy-\u{00E9}"
        let losing = "legacy-e\u{0301}"
        XCTAssertEqual(winning, losing)
        XCTAssertNotEqual(Data(winning.utf8), Data(losing.utf8))
        try BigSyncLifetimeID.validate(winning)
        try BigSyncLifetimeID.validate(losing)
        try await exerciseConcurrentLegacyLifetimeBundles(winningEpoch: winning, losingEpoch: losing)
    }

    @BigSyncBackgroundActor
    func testConcurrentASCIILegacyLifetimesKeepExistingDeterministicArbitration() async throws {
        try await exerciseConcurrentLegacyLifetimeBundles(winningEpoch: "legacy-z", losingEpoch: "legacy-a")
    }
}
