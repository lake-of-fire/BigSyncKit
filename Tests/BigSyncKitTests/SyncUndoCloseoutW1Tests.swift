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
    let noteID = UUID(uuidString: "A0000000-0000-0000-0000-000000000001")!

    @BigSyncBackgroundActor
    func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("w1-realms-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: directory) }
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [W1ContractNote.self, W1RetainedArticle.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
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
}

enum W1InjectedFailure: Error { case afterTarget }

