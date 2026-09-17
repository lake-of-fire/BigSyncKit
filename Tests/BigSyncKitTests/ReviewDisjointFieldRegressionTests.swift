import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(ReviewDisjointFieldRow)
final class ReviewDisjointFieldRow: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var remoteField = "remote-v0"
    @Persisted var localField = "local-v0"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// Desired behavior, not a characterization of the known stale-base defect.
/// Keep this file byte-identical between the red and green stack tiers.
final class ReviewDisjointFieldRegressionTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "review-disjoint-target-" + nonce
        target.objectTypes = [ReviewDisjointFieldRow.self, BigSyncPendingMutation.self]
        let exclusions = configureReviewDisjointFieldFixture(&target)
        BigSyncMutationPolicy(excludedClassNames: exclusions).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "review-replica", replicaBindingGenerationIdentifier: "review-binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "review-disjoint-tracking-" + nonce
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: exclusions,
            recordZoneID: .init(zoneName: "review-disjoint"),
            logger: Logger(label: "ReviewDisjointFieldRegressionTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "review-account",
            replicaBindingGenerationIdentifier: "review-binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.review-disjoint", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(_ adapter: RealmSwiftAdapter, remote: String = "remote-v0", local: String = "local-v0", time: TimeInterval = 10) -> CKRecord {
        let record = CKRecord(recordType: ReviewDisjointFieldRow.className(), recordID: .init(
            recordName: ReviewDisjointFieldRow.className() + ".row", zoneID: adapter.recordZoneID
        ))
        record["remoteField"] = remote as CKRecordValue
        record["localField"] = local as CKRecordValue
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    @BigSyncBackgroundActor
    private func deliver(_ record: CKRecord, to adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(in: [record], forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }

    @BigSyncBackgroundActor
    private func authorLocalEdit(in realm: Realm) throws -> ReviewDisjointFieldRow {
        let value = try XCTUnwrap(realm.object(ofType: ReviewDisjointFieldRow.self, forPrimaryKey: "row"))
        try realm.write {
            value.localField = "local-v1"
            value.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
        }
        return value
    }

    @BigSyncBackgroundActor
    private func pendingGeneration(in realm: Realm) throws -> String {
        try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                 forPrimaryKey: ReviewDisjointFieldRow.className() + ".row")?.generation)
    }

    @BigSyncBackgroundActor
    func testPendingLocalEditPreservesDisjointIncomingField() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(record(adapter), to: adapter)
        let value = try authorLocalEdit(in: realm)
        try await deliver(record(adapter, remote: "remote-v1", time: 30), to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1", "RED: the untouched field must accept the incoming edit")
        XCTAssertEqual(value.localField, "local-v1", "Local authoring must not be discarded")
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v1", "Do not reupload the stale remote value")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
        XCTAssertEqual(batch.records.count, 1, "Local comparison evidence is not upload work")
    }

    @BigSyncBackgroundActor
    func testDuplicateIncomingDoesNotChurnReconciledPendingGeneration() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(record(adapter), to: adapter)
        let value = try authorLocalEdit(in: realm)
        try await deliver(record(adapter, remote: "remote-v1", time: 30), to: adapter)
        let generation = try pendingGeneration(in: realm)
        try await deliver(record(adapter, remote: "remote-v1", time: 30), to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1")
        XCTAssertEqual(value.localField, "local-v1")
        XCTAssertEqual(try pendingGeneration(in: realm), generation)
    }

    @BigSyncBackgroundActor
    func testOldUploadAcknowledgementCannotEraseDisjointReconciliation() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(record(adapter), to: adapter)
        _ = try authorLocalEdit(in: realm)
        try await adapter.didFinishImport()
        let oldBatch = try await adapter.prepareUploadBatch(limit: 10)
        let oldGeneration = try pendingGeneration(in: realm)
        try await deliver(record(adapter, remote: "remote-v1", time: 30), to: adapter)
        let reconciledGeneration = try pendingGeneration(in: realm)
        XCTAssertNotEqual(reconciledGeneration, oldGeneration)
        try await adapter.acknowledgeUploadedRecords(oldBatch.records, from: oldBatch)
        XCTAssertEqual(try pendingGeneration(in: realm), reconciledGeneration)
        let current = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(current.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v1")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
    }

    @BigSyncBackgroundActor
    func testRemoteBeforeLocalControlPreservesBothFields() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(record(adapter), to: adapter)
        try await deliver(record(adapter, remote: "remote-v1", time: 15), to: adapter)
        _ = try authorLocalEdit(in: realm)
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(batch.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v1")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
    }
}
