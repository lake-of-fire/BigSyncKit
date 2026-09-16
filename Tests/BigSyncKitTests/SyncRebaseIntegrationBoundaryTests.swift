import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(RebaseIntegrationBoundaryRow)
private final class RebaseIntegrationBoundaryRow: Object,
    ChangeMetadataRecordable, BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"])
    }

    @Persisted(primaryKey: true) var id = "article"
    @Persisted var epoch = "E0"
    @Persisted var title = "original"
    @Persisted var count = 0
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

/// These exercise the production adapter, not a second merge implementation.
/// The ordinary deletion path and the value-bearing inbound path are tested
/// separately: a CloudKit deleted-record ID does not contain a lifetime.
final class SyncRebaseIntegrationBoundaryTests: XCTestCase {
    private let lowerNonce = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
    private let higherNonce = UUID(uuidString: "ffffffff-ffff-ffff-ffff-ffffffffffff")!

    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let suffix = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "integration-boundary-target-" + suffix
        target.objectTypes = [RebaseIntegrationBoundaryRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        let policy = BigSyncMutationPolicy(excludedClassNames: [])
        policy.install(configurations: [target], mutationJournalIdentityProvider: {
            .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
        })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "integration-boundary-tracking-" + suffix
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: .init(zoneName: "integration-boundary"),
            logger: Logger(label: "SyncRebaseIntegrationBoundaryTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.integration-boundary", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(
        _ adapter: RealmSwiftAdapter, epoch: String, title: String = "original",
        count: Int = 0, deleted: Bool = false, time: TimeInterval = 10
    ) -> CKRecord {
        let result = CKRecord(
            recordType: RebaseIntegrationBoundaryRow.className(),
            recordID: .init(
                recordName: RebaseIntegrationBoundaryRow.className() + ".article",
                zoneID: adapter.recordZoneID
            )
        )
        result["epoch"] = epoch as CKRecordValue
        result["title"] = title as CKRecordValue
        result["count"] = count as CKRecordValue
        result["isDeleted"] = deleted as CKRecordValue
        result["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        result["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        result["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        return result
    }

    @BigSyncBackgroundActor
    private func deliver(_ record: CKRecord, to adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(in: [record])
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }

    private func object(in realm: Realm) throws -> RebaseIntegrationBoundaryRow {
        try XCTUnwrap(realm.object(ofType: RebaseIntegrationBoundaryRow.self, forPrimaryKey: "article"))
    }

    private func pending(in realm: Realm) throws -> BigSyncPendingMutation {
        try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                  forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
    }

    @BigSyncBackgroundActor
    func testPendingLocalDeletionDoesNotMakeAnOtherwiseValidInboundPageFail() async throws {
        let (adapter, realm) = try await fixture()
        let epoch = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: epoch), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 30))
        }
        let generation = try pending(in: realm).generation
        // This edit is older than the local deletion. Deletion invalidates the
        // field base, but that must not break the existing deletion conflict fence.
        try await deliver(record(adapter, epoch: epoch, title: "remote edit", time: 20), to: adapter)
        realm.refresh()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(try pending(in: realm).generation, generation)
        let proof = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                                               forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
        XCTAssertTrue(proof.isComparisonInvalidated)
    }

    @BigSyncBackgroundActor
    func testValueBearingOldLifetimeDeletionCannotDefeatNewerPendingResetByTimestamp() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.epoch = next
            value.count = 0
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 20))
        }
        try await deliver(record(adapter, epoch: old, count: 7, deleted: true, time: 900), to: adapter)
        realm.refresh()
        XCTAssertFalse(value.isDeleted, "A later unrelated clock is not authority over a successor lifetime")
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
        _ = try pending(in: realm)
    }

    @BigSyncBackgroundActor
    func testValueBearingNewerLifetimeDeletionWinsEvenWithOlderRecordTimestamp() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: higherNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: lowerNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.title = "unrelated later title"
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 900))
        }
        try await deliver(record(adapter, epoch: next, count: 0, deleted: true, time: 20), to: adapter)
        realm.refresh()
        XCTAssertTrue(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
        XCTAssertEqual(value.count, 0)
    }

    @BigSyncBackgroundActor
    func testUnknownInitialAncestorStillCannotBeInventedForDifferentPendingValues() async throws {
        let (adapter, realm) = try await fixture()
        let value = RebaseIntegrationBoundaryRow()
        try realm.write {
            realm.add(value)
            value.title = "local creation"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try pending(in: realm).generation
        do {
            try await deliver(record(adapter, epoch: "E0", title: "other creation"), to: adapter)
            XCTFail("An empty cloud migration is not proof of a shared ancestor for simultaneous creations")
        } catch BigSyncRecordRebaseError.missingBaseline {
            // Explicit unresolved input is safer than discarding either value.
        }
        XCTAssertEqual(value.title, "local creation")
        XCTAssertEqual(try pending(in: realm).generation, generation)
    }
}
