import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(BigSyncNullableLegacyEpochReviewRow)
private final class NullableLegacyEpochReviewRow: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"]),
        deletion: .retained,
        expectedFields: ["epoch", "bodyText", "title", "isDeleted"])
    @Persisted(primaryKey: true) var id = "item"
    @Persisted var epoch: String? = "original"
    @Persisted var bodyText = "original"
    @Persisted var title = "original"
    @Persisted var isDeleted = false
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
}

/// Real Realm/CloudKit companion regressions; not covered by isolated Swift checks.
final class LegacyOptionalLifetimeRealmTests: XCTestCase {
    @BigSyncBackgroundActor
    private lazy var fixtureOwner = RealmAdapterFixtureOwner(testCase: self)

    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var config = Realm.Configuration()
        config.fileURL = nil
        config.inMemoryIdentifier = "nullable-legacy-target-" + nonce
        config.objectTypes = [NullableLegacyEpochReviewRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &config)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [config],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "nullable-legacy-writer",
                      replicaBindingGenerationIdentifier: "nullable-legacy-binding")
            })
        try BigSyncRecordContract.validate(configuration: config)
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = nil
        tracking.inMemoryIdentifier = "nullable-legacy-tracking-" + nonce
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("nullable-legacy-assets-" + nonce)
        fixtureOwner.ownDirectory(directory)
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [config], excludedClassNames: [],
            recordZoneID: .init(zoneName: "nullable-legacy-epoch-review"),
            logger: Logger(label: "NullableLegacyEpochReview"), startSetupTask: false,
            assetDirectoryURL: directory)
        fixtureOwner.own(adapter)
        adapter.forceDataTypeInsteadOfAsset = true
        adapter.mergePolicy = .custom
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "nullable-legacy-account",
            replicaBindingGenerationIdentifier: "nullable-legacy-binding")
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.nullable-legacy-review", databaseScope: .private)
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(_ adapter: RealmSwiftAdapter, epoch: String?, body: String = "original",
                        deleted: Bool = false, time: Double = 10) throws -> CKRecord {
        let row = NullableLegacyEpochReviewRow()
        row.epoch = epoch
        row.bodyText = body
        row.isDeleted = deleted
        row.modifiedAt = Date(timeIntervalSinceReferenceDate: time)
        row.explicitlyModifiedAt = row.modifiedAt
        return try BigSyncRecordPayload.record(from: row, recordID: .init(
            recordName: NullableLegacyEpochReviewRow.className() + ".item",
            zoneID: adapter.recordZoneID))
    }
    @BigSyncBackgroundActor
    private func deliver(_ record: CKRecord, to adapter: RealmSwiftAdapter) async throws {
        _ = try await adapter.saveChanges(in: [record], forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }
    private func object(_ realm: Realm) throws -> NullableLegacyEpochReviewRow {
        try XCTUnwrap(realm.object(ofType: NullableLegacyEpochReviewRow.self, forPrimaryKey: "item"))
    }

    @BigSyncBackgroundActor
    func testNullableContractAndPayloadKeepAbsenceDistinctFromEmpty() async throws {
        let (adapter, _) = try await fixture()
        let absent = try record(adapter, epoch: nil)
        let empty = try record(adapter, epoch: "")
        XCTAssertNil(absent["epoch"])
        XCTAssertEqual(empty["epoch"] as? String, "")
        let decodedAbsent = try XCTUnwrap(adapter.decodedComparisonObject(absent,
            type: NullableLegacyEpochReviewRow.self) as? NullableLegacyEpochReviewRow)
        let decodedEmpty = try XCTUnwrap(adapter.decodedComparisonObject(empty,
            type: NullableLegacyEpochReviewRow.self) as? NullableLegacyEpochReviewRow)
        XCTAssertNil(decodedAbsent.epoch)
        XCTAssertEqual(decodedEmpty.epoch, "")
        XCTAssertNotNil(try BigSyncCompiledRecordContract.compile(decodedAbsent))
        XCTAssertNotEqual(try BigSyncRecordFingerprint.fields(of: decodedAbsent)["epoch"],
                          try BigSyncRecordFingerprint.fields(of: decodedEmpty)["epoch"])
    }

    @BigSyncBackgroundActor
    func testConcurrentAbsentAndEmptyEpochsConvergeWithoutSplittingTombstoneBundle() async throws {
        for reverseClockPreference in [false, true] {
            let (leftAdapter, leftRealm) = try await fixture()
            let (rightAdapter, rightRealm) = try await fixture()
            try await deliver(record(leftAdapter, epoch: "original"), to: leftAdapter)
            try await deliver(record(rightAdapter, epoch: "original"), to: rightAdapter)
            let left = try object(leftRealm), right = try object(rightRealm)
            try leftRealm.write {
                left.epoch = nil
                left.bodyText = "absent-body"
                left.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: reverseClockPreference ? 40 : 30))
            }
            try rightRealm.write {
                right.epoch = ""
                right.bodyText = "empty-body"
                right.isDeleted = true
                right.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: reverseClockPreference ? 30 : 40))
            }
            try await leftAdapter.didFinishImport()
            try await rightAdapter.didFinishImport()
            let leftBatch = try await leftAdapter.prepareUploadBatch(limit: 10)
            let rightBatch = try await rightAdapter.prepareUploadBatch(limit: 10)
            let leftPayload = try XCTUnwrap(leftBatch.records.first)
            let rightPayload = try XCTUnwrap(rightBatch.records.first)
            try await deliver(leftPayload, to: rightAdapter)
            try await deliver(rightPayload, to: leftAdapter)
            leftRealm.refresh(); rightRealm.refresh()
            for row in [left, right] {
                XCTAssertEqual(row.epoch, "")
                XCTAssertEqual(row.bodyText, "empty-body")
                XCTAssertTrue(row.isDeleted)
            }
            for adapter in [leftAdapter, rightAdapter] {
                let pending = try await adapter.prepareUploadBatch(limit: 10)
                if !pending.records.isEmpty {
                    try await adapter.acknowledgeUploadedRecords(pending.records, from: pending)
                }
                try await adapter.cleanUp()
                let deletions = try await adapter.prepareDeletionBatch(limit: 10)
                XCTAssertTrue(deletions.recordIDs.isEmpty)
                XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
            }
            XCTAssertNotNil(leftRealm.object(ofType: NullableLegacyEpochReviewRow.self, forPrimaryKey: "item"))
            XCTAssertNotNil(rightRealm.object(ofType: NullableLegacyEpochReviewRow.self, forPrimaryKey: "item"))
        }
    }

    @BigSyncBackgroundActor
    func testBaseProvenChangedAbsenceStillWinsAgainstUnchangedEmptyEpoch() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(record(adapter, epoch: "", body: "empty-base"), to: adapter)
        let row = try object(realm)
        try realm.write {
            row.title = "independent-local-title"
            row.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 50))
        }
        try await deliver(record(adapter, epoch: nil, body: "changed-absence", time: 20), to: adapter)
        XCTAssertNil(row.epoch)
        XCTAssertEqual(row.bodyText, "changed-absence")
        XCTAssertEqual(row.title, "independent-local-title")
        XCTAssertFalse(row.isDeleted)
    }
}
