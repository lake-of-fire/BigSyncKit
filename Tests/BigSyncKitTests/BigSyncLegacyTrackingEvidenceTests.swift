import CloudKit
import RealmSwift
import XCTest
@testable import BigSyncKit

final class BigSyncLegacyTrackingEvidenceTests: XCTestCase {
    @BigSyncBackgroundActor
    func testInspectionUsesCopiedTrackingRealmAndReturnsSyncedEvidence()
        throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "BigSyncLegacyTrackingEvidenceTests-" + UUID().uuidString,
                isDirectory: true
            )
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: directory) }

        let sourceURL = directory.appendingPathComponent("legacy.realm")
        var configuration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        configuration.fileURL = sourceURL
        let realm = try Realm(configuration: configuration)

        let zoneID = CKRecordZone.ID(zoneName: "ManabiPlatform.v2")
        let syncedRecord = CKRecord(
            recordType: "ArticleReadingProgress",
            recordID: CKRecord.ID(
                recordName: "ArticleReadingProgress.article-a",
                zoneID: zoneID
            )
        )
        syncedRecord[CloudKitSynchronizer.deviceUUIDKey] =
            "released-device-a" as CKRecordValue
        let pendingRecord = CKRecord(
            recordType: "Bookmark",
            recordID: CKRecord.ID(
                recordName: "Bookmark.pending",
                zoneID: zoneID
            )
        )
        try realm.write {
            let synced = SyncedEntity(
                entityType: syncedRecord.recordType,
                identifier: syncedRecord.recordID.recordName,
                state: SyncedEntityState.synced.rawValue
            )
            synced.encodedRecord = QSCoder.shared.data(from: syncedRecord)
            realm.add(synced)
            let pending = SyncedEntity(
                entityType: pendingRecord.recordType,
                identifier: pendingRecord.recordID.recordName,
                state: SyncedEntityState.changed.rawValue
            )
            pending.encodedRecord = QSCoder.shared.data(from: pendingRecord)
            realm.add(pending)
        }

        let sourceSize = try XCTUnwrap(
            FileManager.default.attributesOfItem(
                atPath: sourceURL.path
            )[.size] as? NSNumber
        )
        let evidence = try BigSyncLegacyTrackingEvidence
            .inspectTrackingRealm(at: sourceURL)

        XCTAssertEqual(
            evidence,
            [
                .init(
                    recordName: syncedRecord.recordID.recordName,
                    entityType: syncedRecord.recordType,
                    recordChangeTag: nil,
                    deviceIdentifier: "released-device-a"
                )
            ]
        )
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(
                atPath: sourceURL.path
            )[.size] as? NSNumber,
            sourceSize
        )
    }

    @BigSyncBackgroundActor
    func testMissingTrackingRealmIsNotMembershipEvidence() throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        XCTAssertEqual(
            try BigSyncLegacyTrackingEvidence.inspectTrackingRealm(at: url),
            []
        )
    }

    func testReleasedDeviceIdentifierUsesExactHistoricalNamespace() {
        let suite = "BigSyncLegacyTrackingEvidenceTests."
            + UUID().uuidString
        let defaults = UserDefaults(suiteName: suite)!
        defer { defaults.removePersistentDomain(forName: suite) }

        let key = "iCloud.io.manabi.ManabiPlatform.v2"
            + "-ManabiPlatform-QSCloudKitStoredDeviceUUIDKey"
        defaults.set("device-a", forKey: key)

        XCTAssertEqual(
            BigSyncLegacyTrackingEvidence.storedDeviceIdentifier(
                suiteName: suite,
                containerIdentifier:
                    "iCloud.io.manabi.ManabiPlatform.v2",
                synchronizerIdentifier: "ManabiPlatform"
            ),
            "device-a"
        )
        XCTAssertNil(
            BigSyncLegacyTrackingEvidence.storedDeviceIdentifier(
                suiteName: suite,
                containerIdentifier:
                    "iCloud.io.manabi.ManabiPlatform.v2",
                synchronizerIdentifier: "ManabiPlatform.v3"
            )
        )
    }
}
