import CloudKit
import BigSyncKit
import XCTest

private struct ScriptedCloudKitChangeFeed: CloudKitChangeFeed {
    let databasePage: CloudKitDatabaseChangePage
    let recordPage: CloudKitRecordZoneChangePage

    func databaseChanges(
        since cursor: DatabaseChangeCursor?,
        resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        databasePage
    }

    func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        recordPage
    }
}

final class CloudKitChangeFeedPublicAPITests: XCTestCase {
    func test_publicChangeFeedValues_supportExternalConformerWithoutTestableImport() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "zone", ownerName: "owner")
        let recordID = CKRecord.ID(recordName: "record", zoneID: zoneID)
        let deletion = CloudKitZoneDeletion(zoneID: zoneID, kind: .purged)
        let databaseCursor = DatabaseChangeCursor(serializedData: Data([1]))
        let recordCursor = RecordZoneChangeCursor(serializedData: Data([2]))

        let databasePage = CloudKitDatabaseChangePage(
            cursor: databaseCursor,
            changedZoneIDs: [zoneID],
            deletions: [deletion],
            moreComing: true
        )
        let recordPage = CloudKitRecordZoneChangePage(
            cursor: recordCursor,
            records: [],
            deletedRecordIDs: [recordID],
            moreComing: false
        )
        let feed = ScriptedCloudKitChangeFeed(
            databasePage: databasePage,
            recordPage: recordPage
        )
        let fetchedDatabasePage = try await feed.databaseChanges(
            since: nil,
            resultsLimit: nil
        )
        let fetchedRecordPage = try await feed.recordZoneChanges(
            in: zoneID,
            since: nil,
            desiredKeys: nil,
            resultsLimit: nil
        )

        XCTAssertEqual(deletion.zoneID, zoneID)
        XCTAssertEqual(deletion.kind, .purged)
        XCTAssertEqual(fetchedDatabasePage.cursor, databaseCursor)
        XCTAssertEqual(fetchedDatabasePage.changedZoneIDs, [zoneID])
        XCTAssertEqual(fetchedDatabasePage.deletions.first?.zoneID, zoneID)
        XCTAssertTrue(fetchedDatabasePage.moreComing)
        XCTAssertEqual(fetchedRecordPage.cursor, recordCursor)
        XCTAssertTrue(fetchedRecordPage.records.isEmpty)
        XCTAssertEqual(fetchedRecordPage.deletedRecordIDs, [recordID])
        XCTAssertFalse(fetchedRecordPage.moreComing)
    }
}
