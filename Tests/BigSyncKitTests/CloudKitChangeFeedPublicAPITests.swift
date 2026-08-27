import CloudKit
import BigSyncKit
import XCTest

final class CloudKitChangeFeedPublicAPITests: XCTestCase {
    func test_publicChangeFeedValues_canBeConstructedWithoutTestableImport() {
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

        XCTAssertEqual(deletion.zoneID, zoneID)
        XCTAssertEqual(deletion.kind, .purged)
        XCTAssertEqual(databasePage.cursor, databaseCursor)
        XCTAssertEqual(databasePage.changedZoneIDs, [zoneID])
        XCTAssertEqual(databasePage.deletions.first?.zoneID, zoneID)
        XCTAssertTrue(databasePage.moreComing)
        XCTAssertEqual(recordPage.cursor, recordCursor)
        XCTAssertTrue(recordPage.records.isEmpty)
        XCTAssertEqual(recordPage.deletedRecordIDs, [recordID])
        XCTAssertFalse(recordPage.moreComing)
    }
}
