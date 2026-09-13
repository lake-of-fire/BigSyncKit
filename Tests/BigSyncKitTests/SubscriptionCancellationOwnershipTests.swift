import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

private final class SubscriptionCancellationStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]

    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

private final class SubscriptionCancellationDatabase: NSObject,
    CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

private enum SubscriptionCancellationUnexpectedOperation: Error {
    case unrelatedTransport
}

private actor SubscriptionCancellationServices: CloudKitSubscriptionStore,
    CloudKitChangeFeed, CloudKitZoneStore, CloudKitRecordStore {
    private var subscriptions: [String: CKSubscription] = [:]
    private(set) var deleteCount = 0
    private(set) var deletedIDs: [String] = []

    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        subscriptions[identifier]
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        subscriptions[subscription.subscriptionID] = subscription
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        deleteCount += 1
        deletedIDs.append(identifier)
        subscriptions.removeValue(forKey: identifier)
    }

    func removeSubscription(withID identifier: String) {
        subscriptions.removeValue(forKey: identifier)
    }

    func replaceSubscription(_ subscription: CKSubscription) {
        subscriptions[subscription.subscriptionID] = subscription
    }

    func containsSubscription(withID identifier: String) -> Bool {
        subscriptions[identifier] != nil
    }

    nonisolated func databaseChanges(
        since cursor: DatabaseChangeCursor?,
        resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }

    nonisolated func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }

    nonisolated func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }

    nonisolated func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        throw SubscriptionCancellationUnexpectedOperation.unrelatedTransport
    }
}

final class SubscriptionCancellationOwnershipTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() -> (
        CloudKitSynchronizer,
        SubscriptionCancellationServices
    ) {
        let services = SubscriptionCancellationServices()
        let nonce = UUID().uuidString
        let synchronizer = CloudKitSynchronizer(
            identifier: "subscription-cancel-" + nonce,
            containerIdentifier: "iCloud.test",
            database: SubscriptionCancellationDatabase(),
            recordZoneID: CKRecordZone.ID(
                zoneName: "subscription-cancel",
                ownerName: CKCurrentUserDefaultName
            ),
            keyValueStore: SubscriptionCancellationStore(),
            accountIdentifierProvider: { "account-a" },
            accountStatusProvider: { .available },
            changeFeed: services,
            subscriptionStore: services,
            zoneStore: services,
            recordStore: services,
            backupDetectionBaseURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("subscription-cancel-" + nonce),
            logger: Logger(label: "SubscriptionCancellationOwnershipTests")
        )
        return (synchronizer, services)
    }

    @BigSyncBackgroundActor
    func testCachedDatabaseIDCannotDeleteIncompatibleServerObject() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        await services.replaceSubscription(
            CKRecordZoneSubscription(
                zoneID: synchronizer.recordZoneID,
                subscriptionID: identifier
            )
        )

        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("An incompatible deterministic-ID object must not be deleted")
        } catch {
        }

        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        XCTAssertEqual(await services.deleteCount, 0)
        XCTAssertTrue(await services.containsSubscription(withID: identifier))
    }

    @BigSyncBackgroundActor
    func testCachedZoneIDCannotDeleteDifferentZoneObject() async throws {
        let (synchronizer, services) = fixture()
        let zoneID = synchronizer.recordZoneID
        try await synchronizer.subscribeForChanges(in: zoneID)
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        await services.replaceSubscription(
            CKRecordZoneSubscription(
                zoneID: CKRecordZone.ID(
                    zoneName: "different-zone",
                    ownerName: CKCurrentUserDefaultName
                ),
                subscriptionID: identifier
            )
        )

        do {
            try await synchronizer.cancelSubscriptionForChanges(in: zoneID)
            XCTFail("A deterministic ID is not authority over another zone")
        } catch {
        }

        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: zoneID))
        XCTAssertEqual(await services.deleteCount, 0)
        XCTAssertTrue(await services.containsSubscription(withID: identifier))
    }

    @BigSyncBackgroundActor
    func testCachedDatabaseIDClearsWhenServerObjectIsAlreadyAbsent() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        await services.removeSubscription(withID: identifier)

        try await synchronizer.cancelSubscriptionForChangesInDatabase()

        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        XCTAssertEqual(await services.deleteCount, 0)
    }

    @BigSyncBackgroundActor
    func testCachedZoneIDClearsWhenServerObjectIsAlreadyAbsent() async throws {
        let (synchronizer, services) = fixture()
        let zoneID = synchronizer.recordZoneID
        try await synchronizer.subscribeForChanges(in: zoneID)
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        await services.removeSubscription(withID: identifier)

        try await synchronizer.cancelSubscriptionForChanges(in: zoneID)

        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: zoneID))
        XCTAssertEqual(await services.deleteCount, 0)
    }

    @BigSyncBackgroundActor
    func testOwnedDatabaseSubscriptionStillDeletesNormally() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )

        try await synchronizer.cancelSubscriptionForChangesInDatabase()

        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        XCTAssertEqual(await services.deletedIDs, [identifier])
        XCTAssertFalse(await services.containsSubscription(withID: identifier))
    }

    @BigSyncBackgroundActor
    func testOwnedZoneSubscriptionStillDeletesNormally() async throws {
        let (synchronizer, services) = fixture()
        let zoneID = synchronizer.recordZoneID
        try await synchronizer.subscribeForChanges(in: zoneID)
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )

        try await synchronizer.cancelSubscriptionForChanges(in: zoneID)

        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: zoneID))
        XCTAssertEqual(await services.deletedIDs, [identifier])
        XCTAssertFalse(await services.containsSubscription(withID: identifier))
    }
}
