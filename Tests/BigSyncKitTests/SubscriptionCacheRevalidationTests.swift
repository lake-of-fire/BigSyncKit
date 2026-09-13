import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

private final class SubscriptionCacheStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

private final class SubscriptionCacheDatabase: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

private actor SubscriptionCacheGate {
    private var released = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !released else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        released = true
        let pending = waiters
        waiters.removeAll()
        for continuation in pending { continuation.resume() }
    }
}

private enum SubscriptionCacheUnexpectedOperation: Error {
    case unrelatedTransport
}

private actor SubscriptionCacheServices: CloudKitSubscriptionStore,
    CloudKitChangeFeed, CloudKitZoneStore, CloudKitRecordStore {
    private var account = "account-a"
    private var subscriptions: [String: CKSubscription] = [:]
    private var lookupGate: SubscriptionCacheGate?
    private var lookupEntered: XCTestExpectation?
    private(set) var lookupCount = 0
    private(set) var saveCount = 0

    func currentAccount() -> String { account }

    func pauseLookup(
        gate: SubscriptionCacheGate,
        entered: XCTestExpectation
    ) {
        lookupGate = gate
        lookupEntered = entered
    }

    func removeSubscription(withID identifier: String) {
        subscriptions.removeValue(forKey: identifier)
    }

    func replaceSubscription(_ subscription: CKSubscription) {
        subscriptions[subscription.subscriptionID] = subscription
    }

    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        lookupCount += 1
        let gate = lookupGate
        lookupGate = nil
        lookupEntered?.fulfill()
        lookupEntered = nil
        await gate?.wait()
        return subscriptions[identifier]
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        saveCount += 1
        subscriptions[subscription.subscriptionID] = subscription
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        subscriptions.removeValue(forKey: identifier)
    }

    nonisolated func databaseChanges(
        since cursor: DatabaseChangeCursor?, resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }

    nonisolated func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }

    nonisolated func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }

    nonisolated func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        throw SubscriptionCacheUnexpectedOperation.unrelatedTransport
    }
}

final class SubscriptionCacheRevalidationTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() -> (CloudKitSynchronizer, SubscriptionCacheServices) {
        let services = SubscriptionCacheServices()
        let nonce = UUID().uuidString
        let synchronizer = CloudKitSynchronizer(
            identifier: "subscription-cache-" + nonce,
            containerIdentifier: "iCloud.test",
            database: SubscriptionCacheDatabase(),
            recordZoneID: CKRecordZone.ID(
                zoneName: "subscription-cache",
                ownerName: CKCurrentUserDefaultName
            ),
            keyValueStore: SubscriptionCacheStore(),
            accountIdentifierProvider: { await services.currentAccount() },
            accountStatusProvider: { .available },
            changeFeed: services,
            subscriptionStore: services,
            zoneStore: services,
            recordStore: services,
            backupDetectionBaseURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("subscription-cache-" + nonce),
            logger: Logger(label: "SubscriptionCacheRevalidationTests")
        )
        return (synchronizer, services)
    }

    @BigSyncBackgroundActor
    func testCachedDatabaseIDRecreatesMissingServerSubscription() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        let initialLookupCount = await services.lookupCount
        let initialSaveCount = await services.saveCount
        XCTAssertEqual(initialLookupCount, 1)
        XCTAssertEqual(initialSaveCount, 1)

        await services.removeSubscription(withID: identifier)
        try await synchronizer.subscribeForChangesInDatabase()

        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(), identifier
        )
        let finalLookupCount = await services.lookupCount
        let finalSaveCount = await services.saveCount
        XCTAssertEqual(finalLookupCount, 2)
        XCTAssertEqual(finalSaveCount, 2)
    }

    @BigSyncBackgroundActor
    func testCachedZoneIDRecreatesMissingServerSubscription() async throws {
        let (synchronizer, services) = fixture()
        let zoneID = synchronizer.recordZoneID
        try await synchronizer.subscribeForChanges(in: zoneID)
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        let initialLookupCount = await services.lookupCount
        let initialSaveCount = await services.saveCount
        XCTAssertEqual(initialLookupCount, 1)
        XCTAssertEqual(initialSaveCount, 1)

        await services.removeSubscription(withID: identifier)
        try await synchronizer.subscribeForChanges(in: zoneID)

        XCTAssertEqual(
            synchronizer.subscriptionID(forRecordZoneID: zoneID), identifier
        )
        let finalLookupCount = await services.lookupCount
        let finalSaveCount = await services.saveCount
        XCTAssertEqual(finalLookupCount, 2)
        XCTAssertEqual(finalSaveCount, 2)
    }

    @BigSyncBackgroundActor
    func testCachedIDStillHonorsLifecycleAttemptFence() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )

        let entered = expectation(description: "cached subscription lookup")
        let gate = SubscriptionCacheGate()
        await services.pauseLookup(gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()

        do {
            try await task.value
            XCTFail("A cached ID cannot bypass lifecycle retirement")
        } catch is CancellationError {
        }
        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(), identifier
        )
        let finalSaveCount = await services.saveCount
        XCTAssertEqual(finalSaveCount, 1)
    }

    @BigSyncBackgroundActor
    func testCachedDatabaseIDRejectsIncompatibleServerSubscription() async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        let incompatible = CKRecordZoneSubscription(
            zoneID: synchronizer.recordZoneID,
            subscriptionID: identifier
        )
        await services.replaceSubscription(incompatible)

        do {
            try await synchronizer.subscribeForChangesInDatabase()
            XCTFail("An incompatible deterministic-ID subscription must fail closed")
        } catch {
        }
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let finalSaveCount = await services.saveCount
        XCTAssertEqual(finalSaveCount, 1)
    }
}
