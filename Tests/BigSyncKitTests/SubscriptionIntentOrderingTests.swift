import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

private final class SubscriptionIntentStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]

    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

private final class SubscriptionIntentDatabase: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

private actor SubscriptionIntentGate {
    private var released = false
    private var waiters = [CheckedContinuation<Void, Never>]()

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

private enum SubscriptionIntentUnexpectedOperation: Error {
    case unrelatedTransport
}

private actor SubscriptionIntentServices: CloudKitSubscriptionStore,
    CloudKitChangeFeed, CloudKitZoneStore, CloudKitRecordStore {
    private var subscriptions = [String: CKSubscription]()
    private var saveGate: SubscriptionIntentGate?
    private var saveEntered: XCTestExpectation?
    private var deleteGate: SubscriptionIntentGate?
    private var deleteEntered: XCTestExpectation?
    private(set) var lookupCount = 0
    private(set) var saveCount = 0
    private(set) var deleteCount = 0

    func pauseNextSave(
        gate: SubscriptionIntentGate,
        entered: XCTestExpectation
    ) {
        saveGate = gate
        saveEntered = entered
    }

    func pauseNextDelete(
        gate: SubscriptionIntentGate,
        entered: XCTestExpectation
    ) {
        deleteGate = gate
        deleteEntered = entered
    }

    func containsSubscription(_ identifier: String) -> Bool {
        subscriptions[identifier] != nil
    }

    func onlySubscriptionID() -> String? {
        subscriptions.keys.count == 1 ? subscriptions.keys.first : nil
    }

    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        lookupCount += 1
        return subscriptions[identifier]
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        saveCount += 1
        // Model a remote side effect that has committed before the response is
        // delivered to the caller.
        subscriptions[subscription.subscriptionID] = subscription
        let gate = saveGate
        saveGate = nil
        saveEntered?.fulfill()
        saveEntered = nil
        await gate?.wait()
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        deleteCount += 1
        // Likewise, make deletion visible before delaying the response.
        subscriptions.removeValue(forKey: identifier)
        let gate = deleteGate
        deleteGate = nil
        deleteEntered?.fulfill()
        deleteEntered = nil
        await gate?.wait()
    }

    nonisolated func databaseChanges(
        since cursor: DatabaseChangeCursor?, resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }

    nonisolated func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }

    nonisolated func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }

    nonisolated func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        throw SubscriptionIntentUnexpectedOperation.unrelatedTransport
    }
}

final class SubscriptionIntentOrderingTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() -> (CloudKitSynchronizer, SubscriptionIntentServices) {
        let services = SubscriptionIntentServices()
        let nonce = UUID().uuidString
        let synchronizer = CloudKitSynchronizer(
            identifier: "subscription-intent-" + nonce,
            containerIdentifier: "iCloud.test",
            database: SubscriptionIntentDatabase(),
            recordZoneID: CKRecordZone.ID(
                zoneName: "subscription-intent",
                ownerName: CKCurrentUserDefaultName
            ),
            keyValueStore: SubscriptionIntentStore(),
            accountIdentifierProvider: { "account-a" },
            accountStatusProvider: { .available },
            changeFeed: services,
            subscriptionStore: services,
            zoneStore: services,
            recordStore: services,
            backupDetectionBaseURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("subscription-intent-" + nonce),
            logger: Logger(label: "SubscriptionIntentOrderingTests")
        )
        return (synchronizer, services)
    }

    @BigSyncBackgroundActor
    func testLaterCancelWinsAfterEarlierSubscribeSaveAlreadyCommittedRemotely()
        async throws {
        let (synchronizer, services) = fixture()
        let saveEntered = expectation(description: "subscribe save committed")
        let saveGate = SubscriptionIntentGate()
        await services.pauseNextSave(gate: saveGate, entered: saveEntered)

        let subscribe = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [saveEntered], timeout: 2)
        let onlySubscriptionID = await services.onlySubscriptionID()
        let identifier = try XCTUnwrap(onlySubscriptionID)

        let cancel = Task { @BigSyncBackgroundActor in
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
        }
        // Give an unsafe reentrant implementation enough time to run Cancel
        // while Subscribe is suspended on its already-committed save response.
        try await Task.sleep(for: .milliseconds(100))
        await saveGate.open()

        try await subscribe.value
        try await cancel.value

        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let containsSubscription = await services.containsSubscription(identifier)
        XCTAssertFalse(containsSubscription)
        let saveCount = await services.saveCount
        let deleteCount = await services.deleteCount
        XCTAssertEqual(saveCount, 1)
        XCTAssertEqual(deleteCount, 1)
    }

    @BigSyncBackgroundActor
    func testLaterSubscribeWinsAfterEarlierCancelDeleteAlreadyCommittedRemotely()
        async throws {
        let (synchronizer, services) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )

        let deleteEntered = expectation(description: "cancel delete committed")
        let deleteGate = SubscriptionIntentGate()
        await services.pauseNextDelete(gate: deleteGate, entered: deleteEntered)

        let cancel = Task { @BigSyncBackgroundActor in
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
        }
        await fulfillment(of: [deleteEntered], timeout: 2)

        let subscribe = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        // Without serialization this Subscribe can recreate the server object
        // before the older Cancel response clears local registration.
        try await Task.sleep(for: .milliseconds(100))
        await deleteGate.open()

        try await cancel.value
        try await subscribe.value

        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(), identifier
        )
        let containsSubscription = await services.containsSubscription(identifier)
        XCTAssertTrue(containsSubscription)
        let saveCount = await services.saveCount
        let deleteCount = await services.deleteCount
        XCTAssertEqual(saveCount, 2)
        XCTAssertEqual(deleteCount, 1)
    }

    @BigSyncBackgroundActor
    func testQueuedCallbackIntentCannotReopenAfterLifecycleAttemptRotation()
        async throws {
        let (synchronizer, services) = fixture()
        let saveEntered = expectation(description: "first save committed")
        let saveGate = SubscriptionIntentGate()
        await services.pauseNextSave(gate: saveGate, entered: saveEntered)

        let first = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [saveEntered], timeout: 2)

        let queuedCompletion = expectation(description: "queued completion")
        synchronizer.subscribeForChangesInDatabase { error in
            XCTAssertTrue(error is CancellationError)
            queuedCompletion.fulfill()
        }
        synchronizer.cancelSynchronization()
        await saveGate.open()

        do {
            try await first.value
            XCTFail("The in-flight attempt must retire after lifecycle rotation")
        } catch is CancellationError {
        }
        await fulfillment(of: [queuedCompletion], timeout: 2)

        let lookupCount = await services.lookupCount
        let saveCount = await services.saveCount
        XCTAssertEqual(
            lookupCount, 1,
            "The queued stale intent must fail before issuing another lookup"
        )
        XCTAssertEqual(saveCount, 1)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }
}
