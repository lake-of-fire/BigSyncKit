import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

private final class SubscriptionReviewStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

private final class SubscriptionReviewDatabase: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

private actor SubscriptionReviewGate {
    private var released = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    // Deliberately ignores cancellation to model a transport completion that
    // arrives after the operation's caller has already canceled its task.
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

private enum SubscriptionReviewUnexpectedOperation: Error { case unrelatedTransport }

private actor SubscriptionReviewServices: CloudKitSubscriptionStore,
    CloudKitChangeFeed, CloudKitZoneStore, CloudKitRecordStore {
    private var account = "account-a"
    private var deleteError: Error?
    private var replacementAccountOnDelete: String?
    private var subscriptions: [String: CKSubscription] = [:]
    private var lookupGate: SubscriptionReviewGate?
    private var lookupEntered: XCTestExpectation?
    private var saveGate: SubscriptionReviewGate?
    private var saveEntered: XCTestExpectation?
    private(set) var saveCount = 0
    private(set) var deletedIDs: [String] = []

    func currentAccount() -> String { account }

    func configureDeletion(error: Error?, replacementAccount: String? = nil) {
        deleteError = error
        replacementAccountOnDelete = replacementAccount
    }

    func pauseLookup(gate: SubscriptionReviewGate, entered: XCTestExpectation) {
        lookupGate = gate
        lookupEntered = entered
    }

    func pauseSave(gate: SubscriptionReviewGate, entered: XCTestExpectation) {
        saveGate = gate
        saveEntered = entered
    }

    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? {
        lookupEntered?.fulfill()
        await lookupGate?.wait()
        return subscriptions[identifier]
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        saveCount += 1
        subscriptions[subscription.subscriptionID] = subscription
        saveEntered?.fulfill()
        await saveGate?.wait()
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        deletedIDs.append(identifier)
        if let replacementAccountOnDelete { account = replacementAccountOnDelete }
        if let deleteError { throw deleteError }
        subscriptions.removeValue(forKey: identifier)
    }

    nonisolated func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?)
        async throws -> CloudKitDatabaseChangePage {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZoneChanges(
        in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }

    nonisolated func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }

    nonisolated func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }

    nonisolated func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }

    nonisolated func modifyRecords(
        saving records: [CKRecord], deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        throw SubscriptionReviewUnexpectedOperation.unrelatedTransport
    }
}

final class HotfixSubscriptionSafetyTests: XCTestCase {
    @BigSyncBackgroundActor
    private func fixture() -> (CloudKitSynchronizer, SubscriptionReviewServices) {
        let service = SubscriptionReviewServices()
        let nonce = UUID().uuidString
        let synchronizer = CloudKitSynchronizer(
            identifier: "subscription-review-" + nonce,
            containerIdentifier: "iCloud.test",
            database: SubscriptionReviewDatabase(),
            recordZoneID: CKRecordZone.ID(
                zoneName: "subscription-review", ownerName: CKCurrentUserDefaultName
            ),
            keyValueStore: SubscriptionReviewStore(),
            accountIdentifierProvider: { await service.currentAccount() },
            accountStatusProvider: { .available },
            changeFeed: service,
            subscriptionStore: service,
            zoneStore: service,
            recordStore: service,
            backupDetectionBaseURL: FileManager.default.temporaryDirectory
                .appendingPathComponent("subscription-review-" + nonce),
            logger: Logger(label: "HotfixSubscriptionSafetyTests")
        )
        return (synchronizer, service)
    }

    private func partialError(_ failures: [AnyHashable: Error]) -> CKError {
        CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: failures])
    }

    @BigSyncBackgroundActor
    func testAlreadyAbsentDatabaseSubscriptionClearsCachedRegistration() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        await service.configureDeletion(error: CKError(.unknownItem))
        try await synchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let deleted = await service.deletedIDs
        XCTAssertEqual(deleted, [identifier])
    }

    @BigSyncBackgroundActor
    func testAlreadyAbsentZoneSubscriptionClearsCachedRegistration() async throws {
        let (synchronizer, service) = fixture()
        let zone = synchronizer.recordZoneID
        try await synchronizer.subscribeForChanges(in: zone)
        let identifier = try XCTUnwrap(synchronizer.subscriptionID(forRecordZoneID: zone))
        await service.configureDeletion(error: CKError(.unknownItem))
        try await synchronizer.cancelSubscriptionForChanges(in: zone)
        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: zone))
        let deleted = await service.deletedIDs
        XCTAssertEqual(deleted, [identifier])
    }

    @BigSyncBackgroundActor
    func testExactPerItemUnknownSubscriptionIsIdempotentSuccess() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        await service.configureDeletion(error: partialError([identifier: CKError(.unknownItem)]))
        try await synchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testUnrelatedPartialFailureRetainsCachedSubscription() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let failures: [[AnyHashable: Error]] = [
            ["different-subscription": CKError(.unknownItem)],
            [identifier: CKError(.unknownItem), "different-subscription": CKError(.networkFailure)],
            [identifier: CKError(.networkFailure)],
            [:],
        ]
        for failure in failures {
            await service.configureDeletion(error: partialError(failure))
            do {
                try await synchronizer.cancelSubscriptionForChangesInDatabase()
                XCTFail("Only the exact missing subscription may clear cached state")
            } catch let error as CKError {
                XCTAssertEqual(error.code, .partialFailure)
            }
            XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
        }
    }

    @BigSyncBackgroundActor
    func testNetworkFailureRetainsCachedSubscription() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        await service.configureDeletion(error: CKError(.networkFailure))
        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("Network failure is not evidence of deletion")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .networkFailure)
        }
        XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
    }

    @BigSyncBackgroundActor
    func testMissingSubscriptionFromReplacedAccountCannotClearNewerLocalFence() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        await service.configureDeletion(error: CKError(.unknownItem), replacementAccount: "account-b")
        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("An old account's missing item is not authority to clear local state")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }
        XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
    }

    @BigSyncBackgroundActor
    func testCanceledStandaloneLookupCannotIssueSubscriptionSave() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "subscription lookup entered")
        let gate = SubscriptionReviewGate()
        await service.pauseLookup(gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        task.cancel()
        await gate.open()
        do {
            try await task.value
            XCTFail("A canceled lookup cannot proceed to save")
        } catch is CancellationError {
        }
        let saveCount = await service.saveCount
        XCTAssertEqual(saveCount, 0)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testCanceledStandaloneSaveCannotPublishCachedSubscription() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "subscription save entered")
        let gate = SubscriptionReviewGate()
        await service.pauseSave(gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChanges(in: synchronizer.recordZoneID)
        }
        await fulfillment(of: [entered], timeout: 2)
        task.cancel()
        await gate.open()
        do {
            try await task.value
            XCTFail("A canceled save response cannot publish local success")
        } catch is CancellationError {
        }
        let saveCount = await service.saveCount
        XCTAssertEqual(saveCount, 1, "The server side effect is possible; local publication is fenced")
        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: synchronizer.recordZoneID))
        // A later uncanceled caller can discover the deterministic server ID;
        // cancellation need not create an orphan or a duplicate subscription.
        try await synchronizer.subscribeForChanges(in: synchronizer.recordZoneID)
        XCTAssertNotNil(synchronizer.subscriptionID(forRecordZoneID: synchronizer.recordZoneID))
        let finalSaveCount = await service.saveCount
        XCTAssertEqual(finalSaveCount, 1)
    }
}
