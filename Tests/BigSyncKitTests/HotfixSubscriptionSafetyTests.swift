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
    private var lookupError: Error?
    private var lookupResponse: CKSubscription?
    private var deleteError: Error?
    private var replacementAccountOnDelete: String?
    private var subscriptions: [String: CKSubscription] = [:]
    private var lookupGate: SubscriptionReviewGate?
    private var lookupEntered: XCTestExpectation?
    private var saveGate: SubscriptionReviewGate?
    private var saveEntered: XCTestExpectation?
    private var deleteGate: SubscriptionReviewGate?
    private var deleteEntered: XCTestExpectation?
    private(set) var accountReadCount = 0
    private(set) var accountReadCountAtLastRequest = 0
    private var pausedAccountRead: (
        number: Int, gate: SubscriptionReviewGate, entered: XCTestExpectation
    )?
    private(set) var lookupCount = 0
    private(set) var saveCount = 0
    private(set) var deletedIDs: [String] = []

    func currentAccount() async -> String {
        accountReadCount += 1
        if let pause = pausedAccountRead, pause.number == accountReadCount {
            pausedAccountRead = nil
            pause.entered.fulfill()
            await pause.gate.wait()
        }
        return account
    }

    func pauseAccountRead(
        number: Int, gate: SubscriptionReviewGate, entered: XCTestExpectation
    ) {
        pausedAccountRead = (number, gate, entered)
    }

    func pauseDeletion(gate: SubscriptionReviewGate, entered: XCTestExpectation) {
        deleteGate = gate
        deleteEntered = entered
    }

    func configureDeletion(error: Error?, replacementAccount: String? = nil) {
        deleteError = error
        replacementAccountOnDelete = replacementAccount
    }

    func configureLookup(error: Error? = nil, response: CKSubscription? = nil) {
        lookupError = error
        lookupResponse = response
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
        lookupCount += 1
        accountReadCountAtLastRequest = accountReadCount
        let gate = lookupGate
        lookupGate = nil
        lookupEntered?.fulfill()
        lookupEntered = nil
        await gate?.wait()
        if let lookupError {
            return try await DefaultCloudKitDatabaseAdapter.fetchSubscriptionIfPresent(withID: identifier) {
                throw lookupError
            }
        }
        return lookupResponse ?? subscriptions[identifier]
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        saveCount += 1
        accountReadCountAtLastRequest = accountReadCount
        subscriptions[subscription.subscriptionID] = subscription
        let gate = saveGate
        saveGate = nil
        saveEntered?.fulfill()
        saveEntered = nil
        await gate?.wait()
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        deletedIDs.append(identifier)
        accountReadCountAtLastRequest = accountReadCount
        let gate = deleteGate
        deleteGate = nil
        deleteEntered?.fulfill()
        deleteEntered = nil
        await gate?.wait()
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

    private func partialError(
        _ failures: [AnyHashable: Error], userInfo: [String: Any] = [:]
    ) -> CKError {
        var info = userInfo
        info[CKPartialErrorsByItemIDKey] = failures
        return CKError(.partialFailure, userInfo: info)
    }

    private func missingSubscriptionDetail(userInfo: [String: Any] = [:]) -> NSError {
        NSError(domain: "CKInternalErrorDomain", code: 2003, userInfo: userInfo)
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
            [identifier: partialError(["different-subscription": CKError(.unknownItem)])],
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
    func testConstrainedSubscriptionLookupRetainsCacheWithoutAnotherRequest() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let detail = missingSubscriptionDetail()
        let errors: [Error] = [
            CKError(.unknownItem, userInfo: [CKErrorRetryAfterKey: 60]),
            CKError(.unknownItem, userInfo: [CKErrorRetryAfterKey: "malformed"]),
            CKError(.unknownItem, userInfo: [NSUnderlyingErrorKey: CKError(.notAuthenticated)]),
            CKError(.unknownItem, userInfo: [
                NSMultipleUnderlyingErrorsKey: [
                    detail,
                    partialError(["different-subscription": CKError(.unknownItem)]),
                ] as [Error],
            ]),
            CKError(.unknownItem, userInfo: [
                NSUnderlyingErrorKey: missingSubscriptionDetail(userInfo: [NSUnderlyingErrorKey: "malformed"]),
            ]),
            detail,
        ]

        for lookupError in errors {
            let original = lookupError as NSError
            await service.configureLookup(error: original)
            do {
                try await synchronizer.subscribeForChangesInDatabase()
                XCTFail("An ambiguous lookup cannot authorize a subscription save")
            } catch {
                XCTAssertTrue((error as NSError) === original)
            }
            let subscribeReads = await service.accountReadCount
            let lookupReads = await service.accountReadCountAtLastRequest
            XCTAssertEqual(subscribeReads, lookupReads)

            do {
                try await synchronizer.cancelSubscriptionForChangesInDatabase()
                XCTFail("An ambiguous lookup cannot authorize cancellation")
            } catch {
                XCTAssertTrue((error as NSError) === original)
            }
            let cancelReads = await service.accountReadCount
            let cancelLookupReads = await service.accountReadCountAtLastRequest
            XCTAssertEqual(cancelReads, cancelLookupReads)
            XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
        }
        let saves = await service.saveCount
        let deletions = await service.deletedIDs
        XCTAssertEqual(saves, 1)
        XCTAssertTrue(deletions.isEmpty)
    }

    @BigSyncBackgroundActor
    func testSubscriptionLookupTreatsOnlyTheRequestedMissingItemAsAbsent() async throws {
        let (synchronizer, service) = fixture()
        await service.configureLookup(error: CKError(.unknownItem))
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let foreignMiss = partialError(["different-subscription": CKError(.unknownItem)]) as NSError
        await service.configureLookup(error: foreignMiss)
        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("Another subscription's absence cannot clear this registration")
        } catch {
            XCTAssertTrue((error as NSError) === foreignMiss)
        }
        XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)

        await service.configureLookup(error: partialError([identifier: CKError(.unknownItem)]))
        try await synchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let saves = await service.saveCount
        let deletions = await service.deletedIDs
        XCTAssertEqual(saves, 1)
        XCTAssertTrue(deletions.isEmpty)
    }

    @BigSyncBackgroundActor
    func testSubscriptionLookupAcceptsCloudKitMissingItemDiagnostic() async throws {
        let (synchronizer, service) = fixture()
        let missing = CKError(.unknownItem, userInfo: [NSUnderlyingErrorKey: missingSubscriptionDetail()])
        await service.configureLookup(error: missing)
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())

        await service.configureLookup(error: partialError([identifier: missing]))
        try await synchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let saves = await service.saveCount
        let deletions = await service.deletedIDs
        XCTAssertEqual(saves, 1)
        XCTAssertTrue(deletions.isEmpty)
    }

    @BigSyncBackgroundActor
    func testSubscriptionDeletionAcceptsCloudKitMissingItemDiagnostic() async throws {
        let (synchronizer, service) = fixture()
        let missing = CKError(.unknownItem, userInfo: [NSUnderlyingErrorKey: missingSubscriptionDetail()])
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())

        for deleteError in [missing, partialError([identifier: missing])] {
            try await synchronizer.subscribeForChangesInDatabase()
            await service.configureDeletion(error: deleteError)
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        }
        let deletions = await service.deletedIDs
        XCTAssertEqual(deletions, [identifier, identifier])
    }

    @BigSyncBackgroundActor
    func testConstrainedSubscriptionDeletionRetainsCacheWithoutAnotherAccountRead() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let missing = CKError(.unknownItem)
        let detail = missingSubscriptionDetail()
        let missingWithDetail = CKError(.unknownItem, userInfo: [NSUnderlyingErrorKey: detail])
        let retryInfo = [CKErrorRetryAfterKey: 60]
        let accountInfo = [NSUnderlyingErrorKey: CKError(.accountTemporarilyUnavailable)]
        let errors: [Error] = [
            CKError(.unknownItem, userInfo: retryInfo),
            CKError(.unknownItem, userInfo: accountInfo),
            partialError([identifier: missing], userInfo: retryInfo),
            partialError([identifier: missing], userInfo: accountInfo),
            partialError([identifier: CKError(.unknownItem, userInfo: retryInfo)]),
            partialError([identifier: CKError(.unknownItem, userInfo: accountInfo)]),
            partialError([identifier: missingWithDetail], userInfo: retryInfo),
            partialError([identifier: CKError(.unknownItem, userInfo: [
                NSUnderlyingErrorKey: detail,
                CKErrorRetryAfterKey: 60,
            ])]),
            partialError([identifier: CKError(.unknownItem, userInfo: [
                NSMultipleUnderlyingErrorsKey: [detail, CKError(.accountTemporarilyUnavailable)] as [Error],
            ])]),
            partialError([identifier: CKError(.unknownItem, userInfo: [
                NSUnderlyingErrorKey: missingSubscriptionDetail(userInfo: [CKErrorRetryAfterKey: 60]),
            ])]),
            partialError([identifier: detail]),
        ]

        for deleteError in errors {
            let original = deleteError as NSError
            await service.configureDeletion(error: original)
            do {
                try await synchronizer.cancelSubscriptionForChangesInDatabase()
                XCTFail("A constrained delete cannot clear the subscription registration")
            } catch {
                XCTAssertTrue((error as NSError) === original)
            }
            XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
            let accountReads = await service.accountReadCount
            let deleteReads = await service.accountReadCountAtLastRequest
            XCTAssertEqual(accountReads, deleteReads)
        }
        let deletions = await service.deletedIDs
        XCTAssertEqual(deletions, Array(repeating: identifier, count: errors.count))
    }

    @BigSyncBackgroundActor
    func testSubscriptionLookupRejectsMismatchedResponseID() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let foreign = CKDatabaseSubscription(subscriptionID: "different-subscription")
        let notifications = CKSubscription.NotificationInfo()
        notifications.shouldSendContentAvailable = true
        foreign.notificationInfo = notifications
        await service.configureLookup(response: foreign)

        do {
            try await synchronizer.subscribeForChangesInDatabase()
            XCTFail("The foreign response cannot replace the registered subscription")
        } catch let error as CocoaError {
            XCTAssertEqual(error.code, .coderValueNotFound)
        }
        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("The foreign response cannot authorize deletion")
        } catch let error as CocoaError {
            XCTAssertEqual(error.code, .coderValueNotFound)
        }
        XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
        let saves = await service.saveCount
        let deletions = await service.deletedIDs
        XCTAssertEqual(saves, 1)
        XCTAssertTrue(deletions.isEmpty)
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

    // These calls deliberately do not cancel the caller Task. The public
    // synchronizer lifecycle boundary must retire standalone completions too.
    @BigSyncBackgroundActor
    func testLifecycleCancellationDuringInitialAccountReadStopsTransport() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "initial account read")
        let gate = SubscriptionReviewGate()
        await service.pauseAccountRead(number: 1, gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()
        do {
            try await task.value
            XCTFail("Lifecycle cancellation must retire initial account admission")
        } catch is CancellationError {
        }
        let lookups = await service.lookupCount
        let saves = await service.saveCount
        XCTAssertEqual(lookups, 0)
        XCTAssertEqual(saves, 0)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testLifecycleCancellationDuringLookupCannotStartSave() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "standalone lookup")
        let gate = SubscriptionReviewGate()
        await service.pauseLookup(gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()
        do {
            try await task.value
            XCTFail("A retired standalone lookup cannot start another side effect")
        } catch is CancellationError {
        }
        let saves = await service.saveCount
        XCTAssertEqual(saves, 0)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testLifecycleCancellationDuringSaveFencesPublicationAndAllowsRecovery() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "standalone save")
        let gate = SubscriptionReviewGate()
        await service.pauseSave(gate: gate, entered: entered)
        let zone = synchronizer.recordZoneID
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChanges(in: zone)
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()
        do {
            try await task.value
            XCTFail("A retired save may have committed remotely but cannot publish locally")
        } catch is CancellationError {
        }
        XCTAssertNil(synchronizer.subscriptionID(forRecordZoneID: zone))
        try await synchronizer.subscribeForChanges(in: zone)
        XCTAssertNotNil(synchronizer.subscriptionID(forRecordZoneID: zone))
        let saves = await service.saveCount
        XCTAssertEqual(saves, 1, "The fresh attempt must recover the deterministic server ID")
    }

    @BigSyncBackgroundActor
    func testLifecycleCancellationDuringDeletionRetainsRetryMetadata() async throws {
        let (synchronizer, service) = fixture()
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(synchronizer.subscriptionIDForDatabaseSubscription())
        let entered = expectation(description: "standalone deletion")
        let gate = SubscriptionReviewGate()
        await service.pauseDeletion(gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()
        do {
            try await task.value
            XCTFail("A retired deletion cannot erase current local retry state")
        } catch is CancellationError {
        }
        XCTAssertEqual(synchronizer.subscriptionIDForDatabaseSubscription(), identifier)
        await service.configureDeletion(error: CKError(.unknownItem))
        try await synchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        let deleted = await service.deletedIDs
        // The first delete committed remotely before lifecycle retirement, so
        // the retry's exact ownership lookup observes absence and clears local
        // retry metadata without issuing a redundant second delete.
        XCTAssertEqual(deleted, [identifier])
    }

    @BigSyncBackgroundActor
    func testLifecycleCancellationDuringPostSaveAccountReadFencesPublication() async throws {
        let (synchronizer, service) = fixture()
        let entered = expectation(description: "account read after save")
        let gate = SubscriptionReviewGate()
        // Admission, post-lookup validation, then post-save validation.
        await service.pauseAccountRead(number: 3, gate: gate, entered: entered)
        let task = Task { @BigSyncBackgroundActor in
            try await synchronizer.subscribeForChangesInDatabase()
        }
        await fulfillment(of: [entered], timeout: 2)
        synchronizer.cancelSynchronization()
        await gate.open()
        do {
            try await task.value
            XCTFail("Account equality cannot substitute for the captured lifecycle attempt")
        } catch is CancellationError {
        }
        let saves = await service.saveCount
        XCTAssertEqual(saves, 1)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }
}
