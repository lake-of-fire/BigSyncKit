import CloudKit
import Logging
import XCTest
@testable import BigSyncKit

/// Focused RA-1 regression for explicit-port recovery while ordinary writer
/// authority remains poisoned. A pending port is itself the durable gate; a
/// fresh process must be able to activate it after stable account confirmation
/// without minting a live writer lease on that old worker.
final class RA1ExplicitPortPoisonedRecoveryTests: XCTestCase {
    private final class Store: NSObject, KeyValueStore, @unchecked Sendable {
        var values = [String: Any]()
        func object(forKey defaultName: String) -> Any? { values[defaultName] }
        func bool(forKey defaultName: String) -> Bool { values[defaultName] as? Bool ?? false }
        func set(value: Any?, forKey defaultName: String) { values[defaultName] = value }
        func set(boolValue: Bool, forKey defaultName: String) { values[defaultName] = boolValue }
        func removeObject(forKey defaultName: String) { values.removeValue(forKey: defaultName) }
        func synchronize() -> Bool { true }
    }

    private final class Transport: NSObject, CloudKitDatabaseAdapter,
        CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
        CloudKitChangeFeed, @unchecked Sendable {
        var databaseScope: CKDatabase.Scope { .private }
        func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? { nil }
        func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
        func deleteSubscription(withID identifier: CKSubscription.ID) async throws {}
        func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
            CKRecordZone(zoneID: identifier)
        }
        func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
        func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {}
        func modifyRecords(
            saving recordsToSave: [CKRecord],
            deleting recordIDsToDelete: [CKRecord.ID],
            savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
            atomically: Bool
        ) async throws -> CloudKitRecordMutationResults {
            .init(saveResults: [:], deleteResults: [:])
        }
        func databaseChanges(
            since cursor: DatabaseChangeCursor?, resultsLimit: Int?
        ) async throws -> CloudKitDatabaseChangePage {
            .init(
                cursor: .init(serializedData: Data("port-recovery-db".utf8)),
                changedZoneIDs: [], deletions: [], moreComing: false
            )
        }
        func recordZoneChanges(
            in zoneID: CKRecordZone.ID,
            since cursor: RecordZoneChangeCursor?,
            desiredKeys: [CKRecord.FieldKey]?,
            resultsLimit: Int?
        ) async throws -> CloudKitRecordZoneChangePage {
            .init(
                cursor: .init(serializedData: Data("port-recovery-zone".utf8)),
                records: [], deletedRecordIDs: [], moreComing: false
            )
        }
    }

    private actor Identity {
        var value: String
        init(_ value: String) { self.value = value }
        func current() -> String { value }
        func replace(_ value: String) { self.value = value }
    }

    @BigSyncBackgroundActor
    private func makeSynchronizer(
        store: Store,
        identity: Identity,
        identifier: String,
        zoneID: CKRecordZone.ID
    ) -> CloudKitSynchronizer {
        let transport = Transport()
        return CloudKitSynchronizer(
            identifier: identifier,
            containerIdentifier: "iCloud.ra1-explicit-port-recovery",
            database: transport,
            recordZoneID: zoneID,
            keyValueStore: store,
            accountIdentifierProvider: { await identity.current() },
            accountStatusProvider: { .available },
            changeFeed: transport,
            subscriptionStore: transport,
            zoneStore: transport,
            recordStore: transport,
            initialReplicaBindingAdmissionHandler: { _ in },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            logger: Logger(label: "RA1ExplicitPortPoisonedRecoveryTests")
        )
    }

    @BigSyncBackgroundActor
    func testPersistedPendingPortCanActivateFromFreshInitiallyPoisonedSynchronizer()
    async throws {
        let store = Store()
        let identity = Identity("account-a")
        let identifier = "ra1-port-restart-\(UUID().uuidString)"
        let zoneID = CKRecordZone.ID(
            zoneName: "ra1-explicit-port",
            ownerName: CKCurrentUserDefaultName
        )
        let first = makeSynchronizer(
            store: store,
            identity: identity,
            identifier: identifier,
            zoneID: zoneID
        )
        try await first._test_validateSynchronizationAccount()
        await identity.replace("account-b")

        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await first._test_validateSynchronizationAccount()
            return XCTFail("Expected a durable explicit-port requirement")
        } catch BigSyncCloudAccountPortError.required(let pending) {
            requirement = pending
        }

        let reopened = makeSynchronizer(
            store: store,
            identity: identity,
            identifier: identifier,
            zoneID: zoneID
        )
        XCTAssertNil(try reopened.accountScopeLease())
        XCTAssertEqual(try reopened.pendingCloudAccountPortRequirement(), requirement)

        try await reopened.activateCloudAccountPort(requirement)

        XCTAssertNil(try reopened.pendingCloudAccountPortRequirement())
        XCTAssertNil(try reopened.accountScopeLease())
        do {
            _ = try await reopened.synchronize()
            XCTFail("The worker that activated a port must be restarted")
        } catch let error as BigSyncCloudAccountPortError {
            XCTAssertEqual(error, .workerRestartRequired)
        }

        let restarted = makeSynchronizer(
            store: store,
            identity: identity,
            identifier: identifier,
            zoneID: zoneID
        )
        try await restarted._test_validateSynchronizationAccount()
        XCTAssertEqual(
            try restarted.accountScopeLease()?.accountScopeIdentifier,
            requirement.destinationAccountScopeIdentifier
        )
    }

    @BigSyncBackgroundActor
    func testGenerationOnlyPortCommitStillRejectsPoisonAfterCapture() {
        let fence = AccountScopeAuthorityFence()
        let generation = fence.invalidationGenerationSnapshot
        fence.poison()

        var mutated = false
        let result = fence.withInvalidationGeneration(generation) {
            mutated = true
            return true
        }

        XCTAssertNil(result)
        XCTAssertFalse(mutated)
    }

    @BigSyncBackgroundActor
    func testGenerationOnlyPortCommitDoesNotReopenOrdinaryAuthority() {
        let fence = AccountScopeAuthorityFence()
        let generation = fence.invalidationGenerationSnapshot
        XCTAssertTrue(fence.rejectsAuthority)

        var mutated = false
        let result = fence.withInvalidationGeneration(generation) {
            mutated = true
            return true
        }

        XCTAssertEqual(result, true)
        XCTAssertTrue(mutated)
        XCTAssertTrue(fence.rejectsAuthority)
        XCTAssertNil(fence.authorizedInvalidationGenerationSnapshot)
    }
}
