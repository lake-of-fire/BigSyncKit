import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

private enum ReplayProbeError: Error { case unexpectedTransport, terminal }

// Deliberately has no CKContainer or network-backed implementation.
private final class ReplayProbeTransport: NSObject,
    CloudKitDatabaseAdapter, CloudKitChangeFeed,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? {
        throw ReplayProbeError.unexpectedTransport
    }
    func save(subscription: CKSubscription) async throws -> CKSubscription {
        throw ReplayProbeError.unexpectedTransport
    }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        throw ReplayProbeError.unexpectedTransport
    }
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        throw ReplayProbeError.unexpectedTransport
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        throw ReplayProbeError.unexpectedTransport
    }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        throw ReplayProbeError.unexpectedTransport
    }
    func modifyRecords(
        saving recordsToSave: [CKRecord],
        deleting recordIDsToDelete: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        throw ReplayProbeError.unexpectedTransport
    }
    func databaseChanges(
        since cursor: DatabaseChangeCursor?, resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage {
        throw ReplayProbeError.unexpectedTransport
    }
    func recordZoneChanges(
        in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw ReplayProbeError.unexpectedTransport
    }
}

private final class ReplayProbeStore: NSObject, KeyValueStore {
    private var values = [String: Any]()
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

final class RA1InboundReplayTests: XCTestCase {
    @BigSyncBackgroundActor
    private func makeSynchronizer() -> (CloudKitSynchronizer, URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RA1InboundReplay-\(UUID().uuidString)", isDirectory: true)
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.RA1InboundReplay.offline",
            database: ReplayProbeTransport(),
            recordZoneID: CKRecordZone.ID(zoneName: "RA1InboundReplay", ownerName: CKCurrentUserDefaultName),
            keyValueStore: ReplayProbeStore(),
            accountIdentifierProvider: { throw ReplayProbeError.unexpectedTransport },
            accountStatusProvider: { throw ReplayProbeError.unexpectedTransport },
            backupDetectionBaseURL: directory,
            logger: Logger(label: "RA1InboundReplayTests")
        )
        return (synchronizer, directory)
    }

    @BigSyncBackgroundActor
    func testInboundTargetRaceSchedulesBoundedOrdinaryReplay() async throws {
        let (synchronizer, directory) = makeSynchronizer()
        defer {
            synchronizer.cancelSynchronization()
            try? FileManager.default.removeItem(at: directory)
        }
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let attemptID = synchronizer.synchronizationAttemptID
        let started = Date()
        await synchronizer.failSynchronization(error:
            RealmSwiftInboundTargetChangedError(recordName: "RA1ParityRecord.collision"))
        let completed = Date()

        let assertionValue1 = synchronizer.synchronizationAttemptID
        XCTAssertEqual(assertionValue1, attemptID)
        let assertionValue2 = synchronizer.syncing
        XCTAssertTrue(assertionValue2)
        let assertionValue3 = synchronizer.synchronizationDrainIsActive
        XCTAssertTrue(assertionValue3)
        let assertionValue4 = synchronizer.synchronizationTask
        XCTAssertNotNil(assertionValue4)
        let assertionValue5 = synchronizer.retrySleepUntil
        let retryAt = try XCTUnwrap(assertionValue5)
        XCTAssertGreaterThanOrEqual(retryAt, started.addingTimeInterval(1))
        XCTAssertLessThanOrEqual(retryAt, completed.addingTimeInterval(1))
        let assertionValue6 = synchronizer.consecutiveTransientCloudKitFailures
        XCTAssertEqual(assertionValue6, 0)
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testCancellationRevokesScheduledInboundReplay() async throws {
        let (synchronizer, directory) = makeSynchronizer()
        defer { try? FileManager.default.removeItem(at: directory) }
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        await synchronizer.failSynchronization(error:
            RealmSwiftInboundTargetChangedError(recordName: "RA1ParityRecord.collision"))
        let assertionValue7 = synchronizer.retrySleepUntil
        XCTAssertNotNil(assertionValue7)
        await synchronizer.cancelSynchronizationAndWait()
        let cancelledAttempt = synchronizer.synchronizationAttemptID
        try await Task.sleep(nanoseconds: 1_100_000_000)
        let assertionValue8 = synchronizer.synchronizationAttemptID
        XCTAssertEqual(assertionValue8, cancelledAttempt)
        let assertionValue9 = synchronizer.syncing
        XCTAssertFalse(assertionValue9)
        let assertionValue10 = synchronizer.synchronizationDrainIsActive
        XCTAssertFalse(assertionValue10)
        let assertionValue11 = synchronizer.retrySleepUntil
        XCTAssertNil(assertionValue11)
        let assertionValue12 = synchronizer.synchronizationTask
        XCTAssertNil(assertionValue12)
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledRaceDoesNotScheduleReplay() async throws {
        let (synchronizer, directory) = makeSynchronizer()
        defer { try? FileManager.default.removeItem(at: directory) }
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        synchronizer.cancelSync = true
        await synchronizer.failSynchronization(error:
            RealmSwiftInboundTargetChangedError(recordName: "RA1ParityRecord.collision"))
        let assertionValue13 = synchronizer.syncing
        XCTAssertFalse(assertionValue13)
        let assertionValue14 = synchronizer.synchronizationDrainIsActive
        XCTAssertFalse(assertionValue14)
        let assertionValue15 = synchronizer.retrySleepUntil
        XCTAssertNil(assertionValue15)
        let assertionValue16 = synchronizer.synchronizationTask
        XCTAssertNil(assertionValue16)
    }

    @BigSyncBackgroundActor
    func testUnrelatedTerminalFailureDoesNotBecomeReplay() async throws {
        let (synchronizer, directory) = makeSynchronizer()
        defer { try? FileManager.default.removeItem(at: directory) }
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        await synchronizer.failSynchronization(error: ReplayProbeError.terminal)
        let assertionValue17 = synchronizer.syncing
        XCTAssertFalse(assertionValue17)
        let assertionValue18 = synchronizer.synchronizationDrainIsActive
        XCTAssertFalse(assertionValue18)
        let assertionValue19 = synchronizer.retrySleepUntil
        XCTAssertNil(assertionValue19)
        let assertionValue20 = synchronizer.synchronizationTask
        XCTAssertNil(assertionValue20)
    }
}
