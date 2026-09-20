import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Exercises the actual synchronizer tail, not a copy of its follow-up flag.
/// The empty transport is deliberately not signed CloudKit/app qualification.
final class DomainPrepublicationFollowUpTests: XCTestCase {
    @BigSyncBackgroundActor
    func testLocalOnlyDeclarationRequestsOneFreshPassBeforeReceipt() async throws {
        let fixture = try makeFixture()
        let synchronizer = fixture.synchronizer
        var consumedRuns = [UUID]()
        var preparedRuns = [UUID]()
        synchronizer.synchronizationWillConsumeServerChangesHandler = {
            consumedRuns.append($0.runID)
        }
        synchronizer.domainPrepublicationHandler = { context in
            preparedRuns.append(context.runID)
            if preparedRuns.count == 1 {
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
                return [.init(code: "test-local-declaration-changed")]
            }
            return []
        }
        let result = try await synchronizer.synchronize()
        XCTAssertEqual(consumedRuns.count, 2)
        XCTAssertEqual(preparedRuns, consumedRuns)
        XCTAssertNotEqual(preparedRuns.first, preparedRuns.last)
        XCTAssertEqual(result.receipt?.runID, preparedRuns.last)
        XCTAssertEqual(result.publicationState, .complete)
        XCTAssertEqual(result.completionScope, .fullSynchronization)
        XCTAssertFalse(synchronizer.synchronizationRequestedWhileRunning)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0,
                       "A local declaration must not manufacture upload work")
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testPreviousPassCannotRequestAnotherRun() async throws {
        let fixture = try makeFixture()
        var previous: CloudKitSynchronizer.PrepublicationBoundaryContext?
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            if let previous {
                XCTAssertThrowsError(try fixture.worker.requestFollowUpSynchronization(after: previous))
                XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            } else {
                previous = context
                XCTAssertTrue(try fixture.worker.requestFollowUpSynchronization(after: context))
            }
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 2)
        let completedContext = try XCTUnwrap(previous)
        XCTAssertThrowsError(try fixture.worker.requestFollowUpSynchronization(after: completedContext))
        XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testDownloadOnlyDoesNotAcquireUploadOrFollowUpAuthority() async throws {
        let fixture = try makeFixture()
        fixture.synchronizer.syncMode = .downloadOnly
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            // Changing configuration does not upgrade this already-running
            // download-only drain's immutable completion scope.
            fixture.synchronizer.syncMode = .sync
            XCTAssertFalse(try fixture.worker.requestFollowUpSynchronization(after: context))
            XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        XCTAssertEqual(result.completionScope, .downloadOnly)
        XCTAssertNil(result.receipt)
        XCTAssertEqual(fixture.transport.recordMutationCount, 0)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    func testReplacedWorkerRejectsPreviousWorkerBoundary() async throws {
        let old = try makeFixture()
        let replacement = try makeFixture(account: "replacement-account")
        var oldContext: CloudKitSynchronizer.PrepublicationBoundaryContext?
        old.synchronizer.domainPrepublicationHandler = { context in
            oldContext = context
            return []
        }
        _ = try await old.synchronizer.synchronize()
        let captured = try XCTUnwrap(oldContext)
        old.worker._test_installSynchronizer(
            replacement.synchronizer, performsAccountAvailabilityPreflight: false
        )
        var passes = 0
        replacement.synchronizer.domainPrepublicationHandler = { current in
            passes += 1
            XCTAssertNotEqual(captured.replicaBindingGenerationIdentifier,
                              current.replicaBindingGenerationIdentifier)
            XCTAssertThrowsError(try old.worker.requestFollowUpSynchronization(after: captured))
            XCTAssertFalse(replacement.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        _ = try await replacement.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        await old.stop()
        await replacement.stop()
    }

    @BigSyncBackgroundActor
    func testCancelledCallerCannotRequestFollowUpFromCurrentBoundary() async throws {
        let fixture = try makeFixture()
        var passes = 0
        fixture.synchronizer.domainPrepublicationHandler = { context in
            passes += 1
            let task = Task { @BigSyncBackgroundActor in
                try fixture.worker.requestFollowUpSynchronization(after: context)
            }
            task.cancel()
            do {
                _ = try await task.value
                XCTFail("A cancelled callback must not request another drain")
            } catch is CancellationError {
            }
            XCTAssertFalse(fixture.synchronizer.synchronizationRequestedWhileRunning)
            return []
        }
        _ = try await fixture.synchronizer.synchronize()
        XCTAssertEqual(passes, 1)
        await fixture.stop()
    }

    @BigSyncBackgroundActor
    private func makeFixture(account: String = "follow-up-account") throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("bigsync-domain-follow-up-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let transport = DomainFollowUpTransport()
        let zone = CKRecordZone.ID(zoneName: "follow-up-\(UUID().uuidString)",
                                   ownerName: CKCurrentUserDefaultName)
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString, containerIdentifier: "iCloud.bigsync.test",
            database: transport, recordZoneID: zone,
            keyValueStore: FileKeyValueStore(fileURL: directory.appendingPathComponent("state.json")),
            accountIdentifierProvider: { account }, accountStatusProvider: { .available },
            backupDetectionBaseURL: directory.appendingPathComponent("sentinel"),
            initialReplicaBindingAdmissionHandler: { _ in },
            accountReplacementPolicy: .localDatasetRebootstrap,
            logger: Logger(label: "DomainPrepublicationFollowUpTests")
        )
        synchronizer.addModelAdapter(DomainFollowUpAdapter(zoneID: zone))
        let worker = BigSyncBackgroundActor()
        worker._test_installSynchronizer(synchronizer, performsAccountAvailabilityPreflight: false)
        let fixture = Fixture(directory: directory, worker: worker,
                              synchronizer: synchronizer, transport: transport)
        addTeardownBlock { @BigSyncBackgroundActor in
            await fixture.stop()
            fixture.removeFiles()
        }
        return fixture
    }

    @BigSyncBackgroundActor
    private struct Fixture {
        let directory: URL
        let worker: BigSyncBackgroundActor
        let synchronizer: CloudKitSynchronizer
        let transport: DomainFollowUpTransport

        func stop() async {
            synchronizer.domainPrepublicationHandler = nil
            synchronizer.synchronizationWillConsumeServerChangesHandler = nil
            await synchronizer.cancelSynchronizationAndWait()
        }

        func removeFiles() {
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

private final class DomainFollowUpTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private(set) var recordMutationCount = 0
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {}
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        CKRecordZone(zoneID: identifier)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {}
    func modifyRecords(saving recordsToSave: [CKRecord], deleting recordIDsToDelete: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool)
        async throws -> CloudKitRecordMutationResults {
        recordMutationCount += 1
        return .init(saveResults: [:], deleteResults: [:])
    }
    func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?)
        async throws -> CloudKitDatabaseChangePage {
        .init(cursor: DatabaseChangeCursor(serializedData: Data("follow-up-db".utf8)),
              changedZoneIDs: [], deletions: [], moreComing: false)
    }
    func recordZoneChanges(in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
                           desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?)
        async throws -> CloudKitRecordZoneChangePage {
        .init(cursor: RecordZoneChangeCursor(serializedData: Data("follow-up-zone".utf8)),
              records: [], deletedRecordIDs: [], moreComing: false)
    }
}

private final class DomainFollowUpAdapter: NSObject, ModelAdapter, ChangeFeedResetMigrating,
    TerminalSynchronizationStateModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    private var rebuilding = false
    init(zoneID: CKRecordZone.ID) { recordZoneID = zoneID }
    var hasChanges: Bool { false }
    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func prepareChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws { rebuilding = true }
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws {}
    func isChangeFeedServerBootstrapActive() async -> Bool { rebuilding }
    func changeFeedResetCompletionIsDurable(accountScopeIdentifier: String, epoch: Int,
                                            mode: ChangeFeedResetMode) async throws -> Bool { !rebuilding }
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int,
                                                 mode: ChangeFeedResetMode) async throws {}
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode)
        async throws { rebuilding = false }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
                               recordID: $0.element.recordID), disposition: .applied)
        }
    }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] {
        recordIDs.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: "DomainFollowUpObject", recordID: $0.element),
                  disposition: .appliedTombstone)
        }
    }
    func persistImportedChanges() async throws {}
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] { [] }
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {}
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(accountScopeIdentifier: String, replicaBindingGenerationIdentifier: String?,
                                          containerIdentifier: String, databaseScope: CKDatabase.Scope) throws -> String? { nil }
    @BigSyncBackgroundActor func changeFeedEpoch() throws -> Int? { nil }
    func didFinishImport() async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor func hasPendingChangesAtTerminalBoundary() throws -> Bool { false }
}
