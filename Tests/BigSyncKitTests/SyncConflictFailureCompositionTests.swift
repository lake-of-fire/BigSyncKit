import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

private enum ConflictProcessingPhase { case apply, persist, cancellation }

/// A protocol fake for local import failures. It is intentionally not Realm:
/// deterministic storage faults belong at the existing adapter boundary.
private final class ConflictFailureAdapter: NSObject, ModelAdapter, ChangeFeedResetMigrating, @unchecked Sendable {
    let recordZoneID = CKRecordZone.ID(zoneName: "local-conflict-failure")
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .custom
    var hasChanges: Bool { !pending.isEmpty }
    let phase: ConflictProcessingPhase
    private(set) var pending = Set(["conflict", "success", "other"])
    private(set) var acknowledged: [String] = []
    private var didAttemptConflict = false
    let localFailure = NSError(domain: "TestLocalImportFailure", code: 17)

    init(_ phase: ConflictProcessingPhase) { self.phase = phase }
    // Satisfy the same topology admission as production, but reject any reset:
    // these tests schedule only conflict processing, not bootstrap/migration.
    func isChangeFeedServerBootstrapActive() async -> Bool { false }
    func prepareChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { throw UnexpectedReset() }
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { throw UnexpectedReset() }
    func changeFeedResetCompletionIsDurable(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws -> Bool { throw UnexpectedReset() }
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { throw UnexpectedReset() }
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { throw UnexpectedReset() }
    private struct UnexpectedReset: Error {}
    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: Object) -> Bool { true }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        didAttemptConflict = true
        if phase == .cancellation { throw CancellationError() }
        if phase == .apply { throw localFailure }
        return records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
                               recordID: $0.element.recordID),
                  disposition: .preservedPendingLocal(generation: "pending-conflict"))
        }
    }
    func persistImportedChanges() async throws {
        if didAttemptConflict, phase == .persist { throw localFailure }
    }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        pending.sorted().map { name in
            let record = CKRecord(recordType: "ConflictFixture", recordID: .init(recordName: name, zoneID: recordZoneID))
            record["text"] = "retained work" as CKRecordValue
            return .init(record: record, generation: "pending-" + name)
        }
    }
    @BigSyncBackgroundActor
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {
        for record in savedRecords where matchingGenerations[record.recordID.recordName] == "pending-" + record.recordID.recordName {
            pending.remove(record.recordID.recordName)
            acknowledged.append(record.recordID.recordName)
        }
    }
    @BigSyncBackgroundActor
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func didFinishImport() async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

private final class ConflictDatabaseIdentity: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}
private final class ConflictKeyValueStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}
private actor ConflictFailureTransport: CloudKitRecordStore, CloudKitChangeFeed, CloudKitSubscriptionStore, CloudKitZoneStore {
    let siblingError: CKError?
    private(set) var count = 0
    init(siblingError: CKError?) { self.siblingError = siblingError }
    func modifyRecords(saving records: [CKRecord], deleting: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        count += 1
        var results: [CKRecord.ID: Result<CKRecord, Error>] = [:]
        for record in records {
            switch record.recordID.recordName {
            case "conflict":
                let server = CKRecord(recordType: record.recordType, recordID: record.recordID)
                server["text"] = "server state" as CKRecordValue
                results[record.recordID] = .failure(CKError(.serverRecordChanged, userInfo: [CKRecordChangedErrorServerRecordKey: server]))
            case "other" where siblingError != nil:
                results[record.recordID] = .failure(siblingError!)
            default:
                results[record.recordID] = .success(record)
            }
        }
        return .init(saveResults: results, deleteResults: [:])
    }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage { throw UnexpectedSurface() }
    func recordZoneChanges(in: CKRecordZone.ID, since: RecordZoneChangeCursor?, desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage { throw UnexpectedSurface() }
    func subscription(withID: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID: CKSubscription.ID) async throws {}
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone { CKRecordZone(zoneID: id) }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID: CKRecordZone.ID) async throws { throw UnexpectedSurface() }
    private struct UnexpectedSurface: Error {}
}

final class SyncConflictFailureCompositionTests: XCTestCase {
    @BigSyncBackgroundActor
    private func check(_ phase: ConflictProcessingPhase, sibling: CKError?) async throws {
        let adapter = ConflictFailureAdapter(phase)
        let transport = ConflictFailureTransport(siblingError: sibling)
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: dir) }
        let sync = CloudKitSynchronizer(
            identifier: UUID().uuidString, containerIdentifier: "iCloud.test.local-conflict",
            database: ConflictDatabaseIdentity(), recordZoneID: adapter.recordZoneID,
            keyValueStore: ConflictKeyValueStore(), accountIdentifierProvider: { "account" },
            accountStatusProvider: { .available }, changeFeed: transport,
            subscriptionStore: transport, zoneStore: transport, recordStore: transport,
            backupDetectionBaseURL: dir, logger: Logger(label: "ConflictFailureComposition")
        )
        sync.addModelAdapter(adapter)
        var failure: Error?
        do { try await sync.synchronizeAdapter(adapter) }
        catch { failure = error }
        let error = try XCTUnwrap(failure, "Failed local reconciliation cannot look complete")
        let calls = await transport.count
        XCTAssertEqual(calls, 1, "No local failure may trigger an immediate transport retry")
        XCTAssertTrue(adapter.acknowledged.contains("success"))
        XCTAssertTrue(adapter.pending.contains("conflict"))
        if phase == .cancellation {
            XCTAssertTrue(error is CancellationError, "Cancellation retains its existing authority semantics")
            return
        }
        guard let sibling else {
            XCTAssertEqual((error as NSError).domain, adapter.localFailure.domain)
            XCTAssertEqual((error as NSError).code, adapter.localFailure.code)
            XCTAssertEqual(Set(adapter.acknowledged), ["other", "success"])
            return
        }
        XCTAssertTrue(adapter.pending.contains("other"))
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertTrue(constraints.codes.contains(sibling.code), "Local processing hid a sibling CloudKit constraint")
        if sibling.code == .notAuthenticated || sibling.code == .accountTemporarilyUnavailable {
            XCTAssertTrue(constraints.blocksAccountOperations)
        }
        if sibling.code == .requestRateLimited {
            XCTAssertEqual(constraints.serverMinimum, 137)
            XCTAssertTrue(constraints.requiresDeferredRetry)
        }
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        let cloudError = try XCTUnwrap(error as? CKError)
        let children = try XCTUnwrap(cloudError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let otherID = CKRecord.ID(recordName: "other", zoneID: adapter.recordZoneID)
        XCTAssertEqual(children[otherID]?.code, (sibling as NSError).code)
        let conflictID = CKRecord.ID(recordName: "conflict", zoneID: adapter.recordZoneID)
        XCTAssertEqual(children[conflictID]?.domain, adapter.localFailure.domain)
        XCTAssertNil(children[CKRecord.ID(recordName: "success", zoneID: adapter.recordZoneID)])
    }
    @BigSyncBackgroundActor
    func testApplyFailurePreservesAuthenticationFailure() async throws { try await check(.apply, sibling: CKError(.notAuthenticated)) }
    @BigSyncBackgroundActor
    func testPersistFailurePreservesTemporaryAccountFailure() async throws { try await check(.persist, sibling: CKError(.accountTemporarilyUnavailable)) }
    @BigSyncBackgroundActor
    func testApplyFailurePreservesRetryDeadline() async throws { try await check(.apply, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137])) }
    @BigSyncBackgroundActor
    func testPersistFailurePreservesRetryDeadline() async throws { try await check(.persist, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137])) }
    @BigSyncBackgroundActor
    func testApplyFailureDoesNotBecomeSizeOnlyRetry() async throws { try await check(.apply, sibling: CKError(.limitExceeded)) }
    @BigSyncBackgroundActor
    func testPersistFailurePreservesNetworkFailure() async throws { try await check(.persist, sibling: CKError(.networkFailure)) }
    @BigSyncBackgroundActor
    func testIsolatedLocalFailureKeepsOriginalError() async throws { try await check(.apply, sibling: nil) }
    @BigSyncBackgroundActor
    func testCancellationIsNotWrappedAsRetryablePartialFailure() async throws { try await check(.cancellation, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137])) }
}
