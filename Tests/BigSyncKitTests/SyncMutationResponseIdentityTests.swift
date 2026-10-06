import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

private enum ResponseRoute: Sendable {
    case save, saveConflict, saveMissing, deleteConflict, lookup, lookupMissing
    var deletes: Bool { self == .deleteConflict }
    var looksUp: Bool { self == .lookup || self == .lookupMissing }
}
private enum ResponseAlteration: Sendable {
    case none, name, zone, owner, type, siblingIdentity, missingResult, missingConflictRecord
}

/// Intentional protocol fixture: these tests verify dispatch before it reaches
/// an adapter, not Realm mutation semantics. Even an adapter which accepts the
/// input must never receive another requested item's record in this slot.
private final class ResponseIdentityAdapter: NSObject, ModelAdapter, @unchecked Sendable {
    let recordZoneID = CKRecordZone.ID(zoneName: "response-identity")
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .custom
    let route: ResponseRoute
    private(set) var pending: Set<String>
    private(set) var imported = [CKRecord]()
    private(set) var uploaded = [CKRecord]()
    private(set) var deleted = [CKRecord.ID]()
    private(set) var rebased = [CKRecord]()
    private(set) var requeued = [CKRecord.ID]()
    private(set) var persistCount = 0
    var hasChanges: Bool { !pending.isEmpty }
    var acknowledgeFailure: Error?

    init(route: ResponseRoute, siblingFailure: Bool) {
        self.route = route
        pending = siblingFailure ? ["target", "success", "other"] : ["target", "success"]
    }
    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: Object) -> Bool { true }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        imported.append(contentsOf: records)
        if route.looksUp {
            for record in records { pending.remove(record.recordID.recordName) }
        }
        return records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType, recordID: $0.element.recordID),
                  disposition: .preservedPendingLocal(generation: "pending-" + $0.element.recordID.recordName))
        }
    }
    func persistImportedChanges() async throws { persistCount += 1 }
    func didFinishImport() async throws {}
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        guard !route.deletes else { return [] }
        return pending.sorted().map { name in
            let record = CKRecord(recordType: "IdentityFixture", recordID: .init(recordName: name, zoneID: recordZoneID))
            record["text"] = "local-" + name as CKRecordValue
            return .init(record: record, generation: "pending-" + name,
                         comparisonBase: nil, requiresAcceptanceCheck: route.looksUp)
        }
    }
    @BigSyncBackgroundActor
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {
        if let acknowledgeFailure { throw acknowledgeFailure }
        uploaded.append(contentsOf: savedRecords)
        for record in savedRecords { pending.remove(record.recordID.recordName) }
    }
    @BigSyncBackgroundActor
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] {
        guard route.deletes else { return [] }
        return pending.sorted().map { .init(recordID: .init(recordName: $0, zoneID: recordZoneID), generation: "pending-" + $0) }
    }
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {
        if let acknowledgeFailure { throw acknowledgeFailure }
        deleted.append(contentsOf: recordIDs)
        for id in recordIDs { pending.remove(id.recordName) }
    }
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {
        requeued.append(contentsOf: recordIDs)
    }
    @BigSyncBackgroundActor
    func rebasePendingDeletionMetadata(using serverRecords: [CKRecord], matchingPreparedGenerations: [String: String]) async throws {
        rebased.append(contentsOf: serverRecords)
    }
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

private actor ResponseAccountProbe {
    private var received = false
    private(set) var callsAfterResult = 0
    let failsAfterResult: Bool
    init(failsAfterResult: Bool) { self.failsAfterResult = failsAfterResult }
    func didReceive() { received = true }
    func identity() throws -> String {
        if received {
            callsAfterResult += 1
            if failsAfterResult { throw NSError(domain: "ResponseAccountFailure", code: 41) }
        }
        return "identity-account"
    }
}
private actor ResponseIdentityTransport: CloudKitRecordStore, CloudKitRecordFetching,
    CloudKitChangeFeed, CloudKitSubscriptionStore, CloudKitZoneStore {
    let route: ResponseRoute
    let alteration: ResponseAlteration
    let siblingError: CKError?
    let conflictRetryAfter: TimeInterval?
    let repairUnderlyingError: Error?
    let account: ResponseAccountProbe
    private(set) var mutationCount = 0
    private(set) var lookupCount = 0
    init(route: ResponseRoute, alteration: ResponseAlteration, siblingError: CKError?, account: ResponseAccountProbe,
         conflictRetryAfter: TimeInterval?, repairUnderlyingError: Error? = nil) {
        self.route = route; self.alteration = alteration; self.siblingError = siblingError; self.account = account
        self.conflictRetryAfter = conflictRetryAfter
        self.repairUnderlyingError = repairUnderlyingError
    }
    private func conflict(_ id: CKRecord.ID) -> CKError {
        var info: [String: Any] = alteration == .missingConflictRecord ? [:] : [CKRecordChangedErrorServerRecordKey: returned(id)]
        if let conflictRetryAfter { info[CKErrorRetryAfterKey] = conflictRetryAfter }
        if let repairUnderlyingError { info[NSUnderlyingErrorKey] = repairUnderlyingError }
        return CKError(.serverRecordChanged, userInfo: info)
    }
    private func missing() -> CKError {
        var info = [String: Any]()
        if let conflictRetryAfter { info[CKErrorRetryAfterKey] = conflictRetryAfter }
        if let repairUnderlyingError { info[NSUnderlyingErrorKey] = repairUnderlyingError }
        return CKError(.unknownItem, userInfo: info)
    }
    private func returned(_ id: CKRecord.ID, type: String = "IdentityFixture") -> CKRecord {
        let returnedID: CKRecord.ID
        switch alteration {
        case .name: returnedID = .init(recordName: "unrequested", zoneID: id.zoneID)
        case .siblingIdentity: returnedID = .init(recordName: "success", zoneID: id.zoneID)
        case .zone: returnedID = .init(recordName: id.recordName, zoneID: .init(zoneName: "other-zone", ownerName: id.zoneID.ownerName))
        case .owner: returnedID = .init(recordName: id.recordName, zoneID: .init(zoneName: id.zoneID.zoneName, ownerName: "other-owner"))
        default: returnedID = id
        }
        let record = CKRecord(recordType: alteration == .type ? "OtherFixture" : type, recordID: returnedID)
        record["text"] = "server-response" as CKRecordValue
        return record
    }
    func modifyRecords(saving records: [CKRecord], deleting recordIDs: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        mutationCount += 1
        guard mutationCount <= 3 else { throw UnexpectedRetry() }
        var saves = [CKRecord.ID: Result<CKRecord, Error>]()
        var deletes = [CKRecord.ID: Result<Void, Error>]()
        for record in records {
            let id = record.recordID
            if id.recordName == "other", let siblingError { saves[id] = .failure(siblingError); continue }
            guard id.recordName == "target", mutationCount == 1 else { saves[id] = .success(record); continue }
            if alteration == .missingResult { continue }
            if route == .saveMissing {
                saves[id] = .failure(missing())
            } else if route == .saveConflict {
                saves[id] = .failure(conflict(id))
            } else {
                saves[id] = .success(returned(id))
            }
        }
        for id in recordIDs {
            if id.recordName == "other", let siblingError { deletes[id] = .failure(siblingError); continue }
            // unknownItem remains an idempotent success in the deletion lane.
            if id.recordName == "success" { deletes[id] = .failure(CKError(.unknownItem)); continue }
            guard id.recordName == "target", mutationCount == 1 else { deletes[id] = .success(()); continue }
            if alteration == .missingResult { continue }
            deletes[id] = .failure(conflict(id))
        }
        await account.didReceive()
        return .init(saveResults: saves, deleteResults: deletes)
    }
    func fetchRecords(with recordIDs: [CKRecord.ID]) async throws -> [CKRecord.ID: Result<CKRecord, Error>] {
        lookupCount += 1
        guard lookupCount <= 3 else { throw UnexpectedRetry() }
        var results = [CKRecord.ID: Result<CKRecord, Error>]()
        for id in recordIDs {
            if route == .lookupMissing, lookupCount == 1 {
                results[id] = .failure(missing())
                continue
            }
            if id.recordName == "other", let siblingError { results[id] = .failure(siblingError); continue }
            if id.recordName == "target" {
                if alteration == .missingResult { continue }
                results[id] = .success(returned(id))
            } else { results[id] = .success(CKRecord(recordType: "IdentityFixture", recordID: id)) }
        }
        await account.didReceive()
        return results
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
    private struct UnexpectedRetry: Error {}
}
private final class ResponseDatabaseIdentity: NSObject, CloudKitDatabaseAdapter { var databaseScope: CKDatabase.Scope { .private } }
private final class ResponseKeyValueStore: NSObject, KeyValueStore {
    private var values = [String: Any]()
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}
@BigSyncBackgroundActor
private final class ResponseResultCapture { var calls = 0; var error: Error? }

final class SyncMutationResponseIdentityTests: XCTestCase {
    @BigSyncBackgroundActor
    private func run(_ route: ResponseRoute, _ alteration: ResponseAlteration, sibling: CKError? = nil,
                     acknowledgeFailure: Error? = nil, failsAccountAfterResult: Bool = false,
                     conflictRetryAfter: TimeInterval? = nil,
                     repairUnderlyingError: Error? = nil) async throws -> (ResponseIdentityAdapter, ResponseIdentityTransport, ResponseAccountProbe, Error?) {
        let adapter = ResponseIdentityAdapter(route: route, siblingFailure: sibling != nil)
        adapter.acknowledgeFailure = acknowledgeFailure
        let account = ResponseAccountProbe(failsAfterResult: failsAccountAfterResult)
        let transport = ResponseIdentityTransport(route: route, alteration: alteration, siblingError: sibling,
                                                   account: account, conflictRetryAfter: conflictRetryAfter,
                                                   repairUnderlyingError: repairUnderlyingError)
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("response-identity-" + UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: dir) }
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.response-identity", database: ResponseDatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: ResponseKeyValueStore(),
            accountIdentifierProvider: { try await account.identity() }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport, recordStore: transport,
            backupDetectionBaseURL: dir, logger: Logger(label: "ResponseIdentity"))
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: "identity-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "identity-account"))
        let result = ResponseResultCapture()
        if route.deletes {
            try await sync.uploadDeletionsUsingAsyncStore(adapter: adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID) { result.calls += 1; result.error = $0 }
        } else {
            try await sync.uploadRecordsUsingAsyncStore(adapter: adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID) { result.calls += 1; result.error = $0 }
        }
        XCTAssertEqual(result.calls, 1)
        return (adapter, transport, account, result.error)
    }
    @BigSyncBackgroundActor
    private func reject(_ route: ResponseRoute, _ alteration: ResponseAlteration,
                        sibling: CKError? = nil, file: StaticString = #filePath, line: UInt = #line) async throws {
        let (adapter, transport, account, failure) = try await run(route, alteration, sibling: sibling)
        XCTAssertTrue(adapter.pending.contains("target"), file: file, line: line)
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.requeued.isEmpty, file: file, line: line)
        let mutations = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(mutations, route.looksUp ? 0 : 1, file: file, line: line)
        XCTAssertEqual(lookups, route.looksUp ? 1 : 0, file: file, line: line)
        let acknowledged = route.deletes ? adapter.deleted.map(\.recordName) : adapter.uploaded.map { $0.recordID.recordName }
        XCTAssertEqual(acknowledged, route.looksUp ? [] : ["success"], file: file, line: line)
        let error = try XCTUnwrap(failure, "Malformed response must remain a failure", file: file, line: line)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures, file: file, line: line)
        if let sibling {
            XCTAssertTrue(constraints.codes.contains(sibling.code), file: file, line: line)
            if sibling.code == .requestRateLimited {
                XCTAssertEqual(constraints.serverMinimum, 137, file: file, line: line)
                XCTAssertTrue(constraints.requiresDeferredRetry, file: file, line: line)
            }
            if sibling.code == .notAuthenticated || sibling.code == .accountTemporarilyUnavailable {
                let afterResult = await account.callsAfterResult
                XCTAssertEqual(afterResult, 0, file: file, line: line)
                XCTAssertTrue(constraints.blocksAccountOperations, file: file, line: line)
            }
        }
        let bad = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let children = (error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError]
        if route != .lookup {
            XCTAssertNotNil(children?[bad], file: file, line: line)
            XCTAssertNil(children?[.init(recordName: "success", zoneID: adapter.recordZoneID)], file: file, line: line)
            if alteration != .missingResult && alteration != .missingConflictRecord {
                let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
                XCTAssertEqual(children?[bad]?.domain, expected.domain, file: file, line: line)
                XCTAssertEqual(children?[bad]?.code, expected.code, file: file, line: line)
            }
        }
    }
    @BigSyncBackgroundActor
    func testSaveRejectsAnotherRecordName() async throws { try await reject(.save, .name) }
    @BigSyncBackgroundActor
    func testSaveRejectsAnotherZone() async throws { try await reject(.save, .zone) }
    @BigSyncBackgroundActor
    func testSaveRejectsAnotherZoneOwner() async throws { try await reject(.save, .owner) }
    @BigSyncBackgroundActor
    func testSaveRejectsAnotherRecordType() async throws { try await reject(.save, .type) }
    @BigSyncBackgroundActor
    func testSaveCannotBorrowSuccessfulSiblingIdentity() async throws { try await reject(.save, .siblingIdentity) }
    @BigSyncBackgroundActor
    func testUploadConflictRejectsAnotherRecordName() async throws { try await reject(.saveConflict, .name) }
    @BigSyncBackgroundActor
    func testUploadConflictRejectsAnotherZone() async throws { try await reject(.saveConflict, .zone) }
    @BigSyncBackgroundActor
    func testUploadConflictRejectsAnotherZoneOwner() async throws { try await reject(.saveConflict, .owner) }
    @BigSyncBackgroundActor
    func testUploadConflictRejectsAnotherRecordType() async throws { try await reject(.saveConflict, .type) }
    @BigSyncBackgroundActor
    func testUploadConflictCannotBorrowSuccessfulSiblingIdentity() async throws { try await reject(.saveConflict, .siblingIdentity) }
    @BigSyncBackgroundActor
    func testDeletionConflictRejectsAnotherRecordName() async throws { try await reject(.deleteConflict, .name) }
    @BigSyncBackgroundActor
    func testDeletionConflictRejectsAnotherZone() async throws { try await reject(.deleteConflict, .zone) }
    @BigSyncBackgroundActor
    func testDeletionConflictRejectsAnotherZoneOwner() async throws { try await reject(.deleteConflict, .owner) }
    @BigSyncBackgroundActor
    func testDeletionConflictCannotBorrowAcknowledgedSiblingIdentity() async throws { try await reject(.deleteConflict, .siblingIdentity) }
    @BigSyncBackgroundActor
    func testLookupStillRejectsAnotherRecordName() async throws { try await reject(.lookup, .name) }
    @BigSyncBackgroundActor
    func testLookupStillRejectsAnotherZone() async throws { try await reject(.lookup, .zone) }
    @BigSyncBackgroundActor
    func testLookupStillRejectsAnotherRecordType() async throws { try await reject(.lookup, .type) }
    @BigSyncBackgroundActor
    func testMalformedUploadConflictPreservesSiblingRetryDeadline() async throws {
        try await reject(.saveConflict, .name, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
    }
    @BigSyncBackgroundActor
    func testMalformedDeleteConflictPreservesAccountStopWithoutIdentityRequest() async throws {
        try await reject(.deleteConflict, .name, sibling: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testMalformedSaveDoesNotBecomePureSizeRetry() async throws {
        try await reject(.save, .type, sibling: CKError(.limitExceeded))
    }
    @BigSyncBackgroundActor
    func testMissingMutationResultStillPreservesSuccessfulSibling() async throws { try await reject(.save, .missingResult) }
    @BigSyncBackgroundActor
    func testMissingUploadConflictRecordKeepsOriginalFailure() async throws { try await reject(.saveConflict, .missingConflictRecord) }
    @BigSyncBackgroundActor
    func testMissingDeleteConflictRecordKeepsOriginalFailure() async throws { try await reject(.deleteConflict, .missingConflictRecord) }
    @BigSyncBackgroundActor
    func testMatchingSaveStillAcknowledgesBothRecords() async throws {
        let (adapter, transport, _, error) = try await run(.save, .none)
        XCTAssertNil(error); XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(Set(adapter.uploaded.map { $0.recordID.recordName }), ["success", "target"])
        let count = await transport.mutationCount; XCTAssertEqual(count, 1)
    }
    @BigSyncBackgroundActor
    func testMatchingUploadConflictStillRebasesAndRetries() async throws {
        let (adapter, transport, _, error) = try await run(.saveConflict, .none)
        XCTAssertNil(error); XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(adapter.imported.map { $0.recordID.recordName }, ["target"])
        let count = await transport.mutationCount; XCTAssertEqual(count, 2)
    }
    @BigSyncBackgroundActor
    func testMatchingDeleteConflictStillRebasesWithoutImportingPayload() async throws {
        let (adapter, transport, _, error) = try await run(.deleteConflict, .none)
        XCTAssertNil(error); XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(adapter.rebased.map { $0.recordID.recordName }, ["target"])
        XCTAssertTrue(adapter.imported.isEmpty)
        let count = await transport.mutationCount; XCTAssertEqual(count, 2)
    }
    @BigSyncBackgroundActor
    func testMatchingLookupStillConsumesObservationWithoutSendingMutation() async throws {
        let (adapter, transport, _, error) = try await run(.lookup, .none)
        XCTAssertNil(error); XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(Set(adapter.imported.map { $0.recordID.recordName }), ["success", "target"])
        let count = await transport.mutationCount; XCTAssertEqual(count, 0)
    }
    @BigSyncBackgroundActor
    func testDeleteRecordTypeRemainsAnAdapterOwnedDecision() async throws {
        let (adapter, _, _, error) = try await run(.deleteConflict, .type)
        XCTAssertNil(error)
        XCTAssertEqual(adapter.rebased.first?.recordType, "OtherFixture")
        XCTAssertTrue(adapter.pending.isEmpty)
    }

    @BigSyncBackgroundActor
    func testMalformedSaveRemainsVisibleWhenSiblingAcknowledgementFails() async throws {
        let local = NSError(domain: "LocalAcknowledgementFailure", code: 29)
        let (adapter, transport, _, failure) = try await run(.save, .name, acknowledgeFailure: local)
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual(children[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.domain, expected.domain)
        XCTAssertNil(children[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertEqual((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError, local)
        XCTAssertTrue(adapter.uploaded.isEmpty)
        XCTAssertTrue(adapter.pending.contains("target"))
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testMalformedSaveRemainsVisibleWhenAccountRevalidationFails() async throws {
        let (adapter, transport, _, failure) = try await run(.save, .name, failsAccountAfterResult: true)
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual(children[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.domain, expected.domain)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain, "ResponseAccountFailure")
        XCTAssertTrue(adapter.uploaded.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testMalformedUploadConflictRetainsItsOwnRetryDeadline() async throws {
        let (adapter, transport, _, failure) = try await run(.saveConflict, .name, conflictRetryAfter: 73)
        let error = try XCTUnwrap(failure)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertEqual(constraints.serverMinimum, 73)
        XCTAssertTrue(constraints.requiresDeferredRetry)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertTrue(adapter.pending.contains("target"))
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testMalformedDeletionConflictRetainsItsOwnRetryDeadline() async throws {
        let (adapter, transport, _, failure) = try await run(.deleteConflict, .name, conflictRetryAfter: 73)
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 73)
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertTrue(adapter.pending.contains("target"))
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testAcknowledgementCancellationIsNotWrappedAsMalformedResponseRetry() async throws {
        let (adapter, transport, _, failure) = try await run(.save, .name, acknowledgeFailure: CancellationError())
        XCTAssertTrue(failure is CancellationError)
        XCTAssertTrue(adapter.uploaded.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testLookupStillRejectsAnotherZoneOwner() async throws { try await reject(.lookup, .owner) }
    @BigSyncBackgroundActor
    func testMalformedSavePreservesAuthenticationStopWithoutIdentityRequest() async throws {
        try await reject(.save, .name, sibling: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testMalformedUploadConflictPreservesTemporaryAccountStop() async throws {
        try await reject(.saveConflict, .siblingIdentity, sibling: CKError(.accountTemporarilyUnavailable))
    }
    @BigSyncBackgroundActor
    func testMalformedDeletionConflictDoesNotBecomePureSizeRetry() async throws {
        try await reject(.deleteConflict, .name, sibling: CKError(.limitExceeded))
    }
    @BigSyncBackgroundActor
    func testValidSaveAcknowledgesSuccessfulSiblingsDespiteAccountStop() async throws {
        let (adapter, transport, account, failure) = try await run(.save, .none, sibling: CKError(.notAuthenticated))
        let error = try XCTUnwrap(failure)
        XCTAssertTrue(CloudKitRetryConstraints(error).blocksAccountOperations)
        XCTAssertEqual(Set(adapter.uploaded.map { $0.recordID.recordName }), ["success", "target"])
        XCTAssertEqual(adapter.pending, ["other"])
        let afterResult = await account.callsAfterResult; XCTAssertEqual(afterResult, 0)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testMissingSaveResultRemainsVisibleWhenAccountRevalidationFails() async throws {
        let (adapter, _, _, failure) = try await run(.save, .missingResult, failsAccountAfterResult: true)
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let expected = CocoaError(.coderValueNotFound) as NSError
        let missing = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        XCTAssertEqual(children[missing]?.domain, expected.domain)
        XCTAssertEqual(children[missing]?.code, expected.code)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain, "ResponseAccountFailure")
        XCTAssertTrue(adapter.uploaded.isEmpty)
        XCTAssertTrue(adapter.pending.contains("target"))
    }
    @BigSyncBackgroundActor
    func testMalformedConflictAndItsDeadlineSurviveSiblingAcknowledgementFailure() async throws {
        let local = NSError(domain: "LocalAcknowledgementFailure", code: 29)
        let (adapter, _, _, failure) = try await run(.saveConflict, .name, acknowledgeFailure: local, conflictRetryAfter: 73)
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 73)
        XCTAssertFalse(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual(children[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.domain, expected.domain)
        XCTAssertEqual((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError, local)
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertTrue(adapter.uploaded.isEmpty)
        XCTAssertTrue(adapter.pending.contains("target"))
    }
}


extension SyncMutationResponseIdentityTests {
    // A valid conflict payload does not override a delay, an account stop, or
    // token recovery carried in the same error. Successful sibling receipts
    // remain independently eligible; no repair or transport retry occurs here.
    @BigSyncBackgroundActor
    private func requireDeferredRepair(
        _ route: ResponseRoute,
        retryAfter: TimeInterval? = nil,
        underlying: CKError? = nil,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let (adapter, transport, account, failure) = try await run(
            route, .none, conflictRetryAfter: retryAfter,
            repairUnderlyingError: underlying
        )
        XCTAssertTrue(adapter.pending.contains("target"), file: file, line: line)
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.requeued.isEmpty, file: file, line: line)
        let mutationCount = await transport.mutationCount
        let lookupCount = await transport.lookupCount
        XCTAssertEqual(mutationCount, route.looksUp ? 0 : 1, file: file, line: line)
        XCTAssertEqual(lookupCount, route.looksUp ? 1 : 0, file: file, line: line)
        let acknowledged = route.deletes ? adapter.deleted.map(\.recordName)
            : adapter.uploaded.map { $0.recordID.recordName }
        XCTAssertEqual(acknowledged, route.looksUp ? [] : ["success"], file: file, line: line)
        if underlying?.code == .notAuthenticated || underlying?.code == .accountTemporarilyUnavailable {
            let probes = await account.callsAfterResult
            XCTAssertEqual(probes, 0, "Known account stop cannot trigger another identity request", file: file, line: line)
        }
        let error = try XCTUnwrap(failure, file: file, line: line)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures, file: file, line: line)
        if let retryAfter {
            XCTAssertEqual(constraints.serverMinimum, retryAfter, file: file, line: line)
            XCTAssertTrue(constraints.requiresDeferredRetry, file: file, line: line)
        }
        if let underlying {
            XCTAssertTrue(constraints.codes.contains(underlying.code), file: file, line: line)
            if underlying.code == .requestRateLimited {
                XCTAssertEqual(constraints.serverMinimum, 137, file: file, line: line)
            }
            if underlying.code == .changeTokenExpired {
                XCTAssertTrue(constraints.requestsTokenRecovery, file: file, line: line)
            } else if underlying.code == .notAuthenticated || underlying.code == .accountTemporarilyUnavailable {
                XCTAssertTrue(constraints.blocksAccountOperations, file: file, line: line)
            } else if underlying.code == .requestRateLimited || underlying.code == .networkFailure {
                XCTAssertTrue(constraints.requiresDeferredRetry, file: file, line: line)
            }
        }
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError], file: file, line: line)
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let original = try XCTUnwrap(children[target], file: file, line: line)
        XCTAssertEqual(original.domain, CKErrorDomain, file: file, line: line)
        XCTAssertEqual(original.code,
            (route == .saveMissing || route == .lookupMissing)
                ? CKError.unknownItem.rawValue : CKError.serverRecordChanged.rawValue,
            "Preserve the original record failure, not an invented new retry error", file: file, line: line)
        if !route.looksUp {
            XCTAssertNil(children[.init(recordName: "success", zoneID: adapter.recordZoneID)], file: file, line: line)
        }
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesOwnRetryDeadline() async throws {
        try await requireDeferredRepair(.saveConflict, retryAfter: 73)
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesNestedRateLimit() async throws {
        try await requireDeferredRepair(.saveConflict, underlying: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesNestedAuthenticationStop() async throws {
        try await requireDeferredRepair(.saveConflict, underlying: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesNestedTemporaryAccountStop() async throws {
        try await requireDeferredRepair(.saveConflict, underlying: CKError(.accountTemporarilyUnavailable))
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesNestedTokenRecovery() async throws {
        try await requireDeferredRepair(.saveConflict, underlying: CKError(.changeTokenExpired))
    }
    @BigSyncBackgroundActor
    func testUploadConflictPreservesNestedNetworkFailure() async throws {
        try await requireDeferredRepair(.saveConflict, underlying: CKError(.networkFailure))
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesOwnRetryDeadline() async throws {
        try await requireDeferredRepair(.saveMissing, retryAfter: 73)
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesNestedRateLimit() async throws {
        try await requireDeferredRepair(.saveMissing, underlying: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesNestedAuthenticationStop() async throws {
        try await requireDeferredRepair(.saveMissing, underlying: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesNestedTemporaryAccountStop() async throws {
        try await requireDeferredRepair(.saveMissing, underlying: CKError(.accountTemporarilyUnavailable))
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesNestedTokenRecovery() async throws {
        try await requireDeferredRepair(.saveMissing, underlying: CKError(.changeTokenExpired))
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesNestedNetworkFailure() async throws {
        try await requireDeferredRepair(.saveMissing, underlying: CKError(.networkFailure))
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesOwnRetryDeadline() async throws {
        try await requireDeferredRepair(.deleteConflict, retryAfter: 73)
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesNestedRateLimit() async throws {
        try await requireDeferredRepair(.deleteConflict, underlying: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesNestedAuthenticationStop() async throws {
        try await requireDeferredRepair(.deleteConflict, underlying: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesNestedTemporaryAccountStop() async throws {
        try await requireDeferredRepair(.deleteConflict, underlying: CKError(.accountTemporarilyUnavailable))
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesNestedTokenRecovery() async throws {
        try await requireDeferredRepair(.deleteConflict, underlying: CKError(.changeTokenExpired))
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesNestedNetworkFailure() async throws {
        try await requireDeferredRepair(.deleteConflict, underlying: CKError(.networkFailure))
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesOwnRetryDeadline() async throws {
        try await requireDeferredRepair(.lookupMissing, retryAfter: 73)
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesNestedRateLimit() async throws {
        try await requireDeferredRepair(.lookupMissing, underlying: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesNestedAuthenticationStop() async throws {
        try await requireDeferredRepair(.lookupMissing, underlying: CKError(.notAuthenticated))
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesNestedTemporaryAccountStop() async throws {
        try await requireDeferredRepair(.lookupMissing, underlying: CKError(.accountTemporarilyUnavailable))
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesNestedTokenRecovery() async throws {
        try await requireDeferredRepair(.lookupMissing, underlying: CKError(.changeTokenExpired))
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesNestedNetworkFailure() async throws {
        try await requireDeferredRepair(.lookupMissing, underlying: CKError(.networkFailure))
    }

    @BigSyncBackgroundActor
    func testUnconstrainedMissingUploadStillRequeuesAndRetries() async throws {
        let (adapter, transport, _, error) = try await run(.saveMissing, .none)
        XCTAssertNil(error)
        XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(adapter.requeued.map(\.recordName), ["target"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 2)
        XCTAssertEqual(Set(adapter.uploaded.map { $0.recordID.recordName }), ["success", "target"])
    }

    @BigSyncBackgroundActor
    func testUnconstrainedAcceptanceMissStillSubmitsConditionalCandidate() async throws {
        let (adapter, transport, _, error) = try await run(.lookupMissing, .none)
        XCTAssertNil(error)
        XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(calls, 1)
        XCTAssertEqual(lookups, 1)
        XCTAssertEqual(Set(adapter.uploaded.map { $0.recordID.recordName }), ["success", "target"])
    }

    @BigSyncBackgroundActor
    func testConstrainedConflictPreservesLocalAcknowledgementFailure() async throws {
        let local = NSError(domain: "LocalAcknowledgementFailure", code: 29)
        let (adapter, transport, _, failure) = try await run(
            .saveConflict, .none, acknowledgeFailure: local, conflictRetryAfter: 73
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 73)
        XCTAssertEqual((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError, local)
        XCTAssertTrue(adapter.pending.contains("target"))
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testConstrainedConflictKeepsAcknowledgementCancellationTerminal() async throws {
        let (adapter, transport, _, failure) = try await run(
            .saveConflict, .none, acknowledgeFailure: CancellationError(), conflictRetryAfter: 73
        )
        XCTAssertTrue(failure is CancellationError)
        XCTAssertTrue(adapter.pending.contains("target"))
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }
}


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    func testValidConflictPreservesExplicitZeroRetryFloor() async throws {
        try await requireDeferredRepair(.saveConflict, retryAfter: 0)
    }

    @BigSyncBackgroundActor
    func testConstrainedConflictPreservesStrongestSiblingDeadline() async throws {
        let (adapter, transport, _, failure) = try await run(
            .saveConflict, .none,
            sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]),
            conflictRetryAfter: 73
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 137)
        XCTAssertEqual(adapter.pending, ["target", "other"])
        XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"])
        XCTAssertTrue(adapter.imported.isEmpty)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertEqual(children[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.code,
            CKError.serverRecordChanged.rawValue)
        XCTAssertEqual(children[.init(recordName: "other", zoneID: adapter.recordZoneID)]?.code,
            CKError.Code.requestRateLimited.rawValue)
        XCTAssertNil(children[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }
}


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    func testUploadConflictPreservesOtherRecognizedCloudKitFailures() async throws {
        for code: CKError.Code in [.quotaExceeded, .zoneNotFound, .operationCancelled] {
            try await requireDeferredRepair(.saveConflict, underlying: CKError(code))
        }
    }
    @BigSyncBackgroundActor
    func testMissingUploadPreservesOtherRecognizedCloudKitFailures() async throws {
        for code: CKError.Code in [.quotaExceeded, .zoneNotFound, .operationCancelled] {
            try await requireDeferredRepair(.saveMissing, underlying: CKError(code))
        }
    }
    @BigSyncBackgroundActor
    func testDeletionConflictPreservesOtherRecognizedCloudKitFailures() async throws {
        for code: CKError.Code in [.quotaExceeded, .zoneNotFound, .operationCancelled] {
            try await requireDeferredRepair(.deleteConflict, underlying: CKError(code))
        }
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissPreservesOtherRecognizedCloudKitFailures() async throws {
        for code: CKError.Code in [.quotaExceeded, .zoneNotFound, .operationCancelled] {
            try await requireDeferredRepair(.lookupMissing, underlying: CKError(code))
        }
    }

    @BigSyncBackgroundActor
    func testOrdinaryConflictWithInternalSDKDetailStillRepairs() async throws {
        let (adapter, transport, _, error) = try await run(
            .saveConflict, .none,
            repairUnderlyingError: NSError(domain: "CKInternalErrorDomain", code: 2004)
        )
        XCTAssertNil(error)
        XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(adapter.imported.map { $0.recordID.recordName }, ["target"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 2)
    }

    @BigSyncBackgroundActor
    func testOrdinaryAcceptanceMissWithInternalSDKDetailStillSubmits() async throws {
        let (adapter, transport, _, error) = try await run(
            .lookupMissing, .none,
            repairUnderlyingError: NSError(domain: "CKInternalErrorDomain", code: 2004)
        )
        XCTAssertNil(error)
        XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(calls, 1)
        XCTAssertEqual(lookups, 1)
    }
}
