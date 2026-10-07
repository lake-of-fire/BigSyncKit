import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

private enum ResponseRoute: Sendable {
    case save, saveConflict, saveMissing, deleteConflict, deleteMissing, lookup, lookupMissing
    var deletes: Bool { self == .deleteConflict || self == .deleteMissing }
    var looksUp: Bool { self == .lookup || self == .lookupMissing }
}
private enum ResponseSizeLimit: Sendable {
    case thrown, perItem, targetOnly
}
private enum ResponsePreparationAlteration: Sendable {
    case none, duplicateIdentity, conflictingGeneration, zone, owner
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
    var cancelOnPendingStateRead = false
    var cancelAfterConflictImport = false
    var quarantineImportedRecords = false
    var hasChanges: Bool {
        if cancelOnPendingStateRead { withUnsafeCurrentTask { $0?.cancel() } }
        return !pending.isEmpty
    }
    var acknowledgeFailure: Error?
    var requeueFailure: Error?
    private(set) var requeueInvocations = 0
    var preparationAlteration: ResponsePreparationAlteration = .none
    var respectsPreparationLimit = false
    private(set) var preparationLimits = [Int]()

    private func preparedID(_ name: String) -> CKRecord.ID {
        let zone: CKRecordZone.ID
        if name == "target", preparationAlteration == .zone {
            zone = .init(zoneName: "other-zone", ownerName: recordZoneID.ownerName)
        } else if name == "target", preparationAlteration == .owner {
            zone = .init(zoneName: recordZoneID.zoneName, ownerName: "other-owner")
        } else {
            zone = recordZoneID
        }
        return .init(recordName: name, zoneID: zone)
    }

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
        if cancelAfterConflictImport { withUnsafeCurrentTask { $0?.cancel() } }
        return records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType, recordID: $0.element.recordID),
                  disposition: quarantineImportedRecords
                    ? .quarantined(lineageID: "quarantine-" + $0.element.recordID.recordName)
                    : .preservedPendingLocal(generation: "pending-" + $0.element.recordID.recordName))
        }
    }
    func persistImportedChanges() async throws { persistCount += 1 }
    func didFinishImport() async throws {}
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        guard !route.deletes else { return [] }
        preparationLimits.append(limit)
        let names = pending.sorted()
        let selected = respectsPreparationLimit ? Array(names.prefix(max(0, limit))) : names
        var items: [PreparedRecordUpload] = selected.map { name in
            let record = CKRecord(recordType: "IdentityFixture", recordID: preparedID(name))
            record["text"] = "local-" + name as CKRecordValue
            return .init(record: record, generation: "pending-" + name,
                         comparisonBase: nil, requiresAcceptanceCheck: route.looksUp)
        }
        if let last = items.last {
            switch preparationAlteration {
            case .duplicateIdentity: items.append(last)
            case .conflictingGeneration:
                items.append(.init(record: last.record, generation: "successor-generation",
                    comparisonBase: nil, requiresAcceptanceCheck: route.looksUp))
            default: break
            }
        }
        return items
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
        preparationLimits.append(limit)
        let names = pending.sorted()
        let selected = respectsPreparationLimit ? Array(names.prefix(max(0, limit))) : names
        var items: [PreparedRecordDeletion] = selected.map {
            .init(recordID: preparedID($0), generation: "pending-" + $0)
        }
        if let last = items.last {
            switch preparationAlteration {
            case .duplicateIdentity: items.append(last)
            case .conflictingGeneration:
                items.append(.init(recordID: last.recordID, generation: "successor-generation"))
            default: break
            }
        }
        return items
    }
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {
        if let acknowledgeFailure { throw acknowledgeFailure }
        deleted.append(contentsOf: recordIDs)
        for id in recordIDs { pending.remove(id.recordName) }
    }
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {
        requeueInvocations += 1
        if let requeueFailure { throw requeueFailure }
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
    let failureAfterResultCall: Int?
    let inspectionProbe: ResponseErrorInspectionProbe?
    init(failsAfterResult: Bool, failureAfterResultCall: Int? = nil,
         inspectionProbe: ResponseErrorInspectionProbe? = nil) {
        self.inspectionProbe = inspectionProbe
        self.failsAfterResult = failsAfterResult
        self.failureAfterResultCall = failureAfterResultCall
    }
    func didReceive() { received = true }
    func identity() throws -> String {
        if received {
            callsAfterResult += 1
            if failsAfterResult || callsAfterResult == failureAfterResultCall {
                throw NSError(domain: "ResponseAccountFailure", code: 41)
            }
        }
        if received { inspectionProbe?.arm() }
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
    let sizeLimit: ResponseSizeLimit?
    let account: ResponseAccountProbe
    private(set) var mutationCount = 0
    private(set) var attemptedMutationSizes = [Int]()
    private(set) var lookupCount = 0
    init(route: ResponseRoute, alteration: ResponseAlteration, siblingError: CKError?, account: ResponseAccountProbe,
         conflictRetryAfter: TimeInterval?, repairUnderlyingError: Error? = nil,
         sizeLimit: ResponseSizeLimit? = nil) {
        self.route = route; self.alteration = alteration; self.siblingError = siblingError; self.account = account
        self.conflictRetryAfter = conflictRetryAfter
        self.repairUnderlyingError = repairUnderlyingError
        self.sizeLimit = sizeLimit
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
        attemptedMutationSizes.append(records.count + recordIDs.count)
        // A nonshrinking predecessor is failed by the transport watchdog;
        // the actual bounded retry implementation must stop before this point.
        guard mutationCount <= 3 else { throw UnexpectedRetry() }
        if let sizeLimit {
            var info = [String: Any]()
            if let conflictRetryAfter { info[CKErrorRetryAfterKey] = conflictRetryAfter }
            if let repairUnderlyingError { info[NSUnderlyingErrorKey] = repairUnderlyingError }
            let error = CKError(.limitExceeded, userInfo: info)
            if sizeLimit == .thrown { throw error }
            var saves = [CKRecord.ID: Result<CKRecord, Error>]()
            var deletes = [CKRecord.ID: Result<Void, Error>]()
            for record in records {
                saves[record.recordID] = sizeLimit == .targetOnly && record.recordID.recordName != "target"
                    ? .success(record) : .failure(error)
            }
            for id in recordIDs {
                deletes[id] = sizeLimit == .targetOnly && id.recordName != "target"
                    ? .success(()) : .failure(error)
            }
            await account.didReceive()
            return .init(saveResults: saves, deleteResults: deletes)
        }
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
            deletes[id] = .failure(route == .deleteMissing ? missing() : conflict(id))
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
                     repairUnderlyingError: Error? = nil,
                     preparationAlteration: ResponsePreparationAlteration = .none,
                     failAccountAfterResultCall: Int? = nil,
                     requeueFailure: Error? = nil,
                     cancelOnPendingStateRead: Bool = false,
                     cancelAfterConflictImport: Bool = false,
                     quarantineImportedRecords: Bool = false,
                     sizeLimit: ResponseSizeLimit? = nil,
                     respectsPreparationLimit: Bool = false,
                     inspectionProbe: ResponseErrorInspectionProbe? = nil) async throws -> (ResponseIdentityAdapter, ResponseIdentityTransport, ResponseAccountProbe, Error?) {
        let adapter = ResponseIdentityAdapter(route: route, siblingFailure: sibling != nil)
        adapter.acknowledgeFailure = acknowledgeFailure
        adapter.preparationAlteration = preparationAlteration
        adapter.requeueFailure = requeueFailure
        adapter.cancelOnPendingStateRead = cancelOnPendingStateRead
        adapter.cancelAfterConflictImport = cancelAfterConflictImport
        adapter.quarantineImportedRecords = quarantineImportedRecords
        adapter.respectsPreparationLimit = respectsPreparationLimit
        let account = ResponseAccountProbe(failsAfterResult: failsAccountAfterResult,
                                           failureAfterResultCall: failAccountAfterResultCall,
                                           inspectionProbe: inspectionProbe)
        let transport = ResponseIdentityTransport(route: route, alteration: alteration, siblingError: sibling,
                                                   account: account, conflictRetryAfter: conflictRetryAfter,
                                                   repairUnderlyingError: repairUnderlyingError,
                                                   sizeLimit: sizeLimit)
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("response-identity-" + UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: dir) }
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.response-identity", database: ResponseDatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: ResponseKeyValueStore(),
            accountIdentifierProvider: { try await account.identity() }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport, recordStore: transport,
            backupDetectionBaseURL: dir, logger: Logger(label: "ResponseIdentity"))
        sync.accountScopeAuthorityFence.clear() // Controlled fixture starts with validated authority.
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
        inspectionProbe?.recordBatchSize(sync.batchSize)
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


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireAcknowledgedDeletionConstraint(
        retryAfter: TimeInterval? = nil, underlying: CKError? = nil,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteMissing, .none, conflictRetryAfter: retryAfter,
            repairUnderlyingError: underlying
        )
        XCTAssertTrue(adapter.pending.isEmpty, file: file, line: line)
        XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"], file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        let error = try XCTUnwrap(failure, file: file, line: line)
        let constraints = CloudKitRetryConstraints(error)
        if let retryAfter { XCTAssertEqual(constraints.serverMinimum, retryAfter, file: file, line: line) }
        if let underlying { XCTAssertTrue(constraints.codes.contains(underlying.code), file: file, line: line) }
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error], file: file, line: line)
        XCTAssertNil(items[CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)], file: file, line: line)
        XCTAssertNotNil(items["acknowledgedDeletionConstraints"], file: file, line: line)
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1, file: file, line: line)
        XCTAssertEqual(probes, underlying?.code == .notAuthenticated || underlying?.code == .accountTemporarilyUnavailable ? 0 : 1,
                       file: file, line: line)
    }
    @BigSyncBackgroundActor
    func testDeleteMissAcknowledgesWhilePreservingRetryDeadline() async throws {
        try await requireAcknowledgedDeletionConstraint(retryAfter: 73)
        try await requireAcknowledgedDeletionConstraint(retryAfter: 0)
    }
    @BigSyncBackgroundActor
    func testDeleteMissAcknowledgesWhilePreservingNestedIndependentStops() async throws {
        for code: CKError.Code in [.notAuthenticated, .accountTemporarilyUnavailable, .changeTokenExpired,
                                  .networkFailure, .networkUnavailable, .zoneBusy, .zoneNotFound,
                                  .quotaExceeded, .operationCancelled, .requestRateLimited] {
            try await requireAcknowledgedDeletionConstraint(underlying: CKError(code))
        }
    }
    @BigSyncBackgroundActor
    func testDeleteMissConstraintSurvivesAcknowledgementFailure() async throws {
        let local = NSError(domain: "LocalAcknowledgementFailure", code: 29)
        let (adapter, transport, _, failure) = try await run(
            .deleteMissing, .none, acknowledgeFailure: local, conflictRetryAfter: 73
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 73)
        XCTAssertEqual((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError, local)
        XCTAssertTrue(adapter.deleted.isEmpty)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testDeleteMissConstraintKeepsAcknowledgementCancellationTerminal() async throws {
        let (adapter, _, _, failure) = try await run(
            .deleteMissing, .none, acknowledgeFailure: CancellationError(), conflictRetryAfter: 73
        )
        XCTAssertTrue(failure is CancellationError)
        XCTAssertTrue(adapter.deleted.isEmpty)
    }
    @BigSyncBackgroundActor
    func testUnconstrainedDeleteMissStillAcknowledgesWithoutFailure() async throws {
        let (adapter, transport, _, failure) = try await run(.deleteMissing, .none)
        XCTAssertNil(failure); XCTAssertTrue(adapter.pending.isEmpty)
        XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"])
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 1)
    }
    @BigSyncBackgroundActor
    func testMissingDeleteResultPreservesIdempotentSuccessfulSibling() async throws {
        try await reject(.deleteConflict, .missingResult)
    }
    @BigSyncBackgroundActor
    func testMissingLookupResultSurvivesAccountRevalidationFailure() async throws {
        let (adapter, transport, _, failure) = try await run(.lookup, .missingResult, failsAccountAfterResult: true)
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let expected = CocoaError(.coderValueNotFound) as NSError
        XCTAssertEqual(items[target]?.domain, expected.domain); XCTAssertEqual(items[target]?.code, expected.code)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain, "ResponseAccountFailure")
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 0)
    }
    @BigSyncBackgroundActor
    func testMalformedLookupResultSurvivesAccountRevalidationFailure() async throws {
        let (adapter, _, _, failure) = try await run(.lookup, .name, failsAccountAfterResult: true)
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: NSError])
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual(items[target]?.domain, expected.domain); XCTAssertEqual(items[target]?.code, expected.code)
        XCTAssertTrue(adapter.imported.isEmpty)
    }
    @BigSyncBackgroundActor
    func testMissingLookupSlotImportsValidObservationButNeverSubmitsMutation() async throws {
        let (adapter, transport, _, failure) = try await run(.lookup, .missingResult)
        XCTAssertNotNil(failure); XCTAssertEqual(adapter.pending, ["target"])
        XCTAssertEqual(adapter.imported.map { $0.recordID.recordName }, ["success"])
        let calls = await transport.mutationCount; XCTAssertEqual(calls, 0)
        let lookups = await transport.lookupCount; XCTAssertEqual(lookups, 1)
    }
}


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    func testMalformedLookupIdentitySurvivesAccountRevalidationFailure() async throws {
        for alteration: ResponseAlteration in [.name, .zone, .owner, .type, .siblingIdentity] {
            let (adapter, transport, _, failure) = try await run(
                .lookup, alteration, failsAccountAfterResult: true
            )
            let error = try XCTUnwrap(failure)
            let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
                as? [CKRecord.ID: NSError])
            let invalid = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
            let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
            XCTAssertEqual(children[invalid]?.domain, expected.domain)
            XCTAssertEqual(children[invalid]?.code, expected.code)
            XCTAssertNil(children[.init(recordName: "success", zoneID: adapter.recordZoneID)])
            XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
                           "ResponseAccountFailure")
            XCTAssertTrue(adapter.imported.isEmpty)
            XCTAssertTrue(adapter.uploaded.isEmpty)
            XCTAssertEqual(adapter.pending, ["success", "target"])
            let calls = await transport.mutationCount
            let lookups = await transport.lookupCount
            XCTAssertEqual(calls, 0)
            XCTAssertEqual(lookups, 1)
        }
    }

    @BigSyncBackgroundActor
    func testMissingLookupResultSurvivesAccountRevalidationFailureWithPendingAndProbeChecks() async throws {
        let (adapter, transport, _, failure) = try await run(
            .lookup, .missingResult, failsAccountAfterResult: true
        )
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let missing = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let expected = CocoaError(.coderValueNotFound) as NSError
        XCTAssertEqual(children[missing]?.domain, expected.domain)
        XCTAssertEqual(children[missing]?.code, expected.code)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
                       "ResponseAccountFailure")
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertEqual(adapter.pending, ["success", "target"])
        let calls = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(calls, 0)
        XCTAssertEqual(lookups, 1)
    }

    @BigSyncBackgroundActor
    func testMalformedLookupAndSiblingDeadlineSurviveAccountFailureTogether() async throws {
        let (adapter, transport, _, failure) = try await run(
            .lookup, .name,
            sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]),
            failsAccountAfterResult: true
        )
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual(children[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.domain,
                       expected.domain)
        XCTAssertEqual(children[.init(recordName: "other", zoneID: adapter.recordZoneID)]?.code,
                       CKError.Code.requestRateLimited.rawValue)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 137)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
                       "ResponseAccountFailure")
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertEqual(adapter.pending, ["success", "target", "other"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 0)
    }

    @BigSyncBackgroundActor
    func testMalformedLookupKeepsFailFastPolicyWithAuthenticationSibling() async throws {
        try await reject(.lookup, .name, sibling: CKError(.notAuthenticated))
    }

    @BigSyncBackgroundActor
    func testMissingLookupStillImportsValidObservationBeforeReportingMissingSlot() async throws {
        let (adapter, transport, _, failure) = try await run(.lookup, .missingResult)
        let error = try XCTUnwrap(failure)
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertNotNil(children[.init(recordName: "target", zoneID: adapter.recordZoneID)])
        XCTAssertNil(children[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertEqual(adapter.imported.map { $0.recordID.recordName }, ["success"])
        XCTAssertEqual(adapter.pending, ["target"])
        XCTAssertTrue(adapter.uploaded.isEmpty)
        let calls = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(calls, 0)
        XCTAssertEqual(lookups, 1)
    }

    @BigSyncBackgroundActor
    func testValidLookupAccountFailureRetainsOriginalIsolatedError() async throws {
        let (adapter, transport, _, failure) = try await run(
            .lookup, .none, failsAccountAfterResult: true
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual((error as NSError).domain, "ResponseAccountFailure")
        XCTAssertNil((error as NSError).userInfo[CKPartialErrorsByItemIDKey])
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertEqual(adapter.pending, ["success", "target"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 0)
    }
}


/// Computed userInfo creates a graph cycle without retaining a permanent
/// strong-reference cycle in the fixture itself.
private final class MultipleUnderlyingCycle: NSError, @unchecked Sendable {
    private let retry = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
    init() { super.init(domain: CKErrorDomain, code: CKError.Code.limitExceeded.rawValue, userInfo: nil) }
    required init?(coder: NSCoder) { super.init(coder: coder) }
    override var userInfo: [String: Any] {
        [NSMultipleUnderlyingErrorsKey: [self, retry]]
    }
}

extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireMultipleUnderlyingConstraint(_ route: ResponseRoute) async throws {
        for code: CKError.Code in [.notAuthenticated, .accountTemporarilyUnavailable,
                                    .requestRateLimited, .networkFailure, .changeTokenExpired] {
            let cloudError = CKError(code, userInfo: code == .requestRateLimited
                ? [CKErrorRetryAfterKey: 137] : [:])
            // The aggregate's outer Cocoa domain is not itself a repair stop.
            // The classifier must inspect its standard Foundation children.
            let aggregate = NSError(domain: NSCocoaErrorDomain, code: 512,
                userInfo: [NSMultipleUnderlyingErrorsKey: [cloudError as NSError]])
            let (adapter, transport, account, failure) = try await run(
                route, .none, repairUnderlyingError: aggregate
            )
            let error = try XCTUnwrap(failure)
            let constraints = CloudKitRetryConstraints(error)
            XCTAssertTrue(constraints.codes.contains(code))
            if code == .notAuthenticated || code == .accountTemporarilyUnavailable {
                XCTAssertTrue(constraints.blocksAccountOperations)
                let probes = await account.callsAfterResult
                XCTAssertEqual(probes, 0)
            }
            if code == .requestRateLimited {
                XCTAssertEqual(constraints.serverMinimum, 137)
                XCTAssertTrue(constraints.requiresDeferredRetry)
            }
            if code == .changeTokenExpired { XCTAssertTrue(constraints.requestsTokenRecovery) }
            if code == .networkFailure { XCTAssertTrue(constraints.requiresDeferredRetry) }
            XCTAssertTrue(adapter.imported.isEmpty)
            XCTAssertTrue(adapter.rebased.isEmpty)
            XCTAssertTrue(adapter.requeued.isEmpty)
            XCTAssertTrue(adapter.pending.contains("target"))
            let calls = await transport.mutationCount
            let lookups = await transport.lookupCount
            XCTAssertEqual(calls, route.looksUp ? 0 : 1)
            XCTAssertEqual(lookups, route.looksUp ? 1 : 0)
            let acknowledged = route.deletes ? adapter.deleted.map(\.recordName)
                : adapter.uploaded.map { $0.recordID.recordName }
            XCTAssertEqual(acknowledged, route.looksUp ? [] : ["success"])
            let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
                as? [CKRecord.ID: NSError])
            let slot = try XCTUnwrap(children[.init(recordName: "target", zoneID: adapter.recordZoneID)])
            XCTAssertEqual(slot.code, route == .saveMissing || route == .lookupMissing
                ? CKError.unknownItem.rawValue : CKError.serverRecordChanged.rawValue)
            XCTAssertTrue((slot.userInfo[NSUnderlyingErrorKey] as? NSError) === aggregate)
        }
    }

    @BigSyncBackgroundActor
    func testUploadConflictRetainsMultipleUnderlyingConstraints() async throws {
        try await requireMultipleUnderlyingConstraint(.saveConflict)
    }
    @BigSyncBackgroundActor
    func testMissingUploadRetainsMultipleUnderlyingConstraints() async throws {
        try await requireMultipleUnderlyingConstraint(.saveMissing)
    }
    @BigSyncBackgroundActor
    func testDeletionConflictRetainsMultipleUnderlyingConstraints() async throws {
        try await requireMultipleUnderlyingConstraint(.deleteConflict)
    }
    @BigSyncBackgroundActor
    func testAcceptanceMissRetainsMultipleUnderlyingConstraints() async throws {
        try await requireMultipleUnderlyingConstraint(.lookupMissing)
    }

    func testMultipleUnderlyingDeadlineCannotQualifyAsPureSizeFailure() {
        let delay = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        let error = CKError(.limitExceeded, userInfo: [NSMultipleUnderlyingErrorsKey: [delay]])
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertTrue(constraints.requiresDeferredRetry)
    }

    func testMultipleUnderlyingLocalFailureCannotQualifyAsPureSizeFailure() {
        let local = NSError(domain: "LocalDurability", code: 2)
        let error = CKError(.limitExceeded, userInfo: [NSMultipleUnderlyingErrorsKey: [local]])
        XCTAssertFalse(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures)
    }

    func testMultipleUnderlyingPureSizeGraphRetainsImmediateSplitEligibility() {
        let limit = CKError(.limitExceeded) as NSError
        let branch = CKError(.batchRequestFailed,
            userInfo: [NSMultipleUnderlyingErrorsKey: [limit, limit]]) as NSError
        let error = CKError(.partialFailure, userInfo: [
            CKPartialErrorsByItemIDKey: ["first": branch, "second": branch],
            NSUnderlyingErrorKey: limit,
            NSMultipleUnderlyingErrorsKey: [limit],
        ])
        XCTAssertTrue(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures)
        XCTAssertFalse(CloudKitRetryConstraints(error).requiresDeferredRetry)
    }

    func testMultipleUnderlyingSharedGraphVisitsEachNSErrorOnce() {
        let account = CKError(.notAuthenticated) as NSError
        let deadline = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        let shared = NSError(domain: "Aggregate", code: 1,
            userInfo: [NSMultipleUnderlyingErrorsKey: [account, deadline]])
        let error = CKError(.partialFailure, userInfo: [
            CKPartialErrorsByItemIDKey: ["first": shared, "second": shared],
            NSUnderlyingErrorKey: shared,
            NSMultipleUnderlyingErrorsKey: [shared, deadline],
        ])
        let codes = cloudKitErrors(in: error).map(\.code)
        XCTAssertEqual(codes.filter { $0 == .notAuthenticated }.count, 1)
        XCTAssertEqual(codes.filter { $0 == .requestRateLimited }.count, 1)
        XCTAssertEqual(codes.count, 3)
    }

    func testMultipleUnderlyingCycleTerminatesAndPreservesKnownDeadline() {
        let error = MultipleUnderlyingCycle()
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertTrue(constraints.requiresDeferredRetry)
        XCTAssertEqual(cloudKitErrors(in: error).count, 2)
    }

    func testSingularAndMultipleUnderlyingErrorsBothContributeConstraints() {
        let account = CKError(.notAuthenticated) as NSError
        let weaker = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 73]) as NSError
        let stronger = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        let error = CKError(.unknownItem, userInfo: [
            NSUnderlyingErrorKey: weaker,
            NSMultipleUnderlyingErrorsKey: [stronger, account],
        ])
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertTrue(constraints.blocksAccountOperations)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertEqual((error as NSError).underlyingErrors.count, 3)
    }

    @BigSyncBackgroundActor
    func testUnconstrainedInternalMultipleErrorsStillAllowOrdinaryRepair() async throws {
        let detail = NSError(domain: "CKInternalErrorDomain", code: 2004)
        let aggregate = NSError(domain: "InternalAggregate", code: 1,
            userInfo: [NSMultipleUnderlyingErrorsKey: [detail]])
        let (adapter, transport, _, failure) = try await run(
            .saveConflict, .none, repairUnderlyingError: aggregate
        )
        XCTAssertNil(failure)
        XCTAssertTrue(adapter.pending.isEmpty)
        let count = await transport.mutationCount
        XCTAssertEqual(count, 2)
    }

    func testEmptyMultipleUnderlyingListPreservesLeafClassification() {
        let error = CKError(.limitExceeded, userInfo: [NSMultipleUnderlyingErrorsKey: [NSError]()])
        XCTAssertTrue(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures)
        XCTAssertEqual(cloudKitErrors(in: error).count, 1)
    }
}


extension SyncMutationResponseIdentityTests {
    func testAggregateSizeProofKeepsExistingDepthLimit() {
        for edges in [30, 31, 32, 40] {
            var error: NSError = CKError(.limitExceeded) as NSError
            for _ in 0..<edges {
                error = CKError(.batchRequestFailed,
                    userInfo: [NSMultipleUnderlyingErrorsKey: [error]]) as NSError
            }
            XCTAssertEqual(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures,
                           edges < 32)
        }
    }

    func testAggregateTraversalFindsSharedCauseThroughItsShallowestPath() {
        let deadline = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        var deep = deadline
        for _ in 0..<40 {
            deep = NSError(domain: "Wrapper", code: 1,
                userInfo: [NSMultipleUnderlyingErrorsKey: [deep]])
        }
        let error = CKError(.unknownItem, userInfo: [
            NSMultipleUnderlyingErrorsKey: [deep, deadline],
            NSUnderlyingErrorKey: deep,
        ])
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertEqual(cloudKitErrors(in: error).filter { $0.code == .requestRateLimited }.count, 1)
    }

    func testRepeatedAggregateChildrenDoNotMultiplyVisitedCloudErrors() {
        let account = CKError(.notAuthenticated) as NSError
        let shared = NSError(domain: "Aggregate", code: 1,
            userInfo: [NSMultipleUnderlyingErrorsKey: Array(repeating: account, count: 128)])
        let root = CKError(.serverRecordChanged,
            userInfo: [NSMultipleUnderlyingErrorsKey: Array(repeating: shared, count: 128)])
        XCTAssertTrue(CloudKitRetryConstraints(root).blocksAccountOperations)
        XCTAssertEqual(cloudKitErrors(in: root).count, 2)
    }

    func testNonCloudWrapperAggregateAndSingleCauseRemainIndependent() {
        let auth = CKError(.notAuthenticated) as NSError
        let token = CKError(.changeTokenExpired) as NSError
        let root = NSError(domain: "LocalAggregate", code: 2, userInfo: [
            NSUnderlyingErrorKey: auth,
            NSMultipleUnderlyingErrorsKey: [token],
        ])
        let constraints = CloudKitRetryConstraints(root)
        XCTAssertTrue(constraints.blocksAccountOperations)
        XCTAssertTrue(constraints.requestsTokenRecovery)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
    }
}


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    func testDeleteMissAggregateAccountStopAcknowledgesWithoutAnotherProbe() async throws {
        for floor: TimeInterval in [0, 137] {
            let aggregate = NSError(domain: "DeletionAggregate", code: 1, userInfo: [
                NSMultipleUnderlyingErrorsKey: [
                    CKError(.notAuthenticated) as NSError,
                    CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: floor]) as NSError,
                ],
            ])
            let (adapter, transport, account, failure) = try await run(
                .deleteMissing, .none, failsAccountAfterResult: true,
                repairUnderlyingError: aggregate
            )
            let error = try XCTUnwrap(failure)
            let constraints = CloudKitRetryConstraints(error)
            XCTAssertTrue(constraints.blocksAccountOperations)
            XCTAssertEqual(constraints.serverMinimum, floor)
            XCTAssertTrue(constraints.requiresDeferredRetry)
            XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
            XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"])
            XCTAssertTrue(adapter.pending.isEmpty)
            XCTAssertTrue(adapter.rebased.isEmpty)
            let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
                as? [AnyHashable: Error])
            XCTAssertNil(items[CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)])
            let envelope = try XCTUnwrap(items["acknowledgedDeletionConstraints"] as? CKError)
            let causes = try XCTUnwrap(envelope.userInfo[CKPartialErrorsByItemIDKey]
                as? [CKRecord.ID: NSError])
            let target = try XCTUnwrap(causes[.init(recordName: "target", zoneID: adapter.recordZoneID)])
            XCTAssertTrue((target.userInfo[NSUnderlyingErrorKey] as? NSError) === aggregate)
            let probes = await account.callsAfterResult
            let calls = await transport.mutationCount
            XCTAssertEqual(probes, 0)
            XCTAssertEqual(calls, 1)
        }
    }

    @BigSyncBackgroundActor
    func testDeleteMissAggregateAndSiblingDeadlineSurviveLocalAcknowledgementFailure() async throws {
        let local = NSError(domain: "LocalAcknowledgementFailure", code: 29)
        let aggregate = NSError(domain: "DeletionAggregate", code: 1, userInfo: [
            NSMultipleUnderlyingErrorsKey: [
                CKError(.changeTokenExpired) as NSError,
                CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError,
            ],
        ])
        let (adapter, transport, account, failure) = try await run(
            .deleteMissing, .none,
            sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 73]),
            acknowledgeFailure: local, repairUnderlyingError: aggregate
        )
        let error = try XCTUnwrap(failure)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertTrue(constraints.requestsTokenRecovery)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        XCTAssertTrue(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError) === local)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error])
        XCTAssertNil(items[CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)])
        XCTAssertNil(items[CKRecord.ID(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertNotNil(items[CKRecord.ID(recordName: "other", zoneID: adapter.recordZoneID)])
        XCTAssertNotNil(items["acknowledgedDeletionConstraints"])
        XCTAssertTrue(adapter.deleted.isEmpty)
        XCTAssertEqual(adapter.pending, ["success", "target", "other"])
        XCTAssertTrue(adapter.rebased.isEmpty)
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1)
        XCTAssertEqual(probes, 1)
    }

    @BigSyncBackgroundActor
    func testDeleteMissAggregateKeepsAcknowledgementCancellationTerminal() async throws {
        let aggregate = NSError(domain: "DeletionAggregate", code: 1, userInfo: [
            NSMultipleUnderlyingErrorsKey: [
                CKError(.notAuthenticated) as NSError,
                CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError,
            ],
        ])
        let (adapter, transport, account, failure) = try await run(
            .deleteMissing, .none, acknowledgeFailure: CancellationError(),
            repairUnderlyingError: aggregate
        )
        XCTAssertTrue(failure is CancellationError)
        XCTAssertTrue(adapter.deleted.isEmpty)
        XCTAssertEqual(adapter.pending, ["success", "target"])
        XCTAssertTrue(adapter.rebased.isEmpty)
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1)
        XCTAssertEqual(probes, 0)
    }
}


// The mutation response dictionary has one slot per ID. Reject ambiguous or
// out-of-zone preparation before lookup/write rather than after remote effects.
extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func rejectPreparedBatch(
        _ route: ResponseRoute,
        _ alteration: ResponsePreparationAlteration,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let (adapter, transport, _, failure) = try await run(
            route, .none, preparationAlteration: alteration
        )
        let error = try XCTUnwrap(failure, file: file, line: line)
        let expected = BigSyncRecordRebaseError.inconsistentReceipt("target") as NSError
        XCTAssertEqual((error as NSError).domain, expected.domain, file: file, line: line)
        XCTAssertEqual((error as NSError).code, expected.code, file: file, line: line)
        XCTAssertEqual(adapter.pending, ["target", "success"], file: file, line: line)
        XCTAssertTrue(adapter.uploaded.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.requeued.isEmpty, file: file, line: line)
        let mutations = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(mutations, 0, file: file, line: line)
        XCTAssertEqual(lookups, 0, file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testUploadPreparationRejectsDuplicateIdentityBeforeTransport() async throws {
        try await rejectPreparedBatch(.save, .duplicateIdentity)
    }
    @BigSyncBackgroundActor
    func testUploadPreparationRejectsConflictingGenerationBeforeTransport() async throws {
        try await rejectPreparedBatch(.save, .conflictingGeneration)
    }
    @BigSyncBackgroundActor
    func testUploadPreparationRejectsForeignZoneBeforeTransport() async throws {
        try await rejectPreparedBatch(.save, .zone)
    }
    @BigSyncBackgroundActor
    func testUploadPreparationRejectsForeignOwnerBeforeTransport() async throws {
        try await rejectPreparedBatch(.save, .owner)
    }
    @BigSyncBackgroundActor
    func testUncertainPreparationRejectsDuplicateIdentityBeforeLookup() async throws {
        try await rejectPreparedBatch(.lookup, .duplicateIdentity)
    }
    @BigSyncBackgroundActor
    func testUncertainPreparationRejectsConflictingGenerationBeforeLookup() async throws {
        try await rejectPreparedBatch(.lookup, .conflictingGeneration)
    }
    @BigSyncBackgroundActor
    func testUncertainPreparationRejectsForeignZoneBeforeLookup() async throws {
        try await rejectPreparedBatch(.lookup, .zone)
    }
    @BigSyncBackgroundActor
    func testUncertainPreparationRejectsForeignOwnerBeforeLookup() async throws {
        try await rejectPreparedBatch(.lookup, .owner)
    }
    @BigSyncBackgroundActor
    func testDeletionPreparationRejectsDuplicateIdentityBeforeTransport() async throws {
        try await rejectPreparedBatch(.deleteMissing, .duplicateIdentity)
    }
    @BigSyncBackgroundActor
    func testDeletionPreparationRejectsConflictingGenerationBeforeTransport() async throws {
        try await rejectPreparedBatch(.deleteMissing, .conflictingGeneration)
    }
    @BigSyncBackgroundActor
    func testDeletionPreparationRejectsForeignZoneBeforeTransport() async throws {
        try await rejectPreparedBatch(.deleteMissing, .zone)
    }
    @BigSyncBackgroundActor
    func testDeletionPreparationRejectsForeignOwnerBeforeTransport() async throws {
        try await rejectPreparedBatch(.deleteMissing, .owner)
    }
}


// A conflict selected for metadata repair is still an outstanding record
// failure until that repair returns. Acknowledged absence is a separate fact.
extension SyncMutationResponseIdentityTests {
    private func deletionConflict(_ name: String) -> CKError {
        let record = CKRecord(recordType: "IdentityFixture", recordID: .init(
            recordName: name, zoneID: .init(zoneName: "response-identity")
        ))
        return CKError(.serverRecordChanged, userInfo: [
            CKRecordChangedErrorServerRecordKey: record,
        ])
    }

    @BigSyncBackgroundActor
    func testPendingDeletionConflictSurvivesPostAcknowledgementAccountFailure() async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteConflict, .none, failAccountAfterResultCall: 2
        )
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let original = try XCTUnwrap(items[target])
        XCTAssertEqual(original.domain, CKErrorDomain)
        XCTAssertEqual(original.code, CKError.serverRecordChanged.rawValue)
        XCTAssertEqual((original.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord)?.recordID, target)
        XCTAssertNil(items[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
                       "ResponseAccountFailure")
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertEqual(adapter.pending, ["target"])
        let probes = await account.callsAfterResult
        let calls = await transport.mutationCount
        XCTAssertEqual(probes, 2)
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testPostAcknowledgementAccountFailureKeepsConflictAndUnresolvedSibling() async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteConflict, .none, sibling: CKError(.quotaExceeded),
            failAccountAfterResultCall: 2
        )
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertEqual(items[.init(recordName: "target", zoneID: adapter.recordZoneID)]?.code,
                       CKError.serverRecordChanged.rawValue)
        XCTAssertEqual(items[.init(recordName: "other", zoneID: adapter.recordZoneID)]?.code,
                       CKError.Code.quotaExceeded.rawValue)
        XCTAssertEqual(items.count, 2)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
                       "ResponseAccountFailure")
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertEqual(adapter.pending, ["target", "other"])
        let probes = await account.callsAfterResult
        let calls = await transport.mutationCount
        XCTAssertEqual(probes, 2)
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    private func requirePendingConflictBesideAcknowledgedConstraint(
        underlying: Error? = nil, floor: TimeInterval? = 73,
        acknowledgementError: Error? = nil,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let conflict = deletionConflict("other")
        let (adapter, transport, account, failure) = try await run(
            .deleteMissing, .none, sibling: conflict,
            acknowledgeFailure: acknowledgementError,
            conflictRetryAfter: floor, repairUnderlyingError: underlying
        )
        let error = try XCTUnwrap(failure, file: file, line: line)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error], file: file, line: line)
        let other = CKRecord.ID(recordName: "other", zoneID: adapter.recordZoneID)
        let original = try XCTUnwrap(items[other] as? CKError, file: file, line: line)
        XCTAssertEqual(original.code, .serverRecordChanged, file: file, line: line)
        XCTAssertTrue((original.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord)
            === (conflict.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord), file: file, line: line)
        let envelope = try XCTUnwrap(items["acknowledgedDeletionConstraints"] as? CKError,
                                   file: file, line: line)
        let causes = try XCTUnwrap(envelope.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError], file: file, line: line)
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        XCTAssertEqual(causes[target]?.code, CKError.unknownItem.rawValue, file: file, line: line)
        XCTAssertNil(items[target], file: file, line: line)
        XCTAssertNil(items[CKRecord.ID(recordName: "success", zoneID: adapter.recordZoneID)], file: file, line: line)
        let constraints = CloudKitRetryConstraints(error)
        if let floor { XCTAssertEqual(constraints.serverMinimum, floor, file: file, line: line) }
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        if let acknowledgementError {
            XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
            XCTAssertEqual(adapter.pending, ["target", "success", "other"], file: file, line: line)
            XCTAssertEqual((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError,
                           acknowledgementError as NSError, file: file, line: line)
        } else {
            XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["target", "success"], file: file, line: line)
            XCTAssertEqual(adapter.pending, ["other"], file: file, line: line)
        }
        let probes = await account.callsAfterResult
        let calls = await transport.mutationCount
        XCTAssertEqual(probes, constraints.blocksAccountOperations ? 0 : 1, file: file, line: line)
        XCTAssertEqual(calls, 1, file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionDeadlineCannotHideUnrepairedConflict() async throws {
        try await requirePendingConflictBesideAcknowledgedConstraint()
        try await requirePendingConflictBesideAcknowledgedConstraint(floor: 0)
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionAccountStopCannotHideUnrepairedConflict() async throws {
        for code: CKError.Code in [.notAuthenticated, .accountTemporarilyUnavailable] {
            try await requirePendingConflictBesideAcknowledgedConstraint(underlying: CKError(code), floor: nil)
        }
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionAggregateRecoveryCannotHideUnrepairedConflict() async throws {
        let aggregate = NSError(domain: "DeletionAggregate", code: 1, userInfo: [
            NSMultipleUnderlyingErrorsKey: [
                CKError(.changeTokenExpired) as NSError,
                CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError,
            ],
        ])
        try await requirePendingConflictBesideAcknowledgedConstraint(underlying: aggregate, floor: 137)
    }

    @BigSyncBackgroundActor
    func testFailedDeletionAcknowledgementKeepsPendingConflictAndReceiptConstraint() async throws {
        try await requirePendingConflictBesideAcknowledgedConstraint(
            acknowledgementError: NSError(domain: "LocalAcknowledgementFailure", code: 29)
        )
    }

    @BigSyncBackgroundActor
    func testCompletedDeletionRepairIsNotReintroducedAfterLaterAccountFailure() async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteConflict, .none, failAccountAfterResultCall: 3
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual((error as NSError).domain, "ResponseAccountFailure")
        XCTAssertNil((error as NSError).userInfo[CKPartialErrorsByItemIDKey])
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        XCTAssertEqual(adapter.rebased.map { $0.recordID.recordName }, ["target"])
        XCTAssertEqual(adapter.pending, ["target"])
        let probes = await account.callsAfterResult
        let calls = await transport.mutationCount
        XCTAssertEqual(probes, 3)
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testMixedDeletionAcknowledgementCancellationRemainsTerminal() async throws {
        let (adapter, transport, _, failure) = try await run(
            .deleteMissing, .none, sibling: deletionConflict("other"),
            acknowledgeFailure: CancellationError(), conflictRetryAfter: 73
        )
        XCTAssertTrue(failure is CancellationError)
        XCTAssertTrue(adapter.deleted.isEmpty)
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertEqual(adapter.pending, ["target", "success", "other"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }
}


// Classification is not settlement. Returned conflicts remain failures until
// their local repair actually completes; an earlier stop must preserve them.
extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireUnrepairedSiblingConflict(
        retryAfter: TimeInterval? = nil,
        underlying: CKError? = nil,
        acknowledgementError: Error? = nil,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let zone = CKRecordZone.ID(zoneName: "response-identity")
        let other = CKRecord.ID(recordName: "other", zoneID: zone)
        let server = CKRecord(recordType: "IdentityFixture", recordID: other)
        server["text"] = "unrepaired-server-value" as CKRecordValue
        let conflict = CKError(.serverRecordChanged,
            userInfo: [CKRecordChangedErrorServerRecordKey: server])
        let (adapter, transport, account, failure) = try await run(
            .deleteMissing, .none, sibling: conflict,
            acknowledgeFailure: acknowledgementError,
            conflictRetryAfter: retryAfter, repairUnderlyingError: underlying
        )
        let error = try XCTUnwrap(failure, file: file, line: line)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error], file: file, line: line)
        let unhandled = try XCTUnwrap(items[other] as? NSError,
            "Selecting a conflict for later repair is not settlement", file: file, line: line)
        XCTAssertEqual(unhandled.domain, CKErrorDomain, file: file, line: line)
        XCTAssertEqual(unhandled.code, CKError.serverRecordChanged.rawValue, file: file, line: line)
        XCTAssertTrue((unhandled.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord) === server,
            file: file, line: line)
        let envelope = try XCTUnwrap(items["acknowledgedDeletionConstraints"] as? CKError,
            file: file, line: line)
        let causes = try XCTUnwrap(envelope.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError], file: file, line: line)
        XCTAssertEqual(causes[.init(recordName: "target", zoneID: zone)]?.code,
            CKError.unknownItem.rawValue, file: file, line: line)
        XCTAssertNil(items[CKRecord.ID(recordName: "target", zoneID: zone)], file: file, line: line)
        XCTAssertNil(items[CKRecord.ID(recordName: "success", zoneID: zone)], file: file, line: line)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertTrue(constraints.codes.contains(.serverRecordChanged), file: file, line: line)
        if let retryAfter { XCTAssertEqual(constraints.serverMinimum, retryAfter, file: file, line: line) }
        if let underlying { XCTAssertTrue(constraints.codes.contains(underlying.code), file: file, line: line) }
        if let acknowledgementError {
            XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
            XCTAssertEqual(adapter.pending, ["success", "target", "other"], file: file, line: line)
            XCTAssertTrue(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)
                === (acknowledgementError as NSError), file: file, line: line)
        } else {
            XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"], file: file, line: line)
            XCTAssertEqual(adapter.pending, ["other"], file: file, line: line)
        }
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1, file: file, line: line)
        XCTAssertEqual(probes,
            underlying?.code == .notAuthenticated || underlying?.code == .accountTemporarilyUnavailable ? 0 : 1,
            file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionDeadlinePreservesUnrepairedSiblingConflict() async throws {
        for floor: TimeInterval in [0, 73] {
            try await requireUnrepairedSiblingConflict(retryAfter: floor)
        }
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionAccountStopPreservesUnrepairedSiblingConflict() async throws {
        for code: CKError.Code in [.notAuthenticated, .accountTemporarilyUnavailable] {
            try await requireUnrepairedSiblingConflict(underlying: CKError(code))
        }
    }

    @BigSyncBackgroundActor
    func testAcknowledgedDeletionRecoveryStopPreservesUnrepairedSiblingConflict() async throws {
        for code: CKError.Code in [.changeTokenExpired, .networkFailure, .quotaExceeded] {
            try await requireUnrepairedSiblingConflict(underlying: CKError(code))
        }
    }

    @BigSyncBackgroundActor
    func testRejectedDeletionAcknowledgementRetainsConflictAndIndependentDeadline() async throws {
        try await requireUnrepairedSiblingConflict(retryAfter: 73,
            acknowledgementError: NSError(domain: "AcknowledgementRejected", code: 9))
    }

    @BigSyncBackgroundActor
    func testPostAcknowledgementAccountFailurePreservesUnrepairedDeletionConflict() async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteConflict, .none, failAccountAfterResultCall: 2
        )
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let target = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        XCTAssertEqual(items[target]?.domain, CKErrorDomain)
        XCTAssertEqual(items[target]?.code, CKError.serverRecordChanged.rawValue)
        XCTAssertEqual((items[target]?.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord)?.recordID,
            target)
        XCTAssertNil(items[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain,
            "ResponseAccountFailure")
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertEqual(adapter.pending, ["target"])
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1)
        XCTAssertEqual(probes, 2)
    }

    @BigSyncBackgroundActor
    func testPostAcknowledgementAccountFailureStillPreservesMissingDeletionSlot() async throws {
        let (adapter, _, account, failure) = try await run(
            .deleteConflict, .missingResult, failAccountAfterResultCall: 2
        )
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let missing = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        XCTAssertEqual(items[missing]?.domain, NSCocoaErrorDomain)
        XCTAssertEqual(items[missing]?.code, CocoaError.coderValueNotFound.rawValue)
        XCTAssertNil(items[.init(recordName: "success", zoneID: adapter.recordZoneID)])
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        XCTAssertEqual(adapter.pending, ["target"])
        let probes = await account.callsAfterResult
        XCTAssertEqual(probes, 2)
    }

    @BigSyncBackgroundActor
    func testPostRepairAccountFailureDoesNotResurrectHandledDeletionConflict() async throws {
        let (adapter, transport, account, failure) = try await run(
            .deleteConflict, .none, failAccountAfterResultCall: 3
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual((error as NSError).domain, "ResponseAccountFailure")
        XCTAssertNil((error as NSError).userInfo[CKPartialErrorsByItemIDKey])
        XCTAssertEqual(adapter.rebased.map { $0.recordID.recordName }, ["target"])
        XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"])
        let probes = await account.callsAfterResult
        let calls = await transport.mutationCount
        XCTAssertEqual(probes, 3)
        XCTAssertEqual(calls, 1)
    }
}

extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    func testMissingUploadRepairFailurePreservesUnstartedSiblingConflict() async throws {
        let zone = CKRecordZone.ID(zoneName: "response-identity")
        let other = CKRecord.ID(recordName: "other", zoneID: zone)
        let server = CKRecord(recordType: "IdentityFixture", recordID: other)
        let conflict = CKError(.serverRecordChanged,
            userInfo: [CKRecordChangedErrorServerRecordKey: server])
        let local = NSError(domain: "RequeueRejected", code: 8)
        let (adapter, transport, _, failure) = try await run(
            .saveMissing, .none, sibling: conflict, requeueFailure: local
        )
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertEqual(items[other]?.code, CKError.serverRecordChanged.rawValue)
        XCTAssertTrue((items[other]?.userInfo[CKRecordChangedErrorServerRecordKey] as? CKRecord) === server)
        XCTAssertTrue(items[.init(recordName: "target", zoneID: zone)] === local)
        XCTAssertTrue(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError) === local)
        XCTAssertNil(items[.init(recordName: "success", zoneID: zone)])
        XCTAssertTrue(CloudKitRetryConstraints(error).codes.contains(.serverRecordChanged))
        XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"])
        XCTAssertEqual(adapter.pending, ["target", "other"])
        XCTAssertEqual(adapter.requeueInvocations, 1)
        XCTAssertTrue(adapter.requeued.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testMissingUploadRepairFailureStillPreservesIndependentSiblingDeadline() async throws {
        let local = NSError(domain: "RequeueRejected", code: 8)
        let (adapter, transport, _, failure) = try await run(
            .saveMissing, .none,
            sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]),
            requeueFailure: local
        )
        let error = try XCTUnwrap(failure)
        XCTAssertEqual(CloudKitRetryConstraints(error).serverMinimum, 137)
        XCTAssertTrue(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError) === local)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertTrue(items[.init(recordName: "target", zoneID: adapter.recordZoneID)] === local)
        XCTAssertEqual(items[.init(recordName: "other", zoneID: adapter.recordZoneID)]?.code,
            CKError.Code.requestRateLimited.rawValue)
        XCTAssertEqual(adapter.requeueInvocations, 1)
        XCTAssertEqual(adapter.pending, ["target", "other"])
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1)
    }

    @BigSyncBackgroundActor
    func testMissingUploadRepairWithoutSiblingFailureRetainsIsolatedLocalError() async throws {
        let local = NSError(domain: "RequeueRejected", code: 8)
        let (adapter, _, _, failure) = try await run(.saveMissing, .none, requeueFailure: local)
        XCTAssertTrue((failure as NSError?) === local)
        XCTAssertNil((failure as NSError?)?.userInfo[CKPartialErrorsByItemIDKey])
        XCTAssertEqual(adapter.requeueInvocations, 1)
        XCTAssertEqual(adapter.pending, ["target"])
        XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"])
    }

    @BigSyncBackgroundActor
    func testMissingUploadRepairCancellationDoesNotBecomeSiblingPartialFailure() async throws {
        let zone = CKRecordZone.ID(zoneName: "response-identity")
        let conflict = CKError(.serverRecordChanged, userInfo: [
            CKRecordChangedErrorServerRecordKey: CKRecord(recordType: "IdentityFixture",
                recordID: .init(recordName: "other", zoneID: zone)),
        ])
        let (adapter, _, _, failure) = try await run(
            .saveMissing, .none, sibling: conflict, requeueFailure: CancellationError()
        )
        XCTAssertTrue(failure is CancellationError)
        XCTAssertEqual(adapter.requeueInvocations, 1)
        XCTAssertEqual(adapter.pending, ["target", "other"])
        XCTAssertTrue(adapter.imported.isEmpty)
    }
}


// A synchronous adapter getter is a callout, and a completed import may return
// an outcome after its calling task was cancelled. Neither grants successful
// drain completion or authority to replace cancellation with a semantic error.
extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireCancellationAtPendingStateRead(
        _ route: ResponseRoute,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let caller = Task { @BigSyncBackgroundActor in
            try await self.run(route, .none, cancelOnPendingStateRead: true)
        }
        let (adapter, transport, _, failure) = try await caller.value
        XCTAssertTrue(failure is CancellationError, file: file, line: line)
        // The receipts committed before cancellation remain committed.
        XCTAssertTrue(adapter.pending.isEmpty, file: file, line: line)
        if route.deletes {
            XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"], file: file, line: line)
        } else {
            XCTAssertEqual(Set(adapter.uploaded.map { $0.recordID.recordName }), ["success", "target"], file: file, line: line)
        }
        let requests = await transport.mutationCount
        XCTAssertEqual(requests, 1, file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testUploadDrainRejectsCancellationFromFinalPendingStateRead() async throws {
        try await requireCancellationAtPendingStateRead(.save)
    }

    @BigSyncBackgroundActor
    func testDeletionDrainRejectsCancellationFromFinalPendingStateRead() async throws {
        try await requireCancellationAtPendingStateRead(.deleteMissing)
    }

    @BigSyncBackgroundActor
    private func requireCancelledImportOutcome(
        _ route: ResponseRoute, quarantined: Bool,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let caller = Task { @BigSyncBackgroundActor in
            try await self.run(route, .none, cancelAfterConflictImport: true,
                               quarantineImportedRecords: quarantined)
        }
        let (adapter, transport, _, failure) = try await caller.value
        XCTAssertTrue(failure is CancellationError, file: file, line: line)
        XCTAssertFalse(adapter.imported.isEmpty, "Must reach the actual import callout", file: file, line: line)
        XCTAssertEqual(adapter.persistCount, 0, file: file, line: line)
        if !route.looksUp {
            XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"], file: file, line: line)
            XCTAssertEqual(adapter.pending, ["target"], file: file, line: line)
        }
        let requests = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(requests, route.looksUp ? 0 : 1, file: file, line: line)
        XCTAssertEqual(lookups, route.looksUp ? 1 : 0, file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testCancelledUploadConflictQuarantineDoesNotReplaceCancellation() async throws {
        try await requireCancelledImportOutcome(.saveConflict, quarantined: true)
    }

    @BigSyncBackgroundActor
    func testCancelledUploadConflictOrdinaryOutcomeRemainsCancellation() async throws {
        try await requireCancelledImportOutcome(.saveConflict, quarantined: false)
    }

    @BigSyncBackgroundActor
    func testCancelledLookupQuarantineRemainsCancellation() async throws {
        try await requireCancelledImportOutcome(.lookup, quarantined: true)
    }

    @BigSyncBackgroundActor
    func testLiveUploadConflictQuarantineRemainsSemanticStop() async throws {
        let (adapter, transport, _, failure) = try await run(
            .saveConflict, .none, quarantineImportedRecords: true
        )
        let semantic = try XCTUnwrap(failure as? BigSyncSemanticUploadConflictError)
        XCTAssertEqual(semantic.recordNames, ["target"])
        XCTAssertEqual(adapter.pending, ["target"])
        XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"])
        let requests = await transport.mutationCount
        XCTAssertEqual(requests, 1)
    }
}


// Shrinking retry must make progress even if an adapter ignores a requested
// limit. The watchdog is only a failing-fixture escape, never the pass condition.
extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireSizeRetryProgress(
        _ route: ResponseRoute, response: ResponseSizeLimit,
        respectsLimit: Bool, file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let (adapter, transport, _, failure) = try await run(
            route, .none, sizeLimit: response, respectsPreparationLimit: respectsLimit
        )
        let error = try XCTUnwrap(failure, file: file, line: line)
        XCTAssertTrue(CloudKitRetryConstraints(error).containsOnlySizeLimitFailures,
            "Preserve the real size failure, not the fixture retry-watchdog error", file: file, line: line)
        XCTAssertEqual(adapter.preparationLimits.count, 2, file: file, line: line)
        XCTAssertEqual(adapter.preparationLimits.last, 1, file: file, line: line)
        XCTAssertTrue(zip(adapter.preparationLimits, adapter.preparationLimits.dropFirst()).allSatisfy { $1 < $0 },
            "Each retry must strictly reduce its requested limit", file: file, line: line)
        let requests = await transport.mutationCount
        let sizes = await transport.attemptedMutationSizes
        XCTAssertEqual(requests, 2, file: file, line: line)
        XCTAssertEqual(sizes, respectsLimit || response == .targetOnly ? [2, 1] : [2, 2], file: file, line: line)
        if response == .targetOnly {
            XCTAssertEqual(adapter.pending, ["target"], file: file, line: line)
            if route.deletes { XCTAssertEqual(adapter.deleted.map(\.recordName), ["success"], file: file, line: line) }
            else { XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"], file: file, line: line) }
        } else {
            XCTAssertEqual(adapter.pending, ["success", "target"], file: file, line: line)
            XCTAssertTrue(adapter.uploaded.isEmpty, file: file, line: line)
            XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
        }
    }

    @BigSyncBackgroundActor
    func testThrownUploadSizeLimitStopsWhenPreparationIgnoresReducedLimit() async throws {
        try await requireSizeRetryProgress(.save, response: .thrown, respectsLimit: false)
    }

    @BigSyncBackgroundActor
    func testPerItemUploadSizeLimitStopsWhenPreparationIgnoresReducedLimit() async throws {
        try await requireSizeRetryProgress(.save, response: .perItem, respectsLimit: false)
    }

    @BigSyncBackgroundActor
    func testThrownDeletionSizeLimitStopsWhenPreparationIgnoresReducedLimit() async throws {
        try await requireSizeRetryProgress(.deleteMissing, response: .thrown, respectsLimit: false)
    }

    @BigSyncBackgroundActor
    func testPerItemDeletionSizeLimitStopsWhenPreparationIgnoresReducedLimit() async throws {
        try await requireSizeRetryProgress(.deleteMissing, response: .perItem, respectsLimit: false)
    }

    @BigSyncBackgroundActor
    func testCompliantUploadSizeRetryStillReachesSingleton() async throws {
        for response: ResponseSizeLimit in [.thrown, .perItem] {
            try await requireSizeRetryProgress(.save, response: response, respectsLimit: true)
        }
    }

    @BigSyncBackgroundActor
    func testCompliantDeletionSizeRetryStillReachesSingleton() async throws {
        for response: ResponseSizeLimit in [.thrown, .perItem] {
            try await requireSizeRetryProgress(.deleteMissing, response: response, respectsLimit: true)
        }
    }

    @BigSyncBackgroundActor
    func testPartialSizeFailureRetainsUploadSiblingAcknowledgement() async throws {
        try await requireSizeRetryProgress(.save, response: .targetOnly, respectsLimit: true)
    }

    @BigSyncBackgroundActor
    func testPartialSizeFailureRetainsDeletionSiblingAcknowledgement() async throws {
        try await requireSizeRetryProgress(.deleteMissing, response: .targetOnly, respectsLimit: true)
    }

    @BigSyncBackgroundActor
    func testSizeRetryStillHonorsExplicitServerDelay() async throws {
        for route: ResponseRoute in [.save, .deleteMissing] {
            for response: ResponseSizeLimit in [.thrown, .perItem] {
                for floor: TimeInterval in [0, 137] {
                    let (adapter, transport, _, failure) = try await run(
                        route, .none, conflictRetryAfter: floor, sizeLimit: response
                    )
                    let constraints = CloudKitRetryConstraints(try XCTUnwrap(failure))
                    XCTAssertEqual(constraints.serverMinimum, floor)
                    XCTAssertTrue(constraints.requiresDeferredRetry)
                    let requests = await transport.mutationCount
                    XCTAssertEqual(requests, 1)
                    XCTAssertEqual(adapter.preparationLimits.count, 1)
                    XCTAssertEqual(adapter.pending, ["success", "target"])
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testSizeRetryDoesNotEraseAnIndependentLocalFailure() async throws {
        let local = NSError(domain: "IndependentLocalFailure", code: 81)
        for route: ResponseRoute in [.save, .deleteMissing] {
            let (adapter, transport, _, failure) = try await run(
                route, .none, repairUnderlyingError: local, sizeLimit: .perItem
            )
            XCTAssertFalse(CloudKitRetryConstraints(try XCTUnwrap(failure)).containsOnlySizeLimitFailures)
            let requests = await transport.mutationCount
            XCTAssertEqual(requests, 1)
            XCTAssertEqual(adapter.preparationLimits.count, 1)
            XCTAssertEqual(adapter.pending, ["success", "target"])
        }
    }
}


private final class ResponseInternalErrorCycle: NSError, @unchecked Sendable {
    init() { super.init(domain: "InternalCycle", code: 1, userInfo: nil) }
    required init?(coder: NSCoder) { super.init(coder: coder) }
    override var userInfo: [String: Any] { [NSUnderlyingErrorKey: self] }
}

// A depth-limited scan is not proof that no independent constraint exists.
// Preserve bounded traversal, but do not convert incomplete evidence into
// immediate repair permission. No deeper CloudKit cause is invented here.
extension SyncMutationResponseIdentityTests {
    private func wrappedError(_ leaf: NSError, levels: Int) -> NSError {
        var result = leaf
        for _ in 0..<levels {
            result = NSError(domain: "InternalErrorWrapper", code: 1,
                userInfo: [NSUnderlyingErrorKey: result])
        }
        return result
    }

    @BigSyncBackgroundActor
    private func requireIncompleteGraphStopsRepair(
        _ route: ResponseRoute, file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let deep = wrappedError(CKError(.notAuthenticated) as NSError, levels: 40)
        let (adapter, transport, _, failure) = try await run(
            route, .none, repairUnderlyingError: deep
        )
        let error = try XCTUnwrap(failure,
            "A truncated graph cannot establish immediate-repair permission", file: file, line: line)
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.requeued.isEmpty, file: file, line: line)
        // We keep the original error graph rather than pretending to discover
        // an account code beyond the existing traversal budget.
        XCTAssertFalse(CloudKitRetryConstraints(error).blocksAccountOperations, file: file, line: line)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error], file: file, line: line)
        let id = CKRecord.ID(recordName: "target", zoneID: adapter.recordZoneID)
        let recordError: NSError
        if route == .deleteMissing {
            let envelope = try XCTUnwrap(items["acknowledgedDeletionConstraints"] as? CKError,
                file: file, line: line)
            let causes = try XCTUnwrap(envelope.userInfo[CKPartialErrorsByItemIDKey]
                as? [CKRecord.ID: NSError], file: file, line: line)
            recordError = try XCTUnwrap(causes[id], file: file, line: line)
            XCTAssertNil(items[id], file: file, line: line)
            XCTAssertEqual(Set(adapter.deleted.map(\.recordName)), ["success", "target"], file: file, line: line)
            XCTAssertTrue(adapter.pending.isEmpty, file: file, line: line)
        } else {
            recordError = try XCTUnwrap(items[id] as? NSError, file: file, line: line)
            XCTAssertTrue(adapter.pending.contains("target"), file: file, line: line)
        }
        XCTAssertTrue((recordError.userInfo[NSUnderlyingErrorKey] as? NSError) === deep, file: file, line: line)
        let requests = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(requests, route.looksUp ? 0 : 1, file: file, line: line)
        XCTAssertEqual(lookups, route.looksUp ? 1 : 0, file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testTruncatedUploadConflictGraphDoesNotAuthorizeImmediateRepair() async throws {
        try await requireIncompleteGraphStopsRepair(.saveConflict)
    }

    @BigSyncBackgroundActor
    func testTruncatedMissingUploadGraphDoesNotAuthorizeImmediateRepair() async throws {
        try await requireIncompleteGraphStopsRepair(.saveMissing)
    }

    @BigSyncBackgroundActor
    func testTruncatedDeletionConflictGraphDoesNotAuthorizeImmediateRepair() async throws {
        try await requireIncompleteGraphStopsRepair(.deleteConflict)
    }

    @BigSyncBackgroundActor
    func testTruncatedLookupMissGraphDoesNotAuthorizeImmediateMutation() async throws {
        try await requireIncompleteGraphStopsRepair(.lookupMissing)
    }

    @BigSyncBackgroundActor
    func testTruncatedDeletionMissGraphAcknowledgesWithoutDiscardingUnexaminedEvidence() async throws {
        try await requireIncompleteGraphStopsRepair(.deleteMissing)
    }

    @BigSyncBackgroundActor
    func testCompleteRepairGraphAtDepthBoundaryRemainsEligible() async throws {
        // Root repair error at depth zero, wrappers at 1...levels, leaf last.
        for levels in [29, 30, 31] {
            let detail = wrappedError(NSError(domain: "InternalDetail", code: 1), levels: levels)
            let (adapter, transport, _, failure) = try await run(
                .saveMissing, .none, repairUnderlyingError: detail
            )
            let requests = await transport.mutationCount
            if levels < 31 {
                XCTAssertNil(failure)
                XCTAssertTrue(adapter.pending.isEmpty)
                XCTAssertEqual(requests, 2)
            } else {
                XCTAssertNotNil(failure)
                XCTAssertEqual(adapter.pending, ["target"])
                XCTAssertTrue(adapter.requeued.isEmpty)
                XCTAssertEqual(requests, 1)
            }
        }
    }

    @BigSyncBackgroundActor
    func testDeepAliasOfAlreadyInspectedErrorDoesNotInvalidateCompleteGraph() async throws {
        let leaf = NSError(domain: "SharedInternalDetail", code: 1)
        let deep = wrappedError(leaf, levels: 30)
        let aggregate = NSError(domain: "InternalAggregate", code: 1, userInfo: [
            NSUnderlyingErrorKey: deep,
            NSMultipleUnderlyingErrorsKey: [leaf],
        ])
        let (adapter, transport, _, failure) = try await run(
            .saveMissing, .none, repairUnderlyingError: aggregate
        )
        XCTAssertNil(failure)
        XCTAssertTrue(adapter.pending.isEmpty)
        let requests = await transport.mutationCount
        XCTAssertEqual(requests, 2)
    }

    @BigSyncBackgroundActor
    func testFullyVisitedInternalCycleRetainsOrdinaryRepairBehavior() async throws {
        let (adapter, transport, _, failure) = try await run(
            .saveMissing, .none, repairUnderlyingError: ResponseInternalErrorCycle()
        )
        XCTAssertNil(failure)
        XCTAssertTrue(adapter.pending.isEmpty)
        let requests = await transport.mutationCount
        XCTAssertEqual(requests, 2)
    }
}


extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireMalformedRepairBlocked(_ route: ResponseRoute, partial: Bool = false,
                                               size: ResponseSizeLimit? = nil,
                                               code: CKError.Code = .networkFailure) async throws {
        let valid = CKError(code, userInfo: code == .requestRateLimited ? [CKErrorRetryAfterKey: 137] : [:])
        let malformed = partial
            ? CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey:
                ["valid": valid, "bad": "malformed"] as [String: Any]]) as NSError
            : NSError(domain: "MalformedCauseEnvelope", code: 1, userInfo:
                [NSMultipleUnderlyingErrorsKey: [valid, "malformed"] as [Any]])
        let (adapter, transport, account, failure) = try await run(route, .none,
            repairUnderlyingError: malformed, sizeLimit: size)
        let constraints = CloudKitRetryConstraints(try XCTUnwrap(failure))
        XCTAssertFalse(constraints.isErrorGraphComplete)
        XCTAssertTrue(constraints.codes.contains(code))
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        XCTAssertTrue(adapter.pending.contains("target"))
        XCTAssertTrue(adapter.requeued.isEmpty)
        XCTAssertTrue(adapter.rebased.isEmpty)
        XCTAssertTrue(adapter.imported.isEmpty)
        let calls = await transport.mutationCount
        let lookups = await transport.lookupCount
        XCTAssertEqual(calls, route.looksUp ? 0 : 1)
        XCTAssertEqual(lookups, route.looksUp ? 1 : 0)
        if constraints.blocksAccountOperations, size != .thrown {
            let callsAfterResult = await account.callsAfterResult
            XCTAssertEqual(callsAfterResult, 0)
        }
    }
    @BigSyncBackgroundActor
    func testMalformedAggregateBlocksMissingUploadRepair() async throws { try await requireMalformedRepairBlocked(.saveMissing) }
    @BigSyncBackgroundActor
    func testMalformedPartialBlocksUploadConflictRepair() async throws { try await requireMalformedRepairBlocked(.saveConflict, partial: true) }
    @BigSyncBackgroundActor
    func testMalformedAggregateBlocksDeletionConflictRepair() async throws { try await requireMalformedRepairBlocked(.deleteConflict) }
    @BigSyncBackgroundActor
    func testMalformedPartialBlocksAcceptanceMissRepair() async throws { try await requireMalformedRepairBlocked(.lookupMissing, partial: true) }
    @BigSyncBackgroundActor
    func testMalformedAggregateSaveRetainsAccountStop() async throws { try await requireMalformedRepairBlocked(.saveConflict, code: .notAuthenticated) }
    @BigSyncBackgroundActor
    func testMalformedPartialDeletionRetainsDeadline() async throws { try await requireMalformedRepairBlocked(.deleteConflict, partial: true, code: .requestRateLimited) }
    @BigSyncBackgroundActor
    func testMalformedAggregateThrownSaveCannotSplitSizeBatch() async throws { try await requireMalformedRepairBlocked(.save, size: .thrown, code: .limitExceeded) }
    @BigSyncBackgroundActor
    func testMalformedPartialThrownDeletionCannotSplitSizeBatch() async throws { try await requireMalformedRepairBlocked(.deleteConflict, partial: true, size: .thrown, code: .limitExceeded) }
    @BigSyncBackgroundActor
    func testMalformedPartialReturnedSaveCannotSplitSizeBatch() async throws { try await requireMalformedRepairBlocked(.save, partial: true, size: .perItem, code: .limitExceeded) }
    @BigSyncBackgroundActor
    func testMalformedAggregateReturnedDeletionCannotSplitSizeBatch() async throws { try await requireMalformedRepairBlocked(.deleteConflict, size: .perItem, code: .limitExceeded) }
}

// The error supplied by an API is a callback boundary, not inert metadata.
// Keep the trigger outside its lock and arm it only at the intended phase.
private final class ResponseErrorInspectionProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var armed = false
    private var triggered = false
    private var batchSize: Int?
    var didTrigger: Bool { lock.withLock { triggered } }
    var finalBatchSize: Int? { lock.withLock { batchSize } }
    func arm() { lock.withLock { if !triggered { armed = true } } }
    func recordBatchSize(_ value: Int) { lock.withLock { batchSize = value } }
    func inspect() {
        let cancel = lock.withLock {
            guard armed, !triggered else { return false }
            triggered = true
            return true
        }
        if cancel { withUnsafeCurrentTask { $0?.cancel() } }
    }
}

private final class ResponseCallbackError: NSError, @unchecked Sendable {
    private let probe: ResponseErrorInspectionProbe
    private let info: [String: Any]
    init(probe: ResponseErrorInspectionProbe, cause: Error? = nil) {
        self.probe = probe
        info = cause.map { [NSUnderlyingErrorKey: $0] } ?? [:]
        super.init(domain: "ControlledMetadataCallback", code: 1, userInfo: nil)
    }
    required init?(coder: NSCoder) { fatalError("Test-only error is not archived") }
    override var userInfo: [String: Any] {
        probe.inspect()
        return info
    }
}

extension SyncMutationResponseIdentityTests {
    @BigSyncBackgroundActor
    private func requireInspectionCancellation(
        _ route: ResponseRoute, duringAccountStop: Bool = false,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let probe = ResponseErrorInspectionProbe()
        if duringAccountStop { probe.arm() }
        let detail = ResponseCallbackError(probe: probe,
            cause: duringAccountStop ? CKError(.notAuthenticated) : nil)
        // A separate task isolates its intentional cancellation from XCTest.
        let caller = Task { @BigSyncBackgroundActor in
            try await run(route, .none, repairUnderlyingError: detail, inspectionProbe: probe)
        }
        let (adapter, transport, account, failure) = try await caller.value
        XCTAssertTrue(probe.didTrigger, "The production metadata read must execute", file: file, line: line)
        XCTAssertTrue(failure is CancellationError, file: file, line: line)
        XCTAssertTrue(adapter.uploaded.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.imported.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.rebased.isEmpty, file: file, line: line)
        XCTAssertEqual(adapter.requeueInvocations, 0, file: file, line: line)
        XCTAssertEqual(adapter.pending, ["success", "target"], file: file, line: line)
        let calls = await transport.mutationCount
        let probes = await account.callsAfterResult
        XCTAssertEqual(calls, 1, file: file, line: line)
        XCTAssertEqual(probes, duringAccountStop ? 0 : 1, file: file, line: line)
    }
    @BigSyncBackgroundActor
    func testUploadAccountStopInspectionCannotAcknowledgeAfterCancellation() async throws {
        try await requireInspectionCancellation(.saveMissing, duringAccountStop: true)
    }
    @BigSyncBackgroundActor
    func testDeletionAccountStopInspectionCannotAcknowledgeAfterCancellation() async throws {
        try await requireInspectionCancellation(.deleteMissing, duringAccountStop: true)
    }
    @BigSyncBackgroundActor
    func testUploadConflictInspectionCannotStartReceiptOrRepairAfterCancellation() async throws {
        try await requireInspectionCancellation(.saveConflict)
    }
    @BigSyncBackgroundActor
    func testUploadMissInspectionCannotStartReceiptOrRepairAfterCancellation() async throws {
        try await requireInspectionCancellation(.saveMissing)
    }
    @BigSyncBackgroundActor
    func testDeletionConflictInspectionCannotStartReceiptOrRepairAfterCancellation() async throws {
        try await requireInspectionCancellation(.deleteConflict)
    }
    @BigSyncBackgroundActor
    func testDeletionMissInspectionCannotStartReceiptOrRepairAfterCancellation() async throws {
        try await requireInspectionCancellation(.deleteMissing)
    }
    @BigSyncBackgroundActor
    private func requireSizeInspectionCancellation(
        _ route: ResponseRoute, response: ResponseSizeLimit,
        file: StaticString = #filePath, line: UInt = #line
    ) async throws {
        let probe = ResponseErrorInspectionProbe()
        // Thrown operation errors enter the size classifier without the
        // returned-result/account phase. Per-item errors arm after that phase.
        if response == .thrown { probe.arm() }
        let detail = ResponseCallbackError(probe: probe)
        let caller = Task { @BigSyncBackgroundActor in
            try await run(route, .none, repairUnderlyingError: detail,
                          sizeLimit: response, inspectionProbe: probe)
        }
        let (adapter, transport, _, failure) = try await caller.value
        XCTAssertTrue(probe.didTrigger, file: file, line: line)
        XCTAssertTrue(failure is CancellationError, file: file, line: line)
        let initialBatchSize = CloudKitSynchronizer.defaultInitialBatchSize
        XCTAssertEqual(probe.finalBatchSize, initialBatchSize, "Cancelled inspection cannot resize live state", file: file, line: line)
        XCTAssertEqual(adapter.preparationLimits, [initialBatchSize], file: file, line: line)
        XCTAssertTrue(adapter.uploaded.isEmpty, file: file, line: line)
        XCTAssertTrue(adapter.deleted.isEmpty, file: file, line: line)
        let calls = await transport.mutationCount
        XCTAssertEqual(calls, 1, file: file, line: line)
    }
    @BigSyncBackgroundActor
    func testThrownUploadSizeInspectionCannotResizeCancelledAttempt() async throws {
        try await requireSizeInspectionCancellation(.save, response: .thrown)
    }
    @BigSyncBackgroundActor
    func testThrownDeletionSizeInspectionCannotResizeCancelledAttempt() async throws {
        try await requireSizeInspectionCancellation(.deleteMissing, response: .thrown)
    }
    @BigSyncBackgroundActor
    func testReturnedUploadSizeInspectionCannotResizeCancelledAttempt() async throws {
        try await requireSizeInspectionCancellation(.save, response: .perItem)
    }
    @BigSyncBackgroundActor
    func testReturnedDeletionSizeInspectionCannotResizeCancelledAttempt() async throws {
        try await requireSizeInspectionCancellation(.deleteMissing, response: .perItem)
    }
    @BigSyncBackgroundActor
    func testIncompleteResponseGraphDoesNotIssueAccountRevalidation() async throws {
        var deep: NSError = CKError(.notAuthenticated) as NSError
        for _ in 0..<40 { deep = NSError(domain: "OpaqueWrapper", code: 1,
            userInfo: [NSUnderlyingErrorKey: deep]) }
        for route: ResponseRoute in [.saveConflict, .saveMissing, .deleteConflict, .deleteMissing, .lookupMissing] {
            let (_, transport, account, failure) = try await run(route, .none,
                failsAccountAfterResult: true, repairUnderlyingError: deep)
            let error = try XCTUnwrap(failure)
            let constraints = CloudKitRetryConstraints(error)
            XCTAssertFalse(constraints.isErrorGraphComplete)
            XCTAssertFalse(constraints.codes.contains(.notAuthenticated), "Do not invent an unseen account stop")
            XCTAssertNotEqual((error as NSError).domain, "ResponseAccountFailure")
            let probes = await account.callsAfterResult
            let calls = await transport.mutationCount
            XCTAssertEqual(probes, 0)
            XCTAssertEqual(calls, route.looksUp ? 0 : 1)
        }
    }
    @BigSyncBackgroundActor
    func testSuccessfulRequeueDoesNotResurfaceMissingErrorAtNextAccountFailure() async throws {
        let (adapter, _, account, failure) = try await run(.saveMissing, .none,
            failAccountAfterResultCall: 3)
        let error = try XCTUnwrap(failure)
        XCTAssertEqual((error as NSError).domain, "ResponseAccountFailure")
        XCTAssertNil((error as NSError).userInfo[CKPartialErrorsByItemIDKey])
        XCTAssertEqual(adapter.requeued.map(\.recordName), ["target"])
        XCTAssertEqual(adapter.uploaded.map { $0.recordID.recordName }, ["success"])
        let probes = await account.callsAfterResult
        XCTAssertEqual(probes, 3)
    }
    @BigSyncBackgroundActor
    func testSuccessfulRequeueKeepsUnstartedConflictButNotHandledMissingError() async throws {
        let zone = CKRecordZone.ID(zoneName: "response-identity")
        let other = CKRecord.ID(recordName: "other", zoneID: zone)
        let conflict = CKError(.serverRecordChanged, userInfo: [
            CKRecordChangedErrorServerRecordKey: CKRecord(recordType: "IdentityFixture", recordID: other)
        ])
        let (adapter, _, account, failure) = try await run(.saveMissing, .none,
            sibling: conflict, failAccountAfterResultCall: 3)
        let error = try XCTUnwrap(failure)
        let items = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        XCTAssertEqual(items[other]?.code, CKError.serverRecordChanged.rawValue)
        XCTAssertNil(items[.init(recordName: "target", zoneID: zone)])
        XCTAssertNil(items[.init(recordName: "success", zoneID: zone)])
        XCTAssertEqual(adapter.requeued.map(\.recordName), ["target"])
        XCTAssertTrue(adapter.imported.isEmpty)
        XCTAssertEqual(((error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError)?.domain, "ResponseAccountFailure")
        let probes = await account.callsAfterResult
        XCTAssertEqual(probes, 3)
    }
}
