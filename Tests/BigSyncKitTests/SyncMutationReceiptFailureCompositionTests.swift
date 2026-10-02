import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

private enum ReceiptFailurePhase: CaseIterable {
    case uploadAcknowledgement, deleteAcknowledgement, missingRequeue, deletionRebase
    case lookupApply, lookupPersist, lookupFinish, none

    var deletes: Bool { self == .deleteAcknowledgement || self == .deletionRebase }
    var looksUp: Bool { self == .lookupApply || self == .lookupPersist || self == .lookupFinish }
}

/// Fault injection at the existing adapter API: server success and local
/// journal durability are intentionally separate observations in this fixture.
private final class ReceiptFailureAdapter: NSObject, ModelAdapter, @unchecked Sendable {
    let recordZoneID = CKRecordZone.ID(zoneName: "receipt-failure-composition")
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .custom
    let phase: ReceiptFailurePhase
    let localFailure = NSError(domain: "LocalReceiptDurabilityFailure", code: 29)
    private(set) var pending: Set<String>
    private(set) var acknowledged = [String]()
    var hasChanges: Bool { !pending.isEmpty }

    init(_ phase: ReceiptFailurePhase) {
        self.phase = phase
        if phase.looksUp {
            pending = ["observation", "other"]
        } else if phase == .missingRequeue || phase == .deletionRebase {
            pending = ["success-a", "repair", "other"]
        } else {
            pending = ["success-a", "success-b", "other"]
        }
    }

    func cleanUp() async throws {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: Object) -> Bool { true }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        if phase == .lookupApply { throw localFailure }
        return records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
                               recordID: $0.element.recordID),
                  disposition: .preservedPendingLocal(generation: "pending-" + $0.element.recordID.recordName))
        }
    }
    func persistImportedChanges() async throws {
        if phase == .lookupPersist { throw localFailure }
    }
    func didFinishImport() async throws {
        if phase == .lookupFinish { throw localFailure }
    }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        guard !phase.deletes else { return [] }
        return pending.sorted().map { name in
            let record = CKRecord(recordType: "ReceiptFixture", recordID: .init(recordName: name, zoneID: recordZoneID))
            record["text"] = "pending value" as CKRecordValue
            return .init(record: record, generation: "pending-" + name,
                         comparisonBase: nil, requiresAcceptanceCheck: phase.looksUp)
        }
    }
    @BigSyncBackgroundActor
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {
        for record in savedRecords.sorted(by: { $0.recordID.recordName < $1.recordID.recordName }) {
            let name = record.recordID.recordName
            guard matchingGenerations[name] == "pending-" + name else { continue }
            pending.remove(name)
            acknowledged.append(name)
            if phase == .uploadAcknowledgement { throw localFailure }
        }
    }
    @BigSyncBackgroundActor
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] {
        guard phase.deletes else { return [] }
        return pending.sorted().map {
            .init(recordID: .init(recordName: $0, zoneID: recordZoneID), generation: "pending-" + $0)
        }
    }
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {
        for id in recordIDs.sorted(by: { $0.recordName < $1.recordName }) {
            guard matchingGenerations[id.recordName] == "pending-" + id.recordName else { continue }
            pending.remove(id.recordName)
            acknowledged.append(id.recordName)
            if phase == .deleteAcknowledgement { throw localFailure }
        }
    }
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {
        if phase == .missingRequeue { throw localFailure }
    }
    @BigSyncBackgroundActor
    func rebasePendingDeletionMetadata(using serverRecords: [CKRecord], matchingPreparedGenerations: [String: String]) async throws {
        if phase == .deletionRebase { throw localFailure }
    }
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

private actor ReceiptAccountProbe {
    private var returnedResult = false
    private(set) var callsAfterResult = 0
    let failAfterResult: Bool
    init(failAfterResult: Bool) { self.failAfterResult = failAfterResult }
    func didReturnResult() { returnedResult = true }
    func identity() throws -> String {
        if returnedResult {
            callsAfterResult += 1
            if failAfterResult { throw NSError(domain: "AccountRevalidationFailure", code: 31) }
        }
        return "receipt-account"
    }
}

private final class ReceiptDatabaseIdentity: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}
private final class ReceiptKeyValueStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}
private actor ReceiptFailureTransport: CloudKitRecordStore, CloudKitRecordFetching,
    CloudKitChangeFeed, CloudKitSubscriptionStore, CloudKitZoneStore {
    let phase: ReceiptFailurePhase
    let sibling: CKError
    let account: ReceiptAccountProbe
    private(set) var mutationCount = 0
    private(set) var lookupCount = 0
    init(phase: ReceiptFailurePhase, sibling: CKError, account: ReceiptAccountProbe) {
        self.phase = phase
        self.sibling = sibling
        self.account = account
    }
    func modifyRecords(saving records: [CKRecord], deleting recordIDs: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        mutationCount += 1
        var saves: [CKRecord.ID: Result<CKRecord, Error>] = [:]
        var deletes: [CKRecord.ID: Result<Void, Error>] = [:]
        for record in records {
            switch record.recordID.recordName {
            case "other": saves[record.recordID] = .failure(sibling)
            case "repair": saves[record.recordID] = .failure(CKError(.unknownItem))
            default: saves[record.recordID] = .success(record)
            }
        }
        for id in recordIDs {
            switch id.recordName {
            case "other": deletes[id] = .failure(sibling)
            case "repair":
                let server = CKRecord(recordType: "ReceiptFixture", recordID: id)
                deletes[id] = .failure(CKError(.serverRecordChanged,
                    userInfo: [CKRecordChangedErrorServerRecordKey: server]))
            case "success-b": deletes[id] = .failure(CKError(.unknownItem))
            default: deletes[id] = .success(())
            }
        }
        await account.didReturnResult()
        return .init(saveResults: saves, deleteResults: deletes)
    }
    func fetchRecords(with recordIDs: [CKRecord.ID]) async throws -> [CKRecord.ID: Result<CKRecord, Error>] {
        lookupCount += 1
        var results: [CKRecord.ID: Result<CKRecord, Error>] = [:]
        for id in recordIDs {
            results[id] = id.recordName == "other" ? .failure(sibling)
                : .success(CKRecord(recordType: "ReceiptFixture", recordID: id))
        }
        await account.didReturnResult()
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
}

@BigSyncBackgroundActor
private final class ReceiptFailureCapture {
    var error: Error?
}

/// The cycle exists only in a returned userInfo dictionary, not in stored
/// strong references. Dropping the test's root therefore releases the graph.
private final class ReceiptCyclicError: NSError, @unchecked Sendable {
    private let limit = CKError(.limitExceeded) as NSError
    init() { super.init(domain: CKErrorDomain, code: CKError.partialFailure.rawValue, userInfo: nil) }
    required init?(coder: NSCoder) { super.init(coder: coder) }
    override var userInfo: [String: Any] {
        let children: [String: NSError] = ["cycle": self, "limit": limit]
        return [CKPartialErrorsByItemIDKey: children]
    }
}

final class SyncMutationReceiptFailureCompositionTests: XCTestCase {
    @BigSyncBackgroundActor
    private func check(_ phase: ReceiptFailurePhase, sibling: CKError,
                       failAccountRevalidation: Bool = false) async throws {
        let adapter = ReceiptFailureAdapter(phase)
        let account = ReceiptAccountProbe(failAfterResult: failAccountRevalidation)
        let transport = ReceiptFailureTransport(phase: phase, sibling: sibling, account: account)
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.receipt-failure", database: ReceiptDatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: ReceiptKeyValueStore(),
            accountIdentifierProvider: { try await account.identity() }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport,
            recordStore: transport, backupDetectionBaseURL: directory, logger: Logger(label: "ReceiptFailureComposition"))
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: "receipt-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "receipt-account"))
        let captured = ReceiptFailureCapture()
        if phase.deletes {
            try await sync.uploadDeletionsUsingAsyncStore(adapter: adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID) { captured.error = $0 }
        } else {
            try await sync.uploadRecordsUsingAsyncStore(adapter: adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID) { captured.error = $0 }
        }
        let error = try XCTUnwrap(captured.error)
        let constraints = CloudKitRetryConstraints(error)
        XCTAssertTrue(constraints.codes.contains(sibling.code))
        let children = try XCTUnwrap((error as? CKError)?.userInfo[CKPartialErrorsByItemIDKey]
            as? [CKRecord.ID: NSError])
        let other = CKRecord.ID(recordName: "other", zoneID: adapter.recordZoneID)
        XCTAssertEqual(children[other]?.code, (sibling as NSError).code)
        // Even a partially committed acknowledgement must not become a server
        // failure or put an already-consumed journal generation back in flight.
        for name in ["success-a", "success-b"] {
            XCTAssertNil(children[.init(recordName: name, zoneID: adapter.recordZoneID)])
        }
        XCTAssertTrue(adapter.pending.contains("other"))
        let callsAfterResult = await account.callsAfterResult
        if sibling.code == .notAuthenticated || sibling.code == .accountTemporarilyUnavailable {
            XCTAssertTrue(constraints.blocksAccountOperations)
            XCTAssertEqual(callsAfterResult, 0, "Account-stop evidence forbids another identity request")
        }
        if sibling.code == .requestRateLimited {
            XCTAssertTrue(constraints.requiresDeferredRetry)
            XCTAssertEqual(constraints.serverMinimum, 137)
        }
        let underlying = (error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError
        if failAccountRevalidation && !constraints.blocksAccountOperations {
            XCTAssertEqual(underlying?.domain, "AccountRevalidationFailure")
            XCTAssertTrue(adapter.acknowledged.isEmpty)
        } else if phase != .none {
            XCTAssertEqual(underlying?.domain, adapter.localFailure.domain)
            XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
            if !phase.looksUp {
                XCTAssertEqual(adapter.acknowledged, ["success-a"])
                XCTAssertFalse(adapter.pending.contains("success-a"))
            }
        } else {
            XCTAssertEqual(Set(adapter.acknowledged), ["success-a", "success-b"])
        }
        let mutationCount = await transport.mutationCount
        let lookupCount = await transport.lookupCount
        XCTAssertEqual(mutationCount, phase.looksUp ? 0 : 1)
        XCTAssertEqual(lookupCount, phase.looksUp ? 1 : 0)
    }

    @BigSyncBackgroundActor
    func testAllLocalReceiptPhasesPreserveRetryDeadlineAndCommittedSuccess() async throws {
        for phase in ReceiptFailurePhase.allCases where phase != .none {
            try await check(phase, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]))
        }
    }
    @BigSyncBackgroundActor
    func testAllLocalReceiptPhasesPreserveAuthenticationStopWithoutAnotherAccountRequest() async throws {
        for phase in ReceiptFailurePhase.allCases where phase != .none {
            try await check(phase, sibling: CKError(.notAuthenticated), failAccountRevalidation: true)
        }
    }
    @BigSyncBackgroundActor
    func testTemporaryAccountStopStillAcknowledgesSuccessfulSiblings() async throws {
        try await check(.none, sibling: CKError(.accountTemporarilyUnavailable), failAccountRevalidation: true)
        try await check(.deleteAcknowledgement, sibling: CKError(.accountTemporarilyUnavailable), failAccountRevalidation: true)
    }
    @BigSyncBackgroundActor
    func testAccountRevalidationFailureCannotHideReturnedRetryDeadline() async throws {
        try await check(.none, sibling: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]), failAccountRevalidation: true)
    }
    @BigSyncBackgroundActor
    func testLocalReceiptFailureNeverBecomesPureSizeRetry() async throws {
        for phase in ReceiptFailurePhase.allCases where phase != .none {
            try await check(phase, sibling: CKError(.limitExceeded))
        }
    }
    func testCancellationAndIsolatedLocalFailureKeepTheirOriginalMeaning() {
        let id = CKRecord.ID(recordName: "failed")
        let sibling = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        XCTAssertTrue(preservingSiblingMutationFailures(CancellationError(), failedRecordIDs: [],
            otherFailures: [id: sibling]) is CancellationError)
        let local = NSError(domain: "LocalReceiptDurabilityFailure", code: 29)
        let isolated = preservingSiblingMutationFailures(local, failedRecordIDs: [], otherFailures: [:]) as NSError
        XCTAssertEqual(isolated.domain, local.domain)
        XCTAssertEqual(isolated.code, local.code)
    }
    func testUnderlyingConstraintTraversalIsBoundedAndPreservesAccountEvidence() {
        let account = CKError(.notAuthenticated)
        let wrapped = NSError(domain: "LocalReceiptWrapper", code: 1,
            userInfo: [NSUnderlyingErrorKey: account])
        XCTAssertTrue(CloudKitRetryConstraints(wrapped).blocksAccountOperations)
        var nested: Error = CKError(.limitExceeded)
        for _ in 0..<40 {
            nested = NSError(domain: "NestedLocalReceiptWrapper", code: 1,
                userInfo: [NSUnderlyingErrorKey: nested])
        }
        let bounded = CloudKitRetryConstraints(nested)
        XCTAssertFalse(bounded.containsOnlySizeLimitFailures)
        XCTAssertTrue(bounded.codes.isEmpty)
    }

    func testSharedPartialAndUnderlyingDAGIsVisitedOnceAndRemainsSizeOnly() {
        var shared = CKError(.limitExceeded) as NSError
        // Without identity memoization this compact graph has 3^24 traversal
        // paths. Each level shares exactly one NSError through three edges.
        for _ in 0..<24 {
            shared = NSError(domain: CKErrorDomain, code: CKError.partialFailure.rawValue,
                userInfo: [CKPartialErrorsByItemIDKey: ["left": shared, "right": shared],
                           NSUnderlyingErrorKey: shared])
        }
        XCTAssertEqual(cloudKitErrors(in: shared).count, 25)
        let constraints = CloudKitRetryConstraints(shared)
        XCTAssertTrue(constraints.containsOnlySizeLimitFailures)
        XCTAssertEqual(constraints.codes, [.partialFailure, .limitExceeded])
    }

    func testSharedDAGRetainsAccountAndRetryEvidenceWithoutDuplicateFanout() {
        let account = CKError(.notAuthenticated) as NSError
        let delayed = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137]) as NSError
        var shared = NSError(domain: CKErrorDomain, code: CKError.partialFailure.rawValue,
            userInfo: [CKPartialErrorsByItemIDKey: ["account": account, "delay": delayed]])
        for _ in 0..<24 {
            shared = NSError(domain: CKErrorDomain, code: CKError.partialFailure.rawValue,
                userInfo: [CKPartialErrorsByItemIDKey: ["left": shared, "right": shared],
                           NSUnderlyingErrorKey: shared])
        }
        XCTAssertEqual(cloudKitErrors(in: shared).count, 27)
        let constraints = CloudKitRetryConstraints(shared)
        XCTAssertTrue(constraints.blocksAccountOperations)
        XCTAssertTrue(constraints.requiresDeferredRetry)
        XCTAssertEqual(constraints.serverMinimum, 137)
        XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
    }

    func testSharedNodeOnLongerPathCannotBypassDepthCeiling() {
        let leaf = CKError(.limitExceeded) as NSError
        var longer = leaf
        for _ in 0..<31 {
            longer = NSError(domain: CKErrorDomain, code: CKError.partialFailure.rawValue,
                userInfo: [CKPartialErrorsByItemIDKey: ["child": longer]])
        }
        // The underlying edge visits/caches the leaf first. Reusing that
        // successful memo through the partial graph must still enforce depth.
        let root = NSError(domain: CKErrorDomain, code: CKError.partialFailure.rawValue,
            userInfo: [NSUnderlyingErrorKey: leaf,
                       CKPartialErrorsByItemIDKey: ["long": longer]])
        XCTAssertTrue(CloudKitRetryConstraints(root).codes.contains(.limitExceeded))
        XCTAssertFalse(CloudKitRetryConstraints(root).containsOnlySizeLimitFailures)
    }

    func testCycleTerminatesAndFailsClosedWithoutLeakingItsRoot() {
        weak var released: ReceiptCyclicError?
        autoreleasepool {
            let cyclic = ReceiptCyclicError()
            released = cyclic
            XCTAssertEqual(cloudKitErrors(in: cyclic).count, 2)
            let constraints = CloudKitRetryConstraints(cyclic)
            XCTAssertTrue(constraints.codes.contains(.limitExceeded))
            XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        }
        XCTAssertNil(released)
    }
}

// Completion-delivery regressions share this already registered native file.
// Their no-op drains send no record mutations; every account/zone API is injected.
private actor ReceiptCompletionAccountProbe {
    private(set) var calls = 0
    func identity() -> String { calls += 1; return "completion-account" }
}

private actor ReceiptCompletionZoneStore: CloudKitZoneStore {
    let missing: Bool
    let fetchError: Error?
    private(set) var fetches = 0
    private(set) var saves = 0
    init(missing: Bool, fetchError: Error? = nil) {
        self.missing = missing
        self.fetchError = fetchError
    }
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone {
        fetches += 1
        if let fetchError { throw fetchError }
        if missing { throw CKError(.zoneNotFound) }
        return CKRecordZone(zoneID: id)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        saves += 1
        return recordZone
    }
    func deleteRecordZone(withID id: CKRecordZone.ID) async throws {
        throw NSError(domain: "UnexpectedCompletionZoneDeletion", code: 1)
    }
}

@BigSyncBackgroundActor
private final class ReceiptCompletionObservation {
    var errors: [Error?] = []
}

extension SyncMutationReceiptFailureCompositionTests {
    private enum CompletionRoute: CaseIterable {
        case allUploads, records, deletions, setupAndUpload, existingZone, createdZone
    }

    @BigSyncBackgroundActor
    private struct CompletionFixture {
        let sync: CloudKitSynchronizer
        let adapter: ReceiptFailureAdapter
        let account: ReceiptCompletionAccountProbe
        let zone: ReceiptCompletionZoneStore
    }

    @BigSyncBackgroundActor
    private func makeCompletionFixture(
        _ route: CompletionRoute, fetchError: Error? = nil
    ) -> CompletionFixture {
        // Uploads see no upload candidates on a delete-only adapter, and
        // deletions see none on the ordinary upload fixture. Do not fabricate
        // receipts or acknowledge pending generations to obtain an empty drain.
        let phase: ReceiptFailurePhase = route == .deletions ? .none : .deleteAcknowledgement
        let adapter = ReceiptFailureAdapter(phase)
        let account = ReceiptCompletionAccountProbe()
        let zone = ReceiptCompletionZoneStore(missing: route == .createdZone, fetchError: fetchError)
        let transport = ReceiptFailureTransport(phase: phase, sibling: CKError(.networkFailure),
            account: ReceiptAccountProbe(failAfterResult: false))
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.completion-delivery", database: ReceiptDatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: ReceiptKeyValueStore(),
            accountIdentifierProvider: { await account.identity() }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: zone,
            recordStore: transport, backupDetectionBaseURL: directory,
            logger: Logger(label: "CompletionDelivery"))
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: "completion-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "completion-account"))
        addTeardownBlock { @BigSyncBackgroundActor in
            await sync.cancelSynchronizationAndWait()
            try? FileManager.default.removeItem(at: directory)
        }
        return .init(sync: sync, adapter: adapter, account: account, zone: zone)
    }

    @BigSyncBackgroundActor
    private func runCompletionRoute(
        _ route: CompletionRoute, fixture: CompletionFixture,
        completion: @escaping @Sendable @BigSyncBackgroundActor (Error?) async throws -> Void
    ) async throws {
        let sync = fixture.sync
        switch route {
        case .allUploads:
            try await sync.uploadChanges(completion: completion)
        case .records:
            try await sync.uploadRecordsUsingAsyncStore(adapter: fixture.adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .deletions:
            try await sync.uploadDeletionsUsingAsyncStore(adapter: fixture.adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .setupAndUpload:
            try await sync.setupZoneAndUploadRecords(adapter: fixture.adapter,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .existingZone, .createdZone:
            try await sync.setupRecordZoneID(fixture.adapter.recordZoneID,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        }
    }

    @BigSyncBackgroundActor
    func testThrowingSuccessfulCompletionIsNotDeliveredAgain() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = NSError(domain: "CompletionConsumerFailure", code: 501)
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    throw original
                }
                XCTFail("Expected completion failure: \(route)")
            } catch { XCTAssertTrue((error as NSError) === original) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertNil(seen.errors.first ?? nil)
        }
    }

    @BigSyncBackgroundActor
    func testCompletionErrorCannotBeSwallowedByAnotherDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = NSError(domain: "CompletionConsumerFailure", code: 502)
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    if $0 == nil { throw original }
                }
                XCTFail("Second callback consumed the first callback's error: \(route)")
            } catch { XCTAssertTrue((error as NSError) === original) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
        }
    }

    @BigSyncBackgroundActor
    func testZoneShapedCompletionErrorDoesNotRestartZoneRecovery() async throws {
        for route in [CompletionRoute.existingZone, .createdZone, .setupAndUpload] {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = CKError(.zoneNotFound)
            var readsAtFirstDelivery: Int?
            var savesAtFirstDelivery: Int?
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    if readsAtFirstDelivery == nil {
                        readsAtFirstDelivery = await fixture.account.calls
                        savesAtFirstDelivery = await fixture.zone.saves
                    }
                    throw original
                }
                XCTFail("Expected completion failure")
            } catch { XCTAssertEqual((error as? CKError)?.code, .zoneNotFound) }
            let finalReads = await fixture.account.calls
            let finalSaves = await fixture.zone.saves
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertEqual(finalReads, readsAtFirstDelivery)
            XCTAssertEqual(finalSaves, savesAtFirstDelivery)
            XCTAssertEqual(finalSaves, route == .createdZone ? 1 : 0)
        }
    }

    @BigSyncBackgroundActor
    func testSuspendedThrowingCompletionIsStillSingleDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    await Task.yield()
                    throw CancellationError()
                }
                XCTFail("Completion cancellation was swallowed")
            } catch { XCTAssertTrue(error is CancellationError) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
        }
    }

    @BigSyncBackgroundActor
    func testAccountStopAndThrowingDeliveryKeepOneOriginalOperationOutcome() async throws {
        let fixture = makeCompletionFixture(.existingZone, fetchError: CKError(.notAuthenticated))
        let seen = ReceiptCompletionObservation()
        let original = NSError(domain: "CompletionConsumerFailure", code: 503)
        do {
            try await runCompletionRoute(.existingZone, fixture: fixture) {
                seen.errors.append($0)
                throw original
            }
            XCTFail("Expected consumer error")
        } catch { XCTAssertTrue((error as NSError) === original) }
        XCTAssertEqual(seen.errors.count, 1)
        XCTAssertEqual(((seen.errors.first ?? nil) as? CKError)?.code, .notAuthenticated)
        let reads = await fixture.account.calls
        let saves = await fixture.zone.saves
        XCTAssertEqual(reads, 1, "No fresh account request after the returned stop")
        XCTAssertEqual(saves, 0)
    }

    @BigSyncBackgroundActor
    func testSuccessfulNoOpAndZoneCreationCompletionsRemainSingleDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            try await runCompletionRoute(route, fixture: fixture) { seen.errors.append($0) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertNil(seen.errors.first ?? nil)
            let saves = await fixture.zone.saves
            XCTAssertEqual(saves, route == .createdZone ? 1 : 0)
        }
    }

    @BigSyncBackgroundActor
    func testMutationStageCancellationStillDeliversExactlyOneFailure() async throws {
        for deletes in [false, true] {
            let fixture = makeCompletionFixture(deletes ? .deletions : .records)
            let seen = ReceiptCompletionObservation()
            let completion: @Sendable @BigSyncBackgroundActor (Error?) async throws -> Void = {
                seen.errors.append($0)
            }
            let retiredAttempt = UUID()
            if deletes {
                try await fixture.sync.uploadDeletionsUsingAsyncStore(adapter: fixture.adapter,
                    restrictedToEntityType: nil, attemptID: retiredAttempt, completion: completion)
            } else {
                try await fixture.sync.uploadRecordsUsingAsyncStore(adapter: fixture.adapter,
                    restrictedToEntityType: nil, attemptID: retiredAttempt, completion: completion)
            }
            XCTAssertEqual(seen.errors.count, 1)
            XCTAssertTrue((seen.errors.first ?? nil) is CancellationError)
        }
    }
}
