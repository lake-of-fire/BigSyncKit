import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

private actor SyncPhaseGate {
    private var opened = false
    private var waiters = [CheckedContinuation<Void, Never>]()
    func wait() async {
        guard !opened else { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    func hasOpened() -> Bool { opened }
    func open() {
        opened = true
        let captured = waiters
        waiters.removeAll()
        captured.forEach { $0.resume() }
    }
}

@BigSyncBackgroundActor
private final class SyncPhaseProbe {
    let accountGate = SyncPhaseGate()
    var blockAccount = false
    var accountIdentifierCalls = 0
    var accountStatusCalls = 0
    var phaseFinished = false
    var terminalBoundaryEntries = 0
    var importCount = 0
    var cleanupCount = 0
    var persistenceCount = 0
    var uploadPreparationCount = 0
    var deletionPreparationCount = 0
    var savedTokens = [RecordZoneChangeCursor?]()
    var recordError: Error?
    var capturedAccountFailures = [BigSyncSynchronizationFailure]()
    var accountSuccessorAttempt: UUID?
    var accountSuccessorTask: Task<Void, Never>?
    var onProgress: (@BigSyncBackgroundActor (String) -> Void)?
    var onPersist: (@BigSyncBackgroundActor () async throws -> Void)?
    var onSave: (@BigSyncBackgroundActor () async throws -> Void)?
    func accountIdentifier() async -> String {
        accountIdentifierCalls += 1
        if blockAccount { await accountGate.wait() }
        return "sync-phase-account"
    }
    func accountStatus() -> CKAccountStatus {
        accountStatusCalls += 1
        return .available
    }
    func noteCleanup() { cleanupCount += 1 }
    func noteImport() { importCount += 1 }
    func persist() async throws {
        persistenceCount += 1
        try await onPersist?()
    }
    func save(_ token: RecordZoneChangeCursor?) async throws {
        savedTokens.append(token)
        try await onSave?()
    }
}

// No Realm model is added to the process-wide default schema. Adapter state is
// isolated by the existing sync actor; these are controlled transport effects,
// not real CloudKit or Realm-durability acceptance.
private final class SyncPhaseAdapter: NSObject, ModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    let priorityEntityTypeNames: [String]
    let probe: SyncPhaseProbe
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    var pendingStateRead: (@Sendable () -> Bool)?
    var hasChanges: Bool { pendingStateRead?() ?? false }
    init(zoneID: CKRecordZone.ID, probe: SyncPhaseProbe, priorities: [String] = []) {
        recordZoneID = zoneID
        self.probe = probe
        priorityEntityTypeNames = priorities
    }
    func cleanUp() async throws { await probe.noteCleanup() }
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] {
        if let error = await probe.recordError { throw error }
        return records.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: $0.element.recordType,
                               recordID: $0.element.recordID), disposition: .applied)
        }
    }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] {
        recordIDs.enumerated().map {
            .init(event: .init(ordinal: $0.offset, entityType: "SyncPhaseFixture",
                               recordID: $0.element), disposition: .appliedTombstone)
        }
    }
    func persistImportedChanges() async throws { try await probe.persist() }
    func didFinishImport() async throws { await probe.noteImport() }
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        probe.uploadPreparationCount += 1
        return []
    }
    @BigSyncBackgroundActor
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] {
        probe.deletionPreparationCount += 1
        return []
    }
    @BigSyncBackgroundActor
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor
    func requeueMissingServerRecords(_ ids: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    var serverChangeToken: RecordZoneChangeCursor? {
        get async { RecordZoneChangeCursor(serializedData: Data("existing-zone".utf8)) }
    }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws { try await probe.save(token) }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
}

private final class SyncPhaseStore: NSObject, KeyValueStore {
    private var values = [String: Any]()
    var onDatabaseTokenWrite: (@Sendable () -> Void)?
    private(set) var databaseTokenWrites = 0
    var persistedPropertyLists: [[String: Any]] {
        values.values.compactMap { $0 as? [String: Any] }
    }
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) {
        values[key] = value
        if key.contains("QSDatabaseServerChangeTokenKey") {
            databaseTokenWrites += 1
            let callback = onDatabaseTokenWrite
            onDatabaseTokenWrite = nil
            callback?()
        }
    }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}
private final class SyncPhaseDatabase: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}
private actor SyncPhaseTransport: CloudKitChangeFeed, CloudKitSubscriptionStore,
    CloudKitZoneStore, CloudKitRecordStore {
    private(set) var databaseFetchCount = 0
    private(set) var recordMutationCount = 0
    private var deletions = [CloudKitZoneDeletion]()
    private var heldFailure: NSError?
    private var failureEntered: XCTestExpectation?
    private var failureGate: SyncPhaseGate?
    func holdDatabaseFailure(_ error: NSError, entered: XCTestExpectation,
                             gate: SyncPhaseGate) {
        heldFailure = error
        failureEntered = entered
        failureGate = gate
    }
    func returnDeletion(in zone: CKRecordZone.ID) { deletions = [.init(zoneID: zone, kind: .deleted)] }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        databaseFetchCount += 1
        if let heldFailure {
            failureEntered?.fulfill()
            await failureGate?.wait()
            throw heldFailure
        }
        return .init(cursor: .init(serializedData: Data("phase-db".utf8)),
                     changedZoneIDs: [], deletions: deletions, moreComing: false)
    }
    func recordZoneChanges(in: CKRecordZone.ID, since: RecordZoneChangeCursor?, desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        XCTFail("No phase-ownership test should request a zone page")
        throw NSError(domain: "UnexpectedPhaseTransport", code: 1)
    }
    func subscription(withID: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID: CKSubscription.ID) async throws {}
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone { CKRecordZone(zoneID: id) }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID: CKRecordZone.ID) async throws {
        XCTFail("No phase-ownership test may delete a zone")
        throw NSError(domain: "UnexpectedPhaseTransport", code: 2)
    }
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID], savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        recordMutationCount += 1
        XCTFail("Empty fixture preparations must not issue a record mutation")
        throw NSError(domain: "UnexpectedPhaseTransport", code: 3)
    }
}

// A selector observer executes synchronously on the posting sync actor. It
// must not dispatch an unstructured Task, which would miss the tested reentry.
@BigSyncBackgroundActor
private final class SyncPhaseNotificationObserver: NSObject {
    private let body: @BigSyncBackgroundActor () -> Void
    private(set) var deliveries = 0
    init(name: Notification.Name, sync: CloudKitSynchronizer,
         body: @escaping @BigSyncBackgroundActor () -> Void) {
        self.body = body
        super.init()
        NotificationCenter.default.addObserver(self, selector: #selector(receive(_:)), name: name, object: sync)
    }
    @objc private func receive(_ notification: Notification) {
        deliveries += 1
        if deliveries == 1 { body() }
    }
    func stop() { NotificationCenter.default.removeObserver(self) }
}


private enum SyncPhaseSwiftFailure: Error { case rejected }
private final class SyncPhaseFailureDelegate: NSObject, CloudKitSynchronizerDelegate, @unchecked Sendable {
    private let lock = NSLock()
    private var failure: Error?
    var captured: Error? { lock.withLock { failure } }
    func synchronizerWillFetchChanges(_ sync: CloudKitSynchronizer, in: CKRecordZone.ID) {}
    func synchronizerWillUploadChanges(_ sync: CloudKitSynchronizer, to: CKRecordZone.ID) {}
    func synchronizerDidSync(_ sync: CloudKitSynchronizer) {}
    func synchronizerDidfailToSync(_ sync: CloudKitSynchronizer, error: Error) { lock.withLock { failure = error } }
    func synchronizer(_ sync: CloudKitSynchronizer, zoneIDWasDeleted: CKRecordZone.ID) {}
}

final class SyncPhaseAttemptOwnershipTests: XCTestCase {
    @BigSyncBackgroundActor
    private func withFixture(priorities: [String] = [], store: SyncPhaseStore? = nil,
        _ body: @BigSyncBackgroundActor (CloudKitSynchronizer, SyncPhaseAdapter, SyncPhaseProbe, SyncPhaseTransport) async throws -> Void
    ) async throws {
        let probe = SyncPhaseProbe()
        let transport = SyncPhaseTransport()
        let zone = CKRecordZone.ID(zoneName: "sync-phase-" + UUID().uuidString)
        let adapter = SyncPhaseAdapter(zoneID: zone, probe: probe, priorities: priorities)
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.phase-ownership", database: SyncPhaseDatabase(),
            recordZoneID: zone, keyValueStore: store ?? SyncPhaseStore(),
            accountIdentifierProvider: { await probe.accountIdentifier() }, accountStatusProvider: { await probe.accountStatus() },
            progressHandler: { probe.onProgress?($0) },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport,
            recordStore: transport, backupDetectionBaseURL: directory, logger: Logger(label: "SyncPhaseOwnership"))
        // Controlled transport fixtures do not implement Realm reset migration.
        // Admit the fake through the existing Debug seam before testing ownership.
        sync._allowRecordZoneRebindingForTesting()
        sync.addModelAdapter(adapter)
        sync.synchronizationRunID = await sync.changeRequestProcessor.beginRun()
        sync.accountScopeAuthorityFence.clear() // Controlled fixture starts with validated authority.
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: "sync-phase-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "sync-phase-account"))
        sync.activeZoneTokens[zone] = .init(serializedData: Data("original-zone".utf8))
        do {
            try await body(sync, adapter, probe, transport)
        } catch {
            sync.cancelSynchronization()
            await probe.accountGate.open()
            await sync.cancelSynchronizationAndWait()
            probe.onPersist = nil; probe.onSave = nil; probe.onProgress = nil
            probe.accountSuccessorTask = nil
            throw error
        }
        sync.cancelSynchronization()
        await probe.accountGate.open()
        await sync.cancelSynchronizationAndWait()
        probe.onPersist = nil; probe.onSave = nil; probe.onProgress = nil
        probe.accountSuccessorTask = nil
    }

    @BigSyncBackgroundActor
    private static func expectCancellation(_ operation: () async throws -> Void) async throws {
        do { try await operation() }
        catch is CancellationError { return }
        catch { XCTFail("Expected cancellation, got \(error)"); throw error }
        XCTFail("An obsolete phase reported success")
        throw NSError(domain: "ExpectedPhaseCancellation", code: 1)
    }

    @BigSyncBackgroundActor
    private func completeBeforeReleasingAccountGate(
        _ probe: SyncPhaseProbe,
        operation: @escaping @BigSyncBackgroundActor @Sendable () async throws -> Void
    ) async throws {
        let finished = expectation(description: "original phase stopped before replacement account work")
        let caller = Task { @BigSyncBackgroundActor in
            defer { probe.phaseFinished = true; finished.fulfill() }
            try await operation()
        }
        // An original-source regression may enter the blocked provider. Fail
        // within the watchdog, then release and join every task we started.
        await fulfillment(of: [finished], timeout: 5)
        let returnedBeforeRelease = probe.phaseFinished
        if !returnedBeforeRelease { caller.cancel(); await probe.accountGate.open() }
        let outcome = await caller.result
        guard returnedBeforeRelease else {
            throw NSError(domain: "RetiredPhaseEnteredReplacementAccount", code: 1)
        }
        try outcome.get()
    }

    @BigSyncBackgroundActor
    private static func replaceAndSeedError(_ sync: CloudKitSynchronizer,
        _ adapter: SyncPhaseAdapter, _ probe: SyncPhaseProbe, error: NSError
    ) async throws {
        // Controlled lifecycle replacement, without a new background account
        // request. These are the real processor's admission/error APIs.
        sync.synchronizationAttemptID = UUID()
        sync.synchronizationRunID = await sync.changeRequestProcessor.beginRun()
        sync.activeRunContext = nil
        probe.recordError = error
        let record = CKRecord(recordType: "SyncPhaseFixture",
            recordID: .init(recordName: "SyncPhaseFixture.error", zoneID: adapter.recordZoneID))
        sync.changeRequestProcessor.addFetchedChangeRequest(.init(downloadedRecord: record,
            deletedRecordID: nil, adapter: adapter, runID: sync.synchronizationRunID))
        _ = try await sync.changeRequestProcessor.finishProcessing(for: adapter)
        probe.recordError = nil
        XCTAssertEqual(sync.changeRequestProcessor.getErrors().count, 1)
        XCTAssertTrue((sync.changeRequestProcessor.getErrors().first as NSError?) === error)
    }

    @BigSyncBackgroundActor
    func testSyncStartObserverCannotOverwriteReplacementAttemptState() async throws {
        try await withFixture { sync, _, probe, transport in
            let successorToken = DatabaseChangeCursor(serializedData: Data("successor-db".utf8))
            let observer = SyncPhaseNotificationObserver(name: .SynchronizerWillSynchronize, sync: sync) {
                probe.blockAccount = true
                sync.cancelSynchronization()
                sync.beginSynchronization()
                sync.serverChangeToken = successorToken
                sync.uploadRetries = 7
                sync.didNotifyUpload = [sync.recordZoneID]
            }
            defer { observer.stop() }
            try await completeBeforeReleasingAccountGate(probe) { await sync.performSynchronization() }
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertEqual(sync.serverChangeToken, successorToken)
            XCTAssertEqual(sync.uploadRetries, 7)
            XCTAssertEqual(sync.didNotifyUpload, [sync.recordZoneID])
            let fetches = await transport.databaseFetchCount
            XCTAssertEqual(fetches, 0)
        }
    }

    @BigSyncBackgroundActor
    func testFetchStartObserverCannotEnterReplacementTransport() async throws {
        try await withFixture { sync, _, probe, transport in
            let observer = SyncPhaseNotificationObserver(name: .SynchronizerWillFetchChanges, sync: sync) {
                probe.blockAccount = true
                sync.cancelSynchronization()
                sync.beginSynchronization()
            }
            defer { observer.stop() }
            try await completeBeforeReleasingAccountGate(probe) { await sync.fetchChanges() }
            XCTAssertEqual(observer.deliveries, 1)
            let fetches = await transport.databaseFetchCount
            XCTAssertEqual(fetches, 0)
        }
    }

    @BigSyncBackgroundActor
    func testUploadStartObserverCannotEnterReplacementAdapter() async throws {
        try await withFixture { sync, _, probe, _ in
            let observer = SyncPhaseNotificationObserver(name: .SynchronizerWillUploadChanges, sync: sync) {
                probe.blockAccount = true
                sync.cancelSynchronization()
                sync.beginSynchronization()
            }
            defer { observer.stop() }
            try await completeBeforeReleasingAccountGate(probe) {
                try await Self.expectCancellation { try await sync.uploadChanges() }
            }
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertEqual(probe.persistenceCount, 0)
            XCTAssertEqual(probe.uploadPreparationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testRetiredSuccessfulPersistenceCannotClearSuccessorProcessorError() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let error = NSError(domain: "successor-processor", code: 1)
            probe.onPersist = { try await Self.replaceAndSeedError(sync, adapter, probe, error: error) }
            try await Self.expectCancellation { try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil) }
            XCTAssertTrue((sync.changeRequestProcessor.getErrors().first as NSError?) === error)
        }
    }

    @BigSyncBackgroundActor
    func testRetiredFailingPersistenceCannotClearSuccessorProcessorError() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let successor = NSError(domain: "successor-processor", code: 2)
            probe.onPersist = {
                try await Self.replaceAndSeedError(sync, adapter, probe, error: successor)
                throw NSError(domain: "retired-persistence", code: 3)
            }
            try await Self.expectCancellation { try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil) }
            XCTAssertTrue((sync.changeRequestProcessor.getErrors().first as NSError?) === successor)
        }
    }

    @BigSyncBackgroundActor
    func testTaskCancellationAfterPersistenceRejectsContinuation() async throws {
        try await withFixture { sync, adapter, probe, _ in
            probe.onPersist = { withUnsafeCurrentTask { $0?.cancel() } }
            let caller = Task { @BigSyncBackgroundActor in
                try await Self.expectCancellation { try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil) }
            }
            try await caller.value
            XCTAssertEqual(probe.persistenceCount, 1)
        }
    }

    @BigSyncBackgroundActor
    func testCompletedTokenSaveCannotStartReplacementUploadPhase() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let expected = sync.activeZoneTokens[adapter.recordZoneID]
            probe.onSave = { sync.synchronizationAttemptID = UUID() }
            try await Self.expectCancellation { try await sync.synchronizeAdapter(adapter) }
            XCTAssertEqual(probe.savedTokens.count, 1)
            let saved = try XCTUnwrap(probe.savedTokens.first)
            XCTAssertEqual(saved, expected)
            XCTAssertEqual(probe.uploadPreparationCount, 0)
            XCTAssertEqual(probe.deletionPreparationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testNilTokenFastPathStillRejectsCancelledAttempt() async throws {
        try await withFixture { sync, adapter, probe, _ in
            sync.activeZoneTokens.removeAll()
            sync.cancelSync = true
            try await Self.expectCancellation { try await sync.saveActiveTokenIfNeeded(for: adapter) }
            XCTAssertTrue(probe.savedTokens.isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testEmptyUploadDeliversCancellationExactlyOnce() async throws {
        try await withFixture { sync, _, _, _ in
            sync.modelAdapterDictionary.removeAll()
            sync.cancelSync = true
            var callbacks = 0
            try await sync.uploadChanges { error in
                callbacks += 1
                XCTAssertTrue(error is CancellationError)
            }
            XCTAssertEqual(callbacks, 1)
        }
    }

    @BigSyncBackgroundActor
    func testCurrentPersistenceErrorRetainsOriginalIdentity() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let expected = NSError(domain: "current-persistence", code: 4)
            probe.onPersist = { throw expected }
            do { try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil); XCTFail("Expected original error") }
            catch { XCTAssertTrue((error as NSError) === expected) }
            XCTAssertTrue(sync.changeRequestProcessor.getErrors().isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testCurrentProcessorErrorIsClearedWithoutStartingPersistence() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let expected = NSError(domain: "current-processor", code: 5)
            try await Self.replaceAndSeedError(sync, adapter, probe, error: expected)
            do { try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil); XCTFail("Expected processor error") }
            catch { XCTAssertTrue((error as NSError) === expected) }
            XCTAssertEqual(probe.persistenceCount, 0)
            XCTAssertTrue(sync.changeRequestProcessor.getErrors().isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testThrowingUploadCompletionIsNeverRedelivered() async throws {
        try await withFixture { sync, _, _, _ in
            sync.modelAdapterDictionary.removeAll()
            var callbacks = 0
            let deliveryError = NSError(domain: "caller-delivery", code: 6)
            do {
                try await sync.uploadChanges { error in
                    callbacks += 1
                    XCTAssertNil(error)
                    throw deliveryError
                }
                XCTFail("Expected delivery error")
            } catch { XCTAssertTrue((error as NSError) === deliveryError) }
            XCTAssertEqual(callbacks, 1)
        }
    }

    @BigSyncBackgroundActor
    func testValidPriorityAndDefaultPhasesRemainOrdered() async throws {
        try await withFixture(priorities: ["Priority"]) { sync, adapter, probe, transport in
            try await sync.synchronizeAdapter(adapter)
            XCTAssertEqual(probe.persistenceCount, 2)
            XCTAssertEqual(probe.uploadPreparationCount, 2)
            XCTAssertEqual(probe.deletionPreparationCount, 2)
            XCTAssertEqual(probe.savedTokens.count, 1)
            let mutations = await transport.recordMutationCount
            XCTAssertEqual(mutations, 0)
        }
    }

    @BigSyncBackgroundActor
    func testCompletedDatabasePageCannotMarkReplacementAccountZoneDeleted() async throws {
        try await withFixture { sync, _, probe, transport in
            await transport.returnDeletion(in: sync.recordZoneID)
            var observed = false
            probe.onProgress = { checkpoint in
                guard checkpoint == "database-fetch-completion", !observed else { return }
                observed = true
                sync.synchronizationAttemptID = UUID()
                sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
                    runID: sync.synchronizationRunID, accountIdentifier: "replacement-account",
                    accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "replacement-account"))
            }
            try await Self.expectCancellation { _ = try await sync.fetchDatabaseChanges() }
            XCTAssertTrue(observed)
            XCTAssertNil(sync.configuredZoneTerminalState(sync.recordZoneID))
        }
    }

    @BigSyncBackgroundActor
    func testCurrentDatabaseDeletionStillRecordsTerminalZone() async throws {
        try await withFixture { sync, _, _, transport in
            await transport.returnDeletion(in: sync.recordZoneID)
            do { _ = try await sync.fetchDatabaseChanges(); XCTFail("Expected current zone deletion") }
            catch let error as ChangeFeedMigrationError { XCTAssertEqual(error.deletionKind, .deleted) }
            XCTAssertEqual(sync.configuredZoneTerminalState(sync.recordZoneID)?.deletionKind, .deleted)
        }
    }

    @BigSyncBackgroundActor
    func testPureSwiftUploadFailureCannotEnterSuccessfulRefetch() async throws {
        try await withFixture { sync, _, probe, transport in
            let observer = SyncPhaseFailureDelegate()
            sync.delegate = observer
            probe.onPersist = { throw SyncPhaseSwiftFailure.rejected }
            try await sync.uploadChanges()
            XCTAssertEqual(observer.captured as? SyncPhaseSwiftFailure, .rejected)
            let fetches = await transport.databaseFetchCount
            XCTAssertEqual(fetches, 0)
            XCTAssertEqual(probe.uploadPreparationCount, 0)
            XCTAssertTrue(probe.savedTokens.isEmpty)
        }
    }


    @BigSyncBackgroundActor
    func testSuspendedPersistenceCannotClearSuccessorErrorOnReturn() async throws {
        try await withFixture { sync, adapter, probe, _ in
            let entered = SyncPhaseGate(), release = SyncPhaseGate()
            let arrived = expectation(description: "persistence entered")
            probe.onPersist = { await entered.open(); arrived.fulfill(); await release.wait() }
            let task = Task { @BigSyncBackgroundActor in
                try await sync.runFetchedChangesPhase(for: adapter, restrictedToEntityType: nil)
            }
            addTeardownBlock { @BigSyncBackgroundActor in
                task.cancel(); await release.open(); _ = await task.result
            }
            // The timeout is a failure watchdog, not how the interleaving is ordered.
            await fulfillment(of: [arrived], timeout: 5)
            guard await entered.hasOpened() else {
                task.cancel(); await release.open(); _ = await task.result
                throw NSError(domain: "SyncPhaseFixtureDidNotEnterPersistence", code: 1)
            }
            // Seed a real current-run processor error while this older caller
            // is actually suspended in a noncooperative adapter operation.
            let successor = NSError(domain: "SuspendedSuccessor", code: 1)
            try await Self.replaceAndSeedError(sync, adapter, probe, error: successor)
            await release.open()
            try await Self.expectCancellation { try await task.value }
            XCTAssertTrue((sync.changeRequestProcessor.getErrors().first as NSError?) === successor)
        }
    }

    @BigSyncBackgroundActor
    func testTerminalProgressReplacementCannotClearOrPublishSuccessorState() async throws {
        for checkpoint in ["terminal-tail-account-revalidated", "terminal-tail-adapters-cleaned",
                           "terminal-tail-pending-checked", "terminal-tail-prepublication-completed",
                           "terminal-receipt", "download-only-completed"] {
            try await withFixture { sync, adapter, probe, _ in
                sync.synchronizationDrainIsActive = true
                if checkpoint == "download-only-completed" { sync.synchronizationDrainMode = .downloadOnly }
                var completions = 0
                sync.synchronizationCompletionHandler = { _ in completions += 1 }
                let expectedAuthorization = UUID()
                let token = RecordZoneChangeCursor(serializedData: Data("replacement-terminal".utf8))
                var reached = false
                probe.onProgress = { value in
                    guard value == checkpoint, !reached else { return }
                    reached = true
                    probe.blockAccount = true
                    sync.synchronizationAttemptID = UUID()
                    sync.synchronizationRunID = UUID()
                    sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
                        runID: sync.synchronizationRunID, accountIdentifier: "sync-phase-account",
                        accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "sync-phase-account"))
                    sync.activeZoneTokens[adapter.recordZoneID] = token
                    sync.uploadRetries = 19
                    sync.activeReceiptAuthorizationID = expectedAuthorization
                    sync.synchronizationRequestedWhileRunning = true
                    sync.syncing = true
                }
                try await completeBeforeReleasingAccountGate(probe) { await sync.changesFinishedSynchronizing() }
                XCTAssertTrue(reached, "The tested checkpoint did not execute: \(checkpoint)")
                XCTAssertEqual(sync.activeZoneTokens[adapter.recordZoneID], token, checkpoint)
                XCTAssertEqual(sync.uploadRetries, 19, checkpoint)
                XCTAssertEqual(sync.activeReceiptAuthorizationID, expectedAuthorization, checkpoint)
                XCTAssertEqual(completions, 0, "Old terminal completion entered the application handler")
#if DEBUG
                XCTAssertEqual(sync._testActiveRunCallbackCount, 0)
#endif
                if checkpoint == "terminal-tail-account-revalidated" {
                    XCTAssertEqual(probe.importCount, 0)
                    XCTAssertEqual(probe.cleanupCount, 0)
                }
                sync.synchronizationCompletionHandler = nil
            }
        }
    }

    @BigSyncBackgroundActor
    func testBlockedHealthObserverCannotClearReplacementReceiptAuthorization() async throws {
        try await withFixture { sync, _, probe, _ in
            sync.synchronizationDrainIsActive = true
            sync.domainPrepublicationHandler = { _ in [.init(code: "fixture-semantic-block")] }
            var completions = 0
            sync.synchronizationCompletionHandler = { _ in completions += 1 }
            let authorization = UUID()
            let observer = SyncPhaseNotificationObserver(name: .SynchronizerSyncHealthDidChange, sync: sync) {
                probe.blockAccount = true
                sync.synchronizationAttemptID = UUID()
                sync.synchronizationRunID = UUID()
                sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
                    runID: sync.synchronizationRunID, accountIdentifier: "sync-phase-account",
                    accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "sync-phase-account"))
                sync.activeReceiptAuthorizationID = authorization
            }
            defer { observer.stop(); sync.synchronizationCompletionHandler = nil; sync.domainPrepublicationHandler = nil }
            try await completeBeforeReleasingAccountGate(probe) { await sync.changesFinishedSynchronizing() }
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertEqual(sync.activeReceiptAuthorizationID, authorization)
            XCTAssertEqual(completions, 0)
        }
    }

}

// Append to the existing registered source; these helpers are file-private.
// Actual synchronizer/NotificationCenter, controlled protocol transports.
extension SyncPhaseAttemptOwnershipTests {
    @BigSyncBackgroundActor
    func testFailureNotificationCannotDeliverOldErrorToSuccessorDelegate() async throws {
        try await withFixture { sync, adapter, _, transport in
            let originalDelegate = SyncPhaseFailureDelegate()
            let replacementDelegate = SyncPhaseFailureDelegate()
            sync.delegate = originalDelegate
            let attempt = sync.synchronizationAttemptID
            let successor = UUID()
            let token = RecordZoneChangeCursor(serializedData: Data("successor-failure-token".utf8))
            let observer = SyncPhaseNotificationObserver(
                name: .SynchronizerDidFailToSynchronize, sync: sync
            ) {
                sync.synchronizationAttemptID = successor
                sync.activeRunContext = nil
                sync.uploadRetries = 91
                sync.syncing = true
                sync.activeZoneTokens[adapter.recordZoneID] = token
                sync.delegate = replacementDelegate
            }
            defer { observer.stop() }
            await sync.failSynchronization(error: SyncPhaseSwiftFailure.rejected, for: attempt)
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertNil(originalDelegate.captured)
            XCTAssertNil(replacementDelegate.captured)
            XCTAssertEqual(sync.synchronizationAttemptID, successor)
            XCTAssertEqual(sync.uploadRetries, 91)
            XCTAssertEqual(sync.activeZoneTokens[adapter.recordZoneID], token)
            XCTAssertTrue(sync.syncing)
            let calls = await transport.databaseFetchCount
            XCTAssertEqual(calls, 0)
        }
    }

    @BigSyncBackgroundActor
    func testFailureNotificationDelegateSwapRetainsOriginalDeliveryRecipient() async throws {
        try await withFixture { sync, _, _, _ in
            let originalDelegate = SyncPhaseFailureDelegate()
            let replacementDelegate = SyncPhaseFailureDelegate()
            let error = NSError(domain: "OriginalFailureRecipient", code: 17)
            sync.delegate = originalDelegate
            let observer = SyncPhaseNotificationObserver(
                name: .SynchronizerDidFailToSynchronize, sync: sync
            ) { sync.delegate = replacementDelegate }
            defer { observer.stop() }
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertTrue((originalDelegate.captured as NSError?) === error)
            XCTAssertNil(replacementDelegate.captured)
        }
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledFailureCallerDoesNotForwardOrNotify() async throws {
        try await withFixture { sync, _, probe, _ in
            sync.synchronizationDrainIsActive = true
            let attempt = sync.synchronizationAttemptID
            let observer = SyncPhaseNotificationObserver(
                name: .SynchronizerDidFailToSynchronize, sync: sync
            ) {}
            defer { observer.stop() }
            let caller = Task { @BigSyncBackgroundActor in
                withUnsafeCurrentTask { $0?.cancel() }
                await sync.failSynchronization(error: SyncPhaseSwiftFailure.rejected, for: attempt)
            }
            await caller.value
            XCTAssertEqual(observer.deliveries, 0)
            XCTAssertEqual(probe.importCount, 0)
            XCTAssertNotEqual(sync.synchronizationAttemptID, attempt)
        }
    }

    // Both callers use the real waiter API. The transport gate holds the first
    // drain while the second registers; the coalesced-request flag is the join
    // signal, with a watchdog only to report a broken fixture without hanging.
    private enum AccountStopInterruption { case none, externalPoison, cancellation, callerCancellation }

    @BigSyncBackgroundActor
    private func checkAccountStopSettlement(
        code: CKError.Code, wrapped: Bool,
        interruption: AccountStopInterruption = .none,
        admitsSuccessor: Bool = false, includesTokenExpiry: Bool = false
    ) async throws {
        try await withFixture { sync, _, probe, transport in
            let accountError = NSError(domain: CKErrorDomain, code: code.rawValue)
            let underlying = includesTokenExpiry
                ? NSError(domain: CKErrorDomain, code: CKError.Code.partialFailure.rawValue,
                          userInfo: [CKPartialErrorsByItemIDKey: [
                            "account": accountError,
                            "cursor": NSError(domain: CKErrorDomain,
                                              code: CKError.Code.changeTokenExpired.rawValue),
                          ]])
                : accountError
            let original = wrapped
                ? NSError(domain: "AccountStopEnvelope", code: 61,
                          userInfo: [NSUnderlyingErrorKey: underlying])
                : underlying
            let entered = self.expectation(description: "first caller reached controlled feed")
            let release = SyncPhaseGate()
            await transport.holdDatabaseFailure(original, entered: entered, gate: release)
            let delegate = SyncPhaseFailureDelegate()
            sync.delegate = delegate
            let handler: BigSyncSynchronizationFailureHandler = { failure in
                probe.capturedAccountFailures.append(failure)
                if admitsSuccessor, probe.accountSuccessorAttempt == nil {
                    probe.blockAccount = true
                    sync.beginSynchronization()
                    probe.accountSuccessorAttempt = sync.synchronizationAttemptID
                    probe.accountSuccessorTask = sync.synchronizationTask
                }
            }
            let first = Task { @BigSyncBackgroundActor in
                try await sync.synchronize(failureHandler: handler)
            }
            await self.fulfillment(of: [entered], timeout: 3)
            guard let context = sync.activeRunContext else {
                sync.cancelSynchronization()
                await release.open()
                _ = await first.result
                XCTFail("Fixture did not publish a run before feed admission")
                return
            }
            let identifierCallsBeforeStop = probe.accountIdentifierCalls
            let statusCallsBeforeStop = probe.accountStatusCalls
            sync.synchronizationRequestedWhileRunning = false
            let second = Task { @BigSyncBackgroundActor in
                try await sync.synchronize(failureHandler: handler)
            }
            let deadline = ProcessInfo.processInfo.systemUptime + 3
            while !sync.synchronizationRequestedWhileRunning,
                  ProcessInfo.processInfo.systemUptime < deadline {
                await Task.yield()
            }
            guard sync.synchronizationRequestedWhileRunning else {
                sync.cancelSynchronization()
                await release.open()
                _ = await first.result
                _ = await second.result
                XCTFail("Second real waiter did not coalesce into held drain")
                return
            }
            // Install after startup's .syncing notification. This exercises the
            // terminal health callout, before the classified stop owns poison.
            let observer = SyncPhaseNotificationObserver(
                name: .SynchronizerSyncHealthDidChange, sync: sync
            ) {
                switch interruption {
                case .none, .callerCancellation: break
                case .externalPoison: sync.accountScopeAuthorityFence.poison()
                case .cancellation: sync.cancelSynchronization()
                }
            }
            defer { observer.stop() }
            if interruption == .callerCancellation {
                first.cancel()
                let cancellationDeadline = ProcessInfo.processInfo.systemUptime + 3
                while probe.capturedAccountFailures.isEmpty,
                      ProcessInfo.processInfo.systemUptime < cancellationDeadline {
                    await Task.yield()
                }
                XCTAssertEqual(probe.capturedAccountFailures.first?.category, .requestCancellation)
            }
            await release.open()
            let results = [await first.result, await second.result]
            for (index, result) in results.enumerated() {
                switch result {
                case .success: XCTFail("Account stop published a receipt success")
                case .failure(let error):
                    if interruption == .none
                        || (interruption == .callerCancellation && index == 1) {
                        // Darwin's throwing continuation may copy the NSError
                        // wrapper. Preserve the entire error value and cause,
                        // rather than requiring that wrapper's object address.
                        let delivered = error as NSError
                        XCTAssertEqual(delivered.domain, original.domain)
                        XCTAssertEqual(delivered.code, original.code)
                        XCTAssertTrue(NSDictionary(dictionary: delivered.userInfo)
                            .isEqual(to: original.userInfo))
                        if let cause = original.userInfo[NSUnderlyingErrorKey] as? NSError {
                            XCTAssertTrue((delivered.userInfo[NSUnderlyingErrorKey] as? NSError) === cause)
                        }
                    } else {
                        XCTAssertTrue(error is CancellationError)
                    }
                }
            }
            let failures = probe.capturedAccountFailures
            XCTAssertEqual(failures.count, 2)
            XCTAssertEqual(Set(failures.map(\.requestIdentifier)).count, 2)
            for failure in failures {
                XCTAssertEqual(failure.attemptIdentifier, context.attemptID)
                XCTAssertEqual(failure.runIdentifier, context.runID)
                if interruption == .none
                    || (interruption == .callerCancellation && failure.category == .failed) {
                    XCTAssertEqual(failure.category, .failed)
                    XCTAssertEqual(failure.errorDomain, original.domain)
                    XCTAssertEqual(failure.errorCode, original.code)
                } else {
                    XCTAssertNotEqual(failure.category, .failed)
                    XCTAssertEqual(failure.errorType, String(reflecting: CancellationError.self))
                }
            }
            if interruption == .callerCancellation {
                XCTAssertEqual(failures.filter { $0.category == .requestCancellation }.count, 1)
                XCTAssertEqual(failures.filter { $0.category == .failed }.count, 1)
            }
            XCTAssertTrue((delegate.captured as NSError?) === original)
            XCTAssertEqual(observer.deliveries, 1)
            XCTAssertThrowsError(try sync.checkSynchronizationAttempt(context.attemptID)) {
                XCTAssertTrue($0 is CancellationError)
            }
            XCTAssertThrowsError(try sync.checkRunContext(context)) {
                XCTAssertTrue($0 is CancellationError)
            }
            XCTAssertEqual(probe.uploadPreparationCount, 0)
            XCTAssertEqual(probe.deletionPreparationCount, 0)
            let fetches = await transport.databaseFetchCount
            let mutations = await transport.recordMutationCount
            XCTAssertEqual(fetches, 1)
            XCTAssertEqual(mutations, 0)
            if admitsSuccessor {
                XCTAssertNotEqual(probe.accountSuccessorAttempt, context.attemptID)
                XCTAssertEqual(sync.synchronizationAttemptID, probe.accountSuccessorAttempt)
                XCTAssertNotNil(probe.accountSuccessorTask)
                XCTAssertNotNil(sync.synchronizationTask)
                XCTAssertTrue(sync.syncing)
                XCTAssertTrue(sync.synchronizationDrainIsActive)
                XCTAssertFalse(sync.cancelSync)
            } else {
                XCTAssertEqual(probe.accountIdentifierCalls, identifierCallsBeforeStop)
                XCTAssertEqual(probe.accountStatusCalls, statusCallsBeforeStop)
                XCTAssertNil(sync.synchronizationTask)
                XCTAssertNil(sync.activeRunContext)
                XCTAssertFalse(sync.syncing)
                XCTAssertFalse(sync.synchronizationDrainIsActive)
                XCTAssertFalse(sync.synchronizationRequestedWhileRunning)
                if interruption == .none || interruption == .callerCancellation {
                    XCTAssertTrue(sync.accountScopeAuthorityFence.rejectsAuthority)
                    XCTAssertTrue(sync.accountValidationRequired)
                    XCTAssertEqual(sync.cancelledDueToUnauthentication, code == .notAuthenticated)
                }
            }
            // The successor state was checked before cancellation. Join its
            // controlled provider before querying health so this read cannot
            // accidentally release new synchronization work.
            if admitsSuccessor {
                sync.cancelSynchronization()
                await probe.accountGate.open()
                await sync.cancelSynchronizationAndWait()
            }
            probe.blockAccount = false
            let savedHealth = try await sync.syncHealthSnapshot()
            let health = try XCTUnwrap(savedHealth)
            XCTAssertEqual(health.category, code == .notAuthenticated
                ? .notAuthenticated : .accountTemporarilyUnavailable)
            XCTAssertNotNil(health.lastFailureAt)
            XCTAssertNil(health.lastSuccessAt)
            XCTAssertNil(health.retryNotBefore)
            XCTAssertEqual(health.accountScopeIdentifier, context.accountScopeIdentifier)
            if includesTokenExpiry {
                let store = try XCTUnwrap(sync.keyValueStore as? SyncPhaseStore)
                let request = try XCTUnwrap(store.persistedPropertyLists.first {
                    ($0["phase"] as? String) == "requested"
                        && ($0["accountScopeIdentifier"] as? String) == context.accountScopeIdentifier
                })
                XCTAssertEqual(request["mode"] as? String, ChangeFeedResetMode.serverReconciliation.rawValue)
                XCTAssertEqual(request["zoneName"] as? String, sync.recordZoneID.zoneName)
                XCTAssertTrue(probe.savedTokens.contains { $0 == nil })
            }
        }
    }

    @BigSyncBackgroundActor
    func testDirectAuthenticationStopSettlesTwoWaitersWithOriginalError() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: false)
    }

    @BigSyncBackgroundActor
    func testWrappedAuthenticationStopSettlesTwoWaitersWithOriginalError() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: true)
    }

    @BigSyncBackgroundActor
    func testDirectTemporaryAccountStopSettlesTwoWaitersWithOriginalError() async throws {
        try await checkAccountStopSettlement(code: .accountTemporarilyUnavailable, wrapped: false)
    }

    @BigSyncBackgroundActor
    func testWrappedTemporaryAccountStopSettlesTwoWaitersWithOriginalError() async throws {
        try await checkAccountStopSettlement(code: .accountTemporarilyUnavailable, wrapped: true)
    }

    @BigSyncBackgroundActor
    func testExternalPoisonAtAccountStopHealthCancelsTwoWaiters() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: true,
                                             interruption: .externalPoison)
    }

    @BigSyncBackgroundActor
    func testCancellationAtAccountStopHealthCancelsTwoWaiters() async throws {
        try await checkAccountStopSettlement(code: .accountTemporarilyUnavailable, wrapped: true,
                                             interruption: .cancellation)
    }

    @BigSyncBackgroundActor
    func testCancelledAccountStopWaiterDoesNotReplaceRemainingWaiterError() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: true,
                                             interruption: .callerCancellation)
    }

    @BigSyncBackgroundActor
    func testWrappedAuthenticationAndTokenExpiryPreservesRecoveryBeforeSettlement() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: true,
                                             includesTokenExpiry: true)
    }

    @BigSyncBackgroundActor
    func testAccountStopFailureHandlerSuccessorPreservesTwoWaiterEvidence() async throws {
        try await checkAccountStopSettlement(code: .notAuthenticated, wrapped: true,
                                             admitsSuccessor: true)
    }

    @BigSyncBackgroundActor
    func testWrappedAuthenticationFailureBlocksDeferredLocalTail() async throws {
        try await withFixture { sync, _, _, transport in
            sync.synchronizationRequestedWhileRunning = true
            let error = NSError(domain: "LocalCloudEnvelope", code: 4, userInfo: [
                NSUnderlyingErrorKey: CKError(.notAuthenticated),
            ])
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            XCTAssertTrue(sync.cancelledDueToUnauthentication)
            XCTAssertTrue(sync.accountScopeAuthorityFence.rejectsAuthority)
            XCTAssertNil(sync.synchronizationTask)
            let calls = await transport.databaseFetchCount
            XCTAssertEqual(calls, 0)
        }
    }

    @BigSyncBackgroundActor
    func testWrappedDeadlineFailureUsesExistingRetrySleep() async throws {
        try await withFixture { sync, _, _, _ in
            let start = Date()
            let error = NSError(domain: "LocalCloudEnvelope", code: 5, userInfo: [
                NSUnderlyingErrorKey: CKError(.requestRateLimited,
                    userInfo: [CKErrorRetryAfterKey: 137]),
            ])
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            let deadline = try XCTUnwrap(sync.retrySleepUntil)
            XCTAssertGreaterThanOrEqual(deadline.timeIntervalSince(start), 137)
            let retry = try XCTUnwrap(sync.synchronizationTask)
            retry.cancel()
            await retry.value
        }
    }

    @BigSyncBackgroundActor
    func testWrappedRetryFailurePreservesOriginalDelegateError() async throws {
        try await withFixture { sync, _, _, _ in
            let delegate = SyncPhaseFailureDelegate()
            sync.delegate = delegate
            let error = NSError(domain: "OriginalCloudEnvelope", code: 19, userInfo: [
                NSUnderlyingErrorKey: CKError(.networkFailure),
            ])
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            XCTAssertTrue((delegate.captured as NSError?) === error)
            let retry = try XCTUnwrap(sync.synchronizationTask)
            retry.cancel()
            await retry.value
        }
    }

    @BigSyncBackgroundActor
    func testCancelledImmediateFailureRetryCannotAdmitAnotherAttempt() async throws {
        try await withFixture { sync, adapter, _, transport in
            let attempt = sync.synchronizationAttemptID
            await sync.failSynchronization(
                error: ChangeFeedMigrationError.establishedZoneUnavailable(
                    adapter.recordZoneID, .encryptedDataReset
                ), for: attempt
            )
            let retry = try XCTUnwrap(sync.synchronizationTask)
            retry.cancel()
            await retry.value
            XCTAssertEqual(sync.synchronizationAttemptID, attempt)
            let calls = await transport.databaseFetchCount
            XCTAssertEqual(calls, 0)
        }
    }
}

// Append to the existing file so these histories use its private, isolated
// synchronizer/transport fixtures. No production hook or standalone source.
extension SyncPhaseAttemptOwnershipTests {
    @BigSyncBackgroundActor
    private func requireNoCoalescedFailureRetry(
        _ error: Error,
        withoutRunContext: Bool = false,
        failAdapterTokenReset: Bool = false,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        try await withFixture { sync, _, probe, transport in
            sync.syncing = true
            sync.synchronizationDrainIsActive = true
            sync.synchronizationRequestedWhileRunning = true
            let attempt = sync.synchronizationAttemptID
            let delegate = SyncPhaseFailureDelegate()
            sync.delegate = delegate
            if withoutRunContext { sync.activeRunContext = nil }
            if failAdapterTokenReset {
                probe.onSave = {
                    throw NSError(domain: "RejectedRecoveryTokenWrite", code: 1)
                }
            }

            await sync.failSynchronization(error: error, for: attempt)

            if failAdapterTokenReset {
                XCTAssertEqual(probe.savedTokens.count, 1,
                    "Must exercise the actual failing adapter reset", file: file, line: line)
                if let savedToken = probe.savedTokens.first {
                    XCTAssertNil(savedToken, file: file, line: line)
                }
            }
            XCTAssertEqual(sync.synchronizationAttemptID, attempt,
                "A coalesced wakeup cannot grant a fresh transport/recovery attempt",
                file: file, line: line)
            XCTAssertNil(sync.synchronizationTask, file: file, line: line)
            XCTAssertFalse(sync.syncing, file: file, line: line)
            XCTAssertFalse(sync.synchronizationDrainIsActive, file: file, line: line)
            let delivered = try XCTUnwrap(delegate.captured, file: file, line: line)
            XCTAssertEqual((delivered as NSError).domain, (error as NSError).domain,
                file: file, line: line)
            XCTAssertEqual((delivered as NSError).code, (error as NSError).code,
                file: file, line: line)
            // An original-source regression may have admitted a retry. Cancel
            // it before this test first yields to the transport probe, rather
            // than letting a failed assertion launch unrelated fixture work.
            if sync.synchronizationAttemptID != attempt || sync.synchronizationTask != nil {
                sync.cancelSynchronization()
            }
            let requests = await transport.databaseFetchCount
            XCTAssertEqual(requests, 0, file: file, line: line)
        }
    }

    @BigSyncBackgroundActor
    func testFailedTokenResetCannotRestartCoalescedLocalWork() async throws {
        try await requireNoCoalescedFailureRetry(
            CKError(.changeTokenExpired), failAdapterTokenReset: true
        )
    }

    @BigSyncBackgroundActor
    func testFailedCorruptCursorResetCannotRestartCoalescedLocalWork() async throws {
        try await requireNoCoalescedFailureRetry(
            CloudKitChangeFeedError.corruptCursor, failAdapterTokenReset: true
        )
    }

    @BigSyncBackgroundActor
    func testMissingRecoveryContextCannotRestartCoalescedLocalWork() async throws {
        for error: Error in [CKError(.changeTokenExpired), CloudKitChangeFeedError.corruptCursor] {
            try await requireNoCoalescedFailureRetry(error, withoutRunContext: true)
        }
    }

    @BigSyncBackgroundActor
    func testTerminalTransportFailuresCannotRestartCoalescedLocalWork() async throws {
        for code: CKError.Code in [.unknownItem, .serverRecordChanged, .limitExceeded, .quotaExceeded] {
            try await requireNoCoalescedFailureRetry(CKError(code))
        }
    }

    @BigSyncBackgroundActor
    func testAuthenticationAndModelVersionFailuresCannotRestartCoalescedLocalWork() async throws {
        for error: CloudKitSynchronizer.SyncError in [.notAuthenticated, .higherModelVersionFound] {
            try await requireNoCoalescedFailureRetry(error)
        }
    }

    @BigSyncBackgroundActor
    func testMutationBudgetAndSemanticStopsCannotRestartCoalescedLocalWork() async throws {
        let failures: [Error] = [
            BigSyncHandledMutationRetryError.generationBudgetExceeded(.init(
                recordID: .init(recordName: "pending"), generation: "g1"
            )),
            BigSyncHandledMutationRetryError.drainBudgetExceeded,
            BigSyncSemanticUploadConflictError(recordNames: ["pending"]),
        ]
        for error in failures {
            try await requireNoCoalescedFailureRetry(error)
        }
    }

    @BigSyncBackgroundActor
    func testOrdinaryLocalFailureRetainsCoalescedFollowupAttempt() async throws {
        try await withFixture { sync, _, _, _ in
            sync.syncing = true
            sync.synchronizationDrainIsActive = true
            sync.synchronizationRequestedWhileRunning = true
            let attempt = sync.synchronizationAttemptID
            await sync.failSynchronization(error: SyncPhaseSwiftFailure.rejected, for: attempt)
            XCTAssertNotEqual(sync.synchronizationAttemptID, attempt)
            XCTAssertTrue(sync.syncing)
            XCTAssertNotNil(sync.synchronizationTask)
            // withFixture cancels and joins this newly admitted attempt.
        }
    }

    @BigSyncBackgroundActor
    func testInboundBoundaryChangeRetainsCoalescedFollowupAttempt() async throws {
        try await withFixture { sync, _, _, _ in
            sync.syncing = true
            sync.synchronizationDrainIsActive = true
            sync.synchronizationRequestedWhileRunning = true
            let attempt = sync.synchronizationAttemptID
            await sync.failSynchronization(error: CloudKitSynchronizer.SyncError.inboundBoundaryChanged,
                for: attempt)
            XCTAssertNotEqual(sync.synchronizationAttemptID, attempt)
            XCTAssertTrue(sync.syncing)
            XCTAssertNotNil(sync.synchronizationTask)
        }
    }
}

// Fetch-to-terminal and bounded-error follow-up. These use the existing
// validated phase fixture; no production admission hook or Realm model is added.
private final class SyncPhaseInspectionCancellationError: NSError, @unchecked Sendable {
    init() { super.init(domain: CKErrorDomain, code: CKError.Code.zoneNotFound.rawValue, userInfo: nil) }
    required init?(coder: NSCoder) { fatalError("Fixture is not archived") }
    override var userInfo: [String: Any] {
        withUnsafeCurrentTask { $0?.cancel() }
        return [:]
    }
}

extension SyncPhaseAttemptOwnershipTests {
    private static func unexaminedPhaseError(_ cause: Error) -> NSError {
        var error = cause as NSError
        for _ in 0..<40 {
            error = NSError(domain: "UnexaminedPhaseCause", code: 1,
                userInfo: [NSUnderlyingErrorKey: error])
        }
        return error
    }

    @BigSyncBackgroundActor
    private func exerciseFetchCursorBoundary(
        downloadOnly: Bool = false, cancelFromPending: Bool? = nil,
        cancelFromCursorWrite: Bool = false
    ) async throws {
        let store = SyncPhaseStore()
        try await withFixture(store: store) { sync, adapter, probe, transport in
            let writesBefore = store.databaseTokenWrites
            sync.activeSynchronizationMode = downloadOnly ? .downloadOnly : .sync
            if let cancelFromPending {
                adapter.pendingStateRead = {
                    withUnsafeCurrentTask { $0?.cancel() }
                    return cancelFromPending
                }
            }
            if cancelFromCursorWrite {
                store.onDatabaseTokenWrite = { withUnsafeCurrentTask { $0?.cancel() } }
            }
            // Bound an original-source regression before it performs the
            // unrelated full terminal publication protocol.
            probe.onProgress = { event in
                if event == "terminal-tail-start" {
                    probe.terminalBoundaryEntries += 1
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            }
            let caller = Task { @BigSyncBackgroundActor in
                await sync.fetchChanges(afterUpload: true)
            }
            await caller.value
            adapter.pendingStateRead = nil
            store.onDatabaseTokenWrite = nil
            XCTAssertEqual(store.databaseTokenWrites - writesBefore, cancelFromPending == nil ? 1 : 0)
            XCTAssertEqual(probe.terminalBoundaryEntries, cancelFromPending == nil && !cancelFromCursorWrite ? 1 : 0)
            XCTAssertEqual(probe.uploadPreparationCount, 0)
            XCTAssertEqual(probe.deletionPreparationCount, 0)
            let mutations = await transport.recordMutationCount
            XCTAssertEqual(mutations, 0)
        }
    }

    @BigSyncBackgroundActor
    func testPostUploadPendingFalseCancellationCannotCommitDatabaseCursor() async throws {
        try await exerciseFetchCursorBoundary(cancelFromPending: false)
    }
    @BigSyncBackgroundActor
    func testPostUploadPendingTrueCancellationCannotStartUploadPhase() async throws {
        try await exerciseFetchCursorBoundary(cancelFromPending: true)
    }
    @BigSyncBackgroundActor
    func testPostUploadCursorWriteCancellationCannotEnterTerminalPhase() async throws {
        try await exerciseFetchCursorBoundary(cancelFromCursorWrite: true)
    }
    @BigSyncBackgroundActor
    func testDownloadOnlyCursorWriteCancellationCannotEnterTerminalPhase() async throws {
        try await exerciseFetchCursorBoundary(downloadOnly: true, cancelFromCursorWrite: true)
    }
    @BigSyncBackgroundActor
    func testHealthyPostUploadCursorStillReachesTerminalBoundary() async throws {
        try await exerciseFetchCursorBoundary()
    }
    @BigSyncBackgroundActor
    func testHealthyDownloadOnlyCursorStillReachesTerminalBoundary() async throws {
        try await exerciseFetchCursorBoundary(downloadOnly: true)
    }
    @BigSyncBackgroundActor
    func testIncompleteZoneFailureCannotAuthorizeOuterUploadRetry() async throws {
        try await withFixture { sync, _, _, _ in
            for code: CKError.Code in [.zoneNotFound, .userDeletedZone] {
                let error = CKError(code, userInfo: [
                    NSUnderlyingErrorKey: Self.unexaminedPhaseError(CKError(.notAuthenticated))
                ])
                XCTAssertFalse(CloudKitRetryConstraints(error).isErrorGraphComplete)
                XCTAssertFalse(sync.shouldRetryUpload(for: error as NSError))
            }
        }
    }
    @BigSyncBackgroundActor
    func testCompleteZoneFailureRetainsExistingOuterUploadRetryBudget() async throws {
        try await withFixture { sync, _, _, _ in
            sync.uploadRetries = 4
            XCTAssertTrue(sync.shouldRetryUpload(for: CKError(.zoneNotFound) as NSError))
            sync.uploadRetries = 5
            XCTAssertFalse(sync.shouldRetryUpload(for: CKError(.zoneNotFound) as NSError))
        }
    }
    @BigSyncBackgroundActor
    func testOuterRetryErrorInspectionCannotAuthorizeCancelledCaller() async throws {
        try await withFixture { sync, _, _, _ in
            let caller = Task { @BigSyncBackgroundActor in
                sync.shouldRetryUpload(for: SyncPhaseInspectionCancellationError())
            }
            let permitted = await caller.value
            XCTAssertFalse(permitted)
        }
    }
    @BigSyncBackgroundActor
    func testUnexaminedLocalErrorCannotRestartCoalescedSynchronization() async throws {
        try await withFixture { sync, _, _, transport in
            sync.synchronizationRequestedWhileRunning = true
            let error = Self.unexaminedPhaseError(CKError(.notAuthenticated))
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            let unexpectedRetry = sync.synchronizationTask
            unexpectedRetry?.cancel()
            XCTAssertNil(unexpectedRetry)
            XCTAssertFalse(sync.cancelledDueToUnauthentication, "Do not invent a hidden authentication stop")
            await unexpectedRetry?.value
            let calls = await transport.databaseFetchCount
            XCTAssertEqual(calls, 0)
        }
    }
    @BigSyncBackgroundActor
    func testKnownRetryFloorDoesNotGrantRetryForIncompleteErrorGraph() async throws {
        try await withFixture { sync, _, _, transport in
            let delegate = SyncPhaseFailureDelegate()
            sync.delegate = delegate
            let error = CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 137,
                NSUnderlyingErrorKey: Self.unexaminedPhaseError(CKError(.notAuthenticated))]) as NSError
            await sync.failSynchronization(error: error, for: sync.synchronizationAttemptID)
            let unexpectedRetry = sync.synchronizationTask
            unexpectedRetry?.cancel()
            XCTAssertNil(unexpectedRetry)
            let delivered = try XCTUnwrap(delegate.captured)
            XCTAssertTrue((delivered as NSError) === error)
            XCTAssertEqual(CloudKitRetryConstraints(delivered).serverMinimum, 137)
            XCTAssertFalse(sync.cancelledDueToUnauthentication)
            await unexpectedRetry?.value
            let calls = await transport.databaseFetchCount
            XCTAssertEqual(calls, 0)
        }
    }
}
