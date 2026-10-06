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
    var phaseFinished = false
    var importCount = 0
    var cleanupCount = 0
    var persistenceCount = 0
    var uploadPreparationCount = 0
    var deletionPreparationCount = 0
    var savedTokens = [RecordZoneChangeCursor?]()
    var recordError: Error?
    var onProgress: (@BigSyncBackgroundActor (String) -> Void)?
    var onPersist: (@BigSyncBackgroundActor () async throws -> Void)?
    var onSave: (@BigSyncBackgroundActor () async throws -> Void)?
    func accountIdentifier() async -> String {
        if blockAccount { await accountGate.wait() }
        return "sync-phase-account"
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
    var hasChanges: Bool { false }
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
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
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
    func returnDeletion(in zone: CKRecordZone.ID) { deletions = [.init(zoneID: zone, kind: .deleted)] }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        databaseFetchCount += 1
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
    private func withFixture(priorities: [String] = [],
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
            recordZoneID: zone, keyValueStore: SyncPhaseStore(),
            accountIdentifierProvider: { await probe.accountIdentifier() }, accountStatusProvider: { .available },
            progressHandler: { probe.onProgress?($0) },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport,
            recordStore: transport, backupDetectionBaseURL: directory, logger: Logger(label: "SyncPhaseOwnership"))
        // Controlled transport fixtures do not implement Realm reset migration.
        // Admit the fake through the existing Debug seam before testing ownership.
        sync._allowRecordZoneRebindingForTesting()
        sync.addModelAdapter(adapter)
        sync.synchronizationRunID = await sync.changeRequestProcessor.beginRun()
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
            throw error
        }
        sync.cancelSynchronization()
        await probe.accountGate.open()
        await sync.cancelSynchronizationAndWait()
        probe.onPersist = nil; probe.onSave = nil; probe.onProgress = nil
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
