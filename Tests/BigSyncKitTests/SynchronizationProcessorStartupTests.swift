import CloudKit
import Foundation
import Logging
import XCTest
@testable import BigSyncKit

/// Full-caller native companions to the processor tests. No real CloudKit or
/// target Realm operation is permitted. These require the owning Apple graph.
final class SynchronizationProcessorStartupTests: XCTestCase, @unchecked Sendable {
    @BigSyncBackgroundActor
    func testStartupCancelledDuringProcessorJoinDoesNotActivateRetiredContext() async throws {
        try await checkHandoff(invalidateAccountOnly: false)
    }

    @BigSyncBackgroundActor
    func testStartupInvalidatedDuringProcessorJoinDoesNotActivateRetiredContext() async throws {
        try await checkHandoff(invalidateAccountOnly: true)
    }

    @BigSyncBackgroundActor
    private func checkHandoff(invalidateAccountOnly: Bool) async throws {
        let applied = expectation(description: "prior processor apply held")
        let joining = expectation(description: "startup cancels prior processor before joining it")
        let gate = StartupProcessorGate()
        let observation = StartupProcessorObservation()
        let transport = StartupProcessorTransport()
        let adapter = ProcessorCancellationAdapter(save: { records in
            await withTaskCancellationHandler {
                applied.fulfill()
                await gate.wait()
                return ProcessorCancellationAdapter.liveResults(records)
            } onCancel: { joining.fulfill() }
        }, activation: { observation.activations += 1 })
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("processor-startup-" + UUID().uuidString)
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.processor-startup", database: transport,
            recordZoneID: adapter.recordZoneID, keyValueStore: StartupProcessorStore(),
            accountIdentifierProvider: { "account-a" }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport,
            recordStore: transport, backupDetectionBaseURL: directory, logger: Logger(label: "ProcessorStartup"))
        // Controlled transport fixtures do not implement Realm reset migration.
        // Admit the fake through the existing Debug seam before testing ownership.
        sync._allowRecordZoneRebindingForTesting()
        sync.addModelAdapter(adapter)
        let oldRun = await sync.changeRequestProcessor.beginRun()
        sync.synchronizationRunID = oldRun
        let record = CKRecord(recordType: "Item", recordID: CKRecord.ID(recordName: "Item.held", zoneID: adapter.recordZoneID))
        sync.changeRequestProcessor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: record,
            deletedRecordID: nil, adapter: adapter, runID: oldRun))
        let oldRequest = Task { @BigSyncBackgroundActor in
            try await sync.changeRequestProcessor.finishProcessing(for: adapter)
        }
        addTeardownBlock { @BigSyncBackgroundActor in
            oldRequest.cancel()
            await gate.open()
            _ = await oldRequest.result
            await sync.cancelSynchronizationAndWait()
            try? FileManager.default.removeItem(at: directory)
        }
        await fulfillment(of: [applied], timeout: 3)
        sync.beginSynchronization()
        let startup = try XCTUnwrap(sync.synchronizationTask)
        addTeardownBlock { await gate.open(); await startup.value }
        await fulfillment(of: [joining], timeout: 3)
        XCTAssertNil(sync.activeRunContext)
        if invalidateAccountOnly {
            sync.accountScopeAuthorityFence.poison()
        } else {
            sync.cancelSynchronization()
        }
        await gate.open()
        await startup.value
        _ = await oldRequest.result
        XCTAssertEqual(observation.activations, 0, "A retired startup reached adapter activation")
        XCTAssertNil(sync.activeRunContext, "A cancelled startup published a stale run context")
        XCTAssertEqual(sync.synchronizationRunID, oldRun, "A retired startup published a processor run")
        XCTAssertEqual(transport.operationCalls, 0, "No CloudKit operation belongs to this retired startup")
    }
}

@BigSyncBackgroundActor
private final class StartupProcessorObservation { var activations = 0 }
private actor StartupProcessorGate {
    private var isOpen = false
    private var waits: [CheckedContinuation<Void, Never>] = []
    func wait() async { if isOpen { return }; await withCheckedContinuation { waits.append($0) } }
    func open() { isOpen = true; let old = waits; waits.removeAll(); old.forEach { $0.resume() } }
}

private final class StartupProcessorStore: NSObject, KeyValueStore, @unchecked Sendable {
    private var values: [String: Any] = [:]
    func object(forKey name: String) -> Any? { values[name] }
    func bool(forKey name: String) -> Bool { values[name] as? Bool ?? false }
    func set(value: Any?, forKey name: String) { values[name] = value }
    func set(boolValue: Bool, forKey name: String) { values[name] = boolValue }
    func removeObject(forKey name: String) { values.removeValue(forKey: name) }
    func synchronize() -> Bool { true }
    override func value(forKey key: String) -> Any? { values[key] }
}

private final class StartupProcessorTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore, CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private let lock = NSLock()
    private var calls = 0
    var operationCalls: Int { lock.withLock { calls } }
    private func unexpected() -> Error {
        lock.withLock { calls += 1 }
        return NSError(domain: "UnexpectedProcessorStartupCloudKitCall", code: 1)
    }
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? { throw unexpected() }
    func save(subscription: CKSubscription) async throws -> CKSubscription { throw unexpected() }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws { throw unexpected() }
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone { throw unexpected() }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { throw unexpected() }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws { throw unexpected() }
    func modifyRecords(saving recordsToSave: [CKRecord], deleting recordIDsToDelete: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        throw unexpected()
    }
    func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage { throw unexpected() }
    func recordZoneChanges(in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
                           desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage { throw unexpected() }
}
