import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(SyncTimelineSnapshot)
private final class SyncTimelineSnapshot: Object, ChangeMetadataRecordable,
    BigSyncAuthoritativeServerSnapshotModel, BigSyncInboundSemanticRecordValidating {
    @Persisted(primaryKey: true) var id = "document"
    @Persisted var text = ""
    @Persisted var createdAt = Date(timeIntervalSince1970: 1)
    @Persisted var modifiedAt = Date(timeIntervalSince1970: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false

    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        guard record["text"] is String,
              record["modifiedAt"] is Date,
              record["explicitlyModifiedAt"] is Date else {
            throw TimelineFailure.invalidRecord
        }
    }
}

private enum TimelineFailure: Error {
    case invalidRecord, unexpectedTransportSurface, nonQuiescent
}

private final class TimelineDatabaseIdentity: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

private final class TimelineKeyValueStore: NSObject, KeyValueStore {
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

/// A protocol fake, not a CloudKit subclass or runtime patch. The script owns
/// conflict outcomes; it deliberately does NOT forge Apple's read-only change
/// tags/modification dates or claim to test server-side compare-and-swap.
/// Secure archives isolate request/response/server values from CKRecord aliases.
/// The real adapter, journal, import policy, and upload drain are never replaced.
private actor TimelineTransport: CloudKitRecordStore, CloudKitChangeFeed,
    CloudKitSubscriptionStore, CloudKitZoneStore {
    struct Step: Sendable {
        var failure: CKError.Code? = nil
        var conflicts: [String: Data] = [:]
        var loseSuccessfulReply = false
        var afterMutation: (@Sendable () async throws -> Void)? = nil
    }
    struct Call: Sendable {
        let savedNames: [String]
        let deletedNames: [String]
        let texts: [String: String]
    }
    private var steps: [Step] = []
    private var storage: [String: Data] = [:]
    private var calls: [Call] = []

    static func archive(_ record: CKRecord) throws -> Data {
        try NSKeyedArchiver.archivedData(withRootObject: record, requiringSecureCoding: true)
    }
    static func unarchive(_ bytes: Data) throws -> CKRecord {
        guard let record = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self, from: bytes)
        else { throw TimelineFailure.invalidRecord }
        return record
    }
    func enqueue(_ step: Step) { steps.append(step) }
    func history() -> [Call] { calls }
    func remainingSteps() -> Int { steps.count }
    func record(named name: String) throws -> CKRecord? {
        try storage[name].map(Self.unarchive)
    }
    func seed(_ record: CKRecord) throws {
        storage[record.recordID.recordName] = try Self.archive(record)
    }
    func modifyRecords(
        saving records: [CKRecord], deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        guard savePolicy == .ifServerRecordUnchanged, !atomically else {
            throw TimelineFailure.unexpectedTransportSurface
        }
        let step = steps.isEmpty ? Step() : steps.removeFirst()
        calls.append(Call(
            savedNames: records.map { $0.recordID.recordName },
            deletedNames: recordIDs.map(\.recordName),
            texts: Dictionary(uniqueKeysWithValues: records.compactMap { record in
                (record["text"] as? String).map { (record.recordID.recordName, $0) }
            })
        ))
        if let failure = step.failure { throw CKError(failure) }
        var saved: [CKRecord.ID: Result<CKRecord, Error>] = [:]
        var deleted: [CKRecord.ID: Result<Void, Error>] = [:]
        for record in records {
            if let conflict = step.conflicts[record.recordID.recordName] {
                saved[record.recordID] = .failure(CKError(.serverRecordChanged, userInfo: [
                    CKRecordChangedErrorServerRecordKey: try Self.unarchive(conflict)
                ]))
            } else {
                let bytes = try Self.archive(record)
                storage[record.recordID.recordName] = bytes
                saved[record.recordID] = .success(try Self.unarchive(bytes))
            }
        }
        for recordID in recordIDs {
            if storage.removeValue(forKey: recordID.recordName) == nil {
                deleted[recordID] = .failure(CKError(.unknownItem))
            } else {
                deleted[recordID] = .success(())
            }
        }
        // The script may advance the local replica while the response is in
        // flight, or cancel its delivery after durable simulated acceptance.
        try await step.afterMutation?()
        if step.loseSuccessfulReply { throw CKError(.networkFailure) }
        return .init(saveResults: saved, deleteResults: deleted)
    }
    // These tests own upload/import schedules, not cursor/bootstrap simulation.
    // An unexpected access must fail rather than falling back to CKContainer.
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        throw TimelineFailure.unexpectedTransportSurface
    }
    func recordZoneChanges(in: CKRecordZone.ID, since: RecordZoneChangeCursor?, desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        throw TimelineFailure.unexpectedTransportSurface
    }
    func subscription(withID: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID: CKSubscription.ID) async throws {}
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone { CKRecordZone(zoneID: id) }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID: CKRecordZone.ID) async throws { throw TimelineFailure.unexpectedTransportSurface }
}

/// Explicit test scheduling owns drains; journal wakeups are observed rather
/// than starting a second synchronization behind the deterministic driver.
@BigSyncBackgroundActor
private final class TimelineWakeups: ModelAdapterDelegate {
    private(set) var count = 0
    func needsInitialSetup() async throws {}
    func hasChangesToUpload() async { count += 1 }
}

@BigSyncBackgroundActor
private final class TimelineReplica {
    let adapter: RealmSwiftAdapter
    let synchronizer: CloudKitSynchronizer
    let targetConfiguration: Realm.Configuration
    let trackingConfiguration: Realm.Configuration
    let binding: String
    private let wakeups = TimelineWakeups()
    var realm: Realm { adapter.realmProvider!.targetReaderRealms!.first! }
    var recordName: String { SyncTimelineSnapshot.className() + ".document" }

    init(label: String, directory: URL, transport: TimelineTransport) async throws {
        binding = "binding-" + label
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent(label + "-target.realm")
        target.objectTypes = [SyncTimelineSnapshot.self, BigSyncPendingMutation.self]
        targetConfiguration = target
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent(label + "-tracking.realm")
        trackingConfiguration = tracking
        let identity = BigSyncMutationJournalIdentity(
            installationIdentifier: label, replicaBindingGenerationIdentifier: binding
        )
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target], mutationJournalIdentityProvider: { identity }
        )
        adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking, targetRealmConfigurations: [target],
            excludedClassNames: [], recordZoneID: .init(zoneName: "timeline"),
            logger: Logger(label: "TimelineReplica"), startSetupTask: false,
            assetDirectoryURL: directory.appendingPathComponent(label + "-assets")
        )
        // Reopen the actual disk state; never clear tracking to simulate restart.
        try await adapter.ensureSetup()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: binding
        )
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.timeline", databaseScope: .private)
        synchronizer = CloudKitSynchronizer(
            identifier: label, containerIdentifier: "iCloud.test.timeline",
            database: TimelineDatabaseIdentity(), recordZoneID: adapter.recordZoneID,
            keyValueStore: TimelineKeyValueStore(), accountIdentifierProvider: { "account" },
            accountStatusProvider: { .available }, changeFeed: transport,
            subscriptionStore: transport, zoneStore: transport, recordStore: transport,
            backupDetectionBaseURL: directory.appendingPathComponent(label + "-backup"),
            logger: Logger(label: "TimelineSynchronizer")
        )
        synchronizer.addModelAdapter(adapter)
        adapter.modelAdapterDelegate = wakeups
    }
    func write(_ text: String, day: Double, deleted: Bool = false) throws {
        let realm = realm
        try realm.write {
            let object = realm.object(ofType: SyncTimelineSnapshot.self, forPrimaryKey: "document")
                ?? realm.create(SyncTimelineSnapshot.self, value: ["id": "document"])
            object.text = text
            object.isDeleted = deleted
            object.refreshChangeMetadata(explicitlyModified: true, at: Self.date(day))
        }
    }
    static func date(_ day: Double) -> Date { Date(timeIntervalSince1970: 1_700_000_000 + day * 86_400) }
    func value() -> String? {
        realm.refresh()
        return realm.object(ofType: SyncTimelineSnapshot.self, forPrimaryKey: "document")?.text
    }
    func generation() -> String? {
        realm.refresh()
        return realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName)?.generation
    }
    func importRecord(_ record: CKRecord) async throws -> [InboundLiveResult] {
        try await adapter.saveChanges(in: [record], forceSave: false)
    }
    func drain() async throws {
        // Matches the outer drain's recheck when a delete acknowledgement
        // exposes a new live generation. It is a bounded test driver, not a
        // second implementation of record reconciliation or acknowledgement.
        for _ in 0..<8 {
            try await adapter.didFinishImport()
            try await synchronizer.synchronizeAdapter(adapter)
            try await adapter.didFinishImport()
            if !adapter.hasChanges { return }
        }
        throw TimelineFailure.nonQuiescent
    }
    func stop() async {
        adapter.cancelSynchronization()
        await adapter.waitForCancellation()
        adapter.invalidateTokens()
    }
}

final class SyncLongLivedReplicaTests: XCTestCase {
    private func directory() throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent("sync-timeline-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: url) }
        return url
    }

    @BigSyncBackgroundActor
    func testSevenDaysOfTypingDuringOldUploadPreservesEveryLineAndDrains() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("day 0\n", day: 0)
        let oldGeneration = try XCTUnwrap(owner.generation())
        let expected = (0...7).map { "day \($0)\n" }.joined()
        await server.enqueue(.init(afterMutation: {
            try await { @BigSyncBackgroundActor in
                for day in 1...7 {
                    try owner.write((0...day).map { "day \($0)\n" }.joined(), day: Double(day))
                }
                XCTAssertNotEqual(owner.generation(), oldGeneration)
                try await owner.adapter.didFinishImport()
            }()
        }))
        try await owner.drain()
        let saved = try await server.record(named: owner.recordName)
        XCTAssertEqual(saved?["text"] as? String, expected)
        XCTAssertEqual(owner.value(), expected)
        XCTAssertNil(owner.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.map { $0.texts[owner.recordName] }, ["day 0\n", expected])
        try await owner.drain()
        let quietCalls = await server.history()
        XCTAssertEqual(quietCalls.count, calls.count, "A second quiet drain must not re-author the same work")
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testLostSuccessfulReplySurvivesDiskAdapterReopen() async throws {
        let server = TimelineTransport(), dir = try directory()
        var owner: TimelineReplica? = try await TimelineReplica(label: "owner", directory: dir, transport: server)
        try owner!.write("seven days of unsent work", day: 7)
        let generation = try XCTUnwrap(owner!.generation())
        let name = owner!.recordName
        await server.enqueue(.init(loseSuccessfulReply: true))
        do { try await owner!.drain(); XCTFail("Lost response must leave explicit failed work") }
        catch { XCTAssertEqual((error as? CKError)?.code, .networkFailure) }
        XCTAssertEqual(owner!.generation(), generation)
        let accepted = try await server.record(named: name)
        XCTAssertEqual(accepted?["text"] as? String, "seven days of unsent work")
        await owner!.stop()
        owner = nil
        let reopened = try await TimelineReplica(label: "owner", directory: dir, transport: server)
        XCTAssertEqual(reopened.generation(), generation)
        XCTAssertEqual(reopened.value(), "seven days of unsent work")
        try await reopened.drain()
        XCTAssertNil(reopened.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.count, 2)
        XCTAssertEqual(try XCTUnwrap(calls.first).texts, try XCTUnwrap(calls.dropFirst().first).texts)
        await reopened.stop()
    }

    @BigSyncBackgroundActor
    func testCancelledReplyCannotAcknowledgeAcceptedWork() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("work across a week", day: 7)
        let generation = try XCTUnwrap(owner.generation())
        await server.enqueue(.init(afterMutation: { throw CancellationError() }))
        do { try await owner.drain(); XCTFail("Cancelled delivery is not an acknowledgement") }
        catch { XCTAssertTrue(error is CancellationError) }
        XCTAssertEqual(owner.generation(), generation)
        try await owner.drain()
        XCTAssertNil(owner.generation())
        XCTAssertEqual(owner.value(), "work across a week")
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testDeleteDuringAcceptedSaveNeverUploadsLiveTombstone() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("old work", day: 1)
        await server.enqueue(.init(afterMutation: {
            try await { @BigSyncBackgroundActor in
                try owner.write("old work", day: 7, deleted: true)
                try await owner.adapter.didFinishImport()
            }()
        }))
        try await owner.drain()
        let saved = try await server.record(named: owner.recordName)
        XCTAssertNil(saved)
        XCTAssertNil(owner.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.count, 2)
        XCTAssertEqual(try XCTUnwrap(calls.first).savedNames, [owner.recordName])
        XCTAssertEqual(try XCTUnwrap(calls.dropFirst().first).deletedNames, [owner.recordName])
        XCTAssertTrue(try XCTUnwrap(calls.dropFirst().first).savedNames.isEmpty)
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testResurrectionDuringDeleteResponsePreservesNewLiveGeneration() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("old", day: 1)
        try await owner.drain()
        try owner.write("old", day: 2, deleted: true)
        await server.enqueue(.init(afterMutation: {
            try await { @BigSyncBackgroundActor in
                try owner.write("new lifetime", day: 9)
                try await owner.adapter.didFinishImport()
            }()
        }))
        try await owner.drain()
        let saved = try await server.record(named: owner.recordName)
        XCTAssertEqual(saved?["text"] as? String, "new lifetime")
        XCTAssertEqual(owner.value(), "new lifetime")
        XCTAssertNil(owner.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.count, 3)
        XCTAssertEqual(try XCTUnwrap(calls.dropFirst().first).deletedNames, [owner.recordName])
        XCTAssertEqual(try XCTUnwrap(calls.dropFirst(2).first).texts[owner.recordName], "new lifetime")
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testReceiverDoesNotReauthorThirtyDaysOfOwnerSnapshotsWithClockRollback() async throws {
        let server = TimelineTransport(), dir = try directory()
        let owner = try await TimelineReplica(label: "owner", directory: dir, transport: server)
        let receiver = try await TimelineReplica(label: "receiver", directory: dir, transport: server)
        for day in 0..<30 {
            // Payload order is causal; the authoring clock jumps both ways.
            let time = day.isMultiple(of: 2) ? Double(day + 100) : -Double(day)
            try owner.write("owner revision \(day)", day: time)
            try await owner.drain()
            let receivedRecord = try await server.record(named: owner.recordName)
            let record = try XCTUnwrap(receivedRecord)
            _ = try await receiver.importRecord(record)
            _ = try await receiver.importRecord(record)
            XCTAssertEqual(receiver.value(), "owner revision \(day)", "day \(day)")
            XCTAssertNil(receiver.generation(), "Replicated snapshots are not receiver-authored work")
            try await receiver.drain()
        }
        let calls = await server.history()
        XCTAssertEqual(calls.count, 30)
        XCTAssertNil(owner.generation())
        await owner.stop(); await receiver.stop()
    }

    @BigSyncBackgroundActor
    func testPendingLocalWeekSurvivesScriptedConflictDuringUpload() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("base", day: 0)
        try await owner.drain()
        let receivedBase = try await server.record(named: owner.recordName)
        let serverBase = try XCTUnwrap(receivedBase)
        serverBase["text"] = "remote predecessor" as CKRecordValue
        serverBase["modifiedAt"] = TimelineReplica.date(90) as CKRecordValue
        serverBase["explicitlyModifiedAt"] = serverBase["modifiedAt"]
        let expected = (1...7).map { "work-\($0)" }.joined(separator: "\n")
        try owner.write(expected, day: 7)
        await server.enqueue(.init(conflicts: [owner.recordName: try TimelineTransport.archive(serverBase)]))
        try await owner.drain()
        let result = try await server.record(named: owner.recordName)
        XCTAssertEqual(result?["text"] as? String, expected)
        XCTAssertEqual(owner.value(), expected)
        XCTAssertNil(owner.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.count, 3)
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testMalformedConflictCannotConsumePendingWeekOrRetryWithoutProgress() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("seven days retained", day: 7)
        let generation = try XCTUnwrap(owner.generation())
        let invalid = CKRecord(recordType: SyncTimelineSnapshot.className(), recordID: .init(
            recordName: owner.recordName, zoneID: owner.adapter.recordZoneID
        ))
        await server.enqueue(.init(conflicts: [owner.recordName: try TimelineTransport.archive(invalid)]))
        do { try await owner.drain(); XCTFail("Invalid conflict cannot become successful rebase") }
        catch { XCTAssertTrue(error is BigSyncSemanticUploadConflictError) }
        XCTAssertEqual(owner.generation(), generation)
        XCTAssertEqual(owner.value(), "seven days retained")
        let calls = await server.history()
        XCTAssertEqual(calls.count, 1)
        let tracking = try XCTUnwrap(owner.adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(tracking.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testOldUploadReceiptCannotAcknowledgeAReplacementBinding() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("old account work", day: 7)
        try await owner.adapter.didFinishImport()
        let batch = try await owner.adapter.prepareUploadBatch(limit: 10)
        let generation = try XCTUnwrap(owner.generation())
        try await owner.adapter.activateReplicaBinding(
            accountScopeIdentifier: "replacement-account", replicaBindingGenerationIdentifier: "replacement-binding"
        )
        try await owner.adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        XCTAssertEqual(owner.generation(), generation)
        XCTAssertEqual(owner.value(), "old account work")
        let replacementBatch = try await owner.adapter.prepareUploadBatch(limit: 10)
        XCTAssertTrue(replacementBatch.records.isEmpty, "Old binding is not work for the replacement account")
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testDuplicateOldAcknowledgementCannotEraseLaterOfflineWeek() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        try owner.write("day 0", day: 0)
        try await owner.adapter.didFinishImport()
        let old = try await owner.adapter.prepareUploadBatch(limit: 10)
        try await owner.adapter.acknowledgeUploadedRecords(old.records, from: old)
        try owner.write("day 0 through day 7", day: 7)
        let generation = try XCTUnwrap(owner.generation())
        try await owner.adapter.didFinishImport()
        for _ in 0..<8 { try await owner.adapter.acknowledgeUploadedRecords(old.records, from: old) }
        XCTAssertEqual(owner.generation(), generation)
        try await owner.drain()
        let saved = try await server.record(named: owner.recordName)
        XCTAssertEqual(saved?["text"] as? String, "day 0 through day 7")
        XCTAssertNil(owner.generation())
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testScriptedFailuresAcrossFourteenDaysKeepExactPendingGenerationUntilAccepted() async throws {
        let server = TimelineTransport()
        let owner = try await TimelineReplica(label: "owner", directory: directory(), transport: server)
        var expected = ""
        for day in 0..<14 {
            expected += "day-\(day): substantive work\n"
            try owner.write(expected, day: Double(day))
            let generation = try XCTUnwrap(owner.generation())
            let failure: CKError.Code = day.isMultiple(of: 2) ? .networkFailure : .serviceUnavailable
            await server.enqueue(.init(failure: failure))
            do { try await owner.drain(); XCTFail("Scheduled failure must not look successful") }
            catch { XCTAssertEqual((error as? CKError)?.code, failure) }
            XCTAssertEqual(owner.generation(), generation, "day \(day)")
            XCTAssertEqual(owner.value(), expected)
        }
        try await owner.drain()
        let saved = try await server.record(named: owner.recordName)
        XCTAssertEqual(saved?["text"] as? String, expected)
        XCTAssertNil(owner.generation())
        let calls = await server.history()
        XCTAssertEqual(calls.count, 15)
        let remaining = await server.remainingSteps()
        XCTAssertEqual(remaining, 0)
        await owner.stop()
    }

    @BigSyncBackgroundActor
    func testFakeServerOwnsSnapshotsRatherThanMutableCKRecordAliases() async throws {
        let server = TimelineTransport()
        let record = CKRecord(recordType: "Example", recordID: .init(recordName: "one"))
        record["text"] = "accepted" as CKRecordValue
        try await server.seed(record)
        record["text"] = "mutated caller" as CKRecordValue
        let received = try await server.record(named: "one")
        let read = try XCTUnwrap(received)
        XCTAssertEqual(read["text"] as? String, "accepted")
        read["text"] = "mutated response" as CKRecordValue
        let reread = try await server.record(named: "one")
        XCTAssertEqual(reread?["text"] as? String, "accepted")
    }
}
