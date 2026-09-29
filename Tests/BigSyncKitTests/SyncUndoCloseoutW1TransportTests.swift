import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func synchronizer(_ adapter: RealmSwiftAdapter, transport: W1ScriptedTransport) -> CloudKitSynchronizer {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("w1-dispatch-" + UUID().uuidString)
        addTeardownBlock { try? FileManager.default.removeItem(at: directory) }
        let synchronizer = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.w1-closeout", database: W1DatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: W1KeyValueStore(),
            accountIdentifierProvider: { "w1-account" }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: transport,
            recordStore: transport, backupDetectionBaseURL: directory, logger: Logger(label: "W1Dispatch"))
        synchronizer.addModelAdapter(adapter)
        return synchronizer
    }

    @BigSyncBackgroundActor
    func fetchScriptedW1ZonePage(
        _ synchronizer: CloudKitSynchronizer, adapter: RealmSwiftAdapter
    ) async throws {
        let runID = await synchronizer.changeRequestProcessor.beginRun()
        synchronizer.synchronizationRunID = runID
        synchronizer.activeRunContext = .init(
            attemptID: synchronizer.synchronizationAttemptID,
            runID: runID,
            accountIdentifier: "w1-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(
                for: "w1-account"
            )
        )
        let zones = try await synchronizer.loadTokens(for: [adapter.recordZoneID])
        try await synchronizer.fetchZoneChanges(zones)
    }

    @BigSyncBackgroundActor
    func testFetchedDeletionPageReplaysAfterTargetFirstInterruptionAndUploadsV2Recreation()
    async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let transport = W1ScriptedTransport()
        let firstSynchronizer = synchronizer(adapter, transport: transport)
        let wakeups = W1ExplicitWakeups()
        adapter.modelAdapterDelegate = wakeups

        // Commit an earlier, empty page through the production fetch path so
        // the deletion must replay from a real durable preceding cursor.
        try await fetchScriptedW1ZonePage(firstSynchronizer, adapter: adapter)
        let seededCursor = await adapter.serverChangeToken
        XCTAssertEqual(seededCursor?.serializedData,
            W1ScriptedTransport.seedZoneCursor)

        let v1 = try await edit(object, text: "V1", time: 30,
            realm: realm, adapter: adapter)
        let oldPrepared = try await adapter.preparedRecordsToUpload(
            limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(oldPrepared.count, 1)
        XCTAssertEqual(oldPrepared.first?.record["text"] as? String, "V1")
        XCTAssertEqual(oldPrepared.first?.record.recordChangeTag, "accepted-A")
        let priorRevision = try XCTUnwrap(realm.objects(
            BigSyncRecordBaseline.self).first?.revision)
        let v2 = try await edit(object, text: "V2", time: 40,
            realm: realm, adapter: adapter)
        XCTAssertNotEqual(v1, v2)
        await transport.serveDeletionPage(for: incoming.recordID)

        adapter._testAfterDisappearanceTargetWrite = {
            throw W1InjectedFailure.afterTarget
        }
        do {
            try await fetchScriptedW1ZonePage(firstSynchronizer,
                adapter: adapter)
            XCTFail("Expected interruption after the deletion target write")
        } catch W1InjectedFailure.afterTarget { }
        adapter._testAfterDisappearanceTargetWrite = nil

        try assertTargetCommittedBeforeTracking(adapter, realm: realm,
            record: incoming, expectedText: "V2", generation: v2,
            previousRevision: priorRevision)
        XCTAssertEqual(object.modifiedAt,
            Date(timeIntervalSinceReferenceDate: 40))
        XCTAssertEqual(object.explicitlyModifiedAt,
            Date(timeIntervalSinceReferenceDate: 40))
        let interruptedCursor = await adapter.serverChangeToken
        XCTAssertEqual(interruptedCursor?.serializedData,
            W1ScriptedTransport.seedZoneCursor)
        XCTAssertEqual(firstSynchronizer.activeZoneTokens[
            adapter.recordZoneID]?.serializedData,
            W1ScriptedTransport.seedZoneCursor)
        let staleTracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?
            .object(ofType: SyncedEntity.self,
                forPrimaryKey: incoming.recordID.recordName))
        XCTAssertEqual(adapter.getRecord(for: staleTracking)?.recordChangeTag,
            "accepted-A")

        let (restarted, reopened) = try await restart(adapter)
        try assertTargetCommittedBeforeTracking(restarted, realm: reopened,
            record: incoming, expectedText: "V2", generation: v2,
            previousRevision: priorRevision)
        let interruptedRevision = try XCTUnwrap(reopened.objects(
            BigSyncRecordBaseline.self).first?.revision)
        try await restarted.didUpload(savedRecords: oldPrepared.map(\.record),
            matchingPreparedUploads: oldPrepared)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision,
            interruptedRevision)
        XCTAssertEqual(reopened.objects(BigSyncPendingMutation.self).first?.generation,
            v2)

        let replaySynchronizer = synchronizer(restarted, transport: transport)
        restarted.modelAdapterDelegate = wakeups
        try await fetchScriptedW1ZonePage(replaySynchronizer,
            adapter: restarted)
        let fetchedCursors = await transport.zoneCursorHistory()
        XCTAssertEqual(fetchedCursors, [
            nil, W1ScriptedTransport.seedZoneCursor,
            W1ScriptedTransport.seedZoneCursor
        ])
        let replayedCursor = await restarted.serverChangeToken
        XCTAssertEqual(replayedCursor?.serializedData,
            W1ScriptedTransport.deletionZoneCursor)
        try assertRecreation(restarted, realm: reopened, record: incoming,
            expectedText: "V2", generation: v2,
            previousRevision: priorRevision)
        let replayedRevision = reopened.objects(BigSyncRecordBaseline.self)
            .first?.revision
        try await restarted.didUpload(savedRecords: oldPrepared.map(\.record),
            matchingPreparedUploads: oldPrepared)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision,
            replayedRevision)
        XCTAssertEqual(reopened.objects(BigSyncPendingMutation.self).first?.generation,
            v2)

        try await replaySynchronizer.synchronizeAdapter(restarted)
        let calls = await transport.history()
        XCTAssertEqual(calls.count, 1)
        XCTAssertEqual(calls.first?.texts, ["V2"])
        XCTAssertEqual(calls.first?.tags, [nil])
        let saved = try await transport.inventory()
        XCTAssertEqual(saved.count, 1)
        XCTAssertEqual(saved.first?["text"] as? String, "V2")
        let audit = try await restarted.auditSynchronizationState(
            serverRecords: saved)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        try await quiet(restarted, realm: reopened)
        _ = wakeups
    }

    @BigSyncBackgroundActor
    func testActualUploadDrainForwardsPreparedMissingEvidenceAndPreservesInFlightV2() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        _ = try await edit(object, text: "V1", time: 30, realm: realm, adapter: adapter)
        let config = realm.configuration
        let id = noteID
        let transport = W1ScriptedTransport(missingFirstSave: true, afterFirstSave: {
            try await { @BigSyncBackgroundActor in
                let writerRealm = try Realm(configuration: config)
                try writerRealm.write {
                    let value = try XCTUnwrap(writerRealm.object(ofType: W1ContractNote.self, forPrimaryKey: id))
                    value.text = "V2 during actual transport"
                    value.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
                }
            }()
        })
        let synchronizer = synchronizer(adapter, transport: transport)
        let wakeups = W1ExplicitWakeups()
        adapter.modelAdapterDelegate = wakeups
        try await synchronizer.synchronizeAdapter(adapter)
        realm.refresh()
        XCTAssertEqual(object.text, "V2 during actual transport")
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 40))
        let calls = await transport.history()
        XCTAssertEqual(calls.count, 2)
        XCTAssertEqual(calls.first?.texts, ["V1"])
        XCTAssertEqual(calls.first?.tags, ["accepted-A"])
        XCTAssertEqual(calls.last?.texts, ["V2 during actual transport"])
        XCTAssertEqual(calls.last?.tags, [nil])
        let saved = try await transport.inventory()
        let audit = try await adapter.auditSynchronizationState(serverRecords: saved)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        XCTAssertEqual(audit.unresolvedSubmissionCount, 0)
        try await quiet(adapter, realm: realm)
        try await synchronizer.synchronizeAdapter(adapter)
        let second = await transport.history()
        XCTAssertEqual(second.count, calls.count)
        _ = wakeups
    }

    @BigSyncBackgroundActor
    func testActualAcceptanceLookupUnknownItemRetriesUncertainFirstCandidateWithoutDiscardingIt() async throws {
        let (original, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        object.text = "uncertain first candidate"
        let authored = Date(timeIntervalSinceReferenceDate: 40)
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true, at: authored)
        }
        try await original.didFinishImport()
        let first = try await original.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let staged = try XCTUnwrap(realm.objects(BigSyncRecordSubmission.self).first)
        let stagedIdentity = staged.candidateIdentity, generation = staged.generation
        let (adapter, reopened) = try await restart(original)
        let transport = W1ScriptedTransport()
        let synchronizer = synchronizer(adapter, transport: transport)
        let wakeups = W1ExplicitWakeups()
        adapter.modelAdapterDelegate = wakeups
        try await synchronizer.synchronizeAdapter(adapter)
        let calls = await transport.history(), lookups = await transport.lookups()
        XCTAssertEqual(lookups, 1)
        XCTAssertEqual(calls.count, 1)
        XCTAssertEqual(calls.first?.texts, ["uncertain first candidate"])
        XCTAssertEqual(calls.first?.tags, [nil])
        let baseline = try XCTUnwrap(reopened.objects(BigSyncRecordBaseline.self).first)
        XCTAssertEqual(baseline.revision, stagedIdentity, "A lookup miss must not cause a new staged identity")
        XCTAssertEqual(first.first?.generation, generation)
        XCTAssertEqual(object.modifiedAt, authored)
        let saved = try await transport.inventory()
        let audit = try await adapter.auditSynchronizationState(serverRecords: saved)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        try await quiet(adapter, realm: reopened)
        try await synchronizer.synchronizeAdapter(adapter)
        let second = await transport.history()
        XCTAssertEqual(second.count, 1)
        _ = wakeups
    }
}
