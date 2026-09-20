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

