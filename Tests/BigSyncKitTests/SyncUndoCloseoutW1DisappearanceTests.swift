import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    /// Native CKRecord archive fixture. This is not signed CloudKit evidence.
    /// Use the SDK's system-field setter, not CKRecord.setValue(forKey:),
    /// which writes user fields and rejects the reserved recordChangeTag key.
    /// The round-trip assertion verifies the resulting real system archive.
    func tagged(_ record: CKRecord, _ tag: String) throws -> CKRecord {
        guard record.responds(to: NSSelectorFromString("setRecordChangeTag:")) else {
            throw NSError(domain: "W1NativeFixture", code: 1,
                userInfo: [NSLocalizedDescriptionKey: "This CloudKit SDK cannot construct the tagged record fixture"])
        }
        _ = record.perform(NSSelectorFromString("setRecordChangeTag:"), with: tag as NSString)
        XCTAssertEqual(record.recordChangeTag, tag)
        let decoded = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(record))
        XCTAssertEqual(decoded.recordChangeTag, tag)
        return decoded
    }

    @BigSyncBackgroundActor
    func restart(_ original: RealmSwiftAdapter) async throws -> (RealmSwiftAdapter, Realm) {
        original.invalidateTokens()
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: original.persistenceRealmConfiguration,
            targetRealmConfigurations: original.targetRealmConfigurations, excludedClassNames: [],
            recordZoneID: original.recordZoneID, logger: Logger(label: "W1Restart"), startSetupTask: false)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "w1-account",
            replicaBindingGenerationIdentifier: "w1-binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.w1-closeout", databaseScope: .private)
        try await adapter.ensureSetup()
        adapter.invalidateTokens()
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    @BigSyncBackgroundActor
    func acceptedNote() async throws -> (RealmSwiftAdapter, Realm, W1ContractNote, CKRecord) {
        let (adapter, realm) = try await fixture()
        let incoming = try tagged(note(adapter), "accepted-A")
        _ = try await deliver([incoming], to: adapter)
        return (adapter, realm, try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)), incoming)
    }

    @BigSyncBackgroundActor
    func edit(_ object: W1ContractNote, text: String, time: Double,
                      realm: Realm, adapter: RealmSwiftAdapter) async throws -> String {
        try realm.write {
            object.text = text
            object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: time))
        }
        try await adapter.didFinishImport()
        return try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
    }

    @BigSyncBackgroundActor
    func assertRecreation(
        _ adapter: RealmSwiftAdapter, realm: Realm, record: CKRecord,
        expectedText: String, generation: String, previousRevision: String,
        file: StaticString = #filePath, line: UInt = #line
    ) throws {
        realm.refresh()
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID), file: file, line: line)
        XCTAssertEqual(object.text, expectedText, file: file, line: line)
        XCTAssertFalse(object.isDeleted, file: file, line: line)
        let base = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: record.recordID.recordName), file: file, line: line)
        XCTAssertTrue(base.isComparisonInvalidated, file: file, line: line)
        XCTAssertNotEqual(base.revision, previousRevision, file: file, line: line)
        XCTAssertFalse(base.revision.isEmpty, file: file, line: line)
        XCTAssertEqual(base.namespace, adapter.recordRebaseContext?.namespace, file: file, line: line)
        XCTAssertNil(base.serverChangeTag, file: file, line: line)
        XCTAssertNil(base.acceptedSystemFields, file: file, line: line)
        XCTAssertTrue(base.fields.count == 0, file: file, line: line)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty, file: file, line: line)
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: record.recordID.recordName)?.generation,
                       generation, file: file, line: line)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(ofType: SyncedEntity.self,
            forPrimaryKey: record.recordID.recordName), file: file, line: line)
        XCTAssertEqual(tracking.entityState, .new, file: file, line: line)
        XCTAssertEqual(tracking.pendingGeneration, generation, file: file, line: line)
        XCTAssertNil(tracking.encodedRecord, file: file, line: line)
    }

    @BigSyncBackgroundActor
    @discardableResult
    func drain(_ adapter: RealmSwiftAdapter, realm: Realm, expectsFreshTemplate: Bool = false) async throws -> [CKRecord] {
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        if expectsFreshTemplate {
            XCTAssertFalse(prepared.isEmpty)
            XCTAssertTrue(prepared.allSatisfy { $0.record.recordChangeTag == nil && !$0.requiresAcceptanceCheck })
        }
        let records = prepared.map(\.record)
        try await adapter.didUpload(savedRecords: records, matchingPreparedUploads: prepared)
        try await adapter.cleanUp()
        try await quiet(adapter, realm: realm)
        let audit = try await adapter.auditSynchronizationState(serverRecords: records)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        XCTAssertEqual(audit.comparisonEvidenceVersion, 1)
        XCTAssertEqual(audit.unresolvedSubmissionCount, 0)
        return records
    }

    @BigSyncBackgroundActor
    func testRemoteDeletionBeforeUploadRepairsAllEvidenceWithoutReauthoringValue() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "local survives", time: 30, realm: realm, adapter: adapter)
        let revision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        let outcomes = try await adapter.deleteRecords(with: [incoming.recordID])
        XCTAssertEqual(outcomes.first?.disposition, .preservedNewerLive(generation: generation))
        try assertRecreation(adapter, realm: realm, record: incoming, expectedText: "local survives",
            generation: generation, previousRevision: revision)
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 30))
        _ = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
    }

    @BigSyncBackgroundActor
    func testMissingServerBeforeDeletionFeedHasTheSameDurableDisposition() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "recreate", time: 30, realm: realm, adapter: adapter)
        let revision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(prepared.first?.record.recordChangeTag, "accepted-A")
        try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: prepared)
        try assertRecreation(adapter, realm: realm, record: incoming, expectedText: "recreate",
            generation: generation, previousRevision: revision)
        let fence = realm.objects(BigSyncRecordBaseline.self).first?.revision
        _ = try await adapter.deleteRecords(with: [incoming.recordID])
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, fence)
        _ = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
    }

    @BigSyncBackgroundActor
    func testRemoteDeletionRetiresStagedCandidateAndFencesItsLateAcknowledgment() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "staged edit", time: 30, realm: realm, adapter: adapter)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let revision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 1)
        _ = try await adapter.deleteRecords(with: [incoming.recordID])
        try assertRecreation(adapter, realm: realm, record: incoming, expectedText: "staged edit",
            generation: generation, previousRevision: revision)
        let fence = realm.objects(BigSyncRecordBaseline.self).first?.revision
        try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: prepared)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, fence)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        _ = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
    }

    @BigSyncBackgroundActor
    func testV2CommittedWhileMissingServerFailureWaitsKeepsItsValueAndGeneration() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let v1 = try await edit(object, text: "V1", time: 30, realm: realm, adapter: adapter)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let revision = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        let configuration = realm.configuration
        let id = noteID
        adapter._testBeforeMissingServerTargetWrite = {
            let current = try Realm(configuration: configuration)
            try current.write {
                let value = try XCTUnwrap(current.object(ofType: W1ContractNote.self, forPrimaryKey: id))
                value.text = "V2"
                value.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
            }
        }
        try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: prepared)
        adapter._testBeforeMissingServerTargetWrite = nil
        realm.refresh()
        let v2 = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first?.generation)
        XCTAssertNotEqual(v1, v2)
        try assertRecreation(adapter, realm: realm, record: incoming, expectedText: "V2",
            generation: v2, previousRevision: revision)
        XCTAssertEqual(object.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 40))
        try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: prepared)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, v2)
        _ = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
    }

    @BigSyncBackgroundActor
    func testOldMissingFailureCannotInvalidateANewerAcceptedBaselineOrCandidate() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        _ = try await edit(object, text: "local pending", time: 30, realm: realm, adapter: adapter)
        let old = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let newer = note(adapter, time: 40)
        newer["number"] = 21 as CKRecordValue
        _ = try await deliver([tagged(newer, "accepted-B")], to: adapter)
        let current = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let revision = base.revision, fields = base.fieldDigests, system = base.acceptedSystemFields
        let identity = realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity
        let generation = realm.objects(BigSyncPendingMutation.self).first?.generation
        let tracked = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first)
        let encoded = tracked.encodedRecord
        try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: old)
        try await adapter.didUpload(savedRecords: old.map(\.record), matchingPreparedUploads: old)
        XCTAssertEqual(base.revision, revision)
        XCTAssertEqual(base.fieldDigests, fields)
        XCTAssertEqual(base.acceptedSystemFields, system)
        XCTAssertEqual(base.serverChangeTag, "accepted-B")
        XCTAssertFalse(base.isComparisonInvalidated)
        XCTAssertEqual(object.number, 21)
        XCTAssertEqual(object.text, "local pending")
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, identity)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertEqual(tracked.encodedRecord, encoded)
        try await adapter.didUpload(savedRecords: current.map(\.record), matchingPreparedUploads: current)
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testCrashAfterDisappearanceTargetCommitIgnoresStaleTrackingOnRestart() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "survives restart", time: 30, realm: realm, adapter: adapter)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let tracked = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.objects(SyncedEntity.self).first)
        XCTAssertEqual(adapter.getRecord(for: tracked)?.recordChangeTag, "accepted-A")
        adapter._testAfterDisappearanceTargetWrite = { throw W1InjectedFailure.afterTarget }
        do {
            try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: prepared)
            XCTFail("Expected interruption after target commit")
        } catch W1InjectedFailure.afterTarget { }
        adapter._testAfterDisappearanceTargetWrite = nil
        XCTAssertEqual(adapter.getRecord(for: tracked)?.recordChangeTag, "accepted-A")
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.isComparisonInvalidated, true)
        XCTAssertNil(realm.objects(BigSyncRecordBaseline.self).first?.acceptedSystemFields)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let (restarted, reopened) = try await restart(adapter)
        let records = try await drain(restarted, realm: reopened, expectsFreshTemplate: true)
        XCTAssertEqual(records.first?["text"] as? String, "survives restart")
        let (again, sameRealm) = try await restart(restarted)
        try await quiet(again, realm: sameRealm)
    }

    @BigSyncBackgroundActor
    func testUncertainFirstSaveUnknownItemPreservesTheExactCandidateAcrossRestart() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        try realm.write { realm.add(object); object.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let first = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let record = try XCTUnwrap(first.first?.record)
        XCTAssertNil(record.recordChangeTag)
        let row = try XCTUnwrap(realm.objects(BigSyncRecordSubmission.self).first)
        let payload = row.payload, identity = row.candidateIdentity, generation = row.generation
        try await adapter.requeueMissingServerRecords([record.recordID], matchingPreparedUploads: first)
        XCTAssertEqual(row.payload, payload)
        XCTAssertEqual(row.candidateIdentity, identity)
        XCTAssertNil(realm.objects(BigSyncRecordBaseline.self).first)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let (restarted, reopened) = try await restart(adapter)
        let retry = try await restarted.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertTrue(try XCTUnwrap(retry.first).requiresAcceptanceCheck)
        XCTAssertEqual(retry.first?.comparisonBase?.submissionIdentity, identity)
        XCTAssertEqual(reopened.objects(BigSyncRecordSubmission.self).first?.payload, payload)
        try await restarted.didUpload(savedRecords: retry.map(\.record), matchingPreparedUploads: retry)
        try await quiet(restarted, realm: reopened)
    }

    @BigSyncBackgroundActor
    func testSecondDisappearanceOfANilBasedRecreationRotatesItsRevisionFence() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "recreated", time: 30, realm: realm, adapter: adapter)
        _ = try await adapter.deleteRecords(with: [incoming.recordID])
        let firstFence = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first?.revision)
        let recreated = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertNil(recreated.first?.record.recordChangeTag)
        _ = try await adapter.deleteRecords(with: [incoming.recordID])
        let secondFence = realm.objects(BigSyncRecordBaseline.self).first?.revision
        XCTAssertNotEqual(firstFence, secondFence)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        try await adapter.didUpload(savedRecords: recreated.map(\.record), matchingPreparedUploads: recreated)
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, secondFence)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        _ = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
    }

    @BigSyncBackgroundActor
    func testRetainedClearNeverEntersPhysicalRecreation() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1RetainedArticle()
        try realm.write { realm.add(object); object.epoch = "E0"; object.isDeleted = true; object.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let id = try XCTUnwrap(prepared.first?.record.recordID)
        let generation = realm.objects(BigSyncPendingMutation.self).first?.generation
        let identity = realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity
        do {
            try await adapter.requeueMissingServerRecords([id], matchingPreparedUploads: prepared)
            XCTFail("Retained lifetimes are not recreated notes")
        } catch BigSyncRecordContractError.unexpectedPhysicalDeletion { }
        let outcomes = try await adapter.deleteRecords(with: [id])
        guard case .quarantined = try XCTUnwrap(outcomes.first).disposition else { return XCTFail("Expected retained-deletion quarantine") }
        XCTAssertEqual(object.epoch, "E0")
        XCTAssertTrue(object.isDeleted)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, identity)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testMissingServerFinalWriteRejectsChangedBindingAndFullRecordID() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "fenced", time: 30, realm: realm, adapter: adapter)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let revision = realm.objects(BigSyncRecordBaseline.self).first?.revision
        let identity = realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity
        do {
            try await adapter.requeueMissingServerRecords([.init(recordName: incoming.recordID.recordName,
                zoneID: .init(zoneName: "other-zone"))], matchingPreparedUploads: prepared)
            XCTFail("A name in another zone is not the prepared record")
        } catch RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared { }
        adapter._testBeforeMissingServerTargetWrite = {
            try await adapter.activateReplicaBinding(accountScopeIdentifier: "other-account", replicaBindingGenerationIdentifier: "other-binding")
        }
        do {
            try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: prepared)
            XCTFail("The queued target write must revalidate binding")
        } catch is CancellationError { }
        adapter._testBeforeMissingServerTargetWrite = nil
        XCTAssertEqual(realm.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, identity)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        XCTAssertEqual(object.text, "fenced")
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "w1-account", replicaBindingGenerationIdentifier: "w1-binding")
        try await adapter.didUpload(savedRecords: prepared.map(\.record), matchingPreparedUploads: prepared)
        try await quiet(adapter, realm: realm)
    }
}

