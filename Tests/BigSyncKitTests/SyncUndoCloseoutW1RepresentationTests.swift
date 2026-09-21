import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testOmissionsClearCollectionsButPreserveIndependentPendingTextAcrossRestart() async throws {
        let (adapter, realm) = try await fixture()
        let first = note(adapter)
        first["optional"] = "previous" as CKRecordValue
        first["list"] = [1, 2] as CKRecordValue
        first["members"] = ["before"] as CKRecordValue
        first["map"] = try PropertyListSerialization.data(fromPropertyList: ["before": 3], format: .binary, options: 0) as CKRecordValue
        _ = try await deliver([first], to: adapter)
        let object = try XCTUnwrap(realm.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        _ = try await edit(object, text: "independent local text", time: 30, realm: realm, adapter: adapter)
        _ = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let omitted = note(adapter, time: 20)
        omitted["number"] = nil
        omitted["flag"] = nil
        _ = try await deliver([omitted], to: adapter)
        XCTAssertEqual(object.number, 0)
        XCTAssertFalse(object.flag)
        XCTAssertNil(object.optional)
        XCTAssertTrue(object.list.isEmpty && object.members.isEmpty && object.map.count == 0)
        XCTAssertEqual(object.text, "independent local text")
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSinceReferenceDate: 30))
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let managed = try BigSyncRecordFingerprint.fields(of: object)
        XCTAssertEqual(Set(managed.keys.filter { managed[$0] != base.fieldDigests[$0] }), ["text"])
        let revision = base.revision
        let generation = realm.objects(BigSyncPendingMutation.self).first?.generation
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        _ = try await deliver([omitted], to: adapter)
        XCTAssertEqual(base.revision, revision)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let (restarted, reopened) = try await restart(adapter)
        _ = try await deliver([omitted], to: restarted)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        XCTAssertEqual(reopened.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let final = try await drain(restarted, realm: reopened)
        XCTAssertEqual(final.first?["text"] as? String, "independent local text")
        XCTAssertEqual(final.first?["number"] as? Int, 0)
        XCTAssertEqual(final.first?["flag"] as? Bool, false)
    }

    @BigSyncBackgroundActor
    func testExplicitDefaultsAndEmptyCollectionsHaveTheSameAcceptedRepresentation() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([note(adapter)], to: adapter)
        let record = note(adapter, time: 20)
        record["number"] = 0 as CKRecordValue
        record["flag"] = false as CKRecordValue
        record["list"] = [Int]() as CKRecordValue
        record["members"] = [String]() as CKRecordValue
        record["map"] = try PropertyListSerialization.data(fromPropertyList: [String: Int](), format: .binary, options: 0) as CKRecordValue
        _ = try await deliver([record], to: adapter)
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let revision = base.revision, fields = base.fieldDigests
        let omitted = note(adapter, time: 20)
        omitted["number"] = nil
        omitted["flag"] = nil
        _ = try await deliver([omitted], to: adapter)
        XCTAssertEqual(base.fieldDigests, fields)
        XCTAssertEqual(base.revision, revision)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: XCTUnwrap(realm.objects(W1ContractNote.self).first)), fields)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        let (again, reopened) = try await restart(adapter)
        _ = try await deliver([omitted], to: again)
        try await quiet(again, realm: reopened)
        let audit = try await again.auditSynchronizationState(serverRecords: [omitted])
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
    }

    @BigSyncBackgroundActor
    func testMissingRequiredPayloadOrTimestampRejectsWithoutChangingEvidence() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([note(adapter)], to: adapter)
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let revision = base.revision, fields = base.fieldDigests, system = base.acceptedSystemFields
        for field in ["text", "createdAt", "modifiedAt"] {
            let missing = note(adapter, time: 20)
            missing[field] = nil
            do {
                _ = try await deliver([missing], to: adapter)
                XCTFail("Required field \(field) must reject the incoming representation")
            } catch let error as BigSyncIncomingRepresentationError {
                XCTAssertEqual(error, .missingRequiredField(recordType: W1ContractNote.className(), field: field))
            }
            XCTAssertEqual(base.revision, revision)
            XCTAssertEqual(base.fieldDigests, fields)
            XCTAssertEqual(base.acceptedSystemFields, system)
            XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: XCTUnwrap(realm.objects(W1ContractNote.self).first)), fields)
            XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        }
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testActualManagedMismatchRollsBackValueJournalBaselineAndPreservationCopy() async throws {
        let (adapter, realm) = try await fixture()
        _ = try await deliver([note(adapter)], to: adapter)
        let object = try XCTUnwrap(realm.objects(W1ContractNote.self).first)
        let generation = try await edit(object, text: "losing local text", time: 30, realm: realm, adapter: adapter)
        _ = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let identity = realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity
        let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let revision = base.revision, fields = base.fieldDigests, system = base.acceptedSystemFields
        let incoming = note(adapter, time: 40)
        incoming["text"] = "winning remote text" as CKRecordValue
        adapter._testAfterComparisonApplication = { value in value.setValue(999, forKey: "number") }
        do {
            _ = try await deliver([incoming], to: adapter)
            XCTFail("The managed result must be checked inside the target transaction")
        } catch let error as BigSyncIncomingRepresentationError {
            XCTAssertEqual(error, .actualValueMismatch(recordName: incoming.recordID.recordName))
        }
        adapter._testAfterComparisonApplication = nil
        realm.refresh()
        XCTAssertEqual(object.text, "losing local text")
        XCTAssertEqual(object.number, 9)
        XCTAssertEqual(realm.objects(W1ContractNote.self).count, 1)
        XCTAssertTrue(realm.objects(BigSyncRecordConflict.self).isEmpty)
        XCTAssertEqual(base.revision, revision)
        XCTAssertEqual(base.fieldDigests, fields)
        XCTAssertEqual(base.acceptedSystemFields, system)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, identity)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(object.text, "winning remote text")
        XCTAssertEqual(realm.objects(W1ContractNote.self).count, 2)
        let receipt = try XCTUnwrap(realm.objects(BigSyncRecordConflict.self).first)
        XCTAssertTrue(receipt.isResolved && receipt.isPreservationReceipt)
        // Seed the server inventory with the accepted original, then apply
        // successful saves by full ID (the journal may also save the original).
        try await adapter.didFinishImport()
        let copies = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        try await adapter.didUpload(savedRecords: copies.map(\.record), matchingPreparedUploads: copies)
        try await adapter.cleanUp()
        try await quiet(adapter, realm: realm)
        var inventory = [incoming.recordID: incoming]
        for item in copies { inventory[item.record.recordID] = item.record }
        let audit = try await adapter.auditSynchronizationState(serverRecords: Array(inventory.values))
        XCTAssertEqual(audit.resolvedPreservationReceiptCount, 1)
        XCTAssertEqual(audit.unresolvedSubmissionCount, 0)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
    }

    @BigSyncBackgroundActor
    func testRetiredSaveCannotReacquireAnIdenticalRecreatedLifetimeAfterRestart() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1ContractNote()
        object.id = noteID
        object.createdAt = Date(timeIntervalSinceReferenceDate: 1)
        try realm.write { realm.add(object); object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 30)) }
        try await adapter.didFinishImport()
        let old = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let oldRecord = try XCTUnwrap(old.first?.record)
        try realm.write { object.isDeleted = true; object.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40)) }
        try await adapter.didFinishImport()
        let deletion = try await adapter.prepareDeletionBatch(limit: 50)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await adapter.cleanUp()
        let fence = realm.objects(BigSyncRecordBaseline.self).first?.revision
        let (restarted, reopened) = try await restart(adapter)
        XCTAssertNil(reopened.object(ofType: W1ContractNote.self, forPrimaryKey: noteID))
        XCTAssertTrue(reopened.objects(BigSyncRecordSubmission.self).isEmpty)
        try await restarted.didUpload(savedRecords: old.map(\.record), matchingPreparedUploads: old)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, fence)
        let newObject = try XCTUnwrap(restarted.decodedComparisonObject(oldRecord, type: W1ContractNote.self) as? W1ContractNote)
        newObject.id = noteID
        try reopened.write { reopened.add(newObject); newObject.journalCurrentValuePreservingChangeMetadata(at: Date()) }
        try await restarted.didFinishImport()
        let current = try await restarted.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        XCTAssertEqual(current.first?.comparisonBase?.fields, old.first?.comparisonBase?.fields)
        XCTAssertNotEqual(current.first?.comparisonBase?.submissionIdentity, old.first?.comparisonBase?.submissionIdentity)
        try await restarted.didUpload(savedRecords: current.map(\.record), matchingPreparedUploads: current)
        let acceptedRevision = reopened.objects(BigSyncRecordBaseline.self).first?.revision
        try reopened.write { newObject.journalCurrentValuePreservingChangeMetadata(at: Date()) }
        try await restarted.didFinishImport()
        let latest = try await restarted.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let latestIdentity = reopened.objects(BigSyncRecordSubmission.self).first?.candidateIdentity
        let latestGeneration = reopened.objects(BigSyncPendingMutation.self).first?.generation
        try await restarted.didUpload(savedRecords: old.map(\.record), matchingPreparedUploads: old)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, acceptedRevision)
        XCTAssertEqual(reopened.objects(BigSyncRecordSubmission.self).first?.candidateIdentity, latestIdentity)
        XCTAssertEqual(reopened.objects(BigSyncPendingMutation.self).first?.generation, latestGeneration)
        XCTAssertFalse(newObject.isDeleted)
        try await restarted.didUpload(savedRecords: latest.map(\.record), matchingPreparedUploads: latest)
        try await quiet(restarted, realm: reopened)
    }

    @BigSyncBackgroundActor
    func testAuditAndPublicationRejectOrphanedActiveSubmissionDespiteEmptyJournals() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        _ = try await edit(object, text: object.text, time: 10, realm: realm, adapter: adapter)
        _ = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        // Deliberate corruption in the adapter test, never application code.
        try realm.write { realm.delete(realm.objects(BigSyncPendingMutation.self)) }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        try tracking.write {
            let row = try XCTUnwrap(tracking.objects(SyncedEntity.self).first)
            row.entityState = .synced
            row.clearPendingMutation()
        }
        let audit = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(audit.isClean)
        XCTAssertEqual(audit.pendingMutationCount, 0)
        XCTAssertEqual(audit.unresolvedSubmissionCount, 1)
        XCTAssertTrue(audit.issues.contains("orphaned-active-submission:" + incoming.recordID.recordName))
        XCTAssertThrowsError(try adapter.hasPendingChangesAtTerminalBoundary()) { error in
            XCTAssertTrue(error is BigSyncComparisonEvidenceError)
        }
        let blockers = try await adapter.semanticPublicationBlockers()
        XCTAssertTrue(blockers.contains { $0.code == "unresolved-record-submission" })
    }

    @BigSyncBackgroundActor
    func testAuditDetectsAcceptedEvidenceThatDoesNotDescribeTheManagedValue() async throws {
        let (adapter, realm, _, incoming) = try await acceptedNote()
        try realm.write {
            let base = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
            base.fields["number"] = Data(repeating: 0, count: 32)
        }
        let audit = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(audit.isClean)
        XCTAssertEqual(audit.acceptedBaselineCount, 1)
        XCTAssertTrue(audit.issues.contains("accepted-comparison-unexplained-local-value:" + incoming.recordID.recordName))
        XCTAssertThrowsError(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testDisappearanceRetiresOnlyTheActiveNamespaceCandidate() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let generation = try await edit(object, text: "active edit", time: 30, realm: realm, adapter: adapter)
        let prepared = try await adapter.preparedRecordsToUpload(limit: 50, restrictedToEntityType: nil)
        let active = try XCTUnwrap(realm.objects(BigSyncRecordSubmission.self).first)
        let foreign = BigSyncRecordSubmission()
        foreign.namespace = "different-account-binding-zone"
        foreign.id = BigSyncRecordPayload.identity([foreign.namespace, active.recordName])
        foreign.recordName = active.recordName
        foreign.schemaSignature = active.schemaSignature
        foreign.generation = active.generation
        foreign.comparisonRevision = active.comparisonRevision
        foreign.payload = active.payload
        for entry in active.fields { foreign.fields[entry.key] = entry.value }
        try realm.write { realm.add(foreign) }
        let identity = foreign.candidateIdentity
        try await adapter.requeueMissingServerRecords([incoming.recordID], matchingPreparedUploads: prepared)
        XCTAssertEqual(realm.objects(BigSyncRecordSubmission.self).count, 1)
        XCTAssertEqual(foreign.candidateIdentity, identity)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, generation)
        let records = try await drain(adapter, realm: realm, expectsFreshTemplate: true)
        let audit = try await adapter.auditSynchronizationState(serverRecords: records)
        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: ","))
        XCTAssertEqual(audit.unresolvedSubmissionCount, 0)
        XCTAssertEqual(foreign.candidateIdentity, identity)
    }

    func testOlderAuditArtifactsDecodeButDoNotClaimEvidenceInspection() throws {
        let data = Data("""
        {"serverRecordCount":0,"ownedServerRecordCount":0,"unknownServerRecordCount":0,
         "localObjectCount":0,"trackingRecordCount":0,"pendingMutationCount":0,
         "pendingRelationshipCount":0,"issues":[]}
        """.utf8)
        let older = try JSONDecoder().decode(BigSyncSynchronizationAudit.self, from: data)
        XCTAssertEqual(older.comparisonEvidenceVersion, 0)
        XCTAssertEqual(older.unresolvedSubmissionCount, 0)
        XCTAssertTrue(older.isClean)
        XCTAssertEqual(try JSONDecoder().decode(BigSyncSynchronizationAudit.self,
            from: JSONEncoder().encode(older)), older)
    }
}

// Protocol transport with scripted outcomes; the real synchronizer, adapter,
// target/tracking Realms and journal own all decisions. No production CloudKit
// container, signed resources, or server-side CAS claims are involved.
final class W1DatabaseIdentity: NSObject, CloudKitDatabaseAdapter {
    var databaseScope: CKDatabase.Scope { .private }
}

final class W1KeyValueStore: NSObject, KeyValueStore {
    var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}

@BigSyncBackgroundActor
final class W1ExplicitWakeups: ModelAdapterDelegate {
    func needsInitialSetup() async throws {}
    func hasChangesToUpload() async {}
}

actor W1ScriptedTransport: CloudKitRecordStore, CloudKitRecordFetching,
    CloudKitChangeFeed, CloudKitSubscriptionStore, CloudKitZoneStore {
    struct Call: Sendable {
        let texts: [String]
        let tags: [String?]
    }
    var missingFirstSave: Bool
    var afterFirstSave: (@Sendable () async throws -> Void)?
    var calls = [Call]()
    var lookupCount = 0
    var storage = [CKRecord.ID: Data]()

    init(missingFirstSave: Bool = false,
         afterFirstSave: (@Sendable () async throws -> Void)? = nil) {
        self.missingFirstSave = missingFirstSave
        self.afterFirstSave = afterFirstSave
    }
    func history() -> [Call] { calls }
    func lookups() -> Int { lookupCount }
    func inventory() throws -> [CKRecord] {
        try storage.values.map { try BigSyncRecordPayload.decode($0) }
    }
    func fetchRecords(with recordIDs: [CKRecord.ID]) async throws -> [CKRecord.ID: Result<CKRecord, Error>] {
        lookupCount += 1
        return try Dictionary(uniqueKeysWithValues: recordIDs.map { id in
            if let bytes = storage[id] { return (id, .success(try BigSyncRecordPayload.decode(bytes))) }
            return (id, .failure(CKError(.unknownItem)))
        })
    }
    func modifyRecords(saving records: [CKRecord], deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        guard savePolicy == .ifServerRecordUnchanged, !atomically else {
            throw NSError(domain: "W1UnexpectedTransportSurface", code: 1)
        }
        calls.append(.init(texts: records.compactMap { $0["text"] as? String },
                           tags: records.map(\.recordChangeTag)))
        var saved = [CKRecord.ID: Result<CKRecord, Error>]()
        var deleted = [CKRecord.ID: Result<Void, Error>]()
        for record in records {
            if calls.count == 1 && missingFirstSave {
                saved[record.recordID] = .failure(CKError(.unknownItem))
            } else {
                let bytes = try BigSyncRecordPayload.encode(record)
                storage[record.recordID] = bytes
                saved[record.recordID] = .success(try BigSyncRecordPayload.decode(bytes))
            }
        }
        for id in recordIDs {
            storage.removeValue(forKey: id)
            deleted[id] = .success(())
        }
        if calls.count == 1 { try await afterFirstSave?() }
        return .init(saveResults: saved, deleteResults: deleted)
    }
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        throw NSError(domain: "W1UnexpectedTransportSurface", code: 2)
    }
    func recordZoneChanges(in: CKRecordZone.ID, since: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        throw NSError(domain: "W1UnexpectedTransportSurface", code: 3)
    }
    func subscription(withID: CKSubscription.ID) async throws -> CKSubscription? { nil }
    func save(subscription: CKSubscription) async throws -> CKSubscription { subscription }
    func deleteSubscription(withID: CKSubscription.ID) async throws {}
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone { CKRecordZone(zoneID: id) }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { recordZone }
    func deleteRecordZone(withID: CKRecordZone.ID) async throws {
        throw NSError(domain: "W1UnexpectedTransportSurface", code: 4)
    }
}

