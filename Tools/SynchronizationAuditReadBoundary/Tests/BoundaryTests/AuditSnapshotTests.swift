import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

@BigSyncBackgroundActor
private struct Fixture {
    let target: Realm
    let tracking: Realm
    let adapter: RealmSwiftAdapter
    let server: CKRecord
    let name = AuditNote.className() + ".note"

    init(scoped: Bool = false) throws {
        target = Realm(types: [AuditNote.self, AuditOtherNote.self, BigSyncPendingMutation.self,
            BigSyncRecordBaseline.self, BigSyncRecordSubmission.self, BigSyncRecordConflict.self])
        tracking = Realm(types: [SyncedEntity.self, PendingRelationship.self, BigSyncInboundSemanticQuarantine.self])
        adapter = RealmSwiftAdapter(target: target, tracking: tracking)
        if scoped { adapter.accountScopePropertyByClassName[AuditNote.className()] = "owner" }
        let note = AuditNote(); note["id"] = "note"; note.owner = "account-1"
        server = try BigSyncRecordPayload.record(from: note, recordID: .init(recordName: name, zoneID: adapter.recordZoneID))
        server.recordChangeTag = "accepted-tag"
        let template = CKRecord(recordType: server.recordType, recordID: server.recordID)
        template.recordChangeTag = server.recordChangeTag
        let baseline = BigSyncRecordBaseline(); baseline.recordName = name
        baseline.serverChangeTag = server.recordChangeTag
        baseline.acceptedSystemFields = try JSONEncoder().encode(template)
        baseline.fieldDigests = try BigSyncRecordFingerprint.fields(of: note)
        try target.write { target.add(note); target.add(baseline) }
        let entity = SyncedEntity(); entity.identifier = name; entity.record = server
        try tracking.write { tracking.add(entity) }
    }
    func note() throws -> AuditNote { try XCTUnwrap(target.object(ofType: AuditNote.self, forPrimaryKey: "note")) }
    func entity() throws -> SyncedEntity { try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)) }
    func base() throws -> BigSyncRecordBaseline { try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)) }
    func mutation() -> BigSyncPendingMutation? { target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name) }
    func audit(records: [CKRecord]? = nil) async throws -> BigSyncSynchronizationAudit {
        try await adapter.auditSynchronizationState(serverRecords: records ?? [server])
    }
    func addMutation() {
        let mutation = BigSyncPendingMutation(); mutation.recordName = name
        mutation.replicaBindingGenerationIdentifier = "binding-1"; mutation.accountScopeIdentifier = "account-1"
        target.add(mutation)
    }
    func addSubmission() {
        let row = BigSyncRecordSubmission(); row["id"] = "submission"; row.recordName = name; target.add(row)
    }
    func clearTargetDebt() throws {
        try note().text = "accepted"
        if let row = mutation() { target.delete(row) }
        if let row = target.object(ofType: BigSyncRecordSubmission.self, forPrimaryKey: "submission") { target.delete(row) }
    }
    func abortOwners() {
        if target.isInWriteTransaction { target.cancelWrite() }
        if tracking.isInWriteTransaction { tracking.cancelWrite() }
    }
}

@BigSyncBackgroundActor
private func exercise(scoped: Bool = false, _ operation: @BigSyncBackgroundActor (Fixture) async throws -> Void) async throws {
    let fixture = try Fixture(scoped: scoped)
    defer { fixture.abortOwners() }
    try await operation(fixture)
}

final class AuditSnapshotTests: XCTestCase {
    nonisolated func testCleanCommittedReplicaRetainsFullAuditRepresentation() async throws {
        try await exercise { f in
            let result = try await f.audit()
            XCTAssertTrue(result.isClean, result.issues.joined(separator: ","))
            XCTAssertEqual(result.acceptedBaselineCount, 1)
            XCTAssertEqual(result.localObjectCount, 1)
            XCTAssertEqual(result.trackingRecordCount, 1)
            XCTAssertEqual(result.comparisonEvidenceVersion, 1)
            XCTAssertEqual(try JSONDecoder().decode(BigSyncSynchronizationAudit.self,
                from: JSONEncoder().encode(result)), result)
        }
    }

    nonisolated func testCommittedMutationDebtRemainsVisible() async throws {
        try await exercise { f in
            try f.target.write { try f.note().text = "pending"; f.addMutation() }
            let result = try await f.audit()
            XCTAssertFalse(result.isClean)
            XCTAssertEqual(result.pendingMutationCount, 1)
            XCTAssertTrue(result.issues.contains("pending-mutations:1"))
        }
    }

    nonisolated func testProvisionalTargetAcknowledgementCannotCertifyClean() async throws {
        try await exercise { f in
            try f.target.write { try f.note().text = "pending"; f.addMutation() }
            let before = try await f.audit()
            try f.target.beginWrite(); try f.clearTargetDebt()
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertFalse(result.isClean)
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertNil(f.mutation())
            XCTAssertEqual(try f.note().text, "accepted")
            f.target.cancelWrite()
            let after = try await f.audit()
            XCTAssertEqual(after, before)
        }
    }

    nonisolated func testProvisionalTrackingAcknowledgementCannotCertifyClean() async throws {
        try await exercise { f in
            try f.tracking.write { try f.entity().entityState = .changed; try f.entity().pendingGeneration = "pending-1" }
            let before = try await f.audit()
            XCTAssertFalse(before.isClean)
            try f.tracking.beginWrite(); try f.entity().entityState = .synced; try f.entity().pendingGeneration = nil
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertFalse(result.isClean)
            XCTAssertTrue(f.tracking.isInWriteTransaction)
            XCTAssertNil(try f.entity().pendingGeneration)
        }
    }

    nonisolated func testProvisionalEditsDoNotManufactureCommittedDebt() async throws {
        try await exercise { f in
            let before = try await f.audit(); XCTAssertTrue(before.isClean)
            try f.target.beginWrite(); try f.note().text = "provisional"; f.addMutation()
            try f.tracking.beginWrite(); try f.entity().entityState = .changed; try f.entity().pendingGeneration = "provisional"
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertTrue(f.target.isInWriteTransaction); XCTAssertTrue(f.tracking.isInWriteTransaction)
            XCTAssertEqual(try f.note().text, "provisional")
            XCTAssertEqual(try f.entity().pendingGeneration, "provisional")
        }
    }

    nonisolated func testProvisionalSubmissionRemovalDoesNotHideEvidenceDebt() async throws {
        try await exercise { f in
            try f.target.write { f.addSubmission() }
            let before = try await f.audit(); XCTAssertEqual(before.unresolvedSubmissionCount, 1)
            try f.target.beginWrite()
            f.target.delete(try XCTUnwrap(f.target.object(ofType: BigSyncRecordSubmission.self, forPrimaryKey: "submission")))
            let result = try await f.audit()
            XCTAssertEqual(result, before); XCTAssertFalse(result.isClean)
            XCTAssertTrue(f.target.isInWriteTransaction)
        }
    }

    nonisolated func testProvisionalRelationshipAndQuarantineRemovalCannotCertifyClean() async throws {
        try await exercise { f in
            let relationship = PendingRelationship(); relationship["id"] = "relationship"
            let quarantine = BigSyncInboundSemanticQuarantine(); quarantine["id"] = "quarantine"
            quarantine["entityType"] = AuditNote.className(); quarantine["account"] = "account-1"; quarantine.recordName = f.name
            try f.tracking.write { f.tracking.add(relationship); f.tracking.add(quarantine) }
            let before = try await f.audit(); XCTAssertFalse(before.isClean)
            try f.tracking.beginWrite()
            f.tracking.delete(try XCTUnwrap(f.tracking.object(ofType: PendingRelationship.self, forPrimaryKey: "relationship")))
            f.tracking.delete(try XCTUnwrap(f.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: "quarantine")))
            let result = try await f.audit()
            XCTAssertEqual(result, before); XCTAssertFalse(result.isClean)
            XCTAssertTrue(f.tracking.isInWriteTransaction)
        }
    }

    nonisolated func testProvisionalBaselineRepairCannotHideCommittedCorruption() async throws {
        try await exercise { f in
            try f.target.write { try f.base().fieldDigests = [:] }
            let before = try await f.audit(); XCTAssertFalse(before.isClean)
            try f.target.beginWrite(); try f.base().fieldDigests = BigSyncRecordFingerprint.fields(of: f.note())
            let result = try await f.audit()
            XCTAssertEqual(result, before); XCTAssertFalse(result.isClean)
            XCTAssertTrue(f.target.isInWriteTransaction)
        }
    }

    nonisolated func testProvisionalAccountChangeCannotHideCurrentAccountTracking() async throws {
        try await exercise(scoped: true) { f in
            let before = try await f.audit(); XCTAssertTrue(before.isClean)
            try f.target.beginWrite(); try f.note().owner = "account-2"
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertEqual(result.trackingRecordCount, 1)
            XCTAssertTrue(f.target.isInWriteTransaction); XCTAssertEqual(try f.note().owner, "account-2")
        }
    }

    nonisolated func testProvisionalAccountChangeCannotAdoptForeignTracking() async throws {
        try await exercise(scoped: true) { f in
            try f.target.write { try f.note().owner = "account-2"; try f.base().namespace = "foreign-namespace" }
            let before = try await f.audit(records: []); XCTAssertTrue(before.isClean, before.issues.joined(separator: ","))
            XCTAssertEqual(before.trackingRecordCount, 0)
            try f.target.beginWrite(); try f.note().owner = "account-1"
            let result = try await f.audit(records: [])
            XCTAssertEqual(result, before)
            XCTAssertEqual(result.trackingRecordCount, 0)
            XCTAssertTrue(f.target.isInWriteTransaction)
        }
    }

    nonisolated func testTargetRefreshReentryStillUsesCommittedState() async throws {
        try await exercise { f in
            let before = try await f.audit()
            f.target.onRefresh = {
                try! f.target.beginWrite()
                f.target.object(ofType: AuditNote.self, forPrimaryKey: "note")!.text = "refresh-owner"
            }
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertTrue(f.target.isInWriteTransaction)
            XCTAssertEqual(try f.note().text, "refresh-owner")
        }
    }

    nonisolated func testTrackingRefreshReentryCannotHideCommittedDebt() async throws {
        try await exercise { f in
            try f.tracking.write { try f.entity().pendingGeneration = "committed-pending" }
            let before = try await f.audit()
            f.tracking.onRefresh = {
                try! f.tracking.beginWrite()
                f.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: f.name)!.pendingGeneration = nil
            }
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertTrue(f.tracking.isInWriteTransaction)
        }
    }

    nonisolated func testLateProvisionalDebtRemovalCannotSplitAuditPasses() async throws {
        try await exercise { f in
            try f.target.write { f.addMutation(); f.addSubmission() }
            let before = try await f.audit()
            f.adapter.beforeComparison = { try! f.target.beginWrite(); try! f.clearTargetDebt() }
            let result = try await f.audit()
            XCTAssertEqual(result, before)
            XCTAssertEqual(result.pendingMutationCount, 1)
            XCTAssertEqual(result.unresolvedSubmissionCount, 1)
            XCTAssertTrue(f.target.isInWriteTransaction)
        }
    }

    nonisolated func testActuallyCommittedOwnerIsVisibleToNextAudit() async throws {
        try await exercise { f in
            let before = try await f.audit(); XCTAssertTrue(before.isClean)
            try f.target.beginWrite(); try f.note().text = "next-commit"; f.addMutation()
            f.target.commitWrite()
            let after = try await f.audit()
            XCTAssertFalse(after.isClean); XCTAssertEqual(after.pendingMutationCount, 1)
            XCTAssertNotEqual(after, before)
        }
    }

    nonisolated func testUnrelatedTransportDebtDoesNotEnterActiveAudit() async throws {
        try await exercise(scoped: true) { f in
            let before = try await f.audit()
            try f.target.write {
                f.addMutation(); f.mutation()?.accountScopeIdentifier = "account-2"
                f.mutation()?.replicaBindingGenerationIdentifier = "binding-2"
            }
            let after = try await f.audit()
            XCTAssertEqual(after, before)
        }
    }

    nonisolated func testMissingTransportNamespaceStillReportsIncompleteEvidence() async throws {
        try await exercise { f in
            f.adapter.recordRebaseContext = nil
            let result = try await f.audit()
            XCTAssertFalse(result.isClean)
            XCTAssertTrue(result.issues.contains("comparison-transport-namespace-unavailable"))
        }
    }

    nonisolated func testWrongZoneAndDuplicateServerRecordsStillFailAudit() async throws {
        try await exercise { f in
            let wrong = CKRecord(recordType: AuditNote.className(), recordID: .init(recordName: f.name, zoneID: .init(zoneName: "other")))
            let result = try await f.audit(records: [f.server, f.server, wrong])
            XCTAssertFalse(result.isClean)
            XCTAssertTrue(result.issues.contains("server-record-wrong-zone:" + f.name))
            XCTAssertTrue(result.issues.contains("duplicate-server-record:" + f.name))
        }
    }

    nonisolated func testSchemaAliasesReuseOneSnapshotPerAudit() async throws {
        try await exercise { f in
            let beforeFreezes = f.target.freezeCount
            let result = try await f.audit()
            XCTAssertTrue(result.isClean)
            XCTAssertEqual(f.target.freezeCount - beforeFreezes, 1,
                "The two schema aliases and later evidence pass must share one captured version")
        }
    }
}
