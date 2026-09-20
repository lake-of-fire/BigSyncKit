import CloudKit
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testInvalidatedFenceCannotCertifyUnjournaledLiveObject() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let values = try BigSyncRecordFingerprint.fields(of: object)
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        // Deliberate evidence corruption only: production disappearance keeps
        // the live recreation's journal in the same target transaction.
        try realm.write {
            BigSyncRecordBaseline.invalidate(recordName: incoming.recordID.recordName, in: realm)
        }
        let revision = baseline.revision
        XCTAssertTrue(baseline.isComparisonInvalidated)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: object), values)
        let issue = "invalidated-comparison-unexplained-live-target:" + incoming.recordID.recordName
        let audit = try await adapter.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertFalse(audit.isClean)
        XCTAssertEqual(audit.invalidatedBaselineCount, 1)
        XCTAssertTrue(audit.issues.contains(issue))
        XCTAssertThrowsError(try adapter.hasPendingChangesAtTerminalBoundary()) { error in
            XCTAssertTrue((error as? BigSyncComparisonEvidenceError)?.issues.contains(issue) == true)
        }
        let blockers = try await adapter.semanticPublicationBlockers()
        XCTAssertTrue(blockers.contains { $0.code == "comparison-evidence-inconsistent" })
        // Restart cannot reinterpret the revision fence as accepted evidence.
        let (restarted, reopened) = try await restart(adapter)
        XCTAssertEqual(reopened.objects(BigSyncRecordBaseline.self).first?.revision, revision)
        XCTAssertThrowsError(try restarted.hasPendingChangesAtTerminalBoundary())
        // A genuine server observation repairs comparison evidence; neither
        // the audit nor the fence is authority to manufacture a local write.
        _ = try await deliver([incoming], to: restarted)
        let recovered = try XCTUnwrap(reopened.objects(W1ContractNote.self).first)
        XCTAssertEqual(try BigSyncRecordFingerprint.fields(of: recovered), values)
        XCTAssertFalse(try XCTUnwrap(reopened.objects(BigSyncRecordBaseline.self).first).isComparisonInvalidated)
        try await quiet(restarted, realm: reopened)
        let cleanAudit = try await restarted.auditSynchronizationState(serverRecords: [incoming])
        XCTAssertTrue(cleanAudit.isClean, cleanAudit.issues.joined(separator: ","))
    }
}
