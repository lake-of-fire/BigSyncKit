import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

/// Callback injection uses explicit SDK/domain collaborators. These histories
/// execute the full production disappearance file, not native Realm scheduling.
final class DisappearanceOwnerTests: XCTestCase {
    nonisolated func testLegacyTrackingRefreshRevocationRollsBackTargetDisposition() async throws {
        try await exerciseSnapshots(journal: false, state: .changed) { f in
            let before = try f.baseline().revision
            f.tracking.onRefresh = { f.adapter.cancellationGeneration += 1 }
            do { _ = try await f.reconcile(); XCTFail("Revoked target write must reject") }
            catch is CancellationError { }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
            XCTAssertFalse(try f.note().isDeleted)
            XCTAssertNil(f.mutation())
            XCTAssertEqual(try f.entity().entityState, .changed)
        }
    }

    nonisolated func testLegacyTrackingRefreshProviderReplacementRollsBackTarget() async throws {
        try await exerciseSnapshots(journal: false, state: .changed) { f in
            let before = try f.baseline().revision
            f.tracking.onRefresh = {
                f.adapter.realmProvider = RealmProvider(target: f.target, tracking: f.tracking)
            }
            do { _ = try await f.reconcile(); XCTFail("Replacement provider cannot inherit old operation") }
            catch is CancellationError { }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertNil(f.mutation())
            XCTAssertFalse(try f.note().isDeleted)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testLegacyTrackingRefreshContextReplacementRollsBackTarget() async throws {
        try await exerciseSnapshots(journal: false, state: .synced) { f in
            let before = try f.baseline().revision
            f.tracking.onRefresh = { f.adapter.context = .init(namespace: "replacement") }
            do { _ = try await f.reconcile(); XCTFail("Replaced context must reject before target commit") }
            catch is CancellationError { }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertFalse(try f.note().isDeleted)
            XCTAssertNil(f.mutation())
        }
    }

    nonisolated func testCurrentLegacyRefreshStillPreservesLocalWork() async throws {
        try await exerciseSnapshots(journal: false, state: .changed) { f in
            f.tracking.onRefresh = { }
            let outcome = try await f.reconcile()
            guard case .preservedNewerLive(let generation) = outcome else {
                return XCTFail("Committed old tracking intent must remain journaled")
            }
            XCTAssertEqual(f.mutation()?.generation, generation)
            XCTAssertEqual(try f.entity().pendingGeneration, generation)
            XCTAssertFalse(try f.note().isDeleted)
        }
    }

    nonisolated func testPreparationRejectsRevocationInsideIdentityProvider() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            try await DisappearanceCallbacks.$identity.withValue({
                f.adapter.cancellationGeneration += 1
            }) {
                do { _ = try await f.prepare(); XCTFail("Identity callback revoked preparation") }
                catch is CancellationError { }
            }
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testPreparationRejectsRevocationInsideModelContract() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            try await DisappearanceCallbacks.$compile.withValue({
                f.adapter.cancellationGeneration += 1
            }) {
                do { _ = try await f.prepare(); XCTFail("Contract callback revoked preparation") }
                catch is CancellationError { }
            }
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testPreparationRejectsTaskCancellationDuringContractRead() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            let request = Task { @BigSyncBackgroundActor in
                try await DisappearanceCallbacks.$compile.withValue({
                    withUnsafeCurrentTask { $0?.cancel() }
                }) { try await f.prepare() }
            }
            let result = await request.result
            if case .success = result { XCTFail("Cancelled read cannot return transport evidence") }
            if case .failure(let error) = result { XCTAssertTrue(error is CancellationError) }
            XCTAssertTrue(request.isCancelled)
            XCTAssertFalse(Task.isCancelled)
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testCommitRejectsContractRevocationBeforeInvalidation() async throws {
        try await exerciseSnapshots { f in
            let before = try f.baseline().revision
            let cut = try f.adapter.currentRecordEvidenceCut()
            try DisappearanceCallbacks.$compile.withValue({ f.adapter.cancellationGeneration += 1 }) {
                do {
                    try f.target.write {
                        _ = try f.adapter.commitPhysicalDisappearance(recordID: f.record, type: HarnessNote.self,
                            cut: cut, expectedRevision: before, expectedSubmissionIdentity: nil, in: f.target)
                    }
                    XCTFail("Invalidation cannot commit after contract revocation")
                } catch is CancellationError { }
            }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
        }
    }

    nonisolated func testCommitRejectsRevocationInsideWriteIdentityValidation() async throws {
        try await exerciseSnapshots { f in
            let before = try f.baseline().revision
            let cut = try f.adapter.currentRecordEvidenceCut()
            try DisappearanceCallbacks.$identity.withValue({ f.adapter.cancellationGeneration += 1 }) {
                do {
                    try f.target.write {
                        _ = try f.adapter.commitPhysicalDisappearance(recordID: f.record, type: HarnessNote.self,
                            cut: cut, expectedRevision: before, expectedSubmissionIdentity: nil, in: f.target)
                    }
                    XCTFail("A valid returned identity does not undo owner revocation")
                } catch is CancellationError { }
            }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
        }
    }

    nonisolated func testPublicationIdentityRevocationPreservesAlreadyCommittedTarget() async throws {
        try await exerciseSnapshots { f in
            let before = try f.baseline().revision
            try await DisappearanceCallbacks.$identity.withValue({
                if f.tracking.isInWriteTransaction { f.adapter.cancellationGeneration += 1 }
            }) {
                do { _ = try await f.reconcile(); XCTFail("Revoked tracking publication must reject") }
                catch is CancellationError { }
            }
            XCTAssertNotEqual(try f.baseline().revision, before, "Earlier target commit is not rolled back")
            XCTAssertTrue(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testPublicationLifecycleRevocationRollsBackTracking() async throws {
        try await exerciseSnapshots { f in
            try await DisappearanceCallbacks.$lifecycle.withValue({
                if f.tracking.isInWriteTransaction { f.adapter.cancellationGeneration += 1 }
            }) {
                do { _ = try await f.reconcile(); XCTFail("Lifecycle callback revoked tracking owner") }
                catch is CancellationError { }
            }
            XCTAssertTrue(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
        }
    }

    nonisolated func testPublicationCannotAdoptReplacementProviderAfterTargetCommit() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = {
                f.adapter.realmProvider = RealmProvider(target: f.target, tracking: f.tracking)
            }
            do { _ = try await f.reconcile(); XCTFail("Provider replacement must not retarget publication") }
            catch is CancellationError { }
            XCTAssertTrue(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testSkippedAuthorityResultDoesNotHideCallbackRevocation() async throws {
        try await exerciseSnapshots { f in
            try f.target.write { try f.note().account = "other-account" }
            try await DisappearanceCallbacks.$eligibility.withValue({
                f.adapter.cancellationGeneration += 1
            }) {
                do { _ = try await f.reconcile(); XCTFail("Revocation is not a successful ignored disposition") }
                catch is CancellationError { }
            }
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(try f.entity().encodedRecord, Data([1]))
        }
    }

    nonisolated func testJournalCallbackRevocationCannotCommitLegacyRecovery() async throws {
        try await exerciseSnapshots(journal: false, state: .changed) { f in
            let before = try f.baseline().revision
            try await DisappearanceCallbacks.$journal.withValue({
                f.adapter.cancellationGeneration += 1
            }) {
                do { _ = try await f.reconcile(); XCTFail("Revoked legacy journal callback must roll back") }
                catch is CancellationError { }
            }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertNil(f.mutation())
            XCTAssertEqual(try f.entity().entityState, .changed)
        }
    }

    nonisolated func testValidPreparationWithCallbacksStillReturnsExactCut() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            let cut = try f.adapter.currentRecordEvidenceCut()
            let result = try await DisappearanceCallbacks.$compile.withValue({ }) { try await f.prepare() }
            XCTAssertEqual(result?.cut.cancellationGeneration, cut.cancellationGeneration)
            XCTAssertEqual(result?.cut.context, cut.context)
            XCTAssertEqual(result?.revision, try f.baseline().revision)
            XCTAssertEqual(result?.recordID, f.record)
        }
    }

    nonisolated func testRejectedPublicationCanRetryWithoutReauthoringMutation() async throws {
        try await exerciseSnapshots { f in
            f.adapter._testAfterDisappearanceTargetWrite = { f.adapter.cancellationGeneration += 1 }
            do { _ = try await f.reconcile(); XCTFail("Old publication must reject") }
            catch is CancellationError { }
            let revision = try f.baseline().revision
            let generation = f.mutation()?.generation
            f.adapter._testAfterDisappearanceTargetWrite = nil
            let result = try await f.reconcile()
            guard case .preservedNewerLive(let returned) = result else { return XCTFail("Retry must converge") }
            XCTAssertEqual(returned, generation)
            XCTAssertEqual(f.mutation()?.generation, generation)
            XCTAssertEqual(try f.baseline().revision, revision)
            XCTAssertEqual(try f.entity().pendingGeneration, generation)
        }
    }
}

extension DisappearanceOwnerTests {
    nonisolated func testPublicationResamplesSuccessorCommittedByIdentityProvider() async throws {
        try await exerciseSnapshots { f in
            try await DisappearanceCallbacks.$identity.withValue({
                guard f.tracking.isInWriteTransaction else { return }
                do {
                    try f.target.write {
                        f.target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: f.name)?.revision = "callback-successor"
                        let row = BigSyncPendingMutation(); row.recordName = f.name; row.generation = "generation-2"; f.target.add(row)
                        f.target.object(ofType: HarnessNote.self, forPrimaryKey: "note")?.text = "successor committed during validation"
                    }
                    let row = try XCTUnwrap(f.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: f.name))
                    row.pendingGeneration = "generation-2"
                    row.entityState = .changed
                    row.encodedRecord = Data([2])
                } catch { XCTFail("Unexpected fixture failure: \(error)") }
            }) { _ = try await f.reconcile() }
            XCTAssertEqual(try f.baseline().revision, "callback-successor")
            XCTAssertEqual(f.mutation()?.generation, "generation-2")
            XCTAssertEqual(try f.entity().pendingGeneration, "generation-2")
            XCTAssertEqual(try f.entity().entityState, .changed)
            XCTAssertEqual(try f.entity().encodedRecord, Data([2]))
        }
    }

    nonisolated func testPreparationResamplesGenerationCommittedByIdentityProvider() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            let result = try await DisappearanceCallbacks.$identity.withValue({
                do { try f.target.write { let row = BigSyncPendingMutation(); row.recordName = f.name; row.generation = "generation-2"; f.target.add(row) } }
                catch { XCTFail("Unexpected fixture failure: \(error)") }
            }) { try await f.prepare() }
            XCTAssertNil(result, "Generation-1 no longer owns the deletion preparation")
            XCTAssertEqual(f.mutation()?.generation, "generation-2")
        }
    }
}

extension DisappearanceOwnerTests {
    @BigSyncBackgroundActor
    private static func missingServerUploads(_ f: Fixture) throws -> [PreparedRecordUpload] {
        var uploads = [PreparedRecordUpload]()
        try f.target.write {
            for suffix in ["note", "second"] {
                let name = HarnessNote.className() + "." + suffix
                if suffix != "note" {
                    let object = HarnessNote(); object.id = suffix; f.target.add(object)
                    let baseline = BigSyncRecordBaseline(); baseline.recordName = name; f.target.add(baseline)
                    let mutation = BigSyncPendingMutation(); mutation.recordName = name; f.target.add(mutation)
                }
                let submission = BigSyncRecordSubmission()
                submission.id = BigSyncRecordPayload.identity([f.adapter.context.namespace, name])
                submission.recordName = name; submission.payload = Data(name.utf8)
                f.target.add(submission)
                let record = CKRecord(recordType: HarnessNote.className(), recordID: .init(recordName: name, zoneID: f.adapter.recordZoneID))
                record.recordChangeTag = "accepted-tag"
                uploads.append(.init(record: record, generation: "generation-1", comparisonBase: .init(
                    context: f.adapter.context, revision: "revision-1", submissionIdentity: "submission-1",
                    schemaSignature: "signature-1", fields: [:])))
            }
        }
        try f.tracking.write {
            let row = SyncedEntity(entityType: HarnessNote.className(), identifier: uploads[1].record.recordID.recordName,
                                   state: SyncedEntityState.changed.rawValue)
            row.setPendingMutation(generation: "generation-1", replicaBindingGenerationIdentifier: "binding-1")
            f.tracking.add(row)
        }
        return uploads
    }

    nonisolated func testMissingServerBatchCannotAdoptResumedAttemptBetweenRecords() async throws {
        try await exerciseSnapshots { f in
            let uploads = try Self.missingServerUploads(f)
            let secondName = uploads[1].record.recordID.recordName
            try await DisappearanceCallbacks.$afterWrite.withValue({ realm in
                if realm === f.tracking { f.adapter.cancellationGeneration += 1 }
            }) {
                do {
                    try await f.adapter.requeueMissingServerRecords(uploads.map { $0.record.recordID }, matchingPreparedUploads: uploads)
                    XCTFail("The same response must not adopt the next attempt's generation")
                } catch is CancellationError { }
            }
            XCTAssertTrue(try f.baseline().isComparisonInvalidated, "First target phase remains durable")
            XCTAssertEqual(try f.entity().entityState, .new, "First tracking phase completed before replacement")
            XCTAssertEqual(f.target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: secondName)?.isComparisonInvalidated, false)
            XCTAssertEqual(f.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.encodedRecord, Data([1]))
            XCTAssertNotNil(f.target.object(ofType: BigSyncRecordSubmission.self,
                forPrimaryKey: BigSyncRecordPayload.identity([f.adapter.context.namespace, secondName])))
            // A separately authorized invocation can converge the unhandled
            // second record without repeating or relabeling the first phase.
            try await f.adapter.requeueMissingServerRecords([uploads[1].record.recordID], matchingPreparedUploads: [uploads[1]])
            XCTAssertEqual(f.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.entityState, .new)
            XCTAssertEqual(f.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: secondName)?.generation, "generation-1")
        }
    }

    nonisolated func testCurrentMissingServerBatchStillCompletesBothRecords() async throws {
        try await exerciseSnapshots { f in
            let uploads = try Self.missingServerUploads(f)
            try await f.adapter.requeueMissingServerRecords(uploads.map { $0.record.recordID }, matchingPreparedUploads: uploads)
            for upload in uploads {
                let name = upload.record.recordID.recordName
                XCTAssertEqual(f.target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.isComparisonInvalidated, true)
                XCTAssertEqual(f.tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)?.entityState, .new)
                XCTAssertEqual(f.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation, "generation-1")
            }
        }
    }
}

private final class DisappearanceCallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var stored = 0
    func increment() { lock.lock(); defer { lock.unlock() }; stored += 1 }
    var value: Int { lock.lock(); defer { lock.unlock() }; return stored }
}

extension DisappearanceOwnerTests {
    private enum ModelRejection: Error { case original }

    nonisolated func testRevokedIdentityDoesNotEnterFurtherModelCallbacks() async throws {
        try await exerciseSnapshots(deleted: true, state: .deletedLocally) { f in
            let calls = DisappearanceCallCounter()
            try await DisappearanceCallbacks.$compile.withValue({ calls.increment() }) {
                try await DisappearanceCallbacks.$identity.withValue({ f.adapter.cancellationGeneration += 1 }) {
                    do { _ = try await f.prepare(); XCTFail("Revoked identity must stop admission") }
                    catch is CancellationError { }
                }
            }
            XCTAssertEqual(calls.value, 0, "Do not run model code after identity validation revokes the owner")
        }
    }

    nonisolated func testOriginalModelErrorIsPreservedAndTargetRollsBack() async throws {
        try await exerciseSnapshots { f in
            let before = try f.baseline().revision
            let cut = try f.adapter.currentRecordEvidenceCut()
            try DisappearanceCallbacks.$compile.withValue({
                f.adapter.cancellationGeneration += 1
                throw ModelRejection.original
            }) {
                do {
                    try f.target.write {
                        _ = try f.adapter.commitPhysicalDisappearance(recordID: f.record, type: HarnessNote.self,
                            cut: cut, expectedRevision: before, expectedSubmissionIdentity: nil, in: f.target)
                    }
                    XCTFail("Original model rejection must propagate")
                } catch ModelRejection.original { }
            }
            XCTAssertEqual(try f.baseline().revision, before)
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
        }
    }

    nonisolated func testUnboundLegacyMissingServerRouteDoesNotRequireComparisonAuthority() async throws {
        try await exerciseSnapshots { f in
            f.adapter.comparisonEnabled = false
            let record = CKRecord(recordType: HarnessLegacyNote.className(),
                recordID: .init(recordName: HarnessLegacyNote.className() + ".legacy", zoneID: f.adapter.recordZoneID))
            let upload = PreparedRecordUpload(record: record, generation: "legacy-generation", comparisonBase: nil)
            try await f.adapter.requeueMissingServerRecords([record.recordID], matchingPreparedUploads: [upload])
            XCTAssertEqual(f.adapter.legacyRequeues.count, 1)
            XCTAssertEqual(f.adapter.legacyRequeues.first?.0, [record.recordID])
            XCTAssertEqual(f.adapter.legacyRequeues.first?.1, [record.recordID.recordName: "legacy-generation"])
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
        }
    }
}

extension DisappearanceOwnerTests {
    nonisolated func testEmptyMissingResponsePreservesExistingNoOpDuringCancellation() async throws {
        try await exerciseSnapshots { f in
            f.adapter.cancelSync = true
            try await f.adapter.requeueMissingServerRecords([], matchingPreparedUploads: [])
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
            XCTAssertEqual(f.mutation()?.generation, "generation-1")
            XCTAssertTrue(f.adapter.legacyRequeues.isEmpty)
        }
    }

    nonisolated func testEmptyMissingResponseStillValidatesPreparedIdentity() async throws {
        try await exerciseSnapshots { f in
            let record = CKRecord(recordType: HarnessNote.className(), recordID: f.record)
            let prepared = PreparedRecordUpload(record: record, generation: "generation-1", comparisonBase: nil)
            do {
                try await f.adapter.requeueMissingServerRecords([], matchingPreparedUploads: [prepared, prepared])
                XCTFail("Empty results must not bypass complete prepared-input validation")
            } catch BigSyncRecordRebaseError.inconsistentReceipt(let name) {
                XCTAssertEqual(name, f.name)
            }
            XCTAssertFalse(try f.baseline().isComparisonInvalidated)
        }
    }
}
