import CloudKit
import Dispatch
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

private final class DisappearanceRefreshObservation: @unchecked Sendable {
    private let lock = NSLock()
    private var armed = false
    private var observed = false

    func arm() {
        lock.lock(); defer { lock.unlock() }
        armed = true
    }

    func receive() -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard armed, !observed else { return false }
        observed = true
        return true
    }

    var didObserve: Bool {
        lock.lock(); defer { lock.unlock() }
        return observed
    }
}

private enum DisappearanceIdentityObservation {
    @TaskLocal static var isReading = false
    @TaskLocal static var cancelCurrentRead = false
}

extension SyncUndoCloseoutW1Tests {
    private enum DisappearanceRefreshMode: Sendable, Equatable {
        case current, cancellationGeneration, account, task
    }

    /// Only the existing file-backed fixture's tracking metadata is edited by
    /// the second scheduler. The observed callback must occur inside the real
    /// target transaction, not during setup or a later tracking publication.
    @BigSyncBackgroundActor
    private func exerciseDisappearanceRefreshOwner(
        _ mode: DisappearanceRefreshMode, retry: Bool = false
    ) async throws {
        let (adapter, target, object, incoming) = try await acceptedNote()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let name = incoming.recordID.recordName
        let tracked = try XCTUnwrap(tracking.object(
            ofType: SyncedEntity.self, forPrimaryKey: name))
        XCTAssertNil(target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
        try tracking.write {
            tracked.entityState = .changed
            tracked.setPendingMutation(generation: "legacy-disappearance-work",
                replicaBindingGenerationIdentifier: "w1-binding")
        }
        let originalRevision = try XCTUnwrap(target.object(
            ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)).revision
        let originalText = object.text
        let originalModifiedAt = object.modifiedAt
        let originalExplicitlyModifiedAt = object.explicitlyModifiedAt
        let originalEncodedRecord = tracked.encodedRecord
        let signal = DisappearanceRefreshObservation()
        let priorAutorefresh = tracking.autorefresh
        tracking.autorefresh = false
        let writerQueue = DispatchQueue(label: "test.disappearance-tracking-refresh." + UUID().uuidString)
        let configuration = tracking.configuration
        let observation = tracking.observe { notification, _ in
            guard case .didChange = notification, signal.receive() else { return }
            XCTAssertTrue(target.isInWriteTransaction,
                "Revocation must happen during target mutation, not after its commit")
            switch mode {
            case .current:
                break
            case .cancellationGeneration:
                adapter.cancelSynchronization()
                do { try adapter.prepareForFencedMigrationAfterCancellation() }
                catch { XCTFail("Could not resume the cancellation Boolean: \(error)") }
            case .account:
                adapter.activeAccountScopeIdentifier = "replacement-account"
            case .task:
                withUnsafeCurrentTask { $0?.cancel() }
            }
        }
        adapter._testBeforeRemoteDeletionTargetWrite = {
            signal.arm()
            try writerQueue.sync {
                let writer = try Realm(configuration: configuration, queue: writerQueue)
                try writer.write {
                    let row = try XCTUnwrap(writer.object(ofType: SyncedEntity.self, forPrimaryKey: name))
                    // Preserve the legacy dirty state while requiring a real
                    // refresh notification when its committed version is read.
                    row.setPendingMutation(generation: "legacy-refresh-signal",
                        replicaBindingGenerationIdentifier: "w1-binding")
                }
            }
        }
        defer {
            adapter._testBeforeRemoteDeletionTargetWrite = nil
            observation.invalidate()
            tracking.autorefresh = priorAutorefresh
        }
        let request = Task { @BigSyncBackgroundActor in
            try await adapter.reconcilePhysicalDeletion(
                recordID: incoming.recordID, type: W1ContractNote.self, in: target)
        }
        addTeardownBlock { request.cancel(); _ = await request.result }
        let outcome = await request.result
        XCTAssertTrue(signal.didObserve, "A real tracking Realm refresh must exercise the boundary")
        XCTAssertEqual(request.isCancelled, mode == .task)
        if mode == .current {
            let disposition = try outcome.get()
            guard case let .preservedNewerLive(generation) = disposition else {
                return XCTFail("Committed legacy intent must remain pending")
            }
            XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
            XCTAssertEqual(tracked.pendingGeneration, generation)
            XCTAssertTrue(try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: name)).isComparisonInvalidated)
        } else {
            switch outcome {
            case .success: XCTFail("A revoked target mutation must not commit")
            case .failure(let error): XCTAssertTrue(error is CancellationError)
            }
            let baseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: name))
            XCTAssertEqual(baseline.revision, originalRevision)
            XCTAssertFalse(baseline.isComparisonInvalidated)
            XCTAssertNil(target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
            XCTAssertEqual(tracked.entityState, .changed)
            XCTAssertEqual(tracked.pendingGeneration, "legacy-refresh-signal")
            XCTAssertEqual(tracked.encodedRecord, originalEncodedRecord)
            if retry {
                observation.invalidate()
                adapter._testBeforeRemoteDeletionTargetWrite = nil
                adapter.activeAccountScopeIdentifier = "w1-account"
                try await adapter.unsetCancellation()
                let disposition = try await adapter.reconcilePhysicalDeletion(
                    recordID: incoming.recordID, type: W1ContractNote.self, in: target)
                guard case let .preservedNewerLive(generation) = disposition else {
                    return XCTFail("A fresh authorized invocation must converge")
                }
                XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: name)?.generation, generation)
                XCTAssertEqual(tracked.pendingGeneration, generation)
            }
        }
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(object.text, originalText)
        XCTAssertEqual(object.modifiedAt, originalModifiedAt)
        XCTAssertEqual(object.explicitlyModifiedAt, originalExplicitlyModifiedAt)
    }

    @BigSyncBackgroundActor
    func testDisappearanceTrackingRefreshGenerationABARejectsBeforeTargetCommit() async throws {
        try await exerciseDisappearanceRefreshOwner(.cancellationGeneration)
    }

    @BigSyncBackgroundActor
    func testDisappearanceTrackingRefreshAccountReplacementRollsBackTarget() async throws {
        try await exerciseDisappearanceRefreshOwner(.account)
    }

    @BigSyncBackgroundActor
    func testDisappearanceTrackingRefreshTaskCancellationPreservesTarget() async throws {
        try await exerciseDisappearanceRefreshOwner(.task)
    }

    @BigSyncBackgroundActor
    func testCurrentDisappearanceTrackingRefreshPreservesLegacyIntent() async throws {
        try await exerciseDisappearanceRefreshOwner(.current)
    }

    @BigSyncBackgroundActor
    func testDisappearanceRefreshRejectionCanRetryWithoutReauthoringContent() async throws {
        try await exerciseDisappearanceRefreshOwner(.cancellationGeneration, retry: true)
    }

    @BigSyncBackgroundActor
    private func exerciseDisappearancePreparationIdentity(_ cancelRead: Bool) async throws {
        let (adapter, target, object, incoming) = try await acceptedNote()
        try target.write {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let name = incoming.recordID.recordName
        let generation = try XCTUnwrap(target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: name)?.generation)
        let revision = target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision
        let signal = DisappearanceRefreshObservation()
        signal.arm()
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target.configuration],
            mutationJournalIdentityProvider: {
                if DisappearanceIdentityObservation.isReading, signal.receive(),
                   DisappearanceIdentityObservation.cancelCurrentRead {
                    withUnsafeCurrentTask { $0?.cancel() }
                }
                return .init(installationIdentifier: "w1-local", replicaBindingGenerationIdentifier: "w1-binding")
            })
        defer {
            BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target.configuration],
                mutationJournalIdentityProvider: {
                    .init(installationIdentifier: "w1-local", replicaBindingGenerationIdentifier: "w1-binding")
                })
        }
        let request = Task { @BigSyncBackgroundActor in
            try await DisappearanceIdentityObservation.$isReading.withValue(true) {
                try await DisappearanceIdentityObservation.$cancelCurrentRead.withValue(cancelRead) {
                    try await adapter.preparePhysicalDeletionEvidence(recordID: incoming.recordID,
                        type: W1ContractNote.self, generation: generation, in: target)
                }
            }
        }
        addTeardownBlock { request.cancel(); _ = await request.result }
        let result = await request.result
        XCTAssertTrue(signal.didObserve)
        XCTAssertEqual(request.isCancelled, cancelRead)
        if cancelRead {
            switch result {
            case .success: XCTFail("Cancelled read must not return transport deletion evidence")
            case .failure(let error): XCTAssertTrue(error is CancellationError)
            }
        } else {
            let evidence = try XCTUnwrap(try result.get())
            XCTAssertEqual(evidence.recordID, incoming.recordID)
            XCTAssertEqual(evidence.revision, revision)
        }
        XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation, generation)
        XCTAssertEqual(target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)?.revision, revision)
        XCTAssertTrue(object.isDeleted)
    }

    @BigSyncBackgroundActor
    func testDisappearancePreparationRejectsIdentityProviderCancellation() async throws {
        try await exerciseDisappearancePreparationIdentity(true)
    }

    @BigSyncBackgroundActor
    func testCurrentDisappearancePreparationRetainsExactEvidence() async throws {
        try await exerciseDisappearancePreparationIdentity(false)
    }

    @BigSyncBackgroundActor
    private func twoAcceptedNotes() async throws -> (
        adapter: RealmSwiftAdapter, target: Realm, notes: [W1ContractNote], records: [CKRecord]
    ) {
        let (adapter, target) = try await fixture()
        let first = try tagged(note(adapter), "accepted-first")
        let secondID = UUID(uuidString: "A0000000-0000-0000-0000-000000000002")!
        let second = CKRecord(recordType: W1ContractNote.className(), recordID: .init(
            recordName: W1ContractNote.className() + "." + secondID.uuidString,
            zoneID: adapter.recordZoneID))
        second["text"] = "server-second" as CKRecordValue
        second["number"] = 10 as CKRecordValue
        second["flag"] = false as CKRecordValue
        second["isDeleted"] = false as CKRecordValue
        second["createdAt"] = Date(timeIntervalSinceReferenceDate: 2) as CKRecordValue
        second["modifiedAt"] = Date(timeIntervalSinceReferenceDate: 20) as CKRecordValue
        second["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: 20) as CKRecordValue
        let records = [first, try tagged(second, "accepted-second")]
        _ = try await deliver(records, to: adapter)
        let notes = [
            try XCTUnwrap(target.object(ofType: W1ContractNote.self, forPrimaryKey: noteID)),
            try XCTUnwrap(target.object(ofType: W1ContractNote.self, forPrimaryKey: secondID)),
        ]
        XCTAssertEqual(notes.count, 2)
        return (adapter, target, notes, records)
    }

    @BigSyncBackgroundActor
    private func assertTwoPreparedUploadReceipts(
        _ prepared: [PreparedRecordUpload], records: [CKRecord], target: Realm
    ) throws -> [String: String] {
        XCTAssertEqual(prepared.count, 2, "The batch must carry exactly both CloudKit input receipts")
        XCTAssertEqual(Set(prepared.map(\.record.recordID)), Set(records.map(\.recordID)))
        var generations = [String: String]()
        for receipt in prepared {
            let name = receipt.record.recordID.recordName
            XCTAssertNotNil(receipt.comparisonBase, "Each server-backed record must retain its prepared proof")
            XCTAssertEqual(receipt.generation, target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation)
            generations[name] = try XCTUnwrap(receipt.generation)
        }
        return generations
    }

    @BigSyncBackgroundActor
    private func assertRequeueFirstCompletedSecondUntouched(
        adapter: RealmSwiftAdapter, target: Realm, records: [CKRecord], generations: [String: String],
        secondRevision: String, secondSubmission: String?, secondTrackingRecord: Data?
    ) throws {
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let first = records[0].recordID.recordName
        let second = records[1].recordID.recordName
        let firstBaseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: first))
        XCTAssertTrue(firstBaseline.isComparisonInvalidated)
        XCTAssertNil(target.object(ofType: BigSyncRecordSubmission.self,
            forPrimaryKey: BigSyncRecordPayload.identity([try XCTUnwrap(adapter.recordRebaseContext).namespace, first])))
        XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: first)?.generation,
            generations[first])
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: first)?.entityState, .new)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: first)?.pendingGeneration,
            generations[first])
        XCTAssertNil(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: first)?.encodedRecord)

        let secondBaseline = try XCTUnwrap(target.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: second))
        XCTAssertEqual(secondBaseline.revision, secondRevision)
        XCTAssertFalse(secondBaseline.isComparisonInvalidated)
        XCTAssertEqual(target.object(ofType: BigSyncRecordSubmission.self,
            forPrimaryKey: BigSyncRecordPayload.identity([try XCTUnwrap(adapter.recordRebaseContext).namespace, second]))?.candidateIdentity,
            secondSubmission)
        XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: second)?.generation,
            generations[second])
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: second)?.entityState, .changed)
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: second)?.pendingGeneration,
            generations[second])
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: second)?.encodedRecord,
            secondTrackingRecord)
    }

    @BigSyncBackgroundActor
    private func exerciseTwoRecordMissingServerOwner(
        revokeAfterFirstTrackingWrite: Bool
    ) async throws {
        let setup = try await twoAcceptedNotes()
        let firstGeneration = try await edit(setup.notes[0], text: "first local", time: 30,
            realm: setup.target, adapter: setup.adapter)
        _ = try await edit(setup.notes[1], text: "second local", time: 40,
            realm: setup.target, adapter: setup.adapter)
        let secondGeneration = try XCTUnwrap(setup.target.object(ofType: BigSyncPendingMutation.self,
            forPrimaryKey: setup.records[1].recordID.recordName)?.generation)
        let prepared = try await setup.adapter.preparedRecordsToUpload(limit: 50,
            restrictedToEntityType: nil)
        let generations = try assertTwoPreparedUploadReceipts(prepared, records: setup.records,
            target: setup.target)
        XCTAssertEqual(generations[setup.records[0].recordID.recordName], firstGeneration)
        XCTAssertEqual(generations[setup.records[1].recordID.recordName], secondGeneration)
        let secondName = setup.records[1].recordID.recordName
        let secondRevision = try XCTUnwrap(setup.target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: secondName)?.revision)
        let secondSubmission = setup.target.object(ofType: BigSyncRecordSubmission.self,
            forPrimaryKey: BigSyncRecordPayload.identity([try XCTUnwrap(setup.adapter.recordRebaseContext).namespace,
                secondName]))?.candidateIdentity
        let secondTrackingRecord = setup.adapter.realmProvider?.persistenceRealm?
            .object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.encodedRecord
        let firstTrackingCompletion = DisappearanceRefreshObservation()
        firstTrackingCompletion.arm()
        if revokeAfterFirstTrackingWrite {
            setup.adapter._testAfterDisappearanceTrackingWrite = {
                guard firstTrackingCompletion.receive() else { return }
                setup.adapter.cancelSynchronization()
                try setup.adapter.prepareForFencedMigrationAfterCancellation()
            }
        }
        defer { setup.adapter._testAfterDisappearanceTrackingWrite = nil }
        if revokeAfterFirstTrackingWrite {
            do {
                try await setup.adapter.requeueMissingServerRecords(setup.records.map(\.recordID),
                    matchingPreparedUploads: prepared)
                XCTFail("The resumed cancellation Boolean must not authorize the second receipt")
            } catch is CancellationError { }
            XCTAssertTrue(firstTrackingCompletion.didObserve,
                "The original first target and tracking phases must commit before revocation")
            try assertRequeueFirstCompletedSecondUntouched(adapter: setup.adapter, target: setup.target,
                records: setup.records, generations: generations, secondRevision: secondRevision,
                secondSubmission: secondSubmission, secondTrackingRecord: secondTrackingRecord)
        } else {
            try await setup.adapter.requeueMissingServerRecords(setup.records.map(\.recordID),
                matchingPreparedUploads: prepared)
            for record in setup.records {
                let name = record.recordID.recordName
                XCTAssertTrue(setup.target.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: name)?.isComparisonInvalidated == true)
                XCTAssertNil(setup.target.object(ofType: BigSyncRecordSubmission.self,
                    forPrimaryKey: BigSyncRecordPayload.identity([try XCTUnwrap(setup.adapter.recordRebaseContext).namespace,
                        name])))
                XCTAssertEqual(setup.adapter.realmProvider?.persistenceRealm?
                    .object(ofType: SyncedEntity.self, forPrimaryKey: name)?.pendingGeneration, generations[name])
            }
        }
    }

    @BigSyncBackgroundActor
    func testTwoRecordMissingServerCancellationGenerationABAStopsAfterCompletedFirstTrackingPhase() async throws {
        try await exerciseTwoRecordMissingServerOwner(revokeAfterFirstTrackingWrite: true)
    }

    @BigSyncBackgroundActor
    func testTwoRecordMissingServerCurrentOwnerCompletesBothTrackingPhases() async throws {
        try await exerciseTwoRecordMissingServerOwner(revokeAfterFirstTrackingWrite: false)
    }

    private enum DisappearanceBatchOwnerReplacement: Equatable {
        case current, account, provider
    }

    @BigSyncBackgroundActor
    private func exerciseTwoRecordDidDeleteOwner(
        ownerReplacement: DisappearanceBatchOwnerReplacement
    ) async throws {
        let setup = try await twoAcceptedNotes()
        let originalTracking = try XCTUnwrap(setup.adapter.realmProvider?.persistenceRealm)
        let replacementAdapter: RealmSwiftAdapter?
        if ownerReplacement == .provider {
            replacementAdapter = try await fixture().0
        } else {
            replacementAdapter = nil
        }
        let replacementProvider = replacementAdapter?.realmProvider
        try setup.target.write {
            setup.notes[0].isDeleted = true
            setup.notes[0].refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 30))
            setup.notes[1].isDeleted = true
            setup.notes[1].refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSinceReferenceDate: 40))
        }
        try await setup.adapter.didFinishImport()
        let prepared = try await setup.adapter.preparedRecordDeletions(limit: 50,
            restrictedToEntityType: nil)
        XCTAssertEqual(prepared.count, 2, "The deletion acknowledgement must retain exactly two prepared receipts")
        XCTAssertEqual(Set(prepared.map(\.recordID)), Set(setup.records.map(\.recordID)))
        for receipt in prepared {
            XCTAssertNotNil(receipt.evidence)
            XCTAssertNotNil(receipt.generation)
        }
        let secondName = setup.records[1].recordID.recordName
        let secondGeneration = try XCTUnwrap(prepared.first { $0.recordID == setup.records[1].recordID }?.generation)
        let secondRevision = try XCTUnwrap(setup.target.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: secondName)?.revision)
        let secondSubmission = setup.target.objects(BigSyncRecordSubmission.self).first {
            $0.recordName == secondName
        }?.candidateIdentity
        let secondTrackingRecord = originalTracking
            .object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.encodedRecord
        let firstTrackingCompletion = DisappearanceRefreshObservation()
        firstTrackingCompletion.arm()
        if ownerReplacement != .current {
            setup.adapter._testAfterDisappearanceTrackingWrite = {
                guard firstTrackingCompletion.receive() else { return }
                switch ownerReplacement {
                case .current:
                    XCTFail("Current-owner control must not install a revocation callback")
                case .account:
                    try await setup.adapter.activateReplicaBinding(accountScopeIdentifier: "replacement-account",
                        replicaBindingGenerationIdentifier: "replacement-binding")
                case .provider:
                    setup.adapter.realmProvider = try XCTUnwrap(replacementProvider)
                }
            }
        }
        defer { setup.adapter._testAfterDisappearanceTrackingWrite = nil }
        if ownerReplacement != .current {
            do {
                try await setup.adapter.didDelete(recordIDs: setup.records.map(\.recordID),
                    matchingPreparedDeletions: prepared)
                XCTFail("A replacement owner must not authorize the second deletion receipt")
            } catch is CancellationError { }
            XCTAssertTrue(firstTrackingCompletion.didObserve,
                "The original first target and tracking phases must commit before owner replacement")
            let firstName = setup.records[0].recordID.recordName
            XCTAssertTrue(setup.target.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: firstName)?.isComparisonInvalidated == true)
            XCTAssertNil(setup.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: firstName))
            XCTAssertNil(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: firstName)?.pendingGeneration)
            XCTAssertNil(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: firstName)?.encodedRecord)
            XCTAssertEqual(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: firstName)?.entityState, .deletedRemotely)

            XCTAssertEqual(setup.target.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: secondName)?.revision, secondRevision)
            XCTAssertFalse(setup.target.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: secondName)?.isComparisonInvalidated ?? true)
            XCTAssertEqual(setup.target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: secondName)?.generation, secondGeneration)
            XCTAssertEqual(setup.target.objects(BigSyncRecordSubmission.self).first {
                $0.recordName == secondName
            }?.candidateIdentity, secondSubmission)
            XCTAssertEqual(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.entityState, .deletedLocally)
            XCTAssertEqual(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.pendingGeneration, secondGeneration)
            XCTAssertEqual(originalTracking
                .object(ofType: SyncedEntity.self, forPrimaryKey: secondName)?.encodedRecord, secondTrackingRecord)
            if let replacementAdapter, let replacementProvider {
                XCTAssertTrue(setup.adapter.realmProvider === replacementProvider)
                let replacementTarget = try XCTUnwrap(replacementProvider.targetReaderRealms?.first)
                let replacementTracking = try XCTUnwrap(replacementProvider.persistenceRealm)
                XCTAssertTrue(replacementTarget.objects(BigSyncRecordBaseline.self).isEmpty)
                XCTAssertTrue(replacementTarget.objects(BigSyncPendingMutation.self).isEmpty)
                XCTAssertTrue(replacementTarget.objects(W1ContractNote.self).isEmpty)
                XCTAssertTrue(replacementTracking.objects(SyncedEntity.self).isEmpty,
                    "The old response cannot publish either record into the replacement provider")
                XCTAssertNotNil(replacementAdapter.realmProvider)
            }
        } else {
            try await setup.adapter.didDelete(recordIDs: setup.records.map(\.recordID),
                matchingPreparedDeletions: prepared)
            for record in setup.records {
                let name = record.recordID.recordName
                XCTAssertTrue(setup.target.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: name)?.isComparisonInvalidated == true)
                XCTAssertNil(setup.target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name))
                XCTAssertEqual(originalTracking
                    .object(ofType: SyncedEntity.self, forPrimaryKey: name)?.entityState, .deletedRemotely)
            }
        }
    }

    @BigSyncBackgroundActor
    func testTwoRecordDidDeleteAccountReplacementStopsAfterCompletedFirstTrackingPhase() async throws {
        try await exerciseTwoRecordDidDeleteOwner(ownerReplacement: .account)
    }

    @BigSyncBackgroundActor
    func testTwoRecordDidDeleteProviderReplacementCannotPublishIntoReplacementTracking() async throws {
        try await exerciseTwoRecordDidDeleteOwner(ownerReplacement: .provider)
    }

    @BigSyncBackgroundActor
    func testTwoRecordDidDeleteCurrentOwnerCompletesBothTrackingPhases() async throws {
        try await exerciseTwoRecordDidDeleteOwner(ownerReplacement: .current)
    }
}
