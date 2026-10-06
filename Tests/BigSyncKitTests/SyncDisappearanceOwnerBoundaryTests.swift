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
}
