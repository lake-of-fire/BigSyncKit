import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

/// Real synchronizer drains with injected transport/account providers. These
/// tests do not access a CloudKit account, manufacture journals, or arm a cutoff.
final class CloudKitTerminalReceiptTests: XCTestCase {
    @BigSyncBackgroundActor
    func testOrdinaryReceiptCanBeRevalidatedRepeatedlyWithoutCloudMutations() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        let operations = fixture.transport.operations
        let writes = fixture.store.writes
        for _ in 0..<3 {
            try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
            try fixture.synchronizer.validateTerminalReceipt(receipt)
        }
        XCTAssertEqual(fixture.transport.operations, operations)
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertFalse(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testNextCompletedDrainInvalidatesPreviousReceipt() async throws {
        let fixture = Fixture()
        let first = try await fixture.drain()
        let second = try await fixture.drain()
        XCTAssertNotEqual(first.runID, second.runID)
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
        try await fixture.synchronizer.revalidateTerminalReceipt(second)
    }

    @BigSyncBackgroundActor
    func testOtherSynchronizerCannotValidateReceipt() async throws {
        let first = Fixture()
        let second = Fixture()
        let receipt = try await first.drain()
        _ = try await second.drain()
        await assertRejected { try await second.synchronizer.revalidateTerminalReceipt(receipt) }
        try await first.synchronizer.revalidateTerminalReceipt(first)
    }

    @BigSyncBackgroundActor
    func testPendingEditAfterDrainIsRejectedWithoutAcknowledgement() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        fixture.adapter.hasPendingTerminalChanges = true
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testChangedOrMissingConsumedBoundaryRejectsReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        for boundary: String? in ["new-boundary", nil] {
            fixture.adapter.boundary = boundary
            await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testFinalSynchronousCheckRejectsChangesAfterAccountValidation() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
        fixture.adapter.hasPendingTerminalChanges = true
        XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testUnavailableDurabilityRejectsPreviouslyCompletedReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        fixture.store.durable = false
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
    }

    @BigSyncBackgroundActor
    func testAccountReplacementDuringValidationRejectsOriginalReceipt() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        await fixture.account.replace("replacement-account")
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testNewRunDuringAccountAwaitCannotReturnOldReceiptAsCurrent() async throws {
        let fixture = Fixture()
        let first = try await fixture.drain()
        await fixture.account.onNextRead {
            _ = try await fixture.synchronizer.synchronize()
        }
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
        let current = try await fixture.drain()
        try await fixture.synchronizer.revalidateTerminalReceipt(current)
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementDuringValidationRejectsCompletion() async throws {
        let first = Fixture()
        let second = Fixture()
        let worker = BigSyncBackgroundActor()
        let receipt = try await first.drain()
        await worker._test_installSynchronizer(first.synchronizer)
        await first.account.onNextRead {
            await worker._test_installSynchronizer(second.synchronizer)
        }
        await assertRejected { try await worker.revalidateTerminalReceipt(receipt) }
    }

    @BigSyncBackgroundActor
    func testCancellationRejectsReceiptWithoutStartingAnotherDrain() async throws {
        let fixture = Fixture()
        let receipt = try await fixture.drain()
        let operations = fixture.transport.operations
        let task = Task { @BigSyncBackgroundActor in
            try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
        }
        task.cancel()
        await assertRejected { try await task.value }
        XCTAssertEqual(fixture.transport.operations, operations)
    }

    @BigSyncBackgroundActor
    func testDomainReceiptRevalidationIsReadOnlyAtExactDurableBoundary() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let receipt = try await fixture.drain()
        XCTAssertEqual(receipt.domainPublicationScopeIdentifier, "certified-domain")
        let operations = fixture.transport.operations
        let writes = fixture.store.writes
        for _ in 0..<3 {
            try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
            try fixture.synchronizer.validateTerminalReceipt(receipt)
        }
        XCTAssertEqual(fixture.transport.operations, operations)
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testClearedDurableEvidenceRevokesDomainReceiptWithoutChangingCursor() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let receipt = try await fixture.drain()
        try fixture.synchronizer.clearDurablePublicationEvidence()
        let writes = fixture.store.writes
        XCTAssertEqual(fixture.adapter.boundary, receipt.consumedServerBoundaryIdentifier)
        XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testReplacementDomainScopeCannotValidateEarlierReceipt() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let receipt = try await fixture.drain()
        let context = try XCTUnwrap(fixture.synchronizer.activeRunContext)
        try fixture.synchronizer.persistDurablePublicationEvidence(
            domainScopeIdentifier: "replacement-domain", context: context,
            consumedServerBoundaryIdentifier: try XCTUnwrap(fixture.adapter.boundary),
            changeFeedEpoch: try XCTUnwrap(fixture.adapter.epoch))
        XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testChangedOrMissingFeedEpochRevokesDomainReceiptAtSameCursor() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let receipt = try await fixture.drain()
        for epoch: Int? in [8, nil] {
            fixture.adapter.epoch = epoch
            XCTAssertEqual(fixture.adapter.boundary, receipt.consumedServerBoundaryIdentifier)
            XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
            await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testEvidenceRevokedDuringAccountAwaitCannotPublishOldReceipt() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let receipt = try await fixture.drain()
        await fixture.account.onNextRead {
            try await fixture.synchronizer.clearDurablePublicationEvidence()
        }
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testNewDrainCanRepublishAfterDurableEvidenceRevocation() async throws {
        let fixture = Fixture(domainScopeIdentifier: "certified-domain")
        let first = try await fixture.drain()
        try fixture.synchronizer.clearDurablePublicationEvidence()
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
        let replacement = try await fixture.drain()
        XCTAssertNotEqual(first.runID, replacement.runID)
        try await fixture.synchronizer.revalidateTerminalReceipt(replacement)
        await assertRejected { try await fixture.synchronizer.revalidateTerminalReceipt(first) }
    }


    @BigSyncBackgroundActor
    func testLatePendingEditDuringFinalAccountReadRestartsBeforeReceipt() async throws {
        let fixture = Fixture(domainScopeIdentifier: "domain-a")
        fixture.armFinalAccountInjection { fixture.introducePendingEdit() }
        fixture.adapter.finishImportHook = {
            if fixture.adapter.hasPendingTerminalChanges {
                fixture.sawPrematureEvidence = try fixture.synchronizer.cloudKitE2EDurablePublicationEvidence() != nil
                fixture.forwardedLateWork = true
                fixture.adapter.hasPendingTerminalChanges = false
            }
        }
        let receipt = try await fixture.drain()
        XCTAssertTrue(fixture.injected)
        XCTAssertTrue(fixture.forwardedLateWork)
        XCTAssertFalse(fixture.sawPrematureEvidence)
        XCTAssertNotEqual(receipt.runID, fixture.injectedRunID)
        XCTAssertFalse(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testChangedCursorDuringFinalAccountReadRestartsBeforeReceipt() async throws {
        let fixture = Fixture(domainScopeIdentifier: "domain-a")
        fixture.armFinalAccountInjection { fixture.adapter.boundary = "changed-boundary" }
        let receipt = try await fixture.drain()
        XCTAssertTrue(fixture.injected)
        XCTAssertNotEqual(receipt.runID, fixture.injectedRunID)
        XCTAssertEqual(receipt.consumedServerBoundaryIdentifier, "changed-boundary")
    }

    @BigSyncBackgroundActor
    func testBlockedBranchRechecksLatePendingEditAfterItsOwnAccountAwait() async throws {
        let fixture = Fixture()
        fixture.synchronizer.domainPrepublicationHandler = { _ in
            fixture.synchronizer.publicationConsumptionPending = true
            return [.init(code: "test-blocker")]
        }
        fixture.synchronizer.publicationConsumptionHandler = { _ in
            guard !fixture.injected else { return }
            // Consumption itself revalidates once; the blocked branch then
            // performs the account await whose return must refresh journals.
            await fixture.account.onNextRead {
                await fixture.account.onNextRead { await fixture.injectPendingAtCurrentRun() }
            }
        }
        fixture.adapter.finishImportHook = {
            if fixture.adapter.hasPendingTerminalChanges {
                fixture.forwardedLateWork = true
                fixture.adapter.hasPendingTerminalChanges = false
            }
        }
        let result = try await fixture.synchronizer.synchronize()
        XCTAssertTrue(fixture.injected)
        XCTAssertTrue(fixture.forwardedLateWork)
        XCTAssertNil(result.receipt)
        XCTAssertEqual(result.publicationState, .blocked([.init(code: "test-blocker")]))
        XCTAssertFalse(fixture.synchronizer.synchronizationDrainIsActive)
    }

    @BigSyncBackgroundActor
    func testReturningEvidenceCheckpointRechecksPendingJournalAndRevokesEvidence() async throws {
        let fixture = Fixture(domainScopeIdentifier: "domain-a")
        fixture.synchronizer.processKillCheckpointHandler = { point in
            guard point == .terminalEvidenceBeforeCompletionDelivery, !fixture.injected else { return }
            XCTAssertNotNil(try fixture.synchronizer.cloudKitE2EDurablePublicationEvidence())
            fixture.injectPendingAtCurrentRun()
        }
        fixture.adapter.finishImportHook = {
            if fixture.adapter.hasPendingTerminalChanges {
                fixture.sawPrematureEvidence = try fixture.synchronizer.cloudKitE2EDurablePublicationEvidence() != nil
                fixture.forwardedLateWork = true
                fixture.adapter.hasPendingTerminalChanges = false
            }
        }
        let receipt = try await fixture.drain()
        XCTAssertTrue(fixture.forwardedLateWork)
        XCTAssertFalse(fixture.sawPrematureEvidence)
        XCTAssertNotEqual(receipt.runID, fixture.injectedRunID)
    }

    @BigSyncBackgroundActor
    func testScopeCollaboratorCancellationClosesOwnedDrainAndAllowsRetry() async throws {
        let fixture = Fixture()
        fixture.synchronizer.domainPublicationScopeIdentifierProvider = { throw CancellationError() }
        await assertCancelled { _ = try await fixture.synchronizer.synchronize() }
        XCTAssertFalse(fixture.synchronizer.syncing)
        XCTAssertFalse(fixture.synchronizer.synchronizationDrainIsActive)
        XCTAssertEqual(fixture.synchronizer._testActiveRunCallbackCount, 0)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
        fixture.synchronizer.domainPublicationScopeIdentifierProvider = { "recovered-domain" }
        _ = try await fixture.drain()
    }

    @BigSyncBackgroundActor
    func testAdapterCollaboratorCancellationClosesOwnedDrain() async throws {
        let fixture = Fixture()
        fixture.adapter.semanticHook = { throw CancellationError() }
        await assertCancelled { _ = try await fixture.synchronizer.synchronize() }
        XCTAssertFalse(fixture.synchronizer.syncing)
        XCTAssertFalse(fixture.synchronizer.synchronizationDrainIsActive)
        XCTAssertEqual(fixture.synchronizer._testActiveRunCallbackCount, 0)
    }

    @BigSyncBackgroundActor
    func testLateAccountCollaboratorCancellationClosesOwnedDrain() async throws {
        let fixture = Fixture(domainScopeIdentifier: "domain-a")
        fixture.armFinalAccountInjection { throw CancellationError() }
        await assertCancelled { _ = try await fixture.synchronizer.synchronize() }
        XCTAssertTrue(fixture.injected)
        XCTAssertFalse(fixture.synchronizer.synchronizationDrainIsActive)
        XCTAssertNil(fixture.synchronizer.activeReceiptAuthorizationID)
    }

    @BigSyncBackgroundActor
    func testCancelledCheckpointDoesNotStrandOwnedDrain() async throws {
        for point in [BigSyncBackgroundWorkerConfiguration.ProcessKillCheckpoint.localAcknowledgementBeforeTerminalPublication,
                      .terminalEvidenceBeforeCompletionDelivery] {
            let fixture = Fixture(domainScopeIdentifier: "domain-a")
            fixture.synchronizer.processKillCheckpointHandler = { checkpoint in
                if checkpoint == point { throw CancellationError() }
            }
            await assertCancelled { _ = try await fixture.synchronizer.synchronize() }
            XCTAssertFalse(fixture.synchronizer.syncing)
            XCTAssertFalse(fixture.synchronizer.synchronizationDrainIsActive)
            XCTAssertNil(fixture.synchronizer.activeReceiptAuthorizationID)
        }
    }

    @BigSyncBackgroundActor
    func testColdStartRestoresWithoutGrantingLiveAccountLease() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        let receipt = try await first.drain()
        let cold = first.reopened()
        XCTAssertTrue(cold.synchronizer.accountValidationRequired)
        XCTAssertNil(try cold.synchronizer.accountScopeLease())
        let writes = cold.store.writes
        let evidence = try await cold.synchronizer.restoredDurablePublicationEvidence()
        XCTAssertEqual(evidence?.runID, receipt.runID)
        XCTAssertEqual(cold.store.writes, writes)
        XCTAssertTrue(cold.synchronizer.accountValidationRequired)
        XCTAssertNil(try cold.synchronizer.accountScopeLease())
        XCTAssertNil(cold.synchronizer.activeRunContext)
    }

    @BigSyncBackgroundActor
    func testColdStartAccountSwitchDuringNamespaceActivationRejectsEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = { await cold.account.replace("account-b") }
        let evidence = try await cold.synchronizer.restoredDurablePublicationEvidence()
        XCTAssertNil(evidence)
        XCTAssertEqual(cold.adapter.bindingActivations, 0)
    }

    @BigSyncBackgroundActor
    func testColdStartAccountSwitchDuringBindingActivationRejectsEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.bindingHook = { await cold.account.replace("account-b") }
        let evidence = try await cold.synchronizer.restoredDurablePublicationEvidence()
        XCTAssertNil(evidence)
    }

    @BigSyncBackgroundActor
    func testColdStartSameAccountBindingReplacementRejectsOriginalEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = { try cold.replacePersistedBinding() }
        await assertCancelled { _ = try await cold.synchronizer.restoredDurablePublicationEvidence() }
        XCTAssertNil(cold.synchronizer.activeReceiptAuthorizationID)
    }

    @BigSyncBackgroundActor
    func testColdStartAccountReturnWithNewGenerationRejectsOriginalEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = {
            await cold.account.replace("account-b")
            try cold.advancePersistedLeaseGeneration()
            await cold.account.replace("original-account")
        }
        await assertCancelled { _ = try await cold.synchronizer.restoredDurablePublicationEvidence() }
    }

    @BigSyncBackgroundActor
    func testColdStartEvidenceReplacementDuringActivationDoesNotBorrowAdmission() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = { try cold.synchronizer.clearDurablePublicationEvidence() }
        await assertCancelled { _ = try await cold.synchronizer.restoredDurablePublicationEvidence() }
    }

    @BigSyncBackgroundActor
    func testColdStartPoisonBeforeNotificationActorRunsRejectsEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = { cold.synchronizer.accountScopeAuthorityFence.poison() }
        await assertCancelled { _ = try await cold.synchronizer.restoredDurablePublicationEvidence() }
    }

    @BigSyncBackgroundActor
    func testColdStartSupersededByNewRunCannotReturnOldEvidence() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        cold.adapter.namespaceHook = { _ = try await cold.drain() }
        await assertCancelled { _ = try await cold.synchronizer.restoredDurablePublicationEvidence() }
        XCTAssertNotNil(cold.synchronizer.activeRunContext)
        XCTAssertFalse(cold.synchronizer.synchronizationDrainIsActive)
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementDuringColdStartNeverReceivesOldCallback() async throws {
        let first = Fixture(domainScopeIdentifier: "domain-a", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        let successor = Fixture()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(cold.synchronizer)
        cold.adapter.namespaceHook = { await worker._test_installSynchronizer(successor.synchronizer) }
        await assertCancelled {
            try await worker.restorePublicationEvidence(from: cold.synchronizer) { _ in
                XCTFail("Old restoration must not be handed to replacement worker")
            }
        }
        XCTAssertFalse(successor.synchronizer.cancelSync)
    }

    @BigSyncBackgroundActor
    func testRestorationReservesConsumerBeforeAsynchronousLookup() async throws {
        let first = Fixture(domainScopeIdentifier: "restored-domain", useReplicaBinding: true)
        _ = try await first.drain()
        let cold = first.reopened()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(cold.synchronizer)
        var prepared = false
        var delivered = false
        cold.adapter.namespaceHook = { XCTAssertTrue(prepared) }
        try await worker.restorePublicationEvidence(from: cold.synchronizer, preparing: {
            prepared = true
            return { evidence in
                XCTAssertTrue(prepared)
                XCTAssertEqual(evidence?.domainScopeIdentifier, "restored-domain")
                delivered = true
            }
        })
        XCTAssertTrue(delivered)
    }

    @BigSyncBackgroundActor
    func testCancelledRestorationDoesNotReserveConsumer() async throws {
        let fixture = Fixture()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        // Block entry independently of scheduling, then cancel before entering.
        let entry = ReceiptPause()
        let task = Task { @BigSyncBackgroundActor in
            await entry.wait()
            try await worker.restorePublicationEvidence(from: fixture.synchronizer, preparing: {
                XCTFail("Cancelled entry must not invalidate the consumer")
                return { _ in XCTFail("Cancelled entry must not deliver") }
            })
        }
        task.cancel()
        await entry.release()
        await assertCancelled { try await task.value }
    }

    @BigSyncBackgroundActor
    func testRestorationRevalidatesReentrantProviderBeforeLookup() async throws {
        let fixture = Fixture()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        fixture.adapter.namespaceHook = { XCTFail("An obsolete preparation must not open adapters") }
        await assertCancelled {
            try await worker.restorePublicationEvidence(from: fixture.synchronizer, preparing: {
                fixture.synchronizer.cancelSynchronization()
                return { _ in XCTFail("An obsolete preparation must not deliver") }
            })
        }
    }

    @BigSyncBackgroundActor
    func testCorruptCursorReturningOldSaveCannotCompleteCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: false, outcome: .returned)
    }

    @BigSyncBackgroundActor
    func testCorruptCursorCancelledOldSaveCannotCompleteCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: false, outcome: .cancelled)
    }

    @BigSyncBackgroundActor
    func testCorruptCursorFailedOldSaveCannotCompleteCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: false, outcome: .failed)
    }

    @BigSyncBackgroundActor
    func testExpiredCursorReturningOldSaveCannotRetryCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: true, outcome: .returned)
    }

    @BigSyncBackgroundActor
    func testExpiredCursorCancelledOldSaveCannotCompleteCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: true, outcome: .cancelled)
    }

    @BigSyncBackgroundActor
    func testExpiredCursorFailedOldSaveCannotCompleteCoalescedSuccessor() async throws {
        try await exerciseCursorFailureOwnership(expired: true, outcome: .failed)
    }

    @BigSyncBackgroundActor
    func testOwnedCursorRecoveryReturnAndThrowAlwaysResolveOriginalDrain() async throws {
        for expired in [false, true] {
            for outcome: CursorSaveOutcome in [.returned, .cancelled, .failed] {
                let fixture = Fixture(useReplicaBinding: true)
                _ = try await fixture.drain()
                var recoveryStarted = false
                var cursorWrites = 0
                fixture.synchronizer.domainPublicationScopeIdentifierProvider = {
                    fixture.synchronizer.domainPublicationScopeIdentifierProvider = nil
                    recoveryStarted = true
                    if expired { throw CKError(.changeTokenExpired) }
                    throw CloudKitChangeFeedError.corruptCursor
                }
                fixture.adapter.saveTokenHook = { token in
                    guard recoveryStarted, token == nil else { return }
                    fixture.adapter.saveTokenHook = nil
                    cursorWrites += 1
                    switch outcome {
                    case .returned: return
                    case .cancelled: throw CancellationError()
                    case .failed: throw CursorTestError.persistence
                    }
                }
                let watchdog = Task { @BigSyncBackgroundActor in
                    try await Task.sleep(nanoseconds: 5_000_000_000)
                    XCTFail("Owned recovery stranded its original caller")
                    fixture.synchronizer.cancelSynchronization()
                }
                defer { watchdog.cancel(); fixture.synchronizer.cancelSynchronization() }
                do {
                    let result = try await fixture.synchronizer.synchronize()
                    XCTAssertEqual(outcome, .returned)
                    XCTAssertEqual(result.publicationState, .complete)
                } catch {
                    XCTAssertNotEqual(outcome, .returned)
                    if expired { XCTAssertEqual((error as? CKError)?.code, .changeTokenExpired) }
                    else { XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor) }
                }
                XCTAssertEqual(cursorWrites, 1)
                XCTAssertEqual(fixture.synchronizer._testSynchronizationWaiterCount, 0)
            }
        }
    }

    private enum CursorSaveOutcome: Sendable, Equatable { case returned, cancelled, failed }
    private enum CursorTestError: Error { case persistence, timeout }

    @BigSyncBackgroundActor
    private func exerciseCursorFailureOwnership(expired: Bool, outcome: CursorSaveOutcome) async throws {
        let fixture = Fixture(useReplicaBinding: true)
        _ = try await fixture.drain()
        let oldSave = ReceiptPause()
        let successorTransport = ReceiptPause()
        var recoveryStarted = false
        var oldSaveEntered = false
        var successorEntered = false
        var completions = 0
        fixture.synchronizer.domainPublicationScopeIdentifierProvider = {
            // Fail the real registered terminal callback exactly once.
            recoveryStarted = true
            fixture.synchronizer.domainPublicationScopeIdentifierProvider = nil
            if expired { throw CKError(.changeTokenExpired) }
            throw CloudKitChangeFeedError.corruptCursor
        }
        fixture.adapter.saveTokenHook = { token in
            guard recoveryStarted, token == nil else { return }
            fixture.adapter.saveTokenHook = nil
            oldSaveEntered = true
            await oldSave.wait()
            switch outcome {
            case .returned: return
            case .cancelled: throw CancellationError()
            case .failed: throw CursorTestError.persistence
            }
        }
        let original = Task { @BigSyncBackgroundActor in try await fixture.synchronizer.synchronize() }
        defer {
            fixture.synchronizer.cancelSynchronization()
            Task { await oldSave.release(); await successorTransport.release() }
        }
        try await waitFor { oldSaveEntered }
        XCTAssertGreaterThan(fixture.synchronizer._testActiveRunCallbackCount, 0)
        fixture.synchronizer.cancelSynchronization()
        await assertCancelled { _ = try await original.value }
        fixture.transport.databaseChangesHook = {
            fixture.transport.databaseChangesHook = nil
            successorEntered = true
            await successorTransport.wait()
        }
        let first = Task { @BigSyncBackgroundActor in
            let result = try await fixture.synchronizer.synchronize()
            completions += 1
            return result
        }
        let second = Task { @BigSyncBackgroundActor in
            let result = try await fixture.synchronizer.synchronize()
            completions += 1
            return result
        }
        try await waitFor { fixture.synchronizer._testSynchronizationWaiterCount == 2 }
        let successorID = fixture.synchronizer.synchronizationAttemptID
        let successorTask = fixture.synchronizer.synchronizationTask
        XCTAssertNotNil(successorTask)
        await oldSave.release()
        try await waitFor { successorEntered || completions > 0 }
        XCTAssertTrue(successorEntered)
        XCTAssertEqual(completions, 0)
        XCTAssertEqual(fixture.synchronizer._testSynchronizationWaiterCount, 2)
        XCTAssertEqual(fixture.synchronizer.synchronizationAttemptID, successorID)
        XCTAssertEqual(fixture.synchronizer.synchronizationTask, successorTask)
        XCTAssertFalse(fixture.synchronizer.cancelSync)
        await successorTransport.release()
        let firstResult = try await first.value
        let secondResult = try await second.value
        XCTAssertEqual(firstResult.publicationState, .complete)
        XCTAssertEqual(secondResult.publicationState, .complete)
        XCTAssertEqual(completions, 2)
        XCTAssertEqual(fixture.synchronizer._testSynchronizationWaiterCount, 0)
    }


    // Worker requests have a cancellation lifetime separate from a transport
    // run: ordinary concurrent callers coalesce, while explicit cancellation
    // revokes callers still suspended before a run exists.
    @BigSyncBackgroundActor
    func testWorkerCancellationRevokesAvailablePreflightButAllowsFreshRequest() async throws {
        try await assertCancelledPreflight(.available)
    }

    @BigSyncBackgroundActor
    func testWorkerCancellationRevokesUndeterminedPreflightWithoutRetry() async throws {
        try await assertCancelledPreflight(.unavailable(.couldNotDetermine))
    }

    @BigSyncBackgroundActor
    func testWorkerCancellationRevokesFailedPreflightWithoutRetry() async throws {
        try await assertCancelledPreflight(.failed)
    }

    @BigSyncBackgroundActor
    private func assertCancelledPreflight(_ status: CloudKitAccountAvailability) async throws {
        let fixture = Fixture()
        let entered = ReceiptPause(), release = ReceiptPause()
        let provider = ReceiptStatusProvider(first: status, entered: entered, release: release)
        let worker = BigSyncBackgroundActor(accountAvailabilityGate:
            CloudKitAccountAvailabilityGate(statusProvider: { _ in await provider.read() }))
        await worker._test_installSynchronizer(fixture.synchronizer)
        fixture.synchronizer.accountValidationRequired = true
        fixture.synchronizer.cancelledDueToUnauthentication = true
        let old = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        await entered.wait()
        await worker.cancelSynchronization() // Deliberately do NOT cancel `old`.
        let operations = fixture.transport.operations
        let validation = fixture.synchronizer.accountValidationRequired
        let unauthenticated = fixture.synchronizer.cancelledDueToUnauthentication
        await release.release()
        let result = await old.value
        XCTAssertNil(result)
        XCTAssertEqual(fixture.transport.operations, operations)
        XCTAssertEqual(fixture.synchronizer.accountValidationRequired, validation)
        XCTAssertEqual(fixture.synchronizer.cancelledDueToUnauthentication, unauthenticated)
        let retry = await worker._test_hasScheduledAccountAvailabilityRetry
        XCTAssertFalse(retry)
        let fresh = await worker.synchronizeCloudKit()
        XCTAssertEqual(fresh?.publicationState, .complete)
        XCTAssertGreaterThan(fixture.transport.operations, operations)
    }

    @BigSyncBackgroundActor
    func testWorkerCallerCancellationDoesNotMutateAvailablePreflightState() async throws {
        let fixture = Fixture()
        let entered = ReceiptPause(), release = ReceiptPause()
        let provider = ReceiptStatusProvider(first: .available, entered: entered, release: release)
        let worker = BigSyncBackgroundActor(accountAvailabilityGate:
            CloudKitAccountAvailabilityGate(statusProvider: { _ in await provider.read() }))
        await worker._test_installSynchronizer(fixture.synchronizer)
        fixture.synchronizer.accountValidationRequired = true
        fixture.synchronizer.cancelledDueToUnauthentication = true
        let old = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        await entered.wait()
        let retryRelease = ReceiptPause()
        let retry = Task { await retryRelease.wait() }
        await worker._test_installAccountAvailabilityRetryTask(retry)
        old.cancel()
        await release.release()
        let result = await old.value
        XCTAssertNil(result)
        XCTAssertTrue(fixture.synchronizer.accountValidationRequired)
        XCTAssertTrue(fixture.synchronizer.cancelledDueToUnauthentication)
        XCTAssertEqual(fixture.transport.operations, 0)
        let retained = await worker._test_accountAvailabilityRetryTask
        XCTAssertEqual(retained, retry)
        XCTAssertFalse(retry.isCancelled)
        await retryRelease.release()
        let fresh = await worker.synchronizeCloudKit()
        XCTAssertEqual(fresh?.publicationState, .complete)
    }

    @BigSyncBackgroundActor
    func testAlreadyCancelledWorkerEntryPreservesEligibleDelayedWork() async throws {
        let fixture = Fixture()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer, performsAccountAvailabilityPreflight: false)
        await worker._test_scheduleDormantInitialSynchronization()
        let retryRelease = ReceiptPause(), entryRelease = ReceiptPause()
        let retry = Task { await retryRelease.wait() }
        await worker._test_installAccountAvailabilityRetryTask(retry)
        let old = Task { @BigSyncBackgroundActor in
            await entryRelease.wait()
            return await worker.synchronizeCloudKit()
        }
        old.cancel()
        await entryRelease.release()
        let result = await old.value
        let initial = await worker._test_hasScheduledInitialSynchronization
        let retained = await worker._test_accountAvailabilityRetryTask
        XCTAssertNil(result)
        XCTAssertTrue(initial)
        XCTAssertEqual(retained, retry)
        XCTAssertFalse(retry.isCancelled)
        XCTAssertEqual(fixture.transport.operations, 0)
        await retryRelease.release()
        await worker.cancelSynchronization()
    }

    @BigSyncBackgroundActor
    func testWorkerCancellationRevokesRequestWaitingForRestoration() async throws {
        let fixture = Fixture()
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer, performsAccountAvailabilityPreflight: false)
        let restorationRelease = ReceiptPause()
        let restoration = Task { await restorationRelease.wait() }
        await worker._test_installPublicationRestorationTask(restoration)
        let old = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        try await waitFor { worker._test_activeSynchronizationRequestCount == 1 }
        await worker.cancelSynchronization()
        await restorationRelease.release()
        let result = await old.value
        XCTAssertNil(result)
        XCTAssertEqual(fixture.transport.operations, 0)
        let fresh = await worker.synchronizeCloudKit()
        XCTAssertEqual(fresh?.publicationState, .complete)
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementCannotClearSuccessorAvailabilityRetry() async throws {
        let fixture = Fixture(), successor = Fixture()
        let entered = ReceiptPause(), release = ReceiptPause()
        let provider = ReceiptStatusProvider(first: .available, entered: entered, release: release)
        let worker = BigSyncBackgroundActor(accountAvailabilityGate:
            CloudKitAccountAvailabilityGate(statusProvider: { _ in await provider.read() }))
        var completions = 0
        await worker._test_installSynchronizer(fixture.synchronizer,
            synchronizationCompletionHandler: { _ in completions += 1 })
        let old = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        await entered.wait()
        let retryRelease = ReceiptPause()
        let retry = Task { await retryRelease.wait() }
        await worker._test_installSynchronizer(successor.synchronizer)
        await worker._test_installAccountAvailabilityRetryTask(retry)
        successor.synchronizer.accountValidationRequired = true
        successor.synchronizer.cancelledDueToUnauthentication = true
        await release.release()
        let result = await old.value
        let retained = await worker._test_accountAvailabilityRetryTask
        XCTAssertNil(result)
        XCTAssertEqual(retained, retry)
        XCTAssertFalse(retry.isCancelled)
        XCTAssertTrue(successor.synchronizer.accountValidationRequired)
        XCTAssertTrue(successor.synchronizer.cancelledDueToUnauthentication)
        XCTAssertEqual(fixture.transport.operations, 0)
        XCTAssertEqual(completions, 0)
        await retryRelease.release()
        let fresh = await worker.synchronizeCloudKit()
        XCTAssertEqual(fresh?.publicationState, .complete)
    }

    @BigSyncBackgroundActor
    func testWorkerRejectsReturnAfterCallerCancellationInCompletionHandler() async throws {
        try await assertObsoleteCompletion(.callerCancellation)
    }

    @BigSyncBackgroundActor
    func testWorkerRejectsReturnAfterExplicitCancellationInCompletionHandler() async throws {
        try await assertObsoleteCompletion(.workerCancellation)
    }

    @BigSyncBackgroundActor
    func testWorkerRejectsReturnAfterReplacementInCompletionHandler() async throws {
        try await assertObsoleteCompletion(.replacement)
    }

    private enum CompletionRevocation { case callerCancellation, workerCancellation, replacement }

    @BigSyncBackgroundActor
    private func assertObsoleteCompletion(_ revocation: CompletionRevocation) async throws {
        let fixture = Fixture(), successor = Fixture()
        let worker = BigSyncBackgroundActor()
        let entered = ReceiptPause(), release = ReceiptPause()
        var completions = 0
        await worker._test_installSynchronizer(fixture.synchronizer,
            performsAccountAvailabilityPreflight: false,
            synchronizationCompletionHandler: { result in
                XCTAssertEqual(result.publicationState, .complete)
                XCTAssertNotNil(result.receipt)
                completions += 1
                await entered.release()
                await release.wait()
            })
        let old = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        await entered.wait()
        switch revocation {
        case .callerCancellation: old.cancel()
        case .workerCancellation: await worker.cancelSynchronization()
        case .replacement:
            await worker._test_installSynchronizer(successor.synchronizer,
                performsAccountAvailabilityPreflight: false)
        }
        await release.release()
        let result = await old.value
        XCTAssertNil(result)
        XCTAssertEqual(completions, 1)
        let fresh = await worker.synchronizeCloudKit()
        XCTAssertEqual(fresh?.publicationState, .complete)
    }

    @BigSyncBackgroundActor
    func testConcurrentWorkerRequestsCoalesceWithoutRevokingEachOther() async throws {
        try await assertCoalescedWorkerRequests(cancel: false)
    }

    @BigSyncBackgroundActor
    func testExplicitCancellationRevokesBothCoalescedWorkerRequests() async throws {
        try await assertCoalescedWorkerRequests(cancel: true)
    }

    @BigSyncBackgroundActor
    private func assertCoalescedWorkerRequests(cancel: Bool) async throws {
        var terminalCount = 0
        let fixture = Fixture(progressHandler: {
            if $0 == "terminal-receipt" { terminalCount += 1 }
        })
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer, performsAccountAvailabilityPreflight: false)
        let entered = ReceiptPause(), release = ReceiptPause()
        // One synchronization may perform several change-feed requests. Count
        // distinct attempts, not requests, and pause only the first request.
        var attempts = Set<UUID>()
        fixture.transport.databaseChangesHook = {
            if attempts.insert(fixture.synchronizer.synchronizationAttemptID).inserted {
                await entered.release()
                await release.wait()
            }
        }
        let a = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        await entered.wait()
        let b = Task { @BigSyncBackgroundActor in await worker.synchronizeCloudKit() }
        try await waitFor { fixture.synchronizer._testSynchronizationWaiterCount == 2 }
        if cancel { await worker.cancelSynchronization() }
        await release.release()
        let ar = await a.value, br = await b.value
        if cancel {
            XCTAssertNil(ar); XCTAssertNil(br)
            let fresh = await worker.synchronizeCloudKit()
            XCTAssertEqual(fresh?.publicationState, .complete)
        } else {
            XCTAssertEqual(ar?.publicationState, .complete)
            XCTAssertEqual(br?.publicationState, .complete)
            XCTAssertEqual(ar?.receipt?.runID, br?.receipt?.runID)
            // synchronize() requests fresh work as well as registering a
            // waiter. Its second caller deliberately coalesces one refresh
            // attempt into the SAME drain, not a second completion.
            XCTAssertEqual(attempts.count, 2)
            XCTAssertEqual(terminalCount, 1)
        }
        XCTAssertEqual(fixture.synchronizer._testSynchronizationWaiterCount, 0)
    }

    @BigSyncBackgroundActor
    func testTerminalDiagnosticBeginStartsOneSuccessorDrain() async throws {
        try await assertSynchronousTerminalReentry(cancelFirst: false)
    }

    @BigSyncBackgroundActor
    func testTerminalDiagnosticCancelAndBeginPreservesSuccessorAndItsWaiters() async throws {
        try await assertSynchronousTerminalReentry(cancelFirst: true)
    }

    @BigSyncBackgroundActor
    private func assertSynchronousTerminalReentry(cancelFirst: Bool) async throws {
        let observer = ReceiptProgressObserver(cancelFirst: cancelFirst)
        let fixture = Fixture(progressHandler: { observer.record($0) })
        observer.synchronizer = fixture.synchronizer
        let successorEntered = ReceiptPause(), release = ReceiptPause()
        var attempts = Set<UUID>()
        fixture.transport.databaseChangesHook = {
            let isNewAttempt = attempts.insert(fixture.synchronizer.synchronizationAttemptID).inserted
            if isNewAttempt && attempts.count == 2 {
                await successorEntered.release()
                await release.wait()
            }
        }
        let first = try await fixture.synchronizer.synchronize()
        // A bounded wait makes a lost successor request fail rather than hang.
        try await waitFor { attempts.count == 2 }
        await successorEntered.wait()
        XCTAssertEqual(observer.terminalCount, 1)
        XCTAssertNotEqual(fixture.synchronizer.synchronizationAttemptID, observer.firstAttempt)
        XCTAssertTrue(fixture.synchronizer.syncing)
        XCTAssertNotNil(fixture.synchronizer.synchronizationTask)
        let b = Task { @BigSyncBackgroundActor in try await fixture.synchronizer.synchronize() }
        let c = Task { @BigSyncBackgroundActor in try await fixture.synchronizer.synchronize() }
        try await waitFor { fixture.synchronizer._testSynchronizationWaiterCount == 2 }
        await release.release()
        let br = try await b.value, cr = try await c.value
        XCTAssertEqual(br.publicationState, .complete)
        XCTAssertEqual(cr.receipt?.runID, br.receipt?.runID)
        XCTAssertNotEqual(first.receipt?.runID, br.receipt?.runID)
        // The two ordinary synchronize() calls below B's first fetch request
        // one coalesced refresh inside B. A and B still complete exactly once
        // each; the diagnostic itself creates only one successor drain.
        XCTAssertEqual(attempts.count, 3)
        XCTAssertEqual(observer.terminalCount, 2)
        XCTAssertFalse(fixture.synchronizer.syncing)
        XCTAssertNil(fixture.synchronizer.synchronizationTask)
    }

    @BigSyncBackgroundActor
    func testTerminalDiagnosticRecordsOneCompletedDrainAndKeepsKillCheckpoint() async throws {
        var terminalCount = 0, checkpointCount = 0
        let fixture = Fixture(progressHandler: { if $0 == "terminal-receipt" { terminalCount += 1 } })
        fixture.synchronizer.processKillCheckpointHandler = { boundary in
            if boundary == .terminalEvidenceBeforeCompletionDelivery { checkpointCount += 1 }
        }
        _ = try await fixture.drain()
        XCTAssertEqual(terminalCount, 1)
        XCTAssertEqual(checkpointCount, 1)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticBeginDoesNotNotifyForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .begin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticCancelAndBeginDoesNotNotifyForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .cancelAndBegin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessNotificationBeginDoesNotCallDelegateForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: true, action: .begin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticCancellationSuppressesLaterObservers() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .cancel)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticAccountFenceSuppressesLaterObservers() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .fence)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessNotificationAccountFenceSuppressesDelegate() async throws {
        try await assertTerminalSuccessOwnership(atNotification: true, action: .fence)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessUnchangedOwnerNotifiesExactlyOnce() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .none)
    }

    private enum TerminalObserverAction { case begin, cancelAndBegin, cancel, fence, none }

    @BigSyncBackgroundActor
    private func assertTerminalSuccessOwnership(
        atNotification: Bool, action: TerminalObserverAction
    ) async throws {
        let observer = ReceiptSuccessObserver()
        let fixture = Fixture(progressHandler: { milestone in
            guard milestone == "terminal-receipt" else { return }
            observer.diagnostics += 1
            if !atNotification { observer.reenter?() }
        })
        let synchronizer = fixture.synchronizer
        synchronizer.delegate = observer
        observer.reenter = { [weak synchronizer, weak observer] in
            guard let synchronizer, let observer else { return }
            observer.reenter = nil
            switch action {
            case .begin: synchronizer.beginSynchronization()
            case .cancelAndBegin:
                synchronizer.cancelSynchronization()
                synchronizer.beginSynchronization()
            case .cancel: synchronizer.cancelSynchronization()
            case .fence: synchronizer.accountScopeAuthorityFence.poison()
            case .none: break
            }
        }
        // Selector observers run synchronously on the posting thread. An
        // asynchronously scheduled Task would hide this reentrancy boundary.
        observer.reenterOnNotification = atNotification
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptSuccessObserver.receiveSuccessNotification(_:)),
            name: .SynchronizerDidSynchronize, object: synchronizer)
        let release = ReceiptPause()
        var successorEntered = false
        var attempts = Set<UUID>()
        fixture.transport.databaseChangesHook = {
            let inserted = attempts.insert(synchronizer.synchronizationAttemptID).inserted
            if inserted && attempts.count == 2 {
                successorEntered = true
                await release.wait()
            }
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
            observer.reenter = nil
            synchronizer.cancelSynchronization()
            Task { await release.release() }
        }
        let first = try await synchronizer.synchronize()
        // Waiter delivery precedes diagnostics, but the registered terminal
        // callback must have returned before inspecting its observer effects.
        try await waitFor { observer.diagnostics == 1 }
        XCTAssertEqual(first.publicationState, .complete)
        XCTAssertEqual(observer.notifications, atNotification || action == .none ? 1 : 0)
        XCTAssertEqual(observer.completions, action == .none ? 1 : 0)
        guard action == .begin || action == .cancelAndBegin else { return }
        try await waitFor { successorEntered }
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertNotNil(synchronizer.synchronizationTask)
        let successor = Task { @BigSyncBackgroundActor in try await synchronizer.synchronize() }
        try await waitFor { synchronizer._testSynchronizationWaiterCount == 1 }
        await release.release()
        let second = try await successor.value
        try await waitFor { observer.diagnostics == 2 }
        XCTAssertEqual(second.publicationState, .complete)
        XCTAssertNotEqual(first.receipt?.runID, second.receipt?.runID)
        XCTAssertEqual(observer.notifications, atNotification ? 2 : 1)
        XCTAssertEqual(observer.completions, 1)
        XCTAssertEqual(synchronizer._testSynchronizationWaiterCount, 0)
    }

    @BigSyncBackgroundActor
    private func waitFor(_ predicate: () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(5))
        while !predicate() {
            guard ContinuousClock.now < deadline else {
                XCTFail("Timed out waiting for a deterministic test boundary")
                throw CursorTestError.timeout
            }
            try await Task.sleep(nanoseconds: 1_000_000)
        }
    }

    @BigSyncBackgroundActor
    func testOrdinaryReceiptRejectsImmediateNotificationFenceBeforeQueuedCancellation() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let receipt = try await fixture.drain()
        let attempt = fixture.synchronizer.synchronizationAttemptID
        let writes = fixture.store.writes
        XCTAssertNotNil(try fixture.synchronizer.accountScopeLease())
        // Exact synchronous first phase of CKAccountChanged. Intentionally do
        // not run its queued cancellation yet. This is not a real account switch.
        fixture.synchronizer.accountScopeAuthorityFence.poison()
        XCTAssertNil(try fixture.synchronizer.accountScopeLease())
        XCTAssertThrowsError(try fixture.synchronizer.validateTerminalReceipt(receipt))
        await assertCancelled { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertEqual(fixture.synchronizer.synchronizationAttemptID, attempt)
        XCTAssertFalse(fixture.synchronizer.cancelSync)
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testOrdinaryReceiptRejectsSameAccountReturnAfterImmediateFence() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let receipt = try await fixture.drain()
        await fixture.account.onNextRead { @BigSyncBackgroundActor in
            fixture.injected = true
            fixture.synchronizer.accountScopeAuthorityFence.poison()
        }
        await assertCancelled { try await fixture.synchronizer.revalidateTerminalReceipt(receipt) }
        XCTAssertTrue(fixture.injected, "Must reach the actual-account suspension boundary")
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    private func assertCancelled(file: StaticString = #filePath, line: UInt = #line,
                                 _ operation: () async throws -> Void) async {
        do { try await operation(); XCTFail("Expected cancellation", file: file, line: line) }
        catch is CancellationError { }
        catch { XCTFail("Unexpected error: \(error)", file: file, line: line) }
    }

    @BigSyncBackgroundActor
    private func assertRejected(
        file: StaticString = #filePath, line: UInt = #line,
        _ operation: () async throws -> Void
    ) async {
        do {
            try await operation()
            XCTFail("Expected obsolete or incomplete terminal authority to be rejected", file: file, line: line)
        } catch { }
    }

    @BigSyncBackgroundActor
    private final class Fixture {
        let store: ReceiptStore
        let transport = ReceiptTransport()
        let account: ReceiptAccount
        let identifier: String
        let useReplicaBinding: Bool
        let domainScopeIdentifier: String?
        var injected = false
        var injectedRunID: UUID?
        var forwardedLateWork = false
        var sawPrematureEvidence = false
        let adapter: ReceiptAdapter
        let synchronizer: CloudKitSynchronizer

        init(domainScopeIdentifier: String? = nil, useReplicaBinding: Bool = false,
             store: ReceiptStore = ReceiptStore(), account: ReceiptAccount = ReceiptAccount(),
             identifier: String = UUID().uuidString, zoneID: CKRecordZone.ID? = nil,
             progressHandler: CloudKitSynchronizer.ProgressHandler? = nil) {
            self.store = store; self.account = account; self.identifier = identifier
            self.useReplicaBinding = useReplicaBinding; self.domainScopeIdentifier = domainScopeIdentifier
            let zone = zoneID ?? CKRecordZone.ID(zoneName: "receipt-fixture-\(UUID().uuidString)",
                ownerName: CKCurrentUserDefaultName)
            adapter = ReceiptAdapter(zoneID: zone)
            synchronizer = CloudKitSynchronizer(identifier: identifier,
                containerIdentifier: "iCloud.receipt-fixture", database: transport,
                recordZoneID: zone, keyValueStore: store,
                accountIdentifierProvider: { try await account.read() },
                accountStatusProvider: { .available }, progressHandler: progressHandler, changeFeed: transport,
                subscriptionStore: transport, zoneStore: transport, recordStore: transport,
                initialReplicaBindingAdmissionHandler: { _ in },
                accountReplacementPolicy: useReplicaBinding ? .localDatasetRebootstrap : .serverReconciliation,
                logger: Logger(label: "TerminalReceiptTests"))
            synchronizer._allowRecordZoneRebindingForTesting()
            synchronizer.addModelAdapter(adapter)
            if let domainScopeIdentifier {
                synchronizer.domainPublicationScopeIdentifierProvider = { domainScopeIdentifier }
            }
        }

        func introducePendingEdit() { adapter.hasPendingTerminalChanges = true }
        func reopened() -> Fixture {
            Fixture(domainScopeIdentifier: domainScopeIdentifier, useReplicaBinding: useReplicaBinding,
                    store: store, account: account, identifier: identifier, zoneID: adapter.recordZoneID)
        }

        func injectPendingAtCurrentRun() {
            injected = true; injectedRunID = synchronizer.activeRunContext?.runID
            introducePendingEdit()
        }

        func armFinalAccountInjection(_ operation: @escaping @BigSyncBackgroundActor @Sendable () throws -> Void) {
            synchronizer.domainPublicationScopeIdentifierProvider = { [self] in
                guard !injected else { return domainScopeIdentifier }
                await account.onNextRead {
                    await self.account.onNextRead {
                        try await self.runInjection(operation)
                    }
                }
                return domainScopeIdentifier
            }
        }

        func runInjection(_ operation: @BigSyncBackgroundActor @Sendable () throws -> Void) throws {
            injected = true; injectedRunID = synchronizer.activeRunContext?.runID
            try operation()
        }

        func replacePersistedBinding() throws {
            let key = synchronizer.durableStateKey("ReplicaBinding.v1")
            var value = try XCTUnwrap(store.object(forKey: key) as? [String: Any])
            value["activeGenerationIdentifier"] = String(repeating: "e", count: 64)
            try store.bigSyncSetDurably(value: value, forKey: key)
        }

        func advancePersistedLeaseGeneration() throws {
            let key = synchronizer.durableStateKey("AccountScopeLease.v1")
            let old = try BigSyncAccountScopeLeaseState.load(store: store, key: key)
            let lease = try XCTUnwrap(old.lease)
            let next = try BigSyncAccountScopeLeaseState(generation: old.generation + 1,
                accountScopeIdentifier: lease.accountScopeIdentifier, validatedAt: lease.validatedAt)
            try next.persist(store: store, key: key)
        }


        func drain() async throws -> CloudKitSynchronizer.SynchronizationReceipt {
            let result = try await synchronizer.synchronize()
            XCTAssertEqual(result.publicationState, .complete)
            return try XCTUnwrap(result.receipt)
        }
    }
}

private actor ReceiptAccount {
    private var identifier = "original-account"
    private var nextRead: (@Sendable () async throws -> Void)?
    func replace(_ identifier: String) { self.identifier = identifier }
    func onNextRead(_ operation: @escaping @Sendable () async throws -> Void) { nextRead = operation }
    func read() async throws -> String {
        let operation = nextRead
        nextRead = nil
        try await operation?()
        return identifier
    }
}

// These synchronous test doubles are used only by the serial BigSync actor.
private final class ReceiptStore: NSObject, KeyValueStore, @unchecked Sendable {
    private var values: [String: Any] = [:]
    private(set) var writes = 0
    var durable = true
    func object(forKey key: String) -> Any? { values[key] }
    override func value(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value; writes += 1 }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { values.removeValue(forKey: key); writes += 1 }
    func synchronize() -> Bool { durable }
}

private final class ReceiptTransport: NSObject, CloudKitDatabaseAdapter,
    CloudKitSubscriptionStore, CloudKitZoneStore, CloudKitRecordStore,
    CloudKitChangeFeed, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    private(set) var operations = 0
    func subscription(withID identifier: CKSubscription.ID) async throws -> CKSubscription? {
        operations += 1; return nil
    }
    func save(subscription: CKSubscription) async throws -> CKSubscription { operations += 1; return subscription }
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws { operations += 1 }
    func recordZone(withID identifier: CKRecordZone.ID) async throws -> CKRecordZone {
        operations += 1; return CKRecordZone(zoneID: identifier)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { operations += 1; return recordZone }
    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws { operations += 1 }
    @BigSyncBackgroundActor var mutationHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor private(set) var mutationCalls = 0
    @BigSyncBackgroundActor
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        operations += 1
        mutationCalls += 1
        try await mutationHook?()
        return .init(saveResults: Dictionary(uniqueKeysWithValues: saving.map { ($0.recordID, .success($0)) }),
                     deleteResults: Dictionary(uniqueKeysWithValues: deleting.map { ($0, .success(())) }))
    }
    @BigSyncBackgroundActor var databaseChangesHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor
    func databaseChanges(since: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        try await databaseChangesHook?()
        operations += 1
        return .init(cursor: .init(serializedData: Data("db-boundary".utf8)),
            changedZoneIDs: [], deletions: [], moreComing: false)
    }
    func recordZoneChanges(in zoneID: CKRecordZone.ID, since: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        operations += 1
        return .init(cursor: .init(serializedData: Data("zone-boundary".utf8)),
            records: [], deletedRecordIDs: [], moreComing: false)
    }
}

private final class ReceiptAdapter: NSObject, ModelAdapter, ChangeFeedResetMigrating,
    TerminalSynchronizationStateModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server
    var hasChanges: Bool { false }
    var hasPendingTerminalChanges = false
    var boundary: String? = "consumed-boundary"
    var epoch: Int? = 7
    private var bootstrapActive = false
    private(set) var acknowledgements = 0
    init(zoneID: CKRecordZone.ID) { recordZoneID = zoneID }
    func cleanUp() async throws { }
    func resetSyncCaches() async throws { }
    func reconcileReplicaJournalHandoff(_ handoff: BigSyncReplicaJournalHandoff,
        accountScopeIdentifier: String, epoch: Int, verifyOnly: Bool) async throws { }
    func prepareChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws {
        bootstrapActive = true
    }
    func beginChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { }
    func isChangeFeedServerBootstrapActive() async -> Bool { bootstrapActive }
    func changeFeedResetCompletionIsDurable(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws -> Bool {
        !bootstrapActive
    }
    func reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws { }
    func finishChangeFeedReset(accountScopeIdentifier: String, epoch: Int, mode: ChangeFeedResetMode) async throws {
        bootstrapActive = false
    }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { false }
    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws -> [InboundLiveResult] { [] }
    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws -> [InboundDeletionResult] { [] }
    func persistImportedChanges() async throws { }
    @BigSyncBackgroundActor var preparedUploads: [PreparedRecordUpload] = []
    @BigSyncBackgroundActor var preparedDeletions: [PreparedRecordDeletion] = []
    @BigSyncBackgroundActor var preparationHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor var acknowledgementHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] {
        try await preparationHook?()
        return Array(preparedUploads.prefix(limit))
    }
    @BigSyncBackgroundActor
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws {
        try await acknowledgementHook?()
        preparedUploads.removeAll { item in
            savedRecords.contains { $0.recordID == item.record.recordID }
                && matchingGenerations[item.record.recordID.recordName] == item.generation
        }
        acknowledgements += 1
    }
    @BigSyncBackgroundActor
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] {
        Array(preparedDeletions.prefix(limit))
    }
    @BigSyncBackgroundActor
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {
        try await acknowledgementHook?()
        preparedDeletions.removeAll { item in
            recordIDs.contains(item.recordID) && matchingGenerations[item.recordID.recordName] == item.generation
        }
        acknowledgements += 1
    }
    func requeueMissingServerRecords(_ recordIDs: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws { }
    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    @BigSyncBackgroundActor var saveTokenHook: (@BigSyncBackgroundActor @Sendable (RecordZoneChangeCursor?) async throws -> Void)?
    @BigSyncBackgroundActor
    func saveToken(_ token: RecordZoneChangeCursor?) async throws { try await saveTokenHook?(token) }
    @BigSyncBackgroundActor
    func consumedServerBoundaryIdentifier(accountScopeIdentifier: String, replicaBindingGenerationIdentifier: String?,
        containerIdentifier: String, databaseScope: CKDatabase.Scope) throws -> String? { boundary }
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? { epoch }
    @BigSyncBackgroundActor var finishImportHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor var namespaceHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor var bindingHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor var semanticHook: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    @BigSyncBackgroundActor var bindingActivations = 0
    @BigSyncBackgroundActor
    func didFinishImport() async throws { try await finishImportHook?() }
    @BigSyncBackgroundActor
    func activateTransportNamespace(containerIdentifier: String, databaseScope: CKDatabase.Scope) async throws {
        let hook = namespaceHook; namespaceHook = nil; try await hook?()
    }
    @BigSyncBackgroundActor
    func activateReplicaBinding(accountScopeIdentifier: String, replicaBindingGenerationIdentifier: String?) async throws {
        bindingActivations += 1
        let hook = bindingHook; bindingHook = nil; try await hook?()
    }
    @BigSyncBackgroundActor
    func semanticPublicationBlockers() async throws -> [CloudKitSynchronizer.DomainBlocker] {
        try await semanticHook?(); return []
    }
    func cancelSynchronization() { }
    func unsetCancellation() async throws { }
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        hasPendingTerminalChanges || !preparedUploads.isEmpty || !preparedDeletions.isEmpty
    }
}

/// Cancellation intentionally does not release this gate: the tests model a
/// collaborator that returns late despite the original task being cancelled.
private actor ReceiptPause {
    private var released = false
    private var continuations: [CheckedContinuation<Void, Never>] = []
    func wait() async {
        if released { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    func release() {
        released = true
        let pending = continuations
        continuations.removeAll()
        pending.forEach { $0.resume() }
    }
}


private actor ReceiptStatusProvider {
    let first: CloudKitAccountAvailability
    let entered: ReceiptPause
    let release: ReceiptPause
    private var firstRead = true
    init(first: CloudKitAccountAvailability, entered: ReceiptPause, release: ReceiptPause) {
        self.first = first; self.entered = entered; self.release = release
    }
    func read() async -> CloudKitAccountAvailability {
        guard firstRead else { return .available }
        firstRead = false
        await entered.release()
        await release.wait() // Intentionally ignores cooperative cancellation.
        return first
    }
}

@BigSyncBackgroundActor
private final class ReceiptProgressObserver {
    weak var synchronizer: CloudKitSynchronizer?
    let cancelFirst: Bool
    var terminalCount = 0
    var firstAttempt: UUID?
    init(cancelFirst: Bool) { self.cancelFirst = cancelFirst }
    func record(_ milestone: String) {
        guard milestone == "terminal-receipt", let synchronizer else { return }
        terminalCount += 1
        guard terminalCount == 1 else { return }
        firstAttempt = synchronizer.synchronizationAttemptID
        // Synchronous reentry is essential. Scheduling a Task would hide the
        // old-run cleanup race by running only after the original tail returns.
        if cancelFirst { synchronizer.cancelSynchronization() }
        synchronizer.beginSynchronization()
    }
}

/// Invoked synchronously by the actor-isolated synchronizer, including the
/// selector notification callback; no Task hop may hide the reentry boundary.
@BigSyncBackgroundActor
private final class ReceiptSuccessObserver: NSObject, @preconcurrency CloudKitSynchronizerDelegate {
    var diagnostics = 0
    var notifications = 0
    var completions = 0
    var reenterOnNotification = false
    var reenter: (@BigSyncBackgroundActor () -> Void)?
    @objc func receiveSuccessNotification(_ notification: Notification) {
        notifications += 1
        if reenterOnNotification { reenter?() }
    }
    func synchronizerWillFetchChanges(_ synchronizer: CloudKitSynchronizer, in recordZone: CKRecordZone.ID) {}
    func synchronizerWillUploadChanges(_ synchronizer: CloudKitSynchronizer, to recordZone: CKRecordZone.ID) {}
    func synchronizerDidSync(_ synchronizer: CloudKitSynchronizer) { completions += 1 }
    func synchronizerDidfailToSync(_ synchronizer: CloudKitSynchronizer, error: Error) {}
    func synchronizer(_ synchronizer: CloudKitSynchronizer, zoneIDWasDeleted zoneID: CKRecordZone.ID) {}
}


extension CloudKitTerminalReceiptTests {
    @BigSyncBackgroundActor
    func testFailureFenceAtNotificationReleasesAllWaiters() async throws {
        try await assertFailureHealthOwnership(retryable: false, replaceAtHealth: false)
    }

    @BigSyncBackgroundActor
    func testRetryableFailureFenceAtNotificationReleasesAllWaiters() async throws {
        try await assertFailureHealthOwnership(retryable: true, replaceAtHealth: false)
    }

    @BigSyncBackgroundActor
    func testFailureHealthNotificationPreservesSuccessorDrain() async throws {
        try await assertFailureHealthOwnership(retryable: false, replaceAtHealth: true)
    }

    @BigSyncBackgroundActor
    func testRetryableFailureHealthNotificationPreservesSuccessorDrain() async throws {
        try await assertFailureHealthOwnership(retryable: true, replaceAtHealth: true)
    }

    @BigSyncBackgroundActor
    private func assertFailureHealthOwnership(retryable: Bool, replaceAtHealth: Bool) async throws {
        let fixture = Fixture()
        let synchronizer = fixture.synchronizer
        let observer = ReceiptFailureHealthObserver()
        let entered = ReceiptPause(), release = ReceiptPause(), successorRelease = ReceiptPause()
        var calls = 0
        var successorEntered = false
        var firstResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        var secondResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        var successorResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        var successorTask: Task<Void, Never>?
        // Preserve retry semantics without spending the five-second default
        // fallback budget before the successor reaches our held transport.
        let originalError = retryable
            ? CKError(.networkFailure, userInfo: [CKErrorRetryAfterKey: 0])
            : CKError(.permissionFailure)
        observer.reenter = { [weak synchronizer] in
            guard let synchronizer else { return }
            if replaceAtHealth {
                synchronizer.cancelSynchronization()
                synchronizer.beginSynchronization()
                successorTask = synchronizer.synchronizationTask
            } else {
                // Models the immediate account fence before queued cancellation
                // has a chance to rotate the logical synchronization attempt.
                synchronizer.accountScopeAuthorityFence.poison()
            }
        }
        observer.atHealth = replaceAtHealth
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptFailureHealthObserver.receiveFailure(_:)),
            name: .SynchronizerDidFailToSynchronize, object: synchronizer)
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptFailureHealthObserver.receiveHealth(_:)),
            name: .SynchronizerSyncHealthDidChange, object: synchronizer)
        fixture.transport.databaseChangesHook = {
            calls += 1
            if calls == 1 {
                await entered.release()
                await release.wait()
                throw originalError
            }
            if calls == 2 {
                successorEntered = true
                await successorRelease.wait()
            }
        }
        let first = Task { @BigSyncBackgroundActor in
            do { firstResult = .success(try await synchronizer.synchronize()) }
            catch { firstResult = .failure(error) }
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
            observer.reenter = nil
            synchronizer.cancelSynchronization()
            first.cancel()
            Task { await release.release(); await successorRelease.release() }
        }
        await entered.wait()
        let second = Task { @BigSyncBackgroundActor in
            do { secondResult = .success(try await synchronizer.synchronize()) }
            catch { secondResult = .failure(error) }
        }
        defer { second.cancel() }
        try await waitFor { synchronizer._testSynchronizationWaiterCount == 2 }
        await release.release()
        try await waitFor { observer.reentries == 1 }
        try await waitFor { firstResult != nil && secondResult != nil }
        for result in [firstResult, secondResult] {
            guard case .failure(let error) = try XCTUnwrap(result) else {
                XCTFail("A fenced failure must not deliver success")
                continue
            }
            XCTAssertTrue(error is CancellationError)
        }
        if replaceAtHealth {
            // A stale retry can leave a nonnil task but replace B's actual task.
            // Compare the exact task captured synchronously by the observer.
            XCTAssertNotNil(successorTask)
            XCTAssertEqual(synchronizer.synchronizationTask, successorTask)
            try await waitFor { successorEntered }
            XCTAssertTrue(synchronizer.syncing)
            XCTAssertNotNil(synchronizer.synchronizationTask)
            let successorAttempt = synchronizer.synchronizationAttemptID
            let successor = Task { @BigSyncBackgroundActor in
                do { successorResult = .success(try await synchronizer.synchronize()) }
                catch { successorResult = .failure(error) }
            }
            defer { successor.cancel() }
            try await waitFor { synchronizer._testSynchronizationWaiterCount == 1 }
            XCTAssertEqual(synchronizer.synchronizationAttemptID, successorAttempt)
            await successorRelease.release()
            try await waitFor { successorResult != nil }
            let result = try XCTUnwrap(successorResult).get()
            XCTAssertEqual(result.publicationState, .complete)
            // Joining a live drain can legitimately request a tail attempt.
            // Ownership was checked while B was suspended, not after its tail.
            XCTAssertNil(synchronizer.retrySleepUntil)
        } else {
            XCTAssertTrue(synchronizer.cancelSync)
            XCTAssertFalse(synchronizer.syncing)
            XCTAssertNil(synchronizer.synchronizationTask)
            XCTAssertNil(synchronizer.retrySleepUntil)
        }
        XCTAssertEqual(synchronizer._testSynchronizationWaiterCount, 0)
    }
}

@BigSyncBackgroundActor
private final class ReceiptFailureHealthObserver: NSObject {
    var atHealth = false
    var failureSeen = false
    var reentries = 0
    var reenter: (@BigSyncBackgroundActor () -> Void)?
    @objc func receiveFailure(_ notification: Notification) {
        failureSeen = true
        if !atHealth { invokeOnce() }
    }
    @objc func receiveHealth(_ notification: Notification) {
        guard atHealth, failureSeen else { return }
        invokeOnce()
    }
    private func invokeOnce() {
        guard let callback = reenter else { return }
        reenter = nil
        reentries += 1
        callback()
    }
}

// Native composition of the real synchronizer/worker with the production
// filesystem gate. Injected account/transport only; no live CloudKit data.
extension CloudKitTerminalReceiptTests {
    /// Compatibility fixture for an abandoned checkpoint written by an older
    /// client. RA-1 never establishes a Reader cutoff or source-publication phase.
    @BigSyncBackgroundActor
    private func abandonedLegacyCheckpoint(_ fixture: Fixture) async throws -> BigSyncOutboundQuiescenceSnapshot {
        _ = try await fixture.drain()
        let gate = fixture.synchronizer.outboundQuiescenceCoordinator
        var oldOwner: BigSyncOutboundQuiescenceLease? = try gate.begin(
            principal: fixture.synchronizer.currentOutboundPrincipal(),
            writerBarrierEvidenceID: "TEST-ONLY-abandoned-legacy-checkpoint")
        XCTAssertNotNil(oldOwner)
        oldOwner = nil // Drop OS ownership, not the persisted uncertainty.
        return try gate.snapshot()
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementDuringRecoveryProofCannotReopenOldGate() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let expected = try await abandonedLegacyCheckpoint(fixture)
        let worker = BigSyncBackgroundActor()
        let replacement = Fixture(useReplicaBinding: true)
        await worker._test_installSynchronizer(fixture.synchronizer)
        await assertRejected {
            try await worker.recoverOutboundQuiescence(expected: expected) { _ in
                fixture.injected = true
                await worker._test_installSynchronizer(replacement.synchronizer)
                return "TEST-ONLY-domain-proof"
            }
        }
        XCTAssertTrue(fixture.injected, "A prior rejection must not masquerade as owner fencing")
        XCTAssertEqual(try fixture.synchronizer.outboundQuiescenceSnapshot(), expected)
        XCTAssertNil(try replacement.synchronizer.outboundQuiescenceSnapshot().barrier)
    }

}


extension CloudKitTerminalReceiptTests {
    @BigSyncBackgroundActor
    func testOrdinaryUploadAndDeleteRetainPhysicalSubmissionThroughAcknowledgement() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        _ = try await fixture.drain()
        let gate = fixture.synchronizer.outboundQuiescenceCoordinator
        let principal = try fixture.synchronizer.currentOutboundPrincipal()
        let record = CKRecord(recordType: "Item", recordID: .init(
            recordName: "Item.one", zoneID: fixture.adapter.recordZoneID))
        fixture.adapter.preparedUploads = [.init(record: record, generation: "generation-one")]
        fixture.adapter.preparedDeletions = [.init(recordID: .init(
            recordName: "Item.deleted", zoneID: fixture.adapter.recordZoneID), generation: "delete-generation")]
        fixture.transport.mutationHook = {
            XCTAssertNil(try gate.snapshot().barrier)
            XCTAssertEqual(try gate.snapshot().outstandingSubmissions.count, 1)
            XCTAssertThrowsError(try gate.takeRecoveryOwnership(expected: gate.snapshot()))
        }
        fixture.adapter.acknowledgementHook = {
            XCTAssertNil(try gate.snapshot().barrier)
            XCTAssertEqual(try gate.snapshot().outstandingSubmissions.count, 1)
            XCTAssertThrowsError(try gate.takeRecoveryOwnership(expected: gate.snapshot()))
        }
        let receipt = try await fixture.drain()
        XCTAssertEqual(fixture.transport.mutationCalls, 2)
        XCTAssertEqual(fixture.adapter.acknowledgements, 2)
        XCTAssertTrue(fixture.adapter.preparedUploads.isEmpty)
        XCTAssertTrue(fixture.adapter.preparedDeletions.isEmpty)
        XCTAssertNil(try gate.snapshot().barrier)
        XCTAssertTrue(try gate.snapshot().outstandingSubmissions.isEmpty)
        try await fixture.synchronizer.revalidateTerminalReceipt(receipt)
        _ = try gate.admit(principal: principal)
    }

    @BigSyncBackgroundActor
    func testOrdinaryPreparationRevocationCannotSubmitOrAcknowledgePreparedGeneration() async throws {
        enum Revocation { case cancellation, accountFence, bindingReplacement }
        for revocation: Revocation in [.cancellation, .accountFence, .bindingReplacement] {
            let fixture = Fixture(useReplicaBinding: true)
            _ = try await fixture.drain()
            let record = CKRecord(recordType: "Item", recordID: .init(
                recordName: "Item.one", zoneID: fixture.adapter.recordZoneID))
            fixture.adapter.preparedUploads = [.init(record: record, generation: "unsubmitted-generation")]
            fixture.adapter.preparationHook = {
                fixture.adapter.preparationHook = nil
                fixture.injected = true
                switch revocation {
                case .cancellation: fixture.synchronizer.cancelSynchronization()
                case .accountFence: fixture.synchronizer.accountScopeAuthorityFence.poison()
                case .bindingReplacement: try fixture.replacePersistedBinding()
                }
            }
            await assertRejected { _ = try await fixture.synchronizer.synchronize() }
            XCTAssertTrue(fixture.injected, "Must revoke after actual preparation entry")
            XCTAssertEqual(fixture.transport.mutationCalls, 0)
            XCTAssertEqual(fixture.adapter.acknowledgements, 0)
            XCTAssertEqual(fixture.adapter.preparedUploads.first?.generation, "unsubmitted-generation")
            XCTAssertTrue(try fixture.synchronizer.outboundQuiescenceSnapshot().outstandingSubmissions.isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementAtFinalRecoveryAccountLookupKeepsFence() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let checkpoint = try await abandonedLegacyCheckpoint(fixture)
        let worker = BigSyncBackgroundActor()
        let replacement = Fixture(useReplicaBinding: true)
        await worker._test_installSynchronizer(fixture.synchronizer)
        await assertRejected {
            try await worker.recoverOutboundQuiescence(expected: checkpoint) { _ in
                await fixture.account.onNextRead { @BigSyncBackgroundActor in
                    fixture.injected = true
                    await worker._test_installSynchronizer(replacement.synchronizer)
                }
                return "TEST-ONLY-proof-before-final-account-await"
            }
        }
        XCTAssertTrue(fixture.injected, "Must reach the final account lookup")
        XCTAssertEqual(try fixture.synchronizer.outboundQuiescenceSnapshot(), checkpoint)
    }

}
