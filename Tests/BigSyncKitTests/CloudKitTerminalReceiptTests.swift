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
        try await first.synchronizer.revalidateTerminalReceipt(receipt)
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
    func testPostBarrierCapabilityValidatesOnUnchangedWorker() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await worker.completedPostBarrierDrain(using: receipt, authorizedBy: authorization)
        let operations = fixture.transport.operations
        let writes = fixture.store.writes
        try await worker.revalidateCompletedPostBarrierDrain(completed)
        XCTAssertEqual(fixture.transport.operations, operations)
        XCTAssertEqual(fixture.store.writes, writes)
    }

    @BigSyncBackgroundActor
    func testPostBarrierCompletionRejectsWorkerReplacementDuringAccountRead() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let replacement = Fixture(useReplicaBinding: true)
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        await fixture.account.onNextRead {
            await worker._test_installSynchronizer(replacement.synchronizer)
        }
        await assertRejected {
            _ = try await worker.completedPostBarrierDrain(using: receipt, authorizedBy: authorization)
        }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testCompletedPostBarrierRevalidationRejectsWorkerReplacementDuringAccountRead() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let replacement = Fixture(useReplicaBinding: true)
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await worker.completedPostBarrierDrain(using: receipt, authorizedBy: authorization)
        await fixture.account.onNextRead {
            await worker._test_installSynchronizer(replacement.synchronizer)
        }
        await assertRejected { try await worker.revalidateCompletedPostBarrierDrain(completed) }
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testNewPendingEditRejectsCompletedPostBarrierCapability() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await fixture.synchronizer.completedPostBarrierDrain(
            using: receipt, authorizedBy: authorization)
        fixture.adapter.hasPendingTerminalChanges = true
        await assertRejected { try await fixture.synchronizer.revalidateCompletedPostBarrierDrain(completed) }
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
    }

    @BigSyncBackgroundActor
    func testPendingEditDuringAccountAwaitRejectsCompletedPostBarrierCapability() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await fixture.synchronizer.completedPostBarrierDrain(
            using: receipt, authorizedBy: authorization)
        await fixture.account.onNextRead {
            await fixture.introducePendingEdit()
        }
        await assertRejected { try await fixture.synchronizer.revalidateCompletedPostBarrierDrain(completed) }
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
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
    func testPostBootstrapPrincipalAllowsNewWorkButDoesNotRenewAggregateProof() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await fixture.synchronizer.completedPostBarrierDrain(
            using: receipt, authorizedBy: authorization)
        // Bootstrap legitimately creates source journals. The principal is not
        // an empty-journal capability and must not acknowledge those writes.
        fixture.adapter.hasPendingTerminalChanges = true
        let writes = fixture.store.writes
        try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed)
        await assertRejected { try await fixture.synchronizer.revalidateCompletedPostBarrierDrain(completed) }
        XCTAssertEqual(fixture.store.writes, writes)
        XCTAssertEqual(fixture.adapter.acknowledgements, 0)
        XCTAssertTrue(fixture.adapter.hasPendingTerminalChanges)
    }

    @BigSyncBackgroundActor
    func testPostBootstrapPrincipalRejectsBindingReplacementDuringAccountRead() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await fixture.synchronizer.completedPostBarrierDrain(
            using: receipt, authorizedBy: authorization)
        await fixture.account.onNextRead { try await fixture.replacePersistedBinding() }
        await assertRejected { try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed) }
    }

    @BigSyncBackgroundActor
    func testPostBootstrapPrincipalRejectsWorkerReplacementDuringAccountRead() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let successor = Fixture(useReplicaBinding: true)
        let worker = BigSyncBackgroundActor()
        await worker._test_installSynchronizer(fixture.synchronizer)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await worker.completedPostBarrierDrain(using: receipt, authorizedBy: authorization)
        await fixture.account.onNextRead { await worker._test_installSynchronizer(successor.synchronizer) }
        await assertCancelled { try await worker.revalidatePostBarrierDrainPrincipal(completed) }
    }

    @BigSyncBackgroundActor
    func testPostBootstrapPrincipalRejectsNextRunAndCancellation() async throws {
        let fixture = Fixture(useReplicaBinding: true)
        let (receipt, authorization) = try await fixture.postBarrierDrain()
        let completed = try await fixture.synchronizer.completedPostBarrierDrain(
            using: receipt, authorizedBy: authorization)
        _ = try await fixture.drain()
        await assertRejected { try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed) }
        fixture.synchronizer.cancelSynchronization()
        await assertCancelled { try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed) }
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
             identifier: String = UUID().uuidString, zoneID: CKRecordZone.ID? = nil) {
            self.store = store; self.account = account; self.identifier = identifier
            self.useReplicaBinding = useReplicaBinding; self.domainScopeIdentifier = domainScopeIdentifier
            let zone = zoneID ?? CKRecordZone.ID(zoneName: "receipt-fixture-\(UUID().uuidString)",
                ownerName: CKCurrentUserDefaultName)
            adapter = ReceiptAdapter(zoneID: zone)
            synchronizer = CloudKitSynchronizer(identifier: identifier,
                containerIdentifier: "iCloud.receipt-fixture", database: transport,
                recordZoneID: zone, keyValueStore: store,
                accountIdentifierProvider: { try await account.read() },
                accountStatusProvider: { .available }, changeFeed: transport,
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


        func postBarrierDrain() async throws -> (
            CloudKitSynchronizer.SynchronizationReceipt,
            CloudKitSynchronizer.PostBarrierDrainAuthorization
        ) {
            // Establish real account/binding state through the normal drain.
            _ = try await drain()
            synchronizer.postBarrierSnapshotIdentifierProvider = { "snapshot-scope" }
            let authorization = try synchronizer.establishPostBarrierDrain(
                writerBarrierEvidenceID: "test-barrier")
            return (try await drain(), authorization)
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
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        operations += 1; return .init(saveResults: [:], deleteResults: [:])
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
    func preparedRecordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordUpload] { [] }
    func didUpload(savedRecords: [CKRecord], matchingGenerations: [String: String]) async throws { acknowledgements += 1 }
    func preparedRecordDeletions(limit: Int, restrictedToEntityType: String?) async throws -> [PreparedRecordDeletion] { [] }
    func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws { acknowledgements += 1 }
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
    func hasPendingChangesAtTerminalBoundary() throws -> Bool { hasPendingTerminalChanges }
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
