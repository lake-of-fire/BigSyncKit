import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

// Native restoration histories exercise the final synchronous adapter callout.
// The fixture hook is nil by default and does not alter preceding tests.
extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    private func publicationTailFixture() async throws -> (CloudKitSynchronizer, AccountFencingModelAdapter) {
        let sync = makeSynchronizer(
            transport: AccountFencingTransport(), store: AccountFencingStore(),
            identifier: "publication-tail-\(UUID().uuidString)", recordZoneID: makeZoneID(),
            accountReplacementPolicy: .localDatasetRebootstrap,
            initialReplicaBindingAdmissionHandler: { _ in })
        let adapter = AccountFencingModelAdapter(zoneID: sync.recordZoneID)
        adapter.consumedBoundaryIdentifier = "publication-tail-boundary"
        adapter.feedEpoch = 7
        sync.addModelAdapter(adapter)
        sync.domainPublicationScopeIdentifierProvider = { "publication-tail-domain" }
        addTeardownBlock { @BigSyncBackgroundActor in
            adapter.publicationEpochInspectionHook = nil
            await sync.cancelSynchronizationAndWait()
        }
        let result = try await sync.synchronize()
        XCTAssertNotNil(result.receipt)
        let baseline = try await sync.restoredDurablePublicationEvidence()
        XCTAssertNotNil(baseline)
        return (sync, adapter)
    }

    @BigSyncBackgroundActor
    func testFinalPublicationInspectionRejectsReentrantFencePoison() async throws {
        let (sync, adapter) = try await publicationTailFixture()
        adapter.publicationEpochInspectionHook = {
            sync.accountScopeAuthorityFence.poison()
        }
        let evidence = try await sync.restoredDurablePublicationEvidence()
        XCTAssertNil(evidence)
        XCTAssertTrue(sync.accountScopeAuthorityFence.rejectsAuthority)
    }

    @BigSyncBackgroundActor
    func testFinalPublicationInspectionCannotDeliverRevokedPositiveEvidence() async throws {
        let (sync, adapter) = try await publicationTailFixture()
        adapter.publicationEpochInspectionHook = {
            sync.accountScopeAuthorityFence.poison()
        }
        let delivery = ClosureRestorationDelivery()
        try await sync.restoreDurablePublicationEvidence { evidence in
            await delivery.receive(evidence)
        }
        let count = await delivery.count
        let sawNil = await delivery.sawNil
        XCTAssertEqual(count, 1)
        XCTAssertTrue(sawNil)
    }

    @BigSyncBackgroundActor
    func testFinalPublicationInspectionPreservesTaskCancellation() async throws {
        let (sync, adapter) = try await publicationTailFixture()
        adapter.publicationEpochInspectionHook = {
            withUnsafeCurrentTask { $0?.cancel() }
        }
        let inspection = Task { @BigSyncBackgroundActor in
            try await sync.restoredDurablePublicationEvidence()
        }
        do {
            _ = try await inspection.value
            XCTFail("Cancellation during the final adapter predicate must still throw")
        } catch is CancellationError {}
    }

    @BigSyncBackgroundActor
    func testFinalPublicationInspectionRetainsUnchangedOwnerSuccess() async throws {
        let (sync, adapter) = try await publicationTailFixture()
        adapter.publicationEpochInspectionHook = {}
        let evidence = try await sync.restoredDurablePublicationEvidence()
        XCTAssertEqual(evidence?.consumedServerBoundaryIdentifier, "publication-tail-boundary")
        XCTAssertEqual(evidence?.changeFeedEpoch, 7)
    }
}

private final class AccountFencingStore:
    NSObject,
    KeyValueStore,
    @unchecked Sendable {
    private var values = [String: Any]()
    var synchronizesDurably = true
    var undurableKeySubstring: String?
    private var lastMutatedKey: String?

    func object(forKey defaultName: String) -> Any? { values[defaultName] }
    func bool(forKey defaultName: String) -> Bool {
        values[defaultName] as? Bool ?? false
    }
    @BigSyncBackgroundActor
    var publicationEpochInspectionHook: (() -> Void)?
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? {
        publicationEpochInspectionHook?()
        return feedEpoch
    }
    func didFinishImport() async throws {
        if requestsOneUploadWakeupOnFinish {
            requestsOneUploadWakeupOnFinish = false
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        hasPendingTerminalChanges
    }
}
