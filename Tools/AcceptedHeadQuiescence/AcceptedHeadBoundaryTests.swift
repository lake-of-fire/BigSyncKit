import Foundation
import XCTest
@testable import AcceptedHeadBoundary

private enum ProbeError: Error { case unaccepted, revoked }
@BigSyncBackgroundActor private final class Probe {
    let synchronizer = CloudKitSynchronizer()
    var domainCurrent = true
    var proofCalls = 0
    var task: Task<BigSyncOutboundQuiescenceSnapshot, Error>?
    func validate() throws { if !domainCurrent { throw ProbeError.revoked } }
    func proof(_ snapshot: BigSyncOutboundQuiescenceSnapshot) throws {
        try validate()
        proofCalls += 1
        XCTAssertEqual(snapshot.barrier?.phase, .preparing)
        XCTAssertFalse(snapshot.hasUnknownSubmissions)
    }
}

final class AcceptedHeadBoundaryTests: XCTestCase {
    @BigSyncBackgroundActor
    private func rejected(_ body: () async throws -> Void) async {
        do { try await body(); XCTFail("Obsolete or unproved request was admitted") }
        catch { }
    }
    @BigSyncBackgroundActor
    func testAcceptedHeadSealsWithoutAggregateDrainOrSourceGrant() async throws {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let result = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
            revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        XCTAssertEqual(result.barrier?.phase, .recoveryRequired)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 1)
        XCTAssertEqual(p.proofCalls, 1)
        XCTAssertEqual(s.accountReads, 2)
        XCTAssertNil(s.postBarrierDrainAuthorization)
        XCTAssertNil(s.completedPostBarrierDrain)
        XCTAssertNil(s.postBarrierOutboundEstablishmentID)
        XCTAssertEqual(s.abandonments, 0)
    }
    @BigSyncBackgroundActor
    func testRejectedHeadProofPreservesPreparingFence() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let before = s.outboundQuiescenceCoordinator.state
        await rejected {
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() },
                authorizingAcceptedHead: { _ in throw ProbeError.unaccepted })
        }
        XCTAssertEqual(s.outboundQuiescenceCoordinator.state, before)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
    }
    @BigSyncBackgroundActor
    func testUnknownSubmissionNeverReachesHostProofOrSeal() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        s.outboundQuiescenceCoordinator.state.hasUnknownSubmissions = true
        await rejected {
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        }
        XCTAssertEqual(p.proofCalls, 0)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
        XCTAssertTrue(s.outboundQuiescenceCoordinator.state.hasUnknownSubmissions)
    }
    @BigSyncBackgroundActor
    func testIncompatibleEntryStateNeverCallsHost() async {
        for kind in 0..<6 {
            let p = Probe(), s: CloudKitSynchronizer
            s = p.synchronizer
            switch kind {
            case 0: s.syncing = true
            case 1: s.synchronizationDrainIsActive = true
            case 2: s.postBarrierDrainAuthorization = UUID()
            case 3: s.completedPostBarrierDrain = UUID()
            case 4: s.outboundRecoveryID = UUID()
            default: s.postBarrierOutboundEstablishmentID = UUID()
            }
            let previousEstablishment = s.postBarrierOutboundEstablishmentID
            await rejected {
                _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                    revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
            }
            XCTAssertEqual(p.proofCalls, 0)
            XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
            XCTAssertEqual(s.postBarrierOutboundEstablishmentID, previousEstablishment)
        }
    }
    @BigSyncBackgroundActor
    func testCancellationBeforeEntryNeverSeals() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let t = Task { @BigSyncBackgroundActor in
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        }
        t.cancel()
        await rejected { try await t.value }
        XCTAssertEqual(p.proofCalls, 0)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
    }
    @BigSyncBackgroundActor
    func testNoncooperativeProofCancellationNeverSeals() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        p.task = Task { @BigSyncBackgroundActor in
            try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { snapshot in
                    try p.proof(snapshot)
                    p.task?.cancel() // deliberately returns normally
                })
        }
        await rejected { _ = try await p.task!.value }
        XCTAssertEqual(p.proofCalls, 1)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
        p.task = nil
    }
    @BigSyncBackgroundActor
    func testFinalAccountAwaitCancellationNeverSeals() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        s.accountHook = { read in if read == 2 { p.task?.cancel() } }
        p.task = Task { @BigSyncBackgroundActor in
            try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        }
        await rejected { _ = try await p.task!.value }
        XCTAssertEqual(p.proofCalls, 1)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
        p.task = nil
        s.accountHook = nil
    }
    @BigSyncBackgroundActor
    func testEverySuspensionRejectsDomainRevocation() async {
        for boundary in 0..<4 {
            let p = Probe(), s: CloudKitSynchronizer
            s = p.synchronizer
            s.outboundQuiescenceCoordinator.waitHook = { if boundary == 0 { p.domainCurrent = false } }
            s.accountHook = { read in if read == 1 && boundary == 1 || read == 2 && boundary == 3 { p.domainCurrent = false } }
            await rejected {
                _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                    revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { snapshot in
                        try p.proof(snapshot)
                        if boundary == 2 { p.domainCurrent = false }
                    })
            }
            XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
            s.accountHook = nil
            s.outboundQuiescenceCoordinator.waitHook = nil
        }
    }
    @BigSyncBackgroundActor
    func testFinalAwaitRejectsAccountBindingAttemptAndTokenReplacement() async {
        for kind in 0..<4 {
            let p = Probe(), s: CloudKitSynchronizer
            s = p.synchronizer
            s.accountHook = { read in
                if read == 2 {
                    switch kind {
                    case 0: s.account = "account-B"
                    case 1: s.principal = .init(accountScopeIdentifier: "account-A", bindingID: "binding-B")
                    case 2: s.synchronizationAttemptID = UUID()
                    default: s.ticket = .init(identifier: s.ticket.identifier, principal: s.principal)
                    }
                }
            }
            await rejected {
                _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                    revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
            }
            XCTAssertEqual(p.proofCalls, 1)
            XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
            s.accountHook = nil
        }
    }
    @BigSyncBackgroundActor
    func testFirstActualAccountMismatchDoesNotCallHostProof() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        s.account = "account-B"
        await rejected {
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        }
        XCTAssertEqual(p.proofCalls, 0)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
    }
    @BigSyncBackgroundActor
    func testCheckpointChangeDuringProofNeverSealsNewerState() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        await rejected {
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { snapshot in
                    try p.proof(snapshot)
                    s.outboundQuiescenceCoordinator.state.revision = UUID()
                })
        }
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
    }
    @BigSyncBackgroundActor
    func testStaleTaskDoesNotClearSuccessorEstablishment() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let successor = UUID()
        await rejected {
            _ = try await s.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { _ in
                    s.postBarrierOutboundEstablishmentID = successor
                })
        }
        XCTAssertEqual(s.postBarrierOutboundEstablishmentID, successor)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
    }
    @BigSyncBackgroundActor
    func testWorkerReplacementAtLastAccountReadNeverSealsEitherOwner() async {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let worker = BigSyncBackgroundActor(), next = CloudKitSynchronizer()
        worker.realmSynchronizer = s
        s.accountHook = { read in if read == 2 { worker.realmSynchronizer = next } }
        await rejected {
            _ = try await worker.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
                revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        }
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 0)
        XCTAssertEqual(next.outboundQuiescenceCoordinator.seals, 0)
        XCTAssertEqual(next.abandonments, 0)
        s.accountHook = nil
    }
    @BigSyncBackgroundActor
    func testWorkerSuccessReturnsSameExactRecoveryCheckpoint() async throws {
        let p = Probe(), s: CloudKitSynchronizer
        s = p.synchronizer
        let worker = BigSyncBackgroundActor()
        worker.realmSynchronizer = s
        let result = try await worker.sealPostBarrierQuiescenceForAcceptedHead(s.ticket,
            revalidatingDomainOwner: { try p.validate() }, authorizingAcceptedHead: { try p.proof($0) })
        XCTAssertEqual(result, s.outboundQuiescenceCoordinator.state)
        XCTAssertEqual(s.outboundQuiescenceCoordinator.seals, 1)
        XCTAssertNil(s.postBarrierDrainAuthorization)
        XCTAssertNil(s.completedPostBarrierDrain)
    }
}
