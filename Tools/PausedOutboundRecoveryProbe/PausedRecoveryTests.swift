import Foundation
import XCTest
@testable import BigSyncKit

final class PausedOutboundRecoveryTests: XCTestCase {
    enum Failure: Error { case rejected }

    @BigSyncBackgroundActor
    final class Authority {
        var valid = true
        var checks = 0
        func check() throws {
            checks += 1
            guard valid else { throw BigSyncOutboundQuiescenceError.staleAuthority }
        }
    }

    private struct Fixture {
        let base: URL
        let principal: BigSyncOutboundPrincipal
        let expected: BigSyncOutboundQuiescenceSnapshot
    }

    @BigSyncBackgroundActor
    private func sealedFixture() async throws -> Fixture {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("PausedOutboundRecovery-\(UUID().uuidString)")
        let principal = BigSyncOutboundPrincipal(
            durableStateNamespace: "probe",
            installationIdentifier: "install",
            accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding",
            accountInvalidationGeneration: 1
        )
        let gate = BigSyncOutboundQuiescenceCoordinator(
            sharedStateBaseURL: base, durableStateNamespace: "probe")
        var owner: BigSyncOutboundQuiescenceLease? = try gate.begin(
            principal: principal, writerBarrierEvidenceID: "writer-proof")
        try await gate.waitUntilDrained(XCTUnwrap(owner))
        try gate.requireRecovery(XCTUnwrap(owner))
        let expected = try gate.snapshot()
        XCTAssertEqual(expected.barrier?.phase, .recoveryRequired)
        owner = nil
        return .init(base: base, principal: principal, expected: expected)
    }

    @BigSyncBackgroundActor
    private func observer(_ fixture: Fixture) -> BigSyncOutboundQuiescenceCoordinator {
        .init(sharedStateBaseURL: fixture.base, durableStateNamespace: "probe")
    }

    @BigSyncBackgroundActor
    private func assertDurableFenceUnchanged(
        _ fixture: Fixture, file: StaticString = #filePath, line: UInt = #line
    ) throws {
        let observer = observer(fixture)
        XCTAssertEqual(try observer.snapshot(), fixture.expected, file: file, line: line)
        XCTAssertThrowsError(try observer.admit(principal: fixture.principal), file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testPausedRecoveryRetainsRecoveryPhaseAndAdmitsNoOutboundBatch() async throws {
        let fixture = try await sealedFixture()
        defer { try? FileManager.default.removeItem(at: fixture.base) }
        let synchronizer = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        let authority = Authority()
        let token = try await synchronizer.acquirePostBarrierRecoveryOwnership(
            expected: fixture.expected,
            revalidatingDomainOwner: authority.check
        ) { exact in
            XCTAssertEqual(exact, fixture.expected)
            return "settled-reservation"
        }
        let live = try synchronizer.validatePostBarrierOutboundQuiescence(token)
        XCTAssertEqual(live.barrier?.phase, .recoveryRequired)
        XCTAssertEqual(live.barrier?.identifier, fixture.expected.barrier?.identifier)
        XCTAssertGreaterThanOrEqual(authority.checks, 4)
        XCTAssertThrowsError(try synchronizer.outboundQuiescenceCoordinator.admit(
            principal: fixture.principal,
            owner: XCTUnwrap(synchronizer.postBarrierOutboundLease)
        ))
        XCTAssertThrowsError(try observer(fixture).admit(principal: fixture.principal))
        XCTAssertTrue(synchronizer.abandonPostBarrierOutboundQuiescence(token))

        // Successful recovery is not byte-identical to its input checkpoint:
        // retaining paused ownership durably records the recovery evidence and
        // therefore advances the state revision. The barrier identity/phase and
        // empty uncertainty set are preserved, and peers remain fenced.
        let after = try observer(fixture).snapshot()
        XCTAssertEqual(after.barrier, fixture.expected.barrier)
        XCTAssertTrue(after.outstandingSubmissions.isEmpty)
        XCTAssertEqual(after.lastRecoveryEvidenceID, "settled-reservation")
        XCTAssertNotEqual(after.revisionIdentifier, fixture.expected.revisionIdentifier)
        XCTAssertThrowsError(try observer(fixture).admit(principal: fixture.principal))
    }

    @BigSyncBackgroundActor
    func testRejectedDomainProofReleasesPhysicalRecoveryLocksButPreservesFence() async throws {
        let fixture = try await sealedFixture()
        defer { try? FileManager.default.removeItem(at: fixture.base) }
        let first = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        do {
            _ = try await first.acquirePostBarrierRecoveryOwnership(
                expected: fixture.expected, revalidatingDomainOwner: {}
            ) { _ in throw Failure.rejected }
            XCTFail("Rejected proof acquired paused owner")
        } catch Failure.rejected { }
        XCTAssertNil(first.postBarrierOutboundLease)
        XCTAssertNil(first.postBarrierOutboundTicket)
        try assertDurableFenceUnchanged(fixture)

        // The failed proof must not strand owner/batch locks. A new explicit
        // recovery acquisition can take physical ownership of the same durable gate.
        let second = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        let token = try await second.acquirePostBarrierRecoveryOwnership(
            expected: fixture.expected, revalidatingDomainOwner: {}
        ) { _ in "replacement-proof" }
        XCTAssertTrue(second.abandonPostBarrierOutboundQuiescence(token))
    }

    @BigSyncBackgroundActor
    func testDomainRevocationAfterProofCannotReturnLiveOwner() async throws {
        let fixture = try await sealedFixture()
        defer { try? FileManager.default.removeItem(at: fixture.base) }
        let synchronizer = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        let authority = Authority()
        do {
            _ = try await synchronizer.acquirePostBarrierRecoveryOwnership(
                expected: fixture.expected,
                revalidatingDomainOwner: authority.check
            ) { _ in
                authority.valid = false
                return "obsolete-proof"
            }
            XCTFail("Revoked domain owner was returned")
        } catch let error as BigSyncOutboundQuiescenceError {
            XCTAssertEqual(error, .staleAuthority)
        }
        XCTAssertNil(synchronizer.postBarrierOutboundLease)
        XCTAssertNil(synchronizer.postBarrierOutboundTicket)
        try assertDurableFenceUnchanged(fixture)
    }

    @BigSyncBackgroundActor
    func testAccountReplacementOnFinalReadRejectsPausedOwner() async throws {
        let fixture = try await sealedFixture()
        defer { try? FileManager.default.removeItem(at: fixture.base) }
        let synchronizer = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        synchronizer.replaceAccountOnRead = 2
        do {
            _ = try await synchronizer.acquirePostBarrierRecoveryOwnership(
                expected: fixture.expected, revalidatingDomainOwner: {}
            ) { _ in "proof-before-account-replacement" }
            XCTFail("Changed account returned paused owner")
        } catch let error as BigSyncOutboundQuiescenceError {
            XCTAssertEqual(error, .staleAuthority)
        }
        XCTAssertEqual(synchronizer.accountReads, 2)
        XCTAssertNil(synchronizer.postBarrierOutboundLease)
        try assertDurableFenceUnchanged(fixture)
    }

    @BigSyncBackgroundActor
    func testWorkerReplacementDuringProofCannotDeliverOldPausedOwner() async throws {
        let fixture = try await sealedFixture()
        defer { try? FileManager.default.removeItem(at: fixture.base) }
        let original = CloudKitSynchronizer(directory: fixture.base, principal: fixture.principal)
        let replacementBase = FileManager.default.temporaryDirectory
            .appendingPathComponent("PausedOutboundReplacement-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: replacementBase) }
        let replacement = CloudKitSynchronizer(directory: replacementBase, principal: fixture.principal)
        let worker = BigSyncBackgroundActor.shared
        worker.install(original)
        do {
            _ = try await worker.acquirePostBarrierRecoveryOwnership(
                expected: fixture.expected, revalidatingDomainOwner: {}
            ) { _ in
                worker.install(replacement)
                return "proof-from-displaced-worker"
            }
            XCTFail("Displaced worker delivered old owner")
        } catch is CancellationError { }
        catch let error as BigSyncOutboundQuiescenceError {
            XCTAssertEqual(error, .staleAuthority)
        }
        XCTAssertNil(original.postBarrierOutboundLease)
        XCTAssertNil(original.postBarrierOutboundTicket)
        try assertDurableFenceUnchanged(fixture)
        worker.install(nil)
    }
}
