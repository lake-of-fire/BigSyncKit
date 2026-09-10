import Foundation
import XCTest
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif
@testable import BigSyncKit

final class BigSyncOutboundStateBoundsTests: XCTestCase {
    private let byteLimit = 8 * 1_024 * 1_024

    private func fixture() -> (BigSyncOutboundQuiescenceCoordinator, BigSyncOutboundPrincipal) {
        let base = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        addTeardownBlock { try? FileManager.default.removeItem(at: base) }
        return (
            BigSyncOutboundQuiescenceCoordinator(sharedStateBaseURL: base, durableStateNamespace: "client"),
            BigSyncOutboundPrincipal(durableStateNamespace: "client", installationIdentifier: "installation",
                accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding",
                accountInvalidationGeneration: 1)
        )
    }

    private func stateURL(_ gate: BigSyncOutboundQuiescenceCoordinator) -> URL {
        gate.directory.appendingPathComponent("state.json")
    }

    /// Seed a valid persisted checkpoint near the existing byte limit without
    /// thousands of quadratic fsync/encode calls. All field/count invariants are
    /// admitted by the actual production reader before the operation under test.
    private func nearLimit(
        _ gate: BigSyncOutboundQuiescenceCoordinator, escaped: Bool = false
    ) throws -> (BigSyncOutboundQuiescenceSnapshot, BigSyncOutboundPrincipal, Data) {
        let label = String(repeating: escaped ? "\u{0001}" : "x", count: 1_024)
        let principal = BigSyncOutboundPrincipal(durableStateNamespace: "client",
            installationIdentifier: label, accountScopeIdentifier: label,
            replicaBindingGenerationIdentifier: label, accountInvalidationGeneration: 1)
        var state = try gate.snapshot()
        let encoder = JSONEncoder()
        let emptySize = try encoder.encode(state).count
        let entrySize = try encoder.encode(BigSyncOutboundSubmission(identifier: UUID(), principal: principal)).count
        let count = (byteLimit - emptySize + 1) / (entrySize + 1)
        XCTAssertLessThan(count, 4_096)
        state.outstandingSubmissions = (0..<count).map { _ in
            BigSyncOutboundSubmission(identifier: UUID(), principal: principal)
        }
        let bytes = try encoder.encode(state)
        XCTAssertLessThanOrEqual(bytes.count, byteLimit)
        XCTAssertGreaterThan(bytes.count + entrySize + 1, byteLimit)
        try bigSyncWriteDataDurably(bytes, to: stateURL(gate))
        XCTAssertEqual(try gate.snapshot(), state)
        return (state, principal, bytes)
    }

    func testSubmissionByteOverflowPreservesReadableCheckpointAndCanRetryAfterRecovery() throws {
        let (gate, _) = fixture()
        let (expected, principal, bytes) = try nearLimit(gate)
        var batch: BigSyncOutboundBatchLease? = try gate.admit(principal: principal)
        XCTAssertThrowsError(try XCTUnwrap(batch).willSubmit()) {
            XCTAssertEqual($0 as? BigSyncOutboundQuiescenceError, .recoveryRequired)
        }
        XCTAssertEqual(try Data(contentsOf: stateURL(gate)), bytes)
        XCTAssertEqual(try gate.snapshot(), expected)
        // Failed pre-submission persistence must not install a local ticket.
        batch = nil
        let recovery = try gate.takeRecoveryOwnership(expected: expected)
        try gate.resolveRecovery(recovery, evidenceID: "TEST-ONLY-exact-settlement-proof")
        let retry = try gate.admit(principal: principal)
        try retry.willSubmit()
        XCTAssertEqual(try gate.snapshot().outstandingSubmissions.count, 1)
        try retry.didSettle()
    }

    func testEscapedFieldsCannotBypassEncodedByteBudget() throws {
        let (gate, _) = fixture()
        let (expected, principal, bytes) = try nearLimit(gate, escaped: true)
        let batch = try gate.admit(principal: principal)
        XCTAssertThrowsError(try batch.willSubmit())
        XCTAssertEqual(try Data(contentsOf: stateURL(gate)), bytes)
        XCTAssertEqual(try gate.snapshot(), expected)
    }

    func testBarrierByteOverflowPreservesExistingUncertaintyAndReleasesUnreturnedOwner() throws {
        let (gate, _) = fixture()
        let (expected, principal, bytes) = try nearLimit(gate)
        XCTAssertThrowsError(try gate.begin(principal: principal,
            writerBarrierEvidenceID: String(repeating: "e", count: 1_024)))
        XCTAssertEqual(try Data(contentsOf: stateURL(gate)), bytes)
        XCTAssertEqual(try gate.snapshot(), expected)
        // A failed begin never delivered an owner; it must not hold owner.lock.
        let recovery = try gate.takeRecoveryOwnership(expected: expected)
        try gate.resolveRecovery(recovery, evidenceID: "TEST-ONLY-exact-settlement-proof")
    }

    func testExactReaderByteBoundaryIsAcceptedButOneExtraByteIsPreservedAndRejected() throws {
        let (gate, _) = fixture()
        let expected = try gate.snapshot()
        var bytes = try Data(contentsOf: stateURL(gate))
        bytes.append(Data(repeating: 0x20, count: byteLimit - bytes.count))
        try bytes.write(to: stateURL(gate))
        XCTAssertEqual(try gate.snapshot(), expected)
        bytes.append(0x20)
        try bytes.write(to: stateURL(gate))
        XCTAssertThrowsError(try gate.snapshot())
        XCTAssertEqual(try Data(contentsOf: stateURL(gate)), bytes)
    }

    func testFullSubmissionCountRemainsReadableAndRejectsOnlyAdditionalWork() throws {
        let (gate, principal) = fixture()
        var expected = try gate.snapshot()
        expected.outstandingSubmissions = (0..<4_096).map { _ in
            BigSyncOutboundSubmission(identifier: UUID(), principal: principal)
        }
        let bytes = try JSONEncoder().encode(expected)
        XCTAssertLessThan(bytes.count, byteLimit)
        try bytes.write(to: stateURL(gate))
        let batch = try gate.admit(principal: principal)
        XCTAssertThrowsError(try batch.willSubmit())
        XCTAssertEqual(try gate.snapshot(), expected)
        XCTAssertEqual(try Data(contentsOf: stateURL(gate)), bytes)
    }

    func testStateSymlinkIsRejectedWithoutReadingOrReplacingItsTarget() throws {
        let (gate, _) = fixture()
        _ = try gate.snapshot()
        let url = stateURL(gate)
        let target = gate.directory.appendingPathComponent("retained-state.json")
        let bytes = try Data(contentsOf: url)
        try FileManager.default.moveItem(at: url, to: target)
        try FileManager.default.createSymbolicLink(at: url, withDestinationURL: target)
        XCTAssertThrowsError(try gate.snapshot())
        XCTAssertEqual(try FileManager.default.destinationOfSymbolicLink(atPath: url.path), target.path)
        XCTAssertEqual(try Data(contentsOf: target), bytes)
    }

    func testDanglingStateSymlinkIsNotReinitializedAsFirstUse() throws {
        let (gate, _) = fixture()
        try FileManager.default.createDirectory(at: gate.directory, withIntermediateDirectories: true)
        let url = stateURL(gate)
        let target = gate.directory.appendingPathComponent("missing-state.json")
        try FileManager.default.createSymbolicLink(at: url, withDestinationURL: target)
        XCTAssertThrowsError(try gate.snapshot())
        XCTAssertEqual(try FileManager.default.destinationOfSymbolicLink(atPath: url.path), target.path)
        XCTAssertFalse(FileManager.default.fileExists(atPath: target.path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: gate.directory.appendingPathComponent("initialized").path))
    }

    func testFIFOStateDoesNotWaitForAWriter() throws {
        let (gate, _) = fixture()
        _ = try gate.snapshot()
        let url = stateURL(gate)
        try FileManager.default.removeItem(at: url)
        XCTAssertEqual(mkfifo(url.path, S_IRUSR | S_IWUSR), 0)
        XCTAssertThrowsError(try gate.snapshot())
        var metadata = stat()
        XCTAssertEqual(lstat(url.path, &metadata), 0)
        XCTAssertEqual(metadata.st_mode & S_IFMT, S_IFIFO)
    }

    func testMissingInitializedStateStillFailsClosed() throws {
        let (gate, _) = fixture()
        _ = try gate.snapshot()
        try FileManager.default.removeItem(at: stateURL(gate))
        XCTAssertThrowsError(try gate.snapshot())
        XCTAssertFalse(FileManager.default.fileExists(atPath: stateURL(gate).path))
    }

    func testSourcePublicationAndRestartKeepPeersFencedThroughAcknowledgement() async throws {
        let (gate, principal) = fixture()
        var owner: BigSyncOutboundQuiescenceLease? = try gate.begin(principal: principal, writerBarrierEvidenceID: "writer")
        try await gate.waitUntilDrained(XCTUnwrap(owner))
        try XCTUnwrap(owner).armFinalDrain()
        owner?.sealFinalDrain()
        try gate.requireRecovery(XCTUnwrap(owner))
        let publication = try gate.authorizeSourcePublication(XCTUnwrap(owner),
            expected: gate.snapshot(), evidenceID: "TEST-ONLY-source-authority-proof")
        owner = nil
        let recovery = try gate.takeRecoveryOwnership(expected: publication)
        let resumed = try gate.resumeSourcePublication(recovery, principal: principal,
            recoveryEvidenceID: "TEST-ONLY-restart-proof")
        XCTAssertThrowsError(try gate.admit(principal: principal))
        var batch: BigSyncOutboundBatchLease? = try gate.admit(principal: principal, owner: resumed)
        try batch?.willSubmit()
        try batch?.noteDefinitiveTransportOutcome()
        XCTAssertEqual(try gate.snapshot().outstandingSubmissions.count, 1)
        try await batch?.completeLocalResponseProcessingCooperatively()
        XCTAssertTrue(try gate.snapshot().outstandingSubmissions.isEmpty)
        XCTAssertThrowsError(try gate.resolveOwned(resumed, expected: gate.snapshot(), evidenceID: "completion"))
        batch = nil
        try gate.resolveOwned(resumed, expected: gate.snapshot(), evidenceID: "TEST-ONLY-completion-proof")
        _ = try gate.admit(principal: principal)
    }
}
