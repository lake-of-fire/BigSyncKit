// Test-only subprocess driver. Compiled with the unmodified portable production
// gate, not included in the library. No CloudKit calls or domain data operations.
import Foundation

@main
struct ProcessProbe {
    static func emit(_ value: String) { print(value); fflush(stdout) }

    static func main() async {
        do {
            let arguments = CommandLine.arguments
            guard arguments.count == 3 else { throw CocoaError(.fileReadInvalidFileName) }
            let gate = BigSyncOutboundQuiescenceCoordinator(
                sharedStateBaseURL: URL(fileURLWithPath: arguments[2]), durableStateNamespace: "client")
            let principal = BigSyncOutboundPrincipal(durableStateNamespace: "client",
                installationIdentifier: "test-installation", accountScopeIdentifier: "test-account",
                replicaBindingGenerationIdentifier: "test-binding", accountInvalidationGeneration: 1)
            switch arguments[1] {
            case "peer":
                var batch: BigSyncOutboundBatchLease? = try gate.admit(principal: principal)
                emit("admitted")
                while let command = readLine() {
                    switch command {
                    case "submit": try batch?.willSubmit(); emit("submitted")
                    case "settle": try batch?.didSettle(); emit("settled")
                    case "release": batch = nil; emit("released"); return
                    default: throw CocoaError(.fileReadCorruptFile)
                    }
                }
            case "owner":
                let owner = try gate.begin(principal: principal, writerBarrierEvidenceID: "test-writer-barrier")
                emit("fenced")
                do { try await gate.waitUntilDrained(owner); emit("drained") }
                catch BigSyncOutboundQuiescenceError.unresolvedSubmissions { emit("unresolved"); return }
                while let command = readLine() {
                    switch command {
                    case "final":
                        try owner.armFinalDrain()
                        do {
                            let batch = try gate.admit(principal: principal, owner: owner)
                            try batch.willSubmit(); try batch.didSettle()
                        }
                        owner.sealFinalDrain()
                        try gate.validateDrained(owner, principal: principal)
                        emit("final-settled")
                    case "reserve": try gate.requireRecovery(owner); emit("recovery-required")
                    case "abort":
                        do { try gate.abort(owner); emit("aborted"); return }
                        catch BigSyncOutboundQuiescenceError.recoveryRequired { emit("abort-rejected") }
                    case "resolve":
                        try gate.resolveOwned(owner, expected: gate.snapshot(), evidenceID: "TEST-ONLY-domain-transition")
                        emit("resolved"); return
                    default: throw CocoaError(.fileReadCorruptFile)
                    }
                }
            case "try-admit":
                do { let batch = try gate.admit(principal: principal); withExtendedLifetime(batch) { emit("admitted") } }
                catch BigSyncOutboundQuiescenceError.blocked { emit("blocked") }
                catch BigSyncOutboundQuiescenceError.busy { emit("busy") }
            case "try-recover":
                do {
                    let lease = try gate.takeRecoveryOwnership(expected: gate.snapshot())
                    withExtendedLifetime(lease) { emit("recoverable") }
                } catch BigSyncOutboundQuiescenceError.busy { emit("busy") }
            case "recover":
                let lease = try gate.takeRecoveryOwnership(expected: gate.snapshot())
                // This test explicitly supplies a simulated authoritative proof.
                // Production must not assume a real server request has settled.
                try gate.resolveRecovery(lease, evidenceID: "TEST-ONLY-authoritative-settlement")
                emit("recovered")
            case "inspect":
                emit(String(decoding: try JSONEncoder().encode(gate.snapshot()), as: UTF8.self))
            default: throw CocoaError(.fileReadInvalidFileName)
            }
        } catch { emit("ERROR: \(error)"); exit(1) }
    }
}
