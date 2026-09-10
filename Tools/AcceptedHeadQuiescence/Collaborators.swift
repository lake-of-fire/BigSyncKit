// Foundation-only collaborators for the UNCHANGED public synchronizer/worker
// methods copied by the runner. This is control-flow coverage, not CloudKit,
// filesystem-lock, Realm, or production configuration qualification.
import Foundation

@globalActor public actor BigSyncBackgroundActor {
    public static let shared = BigSyncBackgroundActor()
    @BigSyncBackgroundActor var realmSynchronizer: CloudKitSynchronizer?
}

public enum BigSyncOutboundQuiescenceError: Error, Equatable {
    case staleAuthority, recoveryRequired, busy, blocked
}
public struct BigSyncOutboundPrincipal: Equatable, Sendable {
    let accountScopeIdentifier: String
    let bindingID: String
}
public struct BigSyncOutboundBarrier: Equatable, Sendable {
    enum Phase: Sendable { case preparing, recoveryRequired, sourcePublication }
    let identifier: UUID
    var phase: Phase
}
public struct BigSyncOutboundQuiescenceSnapshot: Equatable, Sendable {
    var barrier: BigSyncOutboundBarrier?
    var revision = UUID()
    var hasUnknownSubmissions = false
}
@BigSyncBackgroundActor final class Owner {
    var barrier: BigSyncOutboundBarrier
    var revoked = false
    var drained = false
    init(barrier: BigSyncOutboundBarrier) { self.barrier = barrier }
}
@BigSyncBackgroundActor final class Gate {
    var state: BigSyncOutboundQuiescenceSnapshot
    var waitHook: (@Sendable @BigSyncBackgroundActor () async throws -> Void)?
    var seals = 0
    init(barrier: BigSyncOutboundBarrier) { state = .init(barrier: barrier) }
    func waitUntilDrained(_ owner: Owner, revalidating: @Sendable () async throws -> Void) async throws {
        try await revalidating()
        try await waitHook?()
        try await revalidating()
        guard !state.hasUnknownSubmissions else { throw BigSyncOutboundQuiescenceError.recoveryRequired }
        owner.drained = true
    }
    func validateDrained(_ owner: Owner, principal: BigSyncOutboundPrincipal) throws {
        guard owner.drained, !state.hasUnknownSubmissions, state.barrier == owner.barrier else {
            throw BigSyncOutboundQuiescenceError.busy
        }
    }
    func requireRecovery(_ owner: Owner) throws {
        guard !owner.revoked, owner.drained, owner.barrier.phase == .preparing,
              state.barrier == owner.barrier, !state.hasUnknownSubmissions else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        owner.barrier.phase = .recoveryRequired
        state.barrier = owner.barrier
        state.revision = UUID()
        seals += 1
    }
    func snapshot() throws -> BigSyncOutboundQuiescenceSnapshot { state }
}
@BigSyncBackgroundActor public final class CloudKitSynchronizer {
    public struct PostBarrierOutboundQuiescence: Equatable, Sendable {
        let identifier: UUID
        let principal: BigSyncOutboundPrincipal
        let acquisition = UUID()
    }
    var syncing = false
    var synchronizationDrainIsActive = false
    var postBarrierOutboundEstablishmentID: UUID?
    var postBarrierDrainAuthorization: UUID?
    var completedPostBarrierDrain: UUID?
    var outboundRecoveryID: UUID?
    var synchronizationAttemptID = UUID()
    var ticket: PostBarrierOutboundQuiescence
    let owner: Owner
    let outboundQuiescenceCoordinator: Gate
    var account = "account-A"
    var principal: BigSyncOutboundPrincipal
    var accountReads = 0
    var accountHook: (@Sendable @BigSyncBackgroundActor (Int) async throws -> Void)?
    var abandonments = 0

    init() {
        principal = .init(accountScopeIdentifier: "account-A", bindingID: "binding-A")
        let barrier = BigSyncOutboundBarrier(identifier: UUID(), phase: .preparing)
        owner = Owner(barrier: barrier)
        ticket = .init(identifier: barrier.identifier, principal: principal)
        outboundQuiescenceCoordinator = Gate(barrier: barrier)
    }
    func matchingOutboundOwner(_ token: PostBarrierOutboundQuiescence) -> Owner? {
        token == ticket ? owner : nil
    }
    func validatePostBarrierOutboundQuiescence(_ token: PostBarrierOutboundQuiescence) throws -> BigSyncOutboundQuiescenceSnapshot {
        try Task.checkCancellation()
        guard matchingOutboundOwner(token) != nil, token.principal == principal,
              !owner.revoked, owner.barrier == outboundQuiescenceCoordinator.state.barrier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return outboundQuiescenceCoordinator.state
    }
    func accountIdentifierProvider() async throws -> String {
        accountReads += 1
        try await accountHook?(accountReads)
        return account
    }
    static func accountScopeIdentifier(for account: String) -> String { account }
    @discardableResult func abandonPostBarrierOutboundQuiescence(_ token: PostBarrierOutboundQuiescence) -> Bool {
        guard ticket == token else { return false }
        owner.revoked = true
        abandonments += 1
        return true
    }
}
