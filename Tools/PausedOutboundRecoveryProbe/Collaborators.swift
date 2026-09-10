import Foundation

@globalActor public actor BigSyncBackgroundActor {
    public static let shared = BigSyncBackgroundActor()
    @BigSyncBackgroundActor public var realmSynchronizer: CloudKitSynchronizer?
    @BigSyncBackgroundActor public func install(_ value: CloudKitSynchronizer?) {
        realmSynchronizer = value
    }
}

@BigSyncBackgroundActor
public final class CloudKitSynchronizer {
    public struct PostBarrierOutboundQuiescence: Sendable, Equatable {
        public let identifier: UUID
        public let writerBarrierEvidenceID: String
        let issuerID: UUID
        let principal: BigSyncOutboundPrincipal
        let ownershipID: UUID

        init(identifier: UUID, writerBarrierEvidenceID: String,
             issuerID: UUID, principal: BigSyncOutboundPrincipal) {
            self.identifier = identifier
            self.writerBarrierEvidenceID = writerBarrierEvidenceID
            self.issuerID = issuerID
            self.principal = principal
            self.ownershipID = UUID()
        }
    }

    public struct PostBarrierDrainAuthorization: Sendable {}

    let outboundQuiescenceCoordinator: BigSyncOutboundQuiescenceCoordinator
    let principal: BigSyncOutboundPrincipal
    let synchronizationReceiptIssuerID = UUID()
    var postBarrierOutboundLease: BigSyncOutboundQuiescenceLease?
    var postBarrierOutboundTicket: PostBarrierOutboundQuiescence?
    var postBarrierDrainAuthorization: PostBarrierDrainAuthorization?
    var outboundRecoveryID: UUID?
    var synchronizationAttemptID = UUID()
    var syncing = false
    var synchronizationDrainIsActive = false
    var account = "account"
    var accountReads = 0
    var replaceAccountOnRead: Int?

    init(directory: URL, principal: BigSyncOutboundPrincipal) {
        self.principal = principal
        self.outboundQuiescenceCoordinator = .init(
            sharedStateBaseURL: directory,
            durableStateNamespace: principal.durableStateNamespace
        )
    }

    func accountIdentifierProvider() async throws -> String {
        accountReads += 1
        if replaceAccountOnRead == accountReads { account = "replacement-account" }
        return account
    }

    static func accountScopeIdentifier(for account: String) -> String { account }

    func currentOutboundPrincipal() throws -> BigSyncOutboundPrincipal { principal }

    func validatePostBarrierOutboundQuiescence(
        _ token: PostBarrierOutboundQuiescence
    ) throws -> BigSyncOutboundQuiescenceSnapshot {
        guard token.issuerID == synchronizationReceiptIssuerID,
              token == postBarrierOutboundTicket,
              token.principal == principal,
              let owner = postBarrierOutboundLease else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        try owner.validateAcquisition(principal: principal, requiresDrained: false)
        let snapshot = try outboundQuiescenceCoordinator.snapshot()
        guard snapshot.barrier == owner.barrier else {
            throw BigSyncOutboundQuiescenceError.staleAuthority
        }
        return snapshot
    }

    @discardableResult
    func abandonPostBarrierOutboundQuiescence(
        _ token: PostBarrierOutboundQuiescence
    ) -> Bool {
        guard token == postBarrierOutboundTicket else { return false }
        postBarrierOutboundLease?.sealOutboundAdmission()
        postBarrierOutboundLease = nil
        postBarrierOutboundTicket = nil
        return true
    }
}
