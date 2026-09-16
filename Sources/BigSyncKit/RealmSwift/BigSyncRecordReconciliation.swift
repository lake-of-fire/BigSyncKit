import Foundation

/// A typed result prevents an adopted model from falling through to a second
/// legacy lifecycle decision. The target transaction computes this from its
/// current snapshot; selection-time tracking state is not an input.
enum BigSyncRecordReconciliationResult {
    case commit(BigSyncRecordTransition)
    case preservePhysicalDeletion
    case needsResolution
}

struct BigSyncRecordTransition {
    let incomingFields: Set<String>
    let acceptsIncomingBaseline: Bool
}

enum BigSyncRecordReconciliationPlanner {
    static func plan(
        base: [String: Data]?, local: [String: Data], remote: [String: Data],
        policy: BigSyncRecordRebasePolicy,
        contract: BigSyncRecordContract?, pending: Bool, existing: Bool,
        localDeleted: Bool, remoteDeleted: Bool,
        localLifetime: String?, remoteLifetime: String?, preferRemote: Bool
    ) throws -> BigSyncRecordReconciliationResult {
        let keys = Set(remote.keys)
        let retained = contract?.deletion == .retained
        let lifetimeField: String?
        if case let .lifetimeBundle(field, _) = policy { lifetimeField = field }
        else { lifetimeField = nil }
        let ordered = lifetimeField != nil && localLifetime != remoteLifetime
            ? try BigSyncLifetimeID.prefersIncoming(local: localLifetime, incoming: remoteLifetime)
            : nil

        if !retained && pending && localDeleted {
            if ordered == true {
                return .commit(.init(incomingFields: keys, acceptsIncomingBaseline: true))
            }
            return .preservePhysicalDeletion
        }
        if pending && base == nil {
            guard local == remote else { return .needsResolution }
            return .commit(.init(incomingFields: keys, acceptsIncomingBaseline: true))
        }
        if !pending {
            if existing, let base, ordered == false {
                let selected = try fields(base: base, local: local, remote: remote,
                    policy: policy, contract: contract, preferRemote: true,
                    localLifetime: localLifetime, remoteLifetime: remoteLifetime)
                return .commit(.init(incomingFields: selected, acceptsIncomingBaseline: true))
            }
            return .commit(.init(incomingFields: keys, acceptsIncomingBaseline: true))
        }
        guard let base else { return .needsResolution }
        if !retained && (localDeleted || remoteDeleted) {
            return .commit(.init(incomingFields: (ordered ?? preferRemote) ? keys : [],
                                 acceptsIncomingBaseline: true))
        }
        return .commit(.init(incomingFields: try fields(base: base, local: local, remote: remote,
            policy: policy, contract: contract, preferRemote: preferRemote,
            localLifetime: localLifetime, remoteLifetime: remoteLifetime),
            acceptsIncomingBaseline: true))
    }

    private static func fields(
        base: [String: Data], local: [String: Data], remote: [String: Data],
        policy: BigSyncRecordRebasePolicy, contract: BigSyncRecordContract?,
        preferRemote: Bool, localLifetime: String?, remoteLifetime: String?
    ) throws -> Set<String> {
        var selected = try BigSyncRecordRebasePlanner.incomingFields(
            base: base, local: local, remote: remote, policy: policy,
            preferRemoteOnConflict: preferRemote,
            localLifetime: localLifetime, remoteLifetime: remoteLifetime)
        for group in contract?.atomicFieldGroups ?? [] {
            let localChanged = group.contains { local[$0] != base[$0] }
            let remoteChanged = group.contains { remote[$0] != base[$0] }
            selected.subtract(group)
            if !localChanged || (remoteChanged && preferRemote) {
                selected.formUnion(group)
            }
        }
        return selected
    }
}
