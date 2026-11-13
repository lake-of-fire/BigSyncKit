// Stand-ins for transport, models, journal eligibility and tracking mutation.
// Only the selected production read/forward methods are under test.
import Foundation
import RealmSwift

@globalActor public actor BigSyncBackgroundActor { public static let shared = BigSyncBackgroundActor() }
public enum RealmSwiftAdapterError: Error { case setupUnavailable }
public final class BigSyncPendingMutation: Object, @unchecked Sendable {
    public var recordName: String { get { key } set { set("key", newValue); set("recordName", newValue) } }
    public var entityType: String { get { string("entityType") ?? "" } set { set("entityType", newValue) } }
    public var objectIdentifier: String { get { string("objectIdentifier") ?? "" } set { set("objectIdentifier", newValue) } }
    public var accountScopeIdentifier: String? { get { string("account") } set { set("account", newValue) } }
    public var replicaBindingGenerationIdentifier: String? { get { string("binding") } set { set("binding", newValue) } }
    public var requiresReplicaBindingRecovery: Bool { get { bool("requiresReplicaBindingRecovery") } set { fields["requiresReplicaBindingRecovery"] = .bool(newValue) } }
    public var generation: String { get { string("generation") ?? "" } set { set("generation", newValue) } }
    public var changedAt: Date { get { date("changedAt") } set { fields["changedAt"] = .date(newValue) } }
}
// Match the production state distinctions and raw values; no CloudKit transport.
public enum SyncedEntityState: Int, Sendable {
    case new = 1, changed = 2, deletedLocally = 3, synced = 4
    case deletedRemotely = 5, awaitingServerEvidence = 6, recreatingRemotely = 7
}
// Only value equality is consumed by the extracted lifecycle validator. Context
// construction, installed evidence policy and validate(in:) are not exercised.
struct BigSyncRecordRebaseContext: Equatable, Sendable {
    let namespace: String
    let account: String
    let binding: String
    let preservationNamespace: String

    init(namespace: String, account: String, binding: String, preservationNamespace: String? = nil) {
        self.namespace = namespace
        self.account = account
        self.binding = binding
        self.preservationNamespace = preservationNamespace ?? namespace
    }
}
public final class SyncedEntity: Object, @unchecked Sendable {
    public var pendingGeneration: String? { get { string("generation") } set { set("generation", newValue) } }
    public var pendingReplicaBindingGenerationIdentifier: String? { get { string("binding") } set { set("binding", newValue) } }
    public var entityState: SyncedEntityState {
        get {
            guard let stored = string("state"), let rawValue = Int(stored),
                  let value = SyncedEntityState(rawValue: rawValue) else {
                preconditionFailure("Tracking fixtures must initialize a valid production state")
            }
            return value
        }
        // Store in the cloneable field dictionary so committed/provisional cuts
        // retain state as well as generation; the real Realm column is an Int.
        set { set("state", String(newValue.rawValue)) }
    }
    public var isDeletion: Bool { entityState == .deletedLocally }
}
public final class Note: Object, @unchecked Sendable {
    public var isDeleted: Bool { get { bool("isDeleted") } set { fields["isDeleted"] = .bool(newValue) } }
}
public struct BigSyncPendingMutationSnapshot: Sendable {
    public let recordName: String, entityType: String, objectIdentifier: String
    public let accountScopeIdentifier: String?, replicaBindingGenerationIdentifier: String?
    public let requiresReplicaBindingRecovery: Bool
    public let generation: String
    public let changedAt: Date
    public let isDeletion: Bool
}
@BigSyncBackgroundActor public final class RealmProvider {
    public var targetReaderRealms: [Realm]?
    public var persistenceRealm: Realm?
    public init(_ targets: [Realm]?, _ tracking: Realm?) { targetReaderRealms = targets; persistenceRealm = tracking }
}
@BigSyncBackgroundActor public protocol ModelAdapterDelegate: AnyObject { func hasChangesToUpload() async }
public extension Array {
    func chunks(ofCount size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map { Array(self[$0..<Swift.min($0 + size, count)]) }
    }
}
extension Realm {
    @BigSyncBackgroundActor func asyncWritePreservingOwnership(_ body: () throws -> Void) async throws {
        // Deliberately admit a cancelled caller so the production forwarding
        // guard, not this collaborator, must reject it and roll back the write.
        // This collaborator does not model Realm's async admission queue.
        try write(body)
    }
}
