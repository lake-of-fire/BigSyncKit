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
    public var generation: String { get { string("generation") ?? "" } set { set("generation", newValue) } }
    public var changedAt: Date { get { date("changedAt") } set { fields["changedAt"] = .date(newValue) } }
}
public final class SyncedEntity: Object, @unchecked Sendable {
    public var pendingGeneration: String? { get { string("generation") } set { set("generation", newValue) } }
    public var pendingReplicaBindingGenerationIdentifier: String? { get { string("binding") } set { set("binding", newValue) } }
    public var isDeletion: Bool { get { bool("isDeletion") } set { fields["isDeletion"] = .bool(newValue) } }
}
public final class Note: Object, @unchecked Sendable {
    public var isDeleted: Bool { get { bool("isDeleted") } set { fields["isDeleted"] = .bool(newValue) } }
}
public struct BigSyncPendingMutationSnapshot: Sendable {
    public let recordName: String, entityType: String, objectIdentifier: String
    public let accountScopeIdentifier: String?, replicaBindingGenerationIdentifier: String?
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
