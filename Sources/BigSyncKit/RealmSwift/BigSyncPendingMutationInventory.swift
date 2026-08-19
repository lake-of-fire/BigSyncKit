import Foundation
import RealmSwift

/// Read-only cutover/audit view of the durable target-Realm journal. It does
/// not forward, clear, acknowledge, or otherwise mutate pending work.
public struct BigSyncPendingMutationInventoryItem: Equatable, Sendable {
    public let recordName: String
    public let entityType: String
    public let accountScopeIdentifier: String?
    public let changedAt: Date
    public let isDeletion: Bool

    public init(
        recordName: String,
        entityType: String,
        accountScopeIdentifier: String?,
        changedAt: Date,
        isDeletion: Bool
    ) {
        self.recordName = recordName
        self.entityType = entityType
        self.accountScopeIdentifier = accountScopeIdentifier
        self.changedAt = changedAt
        self.isDeletion = isDeletion
    }
}

extension RealmSwiftAdapter {
    /// Returns exact durable pending rows for the requested model types.
    /// Callers must decide whether legacy unscoped rows are safe to drain or
    /// require a fail-closed cutover; this API never guesses their account.
    @BigSyncBackgroundActor
    public func pendingMutationInventory(
        entityTypes: Set<String>
    ) throws -> [BigSyncPendingMutationInventoryItem] {
        guard let targetReaderRealms = realmProvider?.targetReaderRealms else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        var byRecordName = [String: BigSyncPendingMutationInventoryItem]()
        for realm in targetReaderRealms where realm.schema.objectSchema
            .contains(where: {
                $0.className == BigSyncPendingMutation.className()
            }) {
            realm.refresh()
            for mutation in realm.objects(BigSyncPendingMutation.self)
                where entityTypes.contains(mutation.entityType) {
                let item = BigSyncPendingMutationInventoryItem(
                    recordName: mutation.recordName,
                    entityType: mutation.entityType,
                    accountScopeIdentifier:
                        mutation.accountScopeIdentifier,
                    changedAt: mutation.changedAt,
                    isDeletion: pendingMutationTargetsDeletedObject(
                        mutation,
                        in: realm
                    )
                )
                if let existing = byRecordName[item.recordName],
                   existing != item {
                    throw RealmSwiftAdapterError.setupUnavailable
                }
                byRecordName[item.recordName] = item
            }
        }
        return byRecordName.values.sorted {
            if $0.entityType != $1.entityType {
                return $0.entityType < $1.entityType
            }
            return $0.recordName < $1.recordName
        }
    }
}
