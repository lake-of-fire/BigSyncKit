import Foundation
import RealmSwift

/// Minimal, local-only evidence retained while BigSyncKit rebuilds its tracking
/// Realm. This is deliberately not a second mutation journal: the target
/// Realm's `BigSyncPendingMutation` remains the authority for local intent.
///
/// A row means that a record was previously tracked. `hadValidServerRecord`
/// means either that the prior cache contained a decodable system-fields
/// record whose record ID, zone and change tag were valid, or that its prior
/// tracking state was already server-backed. Such a record must never be
/// blindly re-uploaded merely because a fresh CloudKit bootstrap did not see
/// it; its optional cached system fields may be damaged and it may have been
/// deleted remotely.
final class RebuildProvenance: Object {
    @objc dynamic var identifier = ""
    @objc dynamic var entityType = ""
    @objc dynamic var hadValidServerRecord = false
    @objc dynamic var priorState = 0
    @objc dynamic var priorPendingGeneration: String?
    @objc dynamic var accountScopeIdentifier = ""
    @objc dynamic var epoch = 0

    override static func primaryKey() -> String? { "identifier" }
    override static func indexedProperties() -> [String] {
        ["entityType", "hadValidServerRecord"]
    }
}

/// Singleton, durable state for an in-place tracking rebuild. Keeping this in
/// the tracking Realm makes a process death between provenance capture and the
/// server bootstrap idempotent without touching either target Realm.
final class RebuildProvenanceState: Object {
    static let primaryKeyValue = "__BigSyncKitTrackingRebuild.v1"

    @objc dynamic var identifier = RebuildProvenanceState.primaryKeyValue
    @objc dynamic var isActive = false
    @objc dynamic var serverBootstrapStarted = false
    @objc dynamic var accountScopeIdentifier = ""
    @objc dynamic var epoch = 0
    @objc dynamic var mode = ChangeFeedResetMode.serverReconciliation.rawValue
    @objc dynamic var phase = ""

    override static func primaryKey() -> String? { "identifier" }
}
