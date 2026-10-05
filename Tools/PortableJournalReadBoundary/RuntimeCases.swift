import Foundation
import RealmSwift
#if os(Linux)
import Glibc
#endif

struct Failure: Error, CustomStringConvertible { let description: String }
func require(_ truth: Bool, _ message: String) throws { if !truth { throw Failure(description: message) } }

@BigSyncBackgroundActor
func fixture(journal: Bool = true, deletion: Bool = false, trackingGeneration: String? = nil)
-> (RealmSwiftAdapter, Realm, Realm) {
    let realm = Realm(types: ["BigSyncPendingMutation", "Note"])
    realm.write {
        let object = Note(); object.set("key", "one"); object.isDeleted = deletion; realm.add(object)
        if journal { addJournal(realm) }
    }
    let tracking = Realm(types: ["SyncedEntity"])
    if let trackingGeneration {
        tracking.write {
            let entity = SyncedEntity(); entity.set("key", "Note.one")
            entity.pendingGeneration = trackingGeneration
            entity.pendingReplicaBindingGenerationIdentifier = "binding"
            tracking.add(entity)
        }
    }
    return (RealmSwiftAdapter(RealmProvider([realm], tracking)), realm, tracking)
}
func addJournal(_ realm: Realm, name: String = "Note.one", type: String = "Note", generation: String = "g1") {
    let value = BigSyncPendingMutation(); value.recordName = name; value.entityType = type
    value.objectIdentifier = "one"; value.generation = generation
    value.accountScopeIdentifier = "account"; value.replicaBindingGenerationIdentifier = "binding"
    value.changedAt = Date(timeIntervalSince1970: 30); realm.add(value)
}
func journal(_ realm: Realm) -> BigSyncPendingMutation { realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: "Note.one")! }
func note(_ realm: Realm) -> Note { realm.object(ofType: Note.self, forPrimaryKey: "one")! }
func tracked(_ realm: Realm) -> SyncedEntity? { realm.object(ofType: SyncedEntity.self, forPrimaryKey: "Note.one") }
@BigSyncBackgroundActor func inventory(_ adapter: RealmSwiftAdapter) throws -> [BigSyncPendingMutationInventoryItem] {
    try adapter.pendingMutationInventory(entityTypes: ["Note"])
}
@BigSyncBackgroundActor func proof(_ adapter: RealmSwiftAdapter) throws -> [String: String] {
    try adapter.cloudKitE2EPendingTrackingGenerations(recordNames: ["Note.one"])
}
@BigSyncBackgroundActor
func heldForwarding(deleted: Bool = false, change: @escaping (Realm) -> Void) async throws {
    let (adapter, realm, tracking) = fixture(deletion: deleted)
    adapter._testBeforePendingMutationTrackingWrite = { realm.beginWrite(); change(realm) }
    adapter._testAfterPendingMutationTrackingWrite = {
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        try require(realm.isInWriteTransaction, "independent target ownership lost")
        try require(tracked(tracking.freeze())?.pendingGeneration == "g1", "noncommitted/missing tracking generation")
        try require(tracked(tracking.freeze())?.isDeletion == deleted, "provisional target disposition published")
    }
    defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
    let count = try await adapter.forward(in: realm)
    try require(count == 1, "committed row was not forwarded once")
    try require(journal(realm).generation == "g1", "reader mutated committed journal")
}
struct RuntimeCase {
    let name: String
    let body: @BigSyncBackgroundActor () async throws -> Void
    init(_ name: String, _ body: @escaping @BigSyncBackgroundActor () async throws -> Void) { self.name = name; self.body = body }
}
@main struct RuntimeCases {
    static func main() async { let failed = await run(); exit(failed == 0 ? 0 : 1) }
    @BigSyncBackgroundActor static func run() async -> Int {
        let cases: [RuntimeCase] = [
            RuntimeCase("inventory_committed_values") {
                let (a, r, _) = fixture()
                let values = try inventory(a)
                try require(values.count == 1 && values[0].changedAt == Date(timeIntervalSince1970: 30), "committed values changed")
                try require(values[0].accountScopeIdentifier == "account" && values[0].replicaBindingGenerationIdentifier == "binding", "scope changed")
                try require(!r.isInWriteTransaction, "read opened a write")
            },
            RuntimeCase("inventory_excludes_provisional_insertion") {
                let (a, r, _) = fixture(journal: false)
                r.beginWrite(); defer { r.cancelWrite() }; addJournal(r)
                try require(try inventory(a).isEmpty, "provisional insertion exposed")
                try require(r.isInWriteTransaction && r.objects(BigSyncPendingMutation.self).count == 1, "owner disturbed")
            },
            RuntimeCase("inventory_retains_provisionally_removed_debt") {
                let (a, r, _) = fixture(); let before = try inventory(a)
                r.beginWrite(); defer { r.cancelWrite() }; r.delete(journal(r))
                try require(try inventory(a) == before, "committed debt hidden")
                try require(r.objects(BigSyncPendingMutation.self).isEmpty, "provisional owner rewritten")
            },
            RuntimeCase("inventory_excludes_provisional_fields") {
                let (a, r, _) = fixture(); let before = try inventory(a)
                r.beginWrite(); defer { r.cancelWrite() }
                journal(r).changedAt = Date(timeIntervalSince1970: 40)
                journal(r).accountScopeIdentifier = "other"; journal(r).replicaBindingGenerationIdentifier = "other"
                try require(try inventory(a) == before, "provisional metadata exposed")
                try require(journal(r).accountScopeIdentifier == "other", "owner values changed")
            },
            RuntimeCase("inventory_target_deletion_same_snapshot") {
                let (a, r, _) = fixture()
                r.beginWrite(); defer { r.cancelWrite() }; note(r).isDeleted = true
                try require(try inventory(a).map(\.isDeletion) == [false], "live target mixed with committed journal")
            },
            RuntimeCase("inventory_target_resurrection_same_snapshot") {
                let (a, r, _) = fixture(deletion: true)
                r.beginWrite(); defer { r.cancelWrite() }; note(r).isDeleted = false
                try require(try inventory(a).map(\.isDeletion) == [true], "committed deletion hidden")
            },
            RuntimeCase("inventory_refresh_reentrant_owner") {
                let (a, r, _) = fixture()
                r.onRefresh = { r.onRefresh = nil; r.beginWrite(); r.delete(journal(r)) }
                defer { if r.isInWriteTransaction { r.cancelWrite() } }
                try require(try inventory(a).count == 1, "refresh callback's provisional removal exposed")
                try require(r.isInWriteTransaction, "refresh owner disturbed")
            },
            RuntimeCase("inventory_next_read_sees_commit") {
                let (a, r, _) = fixture(journal: false)
                try require(try inventory(a).isEmpty, "initial inventory not empty")
                r.write { addJournal(r); note(r).isDeleted = true }
                try require(try inventory(a).map(\.isDeletion) == [true], "snapshot improperly cached")
            },
            RuntimeCase("inventory_next_read_sees_rollback") {
                let (a, r, _) = fixture(); let before = try inventory(a)
                r.beginWrite(); r.delete(journal(r)); r.cancelWrite()
                try require(try inventory(a) == before, "rollback lost debt")
            },
            RuntimeCase("inventory_filters_requested_type") {
                let (a, r, _) = fixture()
                r.write { addJournal(r, name: "Other.two", type: "Other") }
                try require(try inventory(a).map(\.recordName) == ["Note.one"], "type filter changed")
            },
            RuntimeCase("inventory_equal_duplicates_still_coalesce") {
                let (a, _, _) = fixture(); let (_, r2, _) = fixture()
                a.realmProvider!.targetReaderRealms!.append(r2)
                try require(try inventory(a).count == 1, "identical duplicate not coalesced")
            },
            RuntimeCase("inventory_conflicting_duplicates_still_reject") {
                let (a, _, _) = fixture(); let (_, r2, _) = fixture(deletion: true)
                a.realmProvider!.targetReaderRealms!.append(r2)
                do { _ = try inventory(a); throw Failure(description: "conflicting duplicate accepted") }
                catch RealmSwiftAdapterError.setupUnavailable { }
            },
            RuntimeCase("inventory_missing_journal_schema_skipped") {
                let (a, _, _) = fixture(); let unrelated = Realm(types: ["Note"])
                a.realmProvider!.targetReaderRealms!.append(unrelated)
                _ = try inventory(a)
                try require(unrelated.refreshCount == 0, "unrelated Realm touched")
            },
            RuntimeCase("inventory_empty_selection_no_provider") {
                let a = RealmSwiftAdapter(nil)
                try require(try a.pendingMutationInventory(entityTypes: []).isEmpty, "empty selection fails")
                try require(try a.cloudKitE2EPendingTrackingGenerations(recordNames: []).isEmpty, "empty proof fails")
            },
            RuntimeCase("inventory_nonempty_selection_requires_provider") {
                let a = RealmSwiftAdapter(nil)
                do { _ = try inventory(a); throw Failure(description: "missing setup accepted") }
                catch RealmSwiftAdapterError.setupUnavailable { }
            },
            RuntimeCase("tracking_retains_provisionally_removed_generation") {
                let (a, _, t) = fixture(trackingGeneration: "g1")
                t.beginWrite(); defer { t.cancelWrite() }; t.delete(tracked(t)!)
                try require(try proof(a) == ["Note.one": "g1"], "provisional tracking removal hid debt")
                try require(t.isInWriteTransaction && tracked(t) == nil, "tracking owner disturbed")
            },
            RuntimeCase("tracking_excludes_provisional_replacement") {
                let (a, _, t) = fixture(trackingGeneration: "g1")
                t.beginWrite(); defer { t.cancelWrite() }; tracked(t)!.pendingGeneration = "g2"
                try require(try proof(a) == ["Note.one": "g1"], "provisional generation exposed")
            },
            RuntimeCase("tracking_excludes_provisional_insertion") {
                let (a, _, t) = fixture()
                t.beginWrite(); defer { t.cancelWrite() }
                let e = SyncedEntity(); e.set("key", "Note.one"); e.pendingGeneration = "g2"; t.add(e)
                try require(try proof(a).isEmpty, "provisional tracking insertion exposed")
            },
            RuntimeCase("tracking_refresh_reentrant_owner") {
                let (a, _, t) = fixture(trackingGeneration: "g1")
                t.onRefresh = { t.onRefresh = nil; t.beginWrite(); tracked(t)!.pendingGeneration = "g2" }
                defer { if t.isInWriteTransaction { t.cancelWrite() } }
                try require(try proof(a) == ["Note.one": "g1"], "refresh callback exposed pending tracking edit")
            },
            RuntimeCase("tracking_next_read_sees_commit") {
                let (a, _, t) = fixture(trackingGeneration: "g1")
                _ = try proof(a); t.write { tracked(t)!.pendingGeneration = "g2" }
                try require(try proof(a) == ["Note.one": "g2"], "committed successor hidden")
            },
            RuntimeCase("observed_discovery_keeps_committed_removed_row") {
                let (a, r, _) = fixture()
                r.beginWrite(); defer { r.cancelWrite() }; r.delete(journal(r))
                try require(a.discover(["Note.one"], in: r).map(\.generation) == ["g1"], "observed committed identity dropped")
            },
            RuntimeCase("observed_discovery_keeps_committed_target_disposition") {
                let (a, r, _) = fixture()
                r.beginWrite(); defer { r.cancelWrite() }; note(r).isDeleted = true
                try require(a.discover(["Note.one"], in: r).map(\.isDeletion) == [false], "observed provisional tombstone")
            },
            RuntimeCase("forwarding_ignores_provisional_generation") {
                try await heldForwarding { journal($0).generation = "g2" }
            },
            RuntimeCase("forwarding_keeps_provisionally_removed_journal") {
                try await heldForwarding { $0.delete(journal($0)) }
            },
            RuntimeCase("forwarding_ignores_provisional_tombstone") {
                try await heldForwarding { note($0).isDeleted = true }
            },
            RuntimeCase("forwarding_keeps_committed_tombstone") {
                try await heldForwarding(deleted: true) { note($0).isDeleted = false }
            },
            RuntimeCase("forwarding_resamples_committed_generation_after_hook") {
                let (a, r, t) = fixture()
                a._testBeforePendingMutationTrackingWrite = { r.write { journal(r).generation = "g2" } }
                let count = try await a.forward(in: r)
                try require(count == 1 && tracked(t)?.pendingGeneration == "g2", "reused obsolete initial snapshot")
            },
            RuntimeCase("forwarding_refresh_reentrant_owner") {
                let (a, r, t) = fixture()
                a._testBeforePendingMutationTrackingWrite = {
                    r.onRefresh = { r.onRefresh = nil; r.beginWrite(); journal(r).generation = "g2" }
                }
                a._testAfterPendingMutationTrackingWrite = {
                    defer { if r.isInWriteTransaction { r.cancelWrite() } }
                    try require(r.isInWriteTransaction && tracked(t)?.pendingGeneration == "g1", "live view used after refresh callback")
                }
                defer { if r.isInWriteTransaction { r.cancelWrite() } }
                _ = try await a.forward(in: r)
            },
            RuntimeCase("forwarding_final_binding_gate_preserved") {
                let (a, r, t) = fixture()
                a._testBeforePendingMutationTrackingWrite = { a.activeReplicaBindingGenerationIdentifier = "replacement" }
                let count = try await a.forward(in: r)
                try require(count == 0 && tracked(t) == nil && journal(r).generation == "g1", "binding fence bypassed")
            },
            RuntimeCase("forwarding_final_account_gate_preserved") {
                let (a, r, t) = fixture()
                a.accountScopePropertyByClassName = ["Note": "account"]
                a._testBeforePendingMutationTrackingWrite = { a.activeAccountScopeIdentifier = "replacement" }
                let count = try await a.forward(in: r)
                try require(count == 0 && tracked(t) == nil && journal(r).generation == "g1", "account fence bypassed")
            },
            RuntimeCase("forwarding_cancellation_keeps_committed_debt") {
                let (a, r, t) = fixture()
                let cancelled = Task { @BigSyncBackgroundActor in
                    a._testBeforePendingMutationTrackingWrite = {
                        withUnsafeCurrentTask { $0?.cancel() }
                    }
                    return try await a.forward(in: r)
                }
                do { _ = try await cancelled.value; throw Failure(description: "cancelled forwarding succeeded") }
                catch is CancellationError { }
                try require(tracked(t) == nil && journal(r).generation == "g1", "cancelled publication consumed debt")
            },
            RuntimeCase("forwarding_empty_selection_stays_noop") {
                let (a, r, t) = fixture(journal: false)
                let count = try await a.forward(in: r)
                try require(count == 0 && t.objects(SyncedEntity.self).isEmpty, "empty drain mutated tracking")
            },
        ]
        var failed = 0
        for test in cases {
            do { try await test.body(); print("PASS \(test.name)") }
            catch { failed += 1; print("FAIL \(test.name): \(error)") }
        }
        print("RESULT total=\(cases.count) passed=\(cases.count-failed) failed=\(failed)")
        return failed
    }
}
