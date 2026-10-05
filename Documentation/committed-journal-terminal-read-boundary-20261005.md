# Committed Realm read boundary for journal and terminal evidence — October 5, 2026

## Decision

Keep the existing actor, journal, acknowledgement and terminal-publication
architecture. The recurring defect class is narrower: read-only evidence code can
reenter the same actor while a shared Realm handle is inside another caller's
write transaction. Such a reader must use the Realm's committed read version,
not the provisional transaction state.

This increment is stacked on BigSyncKit #96 at
`0b19ed598a7503235efcde5d674b9620e1d0f978`, which already contains the
physical-disappearance and terminal-audit committed-read repairs.

## SDK contract checked

RealmSwift 20.0.5 `Realm.freeze()` delegates to
`RLMGetFrozenRealmForSourceRealm`. That implementation calls `read_group()`
and keys the frozen Realm from `read_transaction_version()`; the frozen copy
therefore represents the Realm's read transaction version rather than
provisional changes in an open write transaction.

By contrast, RealmSwift documents collection freezing as unavailable during a
write transaction. Journal forwarding previously called `Results.freeze()`
directly on the shared target handle. That is the wrong primitive when a
reentrant owner can have a write open.

Pinned sources reviewed:

- `realm/realm-swift@v20.0.5 RealmSwift/Realm.swift`
- `realm/realm-swift@v20.0.5 Realm/RLMRealm.mm`
- `realm/realm-swift@v20.0.5 Realm/RLMRealmUtil.mm`

## Production changes

`RealmSwiftAdapter.committedRealmReadSnapshot(in:)` is the shared internal
boundary for this increment. It refreshes only an unfrozen Realm with no open
write and then freezes unconditionally. The unconditional freeze matters because
refresh notification delivery can itself open a reentrant write.

The boundary is now used for:

1. the initial paged pending-mutation journal cut;
2. target re-resolution after tracking-write admission;
3. public pending-mutation inventory and DEBUG tracking-generation inspection;
4. semantic publication blockers;
5. terminal pending/conflict/comparison-evidence checks;
6. the consumed server cursor and change-feed epoch, read from one committed
   tracking version.

Final target/tracking writes still use their existing independently owned
transactions and exact generation/account/binding checks. No queue, schema,
journal, transport representation, acknowledgement rule or persistent authority
is added.

## Authored native regressions

Five native W1 regressions were added to the existing registered test source:

- pending inventory ignores a provisional successor/deletion;
- forwarding can begin while a target owner has a provisional write;
- tracking admission cannot borrow a provisional successor generation;
- terminal target debt cannot disappear through provisional journal removal;
- terminal tracking debt cannot disappear through provisional acknowledgement;
- consumed cursor and change-feed epoch ignore one provisional tracking
  transition.

The last bullet is one test method covering both values, so there are six new
method identities in total.

These tests use real Realm-backed adapter entry points and existing W1 fixtures,
but they have not been executed in this connector session. Native compilation,
Xcode discovery, Realm scheduling/durability, CloudKit, assembled Reader, UI,
signed, performance and release qualification remain pending.

## Integration

Reader #286 should select this successor only after reconciling the current
BigSync gitlink and both native inventories. Historical portable/native evidence
does not transfer to these changed source files. Keep the component/application
candidate draft until the owning Apple/Realm batch executes the new methods
alongside the existing journal-forwarding, terminal-cutoff, disappearance,
audit, acknowledgement and Unmark suites.
