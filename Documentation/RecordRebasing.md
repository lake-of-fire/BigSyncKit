# Record rebasing: accounting and integration contract

This is an opt-in library capability, not a claim that every application model or conflict is solved. It follows the bounded repairs merged in #44. The application integration in Reader #144 and Common #70 remains separately qualified.

## Problem and chosen scope

A pending record is not evidence that every property was edited locally. Keeping the whole pending row while adopting a newer server change tag lets an unrelated local edit overwrite a remote field without another upload conflict.

The adapter now retains one accepted comparison baseline per record in its target Realm. The baseline contains field digests, a server change tag, an opaque receipt revision and the transport namespace. It contains no old payload values and no growing history. The durable record-level mutation journal remains the only source of upload work.

For declared independent fields, compare accepted base, current local value and decoded incoming value. Preserve local-only edits, accept remote-only edits, and apply the configured record-clock/tie decision only where both sides changed a field. Collections are indivisible fields; there is no element-level collection merge. This fixes the demonstrated disjoint-field stale-base mechanism without a CRDT framework or per-field clocks.

This does **not** preserve both texts in a genuine concurrent edit of the same text field, infer deleted ancestor data, or reconstruct exact per-field authoring times. The record clock is an arbitration policy for true collisions, not proof of per-field chronology after prior merges. The Common editor's separate-note protection covers its local commit boundary, not every distributed text conflict.

## Enabling the capability

Before opening each writer/adapter target Realm, use its explicit object-type configuration:

```swift
BigSyncMutationPolicy.enableRecordRebasing(in: &configuration)
mutationPolicy.install(configurations: [configuration], mutationJournalIdentityProvider: identityProvider)
```

Use the same configuration on every access path and include the local table in backup/schema handling. Merely linking the library or discovering the class through Realm's automatic schema discovery does not enable rebasing. The table never becomes a CloudKit entity or journal work item.

The active adapter must have an admitted account, container and binding, `.custom` merge policy, and no custom merge/transport-processing delegates. The comparison namespace includes account, container, database scope, zone owner/name and binding. A baseline from another namespace is not an ancestor.

A model declares semantics, not an application reconciliation callback:

```swift
static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
    .independentFields
}
```

Supported undeclared ordinary models remain indivisible via `.atomicRecord`. `.disabled` explicitly retains the legacy path. Semantic/snapshot models are excluded unless they make a valid supported declaration. Explicit declarations with unsupported relationships or types fail visibly rather than silently falling back to a different merge algorithm. Deferred object relationships remain outside this first capability.

Field fingerprints use the existing decoder's Realm representation. Primitive sets/maps are order-normalized; lists retain order; nil remains distinct from values and dates use millisecond normalization. SHA-256 equality is cryptographic comparison evidence, not a mathematical no-collision guarantee. Storage cost is one digest per included field plus Realm/key/namespace overhead; peak memory, CPU and actual database size still need application-scale measurement.

Changing model fields/types, skip rules or merge policy requires coordinated baseline invalidation/rebuild. This implementation is not an automatic schema migration system. A missing or incompatible base with different pending values fails closed instead of inventing an ancestor. First-upload acknowledgements and ordinary accepted imports establish baselines. Do not erase pending user work to obtain a clean baseline.

## Atomic import and replay

The target values, incoming comparison baseline and any resulting journal generation commit in the **same target-Realm transaction**. Recompute against the current object at that boundary; a selection-time snapshot cannot authorize overwriting newer typing.

The baseline is the incoming accepted server representation, not the merged working row. Otherwise preserved local edits would incorrectly appear already accepted on the next merge.

Tracking metadata and the page cursor are separate later transactions. A failure after target commit must replay safely against the target's matching baseline, not relabel imported changes as fresh local edits. Existing page receipts, account fences, cancellation and terminal domain publication remain in force. The test suite covers rollback and a tracking failure followed by redelivery; that does not claim one atomic transaction across all Realm files.

## Upload receipt contract

Preparation captures immutable field digests and the comparison revision together with the exact mutation generation. Receipts must be tied to that preparation, not sampled from the current edited object.

Before any acknowledgement mutation, validate record IDs, zone, type, uniqueness and payload agreement with the prepared evidence. Reject malformed batches. Comparison-enabled records cannot strip their proof or use the generation-only entry point as a fallback.

A valid receipt may advance the accepted base while a newer local mutation stays pending. A receipt whose base was superseded must not advance tracking or delete journal work merely because a generation still matches. The admitted comparison revision is checked again at the tracking phase and before target-journal deletion. Successful independent siblings remain acknowledgeable even when another receipt is stale.

If the baseline commit succeeded but tracking acknowledgement did not, the same receipt may finish when namespace, fields and server tag exactly match the installed base. That is idempotent recovery, not permission to overwrite a newer base. Deletion retains an invalidated revision to prevent the nil-base ABA case: an old first-upload receipt cannot install evidence from a previous lifetime after delete/resurrection.

The receipt test-first commit `3896cca` reproduced six assertion failures across three cases: wrong record type, duplicate preparation identities and a rejected comparison proof falling through to generation-only acknowledgement. A first repair exposed a write-only validation API being invoked during a target read from the tracking transaction; the subsequent repair separates that read observation while retaining transaction-only verification at target writes. Failed runs are retained, not counted as qualification.

## Lifetime bundles and convergence

A reusable declaration keeps related fields together:

```swift
static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
    .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"])
}
```

All included fields except the explicitly independent ones belong to the lifetime bundle. Newly added fields do not silently become independently mergeable. A reset replaces the old lifetime's aggregate values together with its epoch; copying just the epoch would mislabel old progress.

A per-record base alone cannot guarantee that separate records choose the same reset after partial acknowledgements. `BigSyncLifetimeID.next(after:)` therefore allocates a versioned reset identity: successor generation plus UUID nonce. Allocate it once per logical reset and write **the same ID to every member in the existing domain transaction**. Concurrent successors have one stable nonce tie-break; an observed successor outranks its predecessor regardless of unrelated metadata timestamps.

This is reset ordering, not a global wall clock or per-field clock system. It changes the convention of the epoch value, although it does not add a CloudKit field. Every reset, clear-history, resurrection and recovery writer must adopt it consistently. Legacy opaque IDs are generation zero; the implementation does not pretend random UUID lexical order proves causal history. Reserved malformed IDs and overflow fail explicitly.

The library tests exercise Article/control-shaped records, independent title edits, old-epoch counters, concurrent resets, partial acknowledgements and delayed old resets. They do not establish that the real Common model declarations, validators, reset writers or source-publication readers are integrated. Fetch/upload priority alone does not prove cross-record convergence, and network publication is not atomically bundled by this declaration. Keep the existing graph/publication gate until all relevant records have converged.

## Fast qualification

Use injected protocol providers and the real Realm adapter/journal. Do not sleep for logical days, mutate SDK read-only fields, swizzle, or rewrite production code during tests. Source-edit scripts used to create commits are separate from immutable-source test execution.

From a checkout with the pinned sibling packages and repository toolchain:

```sh
set -euo pipefail
swift test --filter 'SyncRecordRebaseTests|SyncRecordReceiptTests|SyncRecordReceiptValidationTests|BigSyncLifetimeIDTests' 2>&1 | xcsift
swift test 2>&1 | xcsift
```

Cold build time and test execution time are different measurements. Cache builds only under matching OS/architecture/toolchain/dependency keys. Use one focused run and one full run, not repeated week-scale stress by default.

The old characterization suites without opt-in intentionally still demonstrate the legacy bug. The enabled desired-outcome suites must establish the repaired result; a green characterization test is not evidence of a fix. Real CloudKit record tags, OS notification delivery and signed app behavior remain covered by a small separate end-to-end lane.

## Research and limitations

- [Apple: serverRecordChanged](https://developer.apple.com/documentation/cloudkit/ckerror/serverrecordchanged) supplies ancestor/client/server records for upload conflicts and requires retrying from the server record's current tag. Ordinary inbound rebasing still needs local comparison evidence before any upload error exists.
- [SQLite session rebasing](https://www.sqlite.org/session/rebaser.html) distinguishes applying remote changes from updating the remaining local changeset after those decisions. It is conceptual support, not a dependency or algorithm transplant.
- [SQLiteData #354](https://github.com/pointfreeco/sqlite-data/issues/354) motivates schedule-sensitive regression cases; [#95](https://github.com/pointfreeco/sqlite-data/discussions/95) and [#272](https://github.com/pointfreeco/sqlite-data/discussions/272) motivate distinguishing independent fields from coupled invariants; [#356](https://github.com/pointfreeco/sqlite-data/issues/356) motivates separating mocked values from live server behavior. These are historical findings, not assertions about the latest SQLiteData release.
- Steve Troughton-Smith's quoted warning motivates the no-silent-loss work, but the excerpt has no supplied primary permalink. No link or claim of an exact reproduction of his unpublished incident is invented.

CKSyncEngine adoption is excluded. Tapestry cold-start/history replay is deferred to its separate workstream. No application rollout, universal conflict preservation or complete cross-model integration is implied by the standalone library tests.
