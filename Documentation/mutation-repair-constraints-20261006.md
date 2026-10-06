# Mutation repair must retain independent error constraints

## Scope and architecture decision

This follow-up extends BigSyncKit #124 at
`4ca69c9799a6778cf02122179e92757dc1da4fb1`, preserving its response-identity
validation. Its parent remains main `7a47afac6f1f9af9ce47a54c5799e73abef4c867`.
The increment changes one shipping file by +20/-4, expands the same XCTest
source, and adds this report. No Reader pins or target branches are merged.

Review covered the complete mutation drains and retry classifier, the terminal
cutoff/publication/failure/retry path, and current Common Unmark preparation,
reservation settlement and cancellation observation. This is not an exhaustive
repository/runtime qualification. Current Unmark already combines admission and
scope-bound settlement, retains an exact reservation incarnation and calls its
external cancellation source outside the nonrecursive lock. Keep those boundaries;
no further Unmark rewrite is justified by this review.

## Reproduced defect

A correctly identified `serverRecordChanged` or `unknownItem` result was treated
as permission for immediate local repair/retry without retaining independent
conditions carried by that same error. A valid conflict could carry retry-after
or a nested account/network/token/zone/quota error. The existing classifier could
see it initially, but the drain then consumed the outer repairable code and lost
the condition before its next account probe or transport request.

The four affected paths are upload conflict import, missing-upload requeue,
delete-conflict metadata rebase and acceptance-lookup miss. Controlled tests use
constructed error graphs and protocol collaborators; they do not establish that
CloudKit returned these combinations in production or caused observed data loss.

## Correction and preservation

`mutationFailureAllowsImmediateRepair` reuses `CloudKitRetryConstraints`. Only
ordinary miss/conflict codes with no deferred-retry condition can take the fast
path. All other recognized CloudKit codes remain unresolved for the existing
outer lifecycle. A positive allowed-code set avoids another incomplete list of
special cases. Non-CloudKit SDK-internal details retain existing compatibility.

Constrained errors remain under the original requested record ID, with their
original underlying evidence. Valid sibling saves/deletes remain eligible for
acknowledgement. An acknowledgement failure preserves both the local cause and
returned constraints. Cancellation remains terminal. Explicit zero retry-after
is not silently discarded. Unconstrained repairs still converge normally.

Delete `unknownItem` remains the existing idempotent acknowledgement, not a
repair loop. Lookup response-identity rejection remains fail-fast. No new delay,
scheduler, authority, schema, journal, protocol or retry-budget policy is added.
The classifier and outer backoff/recovery code are unchanged.

## Regression-first execution

Swift 6.2.1, Linux x86_64, Swift 6, warnings as errors. The complete actual
`CloudKitSynchronizer+RecordMutations.swift` and complete unchanged
`CloudKitRetryConstraints.swift` execute with explicit CloudKit/error-bridging,
protocol, storage-type and synchronizer-lifecycle collaborators. This does not
execute native CloudKit, the full outer lifecycle, Realm durability or app UI.

| Selected source | Passed methods | Failed methods |
| --- | ---: | ---: |
| #124 predecessor, expanded tests | 46 | 30 |
| Final Debug | 76 | 0 |
| Final optimized | 76 | 0 |
| Bypass save repair guard | 60 | 16 |
| Bypass deletion repair guard | 69 | 7 |
| Bypass acceptance-miss guard | 69 | 7 |
| Ignore deferred-retry condition | 70 | 6 |
| Allow all recognized codes | 60 | 16 |
| Allow account stops | 68 | 8 |
| Allow token recovery | 72 | 4 |

There are 76 distinct XCTest methods: all 40 previous identities plus 36 new
ones. Every original identity passes against the predecessor. Repeated builds
and parameterized error combinations do not inflate the method count. All final
lanes use fresh compiler scratch. Independent reconciliation verifies raw starts,
completions, discovery, process statuses and compiled input/log hashes; no method
is skipped or missing. Optimized portable tests are not app Release qualification.

Earlier 68/70-method stages remain labeled historical. One earlier optimized
outer-wrapper interruption has no final result and is not a pass. No prior
native or applier result is transferred to this increment.

## Exact source and native integration

Shipping blob: `d95ffec53330851c3d6523dd28a86be5d7d1b4dc`.
Test blob: `165a41b07ab5c93a4c9afa3fb575498009c4fb9b`.
Unchanged classifier blob: `92e93fa6b62d2374f72b2f716d345edd661761a5`.

The same `Tests/BigSyncKitTests/SyncMutationResponseIdentityTests.swift` source
is authored for the real package imports using injected adapter/transport inputs.
Shipping and test source pass frontend syntax parsing, not native SDK typechecking.
Reader #286 must register all 76 method identities (36 additions beyond #124's
original requirements) in both native inventories and ensure source membership
before actual Apple SDK discovery/execution. Run alongside existing receipt,
conflict, account, retry, disappearance, retained-quarantine and Unmark coverage.
No native execution, complete Reader build, real network/durability, signed
journey, performance or release acceptance is claimed here. Keep #124 draft.

The companion `bigsync-mutation-repair-constraints-20261006.zip` contains complete
before/after sources, the exact shared tests, sealed manifest/native roster,
standalone runner, explicit collaborators, fault variants and retained logs.
Each runner invocation requires a new evidence directory and writes no checkout.
