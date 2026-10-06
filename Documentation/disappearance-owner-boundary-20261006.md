# Physical-disappearance ownership refinement — October 6, 2026

## Source selection and decision

Retain the existing independent Realm writer, target-first disposition, journal,
comparison and transport architecture. Strengthen the lexical read/write
boundaries instead of adding another queue, ledger or persistent authority.

Review began at BigSync #96 `e15f300c18b263d96c1a82287e9076e87cfd76ac`.
Publication is based on its normal successor
`4c306609ca5d965359eda69da3ffbc0b2fbdcd93`, preserving the independently merged
account-restoration and Unicode work. The complete disappearance file and both
extracted owner methods are unchanged between those checkpoints. No whole
adapter replacement, Reader pin or protected target change is included.

Only one shipping file changes: `BigSyncRecordDisappearance.swift`.
Original Git blob: `8bc82068e4f8cbdefac8ded946dcf42b54524ad9`.
Refined Git blob: `405278f9231ae0b1d7310431963d25c5a5601aa7`.

## Reproduced failures

1. A target mutation can inspect tracking to preserve legacy local work. Its
   synchronous refresh notification can revoke the operation after initial
   validation. The original code can commit the target disposition before a
   later tracking phase notices the revocation. Contract, identity, eligibility,
   lifecycle and journal callbacks expose the same missing final-owner check.
2. The registry identity callback can commit a successor target version while
   the adapter owner remains valid. A snapshot frozen before that callback then
   publishes or prepares obsolete evidence.
3. A multi-record missing-server response captured a new evidence cut for every
   record. After one completed target/tracking pair, its continuation could
   adopt a resumed attempt's generation for the next record.

These are controlled executions of the actual methods, not observed production
incidents or proof of the historical signed-sync failure's cause.

## Structural change

Three private helpers in the existing source share the checks. The lexical
non-suspending scope validates the existing cut and captured provider before
callback-bearing identity validation, again before its body, and after every
successful return. The final check reads only adapter-owned fields; it does not
run another registry callback that could invalidate the check itself.

Read-only scopes refresh an unowned live handle, validate identity through a
frozen view, then obtain their final frozen view after that callback. An already
frozen input stays pinned. Another owner's provisional write is never treated as
this reader's mutation authority.

The original provider is retained through target and tracking phases. A final
throw inside an owned write rolls back only that uncommitted phase; earlier
committed target state is not undone. Original thrown model errors are preserved.
The multi-record comparison path uses one invocation-scoped cut, while the
intentionally unbound legacy route keeps its existing fallback. Empty missing-
server responses retain their original no-op after complete prepared-input
validation; the new attempt capture must not change that compatibility rule.

Original compare-and-swap predicates, submission validation, generation-matched
journal consumption, lifecycle policy, target-before-tracking ordering and wire
representations remain. Per-file snapshots are not globally atomic or a new
quiescence guarantee. No schema, timer, task, lock or persistent state is added.

## Executed verification

Swift 6.2.1 / Linux x86_64 / Swift 6 / warnings as errors. Both configurations
explicitly enable the pre-existing DEBUG suspension hooks. The full production
disappearance file plus the two current verbatim owner methods execute with
explicit Realm, CloudKit, model/registry and adapter collaborators under Tools.
They do not execute the SDK's write queue, disk durability or full adapter graph.

| Exact source selection | Passed methods | Failed methods |
| --- | ---: | ---: |
| Current unmodified disappearance source | 23 | 17 |
| Refined Debug | 40 | 0 |
| Refined optimized with test hooks | 40 | 0 |
| Remove final scope owner check | 31 | 9 |
| Remove post-identity / pre-body owner check | 39 | 1 |
| Remove captured provider check | 38 | 2 |
| Freeze final evidence before identity callback | 38 | 2 |
| Restore per-record attempt recapture | 39 | 1 |
| Remove preserved empty-response no-op | 39 | 1 |

The 40 unique methods include all 15 existing snapshot tests, whose bodies remain
unchanged. Only their fixture/helper visibility changes so the 25 new histories
can reuse them. Started/completed identities and independent SwiftPM discovery
were reconciled against the exact source roster for every completed matrix run;
there are no missing, duplicate or skipped methods.

The updated checked-in runner was also executed in fresh selected-source layouts
in both configurations: 40 passes each. Those wiring layouts contain the actual
complete disappearance source and actual owner-method excerpt, not a complete
repository checkout. Eight separate extractor utility tests pass, including
missing/duplicate declarations, changed isolation/access, malformed extraction,
and refusing to overwrite a destination. They are not eight native sync tests.

A compatibility pass reproduced an introduced empty-response regression in the
first refactor, restored the original no-op, and added positive/negative cases.
That intermediate 39-pass/1-failure run is not a production-predecessor result.

Source and native declarations pass frontend syntax parsing only. Earlier
collaborator-isolation compilation failures, an ambiguous edit-anchor rejection,
and a verification-wrapper attempt that expected unavailable XCTest XML are
retained separately in the evidence packet. None is promoted to a qualifying
run. Final accounting uses complete runtime start/end records plus independent
discovery, not the zero-method Swift Testing summary.

## Native acceptance — authored, not executed

New source file `Tests/BigSyncKitTests/SyncDisappearanceOwnerBoundaryTests.swift`
extends the existing `SyncUndoCloseoutW1Tests` owner. It uses its real file-backed
fixture, actual Realm.observe delivery during the target transaction, existing
suspension hooks, and a task-local read boundary for registry cancellation.
A second queue-confined Realm commits only fixture tracking metadata to force
the real refresh event. No shipping test hook is added.

Register this source and the following seven methods in the owning Reader graph
and both required inventories before native discovery/execution:

- `testDisappearanceTrackingRefreshGenerationABARejectsBeforeTargetCommit`
- `testDisappearanceTrackingRefreshAccountReplacementRollsBackTarget`
- `testDisappearanceTrackingRefreshTaskCancellationPreservesTarget`
- `testCurrentDisappearanceTrackingRefreshPreservesLegacyIntent`
- `testDisappearanceRefreshRejectionCanRetryWithoutReauthoringContent`
- `testDisappearancePreparationRejectsIdentityProviderCancellation`
- `testCurrentDisappearancePreparationRetainsExactEvidence`

Run them with the existing disappearance, journal, native writer, retained
quarantine, account and Unmark suites. New source registration is not proof of
Xcode discovery. Native SDK typechecking/execution, complete package/Reader
build, CloudKit, WebKit/UI, signed journeys, performance and application Release
remain unqualified by this lane. Keep the PR draft until owning acceptance.
