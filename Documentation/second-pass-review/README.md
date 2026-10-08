# BigSyncKit second-pass review — 2026-10-08

## Candidate and evidence boundary

This candidate starts at review branch commit
`abf3d465113c6bd332d06bc72f144ca25529dac9` in `lake-of-fire/BigSyncKit`.
The predecessor snapshot was `1ded43c0baf2b203604e8780ec0f4bb0ea0f4b06`;
all five files changed upstream between those commits were fetched by immutable
GitHub blob SHA before this review. `manifest.json` records their identities.
Four hydration-only files match their exact Git blob hashes. The adapter's exact
upstream blob `40bc759c1cf232f69ccd8ba6a73f3751caa39450` was hash-verified and
used to generate `abf3-to-candidate.patch`. Thus that patch contains only this
review's fixes, without treating the upstream receipt-visibility adjustment or
optional-lifetime correction as new changes. The disappearance source is
unchanged in the upstream delta, so its predecessor snapshot is also its exact
current baseline.

No build, native test, runtime qualification, publishing or CloudKit mutation was
performed by this source reviewer. The native tests below are implemented but
**unexecuted**. Swift/Realm actor isolation and runtime assertions need dedicated
Luna qualification on the appropriate Apple platform. Source/file inspection and
the Git blob identity checks do not establish native behavior passing.

## Concrete defects repaired

### Cancel/reset ABA in split operations

Several continuations checked only `cancelSync`. Cancellation increments the
existing `cancellationGeneration`, but migration preparation can clear
`cancelSync` again. An operation suspended between target and tracking commits
could therefore resume under a successor attempt and retire durable work.

Affected paths were deferred relationship cleanup; target-first comparison
receipt acknowledgement; upload tracking acknowledgement followed by target
journal consumption; legacy physical-delete acknowledgement followed by target
journal consumption; and target deletion followed by tracking cleanup.
Batch acknowledgement wrappers also yielded before capturing any operation
ownership, permitting the same transition at their initial suspension.

`RealmSwiftAdapter.operationOwnerValidator()` (around line 2470) captures the
existing cancellation generation, provider identity, account, replica binding,
comparison context, container and database scope. It checks only adapter-owned
state and task cancellation. The adapter issuer and zone are immutable per
instance. No lock, additional persisted field, independent coordinator or new
merge policy is introduced.

The captured predicate now covers entry, writer admission, post-callout checks,
the final synchronous boundary inside owned transactions, split-phase resumptions
and terminal returns. A synchronous callback that retires/recreates an attempt
causes the current owned phase to roll back. An earlier committed phase remains
durable and recoverable; it is not retroactively undone.

Source entry points:

- `RealmSwiftAdapter.swift`: `applyPendingRelationships`, around line 4769.
- `RealmSwiftAdapter.swift`: `cleanUp`, around line 6806.
- `RealmSwiftAdapter.swift`: `acknowledgeUploadedRecords`, around line 8542.
- `RealmSwiftAdapter.swift`: `acknowledgeUploadReceipts`, around line 8595.
- `RealmSwiftAdapter.swift`: `acknowledgeDeletedRecordIDs` and legacy `didDelete`,
  around lines 8835–8861.
- `RealmSwiftAdapter.swift`: prepared-evidence `didUpload`, around line 10742.
- `BigSyncRecordDisappearance.swift`: prepared-evidence `didDelete`, around line
  462. Its adopted physical-disappearance transaction already uses the existing
  evidence-cut authority. This change additionally fences the enclosing operation
  and the handoff from proof-bearing records to the legacy subset.

### Cleanup borrowed provisional tracking deletion

`cleanUp()` enumerated and rechecked live tracking rows while owning a target
write. An independently owned tracking transaction could provisionally mark a
row `deletedRemotely`. Cleanup could hard-delete a durable soft tombstone even
though the tracking owner subsequently rolled back that provisional state.

Initial cleanup selection now produces detached deletion identities from a
committed frozen tracking snapshot. Target writer admission samples committed
tracking state again before deletion. These snapshots are lexical and do not
survive suspension. Existing target journal, live-object, retained-tombstone and
other-app-server checks remain in place. Target account eligibility is checked
before hard deletion. The final tracking cleanup owns and validates its own
transaction and retains the existing state/generation conditions.

### Candidate staging after a revoking callback

`prepareContractUpload` checked its generation before invoking the journal
identity provider. The provider or a later model/contract callback could retire
the attempt synchronously; the method could then commit a staged upload before
the outer preparation check noticed cancellation.

The same captured owner now fences that target staging transaction (around line
11366), after identity validation, before a staged-candidate reuse early return,
at the final transaction boundary and after its awaited commit. An obsolete
candidate is not left durably staged for the successor.

## Native behavior coverage

### Final conflict-decision account authority

The follow-up review of `resolveRecordConflict` established a separate final
account boundary. Core supplies its original synchronizer account-scope lease.
The production account-change observer poisons that lease synchronously on the
posting thread before its actor-owned cancellation and durable invalidation
run. A conflict decision previously validated the lease at writer entry, then
changed target fields, accepted comparison evidence, journal rows, sibling
conflict resolution and its staged submission without a final account check.
The comparison context's binding check cannot detect that synchronous poison.

The same original `validateAuthority` closure now runs after all provisional
target changes, inside the owned transaction before commit submission. A
revoked lease rolls the complete target decision back. The change adds no new
authority, lock or post-durable rejection. Existing tracking cleanup and
idempotent recovery still handle a decision that committed successfully.

`testConflictDecisionRollsBackAfterSynchronousAccountFencePoison` in the existing
`SyncRetainedRecordContractTests` class exercises both Keep Local and Use
Incoming. Its existing comparison hook poisons the actual fixture synchronizer
fence directly, avoiding a global account notification. It checks preserved
target fields and metadata, baseline absence, pending generation, unresolved
conflicts, the exact staged candidate and tracking page/quarantine evidence.
Identity/status providers and a throwing no-service transport avoid CloudKit
requests. Synchronizer cancellation is joined and the adapter fixture owner
handles teardown.

The existing post-target-commit revocation regression now checks actual durable
conflict state outside a write instead of depending on validation-call count.
The four existing Realm fixture classes in this file now explicitly opt out of
the default schema; its configurations already list their object types. No
method is added by that fixture correction. The two preexisting legacy lifetime
tail methods are preserved and are not counted as additions.

This follow-up is additional to the initial `abf3-to-candidate.patch` and its
initial manifest. The final cumulative publication manifest and selected Git
tree identify all current bytes. Independent source review found no additional
actionable issue in this repair; the native method remains unexecuted.

### Split-operation coverage

New class: `SyncSplitOperationOwnershipTests` in
`Tests/BigSyncKitTests/SyncSplitOperationOwnershipTests.swift`. The complete file
is `#if DEBUG`, matching its source seams.

Exact method names:

1. `testCancelledRelationshipCleanupRetainsCommittedIntentForSuccessor`
2. `testRelationshipCleanupRejectsAccountBindingAndTransportReplacement`
3. `testCancelledComparisonAcknowledgementRetainsTrackingAndJournalAfterBaseCommit`
4. `testUploadCandidateStagingRejectsIdentityCallbackCancellationReset`
5. `testCancelledUploadJournalCleanupRetainsSameAndNewerGenerations`
6. `testCancelledPhysicalDeleteJournalCleanupRetainsTombstoneForSuccessor`
7. `testCleanupIgnoresProvisionalTrackingDeletionAndPreservesOtherOwner`
8. `testCancelledCleanupCannotRetireTrackingAfterTargetCommit`

Tests use actual Realm target/tracking writes, prepared upload/deletion APIs,
acknowledgements and deferred relationship application. DEBUG seams sit at real
phase boundaries or immediately after the existing identity-provider boundary.
The tests assert durable state, journal generations, payloads, staged submissions
and recovery. They do not inspect source text. Successful successor recovery
explicitly calls `unsetCancellation()` after migration preparation, because
`didFinishImport()` intentionally pauses ordinary journal forwarding while
`isPreparingFencedMigration` remains true.

Four fixture models declare unique Objective-C names:
`BigSyncSplitOwnerRow`, `BigSyncSplitOwnerContractRow`, `BigSyncSplitOwnerChild`,
and `BigSyncSplitOwnerParent`. Every fixture opts out of Realm's default schema;
every target configuration lists explicit object types. The contract fixture is
included only in the explicit contract configuration. All adapters and temporary
asset directories use the existing `RealmAdapterFixtureOwner` teardown.

## Compatibility and recovery

Record-level journaling, generation-matched acknowledgement, retained-tombstone
representation, one-time recovery discovery, lifetime ordering, schema contracts
and incoming conflict policy are preserved. The latest upstream absent-versus-
empty legacy lifetime correction is retained unchanged.

A rejected continuation throws `CancellationError`. Durable deferred edges and
remaining journal generations stay available for the successor's normal retry.
If the first tracking acknowledgement already committed, the surviving journal
can be forwarded again after normal cancellation is unset. If a comparison base
already committed, its existing idempotent receipt recovery remains available.
If physical target cleanup already committed, its remaining tracking row can be
retired by a fresh cleanup operation. Newer local generations remain protected
by the pre-existing exact-generation checks.

The initial owned implementation files are the adapter, disappearance source
and new split-operation test file listed in `manifest.json`. The final authority
follow-up additionally changes the existing retained-contract tests as described
above. Reader registration adds 35 native source identities: eight new split
methods, this one final-authority method, and 26 inherited legacy optional-
lifetime methods that were absent from its previous inventory. Registration is
not native discovery or a behavior pass.
No additional substantive defect was established in the inspected latest legacy
lifetime ordering, retained tombstone lane upgrade, committed upload-selection,
accepted-baseline or scalar fingerprint paths during this pass.
