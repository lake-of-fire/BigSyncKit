# 2026-10-10 sync dependency source audit

This is a bounded source audit, not native qualification. No build, test,
benchmark, package installation, native app launch, generated test harness or
CI dispatch was performed. Publication review found only push/pull-request/manual workflow triggers; head commits use supported `[skip ci]`. Draft publication is parent-controlled; no workflow dispatch occurred.

## Compared identities and finite inventory

| Repository | Fresh main | Local hotfix HEAD | Remote Reader v3 pin |
| --- | --- | --- | --- |
| lake-of-fire/BigSyncKit | `012d54e431e8a49da922f2fe3791b3976e38fa0f` | `eca27ef8977e0d8a4f430a226ad3ae1eccfa38df` | `461cd077e5509a20063812aa72fa07771410bc9a` |
| lake-of-fire/RealmSwiftGaps | `ffb9b50248291b7f13d6df42a27d8f1383ca618e` | `d5f21d442467ad5afd5a221014add25d3e6a33d1` | `430671d1108f9e6872033699c0dbb202e8dadc81` |

The isolated branches are `audit/core-dependencies-20261010`. BigSyncKit origin
is `https://github.com/lake-of-fire/BigSyncKit.git`; the older standalone
aehlke checkout was not used. Hotfix working directories were read only.

Inventory is the reachable commit set selected by Git's commit-date window
`2025-10-10T00:00:00Z` through `2026-10-10T23:59:59Z`, inclusive. Changed files
are the union of nonempty names from `git log --format= --name-only -m` over
that set (merge-parent changes included). This definition is finite and
reproducible; it does not claim every historical change was line-reviewed.

| Source | Dated commits | Changed paths | Changed source paths | Main ancestry |
| --- | ---: | ---: | ---: | --- |
| BigSync local hotfix | 524 | 167 | 63 | Every dated commit is an ancestor of main |
| BigSync remote v3 | 680 | 239 | 63 | Remote pin itself is an ancestor of main |
| Realm local hotfix | 22 | 10 | 5 | All 22 dated commits absent from main ancestry |
| Realm remote v3 | 71 | 24 | 7 | All 71 remote-only commits absent from main ancestry |

The remote sets contain the local histories. Ancestry proves incorporation,
not preservation of every behavior after subsequent edits. BigSync main differs
from remote v3 in four runtime files: `BigSyncLocalDomainAdmissionDeferredError`
(new on main), `CloudKitSynchronizer+Sync`, `BigSyncRecordDisappearance`, and
`RealmSwiftAdapter`. They retain newer ownership/publication refinements;
replacing main with v3 would discard them.

## Pending PR comparison

`gh pr list --state open --json number,url,headRefName,headRefOid` returned no
open BigSyncKit PRs and these RealmSwiftGaps PRs:

- [RealmSwiftGaps #7](https://github.com/lake-of-fire/RealmSwiftGaps/pull/7),
  `1f71f67dc9717ab5ee8d36d14fe3c218fe487bd4`:
  configuration/file identity, coalesced opens, default protocol opener
  dispatch, completed-open waiter eviction/replacement checks, and transient
  cache release are already pending. Do not duplicate their port.
- [RealmSwiftGaps #4](https://github.com/lake-of-fire/RealmSwiftGaps/pull/4),
  `5d106736075f97a47ce980a745260728054cfca8`:
  the earlier cache identity port. #7 adds three changed source/test paths
  over #4, including the suspended-open/waiter boundaries.

Neither pending head contains caller cancellation admission in actor write
helpers. Both retain synchronous `writeIfNeeded` behavior. Remote v3 contains
further owned-write and captured-store APIs that these PRs do not cover.

## Classification and caller trace

| Behavior cluster | Classification | Evidence and disposition |
| --- | --- | --- |
| BigSync durable journal and explicit refresh | Ported, source reviewed | `BigSyncPendingMutation`, `SyncedEntityProtocol`, adapter forwarding. Add-before-refresh and same-transaction metadata boundary remain intact; account/binding provenance belongs to each generation. |
| BigSync acknowledgement/generation retention | Ported, source reviewed at boundary | Adapter `preparedGenerationIsEligibleForActiveTransport` validates prepared tracking generation; unscoped target edits may advance independently; scoped records additionally validate journal/transport ownership. Immutable prepared upload evidence remains present. |
| BigSync account/cancellation/publication | Ported with newer main refinements | `checkSynchronizationAttempt` rejects cancellation, stale attempt and poisoned account authority; `checkRunContext` verifies run/binding; cancellation rotates attempt and awaits callback/adapter barriers. `CloudKitSynchronizer+Sync` rechecks around awaits/callouts. |
| BigSync local dirty validation/quarantine | Ported in main | Main already has manual restore `validation` and `retainedDeletionQuarantineEvidence` APIs seen in local dirty diffs. Dirty adapter/project/test edits were not copied. |
| Realm configuration identity/coalescing/file replacement | Partial on main; pending #7 | #7 covers these changes; source difference reviewed against remote v3. Remains dependent on that existing review. |
| Realm pre-cancelled/suspended-open write admission | Missing on main and pending heads; focused local repair | Check cancellation before opening, after the suspended opener, and inside the write boundary. Route all three throwing convenience overloads through the same actor write helper. Resolve references after the transaction has refreshed state. |
| Realm queued independent writes, captured-store admission, native version witness | Unresolved | Remote v3 `RealmOwnedWrite`, `RealmWriteAdmissionSignal`, `RealmStorageAdmission` depend on a broader actor/SDK contract and private Realm 20.0.5 begin-ticket behavior. No blanket port or dependency/platform change made. |
| Realm active-version unlimited normalization and cache lookup/adoption restrictions | Missing beyond pending #7; unresolved | Remote v3 normalizes nil/zero/UInt.max, actor-constrains the cache protocol, rejects stale delayed cache lookup and retires unfenced adoption. Requires coordinated API review with consumers. |
| Realm text-field state and CSV field | Ported/equivalent | Both existing source files are byte-identical between main and remote v3. |
| Retired inventories, temporary source harnesses, generated project files | Divergent; not restored | Historical source-only harnesses/inventory requirements are retired by current hotfix AGENTS. Dated correction added to the evidence decoding document. |

Reader caller evidence (read-only hotfix checkout):

- `Vendor/ManabiReaderCore/Sources/ManabiReaderCore/ManabiReaderCore.swift`,
  `loadDurableMarkedReadSentenceArchives` and
  `acknowledgeDurableMarkedReadSentenceArchive` call the actor write helper.
  These recover corrupt rows and acknowledge an intent generation inside the
  transaction. Cancellation must not invoke those mutation closures afterward.
- `Reader/FeedCategoryView.swift`, `markAllFeedsAsSeen` and
  `setShowsNewBadge` call the throwing reference convenience helper.
  Reference resolution must follow write admission/refresh.
- `Home/Home.swift` calls the throwing configuration convenience helper.
- BigSync `RealmSwiftAdapter` initializes target writer Realms through the
  actor cache but independently manages sync attempt/operation write fences.
  The focused Realm change does not replace those BigSync fences.

## Focused implementation and regression source

RealmSwiftGaps changes only `RealmBackgroundActor.swift`,
`RealmExtensions.swift`, and
`Tests/RealmSwiftGapsTests/RealmCancelledWriteAdmissionTests.swift`.
The authored behavior cases verify that a pre-cancelled configuration write
does not change an existing object, a cancelled reference submission does not
consume its single-use reference (a live successor can still use it), and
the throwing convenience API does not mutate. The fixture has a unique explicit
Objective-C name, opts out of default schema, and uses explicit `objectTypes`.
The gate forces cancellation before entry without timing assumptions.

These sources have not compiled or executed. Cancellation after the opener
has begun is source-fenced but lacks a deterministic suspended-opener behavior
test in this isolated main version; PR #7 supplies opener observation hooks.
No claim is made about cancellation during synchronous write-lock acquisition,
queued asynchronous transaction ownership, nested `writeIfNeeded`, or commit
notification successor ownership. The latter remain the remote owned-write
port's responsibility. The two fire-and-forget `writeAsync` APIs retain their
existing independent task semantics; this change covers structured throwing
callers only.

## Recorded second pass and remaining coverage

The deterministic pass enumerated all commits/changed paths, compared source
diffs and pending heads, then traced the journal/receipt/account/cancellation
and actor-write consumers described above. A shuffled cluster pass using seed
`20261010` revisited these six clusters in order: BigSync metadata, BigSync
acknowledgement, BigSync attempt ownership, Realm cache, Realm actor writes,
Realm convenience writes. This supplements the inventory; it is not a random
sampling claim of full semantic coverage.

All seven Realm changed runtime paths were compared; five were read at their
affected boundaries and the two view paths checked for equivalence. Of the 63
BigSync changed runtime paths, the deep boundary review covered
`BigSyncPendingMutation`, `SyncedEntityProtocol`, `RealmSwiftAdapter`,
`ModelAdapter`, `BigSyncClientIdentity`, `BigSyncBackgroundActor`,
`CloudKitSynchronizer`, and `CloudKitSynchronizer+Sync`. The other 55 runtime
paths have ancestry/diff-level coverage only. The complete native regression
suites, all app mutations/configurations, reset migrations, and all non-source
changed paths have not been semantically re-reviewed. In particular,
configuration exclusion symmetry and replacement-zone migration remain for
the Reader/Core owner's audit. Ancestry must not be presented as executed
runtime evidence or exhaustive proof of those paths.

Before publication, review repository workflows and authorization. Future
verification uses current targets/test plans and actual native discovery/results,
not manually maintained method inventories or copied-source packages. A signed
macOS CloudKit release gate, authentic migration/account replacement, and native
runtime execution remain deferred under the source-only instruction.

## Exact changed-path remainder

Immutable source heads and exact date bounds define the complete commit corpus above. Reproduce commit IDs with `git log --since=2025-10-10T00:00:00Z --until=2026-10-10T23:59:59Z --format=%H <source-sha>`; reproduce changed paths by adding `--format= --name-only -m`. This appendix records every runtime source path. Boundary-reviewed paths still need full behavior closure; other paths remain semantic-review work. All non-source paths remain unreviewed except the named historical-doc correction.

### BigSyncKit (63 runtime paths)

- `Sources/BigSyncKit/QSSynchronizer/BackupDetection.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncAccountScopeLease.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor+DomainFollowUp.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor+DomainTransitionReadiness.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncClientIdentity+InjectedStore.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncClientIdentity.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncDeadlineRace.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncDurablePublicationEvidence.swift`
- `Sources/BigSyncKit/QSSynchronizer/BigSyncLocalStateConfiguration.swift`
- `Sources/BigSyncKit/QSSynchronizer/CancellableCloudKitCallback.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitAccountAvailabilityGate.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitChangeFeed.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitDatabase.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitLossClassifier.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitRecordStore.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitRetryConstraints.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSubscriptionStore.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSyncHealth.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Cancellation.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Private.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+PublicationRestoration.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+RecordMutations.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Subscriptions.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift`
- `Sources/BigSyncKit/QSSynchronizer/CloudKitZoneStore.swift`
- `Sources/BigSyncKit/QSSynchronizer/KeyValueStore.swift`
- `Sources/BigSyncKit/QSSynchronizer/ModelAdapter.swift`
- `Sources/BigSyncKit/QSSynchronizer/Operations/CloudKitSynchronizerOperation.swift`
- `Sources/BigSyncKit/QSSynchronizer/Operations/FetchDatabaseChangesOperation.swift`
- `Sources/BigSyncKit/QSSynchronizer/Operations/FetchZoneChangesOperation.swift`
- `Sources/BigSyncKit/QSSynchronizer/Operations/ModifyRecordsOperation.swift`
- `Sources/BigSyncKit/QSSynchronizer/PersistentAssetManager.swift`
- `Sources/BigSyncKit/QSSynchronizer/SyncedEntityState.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncInboundSemanticValidation.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncIncomingRepresentation.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncLegacyTrackingEvidence.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncLifetimeID.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncPendingMutation.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncPendingMutationInventory.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordBaseline.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordContract.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordDisappearance.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidenceInspection.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordIdentity.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncRecordReconciliation.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncServerRecordEvidence.swift`
- `Sources/BigSyncKit/RealmSwift/BigSyncSynchronizationAudit.swift`
- `Sources/BigSyncKit/RealmSwift/DefaultRealmSwiftAdapterProvider.swift`
- `Sources/BigSyncKit/RealmSwift/PendingRelationship.swift`
- `Sources/BigSyncKit/RealmSwift/QSCloudKitSynchronizer+RealmSwift.swift`
- `Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift`
- `Sources/BigSyncKit/RealmSwift/RebuildProvenance.swift`
- `Sources/BigSyncKit/RealmSwift/SyncedEntity.swift`
- `Sources/BigSyncKit/RealmSwift/SyncedEntityProtocol.swift`
- `Sources/BigSyncKit/RealmSwift/SyncedEntityType.swift`
- `Sources/BigSyncKit/SyncStatusViewModel.swift`
- `Sources/BigSyncKit/Unused Archive/CloudKitSynchronizer+Sharing.swift`
- `Sources/BigSyncKit/Unused Archive/DefaultRealmProvider.swift`
- `Sources/BigSyncKit/Unused Archive/MultiRealmResultsController.swift`
- `Sources/BigSyncKit/Unused Archive/QSCloudKitSynchronizer+MultiRealmResultsController.swift`

### RealmSwiftGaps (7 runtime paths)

- `Sources/RealmSwiftGaps/CachedRealmsActor.swift`
- `Sources/RealmSwiftGaps/RealmBackgroundActor.swift`
- `Sources/RealmSwiftGaps/RealmCSVTextField.swift`
- `Sources/RealmSwiftGaps/RealmExtensions.swift`
- `Sources/RealmSwiftGaps/RealmOwnedWrite.swift`
- `Sources/RealmSwiftGaps/RealmTextField.swift`
- `Sources/RealmSwiftGaps/RealmWriteAdmissionSignal.swift`
