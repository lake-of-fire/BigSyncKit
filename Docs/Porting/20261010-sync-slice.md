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

## 2026-10-11 continuation: independent Realm write ownership

This continuation supersedes the first pass's unresolved owned-write entry and
its statement that the scheduled helpers retain their previous implementation.
No builds, test execution, benchmarks, native launches, generators, dependency
installs or CI dispatch were authorized or performed. Native compilation and
execution remain unavailable under that explicit instruction.

### Exact continuation inputs

The source pins and main bases in the first table remain unchanged. Actual PR
heads were re-read with `gh pr view --json headRefOid,url` before editing:

| PR | Observed head | Disposition |
| --- | --- | --- |
| [Realm #7](https://github.com/lake-of-fire/RealmSwiftGaps/pull/7) | `1f71f67dc9717ab5ee8d36d14fe3c218fe487bd4` | Cache/open fencing pending, not duplicated |
| [Realm #4](https://github.com/lake-of-fire/RealmSwiftGaps/pull/4) | `5d106736075f97a47ce980a745260728054cfca8` | Earlier cache identity port pending, not duplicated |
| [Realm #19](https://github.com/lake-of-fire/RealmSwiftGaps/pull/19) | `0465eb53e4cc71ab1497472a2427a4271d0cf647` | Existing write-admission port extended locally |
| [BigSync #160](https://github.com/lake-of-fire/BigSyncKit/pull/160) | `bbdd7d1ccfa0f55114a1ab213b82687a3c5137be` | Existing audit report extended locally |

### SDK source compatibility, without changing pins

RealmSwiftGaps's manifest remains Swift tools 5.8, Realm from 20.0.3, macOS 12,
iOS 15, with its retained lock at Realm 20.0.4/Core 20.1.4. The local SDK
checkout supplied all three immutable versions for read-only comparison:

| SDK | Peeled commit (not annotated-tag object) | Matching Core commit |
| --- | --- | --- |
| Realm 20.0.3, manifest minimum | `6260534683132eb981338c7c39fd5e69205876e6` | 20.1.0 `15493076ad9fef22c16cc64cbfbf9e5b65c385f9` |
| Realm 20.0.4, retained lock | `600e187711e5fa4e8e2c0429cacee27e8e44f112` | 20.1.4 `4cc46f8607516226e5465062fdacd088fbd94552` |
| Realm 20.0.5, remote hotfix bridge source | `ca03df491ec5e4bb8af0bca1e9957d08c10ec2da` | 20.1.5 `b4192c46305570577c4d08df790fddec5ab3aa04` |

Compared `Realm/RLMRealm.mm`, `RLMRealm_Private.h`, `RLMAsyncTask.mm`,
`RLMAsyncTask_Private.h`, framework `Realm/Realm.modulemap`, SwiftPM
`include/module.modulemap`, and `RealmSwift/ObjectiveCSupport.swift`: these are
byte-identical across the three SDK commits. Both module maps expose
`Realm.Private` and its async-task/private-Realm headers. The private header
exposes the `actor` property and notify-only `beginAsyncWrite`; public
`ObjectiveCSupport.convert(object: Realm)` returns that native Realm.
`RealmSwift/Realm.swift` differs only in two unrelated deletion calls between
20.0.3/20.0.4 and 20.0.5. Its ownership/cancellation/commit machinery is unchanged.
Core `src/realm/object-store/shared_realm.cpp` is byte-identical across the
three listed Core commits, including `run_writes`, notify-only admission and
queued cancellation. This is source compatibility evidence, not typechecking
or runtime qualification.

The bridge retains the upstream compiler branches: Swift compiler 6 and later
take `isolated any Actor = #isolation`; earlier compilers retain
`@_unsafeInheritExecutor`, resolve the SDK Realm owner, and call the same
isolated bridge. The SDK's own corresponding conditional signatures are
present at the manifest minimum. The unchecked carriers keep Realm, closure
and non-Sendable generic result on the original actor/task; they do not move
mutation to another task. Source review cannot prove compiler/module packaging
against a built binary, so that remains a future native check.

### Production boundary and all helper contracts

The remote `RealmOwnedWrite.swift` and `RealmWriteAdmissionSignal.swift` are
ported into maintained package source; only the bridge compatibility comment
is extended. Neither SDK nor dependency manifest/lock is modified. Existing
actor helpers and all convenience helpers use this one boundary:

| Entry | Before mutation | Missing/deleted reference | Completion ownership |
| --- | --- | --- | --- |
| Actor configuration write | Entry/open/admission cancellation checks | n/a | Original caller |
| Actor reference write | Same checks; resolve inside admitted transaction | Throws `unableToResolveObject` | Original caller |
| Throwing static configuration write | Delegates to actor configuration helper | n/a | Original caller |
| Throwing static reference write | Same boundary; resolve after admission | Successful no-op | Original caller |
| Throwing static reference array write | Same boundary; resolve all after admission | Skip unresolved members; all unresolved is successful no-op | Original caller |
| Scheduled static configuration write | Independent actor task uses same boundary | n/a | Returned task handle |
| Scheduled static object write | Capture reference synchronously; resolve after admission | Successful no-op | Returned task handle |

Scheduled helpers now return `@discardableResult Task<Void, Swift.Error>` as
the remote hotfix API does. Discarding a handle retains the established
independent fire-and-forget lifetime; retaining it permits cancellation,
joining and error observation. Cancelling the submitting task does not cancel
an already independently scheduled write. Errors propagate through the handle
instead of being silently turned into success. The throwing signatures retain
their previous Void result and missing-reference distinctions.

The actual isolated current Core/Common sources and read-only shipping sources
were searched for direct uses, stored function references, `return` expressions,
and callback signatures. Current Core's lookup-history alert discards the
configuration helper's result; current Common's note-deletion alert,
`onChange(isDone)`, and explicitly Void `persistDraftTextIfNeeded` discard
the reference helper's result. No typed stored function reference or
`return Realm.writeAsync(...)` use was found in the inspected Core/Common/root
Swift corpus. The instance `readerRealm.writeAsync` sites are distinct SDK
calls and were not changed. Source compatibility still needs native compiler
verification; API users outside this inspected corpus may need to adapt typed
function references to the returned handle.

The SDK public `beginAsyncWrite` callback cannot replace this bridge while
preserving original-task execution: Core rolls back an open non-notify-only
transaction when that callback returns. Public `asyncWrite` itself can infer
rollback rights from another task's currently open transaction after a queued
caller is cancelled. The existing notify-only bridge separately records actual
admission, disarms before cancelling its own SDK ticket, and never treats the
SDK's cancellation-generated callback as admission. No parallel queue, detached
mutation task or new lock around Realm state is introduced; the small existing
signal synchronizes only continuation/admission flags.

Before commit, cancellation or a thrown operation error rolls back only the
admitted write. After asynchronous commit submission it awaits the unchanged
commit callback and returns the durable result even if cancellation arrives.
After a synchronous commit in the operation, the frozen native version witness
distinguishes the admitted write from a notification's successor transaction;
it neither commits nor cancels that successor. There is no late cancellation
check after commit that could report an already committed edit as rollback.
The operation must not cancel and replace its own admitted transaction itself;
throw to signal failure. `writeIfNeeded` stays unchanged for explicit,
same-owner nested transaction use.

New `RealmOwnedWritePortTests.swift` exercises all seven queued helper entries,
same-actor owner survival and successor progress, generic non-Sendable result
and task-local identity, cancellation during the admitted body, asynchronous
and synchronous post-commit cancellation, synchronous notification successor
ownership, actor unresolved-reference error, single/all/mixed array and
scheduled deleted-reference no-op semantics, and explicit nested
`writeIfNeeded`. The production signal's existing runtime regressions are
ported as `RealmWriteAdmissionSignalTests.swift`. These are authored XCTest
sources; none executed. Fixtures use explicit schemas and unique Objective-C
names, opt out of default schema, and retain/join scheduled handles in teardown.

### Captured-storage and cache blocker, with exact requirements

`CachedRealmsActor.swift` remains deliberately unchanged. The remote captured
storage API cannot be safely grafted onto main's path-only cache: if a file is
replaced at the configured path, a newly captured current inode can match
filesystem validation while `cachedRealm` still returns a Realm opened for the
previous inode. A separately recomputed path key cannot establish that handle's
original identity. This is an actual dependency/API boundary, not missing
permission to do a small edit.

To complete it without duplicating pending ports:

1. Resolve/compose existing #7's configuration/resource identity keys, coalesced
   owner/waiter opens and completed-open revalidation. Preserve its scoped
   eviction behavior and its pending regression source.
2. Adopt the remote `CachedRealmsActor: Actor` isolation contract, reject stale
   suspended read-only lookup, and retire configuration-only supplied-Realm
   adoption. Audit all conformers and existing adoption callers; main's current
   cache tests deliberately rely on the legacy adoption API.
3. Normalize nil/zero/UInt.max active-version authority across the native
   configuration round trip, keeping finite limits distinct.
4. Compose `RealmStorageAdmission` and the actor's captured-admission overload
   and `prepareStorage` from remote `430671d`. Preserve weak shared pending
   creation ownership, O_EXCL reservation, descriptor/path device+inode
   validation, rejection of external appearance, read-only and seeded creation,
   and post-open revalidation. Do not relabel an old cached Realm.
5. Audit Core/Common/Reader storage capture and read-actor callers against those
   exact APIs. Register and eventually execute native replacement/first-open,
   coalescing, unlimited-key and delayed-lookup regressions through current
   targets/test plans on an authorized run. No copied-source harness or retired
   compiler/native receipt validator is required.

This prerequisite chain is unresolved pending #7 composition and the public
cache API change. No partial admission API is exposed by #19. All seven Realm
inventory runtime paths now have a concrete disposition: the two text fields
are equivalent; owned-write and signal are ported; the actor/extensions are
ported for write behavior and partial for cache/admission behavior; the cache
protocol/storage boundary is blocked as above.

### Complete BigSync source-port classification

Every one of the 63 runtime inventory paths was compared by immutable Git blob
at remote v3 `461cd077` and main base `012d54e`. Fifty retained paths are
byte-identical: their hotfix implementations are already ported, rather than
merely reachable in history. Ten paths are absent on both heads, preserving
their retirement: `CancellableCloudKitCallback`,
`CloudKitSynchronizer+Cancellation`, all four old `Operations` files, and all
four `Unused Archive` files listed in the exact-path appendix. Their current
structured transport/synchronizer entry points are retained; they are not
missing port candidates.

The remaining three inventory paths contain newer main behavior:
`CloudKitSynchronizer+Sync` routes temporary local domain admission through
the existing fenced delayed retry; `BigSyncRecordDisappearance` keeps one
original owner through missing-server proof/legacy tails;
`RealmSwiftAdapter` centralizes immutable lifecycle/provider ownership and
committed evidence snapshots, then validates original ownership around
journal forwarding, reset, import, receipt, reconciliation and target/tracking
publication boundaries. Main adds `BigSyncLocalDomainAdmissionDeferredError`,
which is outside the historical 63-path set. These newer implementations are
preserved. No remaining missing BigSync runtime source port was identified.

The expanded review follows adapter owner validation, generation/receipt
selection, missing-server replay, deferred relationships and reset/publication
continuations through the structured stores. Record, zone and subscription
stores verify individual CloudKit results; subscription lookup maps only
unknown-item to absence. The retired callback/operation files must not be
restored around these APIs. BigSync main already calls the public
`asyncWritePreservingOwnership` primitive now supplied by the Realm port.
Source equivalence closes port classification for the remaining paths; it
does not prove all historical runtime behaviors or external consumer correctness.
The earlier 55-path semantic re-review limitation remains accurate as a
qualification limitation, not as an unidentified missing-source backlog.

### Advanced-main read-only review — 2026-10-10 local continuation

Fresh isolated fetch confirmed main `2eea7591140f4a599f80f71e07f8e6d77b71daee`, descending from the recorded runtime baseline `012d54e431e8a49da922f2fe3791b3976e38fa0f`. The intervening implementation commit is `2ec9fe1` (deletion migration fixture write ownership), joined by merge `eb1c3f7` and #161 merge `2eea759`. Exact tree comparison changes only `Tests/BigSyncKitTests/ChangeFeedMigrationResumeTests.swift` (106 additions, 6 removals); the Sources delta is empty.

Source review inspected the original foreign-writer fixture's shared one-shot `InboundDeletionProvisionalWrites.cancelIfOwned`, which consumes ownership before rollback and is reused by admission, timeout fallback and deferred cleanup. Two authored tests cover admitted successor preservation and pre-admission failure release. This newer test-fixture work is preserved on main and is not copied or reverted in this documentation draft. No production code or dependency pin changes are needed to retain it.

This confirms runtime-source equivalence to the previously recorded baseline, not independent semantic qualification of every runtime path or execution of #161's tests. No builds, tests, compiler checks, benchmarks or CI ran in this continuation. Queued-write native qualification, #7 captured-store composition, cross-owner caller contracts and historical/non-source coverage remain explicit gaps. The task-17 checkpoint `62f0d706e112c339a6e039569c1a4c35fc5c7550` is unchanged.
