# Default conflict mutation authority and finite public API inventory

## Review scope and concrete package contract

This final bounded followup starts from #144's reviewed own-upload checkpoint
`531af8ebe92e385213ef86556590a8e49fd6b6a2`. It closes original operation lifetime
for `resolveRecordConflict`, `refreshRecordConflict` and the implicit-public
extension method `discardResolvedRecordConflictArchives`.

These APIs already accept a caller authority callback and preserve context,
baseline revision, conflict identity and local generation. Reader supplies an
account lease; ordinary synchronizer cancellation permanently cancels its Task.
The default callback is empty, however, so a direct package caller whose Task
remains alive could cross cancellation/reset with unchanged context and borrow
the successor's lifetime. This is one package authority pattern across three
entries, not three newly observed Reader incidents.

Each entry now inherits the existing immutable operation owner and combines it
with the supplied caller authority. Both checks remain in physical target writes;
quarantine retirement receives that same combined authority. Resumed target and
tracking continuations revalidate before carrying the decision into another
phase or returning. Exact semantic/context/generation fences remain intact.

The independent #147 correction at `b65f5c2cceb6027332269cf8420337079ef9d7c6`
requires the caller's lease after provisional refresh/archive mutation. Its
DEBUG hook and both real account-fence poison fixtures are retained. The combined
validator preserves its final caller check and adds immutable package ownership.

An already committed phase remains durable if authority retires afterward. For
archive cleanup, valid quarantine retirement can precede a rejected target archive
delete; the retired tracking evidence stays retired while the archive remains.
Fresh cleanup selects resolved target archives again and can finish deletion.
No validator claims to undo a settled transaction.

## One scalar predicate, strict provider ownership after setup

A private `operationLifecycleValidator` shares the existing immutable scalar
predicate across setup-owning paths and the full operation validator. It captures
cancellation generation, account, binding, rebase context, container and database
scope. The full operation validator also retains the exact provider. This adds no
state, lock, clock, queue or optional provider relaxation.

`ensureSetup` explicitly treats a non-nil provider as insufficient when setup is
interrupted. `performSetup` sets interrupted readiness and publishes its provider
before journal recovery, initial setup and forwarding finish; only successful
completion clears the interrupted flag. A failed setup can therefore leave a
non-nil provider that the next legitimate setup must replace. Setup-owning public
operations validate the original scalar lifecycle before/after `ensureSetup`,
then capture the resulting strict provider owner. Restricting provider creation
to callers that entered with a nil provider would reject this supported recovery.

The scalar factoring preserves the reviewed own-upload and `didFinishImport`
behavior and applies the same contract to conflict refresh. The nil-only provider
policy from concurrent #148 is deliberately not adopted. Its useful initial
provider scenario is covered using the existing semantic schema, with a second
positive for interrupted non-nil replacement.

## Authored regressions and retained external coverage

The three new `SyncRetainedRecordContractTests` methods use the existing
`unbasedRecoveryFixture` and default authority, leaving the caller Task alive:

- `testDefaultConflictResolutionRejectsCancellationResetAndFreshRetry` covers both
  resolution choices at target admission and retains object fields, exact journal,
  submission, unresolved conflict, quarantine and page evidence; a fresh decision
  then resolves and retires the corresponding evidence.
- `testDefaultConflictRefreshRejectsCancellationResetAndFreshRetry` retires the
  operation after provisional review replacement, requires rollback of that
  replacement without losing the current local edit, then refreshes under a new
  operation with the exact current journal generation.
- `testDefaultConflictArchiveCleanupRejectsCancellationResetAndFreshRetry` retires
  the operation after provisional archive deletion. The earlier valid tracking
  retirement stays committed; the archive and journal remain intact until a fresh
  cleanup completes.

The exact #147 methods retained are
`testConflictRefreshRollsBackAfterSynchronousAccountFencePoison` and
`testConflictArchiveDiscardRollsBackAfterSynchronousAccountFencePoison`, including
their shared existing-schema helper. They exercise actual Reader-style lease
poison while the adapter namespace remains unchanged.

Two `SyncSemanticIntentTests` positives require actual setup contracts:
`testOwnUploadAllowsInitialProviderSetupUnderOriginalLifecycle` starts without a
provider and initializes through the public echo API;
`testOwnUploadRetriesInterruptedNonnullProviderUnderOriginalLifecycle` stops
forwarding after setup published a replacement provider, then requires echo setup
to create a ready replacement and retain the exact local target/tracking journal
generation. No #148 method name or new model is silently assumed present.

These additions are authored evidence pending native compilation, discovery and
execution. The existing supplied-authority tests remain in place. Structural
checks and peer source review do not establish Apple runtime, actor-performance,
assembled Reader or signed CloudKit release qualification.

## Finite public mutation inventory

This inventory distinguishes operation lifetime from exact persistent capability.
It includes the implicit-public extension. Companion source changes for reset,
committed observation and delivery are reviewed by the parent and sync reviewer;
their selected union must be checked at the final #144 head.

| Public entry or family | Authority at mutation boundaries | Review boundary |
| --- | --- | --- |
| `cancelSynchronization`, `waitForCancellation` | Retire generation and cancel/join owned Tasks | Lifecycle control; no incoming work adoption |
| `activateAccountScope`, `activateReplicaBinding`, `activateTransportNamespace` | Actor-serialized authority replacement; original capabilities reject changed fields | Synchronous identity control; status work has #139 owner |
| `resetSyncCaches`, `unsetCancellation` | Destructive reset generation, setup readiness and reset transport ownership | Parent's bounded reset/#142 review; not blanket-qualified here |
| `prepareChangeFeedReset`, `beginChangeFeedServerBootstrap`, `reconcileAfterChangeFeedServerBootstrap`, `finishChangeFeedReset` | Account/epoch/reset mode plus supplied authority and original reset capability | Parent's reset review and final composed source |
| `cleanUp` | Original immutable owner across target/tracking admissions | Existing guarded split operation |
| `saveChanges` | Original full owner across selection, five main writes, inherited tracking insertion and publication | Reviewed same-actor live repair, two four-schedule tests |
| `validateAuthoritativeOwnUploadRecords` | Original scalar lifecycle across setup, then full provider owner across validation/quarantine/return | Reviewed echo repair plus four schedules and setup positives |
| `deleteRecords` | Original full owner across target/tracking and return; disappearance evidence for contract paths | #140 plus #141 durable-target recovery |
| `persistImportedChanges` | Original relationship preparation owner at target/tracking admissions and after callbacks | Existing relationship split-operation tests |
| `preparedRecordsToUpload`, `preparedRecordDeletions` | Immutable preparation lifecycle/provider/issuer/context; exact generation receipts | Preparation and ACK review; no new universal receipt |
| `didUpload` overloads, `didDelete` overloads | Original full owner plus prepared issuer/generation; target comparison receipt before tracking consumption | Existing ACK guards, #138 late cleanup sampling |
| `didFinishImport` overloads | Original scalar setup lifecycle, then full owner through forwarding/quarantine/assets/status | #139; shared scalar factoring preserves setup semantics |
| `requeueMissingServerRecords` overloads | Original owner plus committed disappearance evidence cut | Existing disappearance authority review |
| `rebasePendingDeletionMetadata` | Immutable scalar/context/generation; exact prepared deletion generation and committed target cut | Metadata-only response; provider/reset lifecycle reviewed with reset union |
| `saveToken`, `commitInboundPage` | Transport/epoch/cancellation admission; exact page identity/receipt and token-last publication | Sync review of final composed reset/committed-read union |
| `acknowledgeCommittedInboundIdentityBatch` | Exact delivery identity plus operation admission | #149 owns full provider/lifecycle and committed-delivery observation correction |
| `resolveRecordConflict`, `refreshRecordConflict`, `discardResolvedRecordConflictArchives` | Original full owner combined with supplied caller authority; exact conflict/context/generation/revision | This followup plus retained #147 final caller checks |

Read-only semantic blockers, quarantine/server evidence, server boundary, epoch,
committed identity delivery, conflict snapshots and conflict export are separate
committed-observation contracts. Parent reviews their concrete composed read
changes. Synchronous policy/configuration properties do not submit Realm writes;
this table does not invent an operation queue for those setters.

The finite scope closes the evidenced public mutation authority patterns while
preserving intentionally different epoch, delivery and prepared-generation
capabilities. It does not infer runtime qualification from source coverage.
