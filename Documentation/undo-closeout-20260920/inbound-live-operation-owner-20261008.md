# Inbound live records retain one operation owner

## Exact review baseline

This change starts from the reviewed convergence draft #144 at
`aab1c268d273d971a5d77d21667e4fe7a7735d8b`, tree
`b8cf035a63522bb46808223c3f9765ce22370161`. It preserves the independently
authored #138 retained cleanup, #139 journal forwarding/import completion,
#140 inbound deletion and #141 deletion recovery changes. The adapter baseline
blob is `7f4b5e6cb98fef89bf08f858efc83bcf815ca3f5`; the split-operation test
baseline is `4c39f7876bdc0fd8fa20fd136149779f33251bd0`.

## Concrete admission gap

`saveChanges(in:forceSave:)` captures a provider and selects incoming work on
`BigSyncBackgroundActor`, then previously moved target writes to that provider's
`RealmBackgroundActor` handles. The target path checked Task cancellation and
the current `cancelSync` Boolean. A direct adapter caller could keep its Task
alive while cancellation/reset, account, replica binding or transport activation
replaced the operation's original authority during a suspension. The old legacy
response could then mutate its original target and publish tracking state.

This is a direct package API contract finding. Ordinary ChangeRequestProcessor
cancellation already cancels and joins its child Task; the source schedule does
not claim that clearing a Boolean reactivates a permanently cancelled Task.
The selected RealmSwiftGaps owned-write bridge also checks Task cancellation
after the body and before commit submission while it still owns the write.
Those protections do not establish immutable account/provider/transport ownership
for a Task that remains alive.

Record rebasing is not a universal operation fence: legacy models can return
`notAdopted`, and the rebase context validates journal binding rather than the
adapter cancellation generation, provider and entire transport identity.

## Bounded consolidation

The method now captures the existing `operationOwnerValidator()` once before
selection. It inherits that original capability across target selection,
recreation claims, semantic quarantine writes, target application, imported
tracking and relationship publication, comparison forwarding/quarantine and
final result publication. Write closures validate at admission and before exit;
resumed continuations validate after physical settlement. `writeSyncedEntities`
accepts the original capability from its sole caller instead of adopting a new
owner after suspension.

Target application uses the provider's existing writable target Realm handles
owned by `BigSyncBackgroundActor`, as inbound deletion and deferred relationships
already do. This keeps owner validation synchronous with actual transaction
admission. The `applyChanges` and private `applyRecordRebase` annotations change
to that same actor; both have only the `saveChanges` production callsite. Shared
codecs remain unchanged. The provider's writer handles, DEBUG seeding and the
detached candidate abstraction remain in place. Target configurations are
unchanged; no new queue, clock, lock, persistent table or capability is introduced.

Moving this bounded application work also moves synchronous decoding and any
CKAsset file access to the adapter actor. Native tests must qualify the change's
actor isolation, notification behavior and asset decoding alongside the existing
record, relationship, retained-deletion and ownership cases. This change does
not add a decoding or performance redesign.

A valid target commit can precede owner retirement. A later rejection preserves
that durable target, leaves the old tracking publication unperformed and prevents
the caller's receipt/token-last page commit. A fresh operation can reconcile the
same server response; an exact later local journal generation still wins replay.

## Authored behavior regressions

Two new methods in `SyncSplitOperationOwnershipTests` use the existing target
and persistence hooks and an acknowledged legacy candidate without a comparison
baseline. Each executes four owner replacement schedules while leaving the
calling Task alive:

- `testInboundLiveRejectsCancellationResetAccountBindingAndTransportReplacementBeforeTarget`
  requires target fields, original encoded tracking bytes and journal state to
  remain unchanged, then requires a fresh owner to apply the incoming value.
- `testInboundLiveRetainsCommittedTargetAfterOwnerReplacementAndFreshRetry`
  retires the owner after the target commit and before tracking admission. It
  requires the incoming target to remain durable, original tracking bytes and
  absent journal to remain unchanged, and a fresh retry to succeed. A subsequent
  local edit followed by replay must retain the exact local value and target/
  tracking generation.

The incoming dates deliberately select the remote legacy value; equal dates on
fresh replay remain accepted by the existing `>=` timestamp rule. These fixtures
do not depend on opting into the comparison schema. Their old-body negative
control is the direct-owner schedule, not a cancelled Task resuming.
Fresh retry also decodes the persisted tracking archive and requires the incoming
server change tag, proving tracking convergence after the target was already
durable. The tag uses the existing CloudKit SDK fixture setter and fails the test
if that SDK cannot construct the required system-field fixture.

No Swift compiler or Apple runtime is available in this authoring environment.
Whitespace, original-byte preservation, selector uniqueness and delimiter checks
are structural checks only. The two methods are authored evidence pending native
compile, discovery and execution at the exact candidate/dependency identity. A
component result does not qualify the assembled Reader or signed CloudKit gate.

## Adjacent review boundary

The public `validateAuthoritativeOwnUploadRecords` path retains Task/current
Boolean protection across its own refresh and semantic-quarantine publication.
Its normal processor caller cancels its Task. A direct-call immutable-owner
quarantine admission schedule remains separate, unqualified review work; this
patch does not claim that every public adapter API has the same lifetime fence.
