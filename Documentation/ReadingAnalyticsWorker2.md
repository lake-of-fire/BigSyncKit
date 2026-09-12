# RA-1 Worker 2 — cutover API retirement

## Current batch

The requested production decoupling is implemented on
`codex/reading-analytics-w2-cutover-removal-20260911`, based on the normal BigSync
integration merge `5fd6064578645e7356636cf6d9dd6605cad557c6`. That merge includes
PR #11's reconciled restore, identity-lease and completion-delivery corrections.
This continuation does not rewrite its completed branch or change integration refs.

Paired application prerequisite: Core PR #11 at
`ffcfe79157ecdc35d8417c682b9c2cae959f3c1e`. It removes the ReaderOrderedV2 and
ReaderOrderedCutover runtime families and binds installed-state startup validation
to ordinary synchronization. The older blocked-on-Core status is superseded.
W1 selects the exact composed tuple and alone approves/merges integration.

## Actual production removal

Deleted complete extensions:

- `CloudKitSynchronizer+AcceptedHeadQuiescence.swift`.
- `CloudKitSynchronizer+PausedOutboundRecovery.swift`.
- `CloudKitSynchronizer+OutboundCompletion.swift`.

Removed cutover exports from `BigSyncBackgroundActor` and the synchronizer:
accepted-head sealing, beginning/establishing/completing final drains, reservation
sealing, source-publication start/resume/validation/completion, paused ownership,
cutover token abort/abandon and the domain-owner convenience wrappers. No substitute
API, success alias or always-permitted guard was added.

Removed the associated PostBarrierDrainAuthorization/CompletedPostBarrierDrain/
PostBarrierOutboundQuiescence capabilities, snapshot-provider configuration,
source-owner run field, terminal-receipt field, held cutover ownership state and
begin/cancel/terminal branches. Ordinary batch admission now takes only the current
principal, not a cutover/source owner. Terminal completion retains its normal
journal, cursor, account, binding, reentrancy and durability checks.

The mixed OutboundQuiescence extension retains its existing ordinary principal,
admission, request descriptor, submission and settlement implementation plus generic
checkpoint inspection/recovery. No persistence format, account-generation algorithm,
journal-selection rule or physical-uncertainty contract changed.

## Explicitly retained

- `BigSyncOutboundQuiescence.swift`: checkpoint decoding, historical barrier fields,
  ordinary admission/leases and submitted-state bookkeeping. Its internal barrier
  primitives and low-level process tests remain; no public cutover grant or ordinary
  source-owner branch reaches those obsolete application transitions.
- `CloudKitSynchronizer+OutboundReplayRecovery.swift`: exact long-lived identity,
  replay collector, provenance of terminal failures and generation-matched repair.
- `CloudKitRecordStore`, `CloudKitSynchronizer+RecordMutations`, Realm adapter/model
  preferences, target/tracking recovery and the sole pending mutation journal.
- Installation/account/binding fencing and all reconciled manual-restore lease,
  intent, event, receipt and unchanged-record journal handling from merged PR #11.
- Ordinary terminal receipts, optional generic semantic callbacks and change-feed
  recovery. They contain no new Reader baseline, Mark/Undo or lifetime policy.

An old barrier still blocks ordinary admission. An absent proxy, empty journal,
expired wait or lost process lock never settles a submitted request. Existing
checkpoint decoding and explicit generic recovery are preserved, not repurposed
as Reader migration. See [Outbound transport](OutboundQuiescence.md).

## Exclusive tooling removal

Deleted the AcceptedHeadQuiescence and PausedOutboundRecoveryProbe collaborator/test
files, their two runner scripts, and their two obsolete integration documents.
Removed the dedicated paused-recovery workflow job and accepted-head command/path
references. General native transport and cross-process jobs/scripts remain; no new
workflow or runner was introduced.

## Evidence and remaining work

No tests, compiler/typecheck, syntax checks, native runner or CloudKit operations
were run in this coding pass. Source preimages, generated postimage hashes, Git
ancestry and changed-file diffs were reviewed for safe publication only.

The existing 21 OwnedRecordRevisionTests methods are unchanged. General
account/cancellation/partial-result/replay/journal/restore tests remain. Native
terminal/settlement tests which name removed cutover capabilities still require
compile/test follow-through; retire only the obsolete cases or retarget their
independent safety assertion. Do not restore deleted production APIs for old tests.
W1 also owns root explicit source/test membership cleanup for the paired Core batch.

The production architecture for this assignment stops here unless composition
exposes a concrete BigSync defect. Remaining work is source selection and compile/
test fallout, not another transport design, cloud baseline-election service or
application migration framework. Production readiness, live-user-data permission
and signed multidevice qualification remain separate and are not claimed.

All persisted state is untouched. No default/release/integration merge, force push,
account/zone deletion, journal clearing, live restore or protected Mac worktree
operation was performed. Exact line accounting belongs to this batch's GitHub
compare; prior additions and deleted lines are never netted to hide the project cap.
