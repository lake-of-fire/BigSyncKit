# RA-1 Worker 2 — published tests and remaining collapse boundary

## Status

First deliverable is published; native qualification and cutover removal are NOT
complete. No production source has been changed merely to add a new revision API.
The existing adapter preferences already express the required decision; the new
native suite must qualify their execution before this is called proven.

- Preserved input/base: `2a28f8cfa48fa16c471fa4d54c2689f21567905f`.
- Work branch: `codex/reading-analytics-w2-sync-20260911`.
- Target: `codex/reading-analytics-integration-20260911`.
- Draft PR: https://github.com/lake-of-fire/BigSyncKit/pull/10 .
- Coordinator: https://github.com/aehlke/manabi-reader/issues/21 .
- Actual API note: `99c404cf07a19caf76b6b66fd916c40f1e08e674`,
  [ReadingAnalyticsRevisionContract.md](ReadingAnalyticsRevisionContract.md).
- Native test source: `eefb5d24edc719b9ed009de49b801f44be721064`,
  `Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift`.
- Verified test blob: `453bfbde493f34d01de977fab97b6c464c6de241`.
- Test SHA-256: `0b4e588d76e5eefd21bc15e5f77dd6e773561bba8e36ce3e590677884c3aec57`.

## Implemented test surface

The fixture is an actual Realm Object using the existing integer codec and model
validation/preference protocols. Target and tracking Realms, mutation registration,
throwing journal generation, import, serialization, own-echo validation and
acknowledgement are real BigSync code. The two conflict-loop methods call the real
`CloudKitSynchronizer.synchronizeAdapter`, including outbound admission and normal
`serverRecordChanged -> forceSave import -> reprepare -> acknowledge` handling.
Only remote IO is scripted; unexpected remote operations throw. The direct-upload
run uses the same real account-validation/test-run setup as existing native tests,
not a replacement admission coordinator or fabricated terminal receipt.

This model is not the Common model. W3 owns real application conformance and W1
owns qualification of the composed tuple. The clock test proves no Common inverse:
it checks transport of an explicitly supplied complete higher-version payload.
Reseed uses isolated in-memory data and a controlled empty destination; it does not
select a real user's baseline or qualify stale-copy admission.

W1 must register the new file in its explicit Tuist native test membership. SwiftPM
already discovers files in the existing BigSyncKitTests directory. Exact class and
method identifiers (12 methods, with bounded forceSave/tombstone variants):

```text
OwnedRecordRevisionTests/testLatePopulatedDownloadRequeuesNewerEmptyValueWithoutReauthoring
OwnedRecordRevisionTests/testHigherIncomingRevisionReplacesPendingValueAndMintsNewGeneration
OwnedRecordRevisionTests/testOlderOwnEchoAndG1AcknowledgementCannotRetireG2
OwnedRecordRevisionTests/testClockRevisionThenUndoTransportsNewestCompletePayload
OwnedRecordRevisionTests/testEqualRevisionReplayNormalizesSetsAndIgnoresAuditMetadata
OwnedRecordRevisionTests/testEqualRevisionDivergenceIsQuarantinedWithoutLosingPendingWork
OwnedRecordRevisionTests/testOmittedEmptyMembershipIsTheSameDomainReplay
OwnedRecordRevisionTests/testChangedOwnerCannotAddressExistingRecordEvenWithHigherRevision
OwnedRecordRevisionTests/testRequiredJournalIdentityFailureRollsBackDomainAndRevision
OwnedRecordRevisionTests/testReseedKeepsOriginalOwnerAndRevisionUnderNewJournalIdentity
OwnedRecordRevisionTests/testRealUploadLoopRetriesLowerServerConflictWithNewerLocalEmptyState
OwnedRecordRevisionTests/testRealUploadLoopReplacesStalePendingMarkWithNewerServerEmptyState
```

Executed: Swift frontend syntax parse with DEBUG defined, local file hashing and
GitHub blob readback. NOT executed: Apple typecheck, native Realm tests, composed
Reader tests, actual CloudKit requests or signed multidevice scenarios. No green
historical suite is relabeled as evidence for this test commit.

## Removal inventory and exact dependency boundary

W6 Core PR #7 currently describes old orchestration as not yet removed. W8 Core
PR #6 retains its real backup/restore handoff while implementing explicit import.
Neither has supplied the required dependency-closed exported-API removal SHA.
W2 has requested that confirmation in the umbrella issue. Do not delete exported
APIs before their callers disappear, and do not leave successful no-op aliases.

| Current BigSync surface | Disposition for the collapse batch |
| --- | --- |
| `CloudKitSynchronizer+AcceptedHeadQuiescence.swift` (120 lines) | Entire extension is accepted-head host orchestration; candidate for deletion with W6 accepted-head callers and exclusively matching tests/probes. |
| `CloudKitSynchronizer+PausedOutboundRecovery.swift` (115 lines) | Sealed-domain reservation ownership handoff; candidate with W6/W8 paused-adoption callers. Not the ordinary replay implementation. |
| `CloudKitSynchronizer+OutboundCompletion.swift` | Source-publication completion/token wrappers are candidates. Distinguish the generic external-owner recovery overload before deleting a whole file. |
| `CloudKitSynchronizer+OutboundQuiescence.swift` | Mixed. Remove final-drain/reservation/source-publication entry points only with their clients. Retain ordinary principal, admission, submitted-request identity and local-settlement handling. |
| `CloudKitSynchronizer.swift`, `CloudKitSynchronizer+Sync.swift`, `BigSyncBackgroundActor.swift` | Remove cutover configuration/tokens/run branches and forwarding methods as one closed batch. Preserve ordinary run/account/cancellation checks and independently used terminal/change-feed contracts. |
| `BigSyncOutboundQuiescence.swift` | Mixed persistent format, physical admission, submission markers and old barrier operations. Never delete or reinterpret existing barrier/submission state as settled. Preserve diagnostic decoding and ordinary submission safety while removing domain-only operations. |
| `CloudKitSynchronizer+OutboundReplayRecovery.swift` | Retain actual operation-identity validation, callback replay, generation-matched handling and unknown-result preservation. A failed operation lookup does not prove mutation rejection. |
| `CloudKitRecordStore.swift`, `CloudKitSynchronizer+RecordMutations.swift` | Retain once-only callback collector, partial-result preservation, response identity checks and normal retry/acknowledgement flow. |
| `SyncedEntityProtocol.swift`, `BigSyncPendingMutation.swift`, semantic hooks, identity/binding/restore files | Retain strict journals, generation safety, model winner semantics and owner/binding boundaries. No Reader Undo protocol belongs here. |

This is a dependency checklist, not a new lifecycle design. It does not authorize
clearing a physical checkpoint, starting source mode, or silently reopening an old
paused installation. A real-data decision about an existing barrier remains a
named W1/W8 release boundary; isolated code/tests do not need that permission.

## Runtime reachability retained at this commit

Ordinary upload/delete still invokes `admitOutboundBatch`, then
`modifyRecordsHoldingOutboundLease`. The latter persists original record IDs,
prepared generations and optional long-lived operation identity before submission.
A known server result is not enough: marker retirement follows generation-matched
local handling. Missing results, lost proxies, cancellation and account replacement
must not be turned into successful settlement during collapse.

The 982-line low-level outbound file, 633-line synchronizer quiescence extension,
439-line replay extension and host-only extensions are still present, not counted
as removed. Ordinary work reaches portions of the first three; the host-only paths
remain callable by existing Core. RA-1 itself acquires no new barrier or publication
proof. Removing a feature consumer is not proof that all this general code is dead.

## Accounting and next gates

At test commit `eefb5d24`: 562 test lines, 154 API-document lines, 0 new/reimplemented
production lines, 0 production deletions, 0 identical moves. This status note is
additional documentation only. No unrelated production tests, workflows, manifests,
identity formats or callback behavior were altered.

1. W1-approved native runner executes the 12 methods and retained account,
   cancellation, partial-result, callback lifetime and generation suites. Fix any
   demonstrated adapter gap, not the public API simply because a new model exists.
2. W6/W8 publish removal SHAs for accepted-head, final aggregate drain/seal, paused
   reservation/source publication and completion clients. W2 performs the closed
   removal batch, preserving ordinary physical uncertainty. This gate is currently
   outstanding; the assignment must not be labeled fully collapsed.
3. W1 composes the actual Common conformance and owning Mark/Undo transactions with
   this transport, registers tests and records native/signed qualification separately.

No release/default merge, force push, live migration, account/zone deletion, journal
clearing, source activation or Mac worktree change was performed.
