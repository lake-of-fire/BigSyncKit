# RA-1 Worker 2 — revision review and remaining collapse boundary

## Status and revisions

PR #10 was merged normally by W1 at `5923cdccc39d90952865d27041ee7ac1168a1ac8`.
This continuation does not modify that completed milestone branch. The existing
production preferences remain unchanged; native qualification and cutover removal
are NOT complete. Do not label authored tests as executed behavior.

- Production input: `2a28f8cfa48fa16c471fa4d54c2689f21567905f`.
- Continuation base: `5923cdccc39d90952865d27041ee7ac1168a1ac8`.
- Work branch: `codex/reading-analytics-w2-review-20260911`.
- Target: `codex/reading-analytics-integration-20260911`.
- Coordinator: https://github.com/aehlke/manabi-reader/issues/21 .
- Revised test commit: `e8b899e57cf5493eca82c4598e449c2bafb08c44`.
- Revised test blob: `e14062aeaa9ce1edb164fde2503993765eab5b6e`.
- Test SHA-256: `7472ad3235f64f8b862ab8675258d5b2fe76155d2d7bcb9b59083750fa3de8c3`.
- API clarification commit: `501f62791c0fd18295c3d2af0cf94accc9c784db`,
  [ReadingAnalyticsRevisionContract.md](ReadingAnalyticsRevisionContract.md).

## Findings and refinements

1. The original conflict script required exactly two uploads. Source permits a
   target G2 journal to outrun tracking G1: a retry can serialize the selected
   current payload while acknowledging only G1, then drain the surviving G2.
   The script now permits one conflict and at most two successful saves. Every
   retry must contain the selected complete value and the final journal must drain.
   A separate test explicitly freezes forwarding and asserts G1/G2 behavior without
   fixture forwarding between preparation and acknowledgement. This is a corrected
   harness assumption, not a native-reproduced production failure.
2. The original conflict fixture authored owner-a under a random synchronizer
   installation. The test author now checks owner equality. Conflict setup uses
   actual inbound replication plus lower-version repair to queue a foreign value
   unchanged. It does not relabel ownership or invent an authored revision.
3. Local-dataset reseeding is not backup restoration. The real Realm test model now
   implements the existing restore interface and local-only recovery flag, matching
   W3's admission policy. Actual backupRestore preparation/bootstrap/reconciliation
   is tested with absent, older, equal and newer server records. Validation-only own
   echoes must not clear copied-state admission; normal import may re-admit it.
4. Retained tombstone upserts do not themselves reject incoming hard deletions.
   The fixture now implements the existing deletion validator, and a test checks
   acknowledged empty/live and empty/tombstone state survives actual deletion input
   and still repairs a subsequently received older positive version.
5. New tests cover final-write selection races, equal-version divergence hidden by
   a new pending journal, failure on the second required winner journal in one
   target transaction, and failure between target and tracking Realm commits.
   The former rolls back both target objects; the latter preserves the committed
   winner and its journal for replay. No compensating downgrade is introduced.

The model is not Common. The suite uses real Realm objects, target/tracking Realms,
registration, journals, adapter application, serialization and acknowledgements.
Only remote IO and existing scheduling/identity-failure hooks are controlled. The
clock case transports an explicitly supplied complete payload; it does not test
Common's Undo delta. Backup cases do not replace real Realm files or exercise the
manual restore handoff. Scripted CKRecords have no real server change tags, so these
are not CloudKit CAS or signed multidevice tests.

## Native test membership

The same file remains `Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift`; W1's
root registration for #10 covers it when this revision is selected. No new manifest
entry is needed, but native discovery and execution remain required. There are now
21 methods, not 12:

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
OwnedRecordRevisionTests/testNewerLocalRevisionAtFinalWriteOverridesSelectionSnapshot
OwnedRecordRevisionTests/testHigherRemoteRevisionStillWinsAfterLocalJournalChangesDuringSelection
OwnedRecordRevisionTests/testEqualRevisionDivergenceAtFinalWriteCannotBeHiddenByNewPendingWork
OwnedRecordRevisionTests/testSecondIncomingWinnerJournalFailureRollsBackBothTargets
OwnedRecordRevisionTests/testTrackingFailureAfterTargetCommitPreservesWinnerForReplayAndOldAck
OwnedRecordRevisionTests/testPayloadAheadOfForwardedGenerationStillDrainsNewerJournal
OwnedRecordRevisionTests/testDivergentOwnEchoIsQuarantinedWithoutApplyingOrAcknowledgingIt
OwnedRecordRevisionTests/testIncomingHardDeletionCannotEraseAcknowledgedEmptyRevision
OwnedRecordRevisionTests/testBackupRestoreWithholdsCopiedRowsUntilActualServerImport
```

Executed: DEBUG Swift frontend syntax parse and exact published-blob verification.
NOT executed: Apple typecheck, native Realm methods, composed Reader tests, live
CloudKit requests, filesystem/process-loss restore or signed multidevice scenarios.
Existing account/cancellation/partial-result/callback suites were left unchanged,
not claimed rerun. Their native execution remains part of W1's approved gate.

## Removal boundary rechecked against actual callers

Core PR #7 at `012d9fb4aa0a0717213c454076d0602550f5eb8a` still contains
`ReaderOrderedV2BigSyncTransport` (blob `228a53ffb7c2c74c71927636ed3f7022189d0feb`).
Its acquire path still calls beginPostBarrierOutboundQuiescence and
establishPostBarrierDrain; it retains source-resume/adoption/completion composition.
Removing the recovery panel's mutation UI is not removal of these runtime callers.
W6/W8 have not supplied the required complete caller-removal SHAs.

| BigSync surface | Remaining responsibility |
| --- | --- |
| AcceptedHeadQuiescence (120 lines), PausedOutboundRecovery (115 lines) | Host-only deletion candidates after accepted-head/paused-reservation callers and their exclusive tests close. Neither is deleted here. |
| OutboundCompletion | Remove source-publication wrappers only with callers; distinguish generic external-owner recovery. |
| OutboundQuiescence synchronizer extension | Mixed final-drain/source APIs and ordinary principal/admission/submission handling. Keep the latter. |
| BigSyncOutboundQuiescence persistent coordinator | Preserve decoding, real outstanding submissions and generic admission; never reinterpret an old barrier as settled. |
| OutboundReplayRecovery | Keep exact operation identity, shared callback collector, generation-matched reconciliation and unknown outcomes. |
| CloudKitRecordStore / RecordMutations | Keep once-only result delivery, partial results, record identity checks, retries and acknowledgements. |
| SyncedEntityProtocol / journal / identity / model hooks | Keep general local atomicity, owner/binding and version selection contracts. |

Ordinary save/delete still reaches admitOutboundBatch and
modifyRecordsHoldingOutboundLease. The 982-line low-level outbound file, 633-line
mixed synchronizer extension, 439-line replay extension and host-only extensions
remain present. This continuation removes zero runtime paths, and introduces no
new barrier, publication requirement, phase machine, second journal or success stub.

## W8 raw bootstrap-save question

The inspected ordinary-save path has no public arbitrary-CKRecord admission entry.
`admitOutboundBatch(for:)` and `modifyRecordsHoldingOutboundLease(...)` are internal,
require the actual active run/principal, and are used with model-prepared journal
generations and the ordinary response handler. Calling public
`CloudKitRecordStore.modifyRecords` directly is raw IO, not that admission contract.
An account precheck alone does not supply outstanding-submission bookkeeping.

W8's create-once baseline selection is not automatically implemented by the RA-1
revision preferences. Journal-backed model replication may reuse the ordinary path,
but its selection semantics and real initialization binding require W8/W1 agreement.
Do not fabricate a public wrapper, transport grant or claim a direct CKDatabase save
is covered. Fixture/local baseline work may proceed; production raw-bootstrap
transport binding remains an explicit unresolved boundary. The project-wide 5k stop
is not permission to add another API family in this continuation.

## Accounting and next gates

This continuation: **0 new/reimplemented production lines, 0 production deletions,
0 source moves, +314/-6 test lines** (870 total test lines), two existing docs updated.
No unrelated production test, manifest, workflow, identity format or callback changed.
The earlier merged batch had 562 test lines and 276 documentation lines.

W1 must execute the 21 methods on the approved native runner and compose actual
Common conformance. W6/W8 must supply caller-closure evidence before export removal.
Until those gates close, this is a test/contract refinement, not completed transport
collapse or qualification. No release/default merge, forced ref update, live baseline
selection/migration, account/zone deletion, journal clearing or Mac worktree change.
