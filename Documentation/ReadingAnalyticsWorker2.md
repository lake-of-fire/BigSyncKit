# RA-1 Worker 2 — published code and remaining removal boundary

## Source and ownership

PR #10 merged at `5923cdccc39d90952865d27041ee7ac1168a1ac8`. PR #11 continues on
`codex/reading-analytics-w2-review-20260911`, targeting the dedicated integration
branch. W2 edits BigSync only; W1 alone approves/merges integration and root pins.
Latest production checkpoint: **634aed9f2ff52a39275f11004a909be3deea0599**.

| Commit | Actual implementation |
| --- | --- |
| 7b5183b4 / 3cbed7cd | Deliver save/delete/upload/zone completions outside operation catches; a throwing consumer is not a second result or zone-loss evidence. |
| 33e72a75 | Original account/run revalidation on failed zone IO; fetched zone identity must match. |
| 95e25ba7 | Reconciled manual restore uses existing current-identity repair journals before intent release; completed-but-unremoved intent retries cleanup only. |
| 634aed9f | Reject nested lease downgrades/restores; validate complete manual event bytes; require reconciled completion for identity admission and event acknowledgement independently of the optional intent file. |

## Reconciled restore: actual caller join exists

`BigSyncClientIdentity.withReconciledManualBackupRestore(transactionIdentifier:
configurations:reconciledObjectTypes:_:rollback:)` is the concrete Realm extension.
It uses the existing exclusive identity lease and durable manual intent/event/receipt.
The caller installs final copies reconciled against preserved originals: independently
known current rows are admitted; backup-only rows remain withheld. After publishing the
new sentinel/binding, the helper validates admitted rows and journals their unchanged
values before completing the handoff. It does not change owner, stateRevision, payload
or audit timestamps. The existing backupRestore adapter preserves current-identity
journals, so a known empty43 stays admitted/repairable against stale42.

The earlier missing-caller report is obsolete:
- W8 Common **190d5d5d5ad5ec510086c1e4aa57a76c8f64508d**, ReadingAnalyticsManualRestore,
  registers the live journal provider, invokes this API, validates final installed
  copies without reflagging known rows, and rebinds the local guard after the actual
  returned receipt/current identity.
- W8 Core **0296c8c4620d66d4b47974368d52916c8176415f** calls that Common method from
  the real UserDataBackupManager.resumeRestore production path.
- W3 Common **9dccf505cf2799f94ac0a4538b235708f6a9686c**, policy blob27481a93,
  delegates its stable-ID replacement overload to the same real W8 method and removes
  the obsolete blanket normalizer. These owner branches still need W1 composition;
  source binding is not an executed physical restore qualification.

Pass final installed configurations and concrete reconciled model types, not originals
or staged files. Register the existing live provider before calling; never prepare a
new installation from inside the replacement/finalizer. Required schema, model,
identity and journal failures throw. A post-event failure retains handoffPending and
the caller's physical journal; it does not authorize rollback to the old files.
Automatic/raw backup restoration still withholds copied state. No second journal,
restore mode, record-floor registry, cloud save API or blanket admission was added.

## Latest source review corrections

The recursive registry mutex allowed read-only identity inspection, but also allowed
retainShared to downgrade the outer exclusive file lease or a nested withExclusive
defer to release it. Both mutating reentry paths now throw restoreInProgress before
altering that lease. Read-only current identity remains usable by the existing
finalizer. This uses the same mutex/file lease, not another lock coordinator.

Manual restore event identity previously accepted only a header and UUID, even when
the full receipt was truncated or its required contract unknown. Event decoding now
uses the same full receipt parser; old complete raw receipts and automatic UUID events
retain their format. Malformed manual data is not treated as absence or automatic
recovery permission.

Reconciled journal completion is a requirement of the event itself. A missing intent
file no longer bypasses matching completed-receipt checks in prepareInstallation,
currentInstallationIdentifier or markRestoreResetCompleted. The event check also
requires the expected new sentinel. Failure leaves the state pending; no file-clearing
operation or user restore was executed during this coding pass.

## Revision APIs and test source

Existing preferIncomingRecord/preferExistingObject, strict journaling, target-write
revalidation and generation-matched acknowledgements remain unchanged. See
[ReadingAnalyticsRevisionContract.md](ReadingAnalyticsRevisionContract.md).
The existing native fixture is real Realm/journal/adapter code, not Common's models.
Two tests call the real synchronizeAdapter loop with scripted remote IO. Scripted
records have no real server-assigned change tags. The older fixture's permissive
backup selector is not W3's corrected restore-admission policy.

Already-registered file: Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift.
Test implementation e8b899e57cf5493eca82c4598e449c2bafb08c44, blobe14062ae,
is unchanged by the production coding passes. Exact 21 native methods:

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

No tests, compiler/typecheck, syntax checks, native tools or CloudKit ran in this pass.
Only source/blob/diff and GitHub publication integrity were inspected. Earlier syntax
results do not apply to new production code. Native qualification must exercise the
bound physical restore -> adapter backupRestore -> late42 path, interrupted repair,
missing/malformed handoff records and reentrant preparation, as well as the preserved
account/cancellation/partial-outcome suites. None is claimed passed or newly authored.

## Remaining code removal is caller-dependent

Live W6 Core #7 still at1fa7174e retains ReaderOrderedV2BigSyncTransport and old
startup/cutover callers. W8/W3 source retirement is not complete closure of those
exported calls. The repeated coordinator request names the actual APIs; no false
compatibility alias or disabled check is used to pretend they disappeared.

| Surface | Remaining action |
| --- | --- |
| AcceptedHeadQuiescence / PausedOutboundRecovery | Apply prepared120/115-line deletions only with caller closure and their exclusive probe/script/manifest references. |
| OutboundCompletion | Remove source-publication wrappers with callers; distinguish supported external-owner recovery. |
| Synchronizer OutboundQuiescence | Separate final-drain/source exports from ordinary principal/admission/submission functions. |
| Persistent outbound coordinator / replay | Keep existing checkpoint decoding, uncertain submissions, original operation identity and generation-matched reconciliation. |
| RecordStore / RecordMutations / journals / identity / preferences | Retain general synchronization safety. |

Ordinary save/delete still invokes admitOutboundBatch and
modifyRecordsHoldingOutboundLease. No old checkpoint is cleared/reinterpreted as
settled. The235-line source patch in the prior handoff is unapplied, only a first
slice, not the entire mixed-code cleanup and not counted as production deletion.
No cutover export was removed in this latest commit. W6/W8 caller-closure evidence
remains required before that dependency-closed W2 batch can be published.

W1 excluded cloud baseline election/CAS/publication; W8 withdrew its raw-save API
request. Explicit selected local input suffices for isolated composition. Production
multi-install baseline distribution remains a release decision, not another W2 API.

## Accounting and completion boundary

Latest production delta vs0e7c9f1e: **+53/-17**, two existing files, no new API/file/model,
framework, source move, test or manifest change. Cumulative PR#11 production vs5923:
**+357/-117** across six source files (including the earlier104-line restore helper).
Prior test refinement+314/-6 is unchanged; docs and prepared-but-unapplied deletions
are separate. No cap increase is implied.

Published independent W2 corrections and actual application restore bindings are now
identified. Export/probe removal is still unfinished coding, not merely a test gate.
W1 owns exact source composition, native qualification and release authorization.
No integration/default/release merge, force push, live migration/baseline choice,
account/zone/journal clearing, production activation or foreign/Mac-worktree edit.
