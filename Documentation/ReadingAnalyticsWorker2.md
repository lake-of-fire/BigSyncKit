# RA-1 Worker 2 — actual implementation and remaining joins

## Published source

PR #10 merged normally at `5923cdccc39d90952865d27041ee7ac1168a1ac8`.
PR #11 continues on `codex/reading-analytics-w2-review-20260911`, targeting
`codex/reading-analytics-integration-20260911`. Coordinator: root issue #21.
Only W1 merges integration batches; W2 does not modify completed milestone branches.

| Commit | Implemented change |
| --- | --- |
| `7b5183b4e38320af92ce9817fc990936f0cc2ca0` | Save/delete drain completions are delivered outside operation catches, preventing downstream errors from causing a second delivery. |
| `3cbed7cd89f9413ab2d03fe765a95013fbad8950` | Apply that separation through upload composition and zone lookup/creation; a consumer error is not zone-loss evidence. |
| `33e72a7500d0d7e13b7d827f68a0442c107b7a83` | Revalidate original run/account on zone failures; require the fetched zone identity to match. |
| `95e25ba7119733e0fb91e3ff17a5bd12846d2b55` | Reconciled manual restore queues unchanged known records under the new identity before releasing its existing durable intent. Completed intent cleanup is retryable without repeating replacement. |

The latest restore commit changes four paths: BackupDetection, BigSyncClientIdentity,
SyncedEntityProtocol, and a 104-line Realm-specific handoff extension. It does not
modify RealmSwiftAdapter, introduce another restore mode, or change revision ordering.

## Concrete restore defect and correction

W8's final-copy normalizer can correctly preserve independently current empty A@43
against backup A@42, setting known43 awaiting=false and copied-only rows awaiting=true.
But the existing `.backupRestore` preparation calls retainForRestoreRecovery again
unless a record already has a journal generation in the current installation/binding.
The flag alone is therefore not sufficient to preserve known43 through that later step.

The correction uses that existing current-identity journal exception. The new API is:

```swift
BigSyncClientIdentity.withReconciledManualBackupRestore(
    transactionIdentifier: UUID,
    configurations: [Realm.Configuration],
    reconciledObjectTypes: [Object.Type],
    _ replacement: () throws -> Void,
    rollback: () throws -> Void = {}
) throws -> BigSyncManualBackupRestoreReceipt
```

The actual sequence is: existing exclusive identity lease and durable intent; caller
installs/normalizes final files; publish the new sentinel and replica binding; validate
and journal admitted rows as unchanged repairs; persist the existing completion receipt;
remove the intent. Only then may ordinary startup resume.

The journal changes transport attribution, not record owner, stateRevision, payload or
audit dates. The adapter's existing backupRestore preparation retains those generations
and their admitted records. Lower server versions then use the normal model preference;
backup-only records remain withheld. Automatic/raw restoration remains conservative.

Required repair work is recorded as an optional requirement in the existing manual
intent/event/completion receipt. Six-line old receipts still decode unchanged. A new
reconciled receipt has one required marker; unknown shapes reject. A raw caller cannot
resume a reconciled transaction and silently skip repair. The existing event identifier
and public receipt identity remain unchanged.

A repair failure after event publication keeps the same durable intent and returns
handoffPending. Partial per-Realm journal work can retry; it cannot become permission
to roll back installed files. A completed receipt with failed intent cleanup retries
cleanup only, not replacement or repair. Restore-event acknowledgement cannot bypass
an unfinished reconciled handoff. There is no new journal, floor registry, cloud
publication, general phase machine, or unconditional restore-admission flag.

## Required application binding — not claimed finished

W3 owns BigSyncCloudKitPolicy; W8 owns the real comparison/physical replacement.
Both ends must be composed. At reviewed Common b4846243, policy still called the raw
manual restore and old unconditional copy normalizer, and installed journaling only
after the receipt. The new helper alone does not fix that unmodified application path.
Exact instructions were posted to Common #7 comment5642314851 and Core #6 comment5642294934.

Before invoking the reconciled overload, register the already-existing mutation policy
with identity.makeMutationJournalIdentityProvider(). Registration itself does not publish
identity. Do not call prepareInstallation/installMutationTracking from inside the locked
finalizer; those paths correctly reject a pending intent and may reacquire the lease.
The helper requires that registered live provider to match the new identity.

Pass final installed configurations, not staged or original files. Pass only concrete
model types actually normalized against preserved originals by W8. They must implement
BigSyncRestoredObjectRecovering, outbound validation and ChangeMetadataRecordable.
The last normalizer must preserve known=false versus copied=true; do not run the old
blanket retainForRestoreRecovery over these rows after comparison. Sessions require W5's
real recoverable conformance, not a cast that silently skips them. Missing schema,
registration, identity or model validation throws, rather than granting completion.

The caller's existing replacement journal must retain originals and the exact transaction
on handoffPending. Its replacement closure is idempotent for already-installed files.
Guard rebinding and process receipt clearing belong to W4/W8's real restore completion,
not to BigSync's record transport. No actual user restore or history selection was run.

## Revision APIs and existing test source

The existing preferIncomingRecord/preferExistingObject hooks and strict journals are
unchanged. See [ReadingAnalyticsRevisionContract.md](ReadingAnalyticsRevisionContract.md).
Test implementation e8b899e57cf5493eca82c4598e449c2bafb08c44, blob
e14062aeaa9ce1edb164fde2503993765eab5b6e, remains unchanged in the coding passes.
It uses real Realm/journal/adapter implementations and two actual synchronizeAdapter
conflict loops. Only remote IO and existing boundary/identity-failure hooks are controlled.
The model is not Common, and scripted records have no actual server change tags.

Already-registered file: Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift.
Exact 21 native method IDs:

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

These tests cover adapter backup admission, not the newly added reconciled manual
file-handoff helper. Qualification must compose W8's physical replacement with actual
BigSync backupRestore and a later server42, plus interrupted repair/cleanup retries.
No tests, compiler/typecheck, syntax checks, native tooling or CloudKit operations were
run in the two production coding passes. Earlier parsing evidence is historical and
must not be applied to these changes. Only preimage/postimage hashes, diffs and GitHub
publication identities were inspected for safe source publication.

## Cutover removal: actual code dependency remains

Current inspected W6 Core #7 still retains ReaderOrderedV2BigSyncTransport and old
startup/cutover callers. W8 has begun retiring owned orchestration files, but complete
caller-removal SHAs for BigSync's exported cutover family have not been supplied.
Removing mutation UI does not close those calls. No export or old authority family
is falsely reported as removed in this PR.

| Surface | Disposition |
| --- | --- |
| AcceptedHeadQuiescence / PausedOutboundRecovery | Exact 120/115-line source deletion patch prepared separately, not applied while callers remain. Exclusive probes/test and manifest references need closure in the same batch. |
| OutboundCompletion | Remove source-publication wrappers with callers; distinguish supported external-owner recovery. |
| Synchronizer OutboundQuiescence | Mixed final-drain/source exports and ordinary principal/admission/submission handling. No blanket deletion. |
| Persistent outbound coordinator / replay | Retain current decoding, outstanding submissions, exact operation identity, generation-matched handling and unknown outcomes. Existing barriers are not automatically settled. |
| RecordStore / RecordMutations / journals / identity / preferences | Retain ordinary synchronization safety. |

Ordinary save/delete still reaches admitOutboundBatch and modifyRecordsHoldingOutboundLease.
Their physical submission bookkeeping is not an Undo protocol. A missing proxy, empty
journal, expired timeout, unavailable old provider or lost lock is not settlement.
The pending two-file source patch is only a first removal slice, not a completed mixed
runtime collapse or a dependency-closed test/manifest batch. W1/W6/W8 must authorize
its application after actual calls disappear. No successful compatibility alias exists.

## Accounting and next boundaries

Restore coding delta vs d664de02: **+201/-20 production**, four files, including one
104-line new helper; no test changes, no identical moves, no manifest/workflow changes.
Cumulative PR #11 production delta: **+304/-100**, including the prior two-file callback
fix. Prior test refinement remains +314/-6; documentation and unapplied patch preparation
are separate. The 235 prepared source deletions are NOT counted as runtime reduction.
SwiftPM's existing source discovery includes the helper; actual app composition remains
W1-owned. No cap increase or scope expansion is implied by this supported restore fix.

W1 excluded cloud baseline election/CAS/publication in comment5641950120; W8 withdrew
its create-only API request in5641954172. Explicit selected local input is sufficient
for isolated composition; production multi-install baseline policy remains separate.
No raw-bootstrap save API or another transport family was added.

Remaining: W3/W8's actual reconciled restore caller binding; W6/W8's cutover caller
closure and resulting export/probe removal; W1-approved native and composed qualification.
Published code is not an integrated app or release authorization. No release/default
merge, forced ref, live migration, account/zone/journal clearing, source activation,
foreign-source edit or protected Mac worktree operation was performed.
