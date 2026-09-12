# RA-1 Worker 2 — implementation and remaining integration boundary

## Current production work

PR #10 merged normally at `5923cdccc39d90952865d27041ee7ac1168a1ac8`.
PR #11 continues on `codex/reading-analytics-w2-review-20260911`, targeting
`codex/reading-analytics-integration-20260911`. Coordinator: root issue #21.
The completed milestone branch and integration refs are not edited by W2.

Code-first continuation after `9185ef1d3ba488a7b4febcace4249f79b6d791bb`:

| Commit | Implemented change |
| --- | --- |
| `7b5183b4e38320af92ce9817fc990936f0cc2ca0` | Record save/delete drain wrappers capture only the operation error and call their completion outside the operation catch. |
| `3cbed7cd89f9413ab2d03fe765a95013fbad8950` | Upload composition and zone setup likewise separate downstream completion failures from operation failures; remove the nested upload catch that redelivered completion. |
| `33e72a7500d0d7e13b7d827f68a0442c107b7a83` | Revalidate zone-fetch and zone-save failures before lifecycle writes; verify the returned zone identity before recording establishment. |

The source defect was `do { operation; completion(nil) } catch { completion(error) }`.
A throwing consumer could receive a second result; zone setup could misinterpret a
consumer's error as a failed zone operation. Each repaired wrapper now delivers
one result. A completion error propagates to its caller rather than recursively
invoking that completion or turning it into zone-loss evidence.

Failed remote IO also suspends. Zone-fetch failure now revalidates the original
attempt/account before inspecting the active context. Failed zone creation
revalidates the captured run before classifying the original error. A mismatched
fetched zone is not establishment evidence. These checks use existing APIs.

Final production blobs:
- RecordMutations: `cc4e76eeb76e3115d962d9a2a0b015c480df7fe2`.
- Sync: `69dc38a2ed0f8613f77bfff3a68566383c1d3914`.

Exactly two existing production files changed: **+103/-80 (net +23)** relative to
9185ef1d. There is no new API, model, framework, phase, ledger, source move,
manifest change, or ordinary cancellation-bridge replacement. Record selection,
required journaling, G1/G2 acknowledgement, per-item results, actual request leases,
long-lived replay and persisted physical uncertainty are unchanged.

Per Alex's code-first instruction, this continuation did **not** run tests,
compiler/typecheck, syntax checks, native tooling, or CloudKit operations. Exact
source preimage/postimage hashes and GitHub publication were checked to avoid
replacing unrelated source. That is publication integrity, not behavior evidence.
New callback/error-path regression execution remains a native gate; the earlier
21-method revision suite does not by itself qualify these new callback changes.

## Existing revision contract and authored tests

The model preference APIs remain sufficient at the source level; they are not
newly invented or patched by this continuation. See
[ReadingAnalyticsRevisionContract.md](ReadingAnalyticsRevisionContract.md).
API clarification: `501f62791c0fd18295c3d2af0cf94accc9c784db`.
Real Realm test source: `e8b899e57cf5493eca82c4598e449c2bafb08c44`;
blob `e14062aeaa9ce1edb164fde2503993765eab5b6e`.

That earlier test refinement corrected the overly exact two-upload script,
foreign-author fixture setup, reseed-versus-backupRestore distinction and missing
incoming deletion admission. It added final-write races, divergent own echoes,
second-journal rollback and target-commit/tracking-failure replay cases.

The fixture uses real Realm/journal/adapter implementations and two actual
synchronizeAdapter conflict loops. Only remote IO and existing boundary/identity
failure hooks are controlled. It is not Common's model, inverse, or application
composition. Scripted CKRecords have no actual server change tags. Backup tests
exercise adapter admission, not physical file replacement or process loss.

The same already-registered source file remains
`Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift`. Exact 21 method IDs:

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

Historical evidence for e8b899e5: DEBUG syntax parse and published-blob comparison.
Apple typecheck, the 21 native methods, Common/application composition, filesystem
restore and signed multidevice qualification have not run. Historical parse results
must not be extended to the new production changes. Existing account/cancellation,
partial-result and callback suites remain unchanged and are not claimed rerun.

## Cutover removal: actual remaining code dependency

W6 Core #7 at `1fa7174e09ee22326d20500875ca40358c9aa532` still reports old
startup/lifecycle/cutover families as reachable. W8 Core #6 at
`a841f46153574a40557eded7191b1c440d0e42cd` likewise has not closed its old
orchestration callers. Neither owner has supplied complete caller-removal SHAs.
Removing mutation UI is not removal of ReaderOrderedV2BigSyncTransport callers.
The required coordination request is posted in root #21.

| BigSync surface | Remaining responsibility |
| --- | --- |
| AcceptedHeadQuiescence / PausedOutboundRecovery | Delete host-only exports with actual caller removal and exclusively matching tests. Not deleted yet. |
| OutboundCompletion | Retire source-publication wrappers with callers; preserve independently supported external-owner recovery. |
| Synchronizer OutboundQuiescence extension | Mixed final-drain/source APIs and ordinary principal/admission/submission handling; do not blanket-delete. |
| BigSyncOutboundQuiescence persistent coordinator | Preserve decoding and actual outstanding submissions; old barriers must not silently become settled. |
| OutboundReplayRecovery | Preserve exact operation identity, single-delivery collector, generation-matched reconciliation and unknown outcomes. |
| RecordStore / RecordMutations / journal / identity / model hooks | Preserve ordinary synchronization safety and owned-version selection. |

Ordinary save/delete still reaches admitOutboundBatch and
modifyRecordsHoldingOutboundLease. Host-only exports remain callable by the old
Core runtime. This pass deletes no old authority family and does not claim full
collapse. No empty-success aliases, disabled guards, checkpoint clearing or forced
old-state downgrade are used to make caller dependencies disappear.

## Bootstrap scope decision

W1 issue comment 5641950120 explicitly excludes a cloud winner-election/CAS or
publication service from this batch. W8 withdrew its create-only transport API
request in comment 5641954172. Explicit selected-snapshot/local installation is
sufficient for isolated development composition. Production multi-install baseline
selection remains a release/product decision, not an unfinished new W2 API.

The existing admitted mutation entry remains internal/run-and-generation-bound.
Raw CloudKitRecordStore/CKDatabase saves do not inherit its admission/settlement
contract. No raw-baseline write wrapper or second synchronization path was added.

## Completion and release gates

Independent production fixes above are published. Export removal remains blocked
on W6/W8's actual caller closure; W2 does not edit their repositories to fake closure.
W1 alone approves integration and the exact composed tuple. Native qualification
and production baseline/live-data permission remain separate gates.

Accounting: production +103/-80; prior PR #11 test refinement +314/-6 unchanged;
documentation separate; zero source moves and zero new production files. Existing
21-method file is unchanged in this coding pass. No tests were deleted to hide a
regression. This remains a draft, not a fully completed or qualified RA-1 app.

No release/default merge, forced ref update, live migration, user baseline choice,
account/zone deletion, journal clearing, source activation, native runner or
protected Mac worktree operation was performed.
