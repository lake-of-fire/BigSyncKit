# Sync phase and terminal callout ownership — October 5–6, 2026

## Input and integration ownership

BigSyncKit #113 is based on #96 development head `4482f8b0c96719b98213b677b2c34f1035e1d744`, tree `d040bb45e5a59df745eb2333fa8291c599dafabd`. Initial implementation commit: `f572246fca4190c615392849eaddfa85642ac7f6`. Its final successor changes only the native test fixture and this report; production remains byte-identical. Reader #286 remains the sole hotfix application continuation. Common #257 from the previous review is already merged into its development stack; it is not replayed here.

Only one existing production file changes: `CloudKitSynchronizer+Sync.swift` (GitHub comparison: 121 additions, 43 deletions). Its full 84,215-byte original was verified as Git blob `d8b0e89013169e686324e662a1886f8fc20c085a` before editing. Final complete production blob: `43572ed10569f44c67a3765f26a550e9847c84c3`, SHA-256 `c52cbf4fa97d749ce3cf7a88cdcb470f8e7512cc61d01353cb43dda22d5e8b9a`.

The complementary processor/startup work in #112 changes `CloudKitSynchronizer.swift`, not this file. Its reviewed head was `7cfe47fa2828c46b429f842c6f22ab727e2d877b`, with the overlapping-child registry refinement. Preserve and compose that work separately; this child does not claim to select or replace it. Do not use temporary composition #110/#111 as feature ancestry. No Reader pins, inventories, current Xcode runner, protected target or release flags are changed by this component publication.

## Findings and correction

### Retired adapter-phase continuations could alter successor work

`runFetchedChangesPhase` awaited processor completion and adapter persistence, then cleared the shared processor error array without validating the original attempt. A noncooperative persistence callback can return or throw after replacement and erase a newer run's error. Related wrappers continued into cursor save, upload or deletion phases after their original awaited operation lost ownership. Empty or nil-token paths could return success despite cancellation.

Capture the existing attempt at each owning entry and use the existing `checkSynchronizationAttempt` after awaits and before cleanup or another phase. Current errors retain their original identity. Upload completion is still delivered once and a throwing completion is not fed back into itself. A completed token save remains completed: rejecting a stale continuation does not roll back accepted effects or acknowledge different journal generations.

### Synchronous notifications/progress callbacks could relabel old work

`postNotification` calls NotificationCenter synchronously; `reportProgress` synchronously invokes the configured callback. The sync-start callback ran before cursor/retry initialization, and fetch/upload callbacks ran before child methods captured their current attempt. Cancellation followed by replacement could therefore make an older continuation mutate successor state or enter successor work.

The database-page completion callback exposed a particularly important variant: the old page's zone deletion could be handled using a newly sampled replacement `activeRunContext`. The loss classifier's existing context guard then validates the new context, not the page's original owner. Validate the original attempt immediately after that callback before sampling context or persisting a zone lifecycle result. Genuine current-zone loss remains terminal; this does not recreate established zones or change recovery policy.

### Second pass: terminal cleanup and application handoff had the same gap

Terminal progress callbacks could replace the attempt before token/retry cleanup, restart flags, inbound acknowledgement, receipt-authorization cleanup, or application completion. A blocked-health notification could install a newer receipt authorization which the old blocked path subsequently cleared. Late terminal completion could invoke the application callback before the unchanged publisher's existing post-callback context validation.

A small local `canContinue` function reuses the original attempt predicate and existing cancellation settlement. Check terminal callouts before subsequent state changes or context selection. Existing throwing terminal helpers validate their captured context at entry and after external calls. Health recording uses the original terminal context; blocked cleanup validates again after the synchronous health notification. The existing publication result types and publisher remain unchanged.

### Outcome-handling refinement

The outer upload previously branched on a conditional NSError cast of an optional Swift Error. The controlled Linux Swift 6.2.1 run reproduced a non-nil pure Swift error taking the nil/success branch. Branch on the original optional Error instead, bridge explicitly only for the existing NSError retry classifier/log formatting, and retain the original error for settlement. This is a demonstrated Linux portability/contract correction, not an independently established Apple Foundation production incident. A native regression is authored for that contract.

## Refactoring boundaries

No new actor, coordinator, queue, task registry, lock, account request, schema, durable owner, journal field, receipt format, retry policy or public API. The phase entries which throw no longer perform cancellation cleanup and then return apparent success; their existing outer caller owns settlement. Read/writer fencing, zone-loss policy, accepted upload generations and token-last semantics are retained. The already-existing processor owns cancellation/joining of in-flight adapter work; this review changes its callers, not that ownership implementation.

## Executed evidence

Linux Swift 6.2.1, Swift 5 and strict Swift 6 language modes, each unoptimized and optimized, complete strict concurrency, warnings as errors. **65 distinct controlled cases pass in every final configuration.** The exact original source yields **25 passes and the same 40 expected failures** in every configuration. Repetition gives 260 fixed executions, not 260 unique tests or forty independent product defects.

Four explicitly separated lanes:

| Lane | Unique cases | Original passes / failures | Scope |
| --- | ---: | ---: | --- |
| Adapter phases and sync start | 25 | 8 / 17 | Seven complete production methods; explicit adapter/processor collaborators, real Swift task suspension/cancellation |
| Fetch/upload outer callers | 16 | 6 / 10 | Two complete production methods; controlled downstream transports, callbacks and checkpoints |
| Database page handoff | 6 | 5 / 1 | Complete database-fetch method; controlled pages, account context and lifecycle writer |
| Terminal cleanup/publication | 18 | 6 / 12 | Complete terminal caller and its contiguous helper section; also the unchanged actual publication method from `CloudKitSynchronizer.swift` |

The terminal lane records entry to the application's completion callback; it does not pretend that the original publisher lacks its later context validation. That actual publisher is compiled in both versions. Lifecycle storage, adapter effects, account validation collaborators and domain providers are explicit models. These tests neither execute native Realm/CloudKit transactions nor typecheck the full Apple application module.

All 32 final build/run receipts reconcile individual named outcomes and exit status. A combined optimized wrapper was stopped by the tool timeout while building the original Swift-6 terminal lane; that incomplete folder was separated, then the missing lanes were rerun to completion. It is not counted as a pass. Original and changed full sources, exact extraction script, collaborators, cases, receipt accounting and reproduction runner are supplied in the review artifact. Production and final native files frontend-parse both with and without DEBUG. Parsing is not native SDK typechecking.

### Independent second-pass controls

Four strict Swift 6 unoptimized controls retain identical scenarios and restore only one original boundary. Restoring `runFetchedChangesPhase` yields 19 passes / 6 failures in the 25-case phase lane. Removing only the database-completion guard yields 5 / 1. Restoring only the terminal caller while retaining the corrected helpers yields 11 / 7. Restoring only the conditional NSError branch yields 15 / 1. Each control's failure identities are recorded and agree with a subset of the complete original-source failure set. These controls do not change the production revision or add unique cases.

## Native regressions authored, not executed

`Tests/BigSyncKitTests/SyncPhaseAttemptOwnershipTests.swift`, final blob `0debe312cede9120ff6087e7c6fa2b4fc09cc90d`, contains **19 XCTest methods**. It exercises actual synchronizer/processor APIs and synchronous NotificationCenter delivery with injected in-memory state and transport adapters. No new Realm Object type is declared. Account identifiers and page data are synthetic, not genuine account-switch or released-data evidence.

The second review added terminal/health histories and a genuinely suspended persistence history. Final fixture-only refinement opens the entered gate before fulfilling its expectation, guards the DEBUG-only callback-count assertion without hiding the class in non-DEBUG builds, and applies the existing bounded account-gate watchdog to terminal/health cases too. Notification tests release/join their fixture-owned tasks on failure; a missing callback or missed suspension fails rather than silently counting as coverage. The callback test for terminal progress loops over six checkpoints within one method, not six distinct native method identities. Earlier native blob `cfcac3588b1a89e6928dfde0a82ec47e141b2ded` is the initial published fixture, not the final selection.

Required identities:

```
SyncPhaseAttemptOwnershipTests/testSyncStartObserverCannotOverwriteReplacementAttemptState()
SyncPhaseAttemptOwnershipTests/testFetchStartObserverCannotEnterReplacementTransport()
SyncPhaseAttemptOwnershipTests/testUploadStartObserverCannotEnterReplacementAdapter()
SyncPhaseAttemptOwnershipTests/testRetiredSuccessfulPersistenceCannotClearSuccessorProcessorError()
SyncPhaseAttemptOwnershipTests/testRetiredFailingPersistenceCannotClearSuccessorProcessorError()
SyncPhaseAttemptOwnershipTests/testTaskCancellationAfterPersistenceRejectsContinuation()
SyncPhaseAttemptOwnershipTests/testCompletedTokenSaveCannotStartReplacementUploadPhase()
SyncPhaseAttemptOwnershipTests/testNilTokenFastPathStillRejectsCancelledAttempt()
SyncPhaseAttemptOwnershipTests/testEmptyUploadDeliversCancellationExactlyOnce()
SyncPhaseAttemptOwnershipTests/testCurrentPersistenceErrorRetainsOriginalIdentity()
SyncPhaseAttemptOwnershipTests/testCurrentProcessorErrorIsClearedWithoutStartingPersistence()
SyncPhaseAttemptOwnershipTests/testThrowingUploadCompletionIsNeverRedelivered()
SyncPhaseAttemptOwnershipTests/testValidPriorityAndDefaultPhasesRemainOrdered()
SyncPhaseAttemptOwnershipTests/testCompletedDatabasePageCannotMarkReplacementAccountZoneDeleted()
SyncPhaseAttemptOwnershipTests/testCurrentDatabaseDeletionStillRecordsTerminalZone()
SyncPhaseAttemptOwnershipTests/testPureSwiftUploadFailureCannotEnterSuccessfulRefetch()
SyncPhaseAttemptOwnershipTests/testSuspendedPersistenceCannotClearSuccessorErrorOnReturn()
SyncPhaseAttemptOwnershipTests/testTerminalProgressReplacementCannotClearOrPublishSuccessorState()
SyncPhaseAttemptOwnershipTests/testBlockedHealthObserverCannotClearReplacementReceiptAuthorization()
```

## Remaining integration and native qualification

Keep draft. Compose with #112 through #96 and Reader #286 without resetting concurrent source. Add the new test path to Reader's explicit Project.swift source selection and all nineteen identities to both required-method inventories; default SwiftPM discovery is not assembled Reader membership. Refresh the immutable acceptance tuple through the existing W4 process and obtain real Xcode discovery/typechecking/execution of these methods with current processor, account-fencing, cursor, failure-composition and Mark–Undo suites.

Do not transfer historical native/signed test totals onto these changes. Native SDK, Realm durability, real account replacement, authentic Reader 3.11/build 327 migration, signed two-client, UI/performance and application Release remain independently unqualified here. No production CloudKit mutation, deployment, protected-target merge or release authorization.
