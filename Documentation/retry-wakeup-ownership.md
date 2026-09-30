# Scheduled account retry ownership

## Scope

Based directly on BigSyncKit main `05a09e3e506cdcc90bb077371c7d9beba654602b`,
including merged #75 failure-origin fencing. This is a production scheduler
correction, separate from the already-contained Core #231/#254 recovery work.
No Reader pin, release gate, account policy, schema, journal or CloudKit data
changes. The retry interval remains thirty seconds.

## Review passes

1. The retry task cleared `accountAvailabilityRetryTask` after sleeping without
   rechecking cancellation. A sleep already completed can resume on the worker
   actor after another request cancels that task and installs its replacement.
   For the same synchronizer, the old callback passed identity validation and
   erased the new handle. Later public cancellation could no longer reach the
   still-pending replacement. Check cancellation before retiring the slot.
2. Weak worker/synchronizer references were promoted before the sleep. Move that
   promotion after sleeping so a dormant retry does not extend worker lifetime.
3. Exercise repeated replacement in a different completion order, cancelled
   scheduling, public cancellation and ordinary live retry. Existing task
   cancellation plus synchronizer identity are sufficient; no token, generation,
   registry, additional timer, actor or lock is needed.

The post-sleep guard and slot retirement are on the same actor with no intervening
await. Once admission succeeds, the existing synchronization method remains
responsible for its subsequent cancellation/account/attempt fences. No account
availability result, retry policy, shared restoration, journal or timeout outcome
is changed. This does not prove that any historical signed failure was caused by
the scheduler race, nor does it physically interrupt work already admitted.

## Deterministic behavior coverage

The private scheduler accepts a sleeper defaulting to the unchanged Task.sleep.
A DEBUG-only method exposes that existing scheduler for tests. It introduces no
persistent scheduler state or public configuration. The controlled sleeper can
return normally after cancellation, modeling a real sleep that has completed
before its task resumes on the worker actor.

Seven methods in the existing registered worker-test file cover same-owner and
replacement-synchronizer retries, dormant-worker lifetime, normal delay/retry,
failed sleep, cancelled scheduling, and repeated supersession. Every held task is
released and joined. Native tests use the real worker/account gate and the
existing injected transport that rejects and counts unexpected CloudKit calls.
All original 17 worker and 24 deadline methods are preserved byte-for-byte.

Local Linux Swift 6.2.1 verification compiles the exact scheduler, public
cancellation method and DEBUG seam, and the same seven-method XCTest class,
against explicitly controlled downstream worker/transport collaborators. It is
not the native CloudKit/Realm package. Swift task cancellation, actor execution
and reference lifetime operate normally.

- Initial six cases: two failed (lost successor handle and worker retention).
- Cancellation guard alone: only the retention case failed.
- Both fixes: six passed in Debug and optimized Release.
- Final repeated-replacement case: seven passed in each configuration, followed
  by an independent seven-case passing run in each configuration.
- Unchanged actual deadline suite: all 24 passed in Debug and optimized Release.
- Builds use complete strict concurrency, warnings as errors and enable-testing.
- Complete worker/native test files parsed in Swift 5 mode with DEBUG; worker also
  parsed without DEBUG. Parsing is not Apple SDK typechecking.

An initial deadline-suite command was interrupted after its Debug pass; a new
complete paired run passed separately. The interrupted command is not a paired
pass. Repetitions are not additional distinct test cases. Optimized helper builds
are not shipping application qualification. Raw inputs, process statuses, logs,
xUnit identities, source hashes and source-reconstruction provenance are retained
in the external verification packet.

## Native and application acceptance

Run BigSyncScheduledRetryTests, the complete existing worker-cancellation class
and the package suite on macOS against this exact source. The native tests live
inside the existing worker test file; no new source-path registration is needed,
but actual discovery and execution of the new class must still be verified.
Do not use BIGSYNC_WORKER_DEADLINE_PORTABLE in a native worker test target.

Then qualify any explicitly selected Reader successor independently. Preserve
newer Core/Common/BigSync work, the owner-deferred Mac UI/performance/application
Release scope, signed disposable-zone boundaries and the current release gates.
No native Apple, real account lookup, signed CloudKit or assembled Reader run is
claimed by the local collaborator results. No target branch is merged here.
