# Worker deadline acceptance and restoration ownership

## Scope

This is a follow-up to merged BigSyncKit #68 on `main` at
`c5bb26d50afe8830119729facf58894b50ad3f0f`. Core #182 now consumes its typed
DEBUG outcome; Reader #196 is the separate integration stack. No Reader pin or
release flag is changed by this package patch.

The existing public optional-result signature and DEBUG completed/timed-out/
cancelled cases remain unchanged. Zero-budget and expired results now fail
closed; this is an intentional lifecycle behavior correction, not a claim that
all production behavior is byte-identical. No Realm schema, mutation journal,
accepted baseline, account policy, subscription, or CloudKit record changes.

## Findings

The old timer slept the entire supplied duration only after its detached task
was scheduled. Its race accepted any completion until the timer resolved it.
Thus timer scheduling could extend the request budget and a late completion
could win. An immediate zero-budget request could also enter ordinary admission
and retire valid startup/retry work before the timer ran.

Separately, a waiter resuming from shared publication restoration cleared the
shared task slot before checking cancellation and the expected worker identity.
An obsolete waiter could therefore change shared state despite failing its
subsequent admission fence. The assignment now follows both checks.

These are source/controlled-regression findings. They do not establish that any
historical signed stall, data mutation, or prior qualification was affected.

## Design

`BigSyncDeadlineRace<Value>` is the existing per-request race factored out of
the CloudKit-bearing worker. It is still one actor with one waiter, not a new
shared ownership system. Its generic payload permits the actual implementation
to run in a dependency-free test target; the real worker specializes it with
`CloudKitSynchronizer.SynchronizationResult?`.

The race captures one saturating monotonic deadline before either child task
is scheduled. The worker checks the remaining budget before creating the
request and again before that request enters ordinary admission. A completion
accepted at/after expiry becomes timeout even if the timer has not executed.
An already-accepted on-time completion survives delayed waiter resumption.
Cancellation stays distinct, including the existing final caller-delivery check.

The timer sleeps only what remains of the captured budget and recomputes after
an early wake. Cancelling or failing its sleep does not manufacture a timeout.
Only the losing logical request is cancelled; shared work may still finish for
other waiters. Timeout does not roll back work accepted by CloudKit.

Both existing duration APIs still begin at worker-actor entry. The additional
DEBUG `cloudKitE2ESynchronizeCloudKit(untilUptimeNanoseconds:)` entry accepts an
original DispatchTime uptime cutoff and does not renew it after logging, actor
hops or earlier attempts. Both typed entries share one result mapper and the
same admission/cancellation/race implementation; the normal optional-result API
remains unchanged. No extra actor, task, timer, queue or authority is added.

An absolute cutoff is local to this process and clock. It is not a wall-clock
date, persisted deadline, replica generation or reusable post-relaunch token.
Scheduler/process suspension may delay actual return. This is neither physical
cancellation of an issued Apple call nor a change to the 48-hour Undo clock.

## Caller-cutoff handoff follow-up

The inspected merged Core `9af65fbba29809e333eadd7e0990a812dee02d73` computes
`remainingNanoseconds = absoluteDeadline - now`, then writes progress and hops
to the BigSync actor before supplying that cached duration. Even with #70's
original worker fix, that handoff can renew the wider Core budget. The new
absolute entry closes the worker-side contract required to repair that caller.

**Core adoption is NOT included in this package PR.** In
`CloudKitE2ECoordinator.synchronizeWithDeadline`, remove that cached-remaining
local and change the worker call to:

```swift
let outcome = await BigSyncBackgroundActor.shared
    .cloudKitE2ESynchronizeCloudKit(
        untilUptimeNanoseconds: absoluteDeadline
    )
```

Keep the existing configured budget and the same cutoff across both attempts.
Do not compute `now + remaining` again after logging or the actor hop. Do not
restore Core's old detached race or delayed global cancellation. This consumer
must select a compatible #70 revision before compiling. Its current coordinator
blob is `ea2a74f4cc3f9448f1f8194d2260e22e3d7f9cc6`; recheck newer work first.
Until the consumer adopts this entry, its pre-worker timing gap remains open.
Reader #187's frozen composition and Reader #196's conflicting pins/manifest
are deliberately untouched. Core #182 has merged; do not reopen its old branch
or mistake its historical PR description for current source.

## Coverage and executed evidence

All tests remain in the already-registered
`BigSyncWorkerRequestCancellationTests.swift`; no new Reader test path is needed.
The native section is excluded only when the dedicated local runner explicitly
sets `BIGSYNC_WORKER_DEADLINE_PORTABLE`. Normal Apple package/root builds do not
set it and retain all existing worker cases.

The initial 16 `BigSyncDeadlineRaceTests` run the actual generic race, without
CloudKit/Realm stand-ins. They cover late/exact-boundary completion, delayed
and early timers, on-time completed-nil, overflow, single settlement,
independent requests, cancellation, and waiter resumption. Debug and optimized
Release each passed all 16 methods with strict concurrency complete and warnings
as errors. The runner compares runtime discovery with executed xUnit identities.
A Swift-5 language-mode module check also passed.

Two controlled mutations establish test sensitivity: permitting late completion
caused four failures; sleeping the original duration rather than the remaining
budget caused two failures. Each control executed all 16 methods and exited 1;
its Release lane was not run. These are fault-injection controls, not a claim
that the unmodified native worker was executed on Linux.

Three new native cases cover zero-budget startup preservation, zero-budget retry
preservation, and replacement-restoration ownership. The existing cancelled
restoration-waiter case additionally checks that only a later live caller retires
the barrier. These Apple/BigSync tests were syntax-parsed, not typechecked or
executed against the real graph here. Native execution remains a merge gate.

## Current follow-up verification

At parent `ba283ff8`, all 16 original helper methods were rerun successfully in
Debug and optimized Release. The cutoff follow-up keeps those methods and adds
eight: elapsed caller preparation, remaining-only sleep, one cutoff shared
across attempts, on-time delivery, cancellation, zero, near-overflow, and a
relative-versus-absolute handoff counterexample. The actual 24-method suite
passes in both configurations with strict-concurrency checking and warnings as
errors. The Swift-5 helper check and syntax parsing of complete worker/native
test files also pass; parsing is not Apple SDK typechecking.

A targeted four-method negative control deliberately treats the absolute cutoff
as a fresh duration. All four methods fail, while the normal run passes. Only
those four methods execute in this control; it is not an original native-worker
or Core call-site run. The relative-handoff comparison executes the retained
relative constructor and the new absolute constructor with a controlled clock.

Four more native methods exercise the new SPI: expired startup preservation,
expired retry preservation, precancelled expired admission, and live completed-
nil. These remain authored/unexecuted here, alongside the native cases above.
All stay in the same already-registered worker test file; the portable flag must
never be set in the real Apple/Reader test target. No package-local result
qualifies the unchanged Reader source or changes its release gate.

## Reproduce and finish

```sh
BIGSYNC_DEADLINE_TEST_OUTPUT=/tmp/new-deadline-evidence \
  bash Tools/test-worker-deadline.sh
```

The output directory must not already exist. It retains build/discovery/test
logs, xUnit output and exact per-method summaries for both configurations.
Optimized testing uses enable-testing; neither lane qualifies a shipping app.

On the native host, compile the complete BigSync/Core/Realm composition and run
both classes in the registered worker test file. Then run the existing Core
account/cancellation tests and signed disposable production-reader and control
journeys on the explicitly selected dependency tuple. Preserve shared-restoration
and old-request/new-worker rejection assertions. Do not substitute the 24-method
portable run for the native worker tests, clear pending work to obtain quietness,
or flip Reader qualification flags based on package publication.

Re-read Core #182, Reader #196, target refs and selected pins before integration.
Keep their newer work; this change does not reapply any old independent-account
bundle. No target merge or release authorization is implied.
