# Account preflight shares the existing deadline owner

## Current base and scope

Based on BigSync main `5d3e20fb4b40df5fdb8eb4b8aac80a1ec09e5c06`, containing
merged #75, #76 and #78. Earlier Core recovery and retry-scheduler fixes remain
integrated; neither closed donor is reopened. Reader #187 remains separately
owned and is not repinned by this change.

Only the internal account-availability gate changes at runtime. Its dedicated
race actor is removed in favor of the existing `BigSyncDeadlineRace`. That
shared implementation and the worker are unchanged. The new tests extend the
existing cancellation test file; no runtime file or native source path is added.

## Findings and repair

The old gate began its timeout sleep after the timer task was scheduled. Its
separate race had no absolute cutoff, so a late status could win before a delayed
timeout. Even a zero-budget call scheduled the provider. Cancellation settlement
was request-local, but a previously accepted result could still be delivered to
a caller cancelled before its continuation resumed.

The gate now captures one saturating uptime cutoff at entry and uses the same
acceptance rule as lifecycle synchronization. Check remaining time before task
creation and again at provider admission. Sleep only the remaining budget through
the shared deadline owner; a completion at or after the cutoff cannot win simply
because the timeout task has not executed. Check caller cancellation at delivery.

Caller cancellation also cancels this logical provider immediately. There is no
structured join of a possibly noncooperative Apple request, global worker cancel,
shared result cache, or new ownership registry. Each invocation owns its race and
both child tasks; a timeout or cancellation cannot poison another invocation.

The internal initializer accepts an injectable clock and sleeper with the normal
DispatchTime/Task.sleep defaults. This is a test seam, not a new public policy.
The normal 20-second gate budget, configured Apple 15/20-second request/resource
timeouts, and exact available/unavailable/failed mapping are preserved. Accepted
`temporarilyUnavailable` still reaches the existing worker's quiescent policy;
this patch does not change that policy or the 30-second retry schedule.

A result accepted on time stays accepted if delivery is delayed, unless the
caller was cancelled. This bounds acceptance, not physical Apple cancellation or
real-time return scheduling. It does not cap the earlier caller-to-gate actor
hop, replace the worker deadline, or change the callback-only bridge elsewhere.

## Behavior tests and evidence

The four prior cancellation methods are unchanged. Nine methods in the same
native test source cover zero budget, delayed/expired provider admission, late
available and exact-cutoff unavailable results, timer scheduling consuming the
budget, cancelled delivery after settlement, preserved on-time delivery,
independent next-call budgets, and fresh uncached status reads. The native tests
inject status providers and do not require an actual account or CloudKit mutation.

Local Linux Swift 6.2.1 builds use Swift 5 language mode, complete concurrency
checking, warnings as errors and enable-testing. The complete gate, complete
shared race and complete XCTest file execute unchanged. A separate `CloudKit`
module supplies only SDK declarations; default CKContainer access deliberately
traps if called. These are not Apple SDK, native worker/Realm, account or signed
CloudKit tests. Optimized tests do not qualify a shipping application.

The original four tests pass. The new zero-budget test fails against the exact
old gate while those four still pass. Fixed runs pass all 13 methods in Debug
and optimized Release. Earlier 12-method passes precede the ninth new
preservation case; their counts are not relabeled.

Fault-injection controls separately bypass late-result rejection, final caller
cancellation, provider admission and remaining-only timer sleep. They fail their
respective behavioral cases. One initial late-result mutation also removed clock
observation, which suppressed a cancellation-test hook; the corrected control
retains clock sampling and bypasses only acceptance. Both records are retained.
One combined control command was interrupted during a subsequent build; that
attempt has no complete test result and was rerun independently. An initial
serial test run passed four methods but emitted no xUnit file; its verifier run
failed. The completed runner uses SwiftPM's parallel xUnit mode and compares
executed identities to runtime discovery and a required inventory.

## Native/application acceptance

Run the complete `CloudKitAccountAvailabilityCancellationTests` and new
`CloudKitAccountAvailabilityDeadlineTests`, the separate original
`CloudKitAccountAvailabilityGateTests`, the existing worker cancellation/retry
classes, and the full native macOS package suite. Confirm actual discovery of
all nine additions without a portable define. Keep the default Apple provider
and downstream no-account/temporary-unavailability/preflight-retry behavior in
the assembled acceptance scope; controlled SDK declarations cannot qualify them.

Reader's current selected BigSync predecessor has native package evidence, but
those results are not transferred to this successor. The integration owner must
select and qualify a new exact tuple deliberately. Keep deferred Mac UI,
performance, application Release, authentic installed migration and genuine
second-account evidence separate. No target merge, release authorization,
production data, schema, journal, baseline or account-policy change is implied.
