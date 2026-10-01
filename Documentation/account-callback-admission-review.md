# Default account-identity callback: admission and delivery review

## The path that preflight does not cover

The status gate in #80 uses the shared absolute deadline race. The default
`CloudKitSynchronizer` account-identifier provider separately invokes
`awaitCancellableCloudKitCallback` with a 60-second budget and a String result.
The earlier preflight change did not repair or qualify that callback helper.

The old helper registered the callback before creating a relative timeout and
performed no explicit cancellation checks around buffered result delivery. Exact
original-source controls reproduced registration/success for a precancelled
caller, zero-budget success, and success after registration consumed its budget.
These are direct helper defects, not attribution of earlier signed-sync failures.

## Narrow repair

Keep the existing AsyncThrowingStream and one optional timer. Capture a saturating
absolute uptime cutoff before synchronous registration; reject already-cancelled
or expired admission, and enforce the cutoff when the callback arrives. The timer
uses only the remaining budget and rechecks time after waking. Callback success
or error already accepted on time remains valid even if registration or caller
resumption finishes later. Cancellation still owns delivery to a cancelled caller,
including buffered errors, and late callbacks after termination remain harmless.

The internal generic requires Sendable values, matching the stream transfer and
the observed String callers. An internal injectable clock defaults to DispatchTime.
No public API, new actor, lock, queue, shared timer, account cache or journal is
added. The CKError.networkFailure timeout surface and default identity-provider
60-second limit remain. Worker failure diagnostics, status mappings, current
preflight and retry rules, account/run validation and mutation logic are unchanged.

Registration is synchronous and still cannot be interrupted by this helper.
CKContainer construction in the default provider remains before helper entry.
This bounds callback acceptance, not physical Apple cancellation, real-time return,
rollback, or all preparation outside the helper. No new signed result is claimed.

## Executed controls and preservation

Three direct controls against the exact unchanged helper all failed (five
assertions, process exit 1): precancelled registration, zero budget and elapsed
registration. They use CloudKit error declarations, not real Apple operations.

Eleven methods were added to the existing native gate-test file, retaining its
three original methods byte-for-byte. Combined with #80's unchanged 13 tests,
all 27 methods passed in Debug and optimized Release on Linux Swift6.2.1, strict
concurrency complete, warnings as errors and enable-testing. Final publication
inputs were independently repeated in both configurations after preserving the
original leading documentation comments. Exact runtime discovery agrees with
executed xUnit identities; these are the same 27 distinct methods, not 54.

The new tests cover cancelled/expired admission, registration consuming time,
late success/error, on-time buffered success/error with delayed delivery,
cancellation after buffering, unbounded first-result behavior, overflow, and a
cancelled missing-callback waiter followed by a late reply. The actual gate and
race are compiled intact; the callback is extracted verbatim. SDK declarations
are explicit stand-ins, with default account access forbidden. This is not a
full native CloudKitSynchronizer/Realm build or live account evidence.

An initial strict build identified the unconstrained generic transfer; that failed
build remains retained rather than being counted as a test run. No previous
native #78 result is transferred to this changed helper.

## Publication integrity and current composition

The original complete synchronizer blob is
`82287ae95d945680beb9b5288fed570b4672f242`; the replacement complete blob is
`370f614675a3fee50ec7bd93d46bf18df06b451d`. Its full-file diff contains only the
callback helper (47 added/23 removed lines). Same-repository source calculation
preserved all remaining synchronization/Realm/failure code. Only the complete
blob is selected in the normal feature commit; no fragment/calculation ancestry
is selected. Temporary #81 is closed without merging.

Reader #187 at inspected `e829e168` already selects original #80 `4963c99c`.
This newer callback increment is not selected until its integration owner
explicitly composes it. Selection, generated membership and actual execution
are separate facts. Nine prior root deadline class-identity corrections do not
establish execution of these eleven new cases.

Core #319 separately repairs the current Core #297 regression fixture that
could not compile its new originating-failure branch. Its 77 controlled passes
are not full Core/BigSync/Realm acceptance, and the test-only Core change must
not be mistaken for a changed production terminal-settlement policy.

## Remaining native acceptance

Compile the complete current package and execute the new callback class, both
account-preflight classes, the original gate tests, and worker cancellation/retry
suites on the real Apple graph. Verify the default record-ID caller and status
provider separately; test exact outcome/error preservation and no unnecessary
provider invocation. Retain genuine account/binding/attempt fences downstream.

Do not repin the running Reader checkout or transfer earlier iOS batch results.
Respect deferred Mac UI/signed Mac/performance/application Release. Authentic
installed-release migration and genuine second-account evidence remain separate.
No target merge, production CloudKit mutation or release permission is implied.
