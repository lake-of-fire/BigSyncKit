# Receipt observation and preparation ownership — October 5, 2026

## Decision

Keep the existing journal, independent Realm writer and prepared-quarantine
architecture. Strengthen the boundary between observing a committed receipt and
validating inside a transaction the caller actually owns. An open write flag is
not evidence that the current observer owns that write.

This increment was reviewed against #114 at
`f9b3450b005f289c80e9b6b5b59467555e482c0f`. During publication #114 merged into
#96, which independently advanced. Preserve that newer composition; do not copy
an older complete adapter over it. This report records source-scoped evidence,
not a transfer of the parent application's qualification.

## Findings and changes

### Receipt observation borrowed provisional target state

`comparisonReceiptIsCurrent` inferred permission to use live target data from
`realm.isInWriteTransaction`. Its tracking acknowledgement caller owns a tracking
transaction, not an independently suspended target owner's transaction. A
provisional matching revision could let tracking acknowledgement commit even
though the real committed target receipt did not match. Conversely, provisional
invalidation/removal could hide a valid committed receipt.

The helper now has two explicit private modes. `.committed`, the default for
tracking acknowledgement and quarantine settlement, uses the existing committed
snapshot helper and also accepts a previously frozen view without advancing it.
`.ownedTargetTransaction` is selected only at the journal-consuming target write
call site and validates the live state of that independently owned transaction.
The live mode is necessary: blindly freezing every call would ignore changes
inside the caller's own transaction.

Refresh, registry identity providers and model/contract callouts can revoke or
replace the attempt synchronously. The helper captures cancellation generation,
context and provider identity and checks them around those callouts. Model
contract code runs before the final live baseline sample, preventing a callback
from changing a baseline after its validity was already checked.

### Suspended preparation could adopt a successor owner

`preparedRecordsToUpload` could suspend during selection, then attach quarantine
evidence from whatever attempt was current when it returned. Cancellation and
resumption at the same namespace could therefore relabel an older selection with
the successor's owner. Capture the original generation, provider, context,
account and issuer at entry, and revalidate after selection awaits and before
annotation. Precancelled idle and suspended-empty calls reject as well.

No new queue, actor, persistent marker, journal, schema, receipt format or public
API is introduced. The DEBUG-only receipt test entry invokes the actual private
helper without exposing its admitted-receipt type. Existing prepared evidence,
acknowledgement/cleanup atomicity, generation-matched journal consumption and
original error behavior remain.

## Executed verification

Linux Swift 6.2.1, Swift 6 language mode, warnings as errors. The controlled lane
compiles the actual receipt validator, complete acknowledgement method and
preparation method, alongside the retained-quarantine methods from #114. Realm,
model/schema, registry, comparison codec and transport are explicit collaborators.
Swift task scheduling, cancellation and throwing behavior execute normally; the
storage model is not the native Realm SDK or its write queue.

The final sealed inventory has 35 unique test methods. Repeated configurations
are not additional unique cases.

| Source selection | Passed | Failed |
| --- | ---: | ---: |
| Original receipt/preparation methods | 16 | 19 |
| Final Debug | 35 | 0 |
| Final optimized, DEBUG test hooks | 35 | 0 |
| Restore live observer reads | 25 | 10 |
| Remove receipt owner checks | 29 | 6 |
| Freeze the owned-target validation | 32 | 3 |
| Restore preparation-time owner renewal | 30 | 5 |

Independent clean final Debug and optimized executions reconcile completed test
names against a sealed roster and retain source hashes and process statuses.
Initial collaborator/test-isolation compilation failures and earlier 26/29-case
stages are retained separately, not counted as final passes. The previous
67-case packet is a different scope and is not represented as 67 executions of
this changed helper.

Pinned SDK source review: Realm Swift 20.0.5 pins Realm Core 20.1.5. Core's
`do_refresh` is a no-op for frozen Realms and during a write; this review does not
claim that refreshing a frozen Realm causes a crash. The defect is choosing
provisional live data and retaining stale authority through callouts.

- https://github.com/realm/realm-swift/blob/v20.0.5/dependencies.list
- https://github.com/realm/realm-core/blob/v20.1.5/src/realm/object-store/shared_realm.cpp

## Native acceptance still required

New source: `Tests/BigSyncKitTests/SyncUndoCloseoutW1ReceiptReadBoundaryTests.swift`.
It extends the existing W1 fixture owner with eight methods:

- `testComparisonReceiptObserverRejectsProvisionalMatchingRevision`
- `testComparisonReceiptObserverRetainsCommittedReceiptDuringRemoval`
- `testComparisonReceiptObserverIgnoresProvisionalInvalidation`
- `testComparisonReceiptOwnedWriterUsesLiveRevision`
- `testComparisonReceiptOwnedWriterRejectsLiveInvalidation`
- `testComparisonReceiptFrozenViewDoesNotAdvance`
- `testComparisonReceiptRefreshCancellationGenerationRejectsAndRetries`
- `testComparisonReceiptCurrentRefreshRemainsValid`

The final two require real Realm notification delivery from a separately
queue-confined writer, with current-owner and cancellation-generation controls.
All eight were syntax-parsed only, not natively typechecked/discovered/executed.
The preparation ownership cases are executed in the controlled lane; no new
native preparation race result is claimed.

Register the new file in Reader #286's explicit project graph and all eight
method identities in both required inventories, retaining #114's six methods
and the existing sync/Unmark suites. File membership is not runtime discovery.
Native Realm scheduling/rollback, complete package/application compilation,
CloudKit, UI, signed, performance and application Release acceptance remain
separate. No Reader gitlink, target merge, production data or release gate is
changed by this review.
