# Cross-process outbound quiescence

This is transport infrastructure for a **current-release, cooperating client**. It does not activate Ordered-V2, authorize domain writers, migrate a Realm, change an account binding, or contact CloudKit by itself. The existing record-level Realm mutation journal and generation-matched acknowledgements remain authoritative.

## Scope and automatic participation

Every ordinary save/delete batch now acquires a shared outbound admission **before adapter preparation**. The independent file descriptor stays alive through preparation, transport, response processing, conflicts/requeues, and generation-matched acknowledgements. Logical cancellation of a synchronizer or its waiter is not physical completion of an entered batch.

The gate uses the same durable client namespace and shared base as installation/restore identity:

```
<backupDetectionBaseURL>/OutboundQuiescence/<durableStateNamespace>/
    admission.lock       short state/admission mutex
    owner.lock           exclusive live cutoff/recovery ownership
    batches.lock         shared batches; exclusive cutoff/recovery
    state.json           versioned cutoff and transport-uncertainty metadata
    initialized          fail-closed missing-state detection
```

The production Realm factory already chooses the App Group base when `suiteName` is configured, and custom `localState` uses its existing identity base. All processes writing the same client **must use that same base and durable identity**, just as for the existing client-identity lease. Distinct namespaces remain independent. There is no opt-in per extension and no public bypass for ordinary upload admission. Direct low-level adapter tests now prepare real account authority rather than bypassing the gate.

Lock files are stable and independently opened; never delete, replace, or “clean up” them or the directory while clients may be alive. Files are backup-excluded on Apple platforms. The existing filesystem durability/EINTR primitives were extracted from `KeyValueStore.swift`, not replaced with restore intent/event semantics. No blocking flock parks the cooperative executor. A short metadata collision is retried by batch submission/settlement and acquisition waits; synchronous lifecycle methods may return `busy` and should be retried without changing domain phase.

## Consumer sequence (Workers 2 and 3)

The following APIs are available on **both** `CloudKitSynchronizer` and `BigSyncBackgroundActor`. Worker methods check that the installed synchronizer has not been displaced across their asynchronous boundaries.

1. **Domain transaction:** persist the durable local writer barrier and ensure every relevant author, including extension and inbound/domain paths, checks it in the same write transaction. Allocate a stable, nonempty `writerBarrierEvidenceID`. This is the domain's responsibility; BigSyncKit does not inspect business rows.
2. **Publish outbound fence:** `beginPostBarrierOutboundQuiescence(writerBarrierEvidenceID:)` returns `PostBarrierOutboundQuiescence`. Persist/associate its public identifier with the domain attempt as needed. The public identifier names the durable barrier; token equality additionally identifies a unique live acquisition, including same-synchronizer resume. This token identifies the exact live owner; it is **not** proof of quiescence. The fence is durable before the method returns. New peers cannot prepare another batch. Already-admitted peers may finish.
3. **Await physical drain:** `await establishPostBarrierDrain(quiescence:)` acquires exclusive batch ownership after all old batch scopes exit, verifies no submitted request has an unresolved outcome, revalidates account/installation/binding/epoch and attempt ownership, and arms one final drain. Keep the token even when this method throws or is cancelled.
4. **Run normal synchronization:** the owning next full drain may fetch/import and upload through the existing pipeline while peers remain fenced. Obtain its terminal receipt, then `await completedPostBarrierDrain(using:authorizedBy:)`. Require `completed.outboundQuiescenceIdentifier == token.identifier`. The completed receipt also retains its existing exact run, account, binding, cursor, snapshot and journal checks. The completed drain seals further owner batches; it cannot be rearmed.
5. **Before any reservation/CAS-capable domain write:** call `await requirePostBarrierDrainRecoveryBeforeReservation(completed)`. It revalidates the completed drain and synchronously writes the transport phase `recoveryRequired`. Only **after success** may the domain persist its reservation. A crash or uncertain write between these two stores leaves a stricter fence, not reopened uploads. Continue to use the existing completed-drain checks before reservation/head-CAS, and validate the domain's exact journal/snapshot inside its transaction.
6. **After bootstrap:** use the existing `revalidatePostBarrierDrainPrincipal` / `validatePostBarrierDrainPrincipal` for continuity only; new source journals intentionally invalidate the pre-bootstrap empty-journal witness. These methods do not authorize reservation or publication.
7. **Owner-only source publication:** after the new head/local graph and writer authority are durably committed, take the exact `recoveryRequired` checkpoint and call `beginPostBarrierSourcePublication(token, expected: checkpoint, sourcePublicationEvidenceID:)`. The barrier moves durably to `sourcePublication`: peer/legacy aggregate batches stay blocked, while this exact original principal may run the normal synchronization pipeline to upload and generation-match acknowledge the new source journals. Do **not** arm another aggregate cutoff. Require and retain the ordinary terminal source receipt/certificate needed by the domain completion contract.
8. **Durable completion and release:** revalidate the ordinary source receipt with `revalidatePostBarrierSourcePublication(using:ownedBy:)`, then commit the domain's exact completion state. After that domain suspension, recheck its admission and call `completePostBarrierSourcePublication(token, using: receipt, expected: checkpoint, completionEvidenceID:)`. This non-suspending completion API requires the receipt for the exact live source acquisition, an unchanged clean checkpoint, and no pending journal work. It releases through the existing exact resolution path without revoking the ordinary receipt. Generic `resolvePostBarrierOutboundQuiescence` remains for explicit domain recovery; it is not a substitute for source-receipt validation. A live owner still in `preparing` cannot resolve its fence; use exact pre-reservation abort when rolling back instead. Peer transport reopens only at explicit release.

**Production activation stays disabled.** Workers 2/3 own the business-state proof and invocation; Worker 4 owns subsequent vendor integration. This repository contains no Reader/Common/Core changes.

## Abort, cancellation and abandonment

`abortPostBarrierOutboundQuiescence(token)` applies only to its exact live **pre-reservation** owner, with no active owning batch and unchanged principal. Before invoking it, the domain must irrevocably decide that this attempt cannot write a reservation. It preserves any older unresolved transport submissions and cannot remove a successor's fence. The domain separately rolls back/unblocks its writer row in the correct transaction. An error is not proof that no durable write happened: re-read the checkpoint before choosing recovery.

Cancellation irrevocably seals the live acquisition in every phase but **does not reopen transport**. It also prevents an unarmed `preparing` owner from arming later, and a `recoveryRequired` owner from enabling source publication with its cancelled token. Physical lease validity remains separate so already-submitted requests can still settle. Restarting ordinary synchronization does not revive a cancelled source grant: abandon the exact token and explicitly resume under a new proof. Revoking an aggregate `PostBarrierDrainAuthorization` seals only that aggregate grant, not a subsequent source grant. An owner batch prepared before revocation must recheck admission before entering transport; an already-submitted batch can still finish physical settlement bookkeeping. A cancelled task cannot perform cleanup that requires current authority: dispatch that exact cleanup to a noncancelled lifecycle turn after the domain's cancellation decision. Generic defer/destructor cleanup must not reopen a gate. Once `recoveryRequired` was persisted, even a failed reservation needs explicit domain recovery, not pre-reservation abort.

`abandonPostBarrierOutboundQuiescence(token)` drops local ownership without changing durable state. Actual batch scopes retain the OS lease until they unwind. After those scopes exit, a fresh worker may acquire recovery ownership. If the domain commit preceded a crash while the persisted barrier was still `recoveryRequired`, or the barrier is already `sourcePublication`, prefer `resumePostBarrierSourcePublication(expected:authorizingResume:)`: the host must prove the exact committed domain state and settle every outstanding submission in that checkpoint, then BigSyncKit reacquires owner/batch locks, clears only those proven-settled markers, and promotes or resumes owner-only source publication without opening peers. A `preparing` checkpoint cannot be promoted. The host proof must establish that the new authority/head/local graph is committed; the checkpoint alone never proves a domain commit. Old completed/armed capabilities cannot authorize a successor. A dropped owner, a process exit, a suspended task, or elapsed time never clears durable cutoff state.

## Crash/restart and ambiguous server outcomes

A short shared lease alone is insufficient: an OS releases it at process death, but a submitted server request may still commit. Before entering `CloudKitRecordStore.modifyRecords`, a batch durably records a unique submission identifier and its principal. It removes **only its own** identifier after an exact, definitive response **and completion of the required generation-matched local response handling**, after a recognized whole-operation definitive rejection that requires no per-item acknowledgement, or after proof that this request never entered transport. Cancellation or authority replacement after a definitive CloudKit return but before local acknowledgement intentionally retains the marker, so OS-lease release cannot create a false cutoff. Missing/malformed results, unknown errors and network uncertainty also retain the marker. Successful independent items still follow the existing generation-matched acknowledgement rules even when the overall batch has uncertainty.

These metadata markers contain **no record values, mutation payloads, mutation generations, retry instructions or merge clocks**. They are not a second mutation journal or an alternate acknowledgement mechanism. An owner cannot issue a quiescence capability while markers remain, even when every OS batch lease is free. Marker count/size are bounded and corruption or missing initialized state fails closed.

For a crashed/abandoned owner, read `outboundQuiescenceSnapshot()` and call:

```swift
try await worker.recoverOutboundQuiescence(expected: checkpoint) { exactCheckpoint in
    // Domain implementation, not provided by BigSyncKit:
    // - reconcile its exact preparing/reservation/bootstrap state;
    // - establish that reopening current transport is safe;
    // - establish definitive settlement of EVERY outstanding submission;
    // - commit that decision durably and return its evidence identifier.
    try await domain.commitOutboundRecoveryProof(for: exactCheckpoint)
}
```

The library holds exclusive owner and batch leases across this callback, checks the exact snapshot, and validates current account, binding, installation, invalidation epoch and synchronizer attempt around the proof. Worker replacement is checked **inside the proof wrapper before any reopening**, not only after a return. A changed account/binding requires a fresh recovery decision; an old token never revives on account return.

A fresh worker first needs normal account validation. A normal synchronization can validate account and perform inbound reconciliation while being blocked at outbound admission. Restore reconciliation is allowed to establish a transport principal while its public domain-writer lease remains withheld; explicit recovery can use that validated context without granting writer or cutoff authority. No stale startup snapshot can itself grant a live principal.

**Domain contract:** an arbitrary UUID, a single fetch, cancellation completion, OS-lock disappearance, elapsed time, a retry, or an empty local journal is **not** proof that an unknown earlier request cannot later commit. When the host has no authoritative settlement mechanism for the exact checkpoint, recovery must throw and the cutoff remains blocked. Do not auto-clear markers on restart. This is intentionally fail-closed, not a claim that CloudKit supplies a distributed transaction or that local locks fence old binaries/other devices.

## Legacy API compatibility

`establishPostBarrierDrain(writerBarrierEvidenceID:)` remains for explicitly externally-proven barriers and existing consumers. Its authorization/completed capability has **nil** `outboundQuiescenceIdentifier`; it does **not** certify cross-process upload quiescence. It cannot borrow an in-progress library-owned cutoff. New production cutoff code must use the token overload and reject nil identifiers.

## Checks

`Tools/run-outbound-quiescence-portable-tests.sh` compiles the **unmodified** filesystem/gate sources and the same `BigSyncOutboundQuiescenceTests` XCTest file used by the package. `Tools/run-outbound-quiescence-process-tests.sh` compiles those sources with a test-only subprocess driver and exercises real exec/SIGKILL and independent file descriptions. Both work on a compatible Swift/macOS or Swift/Linux host; these are not CloudKit/Realm qualification.

Native composition tests are added to `CloudKitTerminalReceiptTests`, `CloudKitOutboundSettlementTests`, and `BigSyncKitTests` (generation preservation). Run these and the prior ownership/receipt/cursor/restore suites on the supported Apple package environment before integration/release. The existing native evidence on the parent commit is not evidence for these new changes. Real signed App Group, device suspension/protected-data behavior, server fault injection and multi-device qualification remain separate release checks.

## Source-publication receipt boundary

A source-publication run captures its unique live owner at admission, before account or adapter suspension. Both outbound admission and terminal receipt issuance recheck that acquisition; abandonment cannot silently downgrade the run to ordinary success, and an old token cannot abandon or resolve a same-synchronizer resume. Terminal source publication also requires zero active owning batches and zero outstanding submission markers, including markers left by an earlier failed source run. An empty local journal by itself does not prove this condition. These checks do not turn a transport receipt into domain evidence or filter legacy entity types: the host must have durably switched this owner's outbound preparation to the new authority before granting source publication.

After the domain has durably completed and explicitly released the exact gate, its already-issued ordinary source receipt retains the normal receipt-revalidation contract. Cancellation still revokes receipts; release is not cancellation.


## Consumer validation and completion APIs

`validatePostBarrierOutboundQuiescence(token)` and its async `revalidatePostBarrierOutboundQuiescence` counterpart expose exact live-owner validation without reusing a completed aggregate capability. The async form queries the real account provider and rejects an intervening synchronization attempt. Use the synchronous form after the host's own suspension/transaction to close the last local admission gap. The returned snapshot is not a drain certificate: a preparing owner may still be waiting for peer batches.

For source publication, use `validatePostBarrierSourcePublication(using:ownedBy:)` / `revalidatePostBarrierSourcePublication(using:ownedBy:)`. These compose the existing ordinary terminal-receipt, source-run owner and zero-outstanding-submission checks. They reject unrelated ordinary receipts, pre-bootstrap aggregate receipts and receipts from an earlier acquisition of the same durable barrier. The old aggregate `CompletedPostBarrierDrain` intentionally expires when source synchronization begins; it must not be used for final source completion.

Both `recoverOutboundQuiescence` and `resumePostBarrierSourcePublication` also have a `revalidatingDomainOwner:` overload on the synchronizer and background worker. Its required synchronous, read-only closure validates the host's existing admission/generation at every internal suspension, especially after the final account lookup and immediately before durable resolution or promotion. The worker composes that check with its own synchronizer identity. This avoids a gap where the host proof returns, its admission changes during the last account lookup, and a stale decision mutates transport. Older overloads remain source-compatible, but consumers with replaceable domain operations should use the guarded overload. No wrapper supplies the missing committed-head/graph or remote-settlement proof.

See `OutboundConsumerIntegration.md` for the binding sequence and restart decisions. These APIs do not install a release provider, enable Ordered-V2, alter business entity selection, or change the application's Tuist configuration.
