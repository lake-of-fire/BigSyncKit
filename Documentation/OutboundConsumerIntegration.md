# Binding the outbound fence to domain completion

## Scope

BigSyncKit owns transport admission, live ownership, durable uncertainty and terminal receipts. Common owns the writer barrier, intake, committed graph, retained history and completion rows. ReaderCore owns lifecycle admission, approval and orchestration; the root owns dependency selection and Tuist integration. This contract does not move those responsibilities into BigSyncKit and does not enable production Ordered-V2.

The APIs below exist on both `CloudKitSynchronizer` and `BigSyncBackgroundActor`. Use the background worker surface when the host can replace its installed synchronizer. Never reconstruct live tokens from persisted barrier identifiers.

## Live transition

1. Finish the domain's released-data intake and persist its transaction-enforced preparing writer barrier. Obtain `beginPostBarrierOutboundQuiescence(writerBarrierEvidenceID:)`; persist/associate the actual returned barrier identifier, not a replacement UUID.
2. Await `establishPostBarrierDrain(quiescence:)`, run ordinary synchronization, and obtain `completedPostBarrierDrain(using:authorizedBy:)`. This is the only aggregate cutoff. Keep the original token even when establishment fails.
3. Before any reservation or source-head CAS, call `requirePostBarrierDrainRecoveryBeforeReservation`. Associate the returned exact checkpoint with the original domain operation. A crash between the transport write and association is recovery-required, not abortable preparing state.
4. Finish the original reservation/head/graph operation. Atomically switch every relevant domain writer AND this owner's outbound preparation to the committed new authority. BigSyncKit's source phase does not filter legacy business entity types for the host.
5. Use `revalidatePostBarrierOutboundQuiescence(token)` before the host's next domain read, and `validatePostBarrierOutboundQuiescence(token)` after that suspension. Compare the exact expected operation/head/graph under the domain's own admission. Then call `beginPostBarrierSourcePublication(token, expected: checkpoint, sourcePublicationEvidenceID:)`. Do not resolve the fence here: peers must remain blocked through publication.
6. Run ordinary source synchronization and retain its actual terminal receipt. Call `revalidatePostBarrierSourcePublication(using: receipt, ownedBy: token)` and retain the returned clean checkpoint. This checks the ordinary receipt, the exact source acquisition and all earlier unresolved transport submissions. A nil receipt, semantic block, unrelated receipt or pending generation cannot be treated as completion.
7. Commit the domain's completion state using its existing immutable operation/head/graph and publication certificate checks. Completion bookkeeping must not author unacknowledged source generations. After the domain suspension, recheck its original admission and call `completePostBarrierSourcePublication(token, using: receipt, expected: checkpoint, completionEvidenceID:)`. The call performs synchronous final source/receipt/checkpoint validation before exact release. No await separates those transport checks from the durable release.

Any new source write or newer synchronization can invalidate step 6's receipt. Preserve the committed domain state, obtain another ordinary source receipt under the same eligible owner, and re-evaluate completion; do not start another aggregate cutoff. Explicit release preserves the ordinary receipt's existing validation rules.

## Restart and cancellation decisions

| Observed transport state | Required action |
| --- | --- |
| `preparing`, no reservation, live original acquisition | Exact domain checkpoint validation and pre-reservation abort, or continue the original aggregate preparation. |
| `preparing`, owner lost or outcome ambiguous | Exact host recovery. A preparing checkpoint cannot be promoted to source publication. |
| `recoveryRequired`, domain has not committed | Existing reservation/accepted-head recovery. Do not declare source authority or reopen solely from the transport phase. |
| `recoveryRequired`, domain commit won the crash race | `resumePostBarrierSourcePublication` with proof of the exact committed new authority and settlement of every submitted request. Promotion preserves peer exclusion. |
| `sourcePublication`, owner lost or cancelled | Abandon the old live token when present, then explicitly resume the exact current checkpoint under proof. Resume creates a distinct acquisition even on the same synchronizer. |
| Domain completion committed, barrier still present | Treat as finalization pending. Resume, obtain/revalidate an ordinary source receipt and repeat the domain's idempotent completion before release; do not classify the combined state as complete. |
| Barrier absent after an uncertain release | Reinspect the durable domain completion and transport evidence. An absent barrier by itself is not evidence that this domain operation completed. Never apply an old token to a successor. |
| Unresolved submission without an authoritative settlement mechanism | Preserve the checkpoint and journals. Do not replace proof with elapsed time, process death, empty journals or a single fetch. |

Cancellation permanently revokes the current acquisition, including a preparing owner not yet armed and a recovery-required owner not yet promoted. Starting a new ordinary synchronization or supplying a new evidence string cannot revive it. Exact pre-reservation abort and physical request settlement retain their separate contracts.

## Host admission across recovery awaits

Use the overloads with `revalidatingDomainOwner:` whenever a domain operation can be superseded while account lookup or proof is suspended. The synchronous callback must validate the existing host admission/generation without writing data or starting another operation. It is called again after the final account lookup, before the durable transport write. The worker also checks its own installed synchronizer at these boundaries.

`authorizingRecovery:` and `authorizingResume:` still perform the actual asynchronous domain reconciliation and return the durable evidence identifier. They must preserve exact operation/head/account/install/binding scope and account for all submitted requests in the supplied checkpoint. The synchronous callback cannot turn an unsupported reconciliation into a supported one, and the evidence identifier is not self-authenticating.

New completion APIs are in `Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+OutboundCompletion.swift`. Package source discovery should include that file; explicit Tuist source lists must include it when selecting this vendor revision. Do not edit generated Xcode project files.

## Qualification

This completion increment is implementation-only. No tests, native build, Tuist regeneration or CloudKit account operations were run for it. Earlier evidence belongs to its exact earlier source revision. Keep source PRs draft and production activation disabled pending the composed release qualification.

## Restart before the accepted domain graph is ready

A restarted sealed reservation may need exclusive physical ownership to finish
its accepted snapshot or archive/adopt a winner, while the local graph is not yet
ready for source publication. Neither clearing the peer fence nor claiming a
source-only graph early is a valid acquisition shortcut.

`acquirePostBarrierRecoveryOwnership(expected:revalidatingDomainOwner:authorizingRecovery:)`
exists on both the synchronizer and background worker. It takes exclusive owner
and batch locks, holds them across the host's exact settlement/operation proof,
rechecks the real account and host admission after the final suspension, and
returns a fresh live acquisition in **the same `recoveryRequired` phase**. All
outbound admission remains disabled. The host proof must reconcile every
outstanding request and its required generation-matched local handling. Missing
settlement support must throw and preserve the original checkpoint.

This API cannot reacquire `preparing` as an aggregate-drain authorization and
cannot be used to downgrade `sourcePublication`. It does not assert a committed
domain graph, authorize a conditional head save, or fabricate a terminal receipt.
After the host finishes its exact accepted-domain decision and durably switches
writers/outbound selection, the existing `beginPostBarrierSourcePublication`
handoff can enable this owner's source batches. Final release still follows
source acknowledgement and durable domain completion. Failure or cancellation
abandons only this live acquisition and never reopens peer admission.
