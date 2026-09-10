# Accepted-head transport fencing

`sealPostBarrierQuiescenceForAcceptedHead` on both the synchronizer and worker
closes the transport gap for an installation that already observes the accepted
source head but has no completed final aggregate drain. This is NOT another
aggregate-cutoff path or a replacement for the original reservation protocol.

## Required host sequence

1. Read the actual accepted source head under current account/installation/binding
   admission. Persist the original domain operation and stop every relevant local
   writer in its transaction. For retained input, keep exact consent separate from
   current mutation authority. No old aggregate is converted into a new source.
2. Obtain the existing `beginPostBarrierOutboundQuiescence` token. Retain the full
   acquisition, not just its public durable barrier UUID. Do NOT call
   `establishPostBarrierDrain` or run an aggregate upload for this late adopter.
3. Call `sealPostBarrierQuiescenceForAcceptedHead`. Its synchronous domain-owner
   closure checks the original lifecycle admission. Its asynchronous proof must
   re-read the actual accepted head and exact durable domain operation. The API
   holds physical ownership, rejects unknown submissions, and checks the actual
   account before and after that proof. No source permission, aggregate receipt,
   completed drain or journal acknowledgement is generated.
4. Only after the returned transport snapshot is `recoveryRequired` may the host
   perform its exact archive/adoption/bootstrap transaction. That transaction
   must independently revalidate the original operation/head/principal and every
   retained journal generation. It must preserve original-operation continuity
   through local bootstrap and eventual publication completion.
5. Prove the committed local head/graph and source-only writer/outbound selection,
   then use the existing `beginPostBarrierSourcePublication` / restartable resume
   APIs. Peers stay fenced through real source upload, generation-matched ack,
   receipt certification and durable domain completion. Use receipt-bound final
   completion to release the exact acquisition.

Fresh first-upgrader reservation still requires the one-shot final aggregate
drain and `requirePostBarrierDrainRecoveryBeforeReservation`. The new API cannot
manufacture that capability and refuses an already-armed/completed aggregate
attempt. It must never be used to skip draining an aggregate head that has not
actually transitioned remotely.

## Failure and restart

Failure before sealing leaves `preparing` intact. A transport write may succeed
before cancellation or final delivery fails; re-inspect both durable stores,
never infer rollback from a thrown return. Once sealed, generic preparing abort
is no longer legal, including the crash before the domain records the seal.

Unknown submitted requests stay preserved. Neither disappearance of OS locks,
local-journal emptiness, elapsed time nor a single CloudKit fetch proves remote
settlement. The API does not supply a host reconciliation algorithm.

A kill after accepted-head sealing and before local adoption must resume under
an exact host/domain recovery decision; it must not turn the dead token into a
fresh final aggregate drain. After local commit, use committed-domain promotion
from `recoveryRequired` to owner-only `sourcePublication`.

## Executed coverage and limits

`bash Tools/run-accepted-head-boundary-probes.sh` compiles the unchanged production
source file (both public method bodies) with explicitly separate Foundation-only
synchronizer/account/gate collaborators. Fourteen XCTest methods cover success,
unknown submissions, proof rejection, cancellation, original-domain revocation,
account/binding/attempt/token replacement, state replacement, worker replacement,
and preservation of a successor's establishment task. Multi-input methods test
several boundaries without claiming extra distinct test counts.

This is portable control-flow typechecking/execution, NOT native CloudKit/Realm,
real cross-process lease, or final Reader composition qualification. Production
activation stays disabled. The new transport API alone does not complete the
Common intake/archive or concrete Reader provider implementation.
