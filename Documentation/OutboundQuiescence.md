# Outbound transport and retained checkpoints

The Reader accepted-head/final-drain/source-publication/paused-recovery public
APIs have been removed. Ordinary synchronization does not obtain a cutover token,
seal a reservation, or turn a terminal receipt into application authority. There
are no replacement aliases. Application startup and migration policy are outside
BigSyncKit.

## Ordinary mutations

`admitOutboundBatch` validates the original run, account lease, installation,
replica binding and invalidation generation before preparing a batch. The existing
coordinator's ordinary admission path is used directly, without a special owner
or source-publication branch.

The batch lease remains alive through preparation, network submission, local
response handling and generation-matched acknowledgment. Before entering transport,
`modifyRecordsHoldingOutboundLease` persists the exact record/zone identities,
prepared journal generations and available long-lived operation identity. This
checkpoint contains no independent mutation payload; the Realm journal remains
the sole upload-work journal.

A definitive server response is not sufficient to retire the marker. Required
local acknowledgment, requeue and conflict processing must complete first.
Cancellation, partial or missing results, an unavailable operation proxy, process
loss and a timeout are not definitive settlement.

Record revision is model-owned and independent of journal generation. Existing
model replacement preferences, target-transaction revalidation, strict journal
writes, late-generation acknowledgment protection and ordinary retransmission are
unchanged by cutover API removal.

## Persisted state and explicit recovery

`outboundQuiescenceSnapshot()` remains a read-only inspection of the existing
checkpoint format. Historical barrier phases and their fields remain decodable.
A persisted barrier still blocks ordinary admission; deleting its public creator
is not permission to clear, reinterpret or ignore that state.

The existing generic `recoverOutboundQuiescence(expected:authorizingRecovery:)`
entry remains. It takes exact checkpoint ownership, revalidates the original
account/installation/binding around suspensions and attempts exact long-lived
replay. Replayed outcomes pass through the same generation-matched adapter
callbacks as live results. Only the actual operation's definitive terminal
rejection can settle an operation-wide failure; a lookup error cannot.

Remaining uncertainty requires the existing explicit external settlement decision.
A throwing or cancelled decision leaves the checkpoint intact. The library does
not implement Reader history selection, adoption, baseline publication or a new
recovery UI.

The low-level coordinator and its internal barrier primitives remain unchanged
with the checkpoint/process-safety tests. They are not reachable through the
removed public cutover grants or ordinary owner-only upload branches. This is a
scoped public/runtime decoupling, not a claim that every file or stored field
containing the word "quiescence" was deleted.

## Retained regression surfaces

General gate/state-bound, cross-process, long-lived replay, callback lifetime,
partial-result, account/binding, journal and reconciled-manual-restore coverage
remains. The exclusive accepted-head and paused-recovery portable probes and
scripts were deleted with their APIs. Their workflow jobs/invocations were removed;
the existing general transport jobs remain.

Some native terminal/settlement cases still exercise the removed capabilities.
Those cases must be retired or retargeted during compile/test follow-through;
never restore obsolete production aliases just to compile old tests. No test,
compiler, native runner or CloudKit request was executed for this removal.
