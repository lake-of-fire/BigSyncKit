# RA-1: owned-record revision contract

Worker 2 / BigSyncKit. Adapter preferences inspected at
`2a28f8cfa48fa16c471fa4d54c2689f21567905f` remain unchanged. Reconciled manual
restore handoff: `95e25ba7119733e0fb91e3ff17a5bd12846d2b55`. See
[Worker 2 status](ReadingAnalyticsWorker2.md) for actual commits, caller joins and
unrun qualification. Normal Mark/Undo needs no quiescence, activation or publication proof.

## Existing APIs

A synchronized owned model implements ChangeMetadataRecordable, its normal Realm
primary key, and the existing semantic protocols:

- BigSyncInboundSemanticRecordValidating validates complete received domain values,
  record type/key/immutable owner and bounded assets. Missing local resources are
  not corrupt remote data.
- BigSyncInboundSemanticReplacementValidating implements both validation and
  inboundSemanticReplacementDisposition. Delegate to one model-owned selector.
- BigSyncInboundSemanticTargetValidating takes precedence when selection genuinely
  requires other target-Realm state. It is not a reason for another readiness graph.
- BigSyncOutboundSemanticObjectValidating validates actual local values before
  serialization. Foreign unchanged relay is replication, not reauthoring.

Validators are synchronous and read-only. Replacement/target selection is repeated
inside the actual target write against its current preimage. Do not journal from
inside a model's inbound selector.

For one immutable owner/key with admitted local state:

| Received value | Disposition |
| --- | --- |
| No local object | Apply validated incoming state. |
| Greater revision | preferIncomingRecord |
| Lower revision | preferExistingObject |
| Equal revision/equal normalized domain | preserveExistingObject |
| Equal revision/different domain or incompatible identity | Throw existing model semantic-validation failure. |

Do not use applyIncomingRecord for a greater winner: it leaves ordinary timestamp
and pending-local selection in charge. Do not use preserveExistingObject when a
lower server value requires repair: that alone does not queue upload work.

Semantic rejection is a per-record quarantine/disposition, not necessarily a thrown
failure of the entire saveChanges batch. Cancellation, resource and unavailable
admission errors retain their operational meaning.

## Equality and revisions

Compare typed authored domain values: identity, revision, coverage/facts/counters,
classification, domain tombstone and every other authored synchronized field on that
record. Session clocks, duration, end state, metadata and pace receipts participate in
record versioning without necessarily advancing Undo's semantic guard.

Exclude CloudKit system fields/change tags, server audit dates, serialized system
archives, transport process metadata, journal generation/binding, and intentionally
local-only recovery/presentation state. modifiedAt/explicitlyModifiedAt never override
an explicit model-selected revision. createdAt is excluded only when it is mere audit
metadata rather than domain history.

Compare sets/maps by membership/values, not traversal order, unordered JSON bytes,
CKAsset paths or checksums of unordered encoding. Preserve genuine list ordering.
Define one explicit empty-collection omission rule.

BigSyncStringEncodedIntegerModel/Codec use canonical decimal Int64. Revisions must be
representable and checked on advancement; actual W3 models require positive revisions.
Never wrap. Record revision, local semantic guard and upload journal generation are
three distinct values. Undo restores reading fields, not old revisions or generations.

## Real adapter and upload behavior

RealmSwiftAdapter.saveChanges(in:forceSave:) validates/selects inside the final write:

- A greater incoming winner bypasses metadata ordering. Existing stale local upload
  work is replaced atomically with the selected payload and a fresh journal generation.
- A preferred local winner preserves existing pending work. Without a journal, the
  adapter's internal journalCurrentValuePreservingChangeMetadata creates repair work
  without changing record ownership, revision or audit dates.
- Server system fields remain in tracking storage for conditional retransmission.
  Target/tracking Realms are separate transactions, not one atomic file.
- forceSave forces consideration, not unconditional server victory.

A local change after candidate selection does not defeat an explicit model preference.
Both winner choice and equal-version divergence use the final transaction preimage.
serverRecordChanged follows forceSave import, persistImportedChanges and the normal
upload loop; it uses the same model preferences as ordinary inbound data.

Journal forwarding can lag payload materialization. Tracking G1 may prepare the
current payload while target journal G2 is newer. Acknowledging G1 must preserve and
forward G2; another idempotent upload is valid. No authored version advances solely
for retransmission. An exact two-request test script is not the transport contract.

validateAuthoritativeOwnUploadRecords is validation-only. It cannot apply its returned
preference, rewrite values or clear restore flags. A late lower-version echo against
admitted newer state is valid old input, not necessarily corruption. didUpload uses
the prepared generation, never a fresh sample. Process echo metadata is not record
installation ownership.

## Authored transaction boundary

Capture/recheck the existing BigSyncClientIdentity/BigSyncMutationJournalIdentity.
In one owning Realm transaction validate the application guard/owner/lifetime, change
domain values, advance record revision, and require the journal. Ordinary writes use
refreshChangeMetadataRequiringJournal; identity-bound commands use the throwing
refreshChangeMetadata(explicitlyModified:at:expectedJournalIdentity:) overload.
A nonthrowing refresh inside an outer try does not establish rollback semantics.

Failure of a second required journal rolls back earlier changes in that same target
transaction. A tracking-Realm failure after the target commit cannot roll that earlier
commit back. Keep its selected value/journal for replay; do not compensate to stale state.

## Empty state and incoming deletion

A newer empty record remains a live version. Do not hard-delete it merely because it
has no coverage. BigSyncRetainsSyncedTombstone chooses the retained outbound-upsert
lane; it does not itself reject inbound hard deletion. Models also implement the
existing BigSyncInboundSemanticDeletionValidating according to their domain policy.
Ordinary generic deletion is not an indefinite negative-state guarantee.

## Raw backup versus reconciled manual restore

localDatasetRebootstrap and backupRestore are different contracts. For recovering
models, backupRestore withholds copied objects before retiring copied generations.
Its exception is an existing journal from the current installation/binding. The
isAwaitingRecoveryEvidence flag alone does not preserve independently known state
through a later backup-preparation pass.

W3 correction848b3f26 validates versions/domain even for a withheld copy: lower server
input cannot authorize either downgrading it or re-uploading greater unproven bytes;
it reports unavailable admission. Equal-divergent input rejects. Equal valid or greater
normal server input can use actual application to re-admit that key. Own-echo validation
cannot do so. The older BigSync test fixture deliberately exercises a more permissive
restore selector; it is transport coverage, NOT qualification of this Common policy.

For W8's actual original-versus-backup normalization, BigSync95e25ba7 provides
withReconciledManualBackupRestore(transactionIdentifier:configurations:reconciledObjectTypes:_:rollback:).
Under the existing exclusive lease, the caller installs the final files, with copied-only
rows withheld and independently validated current rows admitted. The helper publishes the
new sentinel/binding, then queues admitted values through the existing journal without
reauthoring them, before releasing the existing restore intent. Current-identity journal
recognition therefore preserves those known values through the real backupRestore pass.

Register the existing policy/live provider before this call. Do not prepare installation
inside its locked finalizer. Pass only final target configurations and explicitly
normalized concrete types. The helper does not establish provenance, choose history,
normalize raw backup flags or replace W8's physical journal/rollback implementation.
W3/W8 must bind it instead of a raw restore followed by unconditional normalization.

Its requirement marker is stored in the existing intent/event/receipt, so a raw retry
cannot skip repair after a crash. Failed post-event work retains handoffPending and the
same transaction; no rollback of installed files is authorized. Completed intent cleanup
retries without repeating file replacement. No second outbox, floor registry, restore
mode, cloud mutation service or publication protocol is introduced. Existing raw restore
entry points remain for their own contract; mismatched restore contracts reject rather
than silently reinterpret an in-flight transaction.

No real-user baseline selection, live restore or state clearing is authorized here.

## Qualification and removal boundary

No production adapter change is needed solely for decreasing owned values. The authored
suite uses actual Realm/journal/adapter code; only remote IO and existing boundary hooks
are controlled. It is not the actual Common inverse or native app. Scripted records have
no real server change tags. Native tests and the full physical restore/bootstrap journey
remain unrun; no compiler/test execution occurred in the production coding passes.

W6/W8 must close accepted-head/final-drain/seal/source/completion callers before W2 deletes
those exports. Ordinary outbound admission, real submitted-operation uncertainty, replay,
account/binding fencing and generation-matched acknowledgement remain. Existing physical
checkpoints must never be silently cleared or relabeled settled. An unapplied deletion
patch is not runtime removal or proof that a cross-repository batch is dependency-closed.
