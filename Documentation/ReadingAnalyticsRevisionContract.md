# RA-1: owned-record revision contract

Worker 2 / BigSyncKit. Production inspected: `2a28f8cfa48fa16c471fa4d54c2689f21567905f`,
unchanged through the first integration merge `5923cdccc39d90952865d27041ee7ac1168a1ac8`.
This is the application-model integration contract for W3, not a new transport API.
Normal Mark/Undo needs no quiescence acquisition, source activation or publication proof.
See [Worker 2 status](ReadingAnalyticsWorker2.md) for exact test commits and unrun gates.

## Existing APIs W3 should implement

A synchronized owned model implements `ChangeMetadataRecordable`, its normal Realm
primary key, and the existing semantic validation protocols:

- `BigSyncInboundSemanticRecordValidating`: validate the complete received domain
  payload and its record type/key/immutable owner. Validate bounds before decoding
  large assets. An unavailable local resource is not corrupt remote data.
- `BigSyncInboundSemanticReplacementValidating`: implement both
  `validateInboundSemanticReplacement(_:existingObject:)` and
  `inboundSemanticReplacementDisposition(_:existingObject:)`. The latter is the
  winner decision. Delegate both to one model-owned validation/selection function.
- Use `BigSyncInboundSemanticTargetValidating` instead only when the decision
  genuinely requires other state in the target Realm. That hook takes precedence
  over the replacement hook. Do not introduce a readiness graph for owned analytics.
- `BigSyncOutboundSemanticObjectValidating`: validate the actual local payload
  before serialization. Unchanged relay/reseed of a foreign-owned version is
  replication, not permission to change its owner or increment its revision.

All validators are synchronous and read-only. The adapter calls replacement/target
selection again inside the actual target write, with the current managed preimage.
The model must not create journals itself while selecting an inbound winner.

For the same immutable owner and logical record key, with admitted local state:

| Received version | Disposition |
| --- | --- |
| No existing object | `applyIncomingRecord` after full validation |
| Higher state revision | `preferIncomingRecord` |
| Lower state revision | `preferExistingObject` |
| Equal revision and equal normalized domain value | `preserveExistingObject` (replay) |
| Equal revision and different domain value, or incompatible immutable identity | Throw a model error using `BigSyncInboundSemanticValidationFailure` diagnostics |

Do not return `applyIncomingRecord` for a higher-version winner: that permits the
ordinary timestamp/pending-local policy to make the final choice. Do not use
`preserveExistingObject` for an older server value that needs repair: preservation
alone does not create upload work when none exists.

A semantic rejection is reported through existing per-record quarantine/disposition
results. It is not necessarily a thrown failure of the entire `saveChanges` batch.
Cancellation/resource/admission-unavailable errors retain their operational error
contract; do not reclassify them as an equal-version conflict.

## What equality means

Compare typed, normalized authored domain values. This includes immutable identity,
revision, reading memberships/facts/counters/classifications, the domain tombstone,
and every other authored synchronized field on that same record. On Sessions (or
owned records containing clocks), duration, clock ownership, end state, metadata and
pace receipts are part of the versioned payload. Those writes advance record revision
without necessarily invalidating Undo.

Exclude CloudKit system fields (record change tag, server creation/modification dates,
encoded system-field archive), synchronizer transport metadata/device UUID, journal
generation/binding, and deliberately local-only recovery or presentation fields.
`modifiedAt`/`explicitlyModifiedAt` are conflict/audit metadata, not a fallback winner
clock after revision selection. Treat `createdAt` according to its actual model
meaning: exclude a mere audit field, retain it when it represents domain history.

Compare sets/maps by semantic membership and values, not iteration order, JSON bytes,
CKAsset paths, or checksums of unordered serialization. Preserve order for genuinely
ordered domain lists. Empty-collection omission must have one explicit decoding rule.

BigSync's existing `BigSyncStringEncodedIntegerModel` and
`BigSyncStringEncodedIntegerCodec` use canonical decimal **Int64** on the wire.
Use a nonnegative representable revision and checked advancement; W3's actual model
requires positive revisions. A conceptual UInt64 is not an implicit promise that
Realm or this codec accepts values above Int64.max. Never wrap a revision.

## How the real adapter handles the preferences

`RealmSwiftAdapter.saveChanges(in:forceSave:)` evaluates the model decision in its
final target transaction before ordinary pending-local/timestamp selection:

- A higher incoming winner bypasses ordinary metadata ordering in `applyChanges`.
  If stale local upload work exists, the selected payload and a **fresh** journal
  generation replace it together. An old acknowledgement cannot retire that new work.
- A preferred local winner with a pending journal preserves that journal and payload.
  With no pending journal, the adapter calls its internal
  `journalCurrentValuePreservingChangeMetadata(at:)` in the target transaction.
  This creates ordinary upload work without incrementing the model's revision,
  changing its owner, or advancing its modification timestamps.
- The server record's system fields are retained in tracking storage for subsequent
  conditional retransmission. Target and tracking Realms are not one physical
  transaction; the existing redelivery/journal mechanisms remain responsible for that
  boundary. No second outbox is added by RA-1.
- `forceSave: true` forces import consideration, not unconditional server victory.

A local write between candidate selection and application does not defeat an explicit
model preference. Both winner selection and equal-version divergence must be checked
against that final preimage, even when the local journal changed in the interval.

The real `CloudKitSynchronizer` upload path receives `serverRecordChanged`, imports
its server record with `saveChanges(... forceSave: true)`, persists imported changes,
and uses the ordinary upload loop again. The same preferences therefore govern
conflict retry, not only ordinary downloads.

Journal forwarding and payload materialization are separate. For the unscoped owned
records exercised here, tracking may still carry G1 while a retry serializes the
current target value whose journal has advanced to G2. `didUpload` may acknowledge
G1 in tracking, but must leave G2 in the target journal and forward it for another
ordinary upload. Two successful saves of the same selected value are permitted; an
exact two-request script is not the transport contract. No authored revision changes
merely because a value needs retransmission.

An authoritative same-process own echo is validation-only via
`validateAuthoritativeOwnUploadRecords`. Its returned preference is not applied to
rewrite the target. A late lower-version own echo must be valid historical input,
not a semantic conflict merely because the current target is newer. A mutation result
is acknowledged separately by `didUpload(savedRecords:matchingGenerations:)` using the
prepared generation, never a newly sampled one. Echo detection is process metadata,
not a domain installation-ownership check.

## Local authored transaction boundary

Use the existing installation/binding APIs, including `BigSyncClientIdentity` and
`BigSyncMutationJournalIdentity`. A prepared command retains/rechecks its original
identity. In the same owning Realm write:

1. Validate the application's owner/lifetime/semantic guard.
2. Change the domain fields and advance the owned record revision.
3. Require the throwing journal refresh.

Ordinary writers use `try refreshChangeMetadataRequiringJournal(at:)`. A command
capturing an identity uses the throwing
`refreshChangeMetadata(explicitlyModified:at:expectedJournalIdentity:)` overload.
Let any error escape the enclosing transaction. Do not call the nonthrowing overload
and assume an outer `try` makes it atomic.

A second journal failure in one target transaction rolls back earlier objects in
that transaction. Conversely, a later tracking-Realm failure cannot roll back an
already committed target-Realm winner. Preserve that value and its journal for
redelivery; do not implement a compensating downgrade across the two physical files.

Record revision, local Undo semantic guard, and upload journal generation are three
different values. A clock-only authored edit advances the first and third, not the
second. Undo restores reading fields, not clocks, old revisions or old generations.

## Empty state, deletion and restore

A newer empty owned record remains a live version. Do not turn its empty membership
into a CloudKit hard deletion. A model using `isDeleted` as retained ordered negative
state must opt into `BigSyncRetainsSyncedTombstone` and validate actual record deletion
through `BigSyncInboundSemanticDeletionValidating`. The former chooses the outbound
upsert lane; it does not by itself reject an incoming hard deletion. W3 owns that
policy; ordinary generic deletion is not indefinite negative-state retention.

Restore/reseed preserves original owner/revision/domain values. A new journal
installation/binding may transport admitted unchanged values; it must not turn copied
bytes into a newly authored version or silently admit stale history as current truth.

`localDatasetRebootstrap` and `backupRestore` are different contracts. For models
implementing `BigSyncRestoredObjectRecovering`, backup preparation withholds copied
objects before retiring copied pending generations. W3 currently uses the local-only
`isAwaitingRecoveryEvidence` flag and blocks outbound serialization while it is set.
A real normal server import can re-admit that exact key through `preferIncomingRecord`,
even when the unadmitted copy had a higher revision. This is a restore-admission
policy, not permission to downgrade already admitted revision-ordered state. Absence
from the restored destination does not authorize republishing the copied object.
Validation-only own echoes cannot apply that preference or clear the recovery flag.

W8 supplies the explicit baseline/restore admission decisions. No live restore,
baseline choice, state clearing or migration is authorized by this note.

## Qualification and removal boundary

The source above already provides the required preference APIs. No production change
is justified solely by the need for decreasing owned values. The targeted RA-1 suite
uses a real Realm model, real target/tracking Realms and the actual adapter; remote IO
may be scripted. Native tests must run on the W1-approved runner. Syntax checks or a
portable comparator do not qualify the adapter or the actual Common models. The
scripted records do not carry real server-assigned change tags: conflict dispatch
coverage is not signed CloudKit conditional-save qualification.

W6/W8 must confirm removal of accepted-head/final-drain/seal/source-publication callers
before W2 deletes their exported APIs. Ordinary upload currently uses outbound admission
and submitted-operation uncertainty bookkeeping; those are not an Undo dependency and
must not be removed by filename. Existing persisted physical uncertainty is never
silently cleared or interpreted as settled.
