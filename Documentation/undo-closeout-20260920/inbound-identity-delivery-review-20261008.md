# Committed inbound identity delivery — 8 October 2026

## Base and consumer boundary

This focused change follows the journal-forwarding ownership repair based on
BigSyncKit `eaffec59709b2f7a6f6f1bf45fc8f6e49b381135` (tree
`303092ac4ff1e232ea91985aa1794da1c62faa64`). It changes the existing domain
repair input boundary, not page application or token advancement.

`commitInboundPage` commits identity page batches with the zone cursor.
`reconcileDomainBeforeTerminalPublication` obtains their immutable merged batch
before awaiting the domain hook, then acknowledges its exact delivery ID only
after that hook returns and the run is revalidated. New committed pages rotate
that ID, so an earlier acknowledgement cannot consume later input.

Selected Reader Core `33aa3e` installs a domain prepublication hook which passes
`context.committedInboundIdentities` to graph reconciliation when imports or
identities are present. `BigSyncBackgroundActor` enables committed delivery when
that hook exists. This is the production repair boundary, not a fixture-only API.

## Defects and repair

The batch accessor read the shared live tracking Realm. An independent writer
could provisionally remove the delivery row or replace its ID and bytes. The
domain callback would then omit committed repair inputs or receive uncommitted
inputs. Inspect the existing frozen committed Realm snapshot instead, leaving
the independent writer untouched. Validate captured operation ownership after
refresh because notification delivery can replace the operation's authority.

Acknowledgement captured only the cancellation generation. An account, provider,
binding or transport replacement while waiting for tracking-write admission did
not necessarily change that generation; the old domain callback could remove
the still-matching delivery. Use the established operation owner validator
before and after the ownership wait and inside the final transaction. Exact
delivery-ID matching remains the authority to consume only the batch repaired.

No new journal, model, lock, schema or transport generation is introduced. The
DEBUG acknowledgement hook is an instance-scoped deterministic test seam and
is unset in production. A transaction which has already durably committed is
not rolled back when a later ownership check rejects the obsolete continuation.

## Authored native requirements

`SyncSplitOperationOwnershipTests` now includes:

- `testInboundIdentityInspectionIgnoresProvisionalReplacementAndRemoval`
  checks replacement and removal under an independent transaction, with both
  commit and cancellation controls. The accessor must return the exact earlier
  committed identity batch and leave the writer's transaction open. After
  settlement it must expose the appropriate committed successor or original.
- `testInboundIdentityAcknowledgementRejectsAccountAndTransportReplacement`
  replaces ownership at acknowledgement admission, requires cancellation with
  the exact repair batch retained, and includes a fresh acknowledgement control.

Fixtures write real tracking Realm records and inspect runtime batches; no
source-string assertions or timed scheduling assumptions are used. Apple native
Realm/Xcode execution is unavailable in this Linux environment. Compilation,
enabled test discovery and execution remain required on the consuming tuple;
no signed CloudKit or release-readiness result is claimed.
