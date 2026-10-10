# Journal forwarding operation ownership — 8 October 2026

## Exact base and reviewed scope

This repair is based on the selected BigSyncKit revision
`eaffec59709b2f7a6f6f1bf45fc8f6e49b381135`, tree
`303092ac4ff1e232ea91985aa1794da1c62faa64`, on
`codex/closeout-retained-bigsynckit-20261008`. The baseline adapter blob is
`333996501da326ee30568d41c7e3a603bcd148b4`; the baseline
`SyncSplitOperationOwnershipTests.swift` blob is
`582aa78b24515fd7190cbb1c1f1660bdec16b695`.

The selected revision already includes the committed upload-read repair and
missing-server retry ownership repair. This finding concerns ordinary durable
journal forwarding rather than either of those previously repaired operations.

## Defect

Journal forwarding captures its persistence provider before an asynchronous
tracking-write admission. Its transaction checked only the mutable cancellation
Boolean and the current journal's account/binding eligibility. Cancellation
followed by migration preparation clears that Boolean for a successor without
restoring the earlier operation's authority. The retired continuation could
therefore publish tracking mutations after cancellation. An account or transport
replacement can likewise leave an unscoped journal row eligible under the same
binding, while changing the authority of the operation which captured it.

The paged loop and multi-target loop also had no immutable owner across their
awaits. The excluded/unknown generation cleanup did not check operation
ownership at target-write admission. A stale continuation could continue into
that journal-consuming phase after its tracked phase committed.

## Repair and invariants

Reuse `operationOwnerValidator()` for the observed-journal processor, the
multi-target loop and both forwarding
overloads. Capture the cancellation generation, provider identity, account,
binding, rebase context, container and database scope at entry. Validate before
queued writes, within the ownership-preserving transactions, after target
refresh notification delivery, across page/target awaits, and around external
progress/delegate callouts. Validate the same captured authority for ignored
generation cleanup.

The public `didFinishImport(progress:)` admission captures operation ownership
before setup callbacks. Setup may initialize or replace an interrupted provider,
so only that setup interval omits provider identity while retaining cancellation,
account, binding and transport checks. After validated setup it captures strict
provider ownership for the remaining import phases. Public progress regressions
exercise cancellation/reset before forwarding, and a nil-provider positive
control requires ordinary initialized setup to succeed. Status publication is
followed by validation before invoking the delegate.

Current committed target snapshots and exact journal generations remain the
mutation authority. Successful tracking phases remain durable if later ownership
is revoked; the still-retained target journal lets a fresh operation retry. No
new journal, transport generation, lock, field clock, schema, timeout or account
operation is introduced.

## Authored behavior regressions

Four methods reuse the existing explicit Realm fixture and forwarding/public progress boundaries:

- `testPublicImportRejectsCancellationResetFromProgressBeforeForwarding`
- `testPublicImportMayInitializeItsProviderWithoutReplacingOperationOwnership`
- `testJournalForwardingRejectsCancellationResetBeforeTrackingAdmission`
- `testJournalForwardingRejectsAccountAndTransportReplacementBeforeTrackingAdmission`

The tests create a real authoritative target mutation, retire or replace its
forwarding owner before tracking admission, and require a cancellation error,
no published tracking row, and the exact surviving target journal generation.
The cancellation case includes a fresh forward, upload and acknowledgement
positive control that drains the retained generation normally.

## Qualification and limits

The production source and fixture changes were reviewed at their exact base.
No source-string behavior assertion was added. This Linux environment has no
Apple Realm/Xcode runtime; these methods have not been compiled, discovered or
executed here. They remain native qualification requirements for the consuming
Reader tuple. This repair does not claim signed CloudKit, process-kill,
two-device, released-client or release-readiness evidence.
