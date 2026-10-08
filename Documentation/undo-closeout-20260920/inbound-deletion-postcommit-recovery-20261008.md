# Inbound deletion postcommit recovery requirement

This test supplement starts from BigSyncKit #140 at
`a90a559af3e148255864e81dd3e2bf4f247d5cd2`, whose production owner repair and
four replacement schedules remain unchanged. The original Reader-selected
`eaffec59709b2f7a6f6f1bf45fc8f6e49b381135` tree is also the live main base
`ea7ccb1702015feab3479d7dc2e96d4e42010a1d` tree.

The new DEBUG hook runs immediately after the target deletion transaction joins
and before the original owner's resume check. The existing immutable owner
validator rejects the retired continuation without undoing a valid committed
target tombstone. The caller commits inbound receipts/cursors only after deletion
returns successfully, so this failure leaves the page eligible for redelivery.

`SyncSplitOperationOwnershipTests.testInboundDeletionRetainsCommittedTombstoneAfterOwnerRetirementAndFreshRetry`
authors a real acknowledged row and cancels/resets the adapter after its target
tombstone commits. It requires the tombstone to remain durable while tracking
state and encoded server bytes remain unchanged. Clearing the hook and starting
a fresh operation must converge tracking to `deletedRemotely`. A later explicit
local Unmark followed by server-deletion replay must keep the live target and
exact journal/tracking generation in the `new` recreation lane, with encoded
server bytes cleared. Inbound deletion must not manufacture a local edit.

Normal ChangeRequestProcessor cancellation already cancels and joins its child
Task. The direct adapter hook specifically exercises the immutable operation
owner and split target/tracking recovery boundary; it does not claim that an
ordinary cancelled processor Task becomes active when the Boolean resets.

The new native method and DEBUG hook were independently source reviewed; there
is no Swift compiler or Apple runtime in this authoring workspace. The method is
authored, not compiled, discovered or executed evidence. The macOS component
workflow must inspect exact source/dependency identity and require its execution.
Current main pins RealmSwiftGaps `fb1af9b1375463d79997c42ac306dc27a6e623b3`;
Reader selects a different Gaps revision, so component success does not qualify
assembled Reader or its signed CloudKit release gate.

This supplement adds no production state transition, queue, clock or lock.
It preserves #140's complete production fence and pretarget coverage. The
adjacent cross-actor `saveChanges(in:forceSave:)` path remains separate review work.
