# Inbound deletion retains its original operation owner

Date: 2026-10-08

## Problem and resulting behavior

`RealmSwiftAdapter.deleteRecords(with:)` could resume an earlier inbound deletion after cancellation was reset, or after the active account, replica binding, or transport namespace changed. Its Boolean cancellation check could become false again while the obsolete operation still held target/tracking work.

The method now captures the existing `operationOwnerValidator()` before its first suspension and revalidates that same owner after refresh/reconciliation, inside and after target/tracking transactions, and before returning dispositions. The validator includes the cancellation generation, Realm provider, account, binding, rebase context, container and database scope. It is the same operation boundary already used by other split target/tracking paths.

An operation which loses ownership before target admission cannot delete the target. An operation which loses ownership after a durable target phase cannot finish tracking cleanup or report an obsolete completed result. A fresh operation can retry the same server deletion. The change uses the existing Realm owned-write queue and does not add another queue, generation registry or timeout.

## Regression

`SyncSplitOperationOwnershipTests.testInboundDeletionRejectsCancellationResetAccountBindingAndTransportReplacement()` executes four replacement schedules at the existing pre-target-write boundary:

1. Cancellation followed by preparation that clears the cancellation Boolean.
2. Active account replacement.
3. Replica binding replacement.
4. Container/database-scope replacement.

Each schedule asserts that the live object and tracking entity remain unchanged and that no local mutation journal is manufactured. The cancellation schedule additionally retries with a fresh operation and verifies the inbound tombstone and tracking state.

## Evidence and limits

The patch was independently reviewed against selected Reader dependency `eaffec59709b2f7a6f6f1bf45fc8f6e49b381135`. That selected tree is the tree merged by BigSyncKit #137 at `ea7ccb1702015feab3479d7dc2e96d4e42010a1d`. This follow-up preserves the live main parent and changes only the inbound deletion implementation, its existing native regression file, and this report.

The current authoring environment has no Swift or Apple toolchain. The new native method is authored, not executed here. Hosted check outcomes must be attributed to their actual source and scope; syntax/ownership checks do not establish Realm runtime behavior. Reader's assembled native, device/account and release gates remain open.
