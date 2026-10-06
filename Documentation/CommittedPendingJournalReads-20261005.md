# Committed journal reads — October 5, 2026

PR #101 is stacked on #96 at `0b19ed598a7503235efcde5d674b9620e1d0f978`. Production source commit: `6dc2b7eee02940818c4bbbe3ba6ad2a584a9f8c7`; tree `efa0887684c22e4fd46c4f69e6a98150ce23b0d3`. This source change preserves #98 disappearance and #99 audit work.

## Findings and repair

`pendingMutationInventory` observed live journal and target rows without owning a target transaction. A held independent write could hide committed debt, expose provisional fields/insertion, or change the reported physical-deletion disposition. Its DEBUG tracking-generation counterpart could similarly report an uncommitted acknowledgement or replacement.

Journal forwarding had the stronger production variant: acquiring the tracking transaction does not grant ownership of the target transaction. Its final target resample could copy an uncommitted generation/deletion into tracking or overlook a provisionally removed committed journal. The observed-ID collector could also drop such a committed identity before forwarding.

All these observations now use committed frozen Realm views. Journal fields and target disposition use the same captured version. Refresh occurs only when not already in a write; freezing occurs afterward regardless, because refresh can synchronously admit another owner. The final forwarding capture is made after tracking admission, not reused from discovery. Final account/binding/cancellation checks and generation comparison remain. Ignored-generation cleanup still uses its original owned target transaction and generation check.

These are per-Realm snapshots, not an atomic multi-file view or evidence that writers have stopped. No observed production data-loss incident is claimed. No schema, journal format, writer authority, lock, root gitlink, or production CloudKit change is introduced.

## Executed controlled evidence

Linux x86_64, Swift 6.2.1, Swift 6 language mode, warnings as errors. The lane compiles the complete inventory file and unchanged selected production snapshot/forwarding method bodies. Explicit collaborators replace Realm MVCC, model/transport state, the asynchronous admission queue and polymorphic target decoding. This is not the full adapter/package or native Realm qualification.

| Input | Runtime result | Exit |
| --- | --- | --- |
| Fixed source, Debug | 32 passed / 0 failed | 0 |
| Fixed source, optimized Release | 32 passed / 0 failed | 0 |
| Original source | 15 passed / 17 intended failures | 1 |
| Inventory target lookup reverted to live | 2 intended failures | 1 |
| Final forwarding target capture reverted to live | 5 intended failures | 1 |
| Observed-ID collector reverted to live | 2 intended failures | 1 |
| Production final cancellation check removed | 1 intended failure | 1 |

The same 32 unique cases run in each configuration. On second review, the admission collaborator's own cancellation check was removed so the actual forwarding guard must reject cancellation. Removing that production check then fails `forwarding_cancellation_keeps_committed_debt`; the fixed source passes again in both configurations. All final runs use fresh retained evidence directories.

Final positive runtime log SHA-256 (same ordered output in both configurations): `18e42a4a6dae031e505737d85ac658bf3943c34553c7591f269add2e231af0e0`.
Original-source runtime log: `99a1758a0c13fce3f80d173afb9f13247f0f2943a6cf1cb573ac40915118d600`.
Live inventory target control: `42b515fec6e2bcc0fb305d3fb2da39b4b42c00aa85a728b0038c4c0d1f118a94`.
Live final forwarding control: `86f76d27968405b534c1142da8ddbcf3ed02876b3e08791fd43c3a8e181d70c3`.
Live observed-ID control: `ff1359ef350ed55434738b1c97ea1998676aa4ed3cb4ac4f2911909bdebf4802`.
Removed cancellation guard: `5d254a29be35042ebe859fb19282d4b56dc83ad59d3a44cbfb3b854675f2a262`.

The published driver was executed using its explicit complete-method-range input mode and the complete inventory source. It retains exact case identities, input hashes, compiler/runtime statuses, failures and limitations. Repository mode reads those same bodies directly from a checkout or existing local Git ref:

```sh
python3 Tools/test-pending-journal-read-boundaries.py --evidence /tmp/pending-journal-fixed-new
python3 Tools/test-pending-journal-read-boundaries.py --source-ref 0b19ed598a7503235efcde5d674b9620e1d0f978 --configuration debug --expect-failures 17 --evidence /tmp/pending-journal-original-new
```

Evidence destinations must not exist. The runner does not fetch dependencies, access production data or remove prior evidence. The `--expect-failures` option is for negative controls, never native qualification.

## Native requirements — authored, not executed

Existing source: `Tests/BigSyncKitTests/SyncUndoCloseoutW1EvidenceFenceTests.swift`.
Existing XCTest owner: `SyncUndoCloseoutW1Tests`.

The 13 new methods use actual file-backed W1 fixtures and actual adapter APIs. Forwarding assertions run immediately after tracking publication, before a later drain can hide an incorrect intermediate result. Held writes and test hooks are released on exceptional paths.

- `testPendingInventoryDoesNotExposeProvisionalJournalInsertion`
- `testPendingInventoryKeepsCommittedDebtDuringProvisionalRemoval`
- `testPendingInventoryKeepsCommittedFieldsDuringProvisionalEdit`
- `testPendingInventoryDoesNotBorrowProvisionalTargetTombstone`
- `testPendingInventoryRetainsCommittedTombstoneDuringProvisionalResurrection`
- `testPendingInventoryResamplesAfterIndependentCommit`
- `testEmptyPendingInventorySelectionsLeaveHeldOwnersUntouched`
- `testTrackingGenerationProofIgnoresProvisionalRemoval`
- `testTrackingGenerationProofIgnoresProvisionalReplacementThenSeesCommit`
- `testJournalForwardingDoesNotPublishProvisionalGeneration`
- `testJournalForwardingKeepsDebtBehindProvisionalJournalRemoval`
- `testJournalForwardingDoesNotPublishProvisionalDeletionDisposition`
- `testJournalForwardingPreservesCommittedDeletionDuringProvisionalResurrection`

Add these exact methods to both Reader #286 required inventories when composing this child. Existing file inclusion is not proof of native method discovery. Native SDK compilation/discovery/execution, actual Realm notification/admission behavior, signed CloudKit and assembled app qualification remain unexecuted here. No previous pass is transferred to this tuple.

## Complete-file publication

Only two production files change: 27 additions / 18 deletions. The native file adds 268 lines, removing no prior source. Full adapter blob: `ee8c27eddb95f27661b4b109d4c241987556a6f5`; inventory blob: `095740eea9d789a52122f458e651db27bd6f20a9`; native file blob: `c1965ad28591bad400768a34dd146a5b69729c9f`.

Temporary calculation #100 is closed unmerged. Its complete result was compared against #96 and copied into a normal single-parent feature commit; no fragment or synthetic ancestry is selected. Keep #101 draft until the owning integration batch. No target branch merged or release authorized.
