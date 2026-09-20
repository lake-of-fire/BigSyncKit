# MR-UNDO-CLOSEOUT-20260920 · W1 · Native qualification checkpoint

Base: `4baa7a4903c9f9372903fedf36a95afcb49ced71` (`main`). This source change follows `3f9c2abfbc6a78acaad68fc6d0d05764cfefad91`, which fixed the native deletion-restart crash by eagerly materializing Realm IDs before suspension. The current commit adds a live-object invalidated-evidence audit regression and preserves the production attempt fence when testing a stale deletion reply after adapter restart.

## Packaging status

The actual adapter edit is still in `qualification/RealmSwiftAdapter.patch` plus `qualification/native-corrections.patch`. Read-only macOS CI verifies original adapter blob `d3ace2cd74f4068f18122c580b6fa09cde072d33` and applied adapter blob `480c4135a76d9b54d51adecdf8f6f6abd363b92c`. The final production tree must contain the actual modified adapter, without this temporary apply step. This is therefore **not yet an integration-ready dependency**, even if the applied-source tests pass. No CI write credentials or private application source/data are used.

## Observed native results

- Baseline `a7b6401de5310d778e0b30bbaced0dd77d1bf239`, run `35491425445`: 619 XCTest tests, one skipped, zero failures.
- Counterexample `eb4f9fe74e3ce08cab744208cbfc753a706d4ec0`, run `35491847410`: 622 tests, one skipped; six failed assertions across `testOmittedScalarsApplyDeclaredDefaultsAndAgreeWithBaseline`, `testRetainedClearIsAcceptedByTheTerminalAudit`, and `testTerminalLocalDeleteRetiresItsSupersededStagedSave`. Build/discovery succeeded. No device incident is claimed.
- `6a103615c4df826ef481f0b0415b463f4ca7a986`, run `35519804746`: native backtrace resolved the restart crash through `RLMFastEnumerate` / `LazyMapSequence.Iterator.next` in `preparedRecordDeletions`; an apparent key snapshot was still lazy across a tracking write.
- `3f9c2abfbc6a78acaad68fc6d0d05764cfefad91`, run `35520354025`: full 646 tests, one skipped, one failure; focused 27 tests, one failure. The crash regression passes, including a separate one-test native rerun. The sole remaining failure is the fixture expecting no throw from a stale delete preparation after adapter restart; production correctly throws `CancellationError`. This commit accepts that rejection while still checking the recreated baseline, empty journal/submission debt and a quiet second drain.
- The additional `testInvalidatedFenceCannotCertifyUnjournaledLiveObject` checks actual target/tracking Realm audit, terminal and publication rejection, restart, and repair by a real server observation. Its native result is pending for this commit. Frontend parsing is supplementary, not native qualification.

## Actual interfaces

`ModelAdapter.requeueMissingServerRecords(_:matchingPreparedUploads:)` and `ModelAdapter.didDelete(recordIDs:matchingPreparedDeletions:)` carry actual preparation-time comparison/submission evidence. W4 must forward the original opaque preparations from Core's CloudKitE2ETransport, not reconstruct them from public fields or generation maps. Generic adapter defaults remain compatible; Realm's adopted contracts reject generation-only shortcuts.

`BigSyncIncomingRepresentationPolicy`, `BigSyncIncomingFieldOmission`, `BigSyncIncomingDefaultValue`, and `BigSyncRecordContract.incomingRepresentation` define omission semantics in comparison signature version 2. A normalized decoded object supplies selected values; the actual managed fingerprint must match the plan before the target Realm commits. Baselines remain incoming representations. Unrelated legacy omission behavior and source-shard predecessor semantics are unchanged.

Audit adds `comparisonEvidenceVersion` (new 1; old decoded artifact 0), `unresolvedSubmissionCount`, `acceptedBaselineCount`, `invalidatedBaselineCount`, `resolvedPreservationReceiptCount`, and `retainedTombstoneCount`. The signed gate must require version 1, zero unresolved submissions and `isClean`. Intentional fences on absent physical notes are not debt, but an unjournaled live object behind a fence cannot establish terminal success. No persisted Realm evidence fields were added.

## Ownership and dependencies

Common PR #88 head `b102905b1f459b0db7734ac31e0632020cdd76b4`, based on `cd5fd21484e35a29c28e664ba5b020087ca194e4`, already contains the Article/control/note policy companion and six real-model `ReaderIncomingRepresentationW1Tests`. Those have not been natively executed. W4 owns shared test membership, Core forwarding, root composition, released-input qualification and isolated signed macOS CloudKit acceptance.

Live Reader re-read at `6896027d165f087d70e19bccec9e7a487657d04c` with Core `cc82afda1e2f2678022e268a253f27dc1e92f09d`; preserve its newer timing-test fixes. BigSync/Common product pins remain unchanged. Public native dependencies are RealmSwiftGaps `2d4fa2bfd8b1c856b45aca2c7a97c301d37204e2` and SwiftUtilities `f437c7d06fc631cd7a67731279411c417cdf8077`.

No target branch, root gitlink or another worker's branch was changed. No production CloudKit data, release, signed acceptance or Common native pass is claimed.
