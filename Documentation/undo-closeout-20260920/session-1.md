# MR-UNDO-CLOSEOUT-20260920 · W1 · Native qualification checkpoint

Product base: `4baa7a4903c9f9372903fedf36a95afcb49ced71` (`main`). This commit extends `209f161ce3bf3426cbe51080d08934ef8f02a7c5` with 27 behavioral adapter/journal/restart/transport tests and fixes to the first native compile findings. It remains a temporary qualification checkpoint, not an integration-ready production source head: `qualification/RealmSwiftAdapter.patch` is still applied by read-only macOS CI to exact input blob `d3ace2cd74f4068f18122c580b6fa09cde072d33`, producing blob `d10af4388f8e2990e8d631e3102c10f883514bbf`. The final source commit must replace the actual adapter file and remove this temporary patch phase. No CI write credentials are used.

## Actual results

Baseline run `35491425445` (`a7b6401de5310d778e0b30bbaced0dd77d1bf239`): 619 XCTest tests, one skipped, zero failures.

Counterexample run `35491847410` (`eb4f9fe74e3ce08cab744208cbfc753a706d4ec0`): 622 tests, one skipped; six failed assertions in the three intended behavioral tests: `testOmittedScalarsApplyDeclaredDefaultsAndAgreeWithBaseline`, `testRetainedClearIsAcceptedByTheTerminalAudit`, `testTerminalLocalDeleteRetiresItsSupersededStagedSave`. Build and discovery succeeded. These are source counterexamples, not claimed device incidents.

First repair run `35495834546` (`209f161ce3bf3426cbe51080d08934ef8f02a7c5`) successfully applied and hash-verified the candidate, then failed native compilation at three Realm Map `isEmpty` uses and one missing explicit `self` capture. It ran no tests; this is not behavioral RED. All four compile findings are fixed in this checkpoint. Linux Swift frontend parsing of all five W1 test files passed; it is supplementary, not native qualification.

## Production interfaces under qualification

`ModelAdapter.requeueMissingServerRecords(_:matchingPreparedUploads:)` and `ModelAdapter.didDelete(recordIDs:matchingPreparedDeletions:)` carry the actual preparation-time comparison/submission evidence. W4 must forward these from Core's CloudKitE2ETransport without reconstructing the prepared structs from public fields. The existing generic adapter defaults remain compatible; Realm's adopted contracts reject generation-only shortcuts.

`BigSyncIncomingRepresentationPolicy` / `BigSyncIncomingFieldOmission` / `BigSyncIncomingDefaultValue` / `BigSyncRecordContract.incomingRepresentation` define omission semantics in the deliberate comparison signature (version 2). A normalized decoded object supplies selected values; an actual managed-object fingerprint must match the plan before the Realm transaction commits. Baselines remain incoming server representations. Unrelated legacy omission behavior is unchanged.

Audit telemetry adds `comparisonEvidenceVersion` (1 for the new inspector, 0 when reading old artifacts), `unresolvedSubmissionCount`, `acceptedBaselineCount`, `invalidatedBaselineCount`, `resolvedPreservationReceiptCount`, `retainedTombstoneCount`. W4's signed gate must require version 1, zero unresolved submissions and `isClean`. No persisted Realm evidence fields were added: candidate acknowledgment uses the existing baseline revision field.

## Added behavior coverage

The 27 tests cover omission/default/empty collections with and without pending work; missing required input; actual-result rollback including preservation copies; retained Clear; remote disappearance; upload unknownItem before the deletion feed; staged candidates; queued V2; newer accepted baseline; stale/identical-value acknowledgments; disk-backed restart after target-first writes; uncertain first-save absence; repeated disappearance of a nil-based recreation; retained negative control; final binding/full-ID fences; terminal orphaned submission and inconsistent baseline detection; foreign-namespace preservation; old audit JSON; actual synchronizer missing-server and acceptance-lookup dispatch; prepared delete versus recreated V2; and deletion target-first crash recovery. Converged journeys check a second drain. Protocol store scripts are transport inputs, not mocks of the changed reconciliation logic or signed CloudKit evidence.

## Integration and scope

Common companion: https://github.com/ManabiIO/ManabiCommon/pull/88, initial policy head `459b03217228d3bb9294b4e82ca87e9ce599eb88`, based on `cd5fd21484e35a29c28e664ba5b020087ca194e4`. Its real-model regressions are being completed independently. W4 owns shared test membership, Core wrapper, root composition, released-input qualification and isolated signed macOS acceptance. No Reader gitlinks/Core files/other workers' branches were edited; no target merge or production CloudKit mutation occurred.
