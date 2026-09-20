# MR-UNDO-CLOSEOUT-20260920 — W1

Status: **W1 BigSync source complete and native-qualified; Common companion source complete with composed native qualification owned by W4.** No target branch is merged by this work.

## Exact revisions

- BigSyncKit target/base: `4baa7a4903c9f9372903fedf36a95afcb49ced71` (`main`).
- BigSyncKit directly buildable source/test head: `3120e2caf68ffef4f3b16302046d706fc7172603`.
- ManabiCommon target/base: `cd5fd21484e35a29c28e664ba5b020087ca194e4` (`v3-hotfix`).
- ManabiCommon W1 source/test head: `4588444426b13b7f7850eebfd40c87285aa700d1` on PR #88.
- Rechecked Reader root: `6896027d165f087d70e19bccec9e7a487657d04c`; it still pins BigSyncKit `4baa7a4903c9f9372903fedf36a95afcb49ced71` and Common `cd5fd21484e35a29c28e664ba5b020087ca194e4`. Core advanced compatibly to `cc82afda1e2f2678022e268a253f27dc1e92f09d`; W1 did not edit root/Core gitlinks.

The completion-note commit is documentation-only after the source/test heads above.

## Implemented BigSync lifecycle

Physical disappearance now has one durable target disposition shared by admitted inbound deletion, prepared missing-server repair, prepared deletion acknowledgment, restart recovery and tracking publication.

- A target transaction invalidates only the still-applicable accepted comparison and retires exactly the captured staged candidate.
- The invalidated baseline retains a nonempty revision fence even when no accepted server representation remains, so old nil-based receipts cannot acquire a recreated lifetime.
- V1 disappearance can repair its still-current accepted ancestor while a newer V2 journal/value remains pending; a newer accepted baseline or namespace rejects the stale failure.
- Tracking publication re-reads the target fence and live journal after its queued tracking write begins. A target-first crash therefore resumes the durable disposition instead of trusting stale encoded tracking system fields.
- Local deletion superseding a staged save retires the old submission. Late save/delete acknowledgments cannot consume a recreated lifetime's mutation or install its baseline.
- An uncertain first-save `unknownItem` remains uncertainty: its exact archived conditional candidate is preserved.
- Retained Article/control records never enter physical note recreation. Their clears remain versioned saves.

No second outbox, field clocks, timestamp re-authoring, universal submission purge or retained-record hard deletion was introduced. Existing record-level journaling and generation-matched transport remain authoritative.

## Incoming representation contract

Published APIs:

- `BigSyncIncomingRepresentationPolicy`
- `BigSyncIncomingFieldOmission`
- `BigSyncIncomingDefaultValue`
- `BigSyncRecordContract.incomingRepresentation`

Contract signature version 2 includes policy identity/version/default meaning. Adopted records validate required field presence before merge, decode one normalized representation, apply selected values from that representation, and fingerprint the actual managed object in the same Realm transaction before committing accepted evidence or journal/preservation changes. A mismatch rolls the transaction back.

Legacy models that have not adopted `BigSyncRecordContractProviding` retain their existing omission behavior.

Common PR #88 declares the actual Reader policies: released aggregate Article scalar defaults and nil legacy epoch, complete current control identity/integrity, and complete note text with optional context clearing. Its eight real-model regressions cover successor-lifetime omission with nonempty counters/sets/maps, pending title/image independence, explicit defaults, legacy nil epoch, required control fields, all four note types, managed-result rollback, contract boundaries, retained Article/control Clear across restart, and rejection of physical disappearance.

## Synchronization audit

`BigSyncSynchronizationAudit` is lifecycle/evidence-aware. Retained clears are validated as saved server representations rather than physical tombstone debt. Active-namespace submissions and accepted/invalidated comparison evidence are inspected in addition to journals/tracking/relationships/quarantine.

Additive telemetry for W4:

- `comparisonEvidenceVersion` — current evidence-aware artifact is `1`; older decoded artifacts report `0`.
- `unresolvedSubmissionCount`
- `acceptedBaselineCount`
- `invalidatedBaselineCount`
- `resolvedPreservationReceiptCount`
- `retainedTombstoneCount`

For release acceptance, W4 should require `comparisonEvidenceVersion == 1`, `unresolvedSubmissionCount == 0`, and `isClean`. An intentional invalidated physical-note revision fence and resolved preservation receipt are classified as durable evidence, not automatically as debt.

## Transport handoff to W4

The prepared-evidence overloads are the integration boundary:

- `ModelAdapter.requeueMissingServerRecords(_:matchingPreparedUploads:)`
- `ModelAdapter.didDelete(recordIDs:matchingPreparedDeletions:)`

Core's `CloudKitE2ETransport` must forward the actual prepared uploads/deletions through these overloads; it must not reconstruct their evidence from public record names or generation strings. Generic/legacy adapters retain compatible generation-only defaults.

W2's generic mutable-predecessor extension point and exact pending-generation protection are unchanged.

## Native qualification

GitHub Actions run `35530038608` qualified the **directly committed source**, not an applied patch:

- commit: `3120e2caf68ffef4f3b16302046d706fc7172603`
- source tree: `af5bfe9a13c7d44e299da4b4d5b9bfdb1413b898`
- `RealmSwiftAdapter.swift` blob: `480c4135a76d9b54d51adecdf8f6f6abd363b92c`
- disappearance tests blob: `72bbc542d32e08b345626f041002fd327338c1bd`
- representation tests blob: `6e43979542b414c200fdedf65d281e9f3aac1ea5`
- full XCTest: **647 executed, 1 skipped, 0 failures**
- focused `SyncUndoCloseoutW1`: **28 executed, 0 failures**
- macOS 15.7.9, Xcode 16.4 (16F6), Apple Swift 6.1.2
- RealmSwiftGaps `2d4fa2bfd8b1c856b45aca2c7a97c301d37204e2`
- SwiftUtilities `f437c7d06fc631cd7a67731279411c417cdf8077`

The focused suites cover remote deletion before upload; missing-server-before-feed equivalence; staged candidate retirement; delayed V1 with V2; stale failure after newer baseline; late acknowledgments after recreation; target/tracking crash recovery; local delete superseding staged save; uncertain first-save absence; retained negative control; full record/binding fences; normalized omissions/defaults/collections; required fields; actual-result rollback; namespace scoping; submission/evidence audit; and actual transport forwarding.

Common has no standalone compatible native runner in this branch: its Swift package intentionally resolves BigSyncKit through the sibling local path, and W4 owns the final package/root composition. The two additional retained-model regressions were Swift-frontend parsed locally; their native discovery/execution remains an explicit W4 integration gate. No private Reader/Common source was copied into BigSyncKit's public vendor CI.

## Remaining external/integration qualification

W4 still owns:

- selecting BigSyncKit W1 and Common W1 real heads together with W2/W3/W4 heads;
- Core `CloudKitE2ETransport` prepared-evidence forwarding and audit telemetry exposure;
- root/Tuist/test membership and actual Common/Core discovery;
- genuine supported released-data upgrade qualification;
- isolated signed macOS CloudKit/two-client release evidence.

No simulator result, synthetic source fixture, personal production Realm or unsigned vendor run is represented as signed release evidence.
