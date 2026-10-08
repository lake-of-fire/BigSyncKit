# Missing-server retry ownership review — 8 October 2026

## Exact base and scope

This followup is based on BigSyncKit #131 at `ca4ce219675b46a1c4510b9e3582f510b3f63935`, tree `49c23aecd518a8a70a478d985b8cd4d16f0dc3c5`. The upstream revision adds operation ownership across upload acknowledgement, relationship cleanup and other split target/tracking phases. This review found the two missing-server requeue overloads outside that new protection.

Baseline source blobs are `4101b5353fd06e28cfe97e626e1b72000cad2fb3` for RealmSwiftAdapter, `fa492997ec69ea4d0af42f64b7b299a63bced063` for BigSyncRecordDisappearance, and `be342e11615a83e68c590eab5069543de4bb4a30` for SyncSplitOperationOwnershipTests.

## Concrete failure

A missing-server response may wait for an owned tracking write. Cancellation can retire the sync attempt, after which preparation for a fresh run resets the Boolean cancellation flag. Checking only that flag at write admission lets the old continuation proceed. Account or transport replacement can similarly change the meaning of the response while it is waiting.

For a matching legacy generation, the stale response could turn a changed tracking record into a new record and discard its encoded server representation. That incorrectly applies an earlier response to current upload tracking. The prepared-envelope overload also crosses proof-backed target and tracking awaits before handing the legacy tail to the second overload.

## Repair

Use the existing operationOwnerValidator for both overloads. Capture the invocation's cancellation generation, provider identity, account, binding, rebase context, container and database scope before any suspension. Revalidate across the proof-backed phase and before and after the legacy handoff. In the legacy overload, revalidate before queued tracking work, inside its ownership-preserving transaction and before publishing the resulting change state.

The existing matching-generation, account eligibility, record-contract and journal rules remain the mutation authority. A fresh retry still resets only the intended tracking representation. No new durable queue, generation scheme, locks, account operation or CloudKit mutation is introduced.

## Two authored native requirements

The existing SyncSplitOperationOwnershipTests fixture now includes:

1. `testMissingServerRetryRejectsCancellationResetBeforeTrackingAdmission`
2. `testMissingServerRetryRejectsAccountAndTransportReplacementBeforeTrackingAdmission`

The helper prepares and acknowledges an initial upload, edits real target metadata, forwards its journal and obtains a changed legacy upload with an encoded record. The tests suspend the actual retry before tracking admission, retire its owner, and assert unchanged tracking state, encoded bytes, pending generation and target journal generation. The cancellation case exercises both overloads and includes a fresh retry positive control that reaches new state and clears the encoded representation without replacing the generation.

The fixtures cover the internal tracking suspension. They do not claim a mixed proof-backed/legacy transport execution or live CloudKit execution.

## Review and validation scope

The complete overloads, owner validator and fixture API/state transitions were independently source-reviewed. Baseline and candidate Git blobs, source delta and added whitespace were checked. No source-text assertions were added as runtime tests.

No Swift/Xcode compiler or Apple native runtime was available here. These two methods remain authored requirements awaiting compilation, enabled discovery and execution. Upstream source-boundary CI on ca4ce219 is historical evidence for that exact revision; it does not qualify these changed native paths. Assembled Reader and release acceptance remain pending.
