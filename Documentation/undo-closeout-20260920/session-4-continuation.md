# MR-UNDO-CLOSEOUT-20260920 — W4 native continuation

Target/base `4baa7a4903c9f9372903fedf36a95afcb49ced71`. Exact tested source/test/workflow head: `4bc3883093508b82eee0a81f47bec1ba89f3ad23`, tree `2eaa6bd45c2819425b84e6a933d34892bd09f970`. This note is a documentation-only descendant, not another claimed test run.

## Interfaces consumed by the application candidate

The injected binding-store overloads share the worker's real local store with domain identity writers. `requestFollowUpSynchronization(after:)` validates the current prepublication run and reuses the existing full-drain tail; it does not start a detached task or promote download-only work. Public `BigSyncLifetimeID.prefersIncoming(local:incoming:)` exposes the existing reconciliation order without a second parser or wall-clock rule.

New `BigSyncBackgroundActor.domainTransitionReadiness(after:)` returns `ready`, `pendingWork`, `blocked`, or `downloadOnly`. It reads the actual adapter semantic-publication and terminal-pending predicates, performs exact account/run validation across suspension, and rejects a worker replacement before returning. It is a read-only transport observation, not a success receipt or a replacement for a domain's final same-Realm writer fence. Core's post-inbound legacy CAS checks it again after taking that fence.

## Verified execution

GitHub Actions run `35527607608`, native job `106123274810`, checked at the exact tested head above, completed successfully on macOS.

- Native build and focused W4 discovery/execution passed.
- Full vendor package: **635 tests, zero failures**.
- The existing qualification lane's focused runs, expected negative controls, restored-source full run, repeated focused checks, and clean-source check passed.

`DomainPrepublicationFollowUpTests` now has nine tests. The four new cases are `testTransitionReadinessRechecksPendingWorkAndSemanticBlockers`, `testTransitionReadinessRejectsEarlierPassWithoutBlockingFreshWork`, `testTransitionReadinessRejectsWorkerReplacementDuringInspection`, and `testCancelledReadinessInspectionCannotAcquireCurrentPass`. Its download-only case also verifies that a configuration change cannot upgrade the active drain's authority. The existing five follow-up cases, four injected-store tests, and three public lifetime-ordering tests remain.

These are native generic BigSync tests with owned temporary storage and controlled transports. They do not execute private Common/Core code or signed CloudKit. No private application source, installed user Realm, or credentials were copied into public vendor CI.

## Integration limits

Consumers are Common #84, Core #134 and Reader root #163. The root manifest records the exact chosen commits. This W4 branch does not contain W1's unfinished reconciliation/audit repair; it cannot stand in for W1 #50. Preserve both changes through a normal candidate merge once W1 publishes its directly buildable final source. No target PR merge, production CloudKit operation or release was performed.
