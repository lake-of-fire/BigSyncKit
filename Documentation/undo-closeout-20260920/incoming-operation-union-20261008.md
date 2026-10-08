# Incoming operation ownership union — 2026-10-08

## Provenance and resulting behavior

The candidate preserves BigSyncKit main `ea7ccb1702015feab3479d7dc2e96d4e42010a1d` and these immutable donor heads:

| Donor | Head | Retained changes |
| --- | --- | --- |
| #138 | `6a93c3dffe6ef211193044d7ce255f910f5bbb5c` | Select retained-deletion cleanup evidence after supported model/identity callbacks. |
| #139 | `a82a05ebbee8d8c23b5590ec8a8ab00ea612e997` | Fence journal forwarding, import completion and queued remaining-count delivery; repair W1 qualification roster/dependency inputs. |
| #140 | `a90a559af3e148255864e81dd3e2bf4f247d5cd2` | Retain the original owner throughout inbound deletion. |

All three donor heads were rechecked unchanged before handoff. Fourteen canonical/donor input files match their expected Git blob hashes. Three-way merging produced one signature conflict, resolved by retaining the Sendable owner closure and default-strict provider policy. The integration commit should preserve the actual donor parents.

The unique fix fences `saveChanges(in:forceSave:)` and `validateAuthoritativeOwnUploadRecords(_:)`. Previously an older suspended operation could continue after cancellation advanced its generation and a successor cleared the cancellation Boolean, or after account/transport replacement. A caller's later attempt check could not undo target or quarantine publication.

Incoming saves now retain the entry owner through target/tracking admission, transaction commit, suspension, shared fetched-marker cleanup, and result publication. The target save uses the existing target Realm handle on BigSyncBackgroundActor so admission can synchronously revalidate the same owner. Reader/writer handles use identical target configurations and object types. Only the incoming callsite invokes the relocated `applyChanges` and `applyRecordRebase` helpers; their synchronous decoder, metadata/journal, comparison and semantic paths remain intact. Reader supplies no custom conflict/property-processing delegates.

A revoked operation cannot begin target mutation. A previously committed target phase remains durable if tracking is revoked; fresh redelivery finishes the missing phase without inventing an accepted ancestor or user mutation. Relationship requests remain Sendable IDs/scalars and tracking publication remains inside its owned transaction.

A private lifecycle-validator factory captures the existing cancellation generation, account, binding, rebase context, container and database scope. The owner factory adds provider identity, strict by default, with a narrowly explicit nil-provider initialization option. Own-echo validation retains its entry lifecycle across setup and then a strict configured provider. Donor import completion retains its scalar-only pre-setup policy because retry may replace an interrupted provider, then captures the ready provider. No new state, clocks, locks, leases or queues were added.

## Twelve authored native selectors

Seven donor methods and five new methods are retained:

| Origin | Selector |
| --- | --- |
| #138 | `SyncRetainedRecordContractTests/testRetainedCleanupIdentityCallbackPreservesSuccessorJournalAndPageEvidence()` |
| #139 | `SyncSplitOperationOwnershipTests/testCancelledJournalForwardingCannotPublishToSuccessorTracking()` |
| #139 | `SyncSplitOperationOwnershipTests/testImportProgressCannotReacquireSuccessorJournalOwnership()` |
| #139 | `SyncSplitOperationOwnershipTests/testCancelledImportCannotClearSuccessorAssetsAfterProgressCallout()` |
| #139 | `SyncSplitOperationOwnershipTests/testCancelledQueuedRemainingCountDoesNotNotifySuccessor()` |
| #139 | `SyncSplitOperationOwnershipTests/testJournalForwardingRejectsTransportReplacementBeforeTrackingAdmission()` |
| #140 | `SyncSplitOperationOwnershipTests/testInboundDeletionRejectsCancellationResetAccountBindingAndTransportReplacement()` |
| New | `SyncSplitOperationOwnershipTests/testCancelledIncomingImportCannotApplyTargetAfterSuccessorResumes()` |
| New | `SyncSplitOperationOwnershipTests/testCancelledIncomingImportRetainsTargetCommitWithoutPublishingTracking()` |
| New | `SyncSplitOperationOwnershipTests/testIncomingImportRejectsAccountAndTransportReplacementBeforeTargetAdmission()` |
| New | `SyncSplitOperationOwnershipTests/testAuthoritativeOwnEchoAllowsInitialProviderSetup()` |
| New | `SyncSplitOperationOwnershipTests/testCancelledAuthoritativeOwnEchoCannotPublishSuccessorQuarantine()` |

These real-Realm tests cover generation ABA, account/transport replacement, durable target-first recovery, own-echo quarantine and initial provider setup. Fixtures use explicit schemas, unique Objective-C names and default-schema exclusions. Authored names must still be resolved against a retained live Xcode test catalog before assembled Reader execution.

## Qualification and deferred scope

The existing Python verifier behavior suite passes **42 tests**, status zero. Source whitespace/input-hash audits passed. The updated Reader-main workflow parsed as YAML and all five Bash run blocks passed syntax checks. These results are tooling/source evidence.

The W1 verifier explicitly requires all twelve additions, preserves full-suite accounting, and adds only the exact retained-cleanup case to the existing focused roster. The existing opt-in mutation-identity performance benchmark exclusion remains explicit; additional arbitrary skips are rejected.

Active W1 and Reader-main jobs select coherent RealmSwiftGaps `1ffbedbb3d8dd90f44f651f618128e7806ce39dd` and SwiftUtilities `f437c7d06fc631cd7a67731279411c417cdf8077` inputs. Reader-main retains its package job identity, uses fresh scratch/prebuilt safeguards, xcsift, immediate pipeline statuses, native discovery/full transcript validation, and retained source/dependency/log artifacts.

PR-triggered Debug package qualification may run. Release requires explicit workflow_dispatch `run_release: true`; its default is false and ordinary PR evidence records `release-gate=false`. No Apple job was manually dispatched. Mac UI, performance, signed macOS CloudKit and Release remain owner-deferred.

There is no Swift/Apple toolchain in this authoring environment. All twelve native methods are **authored, unexecuted here**. Native compilation/Realm behavior, assembled Reader, signed macOS CloudKit and release qualification remain open. The execution-actor change requires coherent native verification before release; external package clients should review undocumented custom delegate executor assumptions.
