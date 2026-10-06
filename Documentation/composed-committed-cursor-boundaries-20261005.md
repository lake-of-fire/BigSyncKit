# Composed committed read and cursor ownership repair — October 5, 2026

The selected composition preserves #96 at `0b19ed598a7503235efcde5d674b9620e1d0f978`, merges #103 `b4a79b8cb17a12ee43ce8112d8ba71efb31c834e`, #101 `096720b366eec14b8bce30d06702230b69c65425`, #105 `3640865dfeb5a8e32a054170977bf5a29d390125`, and the normal cursor source commit `f8b20a35d66055dbe4e2cab4d5bca8ab94e306d2`. Temporary calculation #102 is absent. #103's shared committed Realm helper owns overlapping inventory, journal-page and final tracking-admission reads. #101's observed-ID collector also uses this helper; #103 alone omitted that path. All 19 native journal/terminal methods and all 13 cursor methods from those components are retained.

## Final source refinements

The Realm cursor getter returns detached bytes from the shared committed tracking snapshot. Both `saveToken` and `commitInboundPage` now capture their preparation epoch from that same committed boundary, then retain their existing exact epoch/account/binding/cancellation checks inside independently admitted tracking writes. A provisional epoch that aborts cannot poison the preparation; a genuine committed successor epoch still rejects it. The final transaction intentionally reads live Realm state because this caller owns that write.

The cursor loader snapshots registered adapters, the admitting attempt, run and optional context before awaiting any getter, and publishes its staged dictionary once only after every original owner remains current. The database fetch explicitly passes its original attempt into both loader and zone fetch. Zone fetch snapshots its registered adapters at entry, retains its original context/run, and validates adapter identity before processing returned pages, after asynchronous processing, before committing a cursor, and before publishing its in-memory cursor. Deferred processor-error cleanup belongs to the same attempt, run, context and adapters. No new persisted authority, locks, journal format, or reset policy is added.

Five additional native methods use existing registered source owners:

- `SyncUndoCloseoutW1Tests/testObservedJournalDebtSurvivesProvisionalRemovalAndAbort`
- `SyncUndoCloseoutW1Tests/testCursorSettersUseCommittedEpochAndRevalidateAfterHeldOwnerSettles`
- `CloudKitSynchronizerAccountFencingTests/testSuccessfulHeldZonePageCannotReachReplacedAdapterOwner`
- `CloudKitSynchronizerAccountFencingTests/testZoneFetchRejectsOriginalCallerAttemptBeforeTransportRead`
- `CloudKitSynchronizerAccountFencingTests/testMultipleCursorReadsNeverPublishAnIntermediateMap`

The observed-ID history checks actual tracking before a later lifecycle sweep. The setter history owns a held actual-Realm transaction and settles it at a narrow DEBUG preparation/admission hook; it covers both setters and both abort/commit outcomes. The fetch history holds a real production transport call and rejects a successfully returned page after adapter replacement. Cursor getter tests already cover actual tracking update/removal/insertion followed by rollback or commit. Existing unique named W1 Realm models are reused with explicit fixture objectTypes; no private, aliased, or default-schema-dependent test model is introduced.

## Qualification boundary

No build, syntax parse, test, discovery or runtime lane was run for this composition/refinement. `git diff --check` is formatting evidence only. Earlier component controlled-lane results remain bound to their original source. The 37 newly selected native methods require root inventory union, regeneration/discovery, Apple/Realm execution and composed Reader qualification. Root registration and publication belong to the primary owner. Generated project dirt is preserved.

The #101 portable lane extractor starts at `pendingMutationSnapshots` and omits the now-shared `committedRealmReadSnapshot` helper. It cannot qualify the composed source unchanged. Its extraction boundary and explicit Realm collaborator need a separate task-owned harness update before any current portable result is claimed. Do not substitute an inline synthetic helper and call that actual-source qualification.

The separate inherited `rebasePendingDeletionMetadata` live-target evidence path remains outside this bounded cursor/journal composition; its own committed-read review and behavior qualification remain explicit follow-up work. No full adapter committed-read audit, signed CloudKit, application/UI, performance or release result is claimed.
