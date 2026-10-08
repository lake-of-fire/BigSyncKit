# Committed upload selection and missing-target ownership

Reviewed on 2026-10-07 against Reader candidate `ce8c573d6640d27d86c0b71b03a612f09b450cb9`, which selects BigSyncKit `c32de536bc981fc8c902306cee1a8733a7c8da40`. BigSyncKit main `71482fd3f5b81fcce240629aa976208f136809d3` has the identical source tree and includes that selected commit as a merge parent.

## Defect and scope

`preparedRecordsToUpload` selected live `SyncedEntity` rows and serialized live target objects. Models without a compiled field contract return directly from `prepareContractUpload`, so they have no later target ownership/fingerprint check. An independent provisional write on either shared reader handle could affect the selected generation or payload; provisional target absence could also reach the missing-target disposition. The synchronous serializer attempted a nested tracking write in that disposition.

This is a shared-handle BigSync boundary defect. Common's ordinary foreground Unmark writer uses `RealmBackgroundActor`, whereas this target reader uses `BigSyncBackgroundActor`; this review does not establish direct provisional visibility between those separate handles. The adapter itself has target-reader write paths and reentrant public preparation boundaries. Semantic whole-state records such as `ManualArticleReadSupportShard` intentionally do not use the compiled field-contract path, so that path must provide its own committed read boundary.

## Repair

- Select tracking identity, state, replica binding, and generation from a frozen committed snapshot.
- Use one committed target snapshot per materialized record for account eligibility, generation checks, blockers, serialization, and comparison proof.
- Preserve live defaults in helpers used inside existing owning transactions.
- Keep the synchronous serializer read-only with respect to tracking state.
- Queue missing-target recovery through `asyncWritePreservingOwnership`. After admission, resample committed target absence and validate the captured provider, cancellation generation, account, context, container, database scope, acknowledgement issuer, replica binding, and exact tracking type/state/generation before marking it deleted locally.
- Sample the target before final authority checks because Realm refresh can invoke synchronous notifications.

Retained tombstones continue to require their target and are never converted into this physical-absence repair.

## Regression coverage

Four native XCTest methods in `SyncUndoCloseoutW1UploadSnapshotTests.swift` exercise the real public preparation boundary and Realm state:

1. Target payload edits and target removal under an independent owner, each followed by commit or cancellation.
2. Tracking generation replacement and tracking-row removal under an independent owner, each followed by commit or cancellation.
3. Missing-target serialization while tracking is independently owned, followed by normal owned disposition.
4. Target reappearance while the disposition waits for tracking ownership, preserving the tracking row for a later upload retry.

The last test uses an actor-owned continuation signal and joins the preparation task; it has no elapsed-time or fixed-yield scheduling assumption. The fixture declares a unique Objective-C model name, excludes it from the default schema, and includes it in explicit object types.

## Qualification

Production and regression source were independently reviewed. The current Linux workspace has no Swift compiler, Realm runtime, Xcode, or xcsift. These new native tests have **not** been compiled, discovered, or executed in this review. Their Reader Tuist membership and inventories must accompany the dependency pin. Existing native evidence for the predecessor tuple must not be relabeled as a pass for this changed source.

No account data, CloudKit zone, release deployment, or protected branch was mutated by this repair.
