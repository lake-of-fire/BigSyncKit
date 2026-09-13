# RA-1 adapter encoding review closure

Parent: `8d3bae50e782e4d16221c9d5e63f1da764979e62` (RA-1 integration, including subscription #26).

This is a bounded port of the remaining adapter findings from BigSync #24, not a merge of its divergent main/hotfix lineage. The selected adapter preimage is `52c124131d2d9d5e66b035fb7c6d199ddebb160d`; the complete postimage is `e9b6accdc5f55b93fc408a70fbb13443232e12d1`. The only production changes are in the existing error boundary, `applyChange`, and `recordToUpload`.

## Fixed

- Reject object maps before an absent collection can install an unkeyed deferred relationship that later takes the to-one assignment path.
- Failed list/set casts and unsupported list/set element types throw `unsupportedUploadProperty`; they cannot silently omit authored values and return an acknowledgeable record.
- Unrepresentable to-one and collection relationships fail preparation instead of force-unwrapping primary keys or silently omitting elements. Relationship entity-type optionals are also checked, not force-unwrapped.
- Unsupported non-nil scalar values throw instead of becoming absent fields.

Explicit model skips and processing-delegate overrides retain their existing precedence. Derived backlinks remain skipped. Supported collection order/duplicates, UUID/URL encodings, scalar-map formats, empty-field clears, and soft-deleted collection-target filtering remain unchanged. The existing scalar-map failure already throws and is retained.

No integer transport, RA-1 semantic replacement, pending-generation acknowledgement, cursor, restore, account, subscription, locking, or physical-format algorithm was replaced. In particular, schema-driven integer 0/1 normalization and string-encoded owned revisions remain the selected RA-1 implementation.

## Coverage and qualification

`HotfixCollectionSafetyTests.swift` is the exact nine-method donor suite (blob `8342d4ea059c9761aa98138b303414a3d39bf75d`). It uses real Realm/adapter/journal boundaries for absent/present object-map rejection, rollback, nullable and unsupported collections, unsupported scalar/unkeyed relationships, supported collections/backlinks, and explicit empty-state transport. Existing RA1GenericAdapterParityTests and all prior package suites remain present.

The eight subscription tests selected in #26 remain unchanged. Both new suites must be selected exactly once in the root app-native test target as part of the root handoff.

Verification remains deferred: no Swift parse/typecheck/build, runtime tests, workflows, capacity workload, or CloudKit operation was executed for this continuation. Local Git text composition and full-file hash identity checked assembly only; they are not Swift/native qualification. The historical 682/0 package result belongs to parent `6ddbfc95...`, not this changed tree.

## Assembly provenance

Closed unmerged PR #27 was text-only file assembly. Its tentative merged adapter blob matched the independently composed complete postimage above. Only that blob was reused in this normal child of `8d3bae50...`; no assembly commits or main/hotfix ancestry are part of the implementation. `assembly-only/ra1-adapter-*` refs are not candidates.

## Remaining evidence, not new source design

Rerun the selected exact BigSync package, strict materialized root preflight, approved Tuist/Xcode native discovery/build and acceptance, v1 capacity, authentic released-Realm migration fixtures, and explicitly authorized signed macOS disposable multi-installation CloudKit. Preserve `native_qualified=false`, `cloudkit_qualified=false`, `release_authorized=false`, `production_enabled=false` until those independent gates are actually satisfied.
