# Synchronization evidence decoding — October 5, 2026

## Current source, not a replay of old branches

Reviewed #96 at `9665f09462f7d5ef5bc0acc6163a29cf6d4c6e7f`, including the normally composed #101/#103 journal/terminal read work and #106 cursor/processor successor. Publication is parented by `3c15173ec8d5a3e63c030bfc87429307f3097efa`, whose sole later change selects the Apple compiler in the existing journal runner. Both affected original blobs are identical at these heads.

The audit DTO and terminal-publication decoder are the only production changes. The complete Realm audit enumeration and the asynchronous publication-restoration/DEBUG getter suffix remain byte-identical. No new journal, schema, coordinator, lock, account authority, cursor format or root gitlink is introduced.

## Reproduced defects

### A version-1 audit could invent zero comparison debt

`BigSyncSynchronizationAudit.init(from:)` used legacy `decodeIfPresent(... ) ?? 0` defaults even when the artifact explicitly claimed comparison evidence version 1. An incomplete/current artifact could omit or null its unresolved-submission and other comparison counters, then satisfy the documented `comparisonEvidenceVersion == 1 && isClean` check. An explicit null version also silently became legacy version 0. Separately, a positive pending-mutation/relationship count did not affect `isClean` unless accompanied by a matching string in `issues`.

The decoder now reserves omission defaults for genuinely absent legacy fields in version 0, requires every comparison counter in version 1, rejects unsupported/malformed versions and negative counts, and makes explicit pending counts decisive independently of `issues`. Valid v0 decoding/roundtrip remains supported and remains version 0: it still cannot qualify a v1 gate. Retained tombstones, accepted/invalidated baselines and resolved preservation receipts are not newly classified as debt. Unknown-server-record policy is unchanged.

### Terminal-publication metadata could change meaning while decoding

The old private reader used `NSNumber.intValue` for version and feed epoch. Boolean and fractional values could therefore become a supported version or matching epoch. A present non-string replica binding silently became nil, indistinguishable from an intentionally unbound record. Non-finite Date values were accepted as publication timestamps.

The existing parser now lives in an internal value initializer on `BigSyncDurablePublicationEvidence`, directly called by the durable reader. It accepts only representable nonnegative integer-shaped numbers, excludes CFBoolean and real values, distinguishes absent binding from malformed present binding, and rejects non-finite dates. The writer also rejects a non-finite timestamp before persistence. Writer and reader share the existing version constant; valid version-1 representation and storage key do not change. Opaque strings are preserved without normalization. Finite old/future dates do not acquire a new expiry policy. Extra fields remain permitted within the known version.

These are malformed-evidence and false-clean-report defects, not proof of observed production data loss, the historical staleAuthority failure, or a bypass of every surrounding account/receipt fence.

## Executed behavior, including second-pass controls

Linux x86_64; Swift 6.2.1; Swift 6 language mode; warnings as errors. The same two complete XCTest files execute against the actual complete Foundation value declarations. No Realm/model/CloudKit substitutes are used in the fixed codec lane. The original publication control compiles its complete old private decoder verbatim, with only a raw-value store/access bridge. The existing durable-store error enum is copied unchanged; no storage implementation is exercised.

| Source / independent control | Passed methods | Failed methods |
| --- | ---: | ---: |
| Original audit DTO | 8 | 8 |
| Original publication decoder | 15 | 5 |
| Fixed combined Debug | 36 | 0 |
| Fixed combined optimized | 36 | 0 |
| Restore audit's old clean predicate only | 15 | 1 |
| Restore audit's missing-v1-counter default only | 14 | 2 |
| Restore publication's lossy numeric conversion only | 17 | 3 |
| Restore publication's optional binding cast only | 18 | 2 |
| Remove publication's finite-Date check only | 19 | 1 |

There are **36 unique XCTest methods: 16 audit + 20 publication**, not 72 unique cases. Method identities, process statuses, reviewed source blobs and log digests are in `sync-evidence-decoding-20261005.json`. The checked-in driver was executed from fresh retained directories for both fixed configurations and the original-source control (23 pass / 13 failed methods, runtime exit 1). An expected-negative driver exit of 0 is not a behavioral pass; its report preserves the runtime failure status and identities.

Coverage includes missing/null/mistyped fields; supported/unknown versions; explicit pending debt; negative and large integers; malformed binding; non-finite dates; opaque Unicode bytes; complete binary/XML property-list roundtrips, JSON audit roundtrips and nested decoding errors. Existing required fields and valid unbound records remain covered.

Initial generic decoding-error assertions incorrectly required a key path for Foundation's fractional-number parser rejection. Those fixture assertions were corrected without relaxing the required rejection; the separate nested missing-field test still requires its exact path. Both original and fixed sources were rerun afterward. An interrupted numeric-control build and an interrupted combined original-run setup were retained as incomplete; their fresh completed reruns supplied the outcomes above. Static-check command errors were corrected before recording the final source comparison. No incomplete/zero-method result is promoted to a pass.

## Reproduction

From a checkout containing this change:

```sh
python3 Tools/test-sync-evidence-codecs.py --evidence /tmp/sync-evidence-fixed-new
python3 Tools/test-sync-evidence-codecs.py --source-ref 3c15173ec8d5a3e63c030bfc87429307f3097efa --configuration debug --expect-failed-methods 13 --evidence /tmp/sync-evidence-original-new
```

Evidence paths must not exist. The driver reads local source only, selects complete declarations, copies the actual tests unchanged, records every completed method identity and rejects compiler failures, missing/duplicate executions and unexpected outcomes. macOS uses the selected `xcrun --sdk macosx swift` driver; that route is authored, not executed here. It retains all scratch/output files and does not fetch dependencies or access app data. Runtime assertions concern values/errors, not implementation text.

## Integration and qualification still required

### 2026-10-10 current verification correction

The inventory requirement below is historical. Current hotfix `AGENTS.md`
retires the manually maintained native inventories and receipt/log validators.
Register regression sources in the current target and test plan; check native
discovery and actual `.xcresult` execution on a future authorized run through
the maintained runner. Do not recreate inventories or copy Swift declarations
into temporary packages to qualify behavior. No native run was authorized for
the 2026-10-10 source audit, and historical execution does not qualify its edits.

New native source files / XCTest owners:

- `Tests/BigSyncKitTests/SyncAuditArtifactDecodingTests.swift` — 16 methods.
- `Tests/BigSyncKitTests/DurablePublicationEvidenceDecodingTests.swift` — 20 methods.

SwiftPM includes these automatically, but Reader's explicit Tuist source list and both required native inventories must register them when composing the child into Reader #286. File registration is not actual Xcode discovery. Compile the full Apple/BigSync/Reader graph and run all 36 methods there with the existing W1 representation and account/publication-restoration suites. Exercise the actual durable reader/writer, including invalid timestamp rejection without replacing existing evidence; validate real read-back and account/cursor mismatch behavior.

Complete changed Swift files pass syntax parsing, not Apple SDK typechecking. Foundation-only execution does not qualify live Realm enumeration, storage durability, SwiftUI/WebKit, current signed CloudKit, authentic installed Reader 3.11/build327 migration, genuine second-account replacement, performance or application Release. The runtime writer's new timestamp guard and the full asynchronous restoration route have not received execution coverage here. Prior evidence retains its exact inputs. No target branch merged, production data touched, deployment performed or release gate changed.

## Complete-file integrity

Original audit blob `506da6d67d203006f996e97603b649bbd4580435` becomes `42ef1dda512b5ae0ce78238faa298bc1b68c2ba2` (41 added / 14 removed lines). Original publication blob `5b7316065eebeec920254c5fc77698047a21d847` becomes `6df674e03ec2b1b492ca8f5353cacc1bdf79e544` (74 added / 47 removed lines, primarily relocation of the existing decoder).

Complete originals were checked against their Git blob hashes before editing. Published full blobs are matched to the tested local bytes; no truncated file or temporary fragment ancestry is used. The new tests have blobs `7d2d7b3554d69b6031cd83fdcc37251f3930fed6` and `12e2f1d3009451eeccbe63d4abf2d20d95e75a36`.
