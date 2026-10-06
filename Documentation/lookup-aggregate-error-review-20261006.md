# Lookup evidence and aggregate error constraints — October 6, 2026

## Source and decision

This increment extends BigSyncKit #124 at
`709000c720bfe4b3ada5e51171ef69b0ac90f9b8`, tree
`73555bee57850a1bb2fce5b126c7b29995ca8b3e`.
It preserves the preceding response-identity and immediate-repair guards.
Two existing shipping files change. There is no new retry scheduler, queue,
Realm schema, journal, account authority, public protocol or persistent state.
The 76 prior test declarations and bodies are retained unchanged.

## 1. Lookup evidence must survive account revalidation failure

Acceptance lookup already rejected wrong record identity before importing an
observation. However, it collected only returned transport failures before the
account-routed await. A malformed or missing result was discovered afterward;
when account revalidation itself failed, those already-returned defects were
absent from the composed error.

Reuse `validatedMutationResults` to form the failure evidence before that await.
The raw lookup loop remains intact: malformed identity still fails fast, whereas
a missing result preserves the existing policy of importing valid observations
before reporting the missing slot. Valid lookup with an isolated account failure
still returns that original isolated error, without inventing a partial failure.

Six added methods cover wrong name/zone/owner/type/batch-member identity,
missing slots, a simultaneous sibling deadline, authentication fail-fast,
valid-observation handling for a missing slot, and valid lookup/account failure.
The expanded 82-case predecessor failed three methods; the lookup fix passed 82.

## 2. Both error graph walks must recognize Foundation aggregate causes

`CloudKitRetryConstraints` traversed `NSUnderlyingErrorKey` and CloudKit partial
failures, but omitted `NSMultipleUnderlyingErrorsKey`. An ordinary conflict or
missing-record error could therefore hide an account stop, deadline, network
failure or token-recovery condition in a non-CloudKit aggregate wrapper. A
size-limit error with an aggregated local failure could incorrectly qualify as
a pure size failure and permit immediate splitting.

One private `cloudKitUnderlyingErrors(in:)` helper interprets both Foundation
keys. The constraint-discovery breadth-first walk and the independent size-only
proof use that same edge set. It reads each supplied NSError's userInfo in one
helper invocation, including custom subclass overrides. The existing NSError
identity retention, DAG memoization, cycle rejection and 32-level bounds remain.
No arbitrary-depth completeness or new graph-size bound is claimed.

The sixteen added aggregate methods exercise all four repair routes, singular
plus plural causes, preserved original error identity, maximum retry floor,
local failures, unconstrained SDK details, empty arrays, cyclic and shared error
graphs, repeated aliases, and the existing depth/shallowest-path rules.

Apple documents that the underlying-error list combines both keys:
- https://developer.apple.com/documentation/foundation/cocoaerror/underlyingerrors
- https://developer.apple.com/documentation/foundation/nserror/underlyingerrors

These are constructed response/error histories, not observed CloudKit incidents
or attribution of a production data-loss or historical signed-sync failure.

## Executed final verification

Swift 6.2.1, Linux x86_64, Swift 6 language mode, warnings as errors. The complete
actual response-drain and retry-classifier files compile with explicit CloudKit,
Realm type, protocol and synchronizer-lifecycle collaborators. The identical
repository XCTest file executes in the portable package.

| Exact selection | Passed | Failed |
| --- | ---: | ---: |
| Original #124 source and classifier, final tests | 82 | 16 |
| Final Debug | 98 | 0 |
| Final optimized | 98 | 0 |
| Restore original lookup pre-await evidence | 95 | 3 |
| Restore complete original classifier | 85 | 13 |
| Restore only singular edges in size-only proof | 95 | 3 |
| Restore only singular edges in constraint discovery | 86 | 12 |

There are **98 unique methods = 76 retained + 22 added**, not 196 distinct tests
across two configurations. Every completed final matrix checks exact method
starts/completions and separate SwiftPM discovery, zero skips/duplicates/missing
methods, process statuses, compiler input paths and stable source/log hashes.
Every invocation uses new compiler scratch; no incremental artifact is reused.
Both production files and the native test declarations also pass frontend syntax
parsing, which is not SDK typechecking.

The two isolated edge controls were initially configured with incorrect expected
failure counts (4 and 11). Both completed and reported actual counts 3 and 12;
the drivers correctly rejected the mismatches. Fresh runs with the reviewed
counts completed successfully. Original logs remain separate from these corrected
control receipts. Two earlier optimized wrappers ended before a completed test
receipt; they are retained as incomplete, not passed. The later independent
optimized run completed all 98 cases with status zero. Earlier 76/82/94-case
stages retain their own source selections rather than being relabeled final.

## Foundation / SDK boundary

Linux Swift Foundation in this runtime does not expose the aggregate key or
`NSError.underlyingErrors` property. The portable CloudKit collaborator therefore
supplies a Linux-only named key and a small property proxy; the latter supports
one Foundation-list smoke assertion. Neither is included in native or shipping
targets. The final production parser itself is compiled unchanged, reading the
two userInfo keys; the proxy does not replace either production graph walk.

This tests our selection/traversal/dispatch logic, not Darwin Foundation's native
key/property implementation or NSError-to-CKError bridging. The existing native
imports use Apple's real types and constant. No Apple SDK typecheck, Xcode
runtime discovery, native CloudKit/Realm behavior, disk durability, complete
outer synchronization lifecycle, real networking, assembled Reader, UI, signed,
performance or application Release result is claimed.

## Publication and owning integration

Complete shipping postimages:
- response drain: `70e0540b4ccd9c4360b3728a31900ba5fcc43580`
- retry classifier: `41d169332baa643ba8148de95fcc421da606030b`
- test file: `a7ee53d2f68b6e1d1f260946cd8eeb1dd3f28b80`

Reader #286 must retain the preceding 76 method requirements and register the
22 additions in both inventories when deliberately selecting this successor.
The test source/owner remains `SyncMutationResponseIdentityTests.swift` /
`SyncMutationResponseIdentityTests`; no new native source file is introduced.
Run with existing receipt/conflict, retry/backoff, account, retained-quarantine,
disappearance and Unmark coverage on the owning Apple executor.

The wider inspection did not justify replacing the current sync/Unmark
architecture. The outer failure method was source-reviewed, not modified or
claimed executed here. No root pin, running acceptance checkout, generated
project, target merge, workflow dispatch, production data, deployment or release
gate changes are part of this increment. Keep the component PR draft.
