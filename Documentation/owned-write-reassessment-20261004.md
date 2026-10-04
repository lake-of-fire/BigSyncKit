# Owned-write architecture reassessment — October 4, 2026

## Decision: converge the current stack, not another sync rewrite

This review starts from BigSyncKit #96 at `062827415213076b10bdc2cd8595a2834df56c68`, with RealmSwiftGaps #9 at `cac6eb96fcd6a645d8d1329079a72f7bab41f929`. #96 routes 52 target/tracking instance writes through the shared `asyncWritePreservingOwnership` primitive. It deliberately retains the reconciliation algorithms, target-first ordering, durable journal/receipt flow, actor ownership and account/generation/attempt fences.

The reviewed adapter journal-forwarding and physical-disappearance paths already re-resolve mutable state after admission and preserve captured evidence/generation cuts. This pass did not reproduce an additional reconciliation-algorithm defect in those paths. That is not a proof that every path is correct; it is a reason not to replace the engine merely because the recent repair stack is large.

The separate Common #248 fixes a reproduced Unmark admission-incarnation defect. It keeps durable request/receipt identity distinct from the lifetime of a process-local reservation. That fix illustrates the broader direction: strengthen ownership representations at a demonstrated boundary, rather than introducing a generic coordinator which collapses different kinds of authority.

## What the recent failures say about the architecture

### 1. Write ownership must have one implementation

The reviewed Realm 20.0.5 SDK and RealmSwiftGaps #9 distinguish waiting for a write turn from owning the admitted transaction. Cancellation of a queued request must not roll back an independent owner's transaction. After admission, mutation failure may cancel that request's open transaction. After commit submission, task cancellation does not erase the durable result.

This belongs in RealmSwiftGaps, not fifty adapter-specific catch/defer blocks. Preserve the original actor, task-local context, cancellation and transaction semantics. Do not introduce detached mutation tasks, assume `isInWriteTransaction` means ownership, or join an unrelated open transaction to avoid an exception.

The new source guard protects this existing architectural choice: production Swift sources may not reintroduce the `asyncWrite` identifier. All independently queued instance writes should use the owned primitive. It does not replace the primitive's native cancellation/admission tests.

### 2. Domain authorization is separate from write admission

Obtaining a transaction does not authorize an old account, replica binding, attempt, object incarnation or document. Carry the original identity across suspension; re-resolve managed rows and compare the captured evidence in the final transaction. Do not recapture current account or generation and silently adopt it as the old operation's owner.

The adapter's target and tracking transactions are not one distributed transaction. Preserve the target-first, durable-evidence, conditional-tracking sequence so interrupted forwarding can converge without manufacturing an acknowledgement for a different generation. A general-purpose wrapper cannot safely infer those domain-specific conditions.

### 3. Durable outcomes are not callback outcomes

Cancellation, a lost reply, cleanup failure and a durable commit are distinct facts. Existing receipts and journals are the recovery boundary. Reading a receipt is not permission to perform the mutation again, create new tracking work, renew an Undo offer, finish an Article or scroll a renderer.

Review new catch/defer paths as ownership-sensitive mutations, not harmless cleanup. Reused IDs require incarnation evidence where a lifetime can end and begin again. Common #248 centralizes that transition for Unmark without changing the durable protocol.

### 4. Qualification must follow the exact composed revision

Reader #286 selects the writer-repair composition. The current component PRs explicitly defer native execution of that exact successor. Preserve that distinction: a source guard or an earlier green application run does not qualify a new private SDK bridge, caller migration or dependency composition.

A later evidence batch should cover queued cancellation behind another aborting owner, admitted rollback, cancellation after commit submission, target/tracking forwarding interrupted between stages, account/binding replacement while queued, stale receipts/generations, duplicate Unmark delivery and cancelled-reservation cleanup. Record exact component SHAs. Mac, signed UI, performance and Release remain separate gates.

## Source guard

Run `.github/check-owned-writes.sh` at the repository root. The launcher uses the selected Swift 6 toolchain's bundled host SwiftParser/SwiftSyntax libraries, avoiding package resolution, Realm compilation and CloudKit access. Missing modules or malformed/unreadable/empty sources fail instead of silently producing a pass.

The Swift parser scans real identifier tokens across the entire `Sources` tree, including inactive conditional-compilation branches. It catches direct calls, method references, escaped identifiers, split member accesses, implicit members, interpolation and new declarations named `asyncWrite`. Comments and ordinary/raw/multiline string contents are not identifiers and do not trigger it. This intentionally reserves the name throughout production code; an unrelated method using that name must be renamed or receive an explicit policy review, not silently added to an allowlist.

The workflow is read-only, runs on relevant pull requests, and uses the checkout commit pin already present in this repository. It compiles only this checker, not the app or dependencies. Every invocation first runs 15 syntax self-tests, including malformed-input rejection; `--self-test` runs those without scanning a checkout.

## Evidence and limitations

Executed locally using Swift 6.2.1 on Linux: checker compilation, all 15 embedded syntax self-tests and six command-line fixture cases (owned writer accepted; raw writer, inactive raw writer, malformed Swift, empty source tree and missing source tree rejected). The complete BigSyncKit source tree and macOS workflow have not been executed locally in this review.

The guard checks this one source-level boundary. It does not type-check receivers, expand generated macros, establish transaction ownership for synchronous writes, verify private Realm SDK behavior, or prove domain fences, native/runtime/UI correctness or release readiness. Keep this PR and the runtime repair stack draft until the appropriate composed verification. No reconciliation code, dependency pins, production data or application integration refs change here.
