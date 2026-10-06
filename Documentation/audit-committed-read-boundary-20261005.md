# Terminal audit: committed views rather than provisional success

## Scope and finding

This increment is based on BigSyncKit #98 `32bf433809991249db1fbb752609741a1ba8213c`, stacked on #96. The preceding physical-disappearance repair and its tests stay intact. This changes one additional production file, `BigSyncSynchronizationAudit.swift`, not a mutation, upload, acknowledgement or reconciliation algorithm.

The terminal audit refreshed operational Realms and then read those live handles. With another owner's transaction open, provisional journal removal, acknowledgement, relationship/quarantine cleanup or baseline repair could conceal committed debt. Conversely, provisional edits could appear as durable corruption. The existing account eligibility helper also resolved the target through the live provider, so freezing only the enumerated rows would not close the boundary.

The complete old audit reproduced these errors in a controlled in-memory MVCC collaborator. This is not attribution of a production incident or native SDK execution.

## Correction

Capture a frozen committed view after each appropriate refresh; a refresh callback can itself open a transaction. Cache by native Realm object identity within this one audit. The provider's schema aliases, local-row comparison, journal inventory and comparison-evidence inspection share the same captured version per Realm. Account-scoped tracking resolves its target from that same snapshot map and then uses the unchanged object/account predicate. Unscoped, missing-target and malformed-identity semantics remain unchanged.

No frozen handle escapes the invocation or survives a new actor suspension. A later audit captures new views. These are per-Realm committed views, **not a globally atomic cross-file snapshot, a CloudKit boundary lease, or replacement account authority**. Existing synchronizer quiescence and caller-owned qualification remain necessary. The current namespace/error policy, sorted issue strings, Codable shape and public API are unchanged. No queue, schema, journal, remote operation or production data is added.

## Executed source-boundary results

Swift 6.2.1 / Linux x86_64 / Swift 6 language mode / warnings as errors. The runner compiles the entire actual audit and unchanged actual comparison inspector. The storage/schema/transport collaborators are explicit and excluded from native targets. The three existing eligibility method bodies are carried as unmodified excerpts; record decoding, fingerprints, setup and submission validation are collaborators, not newly claimed SDK coverage.

| Source selection | Passed cases | Failed cases |
| --- | ---: | ---: |
| Complete unchanged audit | 6 | 12 |
| Fixed audit, Debug | 18 | 0 |
| Fixed audit, optimized | 18 | 0 |
| Restore only live tracking reads | 14 | 4 |
| Restore only live model-map reads | 12 | 6 |
| Restore only live account filtering | 16 | 2 |
| Restore the live-after-refresh shortcut | 16 | 2 |

The original has 21 assertions across 12 failing cases. Counts above are unique methods per run, not cumulative assertions/configurations. The exact checked-in runner also passes all 18 in a fresh temporary Debug package. The previous complete disappearance runner was rerun: 15 cases passed; its production bytes are unchanged. Those 15 are a separate portable scope, not assembled application evidence.

An initial collaborator collection implementation incorrectly enumerated other model tables when given a runtime `Object.Type`. Its baseline failures were invalid and retained separately. After correcting table filtering, **both** original and fixed sources plus every single-boundary negative control were rerun. An initial 20-second optimized invocation timed out during discovery compilation and returned no test result; the subsequent completed optimized run is the result above. Original attempts are retained in the attached evidence, not relabelled as passes.

Reproduce with `bash Tools/SynchronizationAuditReadBoundary/run.sh`, optionally `--configuration release` for optimized portable execution only. Neither configuration qualifies application Release behavior.

## Authored native cases

Eight methods are added to the already registered `SyncUndoCloseoutW1EvidenceFenceTests.swift`. All ten existing method bodies/identities remain. New tests use actual file-backed W1 Realms and the real audit entry. They preserve held transaction postimages and their rollback, retain submitted candidates/relationships, and confirm new commits are visible to later audits. Account cases bind an existing string field in a disposable adapter; no test-only model is added to Realm's global default schema.

- `testAuditRejectsProvisionalCompletionAcrossTargetAndTracking`
- `testAuditRetainsTrackingDebtBehindHeldAcknowledgement`
- `testAuditIgnoresHeldProvisionalTargetMutation`
- `testAuditRetainsSubmittedCandidateBehindProvisionalRemoval`
- `testAuditRetainsRelationshipDebtBehindProvisionalRemoval`
- `testAuditResamplesActuallyCommittedOwnerOnNextInvocation`
- `testAuditAccountFilterIgnoresProvisionalDeparture`
- `testAuditAccountFilterDoesNotAdoptProvisionalArrival`

Native source frontend syntax parsing passes. **Native typechecking, Xcode discovery, Realm/CloudKit execution, full package/app, WebKit/UI, signed, performance and release qualification have not run.** Native audit-specific refresh-notification scheduling is not proven by the portable callback cases. No native fixture or assertion is counted among portable passes.

## Integration

During publication another worker merged #98 at its earlier `32bf4338` head into #96. This audit is a separate draft successor of that exact source, not part of the closed PR's merged content. The audit production/test bytes above remain unchanged. Keep the audit successor draft. Reader #286 remains the application candidate. When selecting the combined BigSync successor, preserve the current Common/Core/Realm/Lake composition, reconcile the actual gitlink/source hashes and register these eight methods plus #98's preceding nine disappearance methods in both native inventories. The existing test-file membership is not runtime discovery. Run with the W1 audit/transport/receipt and Realm-owner suites before transferring any readiness to the combined source. This increment changes no Reader pins, inventory/qualification history, generated project, target branch or release authorization.

Preimages verified against complete Git blobs: audit `11d5d7eac3ff1ccfd47197a158082cea0b93475d`; unchanged inspector `14f266d016fd242338e57240954281b0ac71219b`; native file `542acaddfbc31bca66f9ea32968782202b301d49`.
