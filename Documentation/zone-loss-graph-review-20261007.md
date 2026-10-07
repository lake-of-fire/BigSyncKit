# Zone-loss error graphs — October 7, 2026

## Source and scope

Direct increment above combined BigSyncKit #127 at
`7d8cb1a1aa0345ccb40bccdec0c55aae8314bd73` (tree
`7db1efd5697ae59bc5dde3033b55dca6426de91e`). The earlier response completion,
mutation settlement, retry progress, processor startup and health work remains
unchanged. No Reader pin, native inventory, active acceptance checkout, target
merge, schema, journal, new retry mechanism or production data change.

Two existing production files change, plus one new native test file and this
review. The response implementation and its 165 tests are byte-identical to the
parent and are re-executed with the shared-helper change.

## Findings

`CloudKitLossClassifier` previously traversed CloudKit partial-item dictionaries
only, returned immediately from that branch, and rejected non-CloudKit wrappers.
Standard singular and aggregate underlying causes were invisible. A missing-zone
or encrypted-reset observation could therefore hide a known terminal deletion.
The code also recursively revisited partial-error cycles and shared subgraphs.

These are controlled error histories, not claims that CloudKit emitted these
specific combinations or that a production data-loss incident occurred. Apple's
Foundation API describes the combined singular/plural underlying-error list;
CloudKit documents userDeletedZone as deletion of a record zone by the user.

Primary API references:
- https://developer.apple.com/documentation/foundation/nserror/underlyingerrors
- https://developer.apple.com/documentation/foundation/cocoaerror
- https://developer.apple.com/documentation/cloudkit/ckerror/userdeletedzone

## Correction

Use iterative breadth-first traversal with a visited key containing both NSError
identity and the inherited full zone ID. The same shared error under different
record/zone keys must be evaluated in each zone; identity-only memoization would
incorrectly erase one zone's result. The queue retains NSError objects so bridged
temporary wrapper addresses cannot be reused while their identities are stored.

Inspect CloudKit partial-item edges and standard Foundation singular/plural
causes together. Underlying causes inherit the wrapper's zone, not the last
partial item's zone. Explicit record/zone item keys override inherited scope.
Unknown non-CloudKit numeric codes do not acquire CloudKit meaning.

The Foundation cause-key parser is shared with retry classification. Retry
policy, graph traversal and public interfaces are unchanged; it delegates its
existing metadata read to the same internal helper used by the zone classifier.
Each zone-scoped observation obtains one userInfo snapshot for both item and
underlying edges.

Traversal has a 32-level boundary. When a previously unseen scoped observation
cannot be inspected, the classification cannot establish permission to recreate
or rebuild a zone: missing and encrypted-reset dispositions are withheld. Known
terminal observations, record identities and discovered account/transient codes
remain. Merging another classification cannot restore permission discarded for
incomplete evidence. No hidden cause is invented. A deep alias already inspected
through a shallower path in the same zone does not make evidence incomplete.

This bounds depth and terminates identity cycles; it does not claim an independent
node/edge-count bound or arbitrary-depth completeness. It changes classification,
not the enclosing zone setup, recovery/backoff or account lifecycle. The caller's
existing original-error and ownership checks remain responsible for operation
failure. No native network/recovery sequence is qualified here.

## Reproduced verification

Complete actual loss-classifier, retry-classifier and response-drain files run
with explicit SDK/lifecycle collaborators. Both native test files are used
unchanged by the portable build. Swift 6.2.1 Linux x86_64, Swift 6 language mode,
strict concurrency defaults, warnings as errors, fresh scratch per execution.

**197 unique methods = 165 unchanged response tests + 32 new zone-loss tests.**

| Selection | Passed | Failed |
| --- | ---: | ---: |
| Parent loss/retry implementation with final tests | 176 | 21 |
| Final Debug | 197 | 0 |
| Final optimized | 197 | 0 |
| Ignore underlying causes | 179 | 18 |
| Memoize without zone context | 193 | 4 |
| Discard incomplete-observation state | 193 | 4 |
| Ignore partial-item routing | 185 | 12 |
| Let merge restore incomplete recovery permission | 196 | 1 |
| Check depth before already-inspected aliases | 196 | 1 |
| Disable cycle memoization | 192 | 5 |
| Erase known terminal observations on truncation | 196 | 1 |

The 165 existing response tests pass in every completed matrix lane. Of the new
32 methods, the original classifier passes 11 and fails 21. Cyclic fixtures stop
supplying the back edge after eight reads so a regression is reported without
crashing the test process; the corrected classifier reads each scoped cycle node
once. This is not a native stack-overflow reproduction.

An independent evidence audit reconciles exact declarations, raw XCTest starts
and completions, separate executable discovery, compiler inputs, source/log
hashes and process exit statuses. No build cache is reused between variants.
The initial original-source discovery run had the default zero-failure
expectation and was correctly rejected; a separate run declares the observed 21
failures. Two interrupted enclosing tool calls lack complete process/discovery
receipts and are not counted. An earlier successful proposal predates a final
idempotent truncation-cleanup guard and retains its original source identity.

All production and new native test declarations pass frontend syntax parsing.
This is not Apple SDK typechecking. Native CKRecord/zone identity classes,
NSError bridging, real Foundation constants, CloudKit, Realm, disk durability,
actual setup/backoff, Reader UI, signed journeys and application Release remain
unexecuted here. Portable SDK collaborators include the pre-existing Linux-only
Foundation multiple-error constant/property proxy; none ships. The history enum
and struct declarations are copied exactly from the parent CloudKitChangeFeed
source, not a replacement for its transport or cursor implementation.

## Native integration

New source and owner:
`Tests/BigSyncKitTests/CloudKitLossClassifierGraphTests.swift` /
`CloudKitLossClassifierGraphTests`, 32 method identities.

Reader's explicit source graph and both required-method inventories must include
this file when the integration owner selects the successor. Retain all current
response, retry, account, lifecycle, receipt, disappearance, quarantine and Unmark
requirements. Keep #127 draft pending owning Apple/Reader acceptance.

Published production/test postimages:
- loss classifier: `f2f4e70427b50e5bd23a353f5dd0fd0706535439`
- shared retry helper: `de01b2d2bec2f931b1e9fa0bba4ee6d354979902`
- new native tests: `a7b440c56492af3f1f4d1b2ef39f3e3b623e05fb`

The companion archive `bigsync-zone-loss-graph-review-20261007.zip` contains full
original/postimage sources, runnable combined tests, independent fault variants,
raw execution records, source identities and native requirements. Historical
portable or native passes do not qualify an independently changed composition.
