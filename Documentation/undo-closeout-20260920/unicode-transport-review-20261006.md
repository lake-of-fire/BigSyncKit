# Byte-exact Unicode identity in Realm transport

## Baseline and published source

BigSyncKit #118 starts at actual #96 `e15f300c18b263d96c1a82287e9076e87cfd76ac`, selected by Reader #286 `2f202b09f3b5fda8b600fd9a5218eb59b910563e`. Initial feature commit `63b0a2d51f32716789d7f8c894c2908d69350151` has that sole parent. Its second commit changes only fixture cleanup and this report. Previously merged processor, callback, cursor and acknowledgement work is inherited unchanged.

Production files:
- RealmSwiftAdapter.swift: original `4f7437214f445c002a43d08a9c8ceaec23f80f9d`, final `b5acb585d3310b0e3c3eb44cc770d2288f2e9a88`.
- BigSyncRecordIdentity.swift: original `0b29e103cf42f46e6c2379147bc5c241bde2de20`, final `bb0779bb8086b4473d0e8350edc4754a0c4c3325`.
- Native test file: final `0990a5af44b00c640a3b7cecec4386297e3c5897`.

No schema, record name format, fingerprint encoding, wire format, journal, source-shard policy, coordinator or public API changes. The existing record-name ASCII restriction stays intact.

## Findings and fixes

### Incoming string sets lost exact members before assignment

The decoder converted the incoming String array to Swift Set<String>. Swift canonical Unicode equivalence collapses distinct UTF-8 spellings (for example precomposed kana with dakuten and its combining spelling). Realm's existing native string collection identity and the existing BigSync field fingerprint retain those bytes. The member was therefore discarded before the actual Realm assignment. The comparison-object decoder consumes the same branch.

Keep the already type-validated array and pass it to the existing assignment; Realm retains responsibility for deduplication. Exact duplicates remain duplicates of the same stored identity, while canonical-equivalent byte-distinct values can remain separate. Existing cancellation immediately before assignment, empty omission and malformed-input rejection remain unchanged. There is no migration or attempt to reconstruct data already lost by older input processing.

### Change detection and audit disagreed with fingerprint identity

String scalar, list and set comparison used ordinary Swift equality. A byte-level replacement, reordering or missing exact member could appear unchanged even when the established field fingerprint differs. Reuse a small internal BigSyncStringIdentity encoder/comparer alongside the existing identity utility: UTF-8 scalar comparison, streaming ordered sequence comparison, and byte-keyed unordered membership. No Unicode normalization. URL lists still compare their existing absoluteString representation. Non-string branches and the fingerprint code itself are unchanged.

## Review breadth and second pass

Reviewed the incoming representation policy, shared incoming/comparison field traversal, generic audit comparison, adopted payload serialization, baseline fingerprinting, and current disappearance/worker boundaries. The second pass found a separate Common chapter writer-replay defect, published independently in Common #261; it reuses Common's existing exact-byte helper and has no new BigSync API dependency.

The final fixture refinement adds guaranteed rollback cleanup if the managed-set assertion path unexpectedly throws. Native model ownership, explicit schemas, URL-list encoding and the common cancellation boundary were rechecked. No extra issue is claimed merely because a neighboring file was inspected. Map-key transport was not changed or qualified by this patch.

## Executed controlled evidence

Swift 6.2.1, Linux x86_64, complete strict concurrency and warnings as errors. Eight complete primary build/run receipts:

| Source | Swift 5 Onone | Swift 5 O | Swift 6 Onone | Swift 6 O |
| --- | --- | --- | --- | --- |
| Corrected | 85/85 | 85/85 | 85/85 | 85/85 |
| Exact old expressions/branch | 45 pass / 40 expected failures | same | same | same |

The 85 checks cover kana, Latin, Hangul, combining order, scalar symmetry, ordered cardinality/reordering, set permutation/duplicates, empty and NUL values, malformed input and cancellation. These are 85 distinct controlled checks, not 340 distinct tests or forty separate bugs.

Four isolated Swift-6 unoptimized reversions: scalar comparison 65 pass/20 fail, ordered sequence 73/12, unordered membership 77/8, decoder branch 73/12. The union of failure identities is exactly the original forty-case set; scalar equality is also consumed by ordered comparison, hence overlap.

The runner compiles the actual complete comparison helper extracted from BigSyncRecordIdentity.swift and the exact original/changed string-set branch with its type/cancellation admission. It observes bytes delivered to the Realm assignment boundary. It neither substitutes a fake Realm collection nor compiles the complete 543 KB adapter. Actual Realm managed/unmanaged assignment, CloudKit archive and full application behavior require native execution. Full small helper/native files syntax-parse; parser success is not SDK typechecking.

The full adapter result is independently verified by GitHub's normal feature diff to contain only the four intended hunks. Temporary calculation #117 was closed unmerged; only the resulting complete verified blob entered the normal direct-child feature commit, never scratch ancestry. The downloadable installer checks both original and resulting full adapter Git blob identities before writing, rather than trusting substring matches alone.

## Native tests: eighteen authored, none executed here

UnicodeStringTransportTests uses one uniquely named Object fixture excluded from default-schema discovery. Stores are unique in-memory configurations with ordinary journal policy; the existing RealmAdapterFixtureOwner owns the adapter and directory. No production store, CloudKit request, released fixture or genuine account transition is used. Incoming foreign postimages do not invent a local mutation generation.

Required file: `Vendor/BigSyncKit/Tests/BigSyncKitTests/UnicodeStringTransportTests.swift`.

```
UnicodeStringTransportTests/testScalarByteChangeIsVisibleToAudit()
UnicodeStringTransportTests/testListReplacementAndOrderUseByteIdentity()
UnicodeStringTransportTests/testSetMembershipUsesByteIdentityNotSwiftEquivalence()
UnicodeStringTransportTests/testExactReplayAndSetPermutationRemainNoOps()
UnicodeStringTransportTests/testIncomingSetKeepsEquivalentByteDistinctMembersUnmanaged()
UnicodeStringTransportTests/testIncomingSetKeepsEquivalentByteDistinctMembersManaged()
UnicodeStringTransportTests/testIncomingSetAssignmentReplacesInsteadOfUnion()
UnicodeStringTransportTests/testExactDuplicateDeduplicatesButDistinctSpellingsDoNot()
UnicodeStringTransportTests/testAbsentSetFieldClearsManagedCollection()
UnicodeStringTransportTests/testMalformedSetRejectsWithoutPartialMutation()
UnicodeStringTransportTests/testCancelledSetDecodeDoesNotMutateTarget()
UnicodeStringTransportTests/testComparisonDecoderAndIncomingApplyShareExactSetMembers()
UnicodeStringTransportTests/testAdoptedPayloadRoundTripPreservesAllStringIdentities()
UnicodeStringTransportTests/testManagedRollbackRetainsOriginalSetAndJournalGeneration()
UnicodeStringTransportTests/testUnchangedNonStringFieldsRetainComparisonBehavior()
UnicodeStringTransportTests/testRecordIdentityConstructionRemainsUnchanged()
UnicodeStringTransportTests/testExistingFieldFingerprintsRemainUnchangedByReplay()
UnicodeStringTransportTests/testURLListUsesExistingEncodedAbsoluteStrings()
```

## Integration boundary

Keep draft. W4 owns Reader #286's pins, explicit source selection, both required-method inventories and currently executing immutable native tuple. This review changes none of those. Compose the component source, register the new path and eighteen exact identities, regenerate only the owning workspace, confirm actual Xcode discovery and run the native collection/normalization/audit/Unmark batch. The Common #261 companion adds eight separate identities. No historic test totals transfer onto either change.

Native SDK, real Realm/CloudKit durability, authentic released migration, genuine account switching, signed two-client, UI/performance and Release remain independently unqualified here. No protected-target merge, production CloudKit mutation, deployment or release authorization.
