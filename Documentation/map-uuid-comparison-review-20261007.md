# Map/UUID comparison closeout — October 7, 2026

## Current composition, not the old packet

Reader #286 was re-read at `ca4bcc14b3dbe7a4b05d0bedaa1b14744c587b87`, selecting BigSync `c32de536bc981fc8c902306cee1a8733a7c8da40`. Prior #118/#261 and subsequent convergence work are already landed. The unpublished `manabi-map-uuid-review-20261006.zip` remained absent from the selected source.

This change is a normal child of current upload-read PR #131, `3fa8ff2b88cba60cf1e4e25aa1d9f1a8e804c4e8` (tree `8f08b94881f5045eb3dc0f543dd387bb1e8b8f3a`). It deliberately preserves #131's committed tracking/target snapshots, read-only serializer, post-admission missing-target checks, test additions and helper workflow selection. It does not replay the narrower overlapping #130 over #131. All new changes are confined to three existing production files, two native test files and this report.

## Corrections

Six primitive map comparers used a String-keyed Swift dictionary, hiding byte-distinct Unicode key changes. Generalize the existing map comparison helper to match UTF-8 keys and retain the caller's exact value comparator. Duplicate exact keys reject; inputs are consumed once; a stored Optional nil remains distinct from an absent key. Existing String value comparison delegates to the same helper. The existing ambiguous-map transport rejection remains unchanged.

UUID maps are already serialized as strings in a binary property list, but the comparer expected UUID values. Parse existing wire strings before comparison. UUID lists and sets likewise compare decoded UUID identity, preserving list order/cardinality and set membership/duplicate semantics. Malformed strings cannot match any stored nonoptional UUID. Decoder rejection is unchanged; this is neither normalization nor a new wire format.

Map fingerprint ordering gains a UTF-8 tie-break only when Swift's existing ordering ties. Non-equivalent key order, scalar hashes and framing remain unchanged. The tie-break does not make an ambiguous map uploadable.

## Fresh reassessment and refinement

The old portable input model aliased Realm Lists/Sets to Arrays. The refreshed runner instead models RealmCollectionBase's RandomAccessCollection + LazyCollectionProtocol shape. A suspected compile failure of the old proposed list expression did NOT reproduce: it compiled and passed 39 cases against that stronger shape. Do not describe that hypothesis as a confirmed native defect. The final expression uses lazy `elementsEqual`, making sequence comparison explicit and avoiding unnecessary array materialization. The actual-Realm fixture explicitly imports RealmSwiftGaps for its metadata protocol.

Current Common #271 and #272 contain independently owned Finish and original-Mark document rechecks. They are not overwritten or claimed selected by this component change. No additional fresh Unmark writer defect is asserted from source inspection alone.

## Executed scope

Linux Swift 6.2.1, complete strict concurrency, warnings as errors. All eight fresh matrix runs completed:

| Source | Swift 5 Debug | Swift 5 optimized | Swift 6 Debug | Swift 6 optimized |
| --- | --- | --- | --- | --- |
| Final comparison code | 39 pass / 0 fail | 39 / 0 | 39 / 0 | 39 / 0 |
| Current original expressions | 23 pass / 16 fail | 23 / 16 | 23 / 16 | 23 / 16 |

These are 39 distinct methods, not 156 unique tests. The suite compiles the actual helper and exact nine comparison branches with explicit collection-value collaborators, plus Foundation property-list round trips. It does not execute Realm storage, native CloudKit archive behavior, the entire adapter or #131's upload lifecycle. Raw outputs include every expected method, process exit, compiler command and compiled-input hash. The earlier packet's receipts are historical and are not relabeled as these fresh runs.

Twenty-one helper XCTest methods are shared with that suite. Fourteen additional native methods exercise real Realm/CloudKit APIs and remain authored/syntax-parsed, not Apple-typechecked/discovered/executed. Thus 35 native identities require registration. The 18 portable adapter-input methods must NOT be added to the native target. No native or release pass is inferred from portable success.

## Publication integrity

The entire adapter/baseline output was calculated using a temporary GitHub merge preview, then independently checked against the real #131 tree. The full diff contains exactly the nine comparison replacements and one fingerprint expression; every unrelated upload-read source line is retained. Only the resulting full-file blobs enter the normal feature commit. Temporary #134 is closed unmerged and no scratch ancestor belongs in the feature history.

Final blobs: Adapter `4b4caaeaa15030a98ffb36219b285eab30a874fb`; Baseline `e68180f51e6b9fff783cdf3456c28aa911986fad`; Identity `497da7a6c34005171af5bae7bf7ea7dc9b78ba27`; shared tests `1037adde8c412d5af11c5005e13e46ad352f3499`; native companions `155c20bd530cae623cac977e6ad2efd9991be3e1`. Complete smaller published blobs match locally reviewed bytes.

## Integration and qualification

Keep draft. Reader registration must include MapIdentityValueTests.swift and MapUUIDTransportIdentityTests.swift, their 35 exact identities, and #131's existing four upload-read identities/source when selecting this child. Preserve the current active native tuple and all original failed evidence. Root changes belong in a separate reviewed integration commit; this component commit does not itself move Reader.

Native Apple/Realm discovery and execution, actual managed/unmanaged map behavior, CloudKit archive round trips, #131's held-writer/target-reappearance histories, canonical input/receipt/audit/Unmark companions and assembled Reader remain required. Authentic released migration, genuine account switching, signed journeys, Mac UI/performance and application Release retain their independent owner-deferred gates. No protected merge, production CloudKit mutation or release authorization.
