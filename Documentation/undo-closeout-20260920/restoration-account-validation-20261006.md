# Restoration and persisted account-authority review — October 6, 2026

Base: actual BigSync #96 `e15f300c18b263d96c1a82287e9076e87cfd76ac`, not its stale description checkpoint. Reader #286 remains the application integration owner. This increment preserves the latest processor, receipt, journal and cursor work; the concurrent #118 Unicode transport repair has independent adapter hunks and must not be replaced by a whole-file copy.

## Reconciled prior work

The previous unpublished restoration packet was reviewed against this current source. #109 already supplies strict publication metadata decoding; that decoder is retained byte-for-byte rather than replaying the older integral-real policy or duplicate decoder test class. The earlier SPI test import is also already present. Only the outstanding cold-read, final-callout and writer/read symmetry changes are carried forward.

## Findings repaired

1. Cold publication restoration read live target/tracking Realms although it did not own those transactions. Independent provisional journal, comparison, rebuild, tracking and cursor state could authorize or suppress saved completion. Reuse `committedRealmReadSnapshot`; cursor/epoch/tracking checks use one committed tracking view. Preserve the immutable read-only configuration path, existence/schema checks and nonmutating inspection.
2. Final synchronous predicates could revoke the captured restoration owner after its last check. Recheck the existing owner predicate immediately before both positive returns. Actual task cancellation still throws; ineligible stored evidence remains ordinary nil. The unchanged outer delivery wrapper is covered separately.
3. Stored active/restored account owners were optional casts. A malformed present value became absence, letting actual `bindInitialAccount` assign another owner, or `prepare` erase retained ownership during installation replacement. Distinguish absent from malformed and reject before persistence.
4. Lease/version generations used lossy NSNumber conversion, and account envelopes admitted non-finite dates. Use exact integer-shaped nonnegative values, explicit persisted Booleans and finite active timestamps. Pending ports retain their original bound digest/whole-envelope checks and now reject equal source/destination, matching the existing authoring invariant.
5. Account and publication writers now validate their formed dictionaries through the same decoder before replacing good stored state. No alternate writer or persisted format is introduced.

These are malformed-evidence and observation-ordering correctness defects, not evidence of a user data-loss incident or a genuine Apple-account-switch pass. No schema, durable key/version, journal, CloudKit wire format, account transition policy, lock, queue or public API changes. Exact valid original strings, genuine unbound state, invalidated lease generations, unknown extra fields, ordinary port/cancel/activate and same-owner idempotence are preserved.

## Refactoring

A private account-envelope scalar helper is shared by the two existing account formats. The detached internal lease decoder replaces the old private inline parser; the synchronizer forwards to it and retains its existing error/absence behavior. The replica store's existing load and persist share one decoder. Publication uses #109's existing decoder. No repository-wide generic codec or new authority coordinator is added.

Public adapter registration admits only the identical existing instance; unsupported direct dictionary replacement was not promoted into a new production defect or ownership framework.

## Executed current-source evidence

Linux Swift 6.2.1, strict Swift 6, warnings as errors. All final compiler/run processes completed.

| Scope | Current original | Fixed Debug | Fixed optimized |
| --- | --- | --- | --- |
| Complete publication file + unchanged delivery wrapper | 37 pass / 18 fail | 55 pass | 55 pass |
| Complete cold-inspection declaration + existing snapshot helper | 3 pass / 12 fail | 15 pass | 15 pass |
| Complete account-authority file, actual lease reader/writer bodies and tests | 17 pass / 12 fail | 29 pass | 29 pass |

These are three explicit controlled scopes, not native SDK or application qualification. Repeated configurations do not add unique cases. The account scope includes the identical 25 repository XCTest methods plus four tool-only private-writer boundary methods. Its storage/client protocols are explicit collaborators; the CryptoKit import facade uses real system OpenSSL SHA-256, not Apple's CryptoKit SDK. Restoration uses explicit Realm/CloudKit/account collaborators and does not establish actual MVCC or disk durability.

Independent fault controls: remove final restoration owner checks -> 14 failures; remove publication writer validation -> 4; restore only live cold target/tracking views -> 5/7; restore malformed-owner casts -> 3; restore numeric coercion -> 3; remove finite date checks -> 3; remove account writer validation -> 2. Each executes the complete same scope roster.

Initial records are retained separately: one obsolete publication test expected integral-real compatibility already rejected by #109; early expected-failure counts needed correction; a negative test's attempt to stringify an admitted NaN Date trapped in Linux Foundation. That assertion was changed to compare a Boolean, then both original/fixed full rosters were rerun to completion. No partial, crashed or mismatched-inventory run is called a pass.

## Repository test requirements

New `AccountAuthorityPersistenceTests.swift` has 25 methods. The exact file executes in the controlled account module, not the full Apple package. Four new W1 cold-restoration methods and four account-fencing restoration histories are added to existing files; those eight are authored, not natively executed. The nil-by-default epoch fixture hook changes no preceding scenario.

Reader must add the new account test file to Project.swift and all 33 exact method identities to both required inventories when selecting the reviewed child. Existing file inclusion and portable XCTest execution do not establish native discovery. Run the current account/replica, publication-restoration, W1 receipt/journal and composed Mark/Unmark suites on the owning Apple executor. Preserve other workers' running batches and current pins until that deliberate selection.

Native Apple/Realm/CloudKit typechecking, notification/rollback behavior, full package/application, WebKit/UI, actual account replacement, installed-release migration, signed journeys, performance and application Release remain unqualified here. No release flag is changed.

## Source integrity

Complete account source `07b161bee2eb82d069658c541d084d99c53c7880` -> `e05168243128edb4d90e85e72b6795ee4d12cd23`; complete publication source `6df674e03ec2b1b492ca8f5353cacc1bdf79e544` -> `a020b11cc1a9b732192788fea7e507cffd0402a1`. Both proposed whole-file blobs match the locally compiled files. The full generated adapter/synchronizer/test diff contains only the reviewed methods and additive histories. Temporary calculation #119 is not a candidate; the actual feature is a normal direct child of e15f300c with no calculation ancestry. The reproducible companion evidence packet retains exact sources/excerpts, collaborators, test rosters, commands, logs and statuses; excerpt files must never replace complete repository sources.
