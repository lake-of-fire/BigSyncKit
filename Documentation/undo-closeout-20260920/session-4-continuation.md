# MR-UNDO-CLOSEOUT-20260920 — W4 BigSync completion

Target/base: `4baa7a4903c9f9372903fedf36a95afcb49ced71` (`main`).

**Final W4 BigSync source/test head:** `6a46ad36ce7ef9fc0a64dce551b226c2cbfbae2c`.
The commit containing this note is documentation-only; Reader may pin that descendant while qualification remains bound to the unchanged source/test tree above.

This W4 branch integrates W1 reconciliation/evidence lifecycle through normal merge ancestry and adds only W4 integration surfaces: injected binding-store identity, declaration-only follow-up drains, transition readiness, and bounded read-only released-zone tracking evidence.

## Interfaces consumed by the candidate

- `requestFollowUpSynchronization(after:)` validates the exact prepublication run/account/binding and reuses the existing full-drain tail. It cannot promote download-only work and does not create a fake mutation or detached synchronization task.
- `domainTransitionReadiness(after:)` rechecks the actual pending-work inventory and semantic blockers under the current worker/run before a domain transition.
- injected binding-store helpers ensure W4 admission/journal code shares the worker's real durable installation/binding store.
- `BigSyncLegacyTrackingEvidence` reads the released `ManabiPlatform.v2.realm` tracking file from a private copy, accepts only synced rows with decodable system-field CKRecord evidence, and exposes the historical v2 device-key namespace for Common/Core's bounded 3.11 membership proof. It does not import old records or manufacture accepted baselines.

## Native qualification

GitHub Actions run **35535313102**, job **106143642095**, macOS 15 runner, checked out exact source/test head `6a46ad36ce7ef9fc0a64dce551b226c2cbfbae2c`.

Focused W4 discovery/execution:
- **16 discovered / 16 passed / 0 failures** across `InjectedBindingStoreIdentityTests`, `DomainPrepublicationFollowUpTests`, and `BigSyncLegacyTrackingEvidenceTests`.

Full package qualification:
- **666 tests / 0 failures** on the restored candidate.
- the qualification script's deliberate older-source negative-control run produced its expected failures before restoring the candidate; that is not a candidate regression.
- repeat qualification: 10× the selected 49-test closure slice passed.
- mutation-identity benchmark in this runner: fixed identity ~50,307 ns/refresh; durable identity ~209,125 ns/refresh, 1,000 iterations. This is vendor infrastructure evidence, not the requested end-to-end Reader Mark/Undo/activation performance gate.

W1's separately qualified source/test head `3120e2caf68ffef4f3b16302046d706fc7172603` remains in ancestry (run 35530038608: 647 executed, 1 skipped, 0 failures; focused W1 28/28).

## Qualification limits

This public vendor lane contains no private Reader/Common/Core source, installed user Realm, app signing, or CloudKit credentials. It therefore does **not** qualify the assembled app, the genuine released 3.11 installed-file migration, signed two-client CloudKit convergence, W3's producer-to-first-native-receipt boundary, or end-to-end Reader performance.

No target branch was merged, no production CloudKit data was touched, and no release was deployed.
