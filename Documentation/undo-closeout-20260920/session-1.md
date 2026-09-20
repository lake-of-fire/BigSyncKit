# MR-UNDO-CLOSEOUT-20260920 — W1

Implementation in progress; this checkpoint is **not** a source-complete or release-qualified handoff.

Base: `4baa7a4903c9f9372903fedf36a95afcb49ced71` (`main`). Reader composition was re-read at `105da76b8efb4b6ece152c4f2dfa087cf689000a` with Common `cd5fd21484e35a29c28e664ba5b020087ca194e4` and Core `cef4f7c37759dd538988db539b741736733464be`. No target branches or root gitlinks changed.

The incoming-policy API is introduced at `c06d13dc38e9febb75ab74c7640e8896f842a417`, and included in contract v2 identity at `41b4b0a6b14c467204ab1f36c1c6715ca4be9d70`. It is not yet wired into decoding/application in this checkpoint.

Native baseline: macOS run `35491425445`, exact commit `a7b6401de5310d778e0b30bbaced0dd77d1bf239`, discovered/executed 619 XCTest tests, one skip, zero failures. This only establishes the unchanged adapter plus the additive declaration's compatibility. Public dependencies were pinned to RealmSwiftGaps `2d4fa2bfd8b1c856b45aca2c7a97c301d37204e2` and SwiftUtilities `f437c7d06fc631cd7a67731279411c417cdf8077`. No private application code or installed Realm was sent to this public job.

Counterexamples committed at `1886f481c7181e997ca7bf841d443b1b74ec0cfc`:

- `SyncUndoCloseoutW1Tests.testOmittedScalarsApplyDeclaredDefaultsAndAgreeWithBaseline`
- `SyncUndoCloseoutW1Tests.testRetainedClearIsAcceptedByTheTerminalAudit`
- `SyncUndoCloseoutW1Tests.testTerminalLocalDeleteRetiresItsSupersededStagedSave`

These use the actual adapter, target and tracking Realms, journaled mutations and prepared acknowledgements. Their pre-repair native result is pending at this commit. A compilation/setup error will not be reported as the intended behavioral RED.

W1 owns BigSync reconciliation/evidence/audit and only the Common contract-policy/model-test companion. W2's Common PR #85 was inspected and has no W1 file overlap. W4 owns Core transport forwarding, shared Realm migration/configuration, test membership, root pins and final signed macOS CloudKit qualification. The finished note will replace this checkpoint with exact implementation heads, APIs and observed test results.
