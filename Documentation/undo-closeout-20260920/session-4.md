# MR-UNDO-CLOSEOUT-20260920 — W4 binding-resource companion

Base: `4baa7a4903c9f9372903fedf36a95afcb49ced71`.
Verified branch head before this documentation commit: `63d999e462dc937636da8b4f109851ea0dd81e52`.
Branch: `codex/undo-closeout-w4-binding-resources-20260920`.
PR: https://github.com/lake-of-fire/BigSyncKit/pull/51.
Consumers: Common #84, Core #134, Reader root #163.
W1's active closeout implementation is BigSync #50 (the earlier #33 reference was superseded by live marker discovery).

## Concrete integration correction

The signed worker injects `BigSyncLocalStateConfiguration.keyValueStore`, while domain admission/journal helpers previously used the default identity store. W4's final Realm lease checks expose that disagreement. Moving the worker state file would break existing process-kill checks of `bigsync-state.plist`.

`BigSyncClientIdentity+InjectedStore.swift` exposes overloads for binding preparation, current journal identity and a live journal-identity provider against the actual injected store. These delegate to existing `BigSyncReplicaBindingStateStore` and `BigSyncMutationJournalIdentityReader`, preserving installation-before/after validation and pending replacement behavior. No new serialized format, application-side binding decoder, outbox, account policy or reconciliation is introduced. Existing default APIs are unchanged.

Common/Core share the disposable identity directory and existing atomic checkpoint state path. Production resources are unchanged. This companion does not modify W1 implementation files or its branch.

## Named native tests and real evidence

`InjectedBindingStoreIdentityTests` contains:
- `testInjectedStoreIsTheOnlyPreparedBindingStore`
- `testExistingProviderObservesPendingReplacementAndActivation`
- `testFreshStoreAndProviderResumeTheSameDurableBinding`
- `testMalformedBindingFailsClosedWithoutPreparingAnotherStore`

The existing macOS workflow was extended, not replaced. `.github/qualify_w4_binding_resources.py` separately runs `swift test list`, requires discovery of all four exact names, executes their filter and requires four named passing XCTest cases. It writes `qualification-w4-binding-resources.json` and logs, always marking application and signed CloudKit qualification false. The existing package qualification still follows.

Observed initial run: https://github.com/lake-of-fire/BigSyncKit/actions/runs/35493841582, job `106033240081`. Root checkout succeeded; checkout of the pinned RealmSwiftGaps dependency failed with GitHub HTTP 503 and exit 128. Native build, discovery and tests did not execute. This is infrastructure failure, not a behavioral RED. Rerun tool actions returned invocation/binding errors; no successful manual rerun is claimed. Subsequent commits retain the workflow trigger and explicit discovery gate; their result must be read from GitHub rather than assumed.

No native pass, application qualification or signed CloudKit result is claimed in this note. Only generic BigSync source/tests enter vendor CI; no private application source, private Realm or user records are uploaded there.

## Ownership handoff

W4 must combine this commit or a reviewed descendant with W1's completed sync/evidence head on a candidate branch before app qualification. Root must pin the actual combined commit, not merely claim W1's tests are present. No target branch was merged and no deployment was performed.
