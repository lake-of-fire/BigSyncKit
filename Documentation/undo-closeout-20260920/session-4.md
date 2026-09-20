# MR-UNDO-CLOSEOUT-20260920 — W4 binding-resource companion

Base: `4baa7a4903c9f9372903fedf36a95afcb49ced71`.
Source/test head before this note: `fceea5bb5e19a148f3afc6669db078c9852bea43`.
Branch: `codex/undo-closeout-w4-binding-resources-20260920`.

## Concrete composed failure

Core's signed worker injects `BigSyncLocalStateConfiguration.keyValueStore`, but Common's source admission and journal helpers previously read the default `BigSyncClientIdentity` binding store. The stronger W4 final Realm lease check exposes their different identities/bindings. Changing the worker's file path would also invalidate the existing process-kill checkpoint's exact `bigsync-state.plist` checks.

This companion exposes overloads accepting the existing injected store for binding preparation, live journal-identity reads and provider creation. The implementation delegates to the existing production `BigSyncReplicaBindingStateStore` and `BigSyncMutationJournalIdentityReader`, including installation-before/after validation and pending binding semantics. It introduces no serialized format, outbox, account policy or alternate reconciliation. Default-store APIs remain unchanged.

W4 Common/Core use the same disposable identity directory and existing atomic state-file path for both the worker and canonical Realm writers. Normal production resources are unchanged.

## Coverage

`InjectedBindingStoreIdentityTests` authors four tests using the real file store and binding policy: only the injected binding is prepared; an existing provider sees pending replacement and activation; reopening retains the binding; malformed binding fails closed without a fallback store. These are generic BigSync tests; no private application source or installed Realm is copied into vendor CI.

Native execution/discovery is pending at this note. No signed CloudKit or application qualification is claimed. W1 owns its independent sync/evidence changes; this branch does not modify W1 files or its branch and must be composed with its completed head before root qualification.
