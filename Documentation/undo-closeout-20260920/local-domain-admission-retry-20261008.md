# Temporary local initial-admission retry

The Reader initial-head admission callback sometimes cannot safely inspect its Shared Realm because an independent actor write already owns that store. That local condition must defer admission without inspecting provisional data. It must also keep the existing synchronization request alive so the current owner can try again once the store is available.

`BigSyncLocalDomainAdmissionDeferredError` names this one temporary domain-admission condition. Core maps only Common's `ReaderDatasetAdmissionLeaseError.sharedStoreBusy` to it. Account, installation, binding, selection, corruption and cancellation errors retain their original classifications.

The error enters the existing delayed local-target retry branch in `failSynchronization`. It uses the same one-second delay, current-attempt checks, account-authority checks, cancellation handling and synchronization-drain waiters as that existing branch. There is no additional retry worker or persistent queue.

The added `CloudKitSynchronizerAccountFencingTests.testTemporaryLocalInitialAdmissionRetriesExistingDrainAndAdmitsCurrentBinding` uses the existing transport and model-adapter fixtures. One real `synchronize()` request defers its first initial-binding callback, admits the second callback, and finishes through the existing scheduler. It asserts zero CloudKit operations before both admission callbacks, the same pending binding generation, an admitted final account lease, and no scheduled retry or live drain after completion.

**The new native regression has been source-reviewed but has not been compiled or executed in this Linux review environment.** The test is an authored requirement, not a claimed pass. It must be included in both Reader native method inventories and qualified on the exact composed dependency selection with the Common and Core admission repairs. No signed CloudKit, application, UI, or release qualification is implied.

The initial proposal is based on landed BigSync `ea7ccb1702015feab3479d7dc2e96d4e42010a1d`, whose complete tree `303092ac4ff1e232ea91985aa1794da1c62faa64` equals the currently selected reviewed ancestor `eaffec59709b2f7a6f6f1bf45fc8f6e49b381135`.
