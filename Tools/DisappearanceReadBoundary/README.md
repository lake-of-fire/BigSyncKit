# Portable disappearance boundary tests

Run `bash Tools/DisappearanceReadBoundary/run.sh` from a checkout with Swift 6
and Python 3. Add `--configuration release` for an optimized **portable harness**,
not native application Release qualification. Existing DEBUG suspension hooks
remain enabled in both modes. No dependency fetch, network operation or app-data
access is required. Temporary packages are moved to the existing Trash location.

The runner copies the complete selected production
`Sources/BigSyncKit/RealmSwift/BigSyncRecordDisappearance.swift`. It also extracts
`currentRecordEvidenceCut` and `validateRecordEvidenceCut` verbatim from the
selected `RealmSwiftAdapter.swift`. The extractor fails on duplicate, missing or
unsupported declaration layouts instead of substituting an old owner algorithm.
Its checks run with:

```sh
python3 -m unittest discover -s Tools/DisappearanceReadBoundary -p 'test_extract_owner_methods.py'
```

The production methods compile with explicit CloudKit, Realm and adapter/domain
collaborators. The Realm collaborator supplies committed/provisional versions,
rollback, freeze and synchronous refresh callbacks. It does **not** implement the
SDK's queued admission, executor behavior, file lifetime, disk durability or
CloudKit. Swift tasks, cancellation and thrown errors execute normally. Callback
injection is task-scoped and lives outside native/production targets.

There are 40 distinct runtime methods: the original 15 snapshot histories and
25 additional owner/callback histories. They cover provisional-state isolation,
final owner revocation, provider/context replacement, callback-created successor
state, early-return validation, rollback of only the current phase, exact original
errors, multi-record response ownership, authorized retries and unbound legacy
routing, including the preserved empty-response no-op. Repeated optimization modes are not additional unique tests. Assertions
inspect runtime results and postimages, not source strings.

These passes do **not** establish native Realm freezing, actual registry/model
semantics, native transaction scheduling, full BigSyncKit compilation, CloudKit
convergence or application acceptance. Retain the prior native histories in
`SyncUndoCloseoutW1EvidenceFenceTests.swift`; the seven additional file-backed
methods in `SyncDisappearanceOwnerBoundaryTests.swift` require source registration,
discovery and execution in the composed native batch. See
`Documentation/disappearance-owner-boundary-20261006.md` for exact scope/results.
