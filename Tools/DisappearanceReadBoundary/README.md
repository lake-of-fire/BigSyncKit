# Portable disappearance read-boundary tests

Run `bash Tools/DisappearanceReadBoundary/run.sh` from a checkout with Swift 6.
Add `--configuration release` for an optimized **portable harness**, not native
application Release qualification. Existing DEBUG suspension hooks remain enabled
in both modes. There is no package fetch, network operation or app-data access.

The runner copies the complete current production
`Sources/BigSyncKit/RealmSwift/BigSyncRecordDisappearance.swift` into a temporary
package. It compiles that file with explicit CloudKit, Realm and adapter/domain
collaborators. The Realm collaborator has separate committed/provisional versions,
freeze and refresh-callback behavior. It intentionally does not simulate the real
SDK's actor scheduling, queued admission, file lifetime, durability or CloudKit.
The tests exercise the production preparation, reconciliation and publication
methods, not a second disappearance algorithm. Assertions inspect runtime results
and postimages, never source text. All collaborators are outside native targets.

The 15 methods cover provisional generation/revision/resurrection isolation,
provisional tombstone and acknowledgement exclusion, committed deletion evidence,
tracking repair resampling, live CAS rejection, legacy tracking recovery, refresh
reentry, transport replacement and unexplained committed live targets.

These passes do **not** establish native Realm.freeze semantics, real registry or
account correctness, full BigSyncKit compilation, asynchronous write ownership,
CloudKit convergence or application acceptance. The nine new file-backed methods
in `SyncUndoCloseoutW1EvidenceFenceTests.swift` complement these source tests and
must be discovered and executed in the composed native batch.
