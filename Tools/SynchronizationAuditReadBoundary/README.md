# Synchronization audit read boundary

Run `bash Tools/SynchronizationAuditReadBoundary/run.sh`. Add `--configuration release` for optimized **portable** execution, not application Release qualification. Swift 6 is required. The runner creates and removes a temporary package and never resolves external dependencies or accesses app data.

The runner copies the complete production `BigSyncSynchronizationAudit.swift` and unchanged `BigSyncRecordEvidenceInspection.swift` from the selected checkout. Eighteen XCTest methods exercise their outputs against committed/provisional state, later commits, refresh reentry, account-scoped tracking, relationship/quarantine/submission debt, schema aliases and existing error/encoding behavior. Expected results come from a prior actual audit of the committed fixture, not assertions on source text.

The Realm module is an explicit in-memory MVCC/collection collaborator. CloudKit encoding, schema registration, field fingerprints, transport, setup and archive validation have declared stand-ins. The actual adapter's three eligibility functions are retained as unchanged source excerpts; target identity parsing and record decoding remain collaborators. The late-comparison callback is a test collaborator, not a claimed native SDK hook. These modules live only under Tools, outside the real package's native targets.

This suite does not qualify native Realm snapshot semantics, thread confinement, actor scheduling, disk durability, CloudKit/account lifecycle, a globally atomic cross-file cut, the full adapter, or the Reader application. Eight corresponding native W1 methods in the existing `SyncUndoCloseoutW1EvidenceFenceTests.swift` must be discovered and executed in the selected native composition.
