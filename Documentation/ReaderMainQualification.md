# Reader-main canonical BigSync qualification

Canonical BigSyncKit main is `9cee5e112a6c3ca5cf530b3bce3d467f818b77f3`.

Reader main currently selects `e31100998ed6d5c0a515eebc29ddb9acab3090ee`.
Reader v3-hotfix selects `3b126083559226f25fd486b67b7e056d1f515340`.

The hotfix selection is an ancestor of canonical main. Reader main has two historical divergent commits:
- bounded lifecycle synchronization;
- backup/projection coordination.

Current canonical main contains newer implementations of those responsibilities through worker deadline/cancellation ownership, durable committed inbound identity delivery, pending projection debt, and expanded backup/rebuild/account fencing.

Canonical BigSync requires RealmSwift `from: 20.0.5`. Reader #214 pairs this child head with Reader's exact Realm 20.0.5 Tuist selection.

This qualification PR changes no runtime source. It runs the complete BigSync package tests in Debug and Release on macOS. Reader/Core consumer compilation and signed CloudKit journeys remain separate gates.


## Sibling path dependencies

BigSyncKit uses local sibling packages for `RealmSwiftGaps` and `SwiftUtilities`. The first public qualification attempt failed before compiling BigSync because those directories were absent.

The workflow now checks out:
- Reader #214's exact RealmSwiftGaps selection `3c0ccf00d734cae997bd34654a4f2efd01397531`;
- public SwiftUtilities #1 `475973026ca1e953d016ff78e65c0be5bb4885d5`.

Reader #214's vendored SwiftUtilities production tree differs from public #1 only in `StableHash.swift`; BigSyncKit does not reference StableHash. All other production files used by BigSync match, so this sibling selection does not strengthen a BigSync-consumed utility contract relative to Reader #214.
