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
