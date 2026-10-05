#!/usr/bin/env bash
set -euo pipefail
tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$tool_root/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-audit-read.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
cp "$tool_root/Package.swift" "$scratch/"
cp -R "$tool_root/Sources" "$tool_root/Tests" "$scratch/"
# Compile the complete selected production sources, never a second audit model.
cp "$root/Sources/BigSyncKit/RealmSwift/BigSyncSynchronizationAudit.swift" "$scratch/Sources/BigSyncKit/"
cp "$root/Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidenceInspection.swift" "$scratch/Sources/BigSyncKit/"
swift test --package-path "$scratch" --jobs 1 -Xswiftc -warnings-as-errors "$@"
