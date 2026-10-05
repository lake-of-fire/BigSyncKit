#!/usr/bin/env bash
set -euo pipefail
tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$tool_root/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-audit-read.XXXXXX")"
cleanup() {
    if [[ -d "$scratch" ]]; then
        mkdir -p "$HOME/.Trash"
        mv "$scratch" "$HOME/.Trash/$(basename "$scratch")-$(uuidgen)"
    fi
}
trap cleanup EXIT
cp "$tool_root/Package.swift" "$scratch/"
cp -R "$tool_root/Sources" "$tool_root/Tests" "$scratch/"
# Compile the complete selected production sources, never a second audit model.
cp "$root/Sources/BigSyncKit/RealmSwift/BigSyncSynchronizationAudit.swift" "$scratch/Sources/BigSyncKit/"
cp "$root/Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidenceInspection.swift" "$scratch/Sources/BigSyncKit/"
swift test --package-path "$scratch" --jobs 1 -Xswiftc -warnings-as-errors "$@"
