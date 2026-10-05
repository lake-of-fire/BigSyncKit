#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-disappearance.XXXXXX")"
cleanup() {
    if [[ -d "$scratch" ]]; then
        mkdir -p "$HOME/.Trash"
        local trash_directory
        trash_directory="$(mktemp -d "$HOME/.Trash/bigsync-scratch.XXXXXX")"
        mv "$scratch" "$trash_directory/"
    fi
}
trap cleanup EXIT
cp "$here/Package.swift" "$scratch/Package.swift"
cp -R "$here/Sources" "$here/Tests" "$scratch/"
# Compile the complete selected production file, never a copied implementation.
cp "$root/Sources/BigSyncKit/RealmSwift/BigSyncRecordDisappearance.swift" \
    "$scratch/Sources/BigSyncKit/BigSyncRecordDisappearance.swift"
# Existing DEBUG suspension hooks are required in BOTH optimization modes.
# This is a portable read-boundary check, not native SDK or app qualification.
swift test --package-path "$scratch" --jobs 1 \
    -Xswiftc -DDEBUG -Xswiftc -warnings-as-errors "$@"
