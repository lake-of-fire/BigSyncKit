#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compiler_arguments=()
if command -v xcrun >/dev/null 2>&1; then
    compiler="$(xcrun --find swiftc)"
    # Apple toolchains need the selected SDK explicitly for a standalone host tool.
    compiler_arguments=(-sdk "$(xcrun --sdk macosx --show-sdk-path)")
else
    compiler="$(command -v swiftc)"
fi
host="$(dirname "$compiler")/../lib/swift/host"
if [[ ! -d "$host/SwiftParser.swiftmodule" || ! -d "$host/SwiftSyntax.swiftmodule" ]]; then
    echo 'Swift 6 toolchain host SwiftParser/SwiftSyntax modules are required; refusing to skip.' >&2
    exit 1
fi
work="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-owned-writes.XXXXXX")"
preserve_scratch() {
    mkdir -p "$HOME/.Trash"
    mv "$work" "$HOME/.Trash/"
}
trap preserve_scratch EXIT
"$compiler" --version
"$compiler" "${compiler_arguments[@]}" -swift-version 6 -I "$host" -L "$host" \
    -lSwiftParser -lSwiftSyntax -Xlinker -rpath -Xlinker "$host" \
    "$root/.github/check-owned-writes.swift" -o "$work/check-owned-writes"
if [[ $# -eq 0 ]]; then
    "$work/check-owned-writes" "$root"
else
    "$work/check-owned-writes" "$@"
fi
