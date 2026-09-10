#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# Compile async entry points at the same deployment minimum as Package.swift.
# Use positional flags rather than an empty array (macOS ships Bash 3.2).
case "$(uname -s)" in
  Darwin) set -- -target "$(uname -m)-apple-macosx15.0" ;;
  *) set -- ;;
esac
swiftc "$@" -parse-as-library -o "$work/probe" \
  "$root/Sources/BigSyncKit/QSSynchronizer/BigSyncFileSystem.swift" \
  "$root/Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift" \
  "$root/Tools/OutboundQuiescence/ProcessProbe.swift"
python3 "$root/Tools/OutboundQuiescence/test_processes.py" "$work/probe"
