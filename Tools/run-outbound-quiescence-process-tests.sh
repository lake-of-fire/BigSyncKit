#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
swiftc -parse-as-library -o "$work/probe" \
  "$root/Sources/BigSyncKit/QSSynchronizer/BigSyncFileSystem.swift" \
  "$root/Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift" \
  "$root/Tools/OutboundQuiescence/ProcessProbe.swift"
python3 "$root/Tools/OutboundQuiescence/test_processes.py" "$work/probe"
