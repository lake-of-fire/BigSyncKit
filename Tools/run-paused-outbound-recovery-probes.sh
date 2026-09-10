#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/Sources/BigSyncKit" "$work/Tests/PausedRecoveryTests"
for name in BigSyncFileSystem BigSyncOutboundQuiescence CloudKitSynchronizer+PausedOutboundRecovery; do
  cp "$root/Sources/BigSyncKit/QSSynchronizer/$name.swift" "$work/Sources/BigSyncKit/"
done
cp "$root/Tools/PausedOutboundRecoveryProbe/Collaborators.swift" "$work/Sources/BigSyncKit/"
cp "$root/Tools/PausedOutboundRecoveryProbe/PausedRecoveryTests.swift" "$work/Tests/PausedRecoveryTests/"
cat > "$work/Package.swift" <<'SWIFT'
// swift-tools-version: 6.0
import PackageDescription
let package = Package(
    name: "PausedOutboundRecoveryProbe",
    products: [.library(name: "BigSyncKit", targets: ["BigSyncKit"])],
    targets: [
        .target(name: "BigSyncKit"),
        .testTarget(name: "PausedRecoveryTests", dependencies: ["BigSyncKit"]),
    ]
)
SWIFT
swift test --package-path "$work" "$@"
