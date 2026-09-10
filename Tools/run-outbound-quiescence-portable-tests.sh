#!/usr/bin/env bash
# Compiles unmodified production filesystem/gate sources and the same XCTest
# file used by the native package. This is NOT CloudKit/Realm qualification.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/Sources/BigSyncKit" "$work/Tests/BigSyncKitTests"
for file in BigSyncFileSystem.swift BigSyncOutboundQuiescence.swift; do
  cp "$root/Sources/BigSyncKit/QSSynchronizer/$file" "$work/Sources/BigSyncKit/"
done
cp "$root/Tests/BigSyncKitTests/BigSyncOutboundQuiescenceTests.swift" "$work/Tests/BigSyncKitTests/"
cat > "$work/Package.swift" <<'PACKAGE'
// swift-tools-version: 5.9
import PackageDescription
let package = Package(name: "BigSyncOutboundQuiescencePortable", targets: [
    .target(name: "BigSyncKit"),
    .testTarget(name: "BigSyncKitTests", dependencies: ["BigSyncKit"])
])
PACKAGE
swift test --package-path "$work"
