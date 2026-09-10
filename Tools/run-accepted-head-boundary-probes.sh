#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-accepted-head.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/Sources/AcceptedHeadBoundary" "$WORK/Tests/AcceptedHeadBoundaryTests"
cat > "$WORK/Package.swift" <<'EOF'
// swift-tools-version: 6.0
import PackageDescription
let package = Package(name: "AcceptedHeadBoundary", platforms: [.macOS(.v15)],
    targets: [.target(name: "AcceptedHeadBoundary"),
              .testTarget(name: "AcceptedHeadBoundaryTests", dependencies: ["AcceptedHeadBoundary"])])
EOF
SOURCE="$ROOT/Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+AcceptedHeadQuiescence.swift"
cp "$SOURCE" "$WORK/Sources/AcceptedHeadBoundary/"
cp "$ROOT/Tools/AcceptedHeadQuiescence/Collaborators.swift" "$WORK/Sources/AcceptedHeadBoundary/"
cp "$ROOT/Tools/AcceptedHeadQuiescence/AcceptedHeadBoundaryTests.swift" "$WORK/Tests/AcceptedHeadBoundaryTests/"
cmp "$SOURCE" "$WORK/Sources/AcceptedHeadBoundary/$(basename "$SOURCE")"
swift test --package-path "$WORK" "$@"
