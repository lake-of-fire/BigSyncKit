#!/usr/bin/env bash
set -euo pipefail
# Actual deadline implementation and tests only; no Apple/worker qualification.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/bigsync-worker-deadline.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
output="${BIGSYNC_DEADLINE_TEST_OUTPUT:-$scratch/results}"
mkdir "$output"
output="$(cd "$output" && pwd)"
mkdir -p "$scratch/Sources/BigSyncKit" "$scratch/Tests/BigSyncKitTests"
cp "$root/Sources/BigSyncKit/QSSynchronizer/BigSyncDeadlineRace.swift" "$scratch/Sources/BigSyncKit/"
cp "$root/Tests/BigSyncKitTests/BigSyncWorkerRequestCancellationTests.swift" "$scratch/Tests/BigSyncKitTests/"
cat > "$scratch/Package.swift" <<'PACKAGE'
// swift-tools-version: 6.0
import PackageDescription
let package = Package(name: "WorkerDeadlineTests", targets: [
    .target(name: "BigSyncKit"),
    .testTarget(name: "BigSyncKitTests", dependencies: ["BigSyncKit"],
        swiftSettings: [.define("BIGSYNC_WORKER_DEADLINE_PORTABLE")])
])
PACKAGE
for mode in debug release; do
    swift build --package-path "$scratch" -c "$mode" --build-tests --jobs 4 \
        -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors -Xswiftc -enable-testing \
        > "$output/$mode-build.log" 2>&1
    swift test --package-path "$scratch" -c "$mode" list --skip-build --disable-swift-testing \
        > "$output/$mode-discovery.txt" 2> "$output/$mode-discovery.log"
    swift test --package-path "$scratch" -c "$mode" --skip-build --disable-swift-testing \
        --parallel --num-workers 2 --xunit-output "$output/$mode.xml" \
        > "$output/$mode-tests.log" 2>&1
    python3 - "$output" "$mode" <<'VERIFY'
from collections import Counter
import json, pathlib, sys, xml.etree.ElementTree as ET
root, mode = pathlib.Path(sys.argv[1]), sys.argv[2]
methods = (root / f'{mode}-discovery.txt').read_text().splitlines()
xml = ET.parse(root / f'{mode}.xml').getroot()
cases = list(xml.iter('testcase'))
actual = [f'{c.attrib["classname"]}/{c.attrib["name"]}' for c in cases]
assert len(methods) == len(set(methods)) == 24, 'Wrong deadline inventory'
assert all(m.startswith('BigSyncKitTests.BigSyncDeadlineRaceTests/') for m in methods)
assert Counter(methods) == Counter(actual), 'Executed identities differ from discovery'
assert not any(list(xml.iter(t)) for t in ('failure', 'error', 'skipped'))
for element in xml.iter():
    if element.tag in ('testcase', 'testsuite', 'testsuites'):
        assert element.get('status', 'run') == 'run'
        assert element.get('result', 'completed') == 'completed'
        for count in ('failures', 'errors', 'skipped', 'disabled'):
            assert int(element.get(count, '0')) == 0
result = dict(scope='actual deadline race only', native_qualification=False,
              passed=len(actual), failed=0, skipped=0, methods=sorted(methods))
(root / f'{mode}-verified.json').write_text(json.dumps(result, indent=2) + '\n')
print(mode, len(actual), 'actual deadline methods passed; native worker not executed')
VERIFY
done
