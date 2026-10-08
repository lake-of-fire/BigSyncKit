#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
evidence="${BIGSYNC_ASSET_EVIDENCE_DIRECTORY:?Set a fresh absolute evidence directory}"
[[ "$evidence" = /* ]] || { echo 'Evidence directory must be absolute' >&2; exit 1; }
mkdir "$evidence"
scratch="${evidence}-build"
[[ ! -e "$scratch" ]] || { echo 'Build scratch directory must be fresh' >&2; exit 1; }
export BIGSYNC_RUN_MUTATION_BENCHMARK=0
finish() {
  local status=$?
  trap - EXIT
  if [[ -f "$root/Package.resolved" ]]; then
    cp "$root/Package.resolved" "$evidence/Package.resolved" || status=1
  fi
  if [[ -f "$scratch/debug.yaml" ]]; then
    cp "$scratch/debug.yaml" "$evidence/build-plan.yaml" || status=1
  fi
  printf '%s\n' "$status" > "$evidence/overall-status.txt"
  exit "$status"
}
trap finish EXIT
git -C "$root" rev-parse HEAD > "$evidence/source-commit.txt"
git -C "$root" ls-tree -r HEAD > "$evidence/source-tree.txt"
swift --version > "$evidence/swift-version.txt"
xcrun clang --version > "$evidence/apple-clang-version.txt"
xcrun --show-sdk-path > "$evidence/sdk-path.txt"
xcsift --version > "$evidence/xcsift-version.txt"
git -C "$root/../RealmSwiftGaps" rev-parse HEAD > "$evidence/realm-gaps-commit.txt"
git -C "$root/../SwiftUtilities" rev-parse HEAD > "$evidence/swift-utilities-commit.txt"
test "$(cat "$evidence/realm-gaps-commit.txt")" = 1ffbedbb3d8dd90f44f651f618128e7806ce39dd
test "$(cat "$evidence/swift-utilities-commit.txt")" = f437c7d06fc631cd7a67731279411c417cdf8077
printf '%s\n' 'Package Debug collection/asset selection only.' \
  'Existing opt-in mutation performance benchmark is excluded.' \
  'Mac UI, performance, signed macOS CloudKit and Release remain owner-deferred.' \
  > "$evidence/scope.txt"
swift test --help > "$evidence/swift-test-help.txt"
swift_test=(swift test --package-path "$root" --scratch-path "$scratch")
if rg -q -- '--disable-experimental-prebuilts' "$evidence/swift-test-help.txt"; then
  swift_test+=(--disable-experimental-prebuilts)
fi

set +e
# Use a clean package scratch and retain every pipeline status.
# PR143 run 37814850792 stopped in Realm Core 20.1.5 geospatial.cpp before
# executing tests. Compile its C++ headers textually for this package runner;
# this does not change the Reader app's build configuration.
"${swift_test[@]}" --configuration debug \
  -Xcxx -fno-modules \
  --filter HotfixCollectionSafetyTests \
  --parallel --num-workers 1 --disable-swift-testing \
  --xunit-output "$evidence/native.junit.xml" 2>&1 | tee "$evidence/native.log" \
  | xcsift -f json --exit-on-failure > "$evidence/native.formatted.json"
statuses=("${PIPESTATUS[@]}")
set -e
printf '%s\n' "${statuses[*]}" > "$evidence/pipeline-statuses.txt"
if [[ "${statuses[0]}" != 0 || "${statuses[1]}" != 0 || "${statuses[2]}" != 0 ]]; then
  python3 - "$evidence/native.formatted.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
raw = path.read_bytes() if path.exists() else b"No xcsift output was retained."
try:
    formatted = json.loads(raw)
    if isinstance(formatted, dict) and formatted.get("errors"):
        formatted = {"errors": formatted["errors"], "summary": formatted.get("summary")}
    raw = json.dumps(formatted, indent=2, ensure_ascii=False).encode()
except (ValueError, UnicodeError):
    pass
prefix = b"Bounded xcsift failure summary (full formatted output retained in evidence):\n"
sys.stdout.buffer.write((prefix + raw)[:16000] + b"\n")
PY
  exit 1
fi

python3 - "$evidence/native.junit.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET
from collections import Counter

cases = list(ET.parse(sys.argv[1]).iter("testcase"))
required = {
    "testAssetInScalarFieldRejectsNewReceiverBeforeRealmAssignment",
    "testAssetInScalarFieldRollsBackExistingValueAndTracking",
    "testComparisonDecoderRejectsAssetInScalarField",
    "testReadableDataAssetsDecodeAndMissingFilesRollBack",
}
def method_name(case):
    name = case.attrib.get("name", "").removesuffix("()")
    return name.rsplit("/", 1)[-1].rsplit(".", 1)[-1]

counts = Counter(method_name(case) for case in cases)
assert cases, "No native methods executed"
assert all(counts[name] == 1 for name in required), counts
assert not any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")), "Native selection did not pass without skips"
print(f"Passed {len(cases)} native collection/asset methods, including all four required asset regressions")
PY
