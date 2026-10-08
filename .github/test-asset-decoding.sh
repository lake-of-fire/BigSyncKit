#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
evidence="${BIGSYNC_ASSET_EVIDENCE_DIRECTORY:?Set a fresh absolute evidence directory}"
mkdir -p "$evidence"
git -C "$root" rev-parse HEAD > "$evidence/source-commit.txt"
git -C "$root" ls-tree -r HEAD > "$evidence/source-tree.txt"
swift --version > "$evidence/swift-version.txt"

set +e
swift test --package-path "$root" --configuration debug \
  --filter HotfixCollectionSafetyTests \
  --xunit-output "$evidence/native.junit.xml" 2>&1 | tee "$evidence/native.log"
statuses=("${PIPESTATUS[@]}")
set -e
printf '%s\n' "${statuses[*]}" > "$evidence/pipeline-statuses.txt"
if [[ "${statuses[0]}" != 0 || "${statuses[1]}" != 0 ]]; then
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
