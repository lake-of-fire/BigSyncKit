#!/usr/bin/env bash
set -euo pipefail

root="${BIGSYNC_FROZEN_SOURCE_ROOT:?Set the absolute frozen BigSyncKit checkout path}"
[[ "$root" = /* ]] || { echo 'Frozen source root must be absolute' >&2; exit 2; }
test "$(git -C "$root" rev-parse HEAD)" = 7658320bf1b26090f471afe0481012197dbbc0ed
test "$(git -C "$root" rev-parse HEAD^{tree})" = 33006860211d522f05e640595df5347d380c5d6b
evidence="${BIGSYNC_ASSET_EVIDENCE_DIRECTORY:?Set a fresh absolute evidence directory}"
[[ "$evidence" = /* ]] || { echo 'Evidence directory must be absolute' >&2; exit 2; }
[[ ! -e "$evidence" ]] || { echo "Evidence directory must be fresh: $evidence" >&2; exit 2; }
mkdir -p "$evidence"
cp "$0" "$evidence/executed-graph-lane.sh"
shasum -a 256 "$evidence/executed-graph-lane.sh" > "$evidence/executed-graph-lane.sh.sha256"
scratch="$root/.build-owner-qualification"
scratch_created=0
finalize() {
  local status=$?
  trap - EXIT
  if [[ -f "$root/Package.resolved" ]]; then
    cp "$root/Package.resolved" "$evidence/Package.resolved" || status=1
  fi
  if [[ "$scratch_created" == 1 && -f "$scratch/debug.yaml" ]]; then
    cp "$scratch/debug.yaml" "$evidence/scratch-debug.yaml" || status=1
  fi
  if [[ -f "$root/.build/debug.yaml" ]]; then
    cp "$root/.build/debug.yaml" "$evidence/package-debug.yaml" || status=1
  fi
  if [[ "$scratch_created" == 1 && -d "$scratch" && "$scratch" == "$root/.build-owner-qualification" ]]; then
    rm -rf -- "$scratch" || status=1
  fi
  git -C "$root" status --porcelain=v1 --untracked-files=all > "$evidence/dirty-status-after.txt" || status=1
  if [[ -s "$evidence/dirty-status-after.txt" ]]; then status=1; fi
  printf '%s\n' "$status" > "$evidence/overall-status.txt"
  exit "$status"
}
trap finalize EXIT
[[ ! -e "$scratch" ]] || { echo "SwiftPM scratch path must be fresh: $scratch" >&2; exit 2; }


# Capture exact source and dependency identities before invoking SwiftPM.
git -C "$root" rev-parse HEAD > "$evidence/commit.txt"
git -C "$root" rev-parse HEAD^{tree} > "$evidence/tree-sha.txt"
git -C "$root" ls-tree -r HEAD > "$evidence/source-tree.txt"
git -C "$root" ls-tree -r HEAD -- Sources/BigSyncKit Tests/BigSyncKitTests .github/verify_w1_execution.py .github/test-asset-decoding.sh Package.swift Package.resolved > "$evidence/source-test-verifier-tree.txt"

git -C "$root" status --porcelain=v1 --untracked-files=all > "$evidence/dirty-status.txt"
test ! -s "$evidence/dirty-status.txt" || { echo 'BigSyncKit checkout is dirty' >&2; exit 1; }
for repo in "$root"/../RealmSwiftGaps "$root"/../SwiftUtilities; do
  test -d "$repo/.git" || { echo "Missing dependency checkout: $repo" >&2; exit 1; }
  label="${repo##*/}"
  git -C "$repo" status --porcelain=v1 --untracked-files=all > "$evidence/$label-dirty-status.txt"
  test ! -s "$evidence/$label-dirty-status.txt" || { echo "Dependency checkout is dirty: $repo" >&2; exit 1; }
done
git -C "$root/../RealmSwiftGaps" rev-parse HEAD > "$evidence/realm-gaps-commit.txt"
git -C "$root/../SwiftUtilities" rev-parse HEAD > "$evidence/swift-utilities-commit.txt"
test "$(cat "$evidence/realm-gaps-commit.txt")" = 395e3f005b0e178440b3c8e8be8c8b749c58b32c
test "$(cat "$evidence/swift-utilities-commit.txt")" = f437c7d06fc631cd7a67731279411c417cdf8077
for spec in "$root:BigSyncKit" "$root/../RealmSwiftGaps:RealmSwiftGaps" "$root/../SwiftUtilities:SwiftUtilities"; do
  repo="${spec%%:*}"; label="${spec#*:}"
  git -C "$repo" archive --format=tar HEAD > "$evidence/$label.tar" || exit 1
  shasum -a 256 "$evidence/$label.tar" > "$evidence/$label.tar.sha256"
done
mkdir -p "$scratch"
scratch_created=1

{
  printf 'DEVELOPER_DIR=%s\n' "${DEVELOPER_DIR-}"
  sw_vers
  xcodebuild -version
  printf 'MACOSX_DEPLOYMENT_TARGET=%s\n' "${MACOSX_DEPLOYMENT_TARGET-}"
  printf 'BIGSYNC_RUN_MUTATION_BENCHMARK=%s\n' "${BIGSYNC_RUN_MUTATION_BENCHMARK-}"
  printf 'SWIFT_BACKTRACE=%s\n' "${SWIFT_BACKTRACE-}"
  for tool in swift swiftc clang; do
    tool_path="$(xcrun --find "$tool")"
    printf 'xcrun --find %s: %s\n' "$tool" "$tool_path"
    case "$tool_path" in "$DEVELOPER_DIR"/*) ;; *) echo "$tool is not from selected DEVELOPER_DIR" >&2; exit 1 ;; esac
  done
  xcrun swift --version
  xcrun swiftc --version
  xcrun clang --version
  xcrun --show-sdk-path
  xcrun --sdk macosx --show-sdk-version
  xcrun --sdk macosx --show-sdk-build-version
  xcode-select -p
  xcsift --version
} > "$evidence/toolchain.txt" 2>&1 || { cat "$evidence/toolchain.txt"; exit 1; }
grep -q "Xcode 26.1.1" "$evidence/toolchain.txt" || { echo "Expected Xcode 26.1.1" >&2; exit 1; }
grep -q "Build version 17B100" "$evidence/toolchain.txt" || { echo "Expected Xcode build 17B100" >&2; exit 1; }
grep -q "Apple Swift version 6.2.1" "$evidence/toolchain.txt" || { echo "Expected Swift 6.2.1" >&2; exit 1; }
grep -q "/Xcode_26.1.1.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/clang" "$evidence/toolchain.txt" || { echo "Expected clang from selected Xcode 26.1.1 toolchain" >&2; exit 1; }
grep -q "^26.1$" "$evidence/toolchain.txt" || { echo "Expected macOS 26.1 SDK" >&2; exit 1; }
test "${MACOSX_DEPLOYMENT_TARGET-}" = "15.0" || { echo "Expected macOS deployment target 15.0" >&2; exit 1; }
test "${BIGSYNC_RUN_MUTATION_BENCHMARK-}" = "0" || { echo "Expected mutation benchmark disabled" >&2; exit 1; }
python3 - "$evidence/toolchain.txt" <<'PYVER'
import re, sys
s = open(sys.argv[1], encoding="utf-8").read()
m = re.search(r"ProductVersion:\s*([0-9]+)\.([0-9]+)", s)
if not m or tuple(map(int, m.groups())) < (15, 6):
    raise SystemExit("macOS runner must be at least 15.6")
PYVER
xcrun swift test --package-path "$root" --help > "$evidence/swift-test-help.txt" 2>&1 || exit 1
printf 'No compiler workaround flags requested\n' > "$evidence/swift-flags.txt"
set +e
xcrun swift test --package-path "$root" --scratch-path "$scratch" --configuration debug \
  list --verbose --disable-swift-testing 2>&1 | tee "$evidence/discovery.log" \
  | xcsift -f json --exit-on-failure > "$evidence/discovery.formatted.json"
statuses=("${PIPESTATUS[@]}")
set -e
printf '[%s,%s,%s]\n' "${statuses[0]}" "${statuses[1]}" "${statuses[2]}" > "$evidence/discovery.status.json"
if python3 "$root/.github/verify_w1_execution.py" discovery --directory "$evidence"; then discovery_result=0; else discovery_result=$?; fi

focused_filter="$(python3 "$root/.github/verify_w1_execution.py" focused-filter)"
printf '%s\n' "$focused_filter" > "$evidence/focused-filter.txt"
run_phase() {
  phase="$1"; shift
  set +e
  xcrun swift test --package-path "$root" --scratch-path "$scratch" --configuration debug \
    --skip-build --verbose --parallel --num-workers 1 --disable-swift-testing \
    --xunit-output "$evidence/$phase.junit.xml" "$@" 2>&1 \
    | tee "$evidence/$phase.log" \
    | xcsift -f json --exit-on-failure > "$evidence/$phase.formatted.json"
  statuses=("${PIPESTATUS[@]}")
  set -e
  printf '[%s,%s,%s]\n' "${statuses[0]}" "${statuses[1]}" "${statuses[2]}" > "$evidence/$phase.status.json"
  if python3 "$root/.github/verify_w1_execution.py" "$phase" --directory "$evidence"; then return 0; else return $?; fi
}
if run_phase full; then full_result=0; else full_result=$?; fi
if run_phase focused --filter "$focused_filter"; then focused_result=0; else focused_result=$?; fi

if [[ "$discovery_result" != 0 || "$full_result" != 0 || "$focused_result" != 0 ]]; then
  exit 1
fi
exit 0
