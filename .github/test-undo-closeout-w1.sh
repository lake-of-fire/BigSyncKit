#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
evidence="${BIGSYNC_W1_EVIDENCE_DIRECTORY:?Set the existing absolute evidence directory}"
scratch="${BIGSYNC_W1_SCRATCH_DIRECTORY:?Set a fresh absolute scratch directory outside evidence}"
[[ "$evidence" = /* && "$scratch" = /* ]] || { echo 'Evidence and scratch paths must be absolute' >&2; exit 1; }
[[ -d "$evidence" && ! -L "$evidence" ]] || { echo 'Evidence directory must already exist without a redirect' >&2; exit 1; }
[[ "$scratch" != "$evidence" && "$scratch" != "$evidence/"* && ! -e "$scratch" && ! -L "$scratch" ]] || { echo 'Build scratch must be fresh and outside evidence' >&2; exit 1; }
export BIGSYNC_RUN_MUTATION_BENCHMARK=0
finish() {
  local status=$?
  trap - EXIT
  for package in BigSyncKit RealmSwiftGaps SwiftUtilities; do
    if [[ -f "$root/../$package/Package.resolved" ]]; then
      cp "$root/../$package/Package.resolved" "$evidence/$package-Package.resolved" || status=1
    fi
  done
  if [[ -f "$scratch/debug.yaml" ]]; then
    cp "$scratch/debug.yaml" "$evidence/build-plan.yaml" || status=1
  fi
  printf '%s\n' "$status" > "$evidence/overall-status.txt"
  exit "$status"
}
trap finish EXIT
printf '%s\n' 'Full Debug package discovery and execution, then the unchanged focused W1 selection.' \
  'BIGSYNC_RUN_MUTATION_BENCHMARK=0; only the existing explicit benchmark skip is allowed.' \
  'Owner-deferred: Mac UI, performance, signed macOS CloudKit and Release.' > "$evidence/scope.txt"
test "$(git -C "$root" rev-parse HEAD)" = "$(< "$evidence/commit.txt")"
test "$(git -C "$root/../RealmSwiftGaps" rev-parse HEAD)" = 1ffbedbb3d8dd90f44f651f618128e7806ce39dd
test "$(git -C "$root/../SwiftUtilities" rev-parse HEAD)" = f437c7d06fc631cd7a67731279411c417cdf8077
git -C "$root" hash-object "$root/.github/test-undo-closeout-w1.sh" > "$evidence/runner-blob.txt"
printf '%s\n' "$scratch" > "$evidence/scratch-path.txt"
swift test --help > "$evidence/swift-test-help.txt"
swift_test=(swift test --package-path "$root" --scratch-path "$scratch" --configuration debug)
if [[ "$(< "$evidence/swift-test-help.txt")" == *--disable-experimental-prebuilts* ]]; then
  swift_test+=(--disable-experimental-prebuilts)
fi
# Realm Core's C++20 frontend default also needs disabling, as proven by the
# clean collection/asset package run. Do not disable geospatial support or tests.
swift_test+=(-Xcxx -fno-modules -Xcxx -Xclang -Xcxx -fno-cxx-modules)

run_logged() {
  local name="$1"
  shift
  local statuses
  [[ ! -e "$evidence/$name.log" && ! -e "$evidence/$name.status.json" ]] || return 1
  set +e
  "$@" 2>&1 | tee "$evidence/$name.log" | xcsift -f json --exit-on-failure > "$evidence/$name.formatted.json"
  statuses=("${PIPESTATUS[@]}")
  set -e
  printf '[%s, %s, %s]\n' "${statuses[0]}" "${statuses[1]}" "${statuses[2]}" > "$evidence/$name.status.json"
  if [[ "${statuses[0]}" != 0 || "${statuses[1]}" != 0 || "${statuses[2]}" != 0 ]]; then
    python3 - "$evidence/$name.formatted.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
raw = path.read_bytes() if path.exists() else b"No xcsift output retained."
try:
    formatted = json.loads(raw)
    if isinstance(formatted, dict) and formatted.get("errors"):
        formatted = {"errors": formatted["errors"], "summary": formatted.get("summary")}
    raw = json.dumps(formatted, indent=2, ensure_ascii=False).encode()
except (ValueError, UnicodeError):
    pass
sys.stdout.buffer.write((b"Bounded xcsift failure summary:\n" + raw)[:16000] + b"\n")
PY
  fi
}
verify_phase() {
  local phase="$1"
  local status=0
  python3 "$root/.github/verify_w1_execution.py" "$phase" --directory "$evidence" > "$evidence/$phase-verifier.log" 2>&1 || status=$?
  printf '%s\n' "$status" > "$evidence/$phase-verifier-status.txt"
  python3 - "$evidence/$phase-identity.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
if not path.is_file():
    print("No phase identity report; retained verifier log records the setup failure.")
else:
    report = json.loads(path.read_text())
    print(json.dumps({
        "phase": report.get("phase"), "passed": report.get("passed"),
        "expected": len(report.get("expected", [])),
        "passed_cases": len(report.get("passed_cases", [])),
        "skipped_cases": report.get("skipped_cases", []),
        "pipeline_exit_codes": report.get("pipeline_exit_codes"),
        "error_count": len(report.get("errors", [])),
        "first_errors": report.get("errors", [])[:10],
    }, sort_keys=True)[:16000])
PY
  return "$status"
}
run_logged discovery "${swift_test[@]}" list
verify_phase discovery
full_status=0
run_logged full "${swift_test[@]}" --skip-build --xunit-output "$evidence/full.xml"
verify_phase full || full_status=$?
focused_status=0
focused_filter="$(python3 "$root/.github/verify_w1_execution.py" focused-filter)"
printf '%s\n' "$focused_filter" > "$evidence/focused-filter.txt"
run_logged focused "${swift_test[@]}" --skip-build --filter "$focused_filter" --xunit-output "$evidence/focused.xml"
verify_phase focused || focused_status=$?
printf 'full=%s\nfocused=%s\n' "$full_status" "$focused_status" > "$evidence/status.txt"
git -C "$root" diff --exit-code
test "$full_status" -eq 0 && test "$focused_status" -eq 0
