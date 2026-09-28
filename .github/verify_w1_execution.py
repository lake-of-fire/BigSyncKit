"""Check named XCTest discovery/execution in the existing W1 native packet.

Serial SwiftPM may omit --xunit-output. Read actual XCTest start/terminal records
instead; aggregate counts or an exit-zero empty selection cannot qualify a case.
This verifies a runner packet, not an application or signed CloudKit release.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
from typing import Sequence

EXPECTED = {
    "SyncUndoCloseoutW1Tests": (
        "testOmittedScalarsApplyDeclaredDefaultsAndAgreeWithBaseline",
        "testTerminalLocalDeleteRetiresItsSupersededStagedSave",
    ),
    "CloudKitSynchronizerAccountFencingTests": (
        "testCancelledWorkerPreflightDoesNotScheduleRetry",
    ),
    "CloudKitAccountAvailabilityCancellationTests": (
        "testAlreadyCancelledRequestDoesNotInvokeStatusProvider",
        "testCancellationReturnsBeforeNonCooperativeProviderFinishes",
        "testCancellingOneRequestDoesNotCancelAnotherRequest",
        "testCancelledRequestDoesNotPoisonSubsequentUseOfGate",
    ),
    "BigSyncWorkerRequestCancellationTests": (
        "testAlreadyCancelledRequestPreservesScheduledStartup",
        "testAlreadyCancelledDeadlineRequestPreservesScheduledStartup",
        "testAlreadyCancelledRequestPreservesLiveRetry",
        "testAlreadyCancelledDeadlineRequestPreservesLiveRetry",
        "testLiveExplicitRequestStillSupersedesScheduledStartup",
        "testCancelledRestorationWaiterDoesNotEnterPreflight",
        "testCancellingOneWaiterPreservesRestorationAndLiveWaiter",
    ),
}
REQUIRED = tuple(f"{suite}/{method}" for suite, methods in EXPECTED.items() for method in methods)
IDENTIFIER = r"[A-Za-z_][A-Za-z_0-9]*"
DISCOVERY = re.compile(rf"BigSyncKitTests\.({IDENTIFIER})/({IDENTIFIER})")
APPLE_NAME = re.compile(rf"-\[(?:BigSyncKitTests\.)?({IDENTIFIER}) ({IDENTIFIER})\]")
SWIFT_NAME = re.compile(rf"(?:BigSyncKitTests\.)?({IDENTIFIER})\.({IDENTIFIER})")
EVENT = re.compile(
    r"Test Case '([^']+)' (started at .+|(?:passed|failed|skipped) \([0-9.]+ seconds\)\.?)"
)


def validate(
    discovery: str, statuses: Sequence[int], execution: str | None = None
) -> dict:
    errors = []
    if len(statuses) != 3 or any(type(code) is not int or code != 0 for code in statuses):
        errors.append("swift/tee/xcsift must each exit zero")
    listed = Counter()
    for line in discovery.splitlines():
        match = DISCOVERY.fullmatch(line.strip())
        if match:
            listed["/".join(match.groups())] += 1
    for name in REQUIRED:
        if listed[name] != 1:
            errors.append(f"{name}: expected one discovery, found {listed[name]}")

    records = defaultdict(list)
    if execution is not None:
        for line in execution.splitlines():
            event = EVENT.fullmatch(line)
            if not event:
                continue
            name = APPLE_NAME.fullmatch(event[1]) or SWIFT_NAME.fullmatch(event[1])
            if not name:
                continue
            records["/".join(name.groups())].append(event[2].split(" ", 1)[0])
        for name in REQUIRED:
            if records[name] != ["started", "passed"]:
                errors.append(f"{name}: expected one start followed by one pass; got {records[name]}")
        for name, states in records.items():
            if "failed" in states:
                errors.append(f"{name}: execution contains a failed case")

    return {
        "expected": list(REQUIRED),
        "discovered": [name for name in REQUIRED if listed[name] == 1],
        "passed_cases": [name for name in REQUIRED if records[name] == ["started", "passed"]],
        "skipped_cases": sorted(name for name, states in records.items() if "skipped" in states),
        "pipeline_exit_codes": list(statuses),
        "errors": errors,
        "passed": not errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("discovery", "full", "focused"))
    parser.add_argument("--directory", type=Path, default=Path("qualification-w1"))
    args = parser.parse_args(argv)
    report = {
        "schema_version": 1,
        "plan": "MR-UNDO-CLOSEOUT-20260920",
        "phase": args.phase,
        "assembled_application_qualified": False,
        "signed_cloudkit_qualified": False,
        "input_sha256": {},
        "passed": False,
    }

    def read(name: str) -> str:
        data = (args.directory / name).read_bytes()
        report["input_sha256"][name] = hashlib.sha256(data).hexdigest()
        return data.decode("utf-8")

    try:
        commit = read("commit.txt").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise ValueError("invalid recorded source commit")
        report["commit"] = commit
        statuses = json.loads(read(f"{args.phase}.status.json"))
        if not isinstance(statuses, list):
            raise ValueError("pipeline statuses must be a three-integer JSON array")
        discovery = read("discovery.log")
        execution = None if args.phase == "discovery" else read(f"{args.phase}.log")
        report.update(validate(discovery, statuses, execution))
        if execution is not None:
            discovery_statuses = json.loads(read("discovery.status.json"))
            if not isinstance(discovery_statuses, list):
                raise ValueError("invalid discovery pipeline statuses")
            report["discovery_pipeline_exit_codes"] = discovery_statuses
            if not validate(discovery, discovery_statuses)["passed"]:
                report["errors"].append("discovery pipeline did not pass")
                report["passed"] = False
    except (OSError, UnicodeError, ValueError) as error:
        report["passed"] = False
        report["errors"] = [f"invalid or missing runner input: {error}"]
    output = json.dumps(report, indent=2, sort_keys=True) + "\n"
    (args.directory / f"{args.phase}-identity.json").write_text(output, encoding="utf-8")
    print(output, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
