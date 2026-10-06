"""Check named XCTest discovery/execution in the existing W1 native packet.

Serial SwiftPM may omit --xunit-output. Read actual XCTest start/terminal records
instead; aggregate counts or an exit-zero empty selection cannot qualify a case.
A full run must account for every discovered XCTest, not only a hand-picked
critical subset. Focused execution and its SwiftPM filter share one selection.
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
        "testFetchedDeletionPageReplaysAfterTargetFirstInterruptionWithoutDeletingAgain",
    ),
    "CloudKitSynchronizerAccountFencingTests": (
        "testCancelledWorkerPreflightDoesNotScheduleRetry",
        "testReentrantFailureObserversPreserveOneSettlementSnapshot",
        "testFailureObserverSuccessorRetainsAttemptAndTask",
    ),
    "CloudKitAccountAvailabilityCancellationTests": (
        "testAlreadyCancelledRequestDoesNotInvokeStatusProvider",
        "testCancellationReturnsBeforeNonCooperativeProviderFinishes",
        "testCancellingOneRequestDoesNotCancelAnotherRequest",
        "testCancelledRequestDoesNotPoisonSubsequentUseOfGate",
    ),
    "CloudKitAccountAvailabilityGateTests": (
        "testInjectableAsyncStatusProviderIsUsed",
        "testAvailabilityGateReturnsFailedAtItsHardDeadline",
        "testCallbackBridgeHasAHardDeadlineAndIgnoresLateCompletion",
    ),
    "CloudKitAccountAvailabilityDeadlineTests": (
        "testZeroBudgetDoesNotStartProvider",
        "testLateAvailableCannotBeatDelayedTimer",
        "testExactDeadlineCannotPublishUnavailableStatus",
        "testOnTimeStatusSurvivesClockAdvanceDuringDelivery",
        "testDelayedTimerUsesOnlyRemainingBudget",
        "testExpiredProviderAdmissionStartsNoAccountRead",
        "testCancellationAtSettlementCannotDeliverAvailable",
        "testOnTimeStatusesArePreservedWithoutAccountCaching",
        "testExpiredCallDoesNotReuseItsDeadlineForNextCall",
    ),
    "CloudKitCallbackAdmissionTests": (
        "testPrecancelledCallerDoesNotRegisterCallback",
        "testZeroBudgetDoesNotRegisterCallback",
        "testRegistrationConsumesBudgetBeforeSynchronousCompletion",
        "testLateCallbackErrorBecomesDeadlineFailure",
        "testOnTimeBufferedValueSurvivesDelayedRegistrationReturn",
        "testOnTimeBufferedErrorSurvivesDelayedRegistrationReturn",
        "testCancellationDuringRegistrationDefeatsBufferedSuccess",
        "testCancellationDuringRegistrationDefeatsBufferedError",
        "testUnboundedCallDoesNotConsultClockAndKeepsFirstResult",
        "testOverflowSaturatesWithoutRejectingOnTimeCallback",
        "testCancellationWhileCallbackNeverArrivesReturnsAndIgnoresLateReply",
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
    "ChangeRequestProcessorCancellationTests": (
        "testValidatedBeginRunPrecancelledDoesNotResetCurrentRun",
        "testValidatedBeginRunCancellationDuringJoinLeavesProcessorStopped",
        "testValidatedBeginRunAuthorityFailureAfterJoinLeavesProcessorStopped",
    ),
    "SynchronizationProcessorStartupTests": (
        "testStartupCancelledDuringProcessorJoinDoesNotActivateRetiredContext",
        "testStartupInvalidatedDuringProcessorJoinDoesNotActivateRetiredContext",
        "testRunContextRejectsSynchronousAccountPoisonBeforeActorCancellation",
        "testAttemptCheckStillAllowsFreshValidationWhileFenceIsPoisoned",
    ),
}
REQUIRED = tuple(f"{suite}/{method}" for suite, methods in EXPECTED.items() for method in methods)
IDENTIFIER = r"[A-Za-z_][A-Za-z_0-9]*"
DISCOVERY = re.compile(rf"BigSyncKitTests\.({IDENTIFIER})/({IDENTIFIER})")
APPLE_NAME = re.compile(rf"-\[(?:BigSyncKitTests\.)?({IDENTIFIER}) ({IDENTIFIER})\]")
SWIFT_NAME = re.compile(rf"(?:BigSyncKitTests\.)?({IDENTIFIER})\.({IDENTIFIER})")
EVENT = re.compile(
    r"Test Case '([^']+)' (started(?: at .+|\.)|(?:passed|failed|skipped) \([0-9.]+ seconds\)\.?)"
)


# The workflow requests this exact pattern from `focused-filter`; keeping it
# here prevents executed and verified selections from drifting independently.
FOCUSED_FILTER = (
    r"^BigSyncKitTests\.(?:SyncUndoCloseoutW1[^/]*|CloudKitAccountAvailability[^/]*|"
    r"CloudKitCallbackAdmissionTests|CloudKitSynchronizerAccountFencingTests|"
    r"BigSyncWorkerRequestCancellationTests|ChangeRequestProcessorCancellationTests|"
    r"SynchronizationProcessorStartupTests|BigSyncScheduledRetryTests|"
    r"BigSyncDeadlineRaceTests)/"
)
FOCUSED = re.compile(FOCUSED_FILTER)

# This one opt-in benchmark was already excluded from package correctness
# qualification. A skip is retained, never promoted to a pass. Any additional
# skip requires an explicit reviewed exception, not a blanket "resource" rule.
ALLOWED_SKIPS = frozenset({
    "BackupDetectionTests/testMutationJournalIdentityPerformanceBenchmark",
})


def parse_discovery(discovery: str) -> tuple[Counter, list[str]]:
    listed = Counter()
    errors = []
    for line in discovery.splitlines():
        value = line.strip()
        match = DISCOVERY.fullmatch(value)
        if match:
            listed["/".join(match.groups())] += 1
        elif value.startswith("BigSyncKitTests.") or re.match(r"^\w+\.\w+/", value):
            errors.append(f"unsupported discovery identity: {value}")
    for name, count in listed.items():
        if count != 1:
            errors.append(f"{name}: expected one discovery, found {count}")
    return listed, errors


def parse_execution(execution: str) -> tuple[dict[str, list[str]], list[str]]:
    records = defaultdict(list)
    errors = []
    for line in execution.splitlines():
        value = line.strip()
        if not value.startswith("Test Case "):
            continue
        event = EVENT.fullmatch(value)
        if not event:
            errors.append(f"unrecognized XCTest record: {value}")
            continue
        name = APPLE_NAME.fullmatch(event[1]) or SWIFT_NAME.fullmatch(event[1])
        if not name:
            errors.append(f"unsupported XCTest identity: {event[1]}")
            continue
        records["/".join(name.groups())].append(
            event[2].split(" ", 1)[0].removesuffix(".")
        )
    return dict(records), errors


def selected_cases(listed: Counter, phase: str) -> list[str]:
    if phase not in ("discovery", "full", "focused"):
        raise ValueError(f"unknown execution phase: {phase}")
    return [name for name in listed
            if phase != "focused" or FOCUSED.match("BigSyncKitTests." + name)]


def validate(
    discovery: str, statuses: Sequence[int], execution: str | None = None,
    *, phase: str = "full"
) -> dict:
    errors = []
    if len(statuses) != 3 or any(type(code) is not int or code != 0 for code in statuses):
        errors.append("swift/tee/xcsift must each exit zero")
    listed, discovery_errors = parse_discovery(discovery)
    errors.extend(discovery_errors)
    expected = selected_cases(listed, phase)
    for name in REQUIRED:
        if listed[name] != 1:
            errors.append(f"{name}: critical case requires one discovery, found {listed[name]}")
        if phase == "focused" and name not in expected:
            errors.append(f"{name}: critical case absent from focused selection")
    if not expected:
        errors.append("selected XCTest inventory is empty")

    records: dict[str, list[str]] = {}
    if execution is not None:
        records, execution_errors = parse_execution(execution)
        errors.extend(execution_errors)
        for name in expected:
            states = records.get(name, [])
            allowed_skip = name in ALLOWED_SKIPS and name not in REQUIRED
            if states != ["started", "passed"] and not (
                allowed_skip and states == ["started", "skipped"]
            ):
                errors.append(f"{name}: expected one start and pass"
                              f"{' or approved skip' if allowed_skip else ''}; got {states}")
        for name in sorted(records.keys() - set(expected)):
            errors.append(f"{name}: executed outside the selected discovered inventory")

    return {
        "critical_expected": list(REQUIRED),
        "expected": expected,
        "discovered": [name for name in listed if listed[name] == 1],
        "passed_cases": [name for name in expected if records.get(name) == ["started", "passed"]],
        "skipped_cases": sorted(name for name, states in records.items() if "skipped" in states),
        "missing_cases": [name for name in expected if execution is not None and name not in records],
        "unexpected_cases": sorted(records.keys() - set(expected)),
        "pipeline_exit_codes": list(statuses),
        "errors": errors,
        "passed": not errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("discovery", "full", "focused", "focused-filter"))
    parser.add_argument("--directory", type=Path, default=Path("qualification-w1"))
    args = parser.parse_args(argv)
    if args.phase == "focused-filter":
        print(FOCUSED_FILTER)
        return 0
    report = {
        "schema_version": 2,
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
        if args.phase == "focused":
            focused_filter = read("focused-filter.txt").strip()
            if focused_filter != FOCUSED_FILTER:
                raise ValueError("focused filter does not match the declared selection")
            report["focused_filter"] = focused_filter
        execution = None if args.phase == "discovery" else read(f"{args.phase}.log")
        report.update(validate(discovery, statuses, execution, phase=args.phase))
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
