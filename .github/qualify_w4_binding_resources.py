"""MR-UNDO-CLOSEOUT-20260920 W4: generic native integration boundaries.

Uses the existing macOS package lane. No application code, credentials, or
installed user Realm is supplied. Discovery and execution are separate gates.
"""
import json
from pathlib import Path
import re
import subprocess
import sys

EXPECTED = {
    "InjectedBindingStoreIdentityTests": (
        "testInjectedStoreIsTheOnlyPreparedBindingStore",
        "testExistingProviderObservesPendingReplacementAndActivation",
        "testFreshStoreAndProviderResumeTheSameDurableBinding",
        "testMalformedBindingFailsClosedWithoutPreparingAnotherStore",
    ),
    "DomainPrepublicationFollowUpTests": (
        "testLocalOnlyDeclarationRequestsOneFreshPassBeforeReceipt",
        "testPreviousPassCannotRequestAnotherRun",
        "testDownloadOnlyDoesNotAcquireUploadOrFollowUpAuthority",
        "testReplacedWorkerRejectsPreviousWorkerBoundary",
        "testCancelledCallerCannotRequestFollowUpFromCurrentBoundary",
        "testTransitionReadinessRechecksPendingWorkAndSemanticBlockers",
        "testTransitionReadinessRejectsEarlierPassWithoutBlockingFreshWork",
        "testTransitionReadinessRejectsWorkerReplacementDuringInspection",
        "testCancelledReadinessInspectionCannotAcquireCurrentPass",
    ),
    "BigSyncLegacyTrackingEvidenceTests": (
        "testInspectionUsesCopiedTrackingRealmAndReturnsSyncedEvidence",
        "testMissingTrackingRealmIsNotMembershipEvidence",
        "testReleasedDeviceIdentifierUsesExactHistoricalNamespace",
    ),
}


def capture(name, arguments):
    path = Path("qualification-w4-" + name + ".log")
    with path.open("w") as output:
        try:
            code = subprocess.run(arguments, stdout=output, stderr=subprocess.STDOUT,
                                  timeout=900).returncode
        except subprocess.TimeoutExpired:
            output.write("\nQUALIFICATION_TIMEOUT\n")
            code = 124
    return code, path.read_text()


def main():
    expected = [suite + "/" + name for suite, names in EXPECTED.items() for name in names]
    report = {
        "schema_version": 2,
        "plan": "MR-UNDO-CLOSEOUT-20260920", "owner": "W4",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "expected": expected, "discovery_passed": False,
        "execution_passed": False, "application_qualified": False,
        "signed_cloudkit_qualified": False,
    }
    result_path = Path("qualification-w4-binding-resources.json")
    code, listing = capture("discovery", ["swift", "test", "list"])
    report["discovery_exit_code"] = code
    listed = set(line.strip() for line in listing.splitlines())
    report["discovered"] = [name for name in expected
                            if any(line.endswith(name) for line in listed)]
    report["discovery_passed"] = code == 0 and report["discovered"] == expected
    result_path.write_text(json.dumps(report, indent=2) + "\n")
    if not report["discovery_passed"]:
        print("W4 native discovery failed; this is not a behavioral regression result.", flush=True)
        print(listing[:8000], flush=True)
        error_lines = listing.splitlines()
        for index, line in enumerate(error_lines):
            if " error:" in line or line.strip() == "error: fatalError":
                start = max(0, index - 8)
                end = min(len(error_lines), index + 12)
                print("\n".join(error_lines[start:end]), flush=True)
        print(listing[-8000:], flush=True)
        return 1
    code, execution = capture("execution", ["swift", "test", "--skip-build", "--filter",
                                             "|".join(EXPECTED)])
    passed_cases = [suite + "/" + name for suite, names in EXPECTED.items() for name in names
                    if re.search(r"Test Case '-\[[^\]]*" + re.escape(suite) + " "
                                 + re.escape(name) + r"\]' passed", execution)]
    report["execution_exit_code"] = code
    report["passed_cases"] = passed_cases
    report["execution_passed"] = code == 0 and passed_cases == expected
    result_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)
    return 0 if report["execution_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
