"""Behavior tests of runner outputs, never implementation-source assertions."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "verify_w1_execution.py"
spec = importlib.util.spec_from_file_location("verify_w1_execution", SCRIPT)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def discovery():
    return "\n".join("BigSyncKitTests." + name for name in verifier.REQUIRED) + "\n"


def execution(*, apple=True):
    lines = []
    for name in verifier.REQUIRED:
        suite, method = name.split("/")
        identity = f"-[BigSyncKitTests.{suite} {method}]" if apple else f"{suite}.{method}"
        lines.extend((f"Test Case '{identity}' started at 2026-09-28 12:00:00.000",
                      f"Test Case '{identity}' passed (0.001 seconds)."))
    return "\n".join(lines) + "\n"


class W1ExecutionTests(unittest.TestCase):
    def check(self, listing=None, log=None, statuses=(0, 0, 0)):
        return verifier.validate(discovery() if listing is None else listing, statuses,
                                 execution() if log is None else log)

    def test_apple_named_passes_are_accepted(self):
        result = self.check()
        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["passed_cases"], list(verifier.REQUIRED))

    def test_linux_named_passes_are_accepted(self):
        self.assertTrue(self.check(log=execution(apple=False))["passed"])

    def test_discovery_does_not_claim_execution(self):
        result = verifier.validate(discovery(), [0, 0, 0])
        self.assertTrue(result["passed"])
        self.assertEqual(result["passed_cases"], [])

    def test_every_required_identity_must_be_discovered_once(self):
        for name in verifier.REQUIRED:
            line = "BigSyncKitTests." + name + "\n"
            with self.subTest(name=name, condition="missing"):
                self.assertFalse(self.check(listing=discovery().replace(line, ""))["passed"])
            with self.subTest(name=name, condition="duplicate"):
                self.assertFalse(self.check(listing=discovery() + line)["passed"])

    def test_missing_or_started_only_method_is_not_execution(self):
        self.assertFalse(self.check(log="Executed 14 tests, with 0 failures\n")["passed"])
        self.assertFalse(self.check(log=execution().rsplit("Test Case", 1)[0])["passed"])

    def test_each_required_skip_and_failure_is_rejected(self):
        lines = execution().splitlines(keepends=True)
        for index in range(1, len(lines), 2):
            for status in ("skipped", "failed"):
                changed = lines.copy()
                changed[index] = changed[index].replace(" passed ", f" {status} ")
                with self.subTest(index=index, status=status):
                    self.assertFalse(self.check(log="".join(changed))["passed"])

    def test_pass_without_start_or_reversed_events_is_rejected(self):
        lines = execution().splitlines(keepends=True)
        self.assertFalse(self.check(log="".join(lines[1:]))["passed"])
        lines[0], lines[1] = lines[1], lines[0]
        self.assertFalse(self.check(log="".join(lines))["passed"])

    def test_duplicate_pass_and_retry_after_failure_are_rejected(self):
        log = execution()
        self.assertFalse(self.check(log=log + log)["passed"])
        failed = log.replace(" passed ", " failed ")
        self.assertFalse(self.check(log=failed + log)["passed"])

    def test_wrong_modules_and_suffix_names_cannot_satisfy_identity(self):
        self.assertFalse(self.check(listing=discovery().replace("BigSyncKitTests.", "OtherTests."))["passed"])
        self.assertFalse(self.check(log=execution().replace("BigSyncKitTests.", "OtherTests."))["passed"])
        self.assertFalse(self.check(log=execution().replace("testAlreadyCancelledRequestPreservesLiveRetry",
                                                          "testAlreadyCancelledRequestPreservesLiveRetryExtra"))["passed"])

    def test_unrelated_skip_is_retained_not_promoted_to_required_pass(self):
        extra = "Test Case '-[BigSyncKitTests.ResourceTests testMissingFixture]' skipped (0.001 seconds).\n"
        result = self.check(log=execution() + extra)
        self.assertTrue(result["passed"])
        self.assertEqual(result["skipped_cases"], ["ResourceTests/testMissingFixture"])

    def test_unrelated_failure_is_rejected_even_with_exit_zero(self):
        extra = "Test Case '-[BigSyncKitTests.OtherTests testBroken]' failed (0.001 seconds).\n"
        self.assertFalse(self.check(log=execution() + extra)["passed"])

    def test_each_pipeline_failure_or_interruption_is_rejected(self):
        for slot in range(3):
            for code in (1, 64, 65, 124, 137, 143, -9):
                status = [0, 0, 0]
                status[slot] = code
                with self.subTest(slot=slot, code=code):
                    self.assertFalse(self.check(statuses=status)["passed"])

    def test_invalid_pipeline_shape_and_types_are_rejected(self):
        for status in ([], [0], [0, 0], [0, 0, 0, 0], [False, 0, 0], [0.0, 0, 0], ["0", 0, 0]):
            self.assertFalse(self.check(statuses=status)["passed"])

    def test_quotes_and_nonterminal_summaries_are_not_case_evidence(self):
        self.assertFalse(self.check(log="\n".join("example: " + line for line in execution().splitlines()))["passed"])
        self.assertFalse(self.check(log="Test Suite 'All tests' passed\n")["passed"])

    def run_cli(self, directory, phase="full"):
        return subprocess.run([sys.executable, str(SCRIPT), phase, "--directory", str(directory)],
                              capture_output=True, text=True, timeout=10)

    def packet(self, directory):
        files = {"commit.txt": "a" * 40 + "\n", "discovery.log": discovery(),
                 "full.log": execution(), "full.status.json": "[0, 0, 0]\n",
                 "discovery.status.json": "[0, 0, 0]\n"}
        for name, text in files.items():
            (directory / name).write_text(text, encoding="utf-8")
        return {name: text.encode() for name, text in files.items()}

    def test_cli_retains_input_hashes_and_does_not_qualify_app(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            original = self.packet(directory)
            completed = self.run_cli(directory)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads((directory / "full-identity.json").read_text())
            self.assertTrue(report["passed"])
            self.assertFalse(report["assembled_application_qualified"])
            self.assertFalse(report["signed_cloudkit_qualified"])
            for name, data in original.items():
                self.assertEqual((directory / name).read_bytes(), data)
                self.assertEqual(report["input_sha256"][name], hashlib.sha256(data).hexdigest())

    def test_cli_missing_or_corrupt_packet_fails_with_report(self):
        for name, value in (("full.log", None), ("full.status.json", "{}"),
                            ("full.status.json", "broken"), ("commit.txt", "HEAD")):
            with self.subTest(name=name, value=value), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                self.packet(directory)
                if value is None:
                    (directory / name).unlink()
                else:
                    (directory / name).write_text(value)
                completed = self.run_cli(directory)
                self.assertEqual(completed.returncode, 1, completed.stderr)
                self.assertFalse(json.loads((directory / "full-identity.json").read_text())["passed"])

    def test_cli_successful_execution_cannot_hide_failed_discovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.packet(directory)
            (directory / "discovery.status.json").write_text("[65, 0, 0]")
            completed = self.run_cli(directory)
            self.assertEqual(completed.returncode, 1, completed.stderr)
            report = json.loads((directory / "full-identity.json").read_text())
            self.assertFalse(report["passed"])
            self.assertEqual(report["discovery_pipeline_exit_codes"], [65, 0, 0])

    def test_cli_discovery_records_no_case_passes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.packet(directory)
            (directory / "discovery.status.json").write_text("[0, 0, 0]")
            completed = self.run_cli(directory, "discovery")
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads((directory / "discovery-identity.json").read_text())
            self.assertEqual(report["passed_cases"], [])


if __name__ == "__main__":
    unittest.main()
