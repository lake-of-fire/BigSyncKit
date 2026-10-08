"""Native-lane admission tests using synthetic logs; they never execute Swift."""
import importlib.util
from pathlib import Path
import unittest

MODULE_PATH = Path(__file__).resolve().parents[1] / "verify_w1_execution.py"
SPEC = importlib.util.spec_from_file_location("verify_w1_execution_native", MODULE_PATH)
verifier = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(verifier)


def discovery(names=None):
    names = verifier.REQUIRED if names is None else names
    return "\n".join("BigSyncKitTests." + name for name in names) + "\n"


def run_log(names, failed=()):
    failed = set(failed)
    records=[]
    for name in names:
        identity=name.replace("/", " ")
        records.append(f"Test Case '-[BigSyncKitTests.{identity}]' started.")
        result="failed" if name in failed else "passed"
        records.append(f"Test Case '-[BigSyncKitTests.{identity}]' {result} (0.1 seconds).")
    return "\n".join(records)+"\n"


class NativeLaneVerifierTests(unittest.TestCase):
    def test_complete_full_roster_passes(self):
        report=verifier.validate(discovery(),[0,0,0],run_log(verifier.REQUIRED),phase="full")
        self.assertTrue(report["passed"],report["errors"])
        self.assertEqual(report["passed_cases"],list(verifier.REQUIRED))

    def test_focused_run_contains_every_critical_identity(self):
        listed,errors=verifier.parse_discovery(discovery())
        self.assertEqual(errors,[])
        selected=verifier.selected_cases(listed,"focused")
        self.assertTrue(set(verifier.REQUIRED).issubset(selected))
        report=verifier.validate(discovery(),[0,0,0],run_log(selected),phase="focused")
        self.assertTrue(report["passed"],report["errors"])

    def test_removed_recent_selector_fails_discovery(self):
        omitted=next(name for name in verifier.REQUIRED if name.startswith("SyncRetainedRecordContractTests/"))
        names=[name for name in verifier.REQUIRED if name!=omitted]
        report=verifier.validate(discovery(names),[0,0,0],run_log(names),phase="full")
        self.assertFalse(report["passed"])
        self.assertTrue(any(omitted in error for error in report["errors"]))

    def test_duplicate_execution_is_rejected(self):
        names=list(verifier.REQUIRED)
        log=run_log(names)+run_log(names[:1])
        report=verifier.validate(discovery(),[0,0,0],log,phase="full")
        self.assertFalse(report["passed"])

    def test_method_failure_is_not_hidden_by_other_passes(self):
        failed=verifier.REQUIRED[-1]
        report=verifier.validate(discovery(),[0,0,0],run_log(verifier.REQUIRED,{failed}),phase="full")
        self.assertFalse(report["passed"])
        self.assertTrue(any(failed in error for error in report["errors"]))

    def test_each_pipeline_stage_must_succeed(self):
        for statuses in ([1,0,0],[0,1,0],[0,0,1]):
            with self.subTest(statuses=statuses):
                report=verifier.validate(discovery(),statuses,run_log(verifier.REQUIRED),phase="full")
                self.assertFalse(report["passed"])
                self.assertTrue(any("must each exit zero" in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
