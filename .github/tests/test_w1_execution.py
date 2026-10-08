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

    def test_apple_start_without_timestamp_is_accepted(self):
        # Exact record shape observed in the macOS/Xcode 16.4 run artifact.
        # Test cases say "started." while test suites include a timestamp.
        log = execution().replace("started at 2026-09-28 12:00:00.000", "started.")
        result = self.check(log=log)
        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["passed_cases"], list(verifier.REQUIRED))

    def test_timestamp_free_start_does_not_weaken_terminal_or_order_requirements(self):
        log = execution().replace("started at 2026-09-28 12:00:00.000", "started.")
        lines = log.splitlines(keepends=True)
        for changed in (lines[1:], lines[:-1], lines + lines,
                        [lines[1], lines[0]] + lines[2:]):
            with self.subTest(events=changed[:2]):
                self.assertFalse(self.check(log="".join(changed))["passed"])

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

    def test_unregistered_skip_is_retained_but_rejected(self):
        extra = "Test Case '-[BigSyncKitTests.ResourceTests testMissingFixture]' skipped (0.001 seconds).\n"
        result = self.check(log=execution() + extra)
        self.assertFalse(result["passed"])
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


class W1InventoryTests(unittest.TestCase):
    extra = "UnrelatedRegressionTests/testNewRegression"

    @staticmethod
    def listing(names):
        return "".join("BigSyncKitTests." + name + "\n" for name in names)

    @staticmethod
    def events(name, end="passed"):
        suite, method = name.split("/")
        identity = f"-[BigSyncKitTests.{suite} {method}]"
        return (f"Test Case '{identity}' started.\n"
                + (f"Test Case '{identity}' {end} (0.001 seconds).\n" if end else ""))

    def check(self, extra_listing="", extra_log="", **options):
        return verifier.validate(discovery() + extra_listing, [0, 0, 0],
                                 execution() + extra_log, **options)

    def test_full_run_requires_noncritical_discovered_method(self):
        result = self.check(self.listing([self.extra]))
        self.assertFalse(result["passed"], "An incomplete full inventory was accepted")
        self.assertIn(self.extra, result["missing_cases"])

    def test_full_run_rejects_noncritical_start_without_terminal(self):
        result = self.check(self.listing([self.extra]), self.events(self.extra, None))
        self.assertFalse(result["passed"], "A started-only full-inventory case was accepted")

    def test_full_run_rejects_duplicate_noncritical_execution(self):
        result = self.check(self.listing([self.extra]), self.events(self.extra) * 2)
        self.assertFalse(result["passed"], "Duplicate execution outside the old shortlist passed")

    def test_full_run_rejects_undiscovered_pass(self):
        result = self.check(extra_log=self.events(self.extra))
        self.assertFalse(result["passed"], "An undiscovered pass was accepted")
        self.assertIn(self.extra, result["unexpected_cases"])

    def test_duplicate_noncritical_discovery_is_rejected(self):
        result = self.check(self.listing([self.extra, self.extra]), self.events(self.extra))
        self.assertFalse(result["passed"], "A duplicate outside the critical shortlist passed")

    def test_unapproved_discovered_skip_is_not_a_full_pass(self):
        result = self.check(self.listing([self.extra]), self.events(self.extra, "skipped"))
        self.assertFalse(result["passed"], "An unapproved skip became full qualification")

    def test_malformed_extra_case_record_is_not_silently_ignored(self):
        for event in ("pending", "notrun", "suppressed", "passed (unknown seconds)."):
            with self.subTest(event=event):
                extra = "Test Case '-[BigSyncKitTests.OtherTests testUnexpected]' " + event + "\n"
                self.assertFalse(self.check(extra_log=extra)["passed"])

    def test_foreign_extra_case_is_not_silently_ignored(self):
        extra = self.events(self.extra).replace("BigSyncKitTests.", "OtherModule.")
        self.assertFalse(self.check(extra_log=extra)["passed"])

    def test_complete_extra_case_is_counted_not_just_ignored(self):
        result = self.check(self.listing([self.extra]), self.events(self.extra))
        self.assertTrue(result["passed"], result["errors"])
        self.assertIn(self.extra, result["expected"])
        self.assertIn(self.extra, result["passed_cases"])
        self.assertEqual(result["missing_cases"], [])

    def test_approved_benchmark_skip_retains_complete_lifecycle(self):
        name = "BackupDetectionTests/testMutationJournalIdentityPerformanceBenchmark"
        result = self.check(self.listing([name]), self.events(name, "skipped"))
        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["skipped_cases"], [name])
        self.assertNotIn(name, result["passed_cases"])
        self.assertIn(name, result["expected"])
        for listing, log in (
            ("", self.events(name, "skipped")),
            (self.listing([name]), self.events(name, "skipped").splitlines()[1] + "\n"),
            (self.listing([name]), self.events(name, "failed")),
        ):
            with self.subTest(listing=listing, log=log):
                self.assertFalse(self.check(listing, log)["passed"])

    def test_each_new_callback_and_preflight_identity_is_critical(self):
        # Test runtime admission of a missing declared class, not source text.
        for suite in (
            "CloudKitCallbackAdmissionTests",
            "CloudKitAccountAvailabilityDeadlineTests",
            "ChangeRequestProcessorCancellationTests",
            "SynchronizationProcessorStartupTests",
        ):
            remaining = [n for n in verifier.REQUIRED if not n.startswith(suite + "/")]
            result = verifier.validate(self.listing(remaining), [0, 0, 0],
                                       "".join(self.events(n) for n in remaining))
            self.assertFalse(result["passed"], f"Missing {suite} was accepted")

    def test_missing_critical_case_cannot_disappear_from_both_inputs(self):
        for omitted in verifier.REQUIRED:
            remaining = [n for n in verifier.REQUIRED if n != omitted]
            with self.subTest(omitted=omitted):
                result = verifier.validate(self.listing(remaining), [0, 0, 0],
                                           "".join(self.events(n) for n in remaining))
                self.assertFalse(result["passed"], "The same omitted case vanished from both inventories")

    def test_focused_phase_uses_selected_discovery_not_full_inventory(self):
        selected_extra = "CloudKitCallbackAdmissionTests/testLaterCoverage"
        names = [self.extra, selected_extra]
        result = self.check(self.listing(names), self.events(selected_extra), phase="focused")
        self.assertTrue(result["passed"], result["errors"])
        self.assertIn(self.extra, result["discovered"])
        self.assertNotIn(self.extra, result["expected"])
        self.assertIn(selected_extra, result["passed_cases"])
        self.assertFalse(self.check(self.listing(names), phase="focused")["passed"])

    def test_focused_phase_rejects_an_unselected_extra_pass(self):
        result = self.check(self.listing([self.extra]), self.events(self.extra), phase="focused")
        self.assertFalse(result["passed"])
        self.assertIn(self.extra, result["unexpected_cases"])

    def test_emitted_filter_selects_same_cases_as_verifier(self):
        import re
        completed = subprocess.run([sys.executable, str(SCRIPT), "focused-filter"],
                                   capture_output=True, text=True, timeout=10)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        pattern = re.compile(completed.stdout.strip())
        names = list(verifier.REQUIRED) + [
            "BigSyncScheduledRetryTests/testNewRetry",
            "BigSyncDeadlineRaceTests/testNewDeadline",
            "SyncUndoCloseoutW1RepresentationTests/testNewRepresentation",
            "OtherTests/testCloudKitCallbackAdmissionTestsIsOnlyInMethodName",
            "CloudKitCallbackAdmissionTestsExtra/testNeighbor",
            "ChangeRequestProcessorCancellationTestsExtra/testNeighbor",
            "SynchronizationProcessorStartupTestsExtra/testNeighbor",
            self.extra,
        ]
        expected = [n for n in names if pattern.search("BigSyncKitTests." + n)]
        listed, errors = verifier.parse_discovery(self.listing(names))
        self.assertEqual(errors, [])
        self.assertEqual(verifier.selected_cases(listed, "focused"), expected)
        for required in verifier.REQUIRED:
            self.assertIn(required, expected)
        self.assertNotIn(self.extra, expected)
        self.assertNotIn("CloudKitCallbackAdmissionTestsExtra/testNeighbor", expected)
        self.assertNotIn("ChangeRequestProcessorCancellationTestsExtra/testNeighbor", expected)
        self.assertNotIn("SynchronizationProcessorStartupTestsExtra/testNeighbor", expected)
        self.assertFalse(pattern.search("OtherTests.CloudKitCallbackAdmissionTests/testNew"))
        self.assertFalse(pattern.search("OtherTests.ChangeRequestProcessorCancellationTests/testNew"))
        self.assertFalse(pattern.search("OtherTests.SynchronizationProcessorStartupTests/testNew"))

    def test_interleaving_preserves_per_method_terminal_order(self):
        import random
        names = list(verifier.REQUIRED) + [self.extra]
        rng = random.Random(8675309)
        for _ in range(20):
            streams = [self.events(n).splitlines(keepends=True) for n in names]
            log = []
            while streams:
                stream = rng.choice(streams)
                log.append(stream.pop(0))
                if not stream:
                    streams.remove(stream)
            result = verifier.validate(self.listing(names), [0, 0, 0], "".join(log))
            self.assertTrue(result["passed"], result["errors"])
            self.assertEqual(set(result["passed_cases"]), set(names))

    def test_focused_cli_requires_exact_filter_provenance(self):
        old = W1ExecutionTests()
        for filter_text in (None, "SomethingElse", verifier.FOCUSED_FILTER + "\n"):
            with self.subTest(filter_text=filter_text), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                old.packet(directory)
                (directory / "focused.log").write_text(execution())
                (directory / "focused.status.json").write_text("[0,0,0]\n")
                if filter_text is not None:
                    (directory / "focused-filter.txt").write_text(filter_text)
                completed = old.run_cli(directory, "focused")
                expected_code = 0 if filter_text == verifier.FOCUSED_FILTER + "\n" else 1
                self.assertEqual(completed.returncode, expected_code, completed.stderr)
                report = json.loads((directory / "focused-identity.json").read_text())
                self.assertFalse(report["assembled_application_qualified"])
                self.assertFalse(report["signed_cloudkit_qualified"])
                if expected_code == 0:
                    self.assertEqual(report["schema_version"], 2)
                    self.assertEqual(report["focused_filter"], verifier.FOCUSED_FILTER)
                    self.assertIn("focused-filter.txt", report["input_sha256"])

    def test_full_cli_rejects_incomplete_inventory_with_all_pipelines_zero(self):
        old = W1ExecutionTests()
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            old.packet(directory)
            (directory / "discovery.log").write_text(discovery() + self.listing([self.extra]))
            completed = old.run_cli(directory)
            self.assertEqual(completed.returncode, 1, completed.stderr)
            report = json.loads((directory / "full-identity.json").read_text())
            self.assertIn(self.extra, report["missing_cases"])
            self.assertFalse(report["passed"])


class W1MergedNativeContractTests(unittest.TestCase):
    # Independent acceptance contract, not inferred from verifier.REQUIRED.
    # Retain merged #83/#84 and the 43 named regressions in the fixed Oct 8
    # component set (#142/#143/#144/#145/#146/#147).
    merged_cases = (
        "CloudKitSynchronizerAccountFencingTests/testReentrantFailureObserversPreserveOneSettlementSnapshot",
        "CloudKitSynchronizerAccountFencingTests/testFailureObserverSuccessorRetainsAttemptAndTask",
        "SyncUndoCloseoutW1Tests/testFetchedDeletionPageReplaysAfterTargetFirstInterruptionWithoutDeletingAgain",
        "SyncRetainedRecordContractTests/testRetainedCleanupIdentityCallbackPreservesSuccessorJournalAndPageEvidence",
        "SyncRetainedRecordContractTests/testConflictRefreshRollsBackAfterSynchronousAccountFencePoison",
        "SyncRetainedRecordContractTests/testConflictArchiveDiscardRollsBackAfterSynchronousAccountFencePoison",
        "SyncSplitOperationOwnershipTests/testCancelledJournalForwardingCannotPublishToSuccessorTracking",
        "SyncSplitOperationOwnershipTests/testImportProgressCannotReacquireSuccessorJournalOwnership",
        "SyncSplitOperationOwnershipTests/testCancelledImportCannotClearSuccessorAssetsAfterProgressCallout",
        "SyncSplitOperationOwnershipTests/testCancelledQueuedRemainingCountDoesNotNotifySuccessor",
        "SyncSplitOperationOwnershipTests/testJournalForwardingRejectsTransportReplacementBeforeTrackingAdmission",
        "SyncSplitOperationOwnershipTests/testInboundDeletionRejectsCancellationResetAccountBindingAndTransportReplacement",
        "SyncSplitOperationOwnershipTests/testInboundDeletionRetainsCommittedTombstoneAfterOwnerRetirementAndFreshRetry",
        "ChangeFeedMigrationResumeTests/testDurableCompletionExcludesProvisionalTerminalMarkerUntilCommit",
        "ChangeFeedMigrationResumeTests/testBackupRestoreRetiresCommittedJournalBehindRolledBackCurrentMutation",
        "ChangeFeedMigrationResumeTests/testBackupRestorePreservesCurrentMutationCommittedAfterSnapshot",
        "ChangeFeedMigrationResumeTests/testResetPreparationRejectsProvisionalPreparedMarkerAfterRollback",
        "ChangeFeedMigrationResumeTests/testBootstrapCannotSkipItsWriteForProvisionalCompletion",
        "ChangeFeedMigrationResumeTests/testFinishCannotSkipItsWriteForProvisionalCompletion",
        "ChangeFeedMigrationResumeTests/testReconciliationCannotAcceptProvisionalCompletionWithoutBootstrap",
        "ChangeFeedMigrationResumeTests/testEncryptedResetReuploadsRetainedLiveObjectBehindRolledBackDeletion",
        "ChangeFeedMigrationResumeTests/testEncryptedResetPreservesDeletionCommittedAfterRetainedCandidateSnapshot",
        "ChangeFeedMigrationResumeTests/testEstablishedServerEvidenceExcludesProvisionalMembershipUntilCommit",
        "ChangeFeedMigrationResumeTests/testResetTrackingPublicationUsesCommittedJournalBehindRolledBackSuccessor",
        "ChangeFeedMigrationResumeTests/testQueuedPreparationPreservesCommittedPreparedSuccessorAndProvenance",
        "ChangeFeedMigrationResumeTests/testQueuedPreparationPreservesCommittedCompleteSuccessorAndProvenance",
        "ChangeFeedMigrationResumeTests/testQueuedBootstrapTreatsCommittedCompletionAsNoOpWithoutRetiringProof",
        "ChangeFeedMigrationResumeTests/testQueuedFinishTreatsCommittedCompletionAsNoOpWithoutRetiringProof",
        "ChangeFeedMigrationResumeTests/testBootstrapRejectsCancellationAfterCommitSubmissionAndKeepsDurableMarker",
        "ChangeFeedMigrationResumeTests/testReconciliationRejectsCancellationAfterTrackingCommitSubmission",
        "ChangeFeedMigrationResumeTests/testFinishRejectsCancellationAfterCommitSubmissionAndKeepsDurableMarker",
        "ChangeFeedMigrationResumeTests/testFencedResetCancellationAfterTrackingCommitPreservesProviderAndDurableReset",
        "ChangeFeedMigrationResumeTests/testFencedResetPreservesPreparedSuccessorAtOwnedResetAdmission",
        "ChangeFeedMigrationResumeTests/testFencedResetPreservesCompleteSuccessorAtOwnedResetAdmission",
        "HotfixCollectionSafetyTests/testAssetInScalarFieldRejectsNewReceiverBeforeRealmAssignment",
        "HotfixCollectionSafetyTests/testAssetInScalarFieldRollsBackExistingValueAndTracking",
        "HotfixCollectionSafetyTests/testComparisonDecoderRejectsAssetInScalarField",
        "HotfixCollectionSafetyTests/testReadableDataAssetsDecodeAndMissingFilesRollBack",
        "SyncUndoCloseoutW1Tests/testSemanticQuarantineIgnoresProvisionalInsertionAndRemoval",
        "SyncUndoCloseoutW1Tests/testSemanticQuarantineUsesCommittedFeedEpoch",
        "SyncUndoCloseoutW1Tests/testServerEvidenceIgnoresProvisionalAcknowledgement",
        "SyncUndoCloseoutW1Tests/testServerEvidenceIgnoresProvisionalRemovalAndForeignZoneReplacement",
        "SyncUndoCloseoutW1Tests/testServerEvidencePreservesExactAndCatalogStatePolicies",
        "SyncUndoCloseoutW1Tests/testServerEvidenceUsesCommittedAccountScopeAcrossSharedTargetRealm",
        "SyncUndoCloseoutW1Tests/testBootstrapServerEvidenceIgnoresProvisionalTrackingMembership",
        "CloudKitSynchronizerAccountFencingTests/testTemporaryLocalInitialAdmissionRetriesExistingDrainAndAdmitsCurrentBinding",
    )

    def inventory(self):
        return list(dict.fromkeys((*verifier.REQUIRED, *self.merged_cases)))

    def packet_strings(self, names):
        return (W1InventoryTests.listing(names),
                "".join(W1InventoryTests.events(name) for name in names))

    def test_merged_regressions_cannot_disappear_from_both_inputs(self):
        for omitted in self.merged_cases:
            names = [name for name in self.inventory() if name != omitted]
            listing, log = self.packet_strings(names)
            for phase in ("discovery", "full", "focused"):
                with self.subTest(omitted=omitted, phase=phase):
                    report = verifier.validate(
                        listing, [0, 0, 0],
                        None if phase == "discovery" else log, phase=phase)
                    self.assertFalse(report["passed"],
                                     "A merged regression vanished from both inputs")

    def test_complete_merged_contract_retains_real_case_accounting(self):
        names = self.inventory()
        listing, log = self.packet_strings(names)
        for phase in ("full", "focused"):
            with self.subTest(phase=phase):
                report = verifier.validate(listing, [0, 0, 0], log, phase=phase)
                self.assertTrue(report["passed"], report["errors"])
                self.assertEqual(set(report["passed_cases"]), set(names))
                self.assertEqual(report["missing_cases"], [])
                self.assertEqual(report["unexpected_cases"], [])
                for name in self.merged_cases:
                    self.assertIn(name, report["critical_expected"])
                    self.assertIsNotNone(verifier.FOCUSED.match("BigSyncKitTests." + name))

    def test_cli_rejects_missing_merged_regression_with_zero_pipeline_exits(self):
        helper = W1ExecutionTests()
        for omitted in self.merged_cases:
            names = [name for name in self.inventory() if name != omitted]
            listing, log = self.packet_strings(names)
            with self.subTest(omitted=omitted), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                helper.packet(directory)
                (directory / "discovery.log").write_text(listing, encoding="utf-8")
                (directory / "full.log").write_text(log, encoding="utf-8")
                result = helper.run_cli(directory)
                self.assertEqual(result.returncode, 1, result.stderr)
                report = json.loads((directory / "full-identity.json").read_text())
                self.assertFalse(report["passed"])
                self.assertFalse(report["assembled_application_qualified"])
                self.assertFalse(report["signed_cloudkit_qualified"])
                self.assertTrue(any(omitted in error for error in report["errors"]))

    def test_merged_regressions_cannot_pass_as_skips_or_duplicate_execution(self):
        names = self.inventory()
        listing, log = self.packet_strings(names)
        for name in self.merged_cases:
            for replacement in (W1InventoryTests.events(name, "skipped"),
                                W1InventoryTests.events(name) * 2):
                changed = log.replace(W1InventoryTests.events(name), replacement)
                for phase in ("full", "focused"):
                    with self.subTest(name=name, phase=phase, replacement=replacement):
                        report = verifier.validate(listing, [0, 0, 0], changed, phase=phase)
                        self.assertFalse(report["passed"])


if __name__ == "__main__":
    unittest.main()
