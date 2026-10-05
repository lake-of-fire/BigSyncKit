#!/usr/bin/env python3
"""Execute selected production journal reads with explicit non-SDK collaborators.

No package resolution, real Realm files, CloudKit, app launch, or network access.
Output directories are create-only and retained, including failed compilations.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys

ADAPTER = "Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift"
INVENTORY = "Sources/BigSyncKit/RealmSwift/BigSyncPendingMutationInventory.swift"

def blob(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()

def source_bytes(root: Path, path: str, ref: str | None) -> bytes:
    if ref is None:
        return (root / path).read_bytes()
    return subprocess.run(["git", "-C", str(root), "show", f"{ref}:{path}"],
                          check=True, capture_output=True, timeout=30).stdout

def methods(source: str, range_only: bool) -> str:
    # Extraction prepares executable production bodies; behavior is asserted by
    # RuntimeCases, never by matching the implementation's text.
    start = source.index("    private func pendingMutationSnapshots(")
    stop = len(source) if range_only else source.index(
        "    @BigSyncBackgroundActor\n    private func enqueueCreatedAndModified(", start)
    selected = source[start:stop].rstrip("\n") + "\n"
    first = selected.index("    func pendingMutationTargetsDeletedObject(")
    last = selected.index("    /// Immediately updates.", first)
    # Keep the snapshot collector and forwarding bodies unchanged. The SDK's
    # polymorphic primary-key/lifecycle decoder is an explicit collaborator.
    selected = selected[:first] + selected[last:]
    helper_marker = "    @BigSyncBackgroundActor\n    func committedRealmReadSnapshot("
    if not range_only and helper_marker in source:
        # Retain the actual production committed-read boundary used by all
        # selected bodies, rather than replacing it with a harness implementation.
        helper_start = source.index(helper_marker)
        helper_end = source.index("    private func pendingMutationSnapshots(", helper_start)
        selected = source[helper_start:helper_end] + selected
    return selected

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--source-ref", help="Read the two sources from an existing local Git ref")
    parser.add_argument("--evidence", required=True, type=Path, help="New, nonexisting output directory")
    parser.add_argument("--configuration", choices=("debug", "release", "both"), default="both")
    parser.add_argument("--expect-failures", type=int, default=0,
                        help="Expected runtime failures when reproducing an original/negative control")
    parser.add_argument("--adapter-methods", type=Path,
                        help="Explicit complete method-range input; NOT a full-adapter qualification")
    parser.add_argument("--inventory-source", type=Path,
                        help="Complete inventory source paired with --adapter-methods")
    args = parser.parse_args()
    if (args.adapter_methods is None) != (args.inventory_source is None):
        parser.error("--adapter-methods and --inventory-source must be supplied together")
    if args.adapter_methods is not None and args.source_ref is not None:
        parser.error("Method-range inputs cannot be combined with --source-ref")
    if args.expect_failures < 0:
        parser.error("--expect-failures must be nonnegative")
    out = args.evidence.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=False)
    helpers = Path(__file__).resolve().parent / "PortableJournalReadBoundary"
    report: dict = {"native_sdk_qualified": False, "runs": []}
    report_path = out / "report.json"

    def save() -> None:
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    def command(argv: list[str], log: Path, env: dict[str, str] | None = None) -> int:
        entry = {"command": argv, "log": str(log)}
        report["runs"].append(entry)
        save()
        try:
            with log.open("xb") as output:
                result = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT,
                                        env=env, timeout=60)
            entry["exit"] = result.returncode
            entry["log_sha256"] = hashlib.sha256(log.read_bytes()).hexdigest()
            save()
            return result.returncode
        except subprocess.TimeoutExpired:
            entry["incomplete"] = "process timeout; no pass"
            save()
            raise

    try:
        if args.adapter_methods is not None:
            adapter = args.adapter_methods.read_bytes()
            inventory = args.inventory_source.read_bytes()
            report["source_scope"] = "complete inventory plus supplied actual method range; not the full adapter"
        else:
            adapter = source_bytes(args.repository.resolve(), ADAPTER, args.source_ref)
            inventory = source_bytes(args.repository.resolve(), INVENTORY, args.source_ref)
            report["source_scope"] = "complete inventory plus selected actual bodies from full adapter input"
        report["input_blobs"] = {"adapter_input": blob(adapter), "inventory": blob(inventory)}
        report["collaborator_sha256"] = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(helpers.iterdir()) if p.is_file()
        }
        report["limitations"] = [
            "No actual Realm MVCC, native notification delivery, async write admission queue, or journal observer execution",
            "Polymorphic target decoding, tracking mutation, model schema and transport eligibility use explicit collaborators",
            "No full adapter/package typecheck, XCTest/native method discovery, signed account switch or application qualification",
        ]
        selected = methods(adapter.decode(), args.adapter_methods is not None)
        (out / "AdapterReadMethods.swift").write_text(
            (helpers / "AdapterShell.prefix").read_text() + selected + "}\n")
        (out / "BigSyncPendingMutationInventory.swift").write_bytes(inventory)
        cases = re.findall(r'RuntimeCase\("([^"]+)"\)', (helpers / "RuntimeCases.swift").read_text())
        if not cases or len(cases) != len(set(cases)):
            raise RuntimeError("Runtime case inventory is empty or duplicated")
        report["required_runtime_case_names"] = cases
        save()
        configs = ("debug", "release") if args.configuration == "both" else (args.configuration,)
        for configuration in configs:
            build = out / configuration
            build.mkdir()
            opt = "-Onone" if configuration == "debug" else "-O"
            extension = "dylib" if platform.system() == "Darwin" else "so"
            library = build / f"libRealmSwift.{extension}"
            common = ["swiftc", "-swift-version", "6", "-warnings-as-errors", "-parse-as-library"]
            compile_realm = common + ["-emit-module", "-emit-library", "-module-name", "RealmSwift",
                str(helpers / "RealmCollaborator.swift"), "-o", str(library),
                "-emit-module-path", str(build / "RealmSwift.swiftmodule"), opt]
            if command(compile_realm, build / "realm-build.log"):
                raise RuntimeError("Collaborator compilation failed; not a behavioral result")
            executable = build / "runtime-cases"
            compile_cases = common + ["-D", "DEBUG", "-I", str(build), "-L", str(build), "-lRealmSwift",
                str(helpers / "AdapterCollaborators.swift"), str(out / "AdapterReadMethods.swift"),
                str(out / "BigSyncPendingMutationInventory.swift"), str(helpers / "RuntimeCases.swift"),
                opt, "-o", str(executable)]
            if command(compile_cases, build / "build.log"):
                raise RuntimeError("Selected-source compilation failed; not a behavioral result")
            env = os.environ.copy()
            key = "DYLD_LIBRARY_PATH" if platform.system() == "Darwin" else "LD_LIBRARY_PATH"
            env[key] = str(build) + (os.pathsep + env[key] if env.get(key) else "")
            log = build / "runtime.log"
            code = command([str(executable)], log, env)
            lines = log.read_text().splitlines()
            passes = [line[5:] for line in lines if line.startswith("PASS ")]
            failures = [line[5:].split(":", 1)[0] for line in lines if line.startswith("FAIL ")]
            all_names = passes + failures
            expected_code = 1 if args.expect_failures else 0
            matched = (len(all_names) == len(cases) and set(all_names) == set(cases)
                       and len(failures) == args.expect_failures and code == expected_code)
            report.setdefault("results", {})[configuration] = {
                "passed": passes, "failed": failures, "exit": code,
                "matches_expected_outcome": matched,
                "expected_runtime_failures": args.expect_failures,
            }
            save()
            print(f"{configuration}: {len(passes)} passed, {len(failures)} failed; process {code}")
            if not matched:
                raise RuntimeError("Runtime outcomes or exact case identities did not match expectations")
        return 0
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        save()
        print(report["error"], file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
