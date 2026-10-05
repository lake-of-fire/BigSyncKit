#!/usr/bin/env python3
"""Run actual Foundation-only sync evidence types and their XCTest files.

This does not build the BigSync package, Realm/CloudKit, or Reader. It performs
no network access. Evidence and the generated scratch package are create-only
and retained even on failure. --source-ref tests an existing local ancestor
against the current test files, not a reconstructed replacement implementation.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys

AUDIT = "Sources/BigSyncKit/RealmSwift/BigSyncSynchronizationAudit.swift"
PUBLICATION = "Sources/BigSyncKit/QSSynchronizer/BigSyncDurablePublicationEvidence.swift"
ERRORS = "Sources/BigSyncKit/QSSynchronizer/KeyValueStore.swift"
TESTS = ("SyncAuditArtifactDecodingTests", "DurablePublicationEvidenceDecodingTests")


def git_blob(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def source(repo: Path, path: str, ref: str | None) -> bytes:
    if ref is None:
        return (repo / path).read_bytes()
    return subprocess.run(["git", "-C", str(repo), "show", f"{ref}:{path}"],
                          check=True, capture_output=True, timeout=30).stdout


def normalized_identity(value: str) -> str:
    apple = re.fullmatch(r"-\[([^ ]+) ([^\]]+)\]", value)
    if apple:
        return apple[1].split(".")[-1] + "." + apple[2]
    return ".".join(value.split(".")[-2:])


def prepare_types(audit: str, publication: str, errors: str) -> tuple[str, str, str]:
    # These boundaries select complete declarations for compilation; the tests
    # assert decoded values/errors, not whether source contains a phrase.
    audit_type = audit.split("\nextension RealmSwiftAdapter {", 1)[0]
    audit_type = audit_type.replace("import CloudKit\n", "").replace("import RealmSwift\n", "")
    publication_type = publication.split("\nextension CloudKitSynchronizer {", 1)[0]
    publication_type = publication_type.replace("import CloudKit\n", "")
    if "init(persistedValue raw: Any)" not in publication_type:
        # Original-source control: preserve the entire actual old private
        # decoder. The bridge supplies only its raw store value and constant.
        start = publication.index("    private func persistedDurablePublicationEvidence()")
        end = publication.index("    /// Restores terminal evidence", start)
        constant = re.search(r"private static let durablePublicationEvidenceVersion = \d+", publication)
        if constant is None:
            raise ValueError("Unrecognized original publication format")
        publication_type += """
private struct EvidenceControlStore {
    let value: Any
    func bigSyncDurableObject(forKey key: String) throws -> Any? { value }
}
private struct EvidenceControlDecoder {
    let keyValueStore: EvidenceControlStore
    let durablePublicationEvidenceKey = "TerminalPublication.v1"
""" + "    " + constant[0] + "\n" + publication[start:end] + """
    func decode() throws -> BigSyncDurablePublicationEvidence {
        try persistedDurablePublicationEvidence()!
    }
}
extension BigSyncDurablePublicationEvidence {
    init(persistedValue raw: Any) throws {
        self = try EvidenceControlDecoder(keyValueStore: EvidenceControlStore(value: raw)).decode()
    }
}
"""
    start = errors.index("public enum DurableKeyValueStoreError:")
    end = errors.index("\nextension KeyValueStore {", start)
    return audit_type, publication_type, "import Foundation\n" + errors[start:end]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--source-ref", help="Existing local commit/ref for production inputs only")
    parser.add_argument("--evidence", required=True, type=Path, help="New, nonexisting output directory")
    parser.add_argument("--configuration", choices=("debug", "release", "both"), default="both")
    parser.add_argument("--expect-failed-methods", type=int, default=0,
                        help="For intentional original-source/fault controls, never a passing qualification")
    args = parser.parse_args()
    if args.expect_failed_methods < 0:
        parser.error("Expected failed-method count must be nonnegative")
    repo = args.repository.expanduser().resolve()
    out = args.evidence.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=False)
    report: dict = {"scope": "Foundation evidence codecs only", "native_sdk_qualified": False,
                    "original_source_ref": args.source_ref, "commands": [], "results": {}}

    def save() -> None:
        (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    def run(argv: list[str], log: Path) -> int:
        entry = {"argv": argv, "log": log.name}
        report["commands"].append(entry)
        save()
        try:
            with log.open("xb") as output:
                result = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT, timeout=120)
            entry["exit"] = result.returncode
            entry["log_sha256"] = hashlib.sha256(log.read_bytes()).hexdigest()
            save()
            return result.returncode
        except subprocess.TimeoutExpired:
            entry["incomplete"] = "Timed out; not a behavioral pass"
            save()
            raise

    try:
        ref = args.source_ref
        if ref is not None:
            ref = subprocess.run(["git", "-C", str(repo), "rev-parse", "--verify", "--end-of-options",
                                  ref + "^{commit}"], check=True, capture_output=True, text=True,
                                 timeout=30).stdout.strip()
            report["resolved_source_commit"] = ref
        inputs = {path: source(repo, path, ref) for path in (AUDIT, PUBLICATION, ERRORS)}
        report["production_input_blobs"] = {path: git_blob(inputs[path]) for path in (AUDIT, PUBLICATION)}
        types = prepare_types(*(inputs[path].decode() for path in (AUDIT, PUBLICATION, ERRORS)))
        report["error_enum_sha256"] = hashlib.sha256(types[2].encode()).hexdigest()
        package = out / "package"
        source_dir = package / "Sources" / "BigSyncKit"
        tests_dir = package / "Tests" / "BigSyncKitTests"
        source_dir.mkdir(parents=True)
        tests_dir.mkdir(parents=True)
        (package / "Package.swift").write_text('''// swift-tools-version: 6.0
import PackageDescription
let package = Package(name: "SyncEvidenceCodecs", targets: [
    .target(name: "BigSyncKit"),
    .testTarget(name: "BigSyncKitTests", dependencies: ["BigSyncKit"]),
])
''')
        for name, text in zip(("Audit", "Publication", "Errors"), types):
            (source_dir / (name + ".swift")).write_text(text)
        expected: set[str] = set()
        report["test_input_blobs"] = {}
        for owner in TESTS:
            path = f"Tests/BigSyncKitTests/{owner}.swift"
            data = (repo / path).read_bytes()
            report["test_input_blobs"][path] = git_blob(data)
            (tests_dir / (owner + ".swift")).write_bytes(data)
            names = re.findall(r"\bfunc (test\w+)\(", data.decode())
            identities = [owner + "." + name for name in names]
            if not identities or len(set(identities)) != len(identities):
                raise ValueError("Missing or duplicated test identity")
            expected.update(identities)
        report["expected_method_identities"] = sorted(expected)
        report["limitations"] = [
            "No complete BigSync/Reader or native Apple SDK compilation, method discovery or execution",
            "No Realm audit enumeration, CloudKit/account lifecycle, storage durability or restoration journey",
            "Original publication control supplies a raw-value store around the verbatim old private decoder",
            "Optimized portable tests do not qualify application Release flags or performance",
        ]
        save()
        driver = ["xcrun", "--sdk", "macosx", "swift"] if platform.system() == "Darwin" else ["swift"]
        if run(driver + ["--version"], out / "toolchain.log") != 0:
            raise RuntimeError("Swift toolchain unavailable")
        configurations = ("debug", "release") if args.configuration == "both" else (args.configuration,)
        for configuration in configurations:
            log = out / (configuration + ".log")
            code = run(driver + ["test", "--package-path", str(package), "--scratch-path", str(out / "build"),
                        "-c", configuration, "-Xswiftc", "-warnings-as-errors"], log)
            executed = [(normalized_identity(name), status) for name, status in re.findall(
                r"Test Case '([^']+)' (passed|failed)", log.read_text())]
            names = [name for name, _ in executed]
            failed = [name for name, status in executed if status == "failed"]
            passed = [name for name, status in executed if status == "passed"]
            match = (len(names) == len(expected) and set(names) == expected
                     and len(failed) == args.expect_failed_methods
                     and code == (1 if args.expect_failed_methods else 0))
            report["results"][configuration] = {"exit": code, "passed": passed, "failed": failed,
                "matches_expected_outcome": match, "expected_failed_methods": args.expect_failed_methods}
            save()
            print(f"{configuration}: {len(passed)} passed, {len(failed)} failed; process exit {code}", flush=True)
            if not match:
                raise RuntimeError("Compiler/runtime outcome or exact method identities did not match")
        return 0
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        save()
        print(report["error"], file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
