#!/usr/bin/env python3
"""Copy the selected adapter's two owner methods verbatim into the portable lane.

The extraction intentionally requires the current four-space declaration layout.
A changed signature/layout fails rather than silently testing an old substitute.
Swift compiles the resulting source as part of the existing portable test run.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

METHODS = ("currentRecordEvidenceCut", "validateRecordEvidenceCut")


def extract(source: str) -> str:
    declarations: list[str] = []
    for name in METHODS:
        # Require exactly one method definition, including definitions whose
        # access or isolation changed. Do not select an arbitrary overload.
        definitions = list(re.finditer(r"(?m)^\s*(?:(?:private|fileprivate|public|internal|static)\s+)*func\s+"
                                      + re.escape(name) + r"\s*\(", source))
        if len(definitions) != 1:
            raise ValueError(f"{name}: expected exactly one definition")
        header = re.compile(r"(?m)^    @BigSyncBackgroundActor\n    func "
                            + re.escape(name) + r"\(")
        matches = list(header.finditer(source))
        if len(matches) != 1:
            raise ValueError(f"{name}: declaration layout or isolation changed")
        start = matches[0].start()
        body_start = source.find("{", matches[0].end())
        closing = re.search(r"(?m)^    \}\r?$", source[matches[0].end():])
        if body_start < 0 or closing is None:
            raise ValueError(f"{name}: missing function body or closing delimiter")
        end = matches[0].end() + closing.end()
        declaration = source[start:end]
        # These two small owner methods contain no strings/comments with braces.
        # Refuse newly introduced ambiguous lexical constructs: extend this
        # extractor deliberately alongside such a source change.
        body = source[body_start:end]
        if '"' in body or '/*' in body or '//' in body:
            raise ValueError(f"{name}: unsupported lexical construct in body")
        balance = 0
        for index, char in enumerate(body):
            if char == '{':
                balance += 1
            elif char == '}':
                balance -= 1
                if balance == 0 and index != len(body) - 1:
                    raise ValueError(f"{name}: premature closing delimiter")
                if balance < 0:
                    raise ValueError(f"{name}: unmatched closing delimiter")
        if balance != 0:
            raise ValueError(f"{name}: unmatched opening delimiter")
        declarations.append(declaration)
    return "import Foundation\nimport RealmSwift\n\nextension RealmSwiftAdapter {\n" + "\n\n".join(declarations) + "\n}\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    try:
        output = extract(args.source.read_text(encoding="utf-8"))
        # A fresh scratch destination is expected; never replace user source.
        with args.destination.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(output)
    except (OSError, UnicodeError, ValueError) as error:
        print(f"owner-method extraction failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
