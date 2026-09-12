#!/usr/bin/env python3
"""Qualify a clean, committed candidate and restore all negative controls."""
import json
import os
import pathlib
import re
import subprocess
import sys

results = []

def run(name, args, minimum=1, expected=None, env=None):
    path = pathlib.Path('qualification-' + name + '.log')
    with path.open('w') as output:
        try:
            code = subprocess.run(args, stdout=output, stderr=subprocess.STDOUT,
                                  timeout=900, env=env).returncode
        except subprocess.TimeoutExpired:
            output.write('\nQUALIFICATION_TIMEOUT\n')
            code = 124
    text = path.read_text()
    counts = [int(n) for n in re.findall(r'Executed (\d+) tests?', text)]
    passed = code == 0 and max(counts, default=0) >= minimum
    if expected is not None:
        passed = code not in (0, 124) and bool(counts) and expected in text
    results.append(dict(name=name, exit_code=code, passed=passed,
                        executed=max(counts, default=0), expected_failure=expected is not None))
    pathlib.Path('qualification-results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(name, 'exit', code, 'accepted', passed, flush=True)
    for line in text.splitlines():
        if re.search(r': error:|^error:|Executed | failed |REVIEW_', line):
            print(line, flush=True)
    return passed

pattern = 'testReview|testClosure|testReevaluation|testTerminalPublicationEvidenceRestoresOnlyForExactTransportBoundary'
subprocess.run(['git', 'diff', '--exit-code'], check=True)
focused = run('focused', ['swift', 'test', '--filter', pattern], minimum=40)
full = run('full', ['swift', 'test'], minimum=410,
           env=dict(os.environ, BIGSYNC_RUN_MUTATION_BENCHMARK='1'))
if focused and full:
    for i in range(1, 11):
        if not run('repeat-' + str(i), ['swift', 'test', '--skip-build', '--filter', pattern], minimum=40):
            break
    source = pathlib.Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
    original = source.read_bytes()
    old = b'if isNewlyCreatedReceiver\n            || mergePolicy'
    assert original.count(old) == 1
    try:
        source.write_bytes(original.replace(old, b'if false\n            || mergePolicy'))
        run('negative-creation', ['swift', 'test', '--filter',
            'testReviewFirstImportWithoutExplicitTimestampMustNotInventLocalAuthority'],
            expected='Constructor defaults are not a competing local revision')
    finally:
        source.write_bytes(original)
    helper = pathlib.Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Cancellation.swift')
    original_helper = helper.read_bytes()
    try:
        assert original_helper.count(b'        cancelSynchronization()') == 1
        helper.write_bytes(original_helper.replace(b'        cancelSynchronization()', b'        return'))
        run('negative-cancellation', ['swift', 'test', '--filter',
            'testClosureCurrentPrepublicationCancellationSettlesCoalescedWaiters'],
            expected='A current cancellation must settle the logical drain')
    finally:
        helper.write_bytes(original_helper)
    delivery = pathlib.Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+PublicationRestoration.swift')
    original_delivery = delivery.read_text()
    start = original_delivery.index('        guard synchronizationAttemptID == attemptID,')
    end = original_delivery.index('        try await handler(evidence)', start)
    try:
        delivery.write_text(original_delivery[:start] + original_delivery[end:])
        run('negative-restoration-delivery', ['swift', 'test', '--filter',
            'testClosureObsoleteNegativeRestorationDoesNotInvalidateNewLiveCompletion'],
            expected='Obsolete nil evidence must not discard a newer live completion')
    finally:
        delivery.write_text(original_delivery)
    run('restored-focused', ['swift', 'test', '--filter', pattern], minimum=40)
subprocess.run(['git', 'diff', '--exit-code'], check=True)
if pathlib.Path('Package.resolved').exists():
    pathlib.Path('qualification-Package.resolved').write_bytes(pathlib.Path('Package.resolved').read_bytes())
sys.exit(0 if len(results) == 16 and all(r['passed'] for r in results) else 1)
