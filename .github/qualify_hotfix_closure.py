"""Qualify committed source; restore every negative-control edit before continuing."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

results = []

def run(name, args, *, negative_marker=None, env=None):
    path = Path('qualification-' + name + '.log')
    with path.open('w') as output:
        try:
            code = subprocess.run(args, stdout=output, stderr=subprocess.STDOUT,
                                  timeout=900, env=env).returncode
        except subprocess.TimeoutExpired:
            output.write('\nQUALIFICATION_TIMEOUT\n')
            code = 124
    text = path.read_text()
    counts = [int(x) for x in re.findall(r'Executed (\d+) tests?', text)]
    passed = code == 0 and bool(counts) and max(counts) > 0
    if negative_marker:
        passed = code not in (0, 124) and bool(counts) and negative_marker in text
    results.append(dict(name=name, exit_code=code, expected_failure=bool(negative_marker),
                        passed=passed, executed=max(counts, default=0)))
    Path('qualification-results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(name, code, passed, flush=True)
    print('\n'.join(line for line in text.splitlines()
                    if re.search(r': error:|^error:|Executed | failed |benchmark:', line)), flush=True)
    return passed

pattern = 'testClosure|testReview|testReevaluation|testTerminalPublicationEvidenceRestoresOnlyForExactTransportBoundary'
focused = run('focused', ['swift', 'test', '--filter', pattern])
if focused:
    source = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift')
    original = source.read_text()
    start = original.index('    func changesFinishedSynchronizing() async {')
    end = original.index('    func adaptersHavePendingChangesAtTerminalBoundary()', start)
    broken, count = re.subn(r'^\s*settleCancellationIfCurrentAttempt\(attemptID\)\n', '',
                           original[start:end], flags=re.M)
    assert count >= 8
    try:
        source.write_text(original[:start] + broken + original[end:])
        run('negative-cancellation', ['swift', 'test', '--filter',
            'testClosurePrepublicationCancellationSettlesCoalescedCallers'],
            negative_marker='A current CancellationError must settle its drain')
    finally:
        source.write_text(original)
    source = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
    original = source.read_text()
    call = '                        try realmAdapter.prepareForFencedMigrationAfterCancellation()\n'
    assert original.count(call) == 1
    try:
        source.write_text(original.replace(call, '                        _ = realmAdapter\n'))
        run('negative-migration-restart', ['swift', 'test', '--filter',
            'testClosureExplicitDownloadCancellationResumesUnfinishedMigration'],
            negative_marker='caught error: "CancellationError()"')
    finally:
        source.write_text(original)
full = run('full', ['swift', 'test'], env=dict(os.environ, BIGSYNC_RUN_MUTATION_BENCHMARK='1'))
if focused and full:
    for index in range(10):
        if not run('repeat-' + str(index + 1),
                   ['swift', 'test', '--skip-build', '--filter', pattern]):
            break
subprocess.run(['git', 'diff', '--exit-code'], check=True)
if Path('Package.resolved').exists():
    Path('qualification-Package.resolved').write_bytes(Path('Package.resolved').read_bytes())
sys.exit(0 if len(results) == 14 and all(result['passed'] for result in results) else 1)
