"""Qualify the refined account-change test candidate on an immutable checkout."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import sys

out = Path('../qualification-r2').resolve()
out.mkdir(exist_ok=True)
results = []
names = [
    'testAccountChangeDuringReplacementConfirmationLeavesValidationRequired',
    'testAccountChangeRestartRejectsIntermediateValidationAttempt',
    'testSynchronizationAccountSwitchDurablyRequestsAdapterReconciliation',
    'testSynchronizationAccountSwitchRecreatesDatabaseSubscription',
    'testAccountChangeNotifiesDomainInvalidationAfterLeaseIsDurable',
    'testAccountChangeInvalidationFailurePreventsCloudKitAndRetries',
    'testAccountScopeLeaseIsStableUntilDurableInvalidation',
]
pattern = '(' + '|'.join(names) + ')$'


def run(label, args, require_tests=False, require_full=False, timeout=900):
    log = out / (label + '.log')
    with log.open('w') as stream:
        try:
            code = subprocess.run(
                args,
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            ).returncode
        except subprocess.TimeoutExpired:
            stream.write('\nQUALIFICATION_TIMEOUT\n')
            code = 124
    text = log.read_text(errors='replace')
    counts = [int(value) for value in re.findall(r'Executed (\d+) tests?', text)]
    passed_names = [
        name for name in names
        if re.search(
            r"Test Case '-\[[^]]+ " + re.escape(name) + r"\]' passed",
            text,
        )
    ]
    passed = code == 0
    if require_tests:
        passed = passed and len(passed_names) == len(names)
    if require_full:
        passed = passed and max(counts, default=0) >= 500
    item = {
        'name': label,
        'exit_code': code,
        'passed': passed,
        'executed': max(counts, default=0),
        'required_passed': passed_names,
    }
    results.append(item)
    (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(item), flush=True)
    for line in text.splitlines():
        if re.search(r': error:|^error:|Executed | failed | skipped ', line):
            print(line, flush=True)
    return passed


okay = run('focused', ['swift', 'test', '--filter', pattern], require_tests=True)
if okay:
    okay = run('full', ['swift', 'test', '--skip-build'], require_tests=True, require_full=True)
if okay:
    okay = run('release-build', ['swift', 'build', '-c', 'release'], timeout=900)
if okay:
    for index in range(100):
        if not run(
            f'repeat-{index + 1:03}',
            ['swift', 'test', '--skip-build', '--filter', pattern],
            require_tests=True,
            timeout=120,
        ):
            okay = False
            break
if okay:
    for index in range(20):
        if not run(
            f'parallel-{index + 1:02}',
            ['swift', 'test', '--skip-build', '--parallel', '--filter', pattern],
            require_tests=True,
            timeout=120,
        ):
            okay = False
            break

subprocess.run(['git', 'diff', '--exit-code'], check=True)
with (out / 'final-identity.txt').open('w') as stream:
    subprocess.run(
        ['git', 'rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests'],
        stdout=stream,
        check=True,
    )
subprocess.run(
    ['git', 'archive', '--format=tar.gz', '-o', str(out / 'source.tar.gz'), 'HEAD'],
    check=True,
)
files = sorted(path for path in out.iterdir() if path.is_file() and path.name != 'SHA256SUMS')
(out / 'SHA256SUMS').write_text(''.join(
    hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n'
    for path in files
))
expected_rows = 1 + 1 + 1 + 100 + 20
sys.exit(0 if okay and len(results) == expected_rows and all(item['passed'] for item in results) else 1)
