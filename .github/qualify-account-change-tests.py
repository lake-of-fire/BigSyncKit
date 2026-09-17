"""Qualify an immutable candidate using fakes, bounded gates and repeated XCTest runs."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

out = Path('../qualification').resolve()
out.mkdir(exist_ok=True)
results = []
names = [
    'testAccountChangeDuringReplacementConfirmationLeavesValidationRequired',
    'testAccountChangeRestartRejectsIntermediateValidationAttempt',
    'testSynchronizationAccountSwitchDurablyRequestsAdapterReconciliation',
    'testSynchronizationAccountSwitchRecreatesDatabaseSubscription',
]
pattern = '(' + '|'.join(names) + ')$'

def run(label, args, focused=False, timeout=900):
    log = out / (label + '.log')
    with log.open('w') as stream:
        try:
            code = subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            stream.write('\nQUALIFICATION_TIMEOUT\n')
            code = 124
    text = log.read_text()
    counts = [int(value) for value in re.findall(r'Executed (\d+) tests?', text)]
    passed_names = [name for name in names if re.search(r"Test Case '-\[BigSyncKitTests\.BigSyncKitTests " + name + r"\]' passed", text)]
    passed = code == 0 and bool(counts)
    if focused:
        passed = passed and max(counts, default=0) == len(names) and len(passed_names) == len(names)
    else:
        passed = passed and max(counts, default=0) >= 500 and len(passed_names) == len(names)
    item = dict(name=label, exit_code=code, passed=passed, executed=max(counts, default=0), required_passed=passed_names)
    results.append(item)
    (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(item), flush=True)
    for line in text.splitlines():
        if re.search(r': error:|^error:|Executed | failed | skipped ', line):
            print(line, flush=True)
    return passed

okay = run('focused', ['swift', 'test', '--filter', pattern], focused=True)
if okay:
    okay = run('full', ['swift', 'test', '--skip-build'])
if okay:
    for index in range(50):
        if not run(f'repeat-{index + 1:02}', ['swift', 'test', '--skip-build', '--filter', pattern], focused=True, timeout=120):
            okay = False
            break
subprocess.run(['git', 'diff', '--exit-code'], check=True)
with (out / 'final-identity.txt').open('w') as stream:
    subprocess.run(['git', 'rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests'], stdout=stream, check=True)
subprocess.run(['git', 'archive', '--format=tar.gz', '-o', str(out / 'source.tar.gz'), 'HEAD'], check=True)
files = sorted(path for path in out.iterdir() if path.is_file() and path.name != 'SHA256SUMS')
(out / 'SHA256SUMS').write_text(''.join(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n' for path in files))
sys.exit(0 if okay and len(results) == 52 and all(item['passed'] for item in results) else 1)
