"""Run parallel XCTest stress and verify SwiftPM's parallel progress format."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import sys

out = Path('../qualification-parallel').resolve()
out.mkdir(exist_ok=True)
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
results = []

for index in range(20):
    label = f'parallel-{index + 1:02}'
    log = out / f'{label}.log'
    with log.open('w') as stream:
        try:
            code = subprocess.run(
                ['swift', 'test', '--skip-build', '--parallel', '--filter', pattern],
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=120,
            ).returncode
        except subprocess.TimeoutExpired:
            stream.write('\nQUALIFICATION_TIMEOUT\n')
            code = 124
    text = log.read_text(errors='replace')
    observed = re.findall(
        r'^\[\d+/7\] Testing [^\n]+/(test[^\s]+)$',
        text,
        flags=re.MULTILINE,
    )
    passed = code == 0 and sorted(observed) == sorted(names)
    item = {
        'name': label,
        'exit_code': code,
        'passed': passed,
        'observed_tests': observed,
    }
    results.append(item)
    (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(item), flush=True)
    if not passed:
        print(text, flush=True)
        break

subprocess.run(['git', 'diff', '--exit-code'], check=True)
with (out / 'final-identity.txt').open('w') as stream:
    subprocess.run(
        ['git', 'rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests'],
        stdout=stream,
        check=True,
    )
files = sorted(path for path in out.iterdir() if path.is_file() and path.name != 'SHA256SUMS')
(out / 'SHA256SUMS').write_text(''.join(
    hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n'
    for path in files
))
sys.exit(0 if len(results) == 20 and all(item['passed'] for item in results) else 1)
