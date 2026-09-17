import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import xml.etree.ElementTree as ET

BASE = 'fdb0e1313e2839db92126ae3e3ec76333195a045'
BRANCH = 'test/account-invalidation-failure-ordering-20260917'
SOURCE = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
NEW = [
    'testOlderSuccessfulInvalidationPreservesNewerFailedInvalidation',
    'testOlderFailedInvalidationDoesNotRestoreNewerCompletedInvalidation',
    'testUndurableAccountInvalidationDefersDomainCallbackUntilRetry',
]
REQUIRED = NEW + [
    'testAccountChangeObserverCompletionKeepsRegistrationOrder',
    'testAccountChangeObserverCompletionAllowsReverseFinishOrder',
    'testReplacementConfirmationRejectsSynchronousPoisonWithoutAttemptRotation',
    'testAccountChangeNotificationCompletesLocalDatasetRebootstrap',
    'testAccountChangeDuringReplacementConfirmationLeavesValidationRequired',
    'testAccountChangeRestartRejectsIntermediateValidationAttempt',
    'testSynchronizationAccountSwitchDurablyRequestsAdapterReconciliation',
    'testSynchronizationAccountSwitchRecreatesDatabaseSubscription',
    'testAccountChangeNotifiesDomainInvalidationAfterLeaseIsDurable',
    'testAccountChangeInvalidationFailurePreventsCloudKitAndRetries',
    'testAccountScopeLeaseIsStableUntilDurableInvalidation',
]

def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()

def checked(*args):
    subprocess.run(args, check=True)

def assemble():
    assert git('rev-parse', 'HEAD') == BASE
    assert not git('status', '--porcelain')
    payload = Path(__file__).with_name('invalidation-failure-ordering.patch.gz.b64')
    patch = gzip.decompress(base64.b64decode(payload.read_text()))
    assert hashlib.sha256(patch).hexdigest() == '5715da119c3d8e547b608a0d49c2949f6173dfbcc2e195086e770963c9f7abcc'
    path = payload.with_suffix('.decoded.patch').resolve()
    path.write_bytes(patch)
    checked('git', 'apply', '--check', str(path))
    checked('git', 'apply', str(path))
    checked('git', 'diff', '--check')
    expected = ['Tests/BigSyncKitTests/BigSyncKitTests.swift', 'Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift']
    assert git('diff', '--name-only').splitlines() == expected
    checked('git', 'config', 'user.name', 'github-actions[bot]')
    checked('git', 'config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
    checked('git', 'add', 'Tests')
    checked('git', 'commit', '-m', 'test: cover invalidation failure ownership and join gated validation cleanup')
    candidate = git('rev-parse', 'HEAD')
    assert git('rev-parse', 'HEAD:Sources') == git('rev-parse', BASE + ':Sources')
    assert not git('ls-remote', 'origin', 'refs/heads/' + BRANCH), 'Candidate branch already exists'
    checked('git', 'push', 'origin', 'HEAD:refs/heads/' + BRANCH)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write('candidate=' + candidate + '\n')
    print(json.dumps({'base': BASE, 'candidate': candidate}), flush=True)


def qualify(candidate):
    out = Path('../qualification-failures').resolve()
    out.mkdir(exist_ok=True)
    assert git('rev-parse', 'HEAD') == candidate
    original = SOURCE.read_bytes()
    results = []
    (out / 'initial-identity.txt').write_text(git('rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests') + '\n')

    def run(label, command, names, *, expected_failure=False, message=None, parallel=False, total=None, timeout=600):
        log_path = out / (label + '.log')
        xml_path = out / (label + '.xml')
        if parallel:
            command = command + ['--xunit-output', str(xml_path)]
        with log_path.open('w') as log:
            proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
                code = 124
                log.write('\nQUALIFICATION_TIMEOUT\n')
        text = log_path.read_text()
        cases = re.findall(r"Test Case '-\[[^ ]+ (\w+)\]' (passed|failed|skipped)", text)
        selected = {name for name, state in cases if state == ('failed' if expected_failure else 'passed')}
        okay = code == (1 if expected_failure else 0) and set(names).issubset(selected)
        if expected_failure:
            okay = okay and selected == set(names) and len(cases) == len(names)
        elif not parallel and total is not None:
            okay = okay and len(cases) == total
        if message:
            okay = okay and message in text
        if parallel:
            try:
                rows = ET.parse(xml_path).getroot().findall('.//testcase')
                selected = {row.attrib['name'] for row in rows}
                okay = code == 0 and selected == set(names) and len(rows) == len(names)
                okay = okay and all(not any(row.find(tag) is not None for tag in ('failure', 'error', 'skipped')) for row in rows)
            except (OSError, ET.ParseError, KeyError):
                okay = False
        okay = okay and 'leaked its continuation' not in text and 'QUALIFICATION_TIMEOUT' not in text
        item = {'label': label, 'command': command, 'exit_code': code, 'accepted': okay,
                'expected_failure': expected_failure, 'serial_cases': len(cases), 'named_outcomes': sorted(selected)}
        results.append(item)
        (out / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps(item), flush=True)
        if not okay:
            print('\n'.join(line for line in text.splitlines() if re.search(r' error:| failed|Executed|TIMEOUT', line)), flush=True)
            raise RuntimeError(label + ' did not meet its expected outcome')

    def mutation(label, old, new, name, message):
        assert original.count(old) == 1, label
        try:
            SOURCE.write_bytes(original.replace(old, new))
            run(label, ['swift', 'test', '--filter', name + '$'], [name], expected_failure=True, message=message)
        finally:
            SOURCE.write_bytes(original)

    try:
        pattern = '(' + '|'.join(REQUIRED) + ')$'
        run('focused', ['swift', 'test', '--filter', pattern], REQUIRED, total=len(REQUIRED))
        mutation('negative-clear-newer',
            b'        if pendingAccountScopeInvalidation?.id == pending.id {\n            pendingAccountScopeInvalidation = nil\n        }',
            b'        pendingAccountScopeInvalidation = nil', NEW[0],
            'An older success must not erase the newer failed invalidation')
        mutation('negative-requeue-stale',
            b'        try await accountScopeInvalidationHandler?(pending.reason)',
            b'        do {\n            try await accountScopeInvalidationHandler?(pending.reason)\n        } catch {\n            pendingAccountScopeInvalidation = pending\n            throw error\n        }', NEW[1],
            'An older failure must not resurrect completed invalidation debt')
        mutation('negative-callback-before-durability',
            b'        try invalidateAccountScopeLeaseDurably()\n        try await accountScopeInvalidationHandler?(pending.reason)',
            b'        try await accountScopeInvalidationHandler?(pending.reason)\n        try invalidateAccountScopeLeaseDurably()', NEW[2],
            'Domain invalidation must wait for durable lease revocation')
        run('restored-focused', ['swift', 'test', '--filter', pattern], REQUIRED, total=len(REQUIRED))
        run('full', ['swift', 'test', '--skip-build'], REQUIRED, total=517)
        for index in range(1, 21):
            run(f'serial-{index:02}', ['swift', 'test', '--skip-build', '--filter', pattern], REQUIRED, total=len(REQUIRED), timeout=120)
        for index in range(1, 11):
            run(f'parallel-{index:02}', ['swift', 'test', '--skip-build', '--parallel', '--num-workers', '4', '--filter', pattern], REQUIRED, parallel=True, timeout=120)
        assert len(results) == 36 and all(row['accepted'] for row in results)
    finally:
        SOURCE.write_bytes(original)
        checked('git', 'diff', '--exit-code')
        (out / 'final-identity.txt').write_text(git('rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests') + '\n')
        (out / 'source-status.txt').write_text(git('status', '--porcelain'))
        checked('git', 'archive', '--format=tar.gz', '-o', str(out / 'source.tar.gz'), candidate)
        (out / 'SHA256SUMS').write_text(''.join(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n' for path in sorted(out.iterdir()) if path.is_file() and path.name != 'SHA256SUMS'))

if __name__ == '__main__':
    if sys.argv[1] == 'assemble':
        assemble()
    elif sys.argv[1] == 'qualify':
        qualify(sys.argv[2])
    else:
        raise SystemExit('Expected assemble or qualify')
