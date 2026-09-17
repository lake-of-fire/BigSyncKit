#!/usr/bin/env python3
"""Assemble clean test/fix commits and qualify exact heads, including negative controls."""
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

BASE = 'b07889f9d913cf69f1c7887eb41fb108edbc5421'
BRANCH = 'fix/account-change-validation-tests-20260917'
SOURCE = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
HOOKS = [
    'testAccountChangeObserverCompletionKeepsRegistrationOrder',
    'testAccountChangeObserverCompletionAllowsReverseFinishOrder',
]
POISON = 'testReplacementConfirmationRejectsSynchronousPoisonWithoutAttemptRotation'
RECOVERY = 'testAccountChangeNotificationCompletesLocalDatasetRebootstrap'
REQUIRED = HOOKS + [POISON, RECOVERY,
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
    payload = Path(__file__).with_name('account-change-reassessment.patch.b64')
    patch = gzip.decompress(base64.b64decode(payload.read_text()))
    assert hashlib.sha256(patch).hexdigest() == 'f309ed72bebcfad2dada4276d33d10781a392aaa66653d1adf3e5f2aee0252f9'
    patch_path = payload.with_suffix('.decoded.patch').resolve()
    patch_path.write_bytes(patch)
    checked('git', 'apply', '--check', str(patch_path))
    checked('git', 'config', 'user.name', 'github-actions[bot]')
    checked('git', 'config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
    checked('git', 'apply', '--include=Tests/BigSyncKitTests/*', str(patch_path))
    checked('git', 'diff', '--check')
    checked('git', 'add', 'Tests')
    checked('git', 'commit', '-m', 'test: reproduce overlapping observer completion ownership and recovery boundaries')
    red = git('rev-parse', 'HEAD')
    assert git('rev-parse', 'HEAD:Sources') == git('rev-parse', BASE + ':Sources')
    checked('git', 'apply', '--include=Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift', str(patch_path))
    checked('git', 'diff', '--check')
    checked('git', 'add', str(SOURCE))
    checked('git', 'commit', '-m', 'test: bind account-change completion to observer task entry')
    green = git('rev-parse', 'HEAD')
    assert git('rev-parse', red + ':Tests') == git('rev-parse', green + ':Tests')
    remote = git('ls-remote', 'origin', 'refs/heads/' + BRANCH).split()[0]
    assert remote == BASE, ('PR head changed; refusing to overwrite', remote)
    checked('git', 'push', 'origin', 'HEAD:refs/heads/' + BRANCH)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write(f'red={red}\ngreen={green}\n')
    print(json.dumps({'base': BASE, 'red': red, 'green': green}), flush=True)


def qualify(red, green):
    output = Path('../qualification-deep-native').resolve()
    output.mkdir(exist_ok=True)
    results = []

    def run(name, command, required=(), failures=False, parallel=False, minimum=0, message=None):
        path = output / (name + '.log')
        xml_path = output / (name + '.xml')
        if parallel:
            command = command + ['--xunit-output', str(xml_path)]
        with path.open('w') as log:
            try:
                code = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=900).returncode
            except subprocess.TimeoutExpired:
                code = 124
                log.write('\nQUALIFICATION_TIMEOUT\n')
        text = path.read_text()
        cases = re.findall(r"Test Case '-\[[^ ]+ (\w+)\]' (passed|failed|skipped)", text)
        names = {name for name, state in cases if state == ('failed' if failures else 'passed')}
        executed = len(cases)
        valid = code == (1 if failures else 0) and set(required).issubset(names)
        if failures:
            valid = valid and len(names) == len(required) and bool(cases)
        if parallel:
            try:
                cases_xml = ET.parse(xml_path).getroot().findall('.//testcase')
                names = {case.attrib['name'] for case in cases_xml}
                executed = len(cases_xml)
                valid = code == 0 and names == set(required) and executed == len(required)
                valid = valid and all(not any(case.find(tag) is not None for tag in ('failure', 'error', 'skipped')) for case in cases_xml)
            except (OSError, ET.ParseError, KeyError):
                valid = False
        if minimum:
            valid = valid and executed >= minimum
        if message:
            valid = valid and message in text
        valid = valid and 'leaked its continuation' not in text
        row = {'name': name, 'command': command, 'exit_code': code, 'accepted': valid,
               'expected_failure': failures, 'executed': executed, 'matched': sorted(names)}
        results.append(row)
        (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps(row), flush=True)
        if not valid:
            for line in text.splitlines():
                if re.search(r': error:|^error:|Executed | failed|QUALIFICATION_', line):
                    print(line, flush=True)
            raise RuntimeError('Qualification failed: ' + name)

    checked('git', 'checkout', '--detach', red)
    (output / 'red-identity.txt').write_text(git('rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests') + '\n')
    try:
        run('red-overlap', ['swift', 'test', '--filter', '|'.join(HOOKS)], HOOKS, failures=True)
        checked('git', 'checkout', '--detach', green)
        assert git('rev-parse', red + ':Tests') == git('rev-parse', green + ':Tests')
        (output / 'source-identity.txt').write_text(git('rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests') + '\n')
        pattern = '|'.join(REQUIRED)
        run('green-focused', ['swift', 'test', '--filter', pattern], REQUIRED, minimum=len(REQUIRED))
        original = SOURCE.read_bytes()
        try:
            old = b'guard synchronizationAttemptID == attemptID,\n              fenceGeneration.map({\n                  accountScopeAuthorityFence.invalidationGenerationSnapshot == $0\n              }) ?? true else {'
            assert original.count(old) == 1
            SOURCE.write_bytes(original.replace(old, b'guard synchronizationAttemptID == attemptID else {'))
            run('negative-generation', ['swift', 'test', '--filter', POISON], [POISON], failures=True,
                message='A stale confirmation must not publish its durable account marker')
        finally:
            SOURCE.write_bytes(original)
        try:
            old = b'                if modelAdapterDictionary.count == 1 {\n                    beginSynchronization()\n                }'
            assert original.count(old) == 1
            SOURCE.write_bytes(original.replace(old, b'                // Negative control: omit the observer-owned restart.'))
            run('negative-restart', ['swift', 'test', '--filter', RECOVERY], [RECOVERY], failures=True,
                message='notification-owned recovery completed')
        finally:
            SOURCE.write_bytes(original)
        run('restored-focused', ['swift', 'test', '--filter', pattern], REQUIRED, minimum=len(REQUIRED))
        run('full', ['swift', 'test', '--skip-build'], REQUIRED, minimum=514)
        for index in range(1, 51):
            run(f'serial-{index:03}', ['swift', 'test', '--skip-build', '--filter', pattern], REQUIRED, minimum=len(REQUIRED))
        for index in range(1, 21):
            run(f'parallel-{index:02}', ['swift', 'test', '--skip-build', '--parallel', '--num-workers', '4', '--filter', pattern], REQUIRED, parallel=True)
        assert all(result['accepted'] for result in results)
    finally:
        checked('git', 'checkout', '--detach', green)
        checked('git', 'diff', '--exit-code')
        (output / 'final-identity.txt').write_text(git('rev-parse', 'HEAD', 'HEAD^{tree}', 'HEAD:Sources', 'HEAD:Tests') + '\n')
        (output / 'source-status.txt').write_text(git('status', '--porcelain'))
        checked('git', 'archive', '--format=tar.gz', '-o', str(output / 'source.tar.gz'), green)
        manifest = [hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name for path in sorted(output.iterdir()) if path.is_file() and path.name != 'SHA256SUMS']
        (output / 'SHA256SUMS').write_text('\n'.join(manifest) + '\n')

if __name__ == '__main__':
    if sys.argv[1] == 'assemble':
        assemble()
    elif sys.argv[1] == 'qualify':
        qualify(sys.argv[2], sys.argv[3])
    else:
        raise SystemExit('Expected assemble or qualify')
