#!/usr/bin/env python3
"""Compose immutable reviewed inputs and commit the bounded source repair.

Preparation is separate from qualification. Tests run on the resulting clean
commit, never on a hidden or uncommitted patch.
"""
from pathlib import Path
import re
import subprocess

DEEPER = 'e4089d86f49e3874753d37198b108551f9420eed'
BRANCH = 'fix/hotfix-register-closure-20260912'

def git(*args, **kwargs):
    return subprocess.run(['git', *args], check=True, **kwargs)

def text_at(ref, path):
    return subprocess.check_output(['git', 'show', f'{ref}:{path}']).decode()

def once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f'Unexpected source at edit boundary: {old[:100]!r}')
    return text.replace(old, new, 1)

git('config', 'user.name', 'github-actions[bot]')
git('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
git('fetch', 'origin', DEEPER)
if subprocess.run(['git', 'merge-base', '--is-ancestor', DEEPER, 'HEAD']).returncode:
    result = subprocess.run(['git', 'merge', '--no-commit', '--no-ff', DEEPER])
    if result.returncode:
        conflicts = subprocess.check_output(['git', 'diff', '--name-only', '--diff-filter=U']).decode().splitlines()
        for name in conflicts:
            if name == '.github/workflows/hotfix-reevaluation.yml':
                git('checkout', '--theirs', '--', name)
                git('add', '--', name)
            elif name.startswith('.github/') and '.patch' in name:
                git('rm', '-f', '--', name)
            elif name == 'Tests/BigSyncKitTests/BigSyncKitTests.swift':
                # The only deeper-only runtime test is composed below. Shared
                # cold-inspection test bodies were compared before this merge.
                git('checkout', '--ours', '--', name)
                git('add', '--', name)
            else:
                raise RuntimeError('Unreviewed merge conflict: ' + name)
    git('commit', '-m', 'merge: compose deeper restoration and latest BSK boundary repairs')

main_tests = Path('Tests/BigSyncKitTests/BigSyncKitTests.swift')
value = main_tests.read_text()
deep_tests = text_at(DEEPER, str(main_tests))
if 'private actor ReevaluationHeldCompletion {' not in value:
    actor = deep_tests.split('private actor ReevaluationHeldCompletion {', 1)[1].split('\nfinal class BigSyncKitTests:', 1)[0]
    value += '\nprivate actor ReevaluationHeldCompletion {' + actor
if 'func testReevaluationCanceledCallbackRetainsBarrierUntilQuiescent()' not in value:
    start = deep_tests.index('    @BigSyncBackgroundActor\n    func testReevaluationCanceledCallbackRetainsBarrierUntilQuiescent()')
    end = deep_tests.index('\n    @BigSyncBackgroundActor', start + 30)
    value += '\nextension BigSyncKitTests {\n' + deep_tests[start:end] + '\n}\n'
if 'func testClosureCurrentPrepublicationCancellationSettlesCoalescedWaiters()' not in value:
    value += Path('.github/register-closure-tests.swift.txt').read_text()
main_tests.write_text(value)

helper = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Cancellation.swift')
helper.write_text('''import Foundation

extension CloudKitSynchronizer {
    /// A thrown CancellationError does not imply that cancelSynchronization()
    /// already settled this drain. Only its owner may invoke that existing,
    /// non-suspending cleanup path. Obsolete callbacks are harmless no-ops.
    @BigSyncBackgroundActor
    func settleCancellation(ifOwnedBy attemptID: UUID) {
        guard synchronizationAttemptID == attemptID else { return }
        cancelSynchronization()
    }
}
''')

path = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift')
value = path.read_text()
head, tail = value.split('enum CloudKitRetryBackoff {', 1)
head = re.sub(r'(catch is CancellationError \{\n)( +)return\n',
              r'\1\2settleCancellation(ifOwnedBy: attemptID)\n\2return\n', head)
old = '        let attemptID = synchronizationAttemptID\n        logger.info("QSCloudKitSynchronizer >> Failing or backing off synchronization...")'
new = '''        let attemptID = synchronizationAttemptID
        if error is CancellationError {
            settleCancellation(ifOwnedBy: attemptID)
            return
        }
        logger.info("QSCloudKitSynchronizer >> Failing or backing off synchronization...")'''
if old in head: head = once(head, old, new)
path.write_text(head + 'enum CloudKitRetryBackoff {' + tail)

path = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
value = path.read_text()
start = value.index('    internal func publishSynchronizationResult(')
end = value.index('    internal func finishSynchronizationDrain(', start)
old = '        } catch { return }'
part = value[start:end]
if old in part:
    part = once(part, old, '''        } catch {
            settleCancellation(ifOwnedBy: context.attemptID)
            return
        }''')
    value = value[:start] + part + value[end:]
path.write_text(value)

path = Path('Tests/BigSyncKitTests/WorkerReviewReconciliationTests.swift')
value = path.read_text()
old = '''        try await sender.didUpload(savedRecords: batch.records,
                                   matchingGenerations: batch.matchingGenerations)'''
if old in value:
    value = once(value, old, '        try await sender.acknowledgeUploadedRecords(batch.records, from: batch)')
path.write_text(value)

for capsule in Path('.github').glob('*.patch*'):
    capsule.unlink()
git('diff', '--check')
git('add', '--', 'Sources', 'Tests', '.github')
if subprocess.run(['git', 'diff', '--cached', '--quiet']).returncode:
    git('commit', '-m', 'fix: settle owned cancellation and qualify the complete BSK findings register')
git('push', 'origin', 'HEAD:refs/heads/' + BRANCH)
git('diff', '--exit-code')
