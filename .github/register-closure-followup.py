#!/usr/bin/env python3
from pathlib import Path
import subprocess

def replace_once(value, old, new):
    if value.count(old) != 1:
        raise RuntimeError('Unexpected source at reviewed boundary: ' + old[:100])
    return value.replace(old, new, 1)

path = Path('Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift')
value = path.read_text()
old = '''                    let evidence = try await synchronizer
                        .restoredDurablePublicationEvidence()
#if DEBUG
                    self.cloudKitE2ELastRestoredPublicationEvidence = evidence
#endif
                    try await restorationHandler(evidence)'''
new = '''                    try await synchronizer.restoreDurablePublicationEvidence(
                        deliveringTo: { evidence in
#if DEBUG
                            await self.recordRestoredPublicationEvidence(evidence)
#endif
                            try await restorationHandler(evidence)
                        }
                    )'''
if old in value:
    value = replace_once(value, old, new)
    anchor = '''    /// Waits only for configuration's pre-sync restoration task. It does not'''
    addition = '''    private func recordRestoredPublicationEvidence(
        _ evidence: BigSyncDurablePublicationEvidence?
    ) {
        cloudKitE2ELastRestoredPublicationEvidence = evidence
    }

'''
    value = replace_once(value, anchor, addition + anchor)
    path.write_text(value)

path = Path('Tests/BigSyncKitTests/BigSyncKitTests.swift')
value = path.read_text()
start = value.index('    func testReviewPartialSizeRetryKeepsNewerSuccessfulGenerationAndServerDelay()')
end = value.index('\n    @BigSyncBackgroundActor', start)
part = value[start:end]
old = '        sync.addModelAdapter(adapter)\n'
new = '''        sync.addModelAdapter(adapter)
        // This test drives the low-level phase. Own its drain before the
        // injected local edit, so the ordinary journal delegate coalesces
        // instead of starting a competing orchestration attempt.
        sync.syncing = true
        sync.synchronizationDrainIsActive = true
        sync.activeRunContext = reviewContext(sync)
'''
if 'Own its drain before' not in part:
    part = replace_once(part, old, new)
    part = replace_once(part, '        let error = try XCTUnwrap(observedError)\n', '''        let error = try XCTUnwrap(observedError)
        XCTAssertEqual((error as? CKError)?.code, .partialFailure)
        XCTAssertTrue(CloudKitRetryConstraints(error).requiresDeferredRetry)
''')
    value = value[:start] + part + value[end:]
    path.write_text(value)

path = Path('Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift')
value = path.read_text()
if 'func testClosureObsoleteNegativeRestorationDoesNotInvalidateNewLiveCompletion()' not in value:
    value += Path('.github/register-closure-restoration-tests.swift.txt').read_text()
    path.write_text(value)

subprocess.run(['git', 'diff', '--check'], check=True)
subprocess.run(['git', 'add', '--', 'Sources', 'Tests'], check=True)
if subprocess.run(['git', 'diff', '--cached', '--quiet']).returncode:
    subprocess.run(['git', '-c', 'user.name=github-actions[bot]', '-c', 'user.email=41898282+github-actions[bot]@users.noreply.github.com', 'commit', '-m', 'fix: fence restored publication delivery and isolate mixed-retry test authority'], check=True)
    subprocess.run(['git', 'push', 'origin', 'HEAD:refs/heads/fix/hotfix-register-closure-20260912'], check=True)
