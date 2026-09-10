from pathlib import Path
import runpy
import sys

root = Path(sys.argv[1])
mode = sys.argv[2]
runpy.run_path(str(Path(__file__).with_name('health-review.py')), run_name='__main__')
if mode == 'tests':
    path = root / 'Tests/BigSyncKitTests/CloudKitTerminalReceiptTests.swift'
    text = path.read_text()
    old = '        var successorResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?\n'
    assert text.count(old) == 1
    text = text.replace(old, old + '        var successorTask: Task<Void, Never>?\n')
    old = '''            if replaceAtHealth {
                synchronizer.cancelSynchronization()
                synchronizer.beginSynchronization()
'''
    assert text.count(old) == 1
    text = text.replace(old, old + '                successorTask = synchronizer.synchronizationTask\n')
    old = '''        if replaceAtHealth {
            try await waitFor { successorEntered }
'''
    assert text.count(old) == 1
    text = text.replace(old, '''        if replaceAtHealth {
            // A stale retry can leave a nonnil task but replace B's actual task.
            // Compare the exact task captured synchronously by the observer.
            XCTAssertNotNil(successorTask)
            XCTAssertEqual(synchronizer.synchronizationTask, successorTask)
            try await waitFor { successorEntered }
''')
    path.write_text(text)
