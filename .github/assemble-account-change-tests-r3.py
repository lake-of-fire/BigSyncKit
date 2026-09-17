from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])


def blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def load(path: str, expected: str):
    target = root / path
    data = target.read_bytes()
    actual = blob_sha(data)
    assert actual == expected, (path, expected, actual)
    return target, data.decode()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    assert count == 1, (label, count)
    return text.replace(old, new)

source_path, source = load(
    "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift",
    "baf8c0e1a1f31962b04df444a2d19d4ca3b4f42f",
)
source = replace_once(
    source,
    """#if DEBUG
                // Tests need the completion of this actor task, not an
                // intermediate attempt-ID rotation, as their synchronization
                // boundary. Keep the seam out of release builds.
                defer { _testAccountChangeObserverDidFinishHandler?() }
#endif
""",
    """#if DEBUG
                // Tests need the completion of this actor task, not an
                // intermediate attempt-ID rotation, as their synchronization
                // boundary. Keep the seam out of release builds and consume it
                // once so a later account-change cannot over-fulfill the same
                // XCTest expectation.
                defer { _testFinishAccountChangeObserverTask() }
#endif
""",
    "observer defer",
)
source = replace_once(
    source,
    """#if DEBUG
    @BigSyncBackgroundActor
    internal var _testAccountChangeObserverDidFinishHandler: (() -> Void)?

    @BigSyncBackgroundActor
    internal var processKillCheckpointHandler:
""",
    """#if DEBUG
    @BigSyncBackgroundActor
    internal var _testAccountChangeObserverDidFinishHandler: (() -> Void)?

    @BigSyncBackgroundActor
    private func _testFinishAccountChangeObserverTask() {
        let handler = _testAccountChangeObserverDidFinishHandler
        _testAccountChangeObserverDidFinishHandler = nil
        handler?()
    }

    @BigSyncBackgroundActor
    internal var processKillCheckpointHandler:
""",
    "one-shot debug helper",
)
source_path.write_text(source)
print(source_path.relative_to(root), blob_sha(source_path.read_bytes()))

tests_path, tests = load(
    "Tests/BigSyncKitTests/BigSyncKitTests.swift",
    "cd16026d51455c6530ac16a3965447a14602a424",
)
tests = replace_once(
    tests,
    """        let enteredConfirmation = expectation(description: "replacement confirmation entered")
        let invalidatedAccount = expectation(description: "account-change invalidation entered")
        let validationFinished = expectation(description: "superseded validation finished")
""",
    """        let enteredConfirmation = expectation(description: "replacement confirmation entered")
        let observerFinished = expectation(description: "account-change observer finished")
        let validationFinished = expectation(description: "superseded validation finished")
""",
    "stale validation expectations",
)
tests = replace_once(
    tests,
    """        XCTAssertTrue(synchronizer.modelAdapters.isEmpty)
        synchronizer.accountScopeInvalidationHandler = { reason in
            if reason == .accountChanged {
                invalidatedAccount.fulfill()
            }
        }
        try await synchronizer._test_validateSynchronizationAccount()
""",
    """        XCTAssertTrue(synchronizer.modelAdapters.isEmpty)
        synchronizer._testAccountChangeObserverDidFinishHandler = {
            observerFinished.fulfill()
        }
        try await synchronizer._test_validateSynchronizationAccount()
""",
    "stale validation exact observer seam",
)
tests = replace_once(
    tests,
    """        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [invalidatedAccount], timeout: 2)
        await releaseConfirmation.open()
""",
    """        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [observerFinished], timeout: 2)
        await releaseConfirmation.open()
""",
    "stale validation await observer completion",
)
tests_path.write_text(tests)
print(tests_path.relative_to(root), blob_sha(tests_path.read_bytes()))
