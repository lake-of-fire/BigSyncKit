from pathlib import Path
import sys
root = Path(sys.argv[1])
mode = sys.argv[2]
tests = '''

extension CloudKitTerminalReceiptTests {
    @BigSyncBackgroundActor
    func testFailureFenceAtNotificationReleasesAllWaiters() async throws {
        try await assertFailureHealthOwnership(retryable: false, replaceAtHealth: false)
    }

    @BigSyncBackgroundActor
    func testRetryableFailureFenceAtNotificationReleasesAllWaiters() async throws {
        try await assertFailureHealthOwnership(retryable: true, replaceAtHealth: false)
    }

    @BigSyncBackgroundActor
    func testFailureHealthNotificationPreservesSuccessorDrain() async throws {
        try await assertFailureHealthOwnership(retryable: false, replaceAtHealth: true)
    }

    @BigSyncBackgroundActor
    func testRetryableFailureHealthNotificationPreservesSuccessorDrain() async throws {
        try await assertFailureHealthOwnership(retryable: true, replaceAtHealth: true)
    }

    @BigSyncBackgroundActor
    private func assertFailureHealthOwnership(retryable: Bool, replaceAtHealth: Bool) async throws {
        let fixture = Fixture()
        let synchronizer = fixture.synchronizer
        let observer = ReceiptFailureHealthObserver()
        let entered = ReceiptPause(), release = ReceiptPause(), successorRelease = ReceiptPause()
        var calls = 0
        var successorEntered = false
        var firstResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        var secondResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        var successorResult: Result<CloudKitSynchronizer.SynchronizationResult, Error>?
        // Preserve retry semantics without spending the five-second default
        // fallback budget before the successor reaches our held transport.
        let originalError = retryable
            ? CKError(.networkFailure, userInfo: [CKErrorRetryAfterKey: 0])
            : CKError(.permissionFailure)
        observer.reenter = { [weak synchronizer] in
            guard let synchronizer else { return }
            if replaceAtHealth {
                synchronizer.cancelSynchronization()
                synchronizer.beginSynchronization()
            } else {
                // Models the immediate account fence before queued cancellation
                // has a chance to rotate the logical synchronization attempt.
                synchronizer.accountScopeAuthorityFence.poison()
            }
        }
        observer.atHealth = replaceAtHealth
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptFailureHealthObserver.receiveFailure(_:)),
            name: .SynchronizerDidFailToSynchronize, object: synchronizer)
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptFailureHealthObserver.receiveHealth(_:)),
            name: .SynchronizerSyncHealthDidChange, object: synchronizer)
        fixture.transport.databaseChangesHook = {
            calls += 1
            if calls == 1 {
                await entered.release()
                await release.wait()
                throw originalError
            }
            if calls == 2 {
                successorEntered = true
                await successorRelease.wait()
            }
        }
        let first = Task { @BigSyncBackgroundActor in
            do { firstResult = .success(try await synchronizer.synchronize()) }
            catch { firstResult = .failure(error) }
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
            observer.reenter = nil
            synchronizer.cancelSynchronization()
            first.cancel()
            Task { await release.release(); await successorRelease.release() }
        }
        await entered.wait()
        let second = Task { @BigSyncBackgroundActor in
            do { secondResult = .success(try await synchronizer.synchronize()) }
            catch { secondResult = .failure(error) }
        }
        defer { second.cancel() }
        try await waitFor { synchronizer._testSynchronizationWaiterCount == 2 }
        await release.release()
        try await waitFor { observer.reentries == 1 }
        try await waitFor { firstResult != nil && secondResult != nil }
        for result in [firstResult, secondResult] {
            guard case .failure(let error) = try XCTUnwrap(result) else {
                XCTFail("A fenced failure must not deliver success")
                continue
            }
            XCTAssertTrue(error is CancellationError)
        }
        if replaceAtHealth {
            try await waitFor { successorEntered }
            XCTAssertTrue(synchronizer.syncing)
            XCTAssertNotNil(synchronizer.synchronizationTask)
            let successorAttempt = synchronizer.synchronizationAttemptID
            let successor = Task { @BigSyncBackgroundActor in
                do { successorResult = .success(try await synchronizer.synchronize()) }
                catch { successorResult = .failure(error) }
            }
            defer { successor.cancel() }
            try await waitFor { synchronizer._testSynchronizationWaiterCount == 1 }
            XCTAssertEqual(synchronizer.synchronizationAttemptID, successorAttempt)
            await successorRelease.release()
            try await waitFor { successorResult != nil }
            let result = try XCTUnwrap(successorResult).get()
            XCTAssertEqual(result.publicationState, .complete)
            // Joining a live drain can legitimately request a tail attempt.
            // Ownership was checked while B was suspended, not after its tail.
            XCTAssertNil(synchronizer.retrySleepUntil)
        } else {
            XCTAssertTrue(synchronizer.cancelSync)
            XCTAssertFalse(synchronizer.syncing)
            XCTAssertNil(synchronizer.synchronizationTask)
            XCTAssertNil(synchronizer.retrySleepUntil)
        }
        XCTAssertEqual(synchronizer._testSynchronizationWaiterCount, 0)
    }
}

@BigSyncBackgroundActor
private final class ReceiptFailureHealthObserver: NSObject {
    var atHealth = false
    var failureSeen = false
    var reentries = 0
    var reenter: (@BigSyncBackgroundActor () -> Void)?
    @objc func receiveFailure(_ notification: Notification) {
        failureSeen = true
        if !atHealth { invokeOnce() }
    }
    @objc func receiveHealth(_ notification: Notification) {
        guard atHealth, failureSeen else { return }
        invokeOnce()
    }
    private func invokeOnce() {
        guard let callback = reenter else { return }
        reenter = nil
        reentries += 1
        callback()
    }
}
'''
if mode == 'tests':
    path = root/'Tests/BigSyncKitTests/CloudKitTerminalReceiptTests.swift'
    assert 'testFailureFenceAtNotificationReleasesAllWaiters' not in path.read_text()
    path.write_text(path.read_text() + tests)
elif mode == 'runtime':
    p = root/'Sources/BigSyncKit/QSSynchronizer/CloudKitSyncHealth.swift'
    old = '''            userInfo: [cloudKitSynchronizerSyncHealthSnapshotKey: snapshot]
        )
    }
'''
    new = '''            userInfo: [cloudKitSynchronizerSyncHealthSnapshotKey: snapshot]
        )
        // NotificationCenter delivery is synchronous and may cancel, fence,
        // or replace the run. Never let the caller finish a successor's drain.
        try checkRunContext(context)
    }
'''
    text = p.read_text(); assert text.count(old) == 1
    p.write_text(text.replace(old, new))
    p = root/'Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift'
    old = '''            } catch is CancellationError {
                return
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed to persist sync health: \\(error)")
'''
    new = '''            } catch is CancellationError {
                // A notification can revoke authority without rotating this
                // attempt yet. Close its waiters rather than stranding them.
                // A replacement attempt owns its own drain and must survive.
                guard synchronizationAttemptID == attemptID else { return }
                cancelSynchronization()
                return
            } catch {
                logger.error("QSCloudKitSynchronizer >> Failed to persist sync health: \\(error)")
'''
    text = p.read_text(); assert text.count(old) == 1
    p.write_text(text.replace(old, new))
else:
    raise ValueError(mode)
