from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])
mode = sys.argv[2]
def blob(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

tests = '''    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticBeginDoesNotNotifyForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .begin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticCancelAndBeginDoesNotNotifyForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .cancelAndBegin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessNotificationBeginDoesNotCallDelegateForSuccessor() async throws {
        try await assertTerminalSuccessOwnership(atNotification: true, action: .begin)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticCancellationSuppressesLaterObservers() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .cancel)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessDiagnosticAccountFenceSuppressesLaterObservers() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .fence)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessNotificationAccountFenceSuppressesDelegate() async throws {
        try await assertTerminalSuccessOwnership(atNotification: true, action: .fence)
    }

    @BigSyncBackgroundActor
    func testTerminalSuccessUnchangedOwnerNotifiesExactlyOnce() async throws {
        try await assertTerminalSuccessOwnership(atNotification: false, action: .none)
    }

    private enum TerminalObserverAction { case begin, cancelAndBegin, cancel, fence, none }

    @BigSyncBackgroundActor
    private func assertTerminalSuccessOwnership(
        atNotification: Bool, action: TerminalObserverAction
    ) async throws {
        let observer = ReceiptSuccessObserver()
        let fixture = Fixture(progressHandler: { milestone in
            guard milestone == "terminal-receipt" else { return }
            observer.diagnostics += 1
            if !atNotification { observer.reenter?() }
        })
        let synchronizer = fixture.synchronizer
        synchronizer.delegate = observer
        observer.reenter = { [weak synchronizer, weak observer] in
            guard let synchronizer, let observer else { return }
            observer.reenter = nil
            switch action {
            case .begin: synchronizer.beginSynchronization()
            case .cancelAndBegin:
                synchronizer.cancelSynchronization()
                synchronizer.beginSynchronization()
            case .cancel: synchronizer.cancelSynchronization()
            case .fence: synchronizer.accountScopeAuthorityFence.poison()
            case .none: break
            }
        }
        // Selector observers run synchronously on the posting thread. An
        // asynchronously scheduled Task would hide this reentrancy boundary.
        observer.reenterOnNotification = atNotification
        NotificationCenter.default.addObserver(observer,
            selector: #selector(ReceiptSuccessObserver.receiveSuccessNotification(_:)),
            name: .SynchronizerDidSynchronize, object: synchronizer)
        let release = ReceiptPause()
        var successorEntered = false
        var attempts = Set<UUID>()
        fixture.transport.databaseChangesHook = {
            let inserted = attempts.insert(synchronizer.synchronizationAttemptID).inserted
            if inserted && attempts.count == 2 {
                successorEntered = true
                await release.wait()
            }
        }
        defer {
            NotificationCenter.default.removeObserver(observer)
            observer.reenter = nil
            synchronizer.cancelSynchronization()
            Task { await release.release() }
        }
        let first = try await synchronizer.synchronize()
        // Waiter delivery precedes diagnostics, but the registered terminal
        // callback must have returned before inspecting its observer effects.
        try await waitFor { observer.diagnostics == 1 }
        XCTAssertEqual(first.publicationState, .complete)
        XCTAssertEqual(observer.notifications, atNotification || action == .none ? 1 : 0)
        XCTAssertEqual(observer.completions, action == .none ? 1 : 0)
        guard action == .begin || action == .cancelAndBegin else { return }
        try await waitFor { successorEntered }
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertNotNil(synchronizer.synchronizationTask)
        let successor = Task { @BigSyncBackgroundActor in try await synchronizer.synchronize() }
        try await waitFor { synchronizer._testSynchronizationWaiterCount == 1 }
        await release.release()
        let second = try await successor.value
        try await waitFor { observer.diagnostics == 2 }
        XCTAssertEqual(second.publicationState, .complete)
        XCTAssertNotEqual(first.receipt?.runID, second.receipt?.runID)
        XCTAssertEqual(observer.notifications, atNotification ? 2 : 1)
        XCTAssertEqual(observer.completions, 1)
        XCTAssertEqual(synchronizer._testSynchronizationWaiterCount, 0)
    }

'''
helper = '''
/// Invoked synchronously by the actor-isolated synchronizer, including the
/// selector notification callback; no Task hop may hide the reentry boundary.
@BigSyncBackgroundActor
private final class ReceiptSuccessObserver: NSObject, @preconcurrency CloudKitSynchronizerDelegate {
    var diagnostics = 0
    var notifications = 0
    var completions = 0
    var reenterOnNotification = false
    var reenter: (@BigSyncBackgroundActor () -> Void)?
    @objc func receiveSuccessNotification(_ notification: Notification) {
        notifications += 1
        if reenterOnNotification { reenter?() }
    }
    func synchronizerWillFetchChanges(_ synchronizer: CloudKitSynchronizer, in recordZone: CKRecordZone.ID) {}
    func synchronizerWillUploadChanges(_ synchronizer: CloudKitSynchronizer, to recordZone: CKRecordZone.ID) {}
    func synchronizerDidSync(_ synchronizer: CloudKitSynchronizer) { completions += 1 }
    func synchronizerDidfailToSync(_ synchronizer: CloudKitSynchronizer, error: Error) {}
    func synchronizer(_ synchronizer: CloudKitSynchronizer, zoneIDWasDeleted zoneID: CKRecordZone.ID) {}
}
'''
if mode == 'tests':
    p = root / 'Tests/BigSyncKitTests/CloudKitTerminalReceiptTests.swift'
    assert blob(p.read_bytes()) == '86d0290aad1e8c14abdf0b934bf33f690a87d6fe'
    s = p.read_text()
    marker = '    @BigSyncBackgroundActor\n    private func waitFor(_ predicate:'
    assert s.count(marker) == 1
    p.write_text(s.replace(marker, tests + marker) + helper)
    assert blob(p.read_bytes()) == '83dbc58999c2d218b2c22ccd4bbec33355a6b4fa'
elif mode == 'runtime':
    p = root / 'Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift'
    assert blob(p.read_bytes()) == '318396777be3dc04a7ba647a6da9aeb5a363c9d8'
    s = p.read_text()
    old = '''        reportProgress("terminal-receipt")
        postNotification(.SynchronizerDidSynchronize)
        delegate?.synchronizerDidSync(self)
'''
    new = '''        reportProgress("terminal-receipt")
        // Diagnostics and notifications are synchronous, reentrant observers.
        // Once one replaces or fences this run, do not tell the next observer
        // that the successor (which shares this synchronizer object) succeeded.
        do { try checkRunContext(terminalContext) } catch { return }
        postNotification(.SynchronizerDidSynchronize)
        do { try checkRunContext(terminalContext) } catch { return }
        delegate?.synchronizerDidSync(self)
'''
    assert s.count(old) == 1
    p.write_text(s.replace(old, new))
    assert blob(p.read_bytes()) == 'afb2b1e3d6a0c6cd7294f497e6cb2257d6d6c34c'
else:
    raise ValueError(mode)
