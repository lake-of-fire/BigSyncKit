from pathlib import Path
import subprocess

ROOT = Path('.')

def replace(path, old, new, count=1):
    p = Path(path)
    text = p.read_text()
    actual = text.count(old)
    if actual != count:
        raise RuntimeError(f'{path}: expected {count} matches, got {actual}')
    p.write_text(text.replace(old, new))

sync = 'Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift'
replace(sync, '    private func cancelSynchronizationRequest(_ requestID: UUID) {', '''    /// Settle a current attempt even when CancellationError was thrown by an
    /// application hook, or its binding was revoked, without cancelling this
    /// Task. Do not use checkRunContext here: revoked authority is precisely
    /// why this owner must release its waiters. No suspension separates the
    /// ownership test from the existing cancellation cleanup.
    internal func settleCancellationIfCurrentAttempt(_ attemptID: UUID) {
        guard synchronizationAttemptID == attemptID,
              synchronizationDrainIsActive else { return }
        cancelSynchronization()
    }

    private func cancelSynchronizationRequest(_ requestID: UUID) {''')
replace(sync, '''        do {
            try checkRunContext(context)
        } catch { return }
        let needsFollowUp = synchronizationRequestedWhileRunning''', '''        do {
            try checkRunContext(context)
        } catch is CancellationError {
            settleCancellationIfCurrentAttempt(context.attemptID)
            return
        } catch {
            guard synchronizationAttemptID == context.attemptID else { return }
            await failSynchronization(error: error)
            return
        }
        let needsFollowUp = synchronizationRequestedWhileRunning''')

path = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift')
text = path.read_text()
start = text.index('    func changesFinishedSynchronizing() async {')
end = text.index('    func adaptersHavePendingChangesAtTerminalBoundary()', start)
terminal = text[start:end]
import re
terminal, count = re.subn(r'(\} catch is CancellationError \{\n)(\s*)return',
    r'\1\2settleCancellationIfCurrentAttempt(attemptID)\n\2return', terminal)
assert count >= 8, count
text = text[:start] + terminal + text[end:]
start = text.index('    func failSynchronization(error: Error) async {')
end = text.index('enum CloudKitRetryBackoff', start)
failure = text[start:end]
failure = failure.replace('''        let attemptID = synchronizationAttemptID
''', '''        let attemptID = synchronizationAttemptID
        if error is CancellationError {
            settleCancellationIfCurrentAttempt(attemptID)
            return
        }
''', 1)
failure, count = re.subn(r'(\} catch is CancellationError \{\n)(\s*)return',
    r'\1\2settleCancellationIfCurrentAttempt(attemptID)\n\2return', failure)
assert count == 2, count
# A synchronous delegate may cancel/start another attempt, too.
failure = failure.replace('''        self.delegate?.synchronizerDidfailToSync(self, error: error)
''', '''        self.delegate?.synchronizerDidfailToSync(self, error: error)
        guard synchronizationAttemptID == attemptID else { return }
''', 1)
# Corrupt-cursor recovery has the same suspension/ownership obligations as
# expired-token recovery. Never clear a replacement adapter's token.
failure = failure.replace('''                    for adapter in modelAdapters {
                        try await adapter.saveToken(nil)
                    }
                    shouldRetry = true''', '''                    for adapter in modelAdapters {
                        try checkSynchronizationAttempt(attemptID)
                        try await adapter.saveToken(nil)
                        try checkSynchronizationAttempt(attemptID)
                    }
                    shouldRetry = true''', 1)
# Diagnostic persistence may post synchronous notifications. It never owns
# a replacement drain, regardless of whether it succeeded or threw.
failure = failure.replace('''        guard shouldRetry, !cancelSync else {''', '''        guard synchronizationAttemptID == attemptID else { return }
        guard shouldRetry, !cancelSync else {''', 1)
text = text[:start] + failure + text[end:]
path.write_text(text)

replace('Tests/BigSyncKitTests/WorkerReviewReconciliationTests.swift',
    '''        try await sender.didUpload(savedRecords: batch.records,
                                   matchingGenerations: batch.matchingGenerations)''',
    '''        try await sender.acknowledgeUploadedRecords(batch.records, from: batch)''')

# The deeper branch must have been genuinely merged, not replaced by its tree.
evidence = Path('Sources/BigSyncKit/QSSynchronizer/BigSyncDurablePublicationEvidence.swift').read_text()
assert '''guard confirmedAccount == accountIdentifier,
              evidence.replicaBindingGenerationIdentifier == (try''' in evidence

tests = r'''
    @BigSyncBackgroundActor
    private func closureCancellationCase(scopeProvider: Bool,
                                         breaksDurability: Bool = false,
                                         downloadOnly: Bool = false) async throws {
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let fixture = try await reviewJournalBatch(count: 1)
        let synchronizer = makeSynchronizer(database: database, keyValueStore: store,
                                             recordZoneID: fixture.0.recordZoneID)
        synchronizer.addModelAdapter(fixture.0)
        synchronizer.syncMode = downloadOnly ? .downloadOnly : .sync
        let entered = expectation(description: "terminal hook entered")
        let secondStarted = expectation(description: "second caller started")
        let finished = expectation(description: "both callers settled")
        finished.expectedFulfillmentCount = 2
        let release = AsyncGate()
        let fail: @Sendable () async throws -> Void = {
            entered.fulfill()
            await release.wait()
            try await { @BigSyncBackgroundActor in
                if breaksDurability { store.synchronizesDurably = false }
                throw CancellationError()
            }()
        }
        if scopeProvider {
            synchronizer.domainPublicationScopeIdentifierProvider = {
                try await fail()
                return nil
            }
        } else {
            synchronizer.domainPrepublicationHandler = { _ in
                try await fail()
                return []
            }
        }
        let first = Task { @BigSyncBackgroundActor in
            do {
                _ = try await synchronizer.synchronize()
                XCTFail("Cancelled terminal work cannot publish success")
            } catch {
                XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)")
            }
            finished.fulfill()
        }
        await fulfillment(of: [entered], timeout: 5)
        let second = Task { @BigSyncBackgroundActor in
            secondStarted.fulfill()
            do {
                _ = try await synchronizer.synchronize()
                XCTFail("A coalesced caller must receive cancellation")
            } catch {
                XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)")
            }
            finished.fulfill()
        }
        await fulfillment(of: [secondStarted], timeout: 5)
        await release.open()
        await fulfillment(of: [finished], timeout: 5)
        XCTAssertFalse(synchronizer.syncing, "A current CancellationError must settle its drain")
        XCTAssertFalse(synchronizer.synchronizationDrainIsActive)
        XCTAssertNil(synchronizer.activeReceiptAuthorizationID)
        fixture.1.refresh()
        if downloadOnly {
            XCTAssertEqual(fixture.1.objects(BigSyncPendingMutation.self).count, 1)
            XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        }
        // Also bounds the negative control: do not leave abandoned waiters or
        // native tasks alive after a deliberately failed assertion.
        first.cancel()
        second.cancel()
        await synchronizer.cancelSynchronizationAndWait()
        await first.value
        await second.value
        store.synchronizesDurably = true
        synchronizer.domainPrepublicationHandler = nil
        synchronizer.domainPublicationScopeIdentifierProvider = nil
        synchronizer.syncMode = .sync
        let recovered = try await synchronizer.synchronize()
        XCTAssertNotNil(recovered.receipt)
        XCTAssertFalse(synchronizer.synchronizationDrainIsActive)
        fixture.1.refresh()
        XCTAssertTrue(fixture.1.objects(BigSyncPendingMutation.self).isEmpty)
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testClosurePrepublicationCancellationSettlesCoalescedCallers() async throws {
        try await closureCancellationCase(scopeProvider: false)
    }

    @BigSyncBackgroundActor
    func testClosureScopeProviderCancellationSettlesCoalescedCallers() async throws {
        try await closureCancellationCase(scopeProvider: true)
    }

    @BigSyncBackgroundActor
    func testClosureCancellationSettlementDoesNotDependOnHealthDurability() async throws {
        try await closureCancellationCase(scopeProvider: false, breaksDurability: true)
    }

    @BigSyncBackgroundActor
    func testClosureDownloadCancellationPreservesRealJournalAndRecovers() async throws {
        try await closureCancellationCase(scopeProvider: false, downloadOnly: true)
    }

    @BigSyncBackgroundActor
    func testClosureObsoleteCancellationCannotSettleReplacement() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(FakeModelAdapter(zoneID: synchronizer.recordZoneID, priorities: []))
        let obsolete = synchronizer.synchronizationAttemptID
        synchronizer.cancelSynchronization()
        synchronizer.cancelSync = false
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let replacement = synchronizer.synchronizationAttemptID
        synchronizer.settleCancellationIfCurrentAttempt(obsolete)
        XCTAssertEqual(synchronizer.synchronizationAttemptID, replacement)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertTrue(synchronizer.synchronizationDrainIsActive)
        synchronizer.settleCancellationIfCurrentAttempt(replacement)
        XCTAssertFalse(synchronizer.syncing)
        XCTAssertFalse(synchronizer.synchronizationDrainIsActive)
        await synchronizer.cancelSynchronizationAndWait()
    }
'''
replace('Tests/BigSyncKitTests/BigSyncKitTests.swift',
    'final class BigSyncKitTests: XCTestCase {\n',
    'final class BigSyncKitTests: XCTestCase {\n' + tests)
subprocess.run(['git', 'diff', '--check'], check=True)
print('Applied cancellation ownership cleanup, exact opaque batch acknowledgement and regressions')
