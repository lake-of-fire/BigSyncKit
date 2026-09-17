from pathlib import Path
import hashlib
import sys

path = Path(sys.argv[1]) / 'Tests/BigSyncKitTests/BigSyncKitTests.swift'
original = path.read_bytes()
assert hashlib.sha1(b'blob ' + str(len(original)).encode() + b'\0' + original).hexdigest() == '1c4da86e40041921c118201efc61350195387b91'
text = original.decode()

def replace_once(old, new):
    global text
    assert text.count(old) == 1, (text.count(old), old[:100])
    text = text.replace(old, new)

replace_once('    private let enteredGate: AsyncGate\n', '    private let enteredConfirmation: XCTestExpectation\n')
replace_once('        enteredGate: AsyncGate,\n', '        enteredConfirmation: XCTestExpectation,\n')
replace_once('        self.enteredGate = enteredGate\n', '        self.enteredConfirmation = enteredConfirmation\n')
replace_once('            await enteredGate.open()\n', '            enteredConfirmation.fulfill()\n')

start = text.index('    func testSynchronizationAccountSwitchDurablyRequestsAdapterReconciliation()')
end = text.index('    @BigSyncBackgroundActor\n', start)
section = text[start:end]
old = '''        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()

'''
assert section.count(old) == 1
section = section.replace(old, '''        // Exercise account replacement directly. Posting a notification here
        // would also start an automatic drain competing with this validation.
        // The subscription test below covers the observer-owned restart.
''')
text = text[:start] + section + text[end:]

start = text.index('    @BigSyncBackgroundActor\n    func testAccountChangeDuringReplacementConfirmationLeavesValidationRequired()')
end = text.index('    @BigSyncBackgroundActor\n    func testSynchronizationAccountSwitchRecreatesDatabaseSubscription()', start)
text = text[:start] + '''    @BigSyncBackgroundActor
    func testAccountChangeDuringReplacementConfirmationLeavesValidationRequired()
    async throws {
        let database = FakeCloudKitDatabase()
        let enteredConfirmation = expectation(description: "replacement confirmation entered")
        let invalidatedAccount = expectation(description: "account-change invalidation entered")
        let validationFinished = expectation(description: "superseded validation finished")
        let releaseConfirmation = AsyncGate()
        let identifiers = GatedAccountIdentifierSequence(
            ["account-a", "account-b", "account-b", "account-c"],
            gatedCall: 3,
            enteredConfirmation: enteredConfirmation,
            releaseGate: releaseConfirmation
        )
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { await identifiers.next() }
        )
        // This unit test owns validation explicitly. With an adapter attached,
        // the notification observer also starts a drain: its cancel and begin
        // each rotate the attempt ID, so observing one rotation is not a
        // completion barrier. Test that restart separately below.
        XCTAssertTrue(synchronizer.modelAdapters.isEmpty)
        synchronizer.accountScopeInvalidationHandler = { reason in
            if reason == .accountChanged {
                invalidatedAccount.fulfill()
            }
        }
        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertFalse(synchronizer.accountValidationRequired)

        let validation = Task { @BigSyncBackgroundActor in
            defer { validationFinished.fulfill() }
            do {
                try await synchronizer._test_validateSynchronizationAccount()
                XCTFail("Expected the superseded validation to be cancelled")
            } catch is CancellationError {
                XCTAssertFalse(Task.isCancelled, "Authority supersession must reject an uncancelled task")
            } catch {
                XCTFail("Unexpected validation error: \\(error)")
            }
        }
        addTeardownBlock {
            validation.cancel()
            await releaseConfirmation.open()
        }
        await fulfillment(of: [enteredConfirmation], timeout: 2)

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [invalidatedAccount], timeout: 2)
        await releaseConfirmation.open()
        await fulfillment(of: [validationFinished], timeout: 2)

        // A stale confirmation must neither publish account-b nor clear the
        // requirement for a fresh validation of the newly reported account.
        XCTAssertTrue(synchronizer.accountValidationRequired)
        XCTAssertEqual(
            synchronizer.keyValueStore.object(
                forKey: synchronizer.durableStateKey("CloudKitAccountIdentifier")
            ) as? String,
            "account-a"
        )
        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertFalse(synchronizer.accountValidationRequired)
        XCTAssertEqual(
            synchronizer.keyValueStore.object(
                forKey: synchronizer.durableStateKey("CloudKitAccountIdentifier")
            ) as? String,
            "account-c"
        )
        XCTAssertEqual(database.subscriptionFetchCount, 0)
        XCTAssertEqual(database.databaseChangeFetchCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountChangeRestartRejectsIntermediateValidationAttempt()
    async throws {
        let enteredInvalidation = expectation(description: "observer invalidation suspended")
        let enteredValidation = expectation(description: "intermediate validation suspended")
        let restarted = expectation(description: "automatic restart validated its account")
        let validationFinished = expectation(description: "intermediate validation rejected")
        let releaseInvalidation = AsyncGate()
        let releaseValidation = AsyncGate()
        let identifiers = GatedAccountIdentifierSequence(
            ["account-a", "account-b", "account-b"],
            gatedCall: 2,
            enteredConfirmation: enteredValidation,
            releaseGate: releaseValidation
        )
        let synchronizer = makeSynchronizer(
            progressHandler: { checkpoint in
                if checkpoint == "account-identity-validated" {
                    restarted.fulfill()
                }
            },
            accountIdentifierProvider: { await identifiers.next() }
        )
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "account-change-restart-race"),
            priorities: []
        ))
        synchronizer.accountScopeInvalidationHandler = { reason in
            if reason == .accountChanged {
                enteredInvalidation.fulfill()
                await releaseInvalidation.wait()
            }
        }
        addTeardownBlock { @BigSyncBackgroundActor in
            // Prevent a still-suspended observer from starting work after a
            // timeout, then release every gate and retire any active drain.
            synchronizer.cancelSynchronization()
            synchronizer.modelAdapterDictionary.removeAll()
            await releaseInvalidation.open()
            await releaseValidation.open()
            await synchronizer.cancelSynchronizationAndWait()
        }
        try await synchronizer._test_validateSynchronizationAccount()
        let originalAttemptID = synchronizer.synchronizationAttemptID

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [enteredInvalidation], timeout: 2)
        let intermediateAttemptID = synchronizer.synchronizationAttemptID
        XCTAssertNotEqual(intermediateAttemptID, originalAttemptID)
        XCTAssertFalse(synchronizer.syncing)

        let validation = Task { @BigSyncBackgroundActor in
            defer { validationFinished.fulfill() }
            do {
                try await synchronizer._test_validateSynchronizationAccount()
                XCTFail("An intermediate attempt must not survive the observer restart")
            } catch is CancellationError {
                XCTAssertFalse(Task.isCancelled, "Authority supersession must reject an uncancelled task")
            } catch {
                XCTFail("Unexpected validation error: \\(error)")
            }
        }
        addTeardownBlock { validation.cancel() }
        await fulfillment(of: [enteredValidation], timeout: 2)

        // Force the precise formerly flaky ordering: the probe captured the
        // cancellation ID before the observer resumed and called begin.
        await releaseInvalidation.open()
        await fulfillment(of: [restarted], timeout: 2)
        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, intermediateAttemptID)
        await releaseValidation.open()
        await fulfillment(of: [validationFinished], timeout: 2)
        XCTAssertFalse(synchronizer.accountValidationRequired)
        XCTAssertEqual(
            synchronizer.keyValueStore.object(
                forKey: synchronizer.durableStateKey("CloudKitAccountIdentifier")
            ) as? String,
            "account-b"
        )
    }

''' + text[end:]

replace_once('''        backupDetectionBaseURL: URL? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
''', '''        backupDetectionBaseURL: URL? = nil,
        progressHandler: CloudKitSynchronizer.ProgressHandler? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
''')
replace_once('''            accountStatusProvider: { .available },
            backupDetectionBaseURL: backupDetectionBaseURL,
''', '''            accountStatusProvider: { .available },
            progressHandler: progressHandler,
            backupDetectionBaseURL: backupDetectionBaseURL,
''')
data = text.encode()
assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == '2535b2bd8b4b901946120b45d1786f4a7541ca2b'
path.write_bytes(data)
print('Patched only', path)
