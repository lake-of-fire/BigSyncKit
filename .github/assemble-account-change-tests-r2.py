from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])


def git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def load(path: str, expected_sha: str) -> tuple[Path, str]:
    target = root / path
    data = target.read_bytes()
    actual = git_blob_sha(data)
    assert actual == expected_sha, (path, expected_sha, actual)
    return target, data.decode()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    assert count == 1, (label, count)
    return text.replace(old, new)

# 1. Give tests an exact completion boundary for the actor task spawned by the
# CKAccountChanged observer. This is DEBUG-only; release behavior is unchanged.
source_path, source = load(
    "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift",
    "3fa1ecf2e66b2b64d2701eab9fa8532f21a52823",
)
source = replace_once(
    source,
    """            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
                // Revoke run ownership before application invalidation can
""",
    """            Task { @BigSyncBackgroundActor [weak self] in
                guard let self else { return }
#if DEBUG
                // Tests need the completion of this actor task, not an
                // intermediate attempt-ID rotation, as their synchronization
                // boundary. Keep the seam out of release builds.
                defer { _testAccountChangeObserverDidFinishHandler?() }
#endif
                // Revoke run ownership before application invalidation can
""",
    "observer completion defer",
)
source = replace_once(
    source,
    """#if DEBUG
    @BigSyncBackgroundActor
    internal var processKillCheckpointHandler:
""",
    """#if DEBUG
    @BigSyncBackgroundActor
    internal var _testAccountChangeObserverDidFinishHandler: (() -> Void)?

    @BigSyncBackgroundActor
    internal var processKillCheckpointHandler:
""",
    "debug completion seam",
)
source_path.write_text(source)

# 2. Refine the monolithic fixture/tests: wait on exact callbacks, remove the
# generic progress checkpoint dependency, and eliminate subscription polling.
tests_path, tests = load(
    "Tests/BigSyncKitTests/BigSyncKitTests.swift",
    "2535b2bd8b4b901946120b45d1786f4a7541ca2b",
)
tests = replace_once(
    tests,
    """    var subscriptionSaveError: Error?
    var subscriptionDeleteError: Error?
""",
    """    var subscriptionSaveError: Error?
    var subscriptionDeleteError: Error?
    var subscriptionSaveHandler: (@Sendable (Int) -> Void)?
""",
    "subscription save callback property",
)
tests = replace_once(
    tests,
    """    func save(subscription: CKSubscription) async throws -> CKSubscription {
        savedSubscriptionCount += 1
        savedSubscriptions.append(subscription)
        if let subscriptionSaveError {
""",
    """    func save(subscription: CKSubscription) async throws -> CKSubscription {
        savedSubscriptionCount += 1
        savedSubscriptions.append(subscription)
        subscriptionSaveHandler?(savedSubscriptionCount)
        if let subscriptionSaveError {
""",
    "subscription save callback",
)
tests = replace_once(
    tests,
    """        let restarted = expectation(description: "automatic restart validated its account")
        let validationFinished = expectation(description: "intermediate validation rejected")
""",
    """        let observerFinished = expectation(description: "account-change observer finished")
        let validationFinished = expectation(description: "intermediate validation rejected")
""",
    "race observer expectation",
)
tests = replace_once(
    tests,
    """        let synchronizer = makeSynchronizer(
            progressHandler: { checkpoint in
                if checkpoint == "account-identity-validated" {
                    restarted.fulfill()
                }
            },
            accountIdentifierProvider: { await identifiers.next() }
        )
""",
    """        let synchronizer = makeSynchronizer(
            accountIdentifierProvider: { await identifiers.next() }
        )
""",
    "race remove generic progress callback",
)
tests = replace_once(
    tests,
    """        synchronizer.accountScopeInvalidationHandler = { reason in
            if reason == .accountChanged {
                enteredInvalidation.fulfill()
                await releaseInvalidation.wait()
            }
        }
        addTeardownBlock { @BigSyncBackgroundActor in
""",
    """        synchronizer.accountScopeInvalidationHandler = { reason in
            if reason == .accountChanged {
                enteredInvalidation.fulfill()
                await releaseInvalidation.wait()
            }
        }
        synchronizer._testAccountChangeObserverDidFinishHandler = {
            observerFinished.fulfill()
        }
        addTeardownBlock { @BigSyncBackgroundActor in
""",
    "race install exact observer seam",
)
tests = replace_once(
    tests,
    """        await releaseInvalidation.open()
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
""",
    """        await releaseInvalidation.open()
        await fulfillment(of: [observerFinished], timeout: 2)
        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, intermediateAttemptID)
        XCTAssertTrue(synchronizer.syncing)
        await releaseValidation.open()
        await fulfillment(of: [validationFinished], timeout: 2)
""",
    "race wait on observer completion",
)
tests = replace_once(
    tests,
    """        try await synchronizer._test_validateSynchronizationAccount()
        try await synchronizer.subscribeForChangesInDatabase()
        XCTAssertEqual(database.savedSubscriptionCount, 1)

        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        // The account-change observer owns the recovery wakeup. Issuing a
        // second explicit begin here can race the observer and create a tail
        // drain, making an exact save-count assertion scheduler-dependent.
        await Task.yield()

        for _ in 0..<1_000 where database.savedSubscriptionCount < 2 {
            try await Task.sleep(nanoseconds: 1_000_000)
        }

        XCTAssertEqual(database.savedSubscriptionCount, 2)
        await synchronizer.cancelSynchronizationAndWait()
""",
    """        try await synchronizer._test_validateSynchronizationAccount()
        try await synchronizer.subscribeForChangesInDatabase()
        XCTAssertEqual(database.savedSubscriptionCount, 1)

        let replacementSubscriptionSaved = expectation(
            description: "replacement account subscription saved"
        )
        database.subscriptionSaveHandler = { saveCount in
            if saveCount == 2 {
                replacementSubscriptionSaved.fulfill()
            }
        }
        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [replacementSubscriptionSaved], timeout: 2)

        XCTAssertEqual(database.savedSubscriptionCount, 2)
        database.subscriptionSaveHandler = nil
        await synchronizer.cancelSynchronizationAndWait()
""",
    "subscription deterministic callback",
)
tests = replace_once(
    tests,
    """        backupDetectionBaseURL: URL? = nil,
        progressHandler: CloudKitSynchronizer.ProgressHandler? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
""",
    """        backupDetectionBaseURL: URL? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
""",
    "remove helper progress parameter",
)
tests = replace_once(
    tests,
    """            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: { .available },
            progressHandler: progressHandler,
            backupDetectionBaseURL: backupDetectionBaseURL,
""",
    """            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: { .available },
            backupDetectionBaseURL: backupDetectionBaseURL,
""",
    "remove helper progress argument",
)
tests_path.write_text(tests)

# 3. Replace the same account-change polling pattern in the dedicated fencing
# suite with the exact observer-task completion seam.
fencing_path, fencing = load(
    "Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift",
    "ed9d2af32c3412d83a6a217b8346bc101ad529c1",
)
fencing = replace_once(
    fencing,
    """        try await synchronizer._test_validateSynchronizationAccount()
        let establishedLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
""",
    """        try await synchronizer._test_validateSynchronizationAccount()
        let establishedLease = try XCTUnwrap(
            synchronizer.accountScopeLease()
        )
        let observerFinished = expectation(
            description: "account-change observer finished"
        )
        synchronizer._testAccountChangeObserverDidFinishHandler = {
            observerFinished.fulfill()
        }

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
""",
    "lease invalidation observer expectation",
)
fencing = replace_once(
    fencing,
    """        for _ in 0..<100 {
            if !(await recorder.reasons).isEmpty { break }
            await Task.yield()
        }

        let reasons = await recorder.reasons
""",
    """        await fulfillment(of: [observerFinished], timeout: 2)

        let reasons = await recorder.reasons
""",
    "lease invalidation remove polling",
)
fencing = replace_once(
    fencing,
    """        synchronizer.accountScopeInvalidationHandler = { _ in
            try await invalidation.run()
        }

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        for _ in 0..<100 {
            if await invalidation.attempts > 0 { break }
            await Task.yield()
        }
        for _ in 0..<10 { await Task.yield() }

        let attemptsAfterFailure = await invalidation.attempts
""",
    """        synchronizer.accountScopeInvalidationHandler = { _ in
            try await invalidation.run()
        }
        let observerFinished = expectation(
            description: "failed account-change observer finished"
        )
        synchronizer._testAccountChangeObserverDidFinishHandler = {
            observerFinished.fulfill()
        }

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await fulfillment(of: [observerFinished], timeout: 2)

        let attemptsAfterFailure = await invalidation.attempts
""",
    "invalidation failure remove polling",
)
fencing = replace_once(
    fencing,
    """        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        XCTAssertNil(try synchronizer.accountScopeLease())
        for _ in 0..<100 where !synchronizer.accountValidationRequired {
            await Task.yield()
        }
        XCTAssertTrue(synchronizer.accountValidationRequired)
""",
    """        let observerFinished = expectation(
            description: "account-change observer finished"
        )
        synchronizer._testAccountChangeObserverDidFinishHandler = {
            observerFinished.fulfill()
        }
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        XCTAssertNil(try synchronizer.accountScopeLease())
        await fulfillment(of: [observerFinished], timeout: 2)
        XCTAssertTrue(synchronizer.accountValidationRequired)
""",
    "stable lease remove polling",
)
fencing_path.write_text(fencing)

for path in (source_path, tests_path, fencing_path):
    data = path.read_bytes()
    print(path.relative_to(root), git_blob_sha(data), len(data))
