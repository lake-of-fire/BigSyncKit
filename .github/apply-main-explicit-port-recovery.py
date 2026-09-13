#!/usr/bin/env python3
from pathlib import Path
import subprocess

SOURCE = Path("Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift")
TEST = Path("Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift")
EXPECTED_SOURCE_BLOB = "3fa1ecf2e66b2b64d2701eab9fa8532f21a52823"
EXPECTED_TEST_BLOB = "ed9d2af32c3412d83a6a217b8346bc101ad529c1"


def blob(path: Path) -> str:
    return subprocess.check_output(["git", "hash-object", str(path)], text=True).strip()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one preimage, found {count}")
    return text.replace(old, new, 1)


if blob(SOURCE) != EXPECTED_SOURCE_BLOB:
    raise SystemExit(f"unexpected source preimage: {blob(SOURCE)}")
if blob(TEST) != EXPECTED_TEST_BLOB:
    raise SystemExit(f"unexpected test preimage: {blob(TEST)}")

source = SOURCE.read_text()
source = replace_once(
    source,
    '''    func withAuthorizedInvalidationGeneration<T>(
        _ expected: UInt64,
        _ body: () throws -> T
    ) rethrows -> T? {
        lock.lock()
        defer { lock.unlock() }
        guard !isPoisoned, invalidationGeneration == expected else {
            return nil
        }
        return try body()
    }

    func poison(requiresGenerationRotation: Bool = true) {''',
    '''    func withAuthorizedInvalidationGeneration<T>(
        _ expected: UInt64,
        _ body: () throws -> T
    ) rethrows -> T? {
        lock.lock()
        defer { lock.unlock() }
        guard !isPoisoned, invalidationGeneration == expected else {
            return nil
        }
        return try body()
    }

    /// Serializes a commit which is itself recovery for this exact poisoned
    /// generation. Explicit account-port activation performs two fresh account
    /// reads before entering here, but must not reopen ordinary writer authority.
    /// A newer synchronous poison still wins by advancing the generation first.
    func withMatchingInvalidationGeneration<T>(
        _ expected: UInt64,
        _ body: () throws -> T
    ) rethrows -> T? {
        lock.lock()
        defer { lock.unlock() }
        guard invalidationGeneration == expected else { return nil }
        return try body()
    }

    func poison(requiresGenerationRotation: Bool = true) {''',
    "generation-only recovery primitive",
)

source = replace_once(
    source,
    '''    public func activateCloudAccountPort(
        _ expected: BigSyncCloudAccountPortRequirement
    ) async throws {
        guard accountReplacementPolicy == .requireExplicitDatasetPort,
              !syncing,
              !synchronizationDrainIsActive,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let attemptID = synchronizationAttemptID
        guard let fenceGeneration = accountScopeAuthorityFence
            .authorizedInvalidationGenerationSnapshot else {
            throw CancellationError()
        }
        let accountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        guard Self.accountScopeIdentifier(for: accountIdentifier)
                == expected.destinationAccountScopeIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let confirmedAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        guard confirmedAccountIdentifier == accountIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )

        guard try accountScopeAuthorityFence
            .withAuthorizedInvalidationGeneration(fenceGeneration, {
                _ = try BigSyncReplicaBindingStateStore.activatePort(
                    expected,
                    store: keyValueStore,
                    key: replicaBindingStateKey
                )
                try keyValueStore.bigSyncSetDurably(
                    value: confirmedAccountIdentifier,
                    forKey: cloudKitAccountIdentifierKey
                )
                return true
            }) == true else {
            throw CancellationError()
        }
        accountValidationRequired = true
        cancelSync = true
        portActivationRequiresWorkerRestart = true
    }''',
    '''    public func activateCloudAccountPort(
        _ expected: BigSyncCloudAccountPortRequirement
    ) async throws {
        guard accountReplacementPolicy == .requireExplicitDatasetPort,
              !syncing,
              !synchronizationDrainIsActive,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        // A durable pending port is the recovery gate itself. Activation may
        // therefore start while ordinary writer authority is poisoned, including
        // after process restart, but never while restore/account cleanup still
        // owns application-side invalidation.
        refreshBackupRestoreRequirement()
        guard backupDetectionError == nil,
              !backupRestoreDetected,
              pendingAccountScopeInvalidation == nil else {
            throw CancellationError()
        }
        let attemptID = synchronizationAttemptID
        let fenceGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        let accountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        guard Self.accountScopeIdentifier(for: accountIdentifier)
                == expected.destinationAccountScopeIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        let confirmedAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        guard confirmedAccountIdentifier == accountIdentifier,
              try pendingCloudAccountPortRequirement() == expected else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        refreshBackupRestoreRequirement()
        guard backupDetectionError == nil,
              !backupRestoreDetected,
              pendingAccountScopeInvalidation == nil else {
            throw CancellationError()
        }

        guard try accountScopeAuthorityFence
            .withMatchingInvalidationGeneration(fenceGeneration, {
                _ = try BigSyncReplicaBindingStateStore.activatePort(
                    expected,
                    store: keyValueStore,
                    key: replicaBindingStateKey
                )
                try keyValueStore.bigSyncSetDurably(
                    value: confirmedAccountIdentifier,
                    forKey: cloudKitAccountIdentifierKey
                )
                return true
            }) == true else {
            throw CancellationError()
        }
        accountValidationRequired = true
        cancelSync = true
        portActivationRequiresWorkerRestart = true
    }''',
    "explicit port activation",
)
SOURCE.write_text(source)

test = TEST.read_text()
test = replace_once(
    test,
    '''private actor AccountFencingAccountIdentity {
    private var identifier: String
    private var requestCount = 0

    init(_ identifier: String) {
        self.identifier = identifier
    }

    func current() -> String {
        requestCount += 1
        return identifier
    }

    func requests() -> Int { requestCount }

    func replace(with identifier: String) {
        self.identifier = identifier
    }
}''',
    '''private actor AccountFencingAccountIdentity {
    private var identifier: String
    private var requestCount = 0
    private var poisonsNextRead = false

    init(_ identifier: String) {
        self.identifier = identifier
    }

    func current() -> String {
        requestCount += 1
        if poisonsNextRead {
            poisonsNextRead = false
            // Production uses queue:nil, so authority poison is synchronous even
            // though actor-isolated application cleanup runs afterward.
            NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        }
        return identifier
    }

    func requests() -> Int { requestCount }

    func replace(with identifier: String) {
        self.identifier = identifier
    }

    func poisonOnNextRead() {
        poisonsNextRead = true
    }
}''',
    "test account identity poison hook",
)

marker = '''    @BigSyncBackgroundActor
    func testPortActivationRejectsTheWrongDestinationAccount() async throws {'''
new_tests = '''    @BigSyncBackgroundActor
    func testExplicitPortCanActivateAfterRestartWhileWriterAuthorityIsPoisoned()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "explicit-port-restart-\\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let first = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await first._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await first._test_validateSynchronizationAccount()
            XCTFail("Expected a port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }

        let reopened = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort
        )
        XCTAssertTrue(reopened.accountScopeAuthorityFence.rejectsAuthority)

        try await reopened.activateCloudAccountPort(requirement)

        XCTAssertNil(try reopened.pendingCloudAccountPortRequirement())
        XCTAssertTrue(reopened.accountScopeAuthorityFence.rejectsAuthority)
        XCTAssertNil(try reopened.accountScopeLease())
        do {
            _ = try await reopened.synchronize()
            XCTFail("Expected port activation to require worker restart")
        } catch let error as BigSyncCloudAccountPortError {
            XCTAssertEqual(error, .workerRestartRequired)
        }
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testExplicitPortCanActivateAfterSettledSynchronousAccountPoison()
    async throws {
        let transport = AccountFencingTransport()
        let store = AccountFencingStore()
        let identity = AccountFencingAccountIdentity("account-a")
        let identifier = "explicit-port-poison-\\(UUID().uuidString)"
        let zoneID = makeZoneID()
        let first = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await first._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await first._test_validateSynchronizationAccount()
            XCTFail("Expected a port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }

        let reopened = makeSynchronizer(
            transport: transport,
            store: store,
            identifier: identifier,
            recordZoneID: zoneID,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort
        )
        reopened.accountScopeAuthorityFence.poison(
            requiresGenerationRotation: false
        )
        XCTAssertTrue(reopened.accountScopeAuthorityFence.rejectsAuthority)

        try await reopened.activateCloudAccountPort(requirement)

        XCTAssertNil(try reopened.pendingCloudAccountPortRequirement())
        XCTAssertTrue(reopened.accountScopeAuthorityFence.rejectsAuthority)
        XCTAssertNil(try reopened.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testNewerAccountPoisonRejectsExplicitPortBeforeDurableActivation()
    async throws {
        let transport = AccountFencingTransport()
        let identity = AccountFencingAccountIdentity("account-a")
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")
        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected a port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let value) {
            requirement = value
        }

        await identity.poisonOnNextRead()
        do {
            try await synchronizer.activateCloudAccountPort(requirement)
            XCTFail("Expected newer account poison to reject activation")
        } catch is CancellationError {
            // Expected.
        }

        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
        XCTAssertTrue(synchronizer.accountScopeAuthorityFence.rejectsAuthority)
        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertEqual(transport.operationCount, 0)
    }

'''
test = replace_once(test, marker, new_tests + marker, "new explicit-port recovery tests")

obsolete = '''extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    func testExplicitPortActivationCannotAdoptAlreadyPoisonedAuthority()
    async throws {
        let identity = AccountFencingAccountIdentity("account-a")
        let authority = AccountAuthorityFenceReference()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountIdentifierProvider: { await identity.current() },
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { _ in }
        )
        authority.synchronizer = synchronizer
        try await synchronizer._test_validateSynchronizationAccount()
        await identity.replace(with: "account-b")

        let requirement: BigSyncCloudAccountPortRequirement
        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected an explicit dataset port requirement")
            return
        } catch BigSyncCloudAccountPortError.required(let pending) {
            requirement = pending
        }

        await authority.poison()
        do {
            try await synchronizer.activateCloudAccountPort(requirement)
            XCTFail("Expected poisoned port authority to be rejected")
        } catch is CancellationError {
        }

        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
    }
}
'''
test = replace_once(test, obsolete, "", "obsolete poisoned-forever test")
TEST.write_text(test)

print("patched", SOURCE, TEST)
