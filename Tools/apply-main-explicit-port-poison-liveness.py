from pathlib import Path

source = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
tests = Path('Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift')


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f'expected one anchor, found {count}: {old[:100]!r}')
    return text.replace(old, new, 1)


text = source.read_text()
text = replace_once(text, '''    /// Serializes one non-suspending authority commit against synchronous
    /// poison delivery. A poison that wins first rejects the commit; a poison
    /// arriving during the body waits and immediately revokes its result.
    func withAuthorizedInvalidationGeneration<T>(
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
''', '''    /// Serializes one non-suspending writer-authority commit against
    /// synchronous poison delivery. This is intentionally unavailable while
    /// poisoned: ordinary writers may never reopen or adopt revoked authority.
    func withAuthorizedInvalidationGeneration<T>(
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

    /// Serializes a commit that is itself the recovery operation for a
    /// previously poisoned authority generation. Explicit account-port
    /// activation performs fresh account validation before entering here, so
    /// it may commit while ordinary writer authority remains closed. A newer
    /// poison must still win if it advanced the generation first.
    func withMatchingInvalidationGeneration<T>(
        _ expected: UInt64,
        _ body: () throws -> T
    ) rethrows -> T? {
        lock.lock()
        defer { lock.unlock() }
        guard invalidationGeneration == expected else { return nil }
        return try body()
    }
''')
text = replace_once(text, '''        let attemptID = synchronizationAttemptID
        guard let fenceGeneration = accountScopeAuthorityFence
            .authorizedInvalidationGenerationSnapshot else {
            throw CancellationError()
        }
        let accountIdentifier = try await accountIdentifierProvider()
''', '''        // Explicit account-port activation may legitimately begin while
        // ordinary writer authority is poisoned (including a fresh process),
        // but it must not race an unrelated restore or an account-invalidation
        // callback which still owns application cleanup.
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
''')
text = replace_once(text, '''        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )

        guard try accountScopeAuthorityFence
            .withAuthorizedInvalidationGeneration(fenceGeneration, {
''', '''        try checkAccountValidationAttempt(
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
''')
source.write_text(text)

text = tests.read_text()
text = replace_once(text, '''private actor AccountFencingAccountIdentity {
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
}
''', '''private actor AccountFencingAccountIdentity {
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
            // The production observer is queue:nil, so its authority poison is
            // synchronous even though actor-isolated cleanup runs later.
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
}
''')
anchor = '''    @BigSyncBackgroundActor
    func testPortActivationRejectsTheWrongDestinationAccount() async throws {
'''
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

        // Reopening models a settled account invalidation with a retained
        // durable port requirement: writer authority starts closed, while no
        // invalidation callback remains in flight.
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

        // Activation captures the current generation before its first fresh
        // provider read. Poison synchronously inside that read; the following
        // generation check must reject before the binding/account mutation.
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
text = replace_once(text, anchor, new_tests + anchor)
tests.write_text(text)
