from pathlib import Path

tests = Path('Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift')


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f'expected one anchor, found {count}: {old[:120]!r}')
    return text.replace(old, new, 1)


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
text = replace_once(text, anchor, new_tests + anchor)
tests.write_text(text)
