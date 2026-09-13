from pathlib import Path

source_path = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')
test_path = Path('Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift')
source = source_path.read_text()
tests = test_path.read_text()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f'{label}: expected one match, found {count}')
    return text.replace(old, new, 1)


source = replace_once(
    source,
    '''    var invalidationGenerationSnapshot: UInt64 {\n        lock.lock()\n        defer { lock.unlock() }\n        return invalidationGeneration\n    }\n\n    func poison(requiresGenerationRotation: Bool = true) {\n''',
    '''    var invalidationGenerationSnapshot: UInt64 {\n        lock.lock()\n        defer { lock.unlock() }\n        return invalidationGeneration\n    }\n\n    /// Captures authority only while the synchronous fence is open. Callers\n    /// that are themselves responsible for fresh validation use the raw\n    /// generation snapshot instead; external commit paths must not adopt an\n    /// already-poisoned generation.\n    var authorizedInvalidationGenerationSnapshot: UInt64? {\n        lock.lock()\n        defer { lock.unlock() }\n        guard !isPoisoned else { return nil }\n        return invalidationGeneration\n    }\n\n    /// Serializes one non-suspending authority commit against synchronous\n    /// poison delivery. A poison that wins first rejects the commit; a poison\n    /// arriving during the body waits and immediately revokes its result.\n    func withAuthorizedInvalidationGeneration<T>(\n        _ expected: UInt64,\n        _ body: () throws -> T\n    ) rethrows -> T? {\n        lock.lock()\n        defer { lock.unlock() }\n        guard !isPoisoned, invalidationGeneration == expected else {\n            return nil\n        }\n        return try body()\n    }\n\n    func poison(requiresGenerationRotation: Bool = true) {\n''',
    'authorized generation primitives'
)

source = replace_once(
    source,
    '''        let attemptID = synchronizationAttemptID\n        let fenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        let accountIdentifier = try await accountIdentifierProvider()\n''',
    '''        let attemptID = synchronizationAttemptID\n        guard let fenceGeneration = accountScopeAuthorityFence\n            .authorizedInvalidationGenerationSnapshot else {\n            throw CancellationError()\n        }\n        let accountIdentifier = try await accountIdentifierProvider()\n''',
    'explicit port authorized snapshot'
)

source = replace_once(
    source,
    '''        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n\n        _ = try BigSyncReplicaBindingStateStore.activatePort(\n            expected,\n            store: keyValueStore,\n            key: replicaBindingStateKey\n        )\n        try keyValueStore.bigSyncSetDurably(\n            value: confirmedAccountIdentifier,\n            forKey: cloudKitAccountIdentifierKey\n        )\n        accountValidationRequired = true\n        cancelSync = true\n        portActivationRequiresWorkerRestart = true\n''',
    '''        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n\n        guard try accountScopeAuthorityFence\n            .withAuthorizedInvalidationGeneration(fenceGeneration, {\n                _ = try BigSyncReplicaBindingStateStore.activatePort(\n                    expected,\n                    store: keyValueStore,\n                    key: replicaBindingStateKey\n                )\n                try keyValueStore.bigSyncSetDurably(\n                    value: confirmedAccountIdentifier,\n                    forKey: cloudKitAccountIdentifierKey\n                )\n                return true\n            }) == true else {\n            throw CancellationError()\n        }\n        accountValidationRequired = true\n        cancelSync = true\n        portActivationRequiresWorkerRestart = true\n''',
    'explicit port atomic authority commit'
)

# Clean the indentation defects inherited from the original one-file PR while
# the same focused suite is being requalified.
source = replace_once(
    source,
    '''            try checkAccountValidationAttempt(\n            validationAttemptID,\n            fenceGeneration: validationFenceGeneration\n        )\n''',
    '''            try checkAccountValidationAttempt(\n                validationAttemptID,\n                fenceGeneration: validationFenceGeneration\n            )\n''',
    'replacement confirmation indentation'
)
source = source.replace(
    '''                validationAttemptID: validationAttemptID,\n                    validationFenceGeneration: validationFenceGeneration\n''',
    '''                validationAttemptID: validationAttemptID,\n                validationFenceGeneration: validationFenceGeneration\n'''
)
source = source.replace(
    '''                        validationAttemptID: validationAttemptID,\n                    validationFenceGeneration: validationFenceGeneration\n''',
    '''                        validationAttemptID: validationAttemptID,\n                        validationFenceGeneration: validationFenceGeneration\n'''
)
source = replace_once(
    source,
    '''            try checkAccountValidationAttempt(\n            validationAttemptID,\n            fenceGeneration: validationFenceGeneration\n        )\n            let currentScope = currentAccountScopeIdentifier\n''',
    '''            try checkAccountValidationAttempt(\n                validationAttemptID,\n                fenceGeneration: validationFenceGeneration\n            )\n            let currentScope = currentAccountScopeIdentifier\n''',
    'post-invalidation indentation'
)

append = r'''

extension CloudKitSynchronizerAccountFencingTests {
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
        XCTAssertFalse(synchronizer._test_portActivationRequiresWorkerRestart)
    }
}
'''

if 'testExplicitPortActivationCannotAdoptAlreadyPoisonedAuthority' in tests:
    raise SystemExit('final explicit-port regression already present')
tests = tests.rstrip() + append

source_path.write_text(source)
test_path.write_text(tests)
