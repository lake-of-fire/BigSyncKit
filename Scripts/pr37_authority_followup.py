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
    '''    private var completingPublicationAttemptID: UUID?\n    internal var domainPublicationScopeIdentifierProvider:\n''',
    '''    private var completingPublicationAttemptID: UUID?\n    private var activeAccountValidationAuthority: (\n        attemptID: UUID,\n        fenceGeneration: UInt64\n    )?\n    internal var domainPublicationScopeIdentifierProvider:\n''',
    'active validation authority storage'
)

source = replace_once(
    source,
    '''        let attemptID = UUID()\n        synchronizationAttemptID = attemptID\n''',
    '''        let accountValidationFenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        let attemptID = UUID()\n        synchronizationAttemptID = attemptID\n''',
    'attempt authority capture'
)

source = replace_once(
    source,
    '''                try await validateAccountAvailabilityIfNeeded(\n                    attemptID: attemptID\n                )\n                let accountIdentifier = try await validateSynchronizationAccount()\n''',
    '''                try await validateAccountAvailabilityIfNeeded(\n                    attemptID: attemptID,\n                    fenceGeneration: accountValidationFenceGeneration\n                )\n                let accountIdentifier = try await validateSynchronizationAccount(\n                    attemptID: attemptID,\n                    fenceGeneration: accountValidationFenceGeneration\n                )\n''',
    'bootstrap authority threading'
)

old_revalidate = '''    internal func revalidateInitialReplicaBindingContext(\n        _ context: BigSyncInitialReplicaBindingContext\n    ) async throws {\n        guard let expectedAccountIdentifier = context.accountIdentifier,\n              let validationAttemptID = context.validationAttemptID else {\n            throw CancellationError()\n        }\n        try checkAccountValidationAttempt(validationAttemptID)\n        try validatePendingReplicaBinding(context)\n        let currentAccountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(validationAttemptID)\n        try validatePendingReplicaBinding(context)\n        guard currentAccountIdentifier == expectedAccountIdentifier,\n              Self.accountScopeIdentifier(for: currentAccountIdentifier)\n                == context.accountScopeIdentifier else {\n            accountValidationRequired = true\n            throw OneOffRecordZoneResetError.cloudKitAccountChanged\n        }\n    }\n'''
new_revalidate = '''    internal func revalidateInitialReplicaBindingContext(\n        _ context: BigSyncInitialReplicaBindingContext\n    ) async throws {\n        guard let expectedAccountIdentifier = context.accountIdentifier,\n              let validationAttemptID = context.validationAttemptID,\n              let authority = activeAccountValidationAuthority,\n              authority.attemptID == validationAttemptID else {\n            throw CancellationError()\n        }\n        try checkAccountValidationAttempt(\n            validationAttemptID,\n            fenceGeneration: authority.fenceGeneration\n        )\n        try validatePendingReplicaBinding(context)\n        let currentAccountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(\n            validationAttemptID,\n            fenceGeneration: authority.fenceGeneration\n        )\n        try validatePendingReplicaBinding(context)\n        guard currentAccountIdentifier == expectedAccountIdentifier,\n              Self.accountScopeIdentifier(for: currentAccountIdentifier)\n                == context.accountScopeIdentifier else {\n            accountValidationRequired = true\n            throw OneOffRecordZoneResetError.cloudKitAccountChanged\n        }\n    }\n'''
source = replace_once(source, old_revalidate, new_revalidate, 'initial admission revalidation')

old_availability = '''    @BigSyncBackgroundActor\n    private func validateAccountAvailabilityIfNeeded(\n        attemptID: UUID\n    ) async throws {\n        guard accountValidationRequired\n                || cancelledDueToUnauthentication else { return }\n        let status = try await accountStatusProvider()\n        try checkAccountValidationAttempt(attemptID)\n        switch status {\n'''
new_availability = '''    @BigSyncBackgroundActor\n    private func validateAccountAvailabilityIfNeeded(\n        attemptID: UUID,\n        fenceGeneration: UInt64\n    ) async throws {\n        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n        guard accountValidationRequired\n                || cancelledDueToUnauthentication else { return }\n        let status = try await accountStatusProvider()\n        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n        switch status {\n'''
source = replace_once(source, old_availability, new_availability, 'availability fence')

source = replace_once(
    source,
    '''    @BigSyncBackgroundActor\n    private func validateSynchronizationAccount() async throws -> String {\n        let previousAccountIdentifier: String?\n''',
    '''    @BigSyncBackgroundActor\n    private func validateSynchronizationAccount(\n        attemptID validationAttemptID: UUID,\n        fenceGeneration validationFenceGeneration: UInt64\n    ) async throws -> String {\n        try checkAccountValidationAttempt(\n            validationAttemptID,\n            fenceGeneration: validationFenceGeneration\n        )\n        activeAccountValidationAuthority = (\n            validationAttemptID,\n            validationFenceGeneration\n        )\n        defer {\n            if activeAccountValidationAuthority?.attemptID\n                    == validationAttemptID,\n               activeAccountValidationAuthority?.fenceGeneration\n                    == validationFenceGeneration {\n                activeAccountValidationAuthority = nil\n            }\n        }\n        let previousAccountIdentifier: String?\n''',
    'validation signature and active authority'
)

source = replace_once(
    source,
    '''        let validationAttemptID = synchronizationAttemptID\n        let validationFenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        let currentAccountIdentifier = try await accountIdentifierProvider()\n''',
    '''        let currentAccountIdentifier = try await accountIdentifierProvider()\n''',
    'remove late authority capture'
)

old_activate = '''        let attemptID = synchronizationAttemptID\n        let accountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(attemptID)\n        guard Self.accountScopeIdentifier(for: accountIdentifier)\n                == expected.destinationAccountScopeIdentifier,\n              try pendingCloudAccountPortRequirement() == expected else {\n            throw BigSyncCloudAccountPortError.corruptRequirement\n        }\n        let confirmedAccountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(attemptID)\n        guard confirmedAccountIdentifier == accountIdentifier,\n              try pendingCloudAccountPortRequirement() == expected else {\n            throw OneOffRecordZoneResetError.cloudKitAccountChanged\n        }\n\n        _ = try BigSyncReplicaBindingStateStore.activatePort(\n'''
new_activate = '''        let attemptID = synchronizationAttemptID\n        let fenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        let accountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n        guard Self.accountScopeIdentifier(for: accountIdentifier)\n                == expected.destinationAccountScopeIdentifier,\n              try pendingCloudAccountPortRequirement() == expected else {\n            throw BigSyncCloudAccountPortError.corruptRequirement\n        }\n        let confirmedAccountIdentifier = try await accountIdentifierProvider()\n        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n        guard confirmedAccountIdentifier == accountIdentifier,\n              try pendingCloudAccountPortRequirement() == expected else {\n            throw OneOffRecordZoneResetError.cloudKitAccountChanged\n        }\n        try checkAccountValidationAttempt(\n            attemptID,\n            fenceGeneration: fenceGeneration\n        )\n\n        _ = try BigSyncReplicaBindingStateStore.activatePort(\n'''
source = replace_once(source, old_activate, new_activate, 'explicit port authority')

old_debug = '''#if DEBUG\n    @BigSyncBackgroundActor\n    func _test_validateSynchronizationAccount() async throws {\n        _ = try await validateSynchronizationAccount()\n    }\n\n    @BigSyncBackgroundActor\n    func _test_requireBackupDetectionRetry(_ error: Error) {\n'''
new_debug = '''#if DEBUG\n    @BigSyncBackgroundActor\n    func _test_validateSynchronizationAccount() async throws {\n        let attemptID = synchronizationAttemptID\n        let fenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        _ = try await validateSynchronizationAccount(\n            attemptID: attemptID,\n            fenceGeneration: fenceGeneration\n        )\n    }\n\n    @BigSyncBackgroundActor\n    func _test_validateAccountBootstrap() async throws {\n        let attemptID = synchronizationAttemptID\n        let fenceGeneration =\n            accountScopeAuthorityFence.invalidationGenerationSnapshot\n        try await validateAccountAvailabilityIfNeeded(\n            attemptID: attemptID,\n            fenceGeneration: fenceGeneration\n        )\n        _ = try await validateSynchronizationAccount(\n            attemptID: attemptID,\n            fenceGeneration: fenceGeneration\n        )\n    }\n\n    @BigSyncBackgroundActor\n    func _test_requireBackupDetectionRetry(_ error: Error) {\n'''
source = replace_once(source, old_debug, new_debug, 'debug bootstrap helper')

source = replace_once(
    source,
    '''        activeRunContext = nil\n        activeReceiptAuthorizationID = nil\n        reservedReceiptAuthorizationID = nil\n''',
    '''        activeRunContext = nil\n        activeAccountValidationAuthority = nil\n        activeReceiptAuthorizationID = nil\n        reservedReceiptAuthorizationID = nil\n''',
    'cancel clears validation authority'
)

test_append = r'''

@BigSyncBackgroundActor
private final class AccountAuthorityFenceReference: @unchecked Sendable {
    weak var synchronizer: CloudKitSynchronizer?
    private var shouldPoison = false

    func arm() {
        shouldPoison = true
    }

    func poison() {
        synchronizer?.accountScopeAuthorityFence.poison()
    }

    func poisonIfArmed() {
        guard shouldPoison else { return }
        shouldPoison = false
        poison()
    }
}

extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    func testAccountStatusPreflightCannotAdoptAuthorityPoisonedAfterCapture()
    async throws {
        let authority = AccountAuthorityFenceReference()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountStatusProvider: {
                await authority.poison()
                return .available
            }
        )
        authority.synchronizer = synchronizer

        do {
            try await synchronizer._test_validateAccountBootstrap()
            XCTFail("Expected the preflight authority generation to be stale")
        } catch is CancellationError {
        }

        XCTAssertNil(try synchronizer.accountScopeLease())
        XCTAssertTrue(synchronizer.accountValidationRequired)
    }

    @BigSyncBackgroundActor
    func testInitialAdmissionRevalidationRejectsAuthorityGenerationChangeBeforeMutation()
    async throws {
        let mutations = ApplicationBoundaryMutationRecorder()
        let admissionReference = InitialAdmissionSynchronizerReference()
        let authority = AccountAuthorityFenceReference()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountReplacementPolicy: .requireExplicitDatasetPort,
            initialReplicaBindingAdmissionHandler: { context in
                await authority.poison()
                try await admissionReference.revalidate(context)
                await mutations.recordMutation()
            }
        )
        admissionReference.synchronizer = synchronizer
        authority.synchronizer = synchronizer

        do {
            try await synchronizer._test_validateSynchronizationAccount()
            XCTFail("Expected the admission authority generation to be stale")
        } catch is CancellationError {
        }

        let mutationCount = await mutations.mutationCount
        XCTAssertEqual(mutationCount, 0)
        XCTAssertNil(try synchronizer.accountScopeLease())
    }

    @BigSyncBackgroundActor
    func testExplicitPortActivationRejectsAuthorityGenerationChangeAcrossAccountAwait()
    async throws {
        let identity = AccountFencingAccountIdentity("account-a")
        let authority = AccountAuthorityFenceReference()
        let synchronizer = makeSynchronizer(
            transport: AccountFencingTransport(),
            accountIdentifierProvider: {
                let account = await identity.current()
                await authority.poisonIfArmed()
                return account
            },
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

        await authority.arm()
        do {
            try await synchronizer.activateCloudAccountPort(requirement)
            XCTFail("Expected stale port activation authority to be rejected")
        } catch is CancellationError {
        }

        XCTAssertEqual(
            try synchronizer.pendingCloudAccountPortRequirement(),
            requirement
        )
        XCTAssertNil(try synchronizer.accountScopeLease())
    }
}
'''
if 'testAccountStatusPreflightCannotAdoptAuthorityPoisonedAfterCapture' in tests:
    raise SystemExit('follow-up tests already present')
tests = tests.rstrip() + test_append

source_path.write_text(source)
test_path.write_text(tests)
