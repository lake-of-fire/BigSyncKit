#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift"
TESTS = ROOT / "Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one preimage, found {count}")
    return text.replace(old, new, 1)


def add_generation_argument(segment: str, function_name: str, expected_count: int) -> str:
    pattern = re.compile(
        rf"(try await {re.escape(function_name)}\([\s\S]*?"
        rf"validationAttemptID: validationAttemptID)(\n\s*\))"
    )

    def repl(match: re.Match[str]) -> str:
        indent = re.search(r"\n(\s*)\)$", match.group(2)).group(1)
        arg_indent = indent + "    "
        return (
            match.group(1)
            + ",\n"
            + arg_indent
            + "validationFenceGeneration: validationFenceGeneration"
            + match.group(2)
        )

    result, count = pattern.subn(repl, segment)
    if count != expected_count:
        raise RuntimeError(
            f"{function_name}: expected {expected_count} call-site rewrites, found {count}"
        )
    return result


source = SOURCE.read_text(encoding="utf-8")

old_fence = '''final class AccountScopeAuthorityFence: @unchecked Sendable {
    private let lock = NSLock()
    private var isPoisoned = true
    private var rotatesGeneration = false

    func poison(requiresGenerationRotation: Bool = true) {
        lock.lock()
        isPoisoned = true
        rotatesGeneration = rotatesGeneration || requiresGenerationRotation
        lock.unlock()
    }

    func clear() {
        lock.lock()
        isPoisoned = false
        rotatesGeneration = false
        lock.unlock()
    }

    var rejectsAuthority: Bool {
        lock.lock()
        defer { lock.unlock() }
        return isPoisoned
    }

    var requiresGenerationRotation: Bool {
        lock.lock()
        defer { lock.unlock() }
        return rotatesGeneration
    }
}'''
new_fence = '''final class AccountScopeAuthorityFence: @unchecked Sendable {
    private let lock = NSLock()
    private var isPoisoned = true
    private var rotatesGeneration = false
    private var invalidationGeneration: UInt64 = 0

    /// Monotonic identity for one account-validation authority window. This is
    /// readable while poisoned because fresh validation is itself responsible
    /// for reopening authority, but it must lose to every newer synchronous
    /// account-change poison before actor-isolated cancellation catches up.
    var invalidationGenerationSnapshot: UInt64 {
        lock.lock()
        defer { lock.unlock() }
        return invalidationGeneration
    }

    /// External commit paths may capture authority only while the synchronous
    /// fence is already open. Fresh validation uses the raw generation above.
    var authorizedInvalidationGenerationSnapshot: UInt64? {
        lock.lock()
        defer { lock.unlock() }
        guard !isPoisoned else { return nil }
        return invalidationGeneration
    }

    /// Makes one non-suspending authority commit atomic with respect to poison.
    /// If poison wins first, no mutation runs. If it arrives during the body it
    /// waits for this lock, then immediately revokes the just-published result.
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

    func poison(requiresGenerationRotation: Bool = true) {
        lock.lock()
        invalidationGeneration &+= 1
        isPoisoned = true
        rotatesGeneration = rotatesGeneration || requiresGenerationRotation
        lock.unlock()
    }

    func clear() {
        lock.lock()
        isPoisoned = false
        rotatesGeneration = false
        lock.unlock()
    }

    @discardableResult
    func clear(ifInvalidationGenerationMatches expected: UInt64) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard invalidationGeneration == expected else { return false }
        isPoisoned = false
        rotatesGeneration = false
        return true
    }

    var rejectsAuthority: Bool {
        lock.lock()
        defer { lock.unlock() }
        return isPoisoned
    }

    var requiresGenerationRotation: Bool {
        lock.lock()
        defer { lock.unlock() }
        return rotatesGeneration
    }
}'''
source = replace_once(source, old_fence, new_fence, "authority fence")

source = replace_once(
    source,
    '''    internal var activeRunContext: RunContext?
    internal var activeReceiptAuthorizationID: UUID?''',
    '''    internal var activeRunContext: RunContext?
    private var activeAccountValidationAuthority: (
        attemptID: UUID,
        fenceGeneration: UInt64
    )?
    internal var activeReceiptAuthorizationID: UUID?''',
    "active validation authority storage",
)

source = replace_once(
    source,
    '''        retrySleepUntil = nil
        let attemptID = UUID()''',
    '''        retrySleepUntil = nil
        let accountValidationFenceGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        let attemptID = UUID()''',
    "validation generation capture",
)
source = replace_once(
    source,
    '''                try await validateAccountAvailabilityIfNeeded(
                    attemptID: attemptID
                )
                let accountIdentifier = try await validateSynchronizationAccount()''',
    '''                try await validateAccountAvailabilityIfNeeded(
                    attemptID: attemptID,
                    fenceGeneration: accountValidationFenceGeneration
                )
                let accountIdentifier = try await validateSynchronizationAccount(
                    attemptID: attemptID,
                    fenceGeneration: accountValidationFenceGeneration
                )''',
    "bootstrap validation calls",
)

old_revalidate_initial = '''    internal func revalidateInitialReplicaBindingContext(
        _ context: BigSyncInitialReplicaBindingContext
    ) async throws {
        guard let expectedAccountIdentifier = context.accountIdentifier,
              let validationAttemptID = context.validationAttemptID else {
            throw CancellationError()
        }
        try checkAccountValidationAttempt(validationAttemptID)
        try validatePendingReplicaBinding(context)
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
        try validatePendingReplicaBinding(context)
        guard currentAccountIdentifier == expectedAccountIdentifier,
              Self.accountScopeIdentifier(for: currentAccountIdentifier)
                == context.accountScopeIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }'''
new_revalidate_initial = '''    internal func revalidateInitialReplicaBindingContext(
        _ context: BigSyncInitialReplicaBindingContext
    ) async throws {
        guard let expectedAccountIdentifier = context.accountIdentifier,
              let validationAttemptID = context.validationAttemptID,
              let authority = activeAccountValidationAuthority,
              authority.attemptID == validationAttemptID else {
            throw CancellationError()
        }
        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: authority.fenceGeneration
        )
        try validatePendingReplicaBinding(context)
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: authority.fenceGeneration
        )
        try validatePendingReplicaBinding(context)
        guard currentAccountIdentifier == expectedAccountIdentifier,
              Self.accountScopeIdentifier(for: currentAccountIdentifier)
                == context.accountScopeIdentifier else {
            accountValidationRequired = true
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }'''
source = replace_once(
    source,
    old_revalidate_initial,
    new_revalidate_initial,
    "initial admission revalidation",
)

old_check_and_availability = '''    private func checkAccountValidationAttempt(_ attemptID: UUID) throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID else {
            throw CancellationError()
        }
    }

    @BigSyncBackgroundActor
    private func validateAccountAvailabilityIfNeeded(
        attemptID: UUID
    ) async throws {
        guard accountValidationRequired
                || cancelledDueToUnauthentication else { return }
        let status = try await accountStatusProvider()
        try checkAccountValidationAttempt(attemptID)
        switch status {
        case .available:
            return
        case .noAccount, .restricted:
            throw CKError(.notAuthenticated)
        case .couldNotDetermine, .temporarilyUnavailable:
            throw CKError(.accountTemporarilyUnavailable)
        @unknown default:
            throw CKError(.accountTemporarilyUnavailable)
        }
    }'''
new_check_and_availability = '''    private func checkAccountValidationAttempt(
        _ attemptID: UUID,
        fenceGeneration: UInt64? = nil
    ) throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID,
              fenceGeneration.map({
                  accountScopeAuthorityFence.invalidationGenerationSnapshot == $0
              }) ?? true else {
            throw CancellationError()
        }
    }

    @BigSyncBackgroundActor
    private func validateAccountAvailabilityIfNeeded(
        attemptID: UUID,
        fenceGeneration: UInt64
    ) async throws {
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        guard accountValidationRequired
                || cancelledDueToUnauthentication else { return }
        let status = try await accountStatusProvider()
        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
        switch status {
        case .available:
            return
        case .noAccount, .restricted:
            throw CKError(.notAuthenticated)
        case .couldNotDetermine, .temporarilyUnavailable:
            throw CKError(.accountTemporarilyUnavailable)
        @unknown default:
            throw CKError(.accountTemporarilyUnavailable)
        }
    }'''
source = replace_once(
    source,
    old_check_and_availability,
    new_check_and_availability,
    "attempt/generation validation",
)

start = source.index(
    "    @BigSyncBackgroundActor\n    private func validateSynchronizationAccount() async throws -> String {"
)
end = source.index(
    "    /// Resolves application-specific dataset identity before BigSync claims an\n",
    start,
)
segment = source[start:end]
segment = replace_once(
    segment,
    '''    @BigSyncBackgroundActor
    private func validateSynchronizationAccount() async throws -> String {''',
    '''    @BigSyncBackgroundActor
    private func validateSynchronizationAccount(
        attemptID validationAttemptID: UUID,
        fenceGeneration validationFenceGeneration: UInt64
    ) async throws -> String {
        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
        activeAccountValidationAuthority = (
            validationAttemptID,
            validationFenceGeneration
        )
        defer {
            if activeAccountValidationAuthority?.attemptID
                    == validationAttemptID,
               activeAccountValidationAuthority?.fenceGeneration
                    == validationFenceGeneration {
                activeAccountValidationAuthority = nil
            }
        }''',
    "validate account signature",
)
segment = replace_once(
    segment,
    "        let validationAttemptID = synchronizationAttemptID\n",
    "",
    "obsolete validation attempt capture",
)
segment = segment.replace(
    "        try checkAccountValidationAttempt(validationAttemptID)\n",
    '''        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
''',
)
segment = add_generation_argument(
    segment,
    "admitInitialReplicaBindingIfNeeded",
    2,
)
segment = add_generation_argument(segment, "admitReplicaBinding", 2)
segment = replace_once(
    segment,
    "        try establishAccountScopeLeaseDurably(\n",
    '''        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
        try establishAccountScopeLeaseDurably(
''',
    "pre-lease authority check",
)
segment = replace_once(
    segment,
    '''        try persistAccountIdentifier(confirmedAccountIdentifier)
        accountValidationRequired = false
        cancelSync = false
        cancelledDueToUnauthentication = false
        accountScopeAuthorityFence.clear()
        return confirmedAccountIdentifier''',
    '''        try persistAccountIdentifier(confirmedAccountIdentifier)
        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
        accountValidationRequired = false
        guard accountScopeAuthorityFence.clear(
            ifInvalidationGenerationMatches: validationFenceGeneration
        ) else {
            accountValidationRequired = true
            throw CancellationError()
        }
        cancelSync = false
        cancelledDueToUnauthentication = false
        return confirmedAccountIdentifier''',
    "conditional validation authority restoration",
)
source = source[:start] + segment + source[end:]

source = replace_once(
    source,
    '''    private func admitInitialReplicaBindingIfNeeded(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID
    ) async throws {''',
    '''    private func admitInitialReplicaBindingIfNeeded(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID,
        validationFenceGeneration: UInt64
    ) async throws {''',
    "initial admission signature",
)
source = replace_once(
    source,
    '''            expectedBinding: expectedBinding,
            validationAttemptID: validationAttemptID
        )
    }

    private func admitReplicaBinding(''',
    '''            expectedBinding: expectedBinding,
            validationAttemptID: validationAttemptID,
            validationFenceGeneration: validationFenceGeneration
        )
    }

    private func admitReplicaBinding(''',
    "initial admission child generation",
)
source = replace_once(
    source,
    '''        generationIdentifier: String,
        expectedBinding: BigSyncReplicaBindingSnapshot,
        validationAttemptID: UUID
    ) async throws {''',
    '''        generationIdentifier: String,
        expectedBinding: BigSyncReplicaBindingSnapshot,
        validationAttemptID: UUID,
        validationFenceGeneration: UInt64
    ) async throws {''',
    "replica admission signature",
)
replica_start = source.index("    private func admitReplicaBinding(")
replica_end = source.index("    private func durableAccountIdentifier()", replica_start)
replica = source[replica_start:replica_end]
replica = replica.replace(
    "        try checkAccountValidationAttempt(validationAttemptID)\n",
    '''        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
''',
)
if replica.count("fenceGeneration: validationFenceGeneration") != 2:
    raise RuntimeError("replica admission: expected two generation checks")
source = source[:replica_start] + replica + source[replica_end:]

activate_start = source.index("    public func activateCloudAccountPort(")
activate_end = source.index("    /// Stops an ordinary journal wakeup", activate_start)
activate = source[activate_start:activate_end]
activate = replace_once(
    activate,
    "        let attemptID = synchronizationAttemptID\n",
    '''        let attemptID = synchronizationAttemptID
        guard let fenceGeneration = accountScopeAuthorityFence
            .authorizedInvalidationGenerationSnapshot else {
            throw CancellationError()
        }
''',
    "explicit port authority capture",
)
activate = activate.replace(
    "        try checkAccountValidationAttempt(attemptID)\n",
    '''        try checkAccountValidationAttempt(
            attemptID,
            fenceGeneration: fenceGeneration
        )
''',
)
if activate.count("fenceGeneration: fenceGeneration") != 3:
    # Two existing await revalidations plus the explicit precommit check below.
    pass
activate = replace_once(
    activate,
    '''        accountValidationRequired = true
        cancelSync = true
        portActivationRequiresWorkerRestart = true
        _ = try BigSyncReplicaBindingStateStore.activatePort(
            expected,
            store: keyValueStore,
            key: replicaBindingStateKey
        )
        try persistAccountIdentifier(confirmedAccountIdentifier)
''',
    '''        accountValidationRequired = true
        cancelSync = true
        portActivationRequiresWorkerRestart = true
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
                try persistAccountIdentifier(confirmedAccountIdentifier)
                return true
            }) == true else {
            throw CancellationError()
        }
''',
    "atomic explicit port commit",
)
source = source[:activate_start] + activate + source[activate_end:]

source = replace_once(
    source,
    '''    @BigSyncBackgroundActor
    func _test_validateSynchronizationAccount() async throws {
        _ = try await validateSynchronizationAccount()
    }
''',
    '''    @BigSyncBackgroundActor
    func _test_validateSynchronizationAccount() async throws {
        let attemptID = synchronizationAttemptID
        let fenceGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        _ = try await validateSynchronizationAccount(
            attemptID: attemptID,
            fenceGeneration: fenceGeneration
        )
    }

    @BigSyncBackgroundActor
    func _test_validateAccountBootstrap() async throws {
        let attemptID = synchronizationAttemptID
        let fenceGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        try await validateAccountAvailabilityIfNeeded(
            attemptID: attemptID,
            fenceGeneration: fenceGeneration
        )
        _ = try await validateSynchronizationAccount(
            attemptID: attemptID,
            fenceGeneration: fenceGeneration
        )
    }
''',
    "debug validation helpers",
)
source = replace_once(
    source,
    '''        cancelAttemptCallbacks(for: cancelledAttemptID)
        activeRunContext = nil
        publicationConsumptionPending = false''',
    '''        cancelAttemptCallbacks(for: cancelledAttemptID)
        activeRunContext = nil
        activeAccountValidationAuthority = nil
        publicationConsumptionPending = false''',
    "cancel validation authority",
)

# No stale, generation-less production entry point may remain.
if "validateSynchronizationAccount()" in source:
    raise RuntimeError("generation-less validateSynchronizationAccount call remains")

SOURCE.write_text(source, encoding="utf-8")

tests = TESTS.read_text(encoding="utf-8")
marker = "testAccountStatusPreflightCannotAdoptAuthorityPoisonedAfterCapture"
if marker in tests:
    raise RuntimeError("focused authority-generation tests already present")

tests += r'''

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
TESTS.write_text(tests, encoding="utf-8")
