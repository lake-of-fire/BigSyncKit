from pathlib import Path

path = Path("Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift")
source = path.read_text()


def replace_once(old: str, new: str, *, label: str) -> None:
    global source
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    source = source.replace(old, new, 1)


replace_once(
    """    var publicationInspectionGeneration: UInt64? {
        lock.lock()
        defer { lock.unlock() }
        guard !isPoisoned || invalidationGeneration == 0 else { return nil }
        return invalidationGeneration
    }

""",
    """    var publicationInspectionGeneration: UInt64? {
        lock.lock()
        defer { lock.unlock() }
        guard !isPoisoned || invalidationGeneration == 0 else { return nil }
        return invalidationGeneration
    }

    /// Monotonic identity for one account-validation authority window.
    /// This remains readable while poisoned so a validation suspended in an
    /// application callback can detect a newer synchronous account-change
    /// notification before actor-isolated cancellation catches up.
    var invalidationGenerationSnapshot: UInt64 {
        lock.lock()
        defer { lock.unlock() }
        return invalidationGeneration
    }

""",
    label="authority generation snapshot",
)

replace_once(
    """    func clear() {
        lock.lock()
        isPoisoned = false
        rotatesGeneration = false
        lock.unlock()
    }
""",
    """    @discardableResult
    func clear(ifInvalidationGenerationMatches expected: UInt64) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard invalidationGeneration == expected else { return false }
        isPoisoned = false
        rotatesGeneration = false
        return true
    }
""",
    label="conditional authority clear",
)

replace_once(
    """    private func checkAccountValidationAttempt(_ attemptID: UUID) throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID else {
            throw CancellationError()
        }
    }
""",
    """    private func checkAccountValidationAttempt(
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
""",
    label="validation attempt checker",
)

start_marker = """    @BigSyncBackgroundActor
    private func validateSynchronizationAccount() async throws -> String {
"""
end_marker = """    /// Resolves application-specific dataset identity before BigSync claims an
"""
start = source.index(start_marker)
end = source.index(end_marker, start)
region = source[start:end]

old_start = """        let validationAttemptID = synchronizationAttemptID
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(validationAttemptID)
"""
new_start = """        let validationAttemptID = synchronizationAttemptID
        let validationFenceGeneration =
            accountScopeAuthorityFence.invalidationGenerationSnapshot
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
"""
if region.count(old_start) != 1:
    raise SystemExit("validation start: source shape changed")
region = region.replace(old_start, new_start, 1)

remaining_check = "        try checkAccountValidationAttempt(validationAttemptID)\n"
remaining_count = region.count(remaining_check)
if remaining_count != 2:
    raise SystemExit(
        f"validation suspension checks: expected 2 remaining, found {remaining_count}"
    )
region = region.replace(
    remaining_check,
    """        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
""",
)

helper_call = "validationAttemptID: validationAttemptID"
helper_call_count = region.count(helper_call)
if helper_call_count != 4:
    raise SystemExit(
        f"validation helper calls: expected 4, found {helper_call_count}"
    )
region = region.replace(
    helper_call,
    "validationAttemptID: validationAttemptID,\n"
    "                    validationFenceGeneration: validationFenceGeneration",
)

establish = "        try establishAccountScopeLeaseDurably(\n"
if region.count(establish) != 1:
    raise SystemExit("account lease publication: source shape changed")
region = region.replace(
    establish,
    """        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
        try establishAccountScopeLeaseDurably(
""",
    1,
)

terminal = """        accountValidationRequired = false
        cancelSync = false
        cancelledDueToUnauthentication = false
        accountScopeAuthorityFence.clear()
        return confirmedAccountIdentifier
"""
terminal_replacement = """        try checkAccountValidationAttempt(
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
        return confirmedAccountIdentifier
"""
if region.count(terminal) != 1:
    raise SystemExit("validation completion: source shape changed")
region = region.replace(terminal, terminal_replacement, 1)
source = source[:start] + region + source[end:]

replace_once(
    """    private func admitInitialReplicaBindingIfNeeded(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID
    ) async throws {
""",
    """    private func admitInitialReplicaBindingIfNeeded(
        accountIdentifier: String,
        accountScopeIdentifier: String,
        validationAttemptID: UUID,
        validationFenceGeneration: UInt64
    ) async throws {
""",
    label="initial binding helper signature",
)

replace_once(
    """            expectedBinding: expectedBinding,
            validationAttemptID: validationAttemptID
        )
    }

    private func admitReplicaBinding(
""",
    """            expectedBinding: expectedBinding,
            validationAttemptID: validationAttemptID,
            validationFenceGeneration: validationFenceGeneration
        )
    }

    private func admitReplicaBinding(
""",
    label="initial binding helper forwarding",
)

replace_once(
    """        expectedBinding: BigSyncReplicaBindingSnapshot,
        validationAttemptID: UUID
    ) async throws {
""",
    """        expectedBinding: BigSyncReplicaBindingSnapshot,
        validationAttemptID: UUID,
        validationFenceGeneration: UInt64
    ) async throws {
""",
    label="replica binding helper signature",
)

helper_start = source.index("    private func admitReplicaBinding(")
helper_end = source.index("    private func durableAccountIdentifier()", helper_start)
helper = source[helper_start:helper_end]
helper_check = "        try checkAccountValidationAttempt(validationAttemptID)\n"
helper_check_count = helper.count(helper_check)
if helper_check_count != 2:
    raise SystemExit(
        f"replica binding suspension checks: expected 2, found {helper_check_count}"
    )
helper = helper.replace(
    helper_check,
    """        try checkAccountValidationAttempt(
            validationAttemptID,
            fenceGeneration: validationFenceGeneration
        )
""",
)
source = source[:helper_start] + helper + source[helper_end:]

if "accountScopeAuthorityFence.clear()" in source:
    raise SystemExit("unconditional authority-fence clear survived")

path.write_text(source)
