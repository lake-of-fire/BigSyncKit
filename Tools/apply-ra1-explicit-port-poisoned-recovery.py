from pathlib import Path

source = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift')


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f'expected one anchor, found {count}: {old[:120]!r}')
    return text.replace(old, new, 1)


text = source.read_text()
text = replace_once(text, '''    /// Makes one non-suspending authority commit atomic with respect to poison.
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
''', '''    /// Makes one non-suspending writer-authority commit atomic with respect to
    /// poison. Ordinary writers may never adopt already-revoked authority.
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
''')
text = replace_once(text, '''        let attemptID = synchronizationAttemptID
        guard let fenceGeneration = accountScopeAuthorityFence
            .authorizedInvalidationGenerationSnapshot else {
            throw CancellationError()
        }
        let accountIdentifier = try await accountIdentifierProvider()
''', '''        // A durable pending port is the recovery gate itself. Activation may
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
