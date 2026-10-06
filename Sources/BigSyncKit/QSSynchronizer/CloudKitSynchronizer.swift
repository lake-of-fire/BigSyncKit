    @BigSyncBackgroundActor
    public func validateAccountScopeLease(
        _ expected: BigSyncAccountScopeLease
    ) throws {
        guard let current = try accountScopeLease() else {
            throw BigSyncAccountScopeLeaseError.unavailable
        }
        guard current.accountScopeIdentifier
                == expected.accountScopeIdentifier,
              current.invalidationGeneration
                == expected.invalidationGeneration else {
            throw BigSyncAccountScopeLeaseError.stale
        }
    }

    private struct PersistedAccountScopeLease {
        let generation: Int64
        let lease: BigSyncAccountScopeLease?
    }

    private func readAccountScopeLeaseDurably() throws
        -> PersistedAccountScopeLease {
        guard let raw = try keyValueStore.bigSyncDurableObject(
            forKey: accountScopeLeaseKey
        ) else {
            return PersistedAccountScopeLease(generation: 0, lease: nil)
        }
        guard let value = raw as? [String: Any],
              (value["version"] as? NSNumber)?.intValue == 1,
              let generationNumber = value["generation"] as? NSNumber,
              generationNumber.int64Value >= 0,
              let isValid = value["isValid"] as? Bool else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        let generation = generationNumber.int64Value
        guard isValid else {
            return PersistedAccountScopeLease(
                generation: generation,
                lease: nil
            )
        }
        guard let accountScopeIdentifier =
                value["accountScopeIdentifier"] as? String,
              !accountScopeIdentifier.isEmpty,
              let validatedAt = value["validatedAt"] as? Date else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        return PersistedAccountScopeLease(
            generation: generation,
            lease: BigSyncAccountScopeLease(
                accountScopeIdentifier: accountScopeIdentifier,
                invalidationGeneration: generation,
                validatedAt: validatedAt
            )
        )
    }

    private func persistAccountScopeLease(
        generation: Int64,
        accountScopeIdentifier: String?,
        validatedAt: Date?
    ) throws {
        var value: [String: Any] = [
            "version": 1,
            "generation": NSNumber(value: generation),
            "isValid": accountScopeIdentifier != nil,
        ]
        if let accountScopeIdentifier, let validatedAt {
            value["accountScopeIdentifier"] = accountScopeIdentifier
            value["validatedAt"] = validatedAt
        }
        try keyValueStore.bigSyncSetDurably(
            value: value,
            forKey: accountScopeLeaseKey
        )
    }

    private func invalidateAccountScopeLeaseDurably() throws {
        let persisted = try readAccountScopeLeaseDurably()
        guard persisted.lease != nil else { return }
        guard persisted.generation < Int64.max else {
            throw BigSyncAccountScopeLeaseError.corrupt
        }
        try persistAccountScopeLease(
            generation: persisted.generation + 1,
            accountScopeIdentifier: nil,
            validatedAt: nil
        )
    }
