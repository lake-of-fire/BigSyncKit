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

    private typealias PersistedAccountScopeLease = BigSyncPersistedAccountScopeLease

    private func readAccountScopeLeaseDurably() throws
        -> PersistedAccountScopeLease {
        guard let raw = try keyValueStore.bigSyncDurableObject(
            forKey: accountScopeLeaseKey
        ) else {
            return PersistedAccountScopeLease(generation: 0, lease: nil)
        }
        return try PersistedAccountScopeLease(persistedValue: raw)
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
        _ = try PersistedAccountScopeLease(persistedValue: value)
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
