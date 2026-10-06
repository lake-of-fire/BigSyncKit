            "changeFeedEpoch": changeFeedEpoch,
            "consumedServerBoundaryIdentifier":
                consumedServerBoundaryIdentifier,
            "runID": context.runID.uuidString.lowercased(),
            "publishedAt": timestamp,
        ]
        value["replicaBindingGenerationIdentifier"] =
            context.replicaBindingGenerationIdentifier
        // The writer must never replace good evidence with a value that its
        // own persisted-value decoder would reject. Keep one v1 contract.
        _ = try BigSyncDurablePublicationEvidence(persistedValue: value)
        try keyValueStore.bigSyncSetDurably(
            value: value,
            forKey: durablePublicationEvidenceKey
        )
    }

    private func persistedDurablePublicationEvidence() throws
        -> BigSyncDurablePublicationEvidence? {
                        accountScopeIdentifier: accountScopeIdentifier
                    )),
                  try persistedDurablePublicationEvidence() == evidence,
                  try inspection.matches(
                    evidence,
                    containerIdentifier: containerIdentifier,
                    databaseScope: database.databaseScope
                  ) else { return nil }
            // Refresh/inspection may synchronously revoke the original owner.
            // No positive evidence can escape after that final callout.
            guard try inspectionOwnerIsCurrent() else { return nil }
            return evidence
        }

        // Non-Realm adapters retain their existing inspection contract.
        for adapter in modelAdapters {
            try await adapter.activateTransportNamespace(
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
                replicaBindingGenerationIdentifier:
                    evidence.replicaBindingGenerationIdentifier,
                containerIdentifier: containerIdentifier,
                databaseScope: database.databaseScope
              ) == evidence.consumedServerBoundaryIdentifier,
              try adapter.changeFeedEpoch() == evidence.changeFeedEpoch else {
            return nil
        }
        guard try inspectionOwnerIsCurrent() else { return nil }
        return evidence
    }

#if DEBUG
    /// Read-only E2E inventory of the exact durable bytes already validated by
    /// the terminal path. This neither restores publication nor touches Realm.
    @_spi(CloudKitE2E)
    public func cloudKitE2EDurablePublicationEvidence() throws
