            "changeFeedEpoch": changeFeedEpoch,
            "consumedServerBoundaryIdentifier":
                consumedServerBoundaryIdentifier,
            "runID": context.runID.uuidString.lowercased(),
            "publishedAt": timestamp,
        ]
        value["replicaBindingGenerationIdentifier"] =
            context.replicaBindingGenerationIdentifier
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
        return evidence
    }

#if DEBUG
    /// Read-only E2E inventory of the exact durable bytes already validated by
    /// the terminal path. This neither restores publication nor touches Realm.
    @_spi(CloudKitE2E)
    public func cloudKitE2EDurablePublicationEvidence() throws
