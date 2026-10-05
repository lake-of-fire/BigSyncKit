    @BigSyncBackgroundActor
    func notifyDelegateForDeletedZoneIDs(
        _ zoneIDs: [CKRecordZone.ID],
        attemptID: UUID
    ) async throws {
        for zoneID in zoneIDs {
            // Lifecycle state and tracking recovery are owned exclusively by
            // the fenced migration. The delegate receives an informational
            // notification only after the account/run has been revalidated.
            try await revalidateActiveRunContext(for: attemptID)
            self.delegate?.synchronizer(self, zoneIDWasDeleted: zoneID)
        }
    }
    
    @BigSyncBackgroundActor
    func loadTokens(
        for zoneIDs: [CKRecordZone.ID],
        attemptID expectedAttemptID: UUID? = nil
    ) async throws -> [CKRecordZone.ID] {
        let attemptID = expectedAttemptID ?? synchronizationAttemptID
        let runID = synchronizationRunID
        let context = activeRunContext
        // Snapshot only registered adapters before any cursor read can suspend.
        // An old read must neither adopt a replacement adapter nor publish its
        // cursor into a successor run's in-memory page state.
        let adapters = zoneIDs.compactMap { zoneID in
            modelAdapterDictionary[zoneID].map { (zoneID: zoneID, adapter: $0) }
        }
        func validateOwner() throws {
            try checkSynchronizationAttempt(attemptID)
            guard synchronizationRunID == runID, activeRunContext == context,
                  adapters.allSatisfy({ modelAdapterDictionary[$0.zoneID] === $0.adapter }) else {
                throw CancellationError()
            }
            if let context { try checkRunContext(context) }
        }
        try validateOwner()
        var loadedTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
        for (zoneID, adapter) in adapters {
            let token = await adapter.serverChangeToken
            try validateOwner()
            loadedTokens[zoneID] = token
        }
        // No suspension or callout between final validation and publication.
        // Rejection preserves the prior map; success still replaces it, even
        // for empty input or a registered adapter with no persisted cursor.
        try validateOwner()
        activeZoneTokens = loadedTokens
        return adapters.map(\.zoneID)
    }

        let zoneIDsToFetch = try await loadTokens(
            for: Array(changedZoneIDs), attemptID: attemptID
        )
        try await revalidateActiveRunContext(for: attemptID)

    @BigSyncBackgroundActor
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID]) async throws {
        let attemptID = synchronizationAttemptID
        let runID = synchronizationRunID
        defer {
            // A cancelled request may unwind after another run has begun.
            // Clear only the processor errors belonging to this fetch's owner.
            if synchronizationAttemptID == attemptID, synchronizationRunID == runID {
                changeRequestProcessor.clearErrors()
            }
        }

        for zoneID in zoneIDs {
            var pageCursor = activeZoneTokens[zoneID]
