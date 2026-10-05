    @BigSyncBackgroundActor
    func loadTokens(for zoneIDs: [CKRecordZone.ID]) async throws -> [CKRecordZone.ID] {
        var filteredZoneIDs = [CKRecordZone.ID]()
        activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
        
        for zoneID in zoneIDs {
            // Manabi explicitly registers its one supported synchronization
            // zone. Ignore unrelated private-database zones instead of
            // dynamically constructing an incompletely configured adapter.
            guard let adapter = modelAdapterDictionary[zoneID] else { continue }
            filteredZoneIDs.append(zoneID)
            activeZoneTokens[zoneID] = await adapter.serverChangeToken
        }
        
        return filteredZoneIDs
    }

        let zoneIDsToFetch = try await loadTokens(
            for: Array(changedZoneIDs)
        )
        try await revalidateActiveRunContext(for: attemptID)

    @BigSyncBackgroundActor
    func fetchZoneChanges(_ zoneIDs: [CKRecordZone.ID]) async throws {
        let attemptID = synchronizationAttemptID
        let runID = synchronizationRunID
        defer { changeRequestProcessor.clearErrors() }

        for zoneID in zoneIDs {
            var pageCursor = activeZoneTokens[zoneID]
