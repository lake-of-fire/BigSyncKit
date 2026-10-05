    @BigSyncBackgroundActor
    func loadTokens(for zoneIDs: [CKRecordZone.ID]) async throws -> [CKRecordZone.ID] {
        let attemptID = synchronizationAttemptID
        try checkSynchronizationAttempt(attemptID)
        var filteredZoneIDs = [CKRecordZone.ID]()
        var loadedTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
        
        for zoneID in zoneIDs {
            // Manabi explicitly registers its one supported synchronization
            // zone. Ignore unrelated private-database zones instead of
            // dynamically constructing an incompletely configured adapter.
            guard let adapter = modelAdapterDictionary[zoneID] else { continue }
            let token = await adapter.serverChangeToken
            try checkSynchronizationAttempt(attemptID)
            filteredZoneIDs.append(zoneID)
            loadedTokens[zoneID] = token
        }
        
        // Publish one complete snapshot only while the admitting attempt is
        // current. A late nil cursor must not erase a successor's entry either.
        try checkSynchronizationAttempt(attemptID)
        activeZoneTokens = loadedTokens
        return filteredZoneIDs
    }
    
    func resetActiveTokens() {
        activeZoneTokens = [CKRecordZone.ID: RecordZoneChangeCursor]()
    }
