    @BigSyncBackgroundActor
    func uploadChanges(
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        do {
            for adapter in modelAdapters {
                try Task.checkCancellation()
                try await synchronizeAdapter(adapter)
            }
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }
    
    @BigSyncBackgroundActor
    func setupZoneAndUploadRecords(
        adapter: ModelAdapter,
        restrictedToEntityType: String? = nil,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        try await setupRecordZoneIfNeeded(
            adapter: adapter,
            attemptID: attemptID
        ) { [weak self] error in
            guard let self else {
                try await completion(CancellationError())
                return
            }
            do {
                try checkSynchronizationAttempt(attemptID)
            } catch {
                try await completion(error)
                return
            }
            guard error == nil else {
                if let error,
                   let context = activeRunContext,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: adapter.recordZoneID,
                    context: context
                   ) {
                    try await completion(lifecycleError)
                    return
                }
                try await completion(error)
                return
            }
            do {
                try await uploadRecordsUsingAsyncStore(
                    adapter: adapter,
                    restrictedToEntityType: restrictedToEntityType,
                    attemptID: attemptID,
                    completion: { [weak self] (error) in
                        guard let self else {
                            try await completion(CancellationError())
                            return
                        }
                        do {
                            try checkSynchronizationAttempt(attemptID)
                        } catch {
                            try await completion(error)
                            return
                        }
                        try await completion(error)
                    }
                )
            } catch {
                try await completion(error)
            }
        }
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneIfNeeded(
        adapter: ModelAdapter,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        try checkSynchronizationAttempt(attemptID)
        let shouldSetup = try await needsZoneSetup(adapter: adapter)
        try checkSynchronizationAttempt(attemptID)
        guard shouldSetup else {
            try await completion(nil)
            return
        }
        
        try await setupRecordZoneID(
            adapter.recordZoneID,
            attemptID: attemptID,
            completion: completion
        )
    }
    
    @BigSyncBackgroundActor
    func setupRecordZoneID(
        _ zoneID: CKRecordZone.ID,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> ()
    ) async throws {
        do {
            // Validate immediately before and after each account-routed await.
            try await revalidateActiveRunContext(for: attemptID)
            _ = try await zoneStore.recordZone(withID: zoneID)
            try await revalidateActiveRunContext(for: attemptID)
            if let context = activeRunContext {
                try markConfiguredZoneEstablished(
                    zoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
            }
            try await completion(nil)
        } catch {
            do {
                // A returned account stop forbids further CloudKit work,
                // including an otherwise routine account revalidation.
                try checkSynchronizationAttempt(attemptID)
                if !CloudKitRetryConstraints(error).blocksAccountOperations {
                    try await revalidateActiveRunContext(for: attemptID)
                }
            } catch {
                try await completion(error)
                return
            }

            guard !CloudKitRetryConstraints(error).blocksAccountOperations,
                  let context = activeRunContext else {
                try await completion(error)
                return
            }
            let classification = CloudKitLossClassifier.classify(
                error: error,
                defaultZoneID: zoneID
            )
            guard let disposition = classification.zoneDispositions[zoneID]
            else {
                try await completion(error)
                return
            }
            if let lifecycleError = applyCloudKitLoss(
                disposition,
                zoneID: zoneID,
                context: context,
                allowsEncryptedBootstrapAbsence:
                    isEncryptedDataResetRecoveryActive
            ) {
                try await completion(lifecycleError)
                return
            }

            let newZone = CKRecordZone(zoneID: zoneID)
            do {
                try await revalidateActiveRunContext(for: attemptID)
                let savedZone = try await zoneStore.save(recordZone: newZone)
                try await revalidateActiveRunContext(for: attemptID)
                guard savedZone.zoneID == zoneID else {
                    throw CocoaError(.coderValueNotFound)
                }
                try markConfiguredZoneEstablished(
                    zoneID,
                    accountScopeIdentifier: context.accountScopeIdentifier
                )
                logger.info(
                    "QSCloudKitSynchronizer >> Created custom record zone: \(newZone.description)"
                )
                try await completion(nil)
            } catch {
                do {
                    try checkSynchronizationAttempt(attemptID)
                    if !CloudKitRetryConstraints(error).blocksAccountOperations {
                        try await revalidateRunContext(context)
                    }
                } catch {
                    try await completion(error)
                    return
                }
                if !CloudKitRetryConstraints(error).blocksAccountOperations,
                   let lifecycleError = applyCloudKitLoss(
                    error: error,
                    defaultZoneID: zoneID,
                    context: context,
                    allowsEncryptedBootstrapAbsence: false
                ) {
                    try await completion(lifecycleError)
                } else {
                    try await completion(error)
                }
            }
        }
    }
