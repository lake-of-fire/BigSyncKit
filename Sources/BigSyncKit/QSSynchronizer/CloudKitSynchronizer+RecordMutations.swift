    @BigSyncBackgroundActor
    func uploadRecordsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        let operationError: Error?
        do {
            try await drainRecordUploadsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            operationError = nil
        } catch {
            operationError = error
        }
        // Delivery errors belong to the caller, not to the operation just
        // completed. Never feed a throwing callback back into itself.
        try await completion(operationError)
    }

    @BigSyncBackgroundActor
    func uploadDeletionsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        let operationError: Error?
        do {
            try await drainRecordDeletionsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            operationError = nil
        } catch {
            operationError = error
        }
        // Delivery errors belong to the caller, not to the operation just
        // completed. Never feed a throwing callback back into itself.
        try await completion(operationError)
    }
