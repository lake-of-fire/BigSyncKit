    @BigSyncBackgroundActor
    func uploadRecordsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        do {
            try await drainRecordUploadsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }

    @BigSyncBackgroundActor
    func uploadDeletionsUsingAsyncStore(
        adapter: ModelAdapter,
        restrictedToEntityType: String?,
        attemptID: UUID,
        completion: @Sendable @BigSyncBackgroundActor @escaping (Error?) async throws -> Void
    ) async throws {
        do {
            try await drainRecordDeletionsUsingAsyncStore(
                adapter: adapter,
                restrictedToEntityType: restrictedToEntityType,
                attemptID: attemptID
            )
            try await completion(nil)
        } catch {
            try await completion(error)
        }
    }
