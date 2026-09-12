#if DEBUG
import CloudKit

extension RealmSwiftAdapter {
    /// Read-only qualification evidence for the existing outbound serialization
    /// path. Preparing a batch snapshots pending generations but does not
    /// acknowledge, clear, publish, or otherwise advance transport state.
    /// The signed app harness uses these exact locally serialized CKRecords to
    /// compare field values with the records CKDatabase returns successful.
    @_spi(CloudKitE2E)
    @BigSyncBackgroundActor
    public func cloudKitE2EPreparedUploadRecords(
        limit: Int
    ) async throws -> [CKRecord] {
        guard limit > 0 else { return [] }
        return try await prepareUploadBatch(limit: limit).records
    }
}
#endif
