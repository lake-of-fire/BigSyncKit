import CloudKit

/// Per-record results from one non-atomic CloudKit mutation request.
///
/// BigSync consumes every item result independently so a successful record is
/// acknowledged only for the journal generation that produced it, while a
/// conflict, missing item, or transient failure remains explicit.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public struct CloudKitRecordMutationResults {
    public let saveResults: [CKRecord.ID: Result<CKRecord, Error>]
    public let deleteResults: [CKRecord.ID: Result<Void, Error>]

    public init(
        saveResults: [CKRecord.ID: Result<CKRecord, Error>],
        deleteResults: [CKRecord.ID: Result<Void, Error>]
    ) {
        self.saveResults = saveResults
        self.deleteResults = deleteResults
    }
}

/// Structured-concurrency record mutation surface used by BigSyncKit.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitRecordStore: Sendable {
    func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitRecordStore {
    private func recordMutationConfiguration() -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        configuration.timeoutIntervalForRequest = 60
        // Asset-backed uploads observed in the Development sandbox can make
        // steady progress for well over a minute. Bound the whole resource
        // without turning a healthy transfer into a false timeout.
        configuration.timeoutIntervalForResource = 600
        return configuration
    }

    public func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        try await database.configuredWith(
            configuration: recordMutationConfiguration()
        ) { database in
            let results = try await database.modifyRecords(
                saving: records,
                deleting: recordIDs,
                savePolicy: savePolicy,
                atomically: atomically
            )
            return CloudKitRecordMutationResults(
                saveResults: results.saveResults,
                deleteResults: results.deleteResults
            )
        }
    }
}
