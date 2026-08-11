import CloudKit

/// Structured-concurrency record-zone lifecycle surface used by BigSyncKit.
///
/// Keeping the per-item `Result` validation at the transport boundary prevents
/// a top-level successful request from being mistaken for a successful zone
/// mutation.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitZoneStore: Sendable {
    func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone

    func save(recordZone: CKRecordZone) async throws -> CKRecordZone

    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitZoneStore {
    private func zoneConfiguration() -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        configuration.timeoutIntervalForRequest = 60
        configuration.timeoutIntervalForResource = 180
        return configuration
    }

    public func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        try await database.configuredWith(
            configuration: zoneConfiguration()
        ) { database in
            try await database.recordZone(for: identifier)
        }
    }

    public func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        try await database.configuredWith(
            configuration: zoneConfiguration()
        ) { database in
            let results = try await database.modifyRecordZones(
                saving: [recordZone],
                deleting: []
            )
            guard let result = results.saveResults[recordZone.zoneID] else {
                throw CocoaError(.coderValueNotFound)
            }
            return try result.get()
        }
    }

    public func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        try await database.configuredWith(
            configuration: zoneConfiguration()
        ) { database in
            let results = try await database.modifyRecordZones(
                saving: [],
                deleting: [identifier]
            )
            guard let result = results.deleteResults[identifier] else {
                throw CocoaError(.coderValueNotFound)
            }
            try result.get()
        }
    }
}
