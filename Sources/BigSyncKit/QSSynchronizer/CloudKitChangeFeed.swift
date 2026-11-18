import CloudKit

/// The page-oriented CloudKit history surface used by the synchronizer.
///
/// Keeping this separate from `CloudKitDatabaseAdapter` lets download tests use
/// deterministic pages without coupling them to mutation or subscription APIs.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitChangeFeed: Sendable {
    func databaseChanges(
        since cursor: DatabaseChangeCursor?,
        resultsLimit: Int?
    ) async throws -> CloudKitDatabaseChangePage

    func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage
}

/// A persisted CloudKit cursor is opaque transport state. If its secure archive
/// can no longer be decoded, the synchronizer must enter its fenced server-first
/// rebuild instead of silently treating the cursor as a normal nil token.
public enum CloudKitChangeFeedError: Error, Sendable, Equatable {
    case corruptCursor
}

/// Database and zone history tokens intentionally have distinct types. Their
/// archived bytes are private to BigSyncKit so fakes can use stable cursors
/// without fabricating `CKServerChangeToken` instances.
public struct DatabaseChangeCursor: Sendable, Hashable {
    fileprivate let data: Data
    public init(serializedData: Data) { data = serializedData }
    public var serializedData: Data { data }
}
public struct RecordZoneChangeCursor: Sendable, Hashable {
    fileprivate let data: Data
    public init(serializedData: Data) { data = serializedData }
    public var serializedData: Data { data }
}

private func archive(_ token: CKServerChangeToken?) throws -> Data? {
    guard let token else { return nil }
    return try NSKeyedArchiver.archivedData(withRootObject: token, requiringSecureCoding: true)
}
private func unarchive(_ data: Data?) throws -> CKServerChangeToken? {
    guard let data else { return nil }
    // Scripted feeds deliberately use opaque cursor bytes. Only the default
    // CloudKit transport decodes CloudKit's secure token representation. A
    // nonempty cursor which cannot be decoded must *not* become nil: doing so
    // would turn corrupt progress into a normal full fetch, which cannot
    // faithfully replay deletions that happened before the corrupt cursor.
    let token: CKServerChangeToken?
    do {
        token = try NSKeyedUnarchiver.unarchivedObject(
            ofClass: CKServerChangeToken.self,
            from: data
        )
    } catch {
        throw CloudKitChangeFeedError.corruptCursor
    }
    guard let token else {
        throw CloudKitChangeFeedError.corruptCursor
    }
    return token
}
extension DatabaseChangeCursor {
    init(token: CKServerChangeToken?) throws { self.data = try archive(token) ?? Data() }
    func token() throws -> CKServerChangeToken? { try unarchive(data.isEmpty ? nil : data) }
}
extension RecordZoneChangeCursor {
    init(token: CKServerChangeToken?) throws { self.data = try archive(token) ?? Data() }
    func token() throws -> CKServerChangeToken? { try unarchive(data.isEmpty ? nil : data) }
}

public enum CloudKitZoneDeletionKind: String, Sendable, Equatable, Codable {
    case deleted
    case purged
    case encryptedDataReset
    case unknown
}
public struct CloudKitZoneDeletion: Sendable {
    public let zoneID: CKRecordZone.ID
    public let kind: CloudKitZoneDeletionKind

    public init(zoneID: CKRecordZone.ID, kind: CloudKitZoneDeletionKind) {
        self.zoneID = zoneID
        self.kind = kind
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public struct CloudKitDatabaseChangePage: Sendable {
    public let cursor: DatabaseChangeCursor
    public let changedZoneIDs: [CKRecordZone.ID]
    public let deletions: [CloudKitZoneDeletion]
    public let moreComing: Bool

    public init(
        cursor: DatabaseChangeCursor,
        changedZoneIDs: [CKRecordZone.ID],
        deletions: [CloudKitZoneDeletion],
        moreComing: Bool
    ) {
        self.cursor = cursor
        self.changedZoneIDs = changedZoneIDs
        self.deletions = deletions
        self.moreComing = moreComing
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public struct CloudKitRecordZoneChangePage: Sendable {
    public let cursor: RecordZoneChangeCursor
    public let records: [CKRecord]
    public let deletedRecordIDs: [CKRecord.ID]
    public let moreComing: Bool

    public init(
        cursor: RecordZoneChangeCursor,
        records: [CKRecord],
        deletedRecordIDs: [CKRecord.ID],
        moreComing: Bool
    ) {
        self.cursor = cursor
        self.records = records
        self.deletedRecordIDs = deletedRecordIDs
        self.moreComing = moreComing
    }

    private struct ModificationIdentityMismatch: LocalizedError {
        var errorDescription: String? {
            "The returned CloudKit record ID does not match its modification key."
        }
    }

    init<Modification>(
        modificationResultsByID: [CKRecord.ID: Result<Modification, Error>],
        changeToken: CKServerChangeToken?,
        deletedRecordIDs: [CKRecord.ID],
        moreComing: Bool,
        recordForModification: (Modification) -> CKRecord
    ) throws {
        var records: [CKRecord] = []
        records.reserveCapacity(modificationResultsByID.count)
        var failures: [CKRecord.ID: Error] = [:]
        for (recordID, result) in modificationResultsByID {
            switch result {
            case .success(let modification):
                let record = recordForModification(modification)
                guard record.recordID == recordID else {
                    failures[recordID] = ModificationIdentityMismatch()
                    continue
                }
                records.append(record)
            case .failure(let error):
                failures[recordID] = error
            }
        }

        // A failed page must be replayed from the previous cursor. Keep the
        // record ID for malformed modifications and singleton zone losses.
        if failures.count == 1, let (recordID, error) = failures.first,
           !(error is ModificationIdentityMismatch) {
            let loss = CloudKitLossClassifier.classify(error: error, defaultZoneID: recordID.zoneID)
            if loss.zoneDispositions.isEmpty {
                throw error
            }
        }
        guard failures.isEmpty else {
            throw CKError(.partialFailure, userInfo: [
                CKPartialErrorsByItemIDKey: failures.mapValues { $0 as NSError },
            ])
        }
        self.init(
            cursor: try RecordZoneChangeCursor(token: changeToken),
            records: records,
            deletedRecordIDs: deletedRecordIDs,
            moreComing: moreComing
        )
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitChangeFeed {
    private func deletionKind(for deletion: CKDatabase.DatabaseChange.Deletion) -> CloudKitZoneDeletionKind {
        if #available(iOS 17.0, macOS 14.0, watchOS 10.0, *) {
            switch deletion.reason {
            case .deleted: return .deleted
            case .purged: return .purged
            case .encryptedDataReset: return .encryptedDataReset
            @unknown default: return .unknown
            }
        }
        return deletion.purged ? .purged : .unknown
    }

    private func historyConfiguration() -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        // CloudKit's async calls otherwise have no synchronizer-owned bound.
        // The returned page is immutable; no adapter state can be published by
        // a request after the caller has cancelled its run.
        configuration.timeoutIntervalForRequest = 45
        configuration.timeoutIntervalForResource = 90
        return configuration
    }

    public func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        return try await database.configuredWith(configuration: historyConfiguration(), body: { database in
            let page = try await database.databaseChanges(since: try cursor?.token(), resultsLimit: resultsLimit)
            return CloudKitDatabaseChangePage(cursor: try DatabaseChangeCursor(token: page.changeToken), changedZoneIDs: page.modifications.map(\.zoneID), deletions: page.deletions.map { CloudKitZoneDeletion(zoneID: $0.zoneID, kind: deletionKind(for: $0)) }, moreComing: page.moreComing)
        })
    }

    public func recordZoneChanges(in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?, desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        return try await database.configuredWith(configuration: historyConfiguration(), body: { database in
            let page = try await database.recordZoneChanges(inZoneWith: zoneID, since: try cursor?.token(), desiredKeys: desiredKeys, resultsLimit: resultsLimit)
            return try CloudKitRecordZoneChangePage(
                modificationResultsByID: page.modificationResultsByID,
                changeToken: page.changeToken,
                deletedRecordIDs: page.deletions.map(\.recordID),
                moreComing: page.moreComing,
                recordForModification: \.record
            )
        })
    }
}
