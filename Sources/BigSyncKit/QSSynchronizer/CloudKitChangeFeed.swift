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

/// Stored history corruption requires fenced recovery. A malformed new page is
/// a separate feed failure and does not justify resetting intact stored history.
public enum CloudKitChangeFeedError: Error, Sendable, Equatable {
    case corruptCursor
    /// A proposed next checkpoint is empty. The page cannot be committed,
    /// but this does not mean a previously persisted checkpoint is corrupt.
    case invalidPageCursor
}

/// Database and zone history tokens intentionally have distinct types. Their
/// archived bytes are private to BigSyncKit so fakes can use stable cursors
/// without fabricating `CKServerChangeToken` instances.
public struct DatabaseChangeCursor: Sendable, Hashable {
    fileprivate let data: Data
    public init(serializedData: Data) { data = serializedData }
    public var serializedData: Data { data }

    /// Only an absent stored value means first fetch. Nonempty bytes remain
    /// opaque here; the default CloudKit transport validates their secure archive.
    init?(persistedValue: Any?) throws {
        guard let persistedValue else { return nil }
        guard let data = persistedValue as? Data, !data.isEmpty else {
            throw CloudKitChangeFeedError.corruptCursor
        }
        self.data = data
    }
}
public struct RecordZoneChangeCursor: Sendable, Hashable {
    fileprivate let data: Data
    public init(serializedData: Data) { data = serializedData }
    public var serializedData: Data { data }

    /// A legacy token row with nil data is an intentional cleared checkpoint.
    /// More than one row is ambiguous even when the bytes happen to agree.
    /// Inspect at most two rows; never choose a history by collection order.
    init?<Values: Sequence>(persistedValues: Values) throws where Values.Element == Data? {
        var iterator = persistedValues.makeIterator()
        guard let value = iterator.next() else { return nil }
        guard iterator.next() == nil else { throw CloudKitChangeFeedError.corruptCursor }
        guard let value else { return nil }
        guard !value.isEmpty else { throw CloudKitChangeFeedError.corruptCursor }
        data = value
    }
}

private func archive(_ token: CKServerChangeToken) throws -> Data {
    return try NSKeyedArchiver.archivedData(withRootObject: token, requiringSecureCoding: true)
}
private func unarchive(_ data: Data) throws -> CKServerChangeToken {
    guard !data.isEmpty else { throw CloudKitChangeFeedError.corruptCursor }
    // Scripted feeds deliberately use opaque cursor bytes. Only the default
    // CloudKit transport decodes CloudKit's secure token representation. A
    // present cursor which cannot be decoded must *not* become nil. Absence
    // means a first/full fetch; corruption requires the synchronizer's explicit
    // recovery boundary before it can discard progress and rebuild tracking.
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
    init(token: CKServerChangeToken) throws { self.data = try archive(token) }
    func token() throws -> CKServerChangeToken { try unarchive(data) }
}
extension RecordZoneChangeCursor {
    init(token: CKServerChangeToken) throws { self.data = try archive(token) }
    func token() throws -> CKServerChangeToken { try unarchive(data) }
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
            return try CloudKitRecordZoneChangePage(cursor: try RecordZoneChangeCursor(token: page.changeToken), records: page.modificationResultsByID.values.map { try $0.get().record }, deletedRecordIDs: page.deletions.map(\.recordID), moreComing: page.moreComing)
        })
    }
}
