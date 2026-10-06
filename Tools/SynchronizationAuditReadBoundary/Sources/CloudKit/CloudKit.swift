import Foundation

// Codable stand-ins, not native CloudKit record encoding or transport.
public enum CKRecordZone {
    public struct ID: Hashable, Codable, Sendable {
        public let zoneName: String
        public init(zoneName: String) { self.zoneName = zoneName }
    }
}
public final class CKRecord: Codable, @unchecked Sendable {
    public struct ID: Hashable, Codable, Sendable {
        public let recordName: String
        public let zoneID: CKRecordZone.ID
        public init(recordName: String, zoneID: CKRecordZone.ID) {
            self.recordName = recordName; self.zoneID = zoneID
        }
    }
    public let recordType: String
    public let recordID: ID
    public var recordChangeTag: String?
    public var fields: [String: String] = [:]
    public init(recordType: String, recordID: ID) { self.recordType = recordType; self.recordID = recordID }
    public func allKeys() -> [String] { Array(fields.keys) }
}
