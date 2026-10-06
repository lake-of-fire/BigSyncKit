// Portable test collaborator only; this is not the native SDK or adapter implementation.
import Foundation
public enum CKRecordZone {
    public struct ID: Hashable, Sendable { public let zoneName: String
        public init(zoneName: String) { self.zoneName = zoneName }
    }
}
public final class CKRecord: @unchecked Sendable {
    public struct ID: Hashable, Sendable {
        public let recordName: String
        public let zoneID: CKRecordZone.ID
        public init(recordName: String, zoneID: CKRecordZone.ID) {
            self.recordName = recordName; self.zoneID = zoneID
        }
    }
    public let recordID: ID
    public let recordType: String
    public var recordChangeTag: String?
    public init(recordType: String, recordID: ID) {
        self.recordType = recordType; self.recordID = recordID
    }
}
