import Foundation

/// Current-account CloudKit system-field proof retained in BigSync's tracking
/// Realm. Domain cutovers use this instead of treating target-Realm presence as
/// evidence that an unscoped legacy object belongs to the active account.
public struct BigSyncServerRecordEvidence: Equatable, Sendable {
    public let recordName: String
    public let entityType: String
    public let recordChangeTag: String
    public let serverModifiedAt: Date

    public init(
        recordName: String,
        entityType: String,
        recordChangeTag: String,
        serverModifiedAt: Date
    ) {
        self.recordName = recordName
        self.entityType = entityType
        self.recordChangeTag = recordChangeTag
        self.serverModifiedAt = serverModifiedAt
    }
}
