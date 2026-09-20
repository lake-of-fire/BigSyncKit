import CloudKit
import Foundation
import RealmSwift

/// Read-only evidence from a pre-v3 BigSync tracking Realm. The source Realm is
/// copied before opening so current schema migration can never mutate the
/// released client's retained evidence.
public struct BigSyncLegacyTrackedRecordEvidence: Sendable, Equatable {
    public let recordName: String
    public let entityType: String
    public let recordChangeTag: String?
    public let deviceIdentifier: String?

    public init(
        recordName: String,
        entityType: String,
        recordChangeTag: String?,
        deviceIdentifier: String?
    ) {
        self.recordName = recordName
        self.entityType = entityType
        self.recordChangeTag = recordChangeTag
        self.deviceIdentifier = deviceIdentifier
    }
}

public enum BigSyncLegacyTrackingEvidence {
    public static func trackingRealmURL(
        appGroup: String,
        zoneID: CKRecordZone.ID
    ) -> URL {
        URL(
            fileURLWithPath: DefaultRealmSwiftAdapterProvider.realmPath(
                appGroup: appGroup,
                zoneID: zoneID
            )
        )
    }

    public static func storedDeviceIdentifier(
        suiteName: String,
        containerIdentifier: String,
        synchronizerIdentifier: String
    ) -> String? {
        let key = containerIdentifier
            + "-" + synchronizerIdentifier
            + "-QSCloudKitStoredDeviceUUIDKey"
        guard let value = UserDefaults(suiteName: suiteName)?
            .string(forKey: key),
              !value.isEmpty else {
            return nil
        }
        return value
    }

    @BigSyncBackgroundActor
    public static func inspectTrackingRealm(
        at sourceURL: URL
    ) throws -> [BigSyncLegacyTrackedRecordEvidence] {
        let fileManager = FileManager.default
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(
            atPath: sourceURL.path,
            isDirectory: &isDirectory
        ), !isDirectory.boolValue else {
            return []
        }

        let directory = fileManager.temporaryDirectory
            .appendingPathComponent(
                "BigSyncLegacyTrackingEvidence-" + UUID().uuidString,
                isDirectory: true
            )
        try fileManager.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        defer { try? fileManager.removeItem(at: directory) }
        let copyURL = directory.appendingPathComponent("tracking.realm")
        try fileManager.copyItem(at: sourceURL, to: copyURL)

        var configuration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        configuration.fileURL = copyURL
        var result = [BigSyncLegacyTrackedRecordEvidence]()
        try autoreleasepool {
            let realm = try Realm(configuration: configuration)
            for entity in realm.objects(SyncedEntity.self)
            where entity.entityState == .synced {
                guard !entity.identifier.isEmpty,
                      !entity.entityType.isEmpty,
                      let encoded = entity.encodedRecord,
                      let record = QSCoder.shared.object(from: encoded)
                        as? CKRecord,
                      record.recordID.recordName == entity.identifier,
                      record.recordType == entity.entityType else {
                    continue
                }
                result.append(
                    .init(
                        recordName: entity.identifier,
                        entityType: entity.entityType,
                        recordChangeTag: record.recordChangeTag,
                        deviceIdentifier: record[
                            CloudKitSynchronizer.deviceUUIDKey
                        ] as? String
                    )
                )
            }
        }
        return result.sorted {
            ($0.entityType, $0.recordName)
                < ($1.entityType, $1.recordName)
        }
    }
}
