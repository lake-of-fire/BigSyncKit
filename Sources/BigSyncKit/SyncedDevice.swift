import Foundation
import RealmSwift
import RealmSwiftGaps
//import Device

public class SyncedDevice: Object, UnownedSyncableObject, ObjectKeyIdentifiable {
    @Persisted(primaryKey: true) public var id = UUID()
    @Persisted public var deviceName: String = ""
    
    @Persisted public var lastSeenOnline: Date
    
    @Persisted public var modifiedAt: Date
    @Persisted public var isDeleted = false
    
    public override init() {
        super.init()
    }
    
    public var needsSyncToServer: Bool {
        return false
    }
    
    @RealmBackgroundActor
    static func updateLastSeenOnlineIfNeeded(forUUID uuid: UUID, force: Bool = false) async throws {
        let realm = try await Realm(actor: RealmBackgroundActor.shared)
        try await realm.asyncWrite {
            if let device = realm.object(ofType: SyncedDevice.self, forPrimaryKey: uuid) {
                if force || device.lastSeenOnline.distance(to: Date()) > TimeInterval(60 * 60 * 1) {
//                    device.deviceName = Device.current.name
                    device.lastSeenOnline = Date()
                    device.modifiedAt = Date()
                    device.isDeleted = false
                }
            } else {
                let device = realm.create(SyncedDevice.self, value: [
                    "id": uuid,
//                    "deviceName": Device.current.name,
                    "lastSeenOnline": Date(),
                    "isDeleted": false,
                ], update: .modified)
            }
        }
    }
}
