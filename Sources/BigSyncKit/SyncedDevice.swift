//import Foundation
//import RealmSwift
//import RealmSwiftGaps
//
//// Define a custom Device struct for platform-specific device information
//#if os(macOS)
//import AppKit
//import IOKit
//
//fileprivate struct Device {
//    static var current: Device { Device() }
//    
//    @BigSyncBackgroundActor
//    var localizedModel: String {
//        return getHardwareModel() ?? "Mac"
//    }
//    
//    // Method to get the hardware model on macOS
//    private func getHardwareModel() -> String? {
//        var size: size_t = 0
//        sysctlbyname("hw.model", nil, &size, nil, 0)
//        var model = [CChar](repeating: 0, count: Int(size))
//        sysctlbyname("hw.model", &model, &size, nil, 0)
//        return String(cString: model)
//    }
//}
//#elseif os(iOS)
//import UIKit
//
//fileprivate struct Device {
//    static var current: UIDevice { UIDevice.current }
//    
//    @BigSyncBackgroundActor
//    var localizedModel: String { UIDevice.current.localizedModel }
//}
//#endif
//
//public class SyncedDevice: Object, UnownedSyncableObject, ObjectKeyIdentifiable {
//    @Persisted(primaryKey: true) public var id = UUID()
//    @Persisted public var deviceName: String = ""
//    
//    @Persisted public var lastSeenOnline: Date
//    
//    @Persisted public var modifiedAt: Date
//    @Persisted public var isDeleted = false
//    
//    public override init() {
//        super.init()
//    }
//    
//    public var needsSyncToAppServer: Bool {
//        return false
//    }
//    
//    // TODO: To restore this, its crashes must be fixed; I think it's writing to user's realm file but not using the same actor as user's realm's other writers
////    @RealmBackgroundActor
////    static func updateLastSeenOnlineIfNeeded(forUUID uuid: UUID, realmConfiguration: Realm.Configuration, force: Bool = false) async throws {
////        let realm = try await Realm(configuration: realmConfiguration, actor: RealmBackgroundActor.shared)
////        
////        // Ensure the device name is fetched on the main actor
////        let deviceName = await BigSyncBackgroundActor.run {
////            Device.current.localizedModel
////        }
////        
////        try await realm.asyncWrite {
////            if let device = realm.object(ofType: SyncedDevice.self, forPrimaryKey: uuid) {
////                if force || device.lastSeenOnline.distance(to: Date()) > TimeInterval(60 * 60 * 1) {
////                    device.deviceName = deviceName
////                    device.lastSeenOnline = Date()
////                    device.modifiedAt = Date()
////                    device.isDeleted = false
////                }
////            } else {
////                _ = realm.create(SyncedDevice.self, value: [
////                    "id": uuid,
////                    "deviceName": deviceName, // Use the deviceName obtained from the custom Device struct
////                    "lastSeenOnline": Date(),
////                    "isDeleted": false,
////                ], update: .modified)
////            }
////        }
////    }
//}
