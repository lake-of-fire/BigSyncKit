import Foundation
import RealmSwift

class SyncedEntityType: Object {
    @objc dynamic var entityType: String = ""
    @objc dynamic var lastTrackedChangesAt: Date?
    
    convenience init(entityType: String, lastTrackedChangesAt: Date) {
        self.init()
        
        self.entityType = entityType
        self.lastTrackedChangesAt = lastTrackedChangesAt
    }
    
    override static func primaryKey() -> String? {
        return "entityType"
    }
}
