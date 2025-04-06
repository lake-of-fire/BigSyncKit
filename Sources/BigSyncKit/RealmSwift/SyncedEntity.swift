//
//  SyncedEntity.swift
//  Pods
//
//  Created by Manuel Entrena on 29/08/2017.
//
//

import Foundation
import RealmSwift

class SyncedEntity: Object {
    @objc dynamic var entityType: String = ""
    @objc dynamic var identifier: String = ""
    @objc dynamic var state: Int = 0
    @objc dynamic var updated: Date? // TODO: Remove, unused (or start using for sorting recordsToUpload)
    @objc dynamic var share: SyncedEntity?
    @objc dynamic var encodedRecord: Data?

    convenience init(entityType: String, identifier: String, state: Int) {
        self.init()
        
        self.entityType = entityType
        self.identifier = identifier
        self.state = state
    }
    
    override static func primaryKey() -> String? {
        return "identifier"
    }
    
    var entityState: SyncedEntityState {
        set {
            state = newValue.rawValue
        }
        get {
            return SyncedEntityState(rawValue: state)!
        }
    }
}
