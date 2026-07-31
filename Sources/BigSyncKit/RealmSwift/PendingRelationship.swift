//
//  PendingRelationship.swift
//  Pods
//
//  Created by Manuel Entrena on 29/08/2017.
//
//

import Foundation
import RealmSwift

class PendingRelationship: Object {
    @objc dynamic var relationshipName: String!
    @objc dynamic var targetIdentifier: String!
    @objc dynamic var position: Int = 0
    @objc dynamic var sourceRecordChangeTag: String?
    @objc dynamic var expectedModifiedAt: Date?
    @objc dynamic var expectedExplicitlyModifiedAt: Date?
    @objc dynamic var forSyncedEntity: SyncedEntity!
}
