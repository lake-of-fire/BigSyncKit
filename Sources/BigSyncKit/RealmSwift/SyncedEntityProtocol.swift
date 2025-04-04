//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

@objc public protocol ChangeMetadataRecordable: SoftDeletable {
    var createdAt: Date { get }
    var modifiedAt: Date { get set }
    var explicitlyModifiedAt: Date? { get set }
}

public extension ChangeMetadataRecordable {
    func refreshChangeMetadata(explicitlyModified: Bool) {
        modifiedAt = Date()
        if explicitlyModified {
            explicitlyModifiedAt = Date()
        }
    }
}

@objc public protocol SoftDeletable {
    var isDeleted: Bool { get set }
}

public protocol SyncableBase: ChangeMetadataRecordable, RealmSwift.Object, Identifiable, SoftDeletable, Codable {
    var needsSyncToAppServer: Bool { get }
}

public protocol UnownedSyncableObject: SyncableBase {
}

public protocol SyncableObject: SyncableBase {
    var ownerID: Int? { get }
}
