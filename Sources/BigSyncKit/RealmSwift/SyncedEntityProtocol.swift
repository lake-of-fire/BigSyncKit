//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

public protocol SoftDeletable: RealmSwift.Object, Identifiable {
    var isDeleted: Bool { get }
}

public protocol SyncableBase: SoftDeletable, Codable {
    var modifiedAt: Date { get }
    var needsSyncToServer: Bool { get }
}

public protocol UnownedSyncableObject: SyncableBase {
}

public protocol SyncableObject: SyncableBase {
    var owner: OwnerObject? { get }
    
    associatedtype OwnerObject: Object
}
