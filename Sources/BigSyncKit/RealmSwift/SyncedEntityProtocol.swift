//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

public protocol SyncableUser: RealmSwift.Object {
    var isDeleted: Bool { get }
}

public protocol SyncableObject: RealmSwift.Object {
    var isDeleted: Bool { get }
    var owner: SyncableUser? { get }
}
