//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

public protocol SyncableUser: RealmSwift.Object, Identifiable, Codable {
    var isDeleted: Bool { get }
    var modifiedAt: Date { get }
    var needsSyncToServer: Bool { get }
}

public protocol SyncableObject: RealmSwift.Object, Identifiable, Codable {
    var owner: AnyUser? { get }
    var modifiedAt: Date { get }
    var isDeleted: Bool { get }
    var needsSyncToServer: Bool { get }
    
    associatedtype AnyUser: SyncableUser
}
