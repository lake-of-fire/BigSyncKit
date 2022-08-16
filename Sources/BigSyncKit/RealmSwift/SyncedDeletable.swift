//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

public protocol SyncedDeletable: RealmSwift.Object {
    var isDeleted: Bool { get }
}
