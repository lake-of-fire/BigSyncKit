//
//  SyncedEntityState.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation

enum SyncedEntityState: Int, Sendable {
    // Order of the raw values is significant; see recordsToUpload and nextStateToSync
    case new = 1
    case changed = 2
    case deletedLocally = 3
    case synced = 4
    case deletedRemotely = 5
}
