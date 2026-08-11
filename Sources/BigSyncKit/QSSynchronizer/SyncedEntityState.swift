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
    /// A tracking rebuild knew this record had previously reached CloudKit but
    /// its nil-token bootstrap has not supplied a live record or deletion.
    /// It is intentionally outside upload ordering and never creates work.
    case awaitingServerEvidence = 6
}
