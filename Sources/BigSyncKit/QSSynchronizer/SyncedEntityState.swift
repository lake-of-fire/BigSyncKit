//
//  SyncedEntityState.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation

enum SyncedEntityState: Int {
    case newOrChanged = 1
    case deleted
    case synced
    case inserted
}
