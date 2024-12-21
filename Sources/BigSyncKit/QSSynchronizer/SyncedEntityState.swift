//
//  SyncedEntityState.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation

enum SyncedEntityState: Int {
    @available(*, deprecated, message: "Use new or changed")
    case newOrChanged = 1
    case deleted = 2
    case synced = 3
    case new = 4
    case changed = 5
}
