//
//  CloudKitDatabase.swift
//  SyncKit
//
//  Created by Manuel Entrena on 09/06/2019.
//  Copyright © 2019 Manuel Entrena. All rights reserved.
//

import Foundation
import CloudKit

/*
 `CloudKitDatabaseAdapter` carries the database identity shared by the focused
 async change-feed, record-mutation, subscription, and zone-store surfaces.
 Keeping those capabilities separate makes each transport boundary directly
 testable without retaining callback-based CloudKit operations.
 */

@objc public protocol CloudKitDatabaseAdapter {
    /// See https://developer.apple.com/documentation/cloudkit/ckdatabase/1640398-databasescope
    var databaseScope: CKDatabase.Scope { get }
    
}

@objc public class DefaultCloudKitDatabaseAdapter:
    NSObject,
    CloudKitDatabaseAdapter,
    @unchecked Sendable {
    
    
    /// The `CKDatabase` used by this adapter
    public let database: CKDatabase
    
    /// Initialize a `DefaultCloudKitDatabaseAdapter` with a given `CKDatabase`. All calls to the adapter methods will be forwarded to the database instance.
    /// - Parameter database:
    public init(database: CKDatabase) {
        self.database = database
    }
    
    /// See https://developer.apple.com/documentation/cloudkit/ckdatabase/1640398-databasescope
    public var databaseScope: CKDatabase.Scope {
        return database.databaseScope
    }
    
}
