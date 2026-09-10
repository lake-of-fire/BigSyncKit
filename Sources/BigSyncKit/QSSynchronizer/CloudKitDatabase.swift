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
    /// Owning container when the caller can supply it. Production Realm
    /// setup does so, enabling exact long-lived operation recovery after
    /// process death without changing the public database adapter protocol.
    public let container: CKContainer?
    
    /// Initialize a `DefaultCloudKitDatabaseAdapter` with a given `CKDatabase`.
    public init(database: CKDatabase) {
        self.database = database
        self.container = nil
    }

    public init(database: CKDatabase, container: CKContainer) {
        self.database = database
        self.container = container
    }
    
    /// See https://developer.apple.com/documentation/cloudkit/ckdatabase/1640398-databasescope
    public var databaseScope: CKDatabase.Scope {
        return database.databaseScope
    }
    
}
