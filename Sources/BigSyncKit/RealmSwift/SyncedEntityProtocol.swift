//
//  SyncedDeletable.swift
//  BigSyncKit
//
//  Created by Alex Ehlke on 2021-10-10.
//

import Foundation
import RealmSwift

@objc public protocol ChangeMetadataRecordable: SoftDeletable {
    var createdAt: Date { get }
    var modifiedAt: Date { get set }
    var explicitlyModifiedAt: Date? { get set }
}

public extension ChangeMetadataRecordable {
    func refreshChangeMetadata(explicitlyModified: Bool) {
        refreshChangeMetadata(explicitlyModified: explicitlyModified, at: Date())
    }

    func refreshChangeMetadata(explicitlyModified: Bool, at timestamp: Date) {
        modifiedAt = timestamp
        if explicitlyModified {
            explicitlyModifiedAt = timestamp
            recordBigSyncMutation(at: timestamp)
        }
    }

    private func recordBigSyncMutation(at timestamp: Date) {
        guard let object = self as? Object,
              let primaryKey = object.objectSchema.primaryKeyProperty?.name else {
            return
        }

        let objectIdentifier = RealmSwiftAdapter.getTargetObjectStringIdentifier(
            for: object,
            usingPrimaryKey: primaryKey
        )
        let entityType = object.objectSchema.className
        let recordName = entityType + "." + objectIdentifier
        let generation = UUID().uuidString

        // Callers commonly initialize timestamps before adding a new object.
        // That is valid, but durable mutation capture can only happen in the
        // same write transaction after the object has entered Realm.
        guard let realm = object.realm else { return }
        guard realm.isInWriteTransaction,
              BigSyncMutationTrackingRegistry.tracks(className: entityType, in: realm),
              realm.schema.objectSchema.contains(where: {
                  $0.className == BigSyncPendingMutation.className()
              }) else {
            return
        }

        let mutation = realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ) ?? BigSyncPendingMutation(
            recordName: recordName,
            entityType: entityType,
            objectIdentifier: objectIdentifier
        )
        mutation.generation = generation
        mutation.changedAt = timestamp
        realm.add(mutation, update: .modified)
    }
}

@objc public protocol SoftDeletable {
    var isDeleted: Bool { get set }
}

@objc public protocol SyncSkippablePropertiesModel {
    func skipSyncingProperties() -> Set<String>?
}

/// Lets cache-backed models opt individual objects out of broad initial and
/// recovery scans. Normal journaled mutations and downloaded records are not
/// filtered through this protocol.
public protocol CloudKitInitialSyncEligibilityModel {
    static var initialCloudKitSyncEligibilityPredicate: NSPredicate { get }
}

/// Used for syncing with app servers, not just CloudKit.
public protocol SyncableBase: ChangeMetadataRecordable, RealmSwift.Object, Identifiable, SoftDeletable, Codable {
    /// Used in BigSyncKit to avoid hard-deleting after soft deletion before it has been synced to other application servers.
    var needsSyncToAppServer: Bool { get }
}

public protocol UnownedSyncableObject: SyncableBase {
}

public protocol SyncableObject: SyncableBase {
    var ownerID: Int? { get }
}
