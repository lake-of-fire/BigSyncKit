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

public enum ChangeMetadataTimestampPolicy: Sendable {
    case refresh
    case preserve
}

public extension ChangeMetadataRecordable {
    func refreshChangeMetadata(
        explicitlyModified: Bool,
        timestampPolicy: ChangeMetadataTimestampPolicy = .refresh
    ) {
        let journalTimestamp: Date
        switch timestampPolicy {
        case .refresh:
            let timestamp = Date()
            modifiedAt = timestamp
            if explicitlyModified {
                explicitlyModifiedAt = timestamp
            }
            journalTimestamp = timestamp
        case .preserve:
            // The journal clock orders local forwarding events; it is not sync
            // conflict authority. Keep imported/canonical model timestamps
            // untouched while still recording when this Realm write occurred.
            journalTimestamp = Date()
        }
        if explicitlyModified {
            recordBigSyncMutation(at: journalTimestamp)
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

        guard let realm = object.realm else {
            guard BigSyncMutationTrackingRegistry.tracks(className: entityType) else { return }
            BigSyncMutationTrackingRegistry.enqueueUnbound(
                BigSyncPendingMutationSnapshot(
                    recordName: recordName,
                    entityType: entityType,
                    objectIdentifier: objectIdentifier,
                    generation: generation,
                    changedAt: timestamp
                )
            )
            return
        }
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
