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
        guard let object = self as? Object else {
            assertionFailure("BigSync mutations require a Realm Object")
            return
        }
        // Initializers commonly establish timestamps before Realm.add(). They
        // must refresh once after add, but the unmanaged initialization itself
        // is intentionally not diagnosed as a write-boundary violation.
        guard let realm = object.realm else { return }
        let entityType = object.objectSchema.className
        guard realm.isInWriteTransaction else {
            assertionFailure(
                "Explicit BigSync mutation for \(entityType) must occur inside a Realm write transaction"
            )
            return
        }

        switch BigSyncMutationTrackingRegistry.trackingStatus(
            className: entityType,
            in: realm
        ) {
        case .unregistered:
            assertionFailure(
                "No BigSync mutation policy was installed before opening Realm containing \(entityType)"
            )
            return
        case .excluded:
            return
        case .tracked:
            break
        }

        guard realm.schema.objectSchema.contains(where: {
            $0.className == BigSyncPendingMutation.className()
        }) else {
            assertionFailure(
                "Realm containing \(entityType) is missing BigSyncPendingMutation"
            )
            return
        }
        guard let primaryKey = object.objectSchema.primaryKeyProperty?.name else {
            assertionFailure("BigSync tracked type \(entityType) requires a primary key")
            return
        }

        let objectIdentifier = RealmSwiftAdapter.getTargetObjectStringIdentifier(
            for: object,
            usingPrimaryKey: primaryKey
        )
        let recordName = entityType + "." + objectIdentifier
        let generation = BigSyncMutationTrackingRegistry.makeGeneration(in: realm)
        let accountScopeIdentifier = BigSyncMutationTrackingRegistry
            .accountScopeIdentifier(
                for: object,
                entityType: entityType,
                in: realm
            )

        let mutation = realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ) ?? BigSyncPendingMutation(
            recordName: recordName,
            entityType: entityType,
            objectIdentifier: objectIdentifier,
            accountScopeIdentifier: accountScopeIdentifier
        )
        if let existingScope = mutation.accountScopeIdentifier,
           let accountScopeIdentifier,
           existingScope != accountScopeIdentifier {
            preconditionFailure(
                "BigSync account scope changed for immutable record \(recordName)"
            )
        }
        if let accountScopeIdentifier {
            mutation.accountScopeIdentifier = accountScopeIdentifier
        }
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
