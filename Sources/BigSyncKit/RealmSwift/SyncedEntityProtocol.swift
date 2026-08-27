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

        let mutationContext = BigSyncMutationTrackingRegistry.mutationContext(
            className: entityType,
            in: realm
        )
        switch mutationContext.trackingStatus {
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
        let mutationGeneration = BigSyncMutationTrackingRegistry
            .makeMutationGeneration(context: mutationContext)
        let accountScopeIdentifier = BigSyncMutationTrackingRegistry
            .accountScopeIdentifier(
                for: object,
                entityType: entityType,
                propertyName: mutationContext.accountScopePropertyName
            )
        let replicaBindingGenerationIdentifier =
            mutationGeneration.replicaBindingGenerationIdentifier

        let mutation = realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ) ?? BigSyncPendingMutation(
            recordName: recordName,
            entityType: entityType,
            objectIdentifier: objectIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                replicaBindingGenerationIdentifier
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
        mutation.replicaBindingGenerationIdentifier =
            replicaBindingGenerationIdentifier
        mutation.generation = mutationGeneration.generation
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

/// Opts selected scalar Realm integer properties into a canonical decimal-
/// string CloudKit representation.
///
/// CloudKit can infer a newly introduced numeric field containing only zero or
/// one as a Boolean even when `CKRecord` receives an integer-shaped
/// `NSNumber`. Models whose semantic validation requires an exact integer use
/// this transport representation while retaining integer storage and queries
/// in Realm.
public protocol BigSyncStringEncodedIntegerModel {
    static var bigSyncStringEncodedIntegerPropertyNames: Set<String> { get }
}

public enum BigSyncStringEncodedIntegerCodec {
    public static func encode(_ value: Int64) -> String {
        String(value)
    }

    /// Decodes only the unique canonical spelling emitted by `encode`.
    /// Whitespace, leading zeroes, a leading plus, and negative zero are
    /// rejected so semantically equal values have one wire representation.
    public static func decode(_ value: Any?) -> Int64? {
        guard let string = value as? String,
              let integer = Int64(string),
              encode(integer) == string else {
            return nil
        }
        return integer
    }
}

/// Decodes the two representations a CloudKit Boolean can have at an inbound
/// boundary. Synthetic/local `CKRecord`s retain `CFBoolean`, while a value
/// fetched back from CloudKit is commonly an integer-shaped `NSNumber`.
/// Accept only exact integral zero or one so a floating-point or arbitrary
/// numeric field cannot be admitted as a Boolean semantic fact.
public enum BigSyncCloudKitBooleanCodec {
    public static func decode(_ value: Any?) -> Bool? {
        guard let number = value as? NSNumber else { return nil }
        if CFGetTypeID(number) == CFBooleanGetTypeID() {
            return number.boolValue
        }

        switch String(cString: number.objCType) {
        case "c", "C", "s", "S", "i", "I", "l", "L", "q", "Q", "B":
            switch number.int64Value {
            case 0:
                return false
            case 1:
                return true
            default:
                return nil
            }
        default:
            return nil
        }
    }
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
