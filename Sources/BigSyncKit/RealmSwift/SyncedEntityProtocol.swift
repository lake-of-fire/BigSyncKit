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

/// Model-owned, same-transaction invalidation for local metadata writes. This
/// does not authorize the write, create a journal, or validate the post-state.
/// Implementations must be synchronous and may touch only local control state.
public protocol BigSyncLocalTargetMutationObserving {
    func invalidateCertificationForLocalTargetWrite(in realm: Realm)
    /// Value projections may change even when graph certification remains valid.
    func invalidateValueProjectionForLocalTargetWrite(in realm: Realm)
}

public extension BigSyncLocalTargetMutationObserving {
    func invalidateValueProjectionForLocalTargetWrite(in realm: Realm) {}
}

public extension ChangeMetadataRecordable {
    func refreshChangeMetadata(explicitlyModified: Bool) {
        refreshChangeMetadata(explicitlyModified: explicitlyModified, at: Date())
    }

    func refreshChangeMetadata(explicitlyModified: Bool, at timestamp: Date) {
        if let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateCertificationForLocalTargetWrite(in: realm)
        }
        if let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateValueProjectionForLocalTargetWrite(in: realm)
        }
        modifiedAt = timestamp
        if explicitlyModified {
            explicitlyModifiedAt = timestamp
            recordBigSyncMutation(at: timestamp)
        }
    }

    /// Fail-closed form for a command admitted under one exact transport
    /// identity. The witness describes a generation minted by this call, not
    /// a pending row left by an earlier command. Let errors escape the Realm
    /// write so target values, metadata, and journals roll back together.
    @discardableResult
    func refreshChangeMetadata(
        explicitlyModified: Bool,
        at timestamp: Date,
        expectedJournalIdentity: BigSyncMutationJournalIdentity,
        invalidatingCertification: Bool = true
    ) throws -> BigSyncMutationJournalWitness {
        guard explicitlyModified else {
            throw BigSyncMutationJournalError.authoritativeMutationRequired
        }
        guard let witness = try recordBigSyncMutation(
            at: timestamp,
            expectedJournalIdentity: expectedJournalIdentity
        ) else {
            throw BigSyncMutationJournalError.identityUnavailable
        }
        if invalidatingCertification, let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateCertificationForLocalTargetWrite(in: realm)
        }
        if let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateValueProjectionForLocalTargetWrite(in: realm)
        }
        modifiedAt = timestamp
        explicitlyModifiedAt = timestamp
        return witness
    }

    /// Commits an ordinary authoritative update only if its upload work can be
    /// recorded. Call from the same throwing Realm write as the field changes;
    /// do not catch the error inside that write. Requires a managed, tracked
    /// object. Initialization and deliberately local-only writes use their
    /// separate nonauthoritative paths.
    ///
    /// The identity is sampled at this mutation, not at earlier command
    /// preparation. Source commands with captured authority must continue using
    /// the expected-identity/witness overload.
    func refreshChangeMetadataRequiringJournal(
        at timestamp: Date = Date(), invalidatingCertification: Bool = true
    ) throws {
        _ = try recordBigSyncMutation(
            at: timestamp, expectedJournalIdentity: nil,
            requiresAvailableIdentity: true
        )
        if invalidatingCertification, let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateCertificationForLocalTargetWrite(in: realm)
        }
        if let object = self as? Object, let realm = object.realm {
            (self as? BigSyncLocalTargetMutationObserving)?
                .invalidateValueProjectionForLocalTargetWrite(in: realm)
        }
        modifiedAt = timestamp
        explicitlyModifiedAt = timestamp
    }

    /// BigSync has already selected a complete, unchanged record value.
    /// Queue it under a fresh generation without pretending that retransmission
    /// is a new user edit. In particular, catalog repair must not advance the
    /// broad control record's conflict clock. Application commands use the
    /// expected-identity refresh API instead.
    internal func journalCurrentValuePreservingChangeMetadata(at timestamp: Date) throws {
        _ = try recordBigSyncMutation(
            at: timestamp, expectedJournalIdentity: nil,
            requiresAvailableIdentity: true
        )
    }

    private func recordBigSyncMutation(at timestamp: Date) {
        guard let object = self as? Object else {
            assertionFailure("BigSync mutations require a Realm Object")
            return
        }
        // Initializers may set metadata before Realm.add(). The final managed
        // refresh is still required to journal an authoritative mutation.
        guard object.realm != nil else { return }
        do {
            _ = try recordBigSyncMutation(
                at: timestamp,
                expectedJournalIdentity: nil
            )
        } catch BigSyncMutationJournalError.excludedModel {
            // Legacy callers also refresh deliberately local-only objects.
            return
        } catch BigSyncMutationJournalError.invalidAccountScope(let recordName) {
            preconditionFailure("BigSync account scope is invalid for \(recordName)")
        } catch BigSyncMutationJournalError.accountScopeChanged(let recordName) {
            preconditionFailure("BigSync account scope changed for \(recordName)")
        } catch {
            // Preserve the legacy diagnostic contract. Source-authoritative
            // callers use the throwing overload, whose error aborts the write.
            assertionFailure("BigSync mutation was not journaled: \(error)")
        }
    }

    // One implementation owns record identity, schema admission, account
    // scope, generation replacement, and pending-row persistence. The public
    // overloads differ only in identity admission and error handling.
    private func recordBigSyncMutation(
        at timestamp: Date,
        expectedJournalIdentity: BigSyncMutationJournalIdentity?,
        requiresAvailableIdentity: Bool = false
    ) throws -> BigSyncMutationJournalWitness? {
        guard let object = self as? Object, !object.isInvalidated else {
            throw BigSyncMutationJournalError.objectUnavailable
        }
        guard let realm = object.realm else {
            throw BigSyncMutationJournalError.objectUnavailable
        }
        let entityType = object.objectSchema.className
        guard realm.isInWriteTransaction else {
            throw BigSyncMutationJournalError.writeTransactionRequired
        }
        let mutationContext = BigSyncMutationTrackingRegistry.mutationContext(
            className: entityType,
            in: realm
        )
        switch mutationContext.trackingStatus {
        case .unregistered:
            throw BigSyncMutationJournalError.unregisteredModel(entityType)
        case .excluded:
            throw BigSyncMutationJournalError.excludedModel(entityType)
        case .tracked:
            break
        }
        guard realm.schema.objectSchema.contains(where: {
            $0.className == BigSyncPendingMutation.className()
        }) else {
            throw BigSyncMutationJournalError.missingJournalSchema
        }
        guard let primaryKey = object.objectSchema.primaryKeyProperty?.name,
              let value = object[primaryKey] as? CustomStringConvertible else {
            throw BigSyncMutationJournalError.unsupportedPrimaryKey(entityType)
        }
        let objectIdentifier = String(describing: value)
        let recordName = entityType + "." + objectIdentifier
        // Both generation paths sample their provider once. The strict path
        // additionally requires that sample to equal the admitted identity.
        let mutationGeneration: (
            generation: String,
            replicaBindingGenerationIdentifier: String?
        )
        if let expectedJournalIdentity {
            mutationGeneration = try BigSyncMutationTrackingRegistry
                .makeMutationGeneration(
                    context: mutationContext,
                    expectedIdentity: expectedJournalIdentity
                )
        } else if requiresAvailableIdentity {
            mutationGeneration = try BigSyncMutationTrackingRegistry
                .makeMutationGenerationRequiringIdentity(context: mutationContext)
        } else {
            mutationGeneration = BigSyncMutationTrackingRegistry
                .makeMutationGeneration(context: mutationContext)
        }
        let accountScopeIdentifier: String?
        if let property = mutationContext.accountScopePropertyName {
            guard object.objectSchema.properties.contains(where: {
                $0.name == property && $0.type == .string
            }), let scope = object[property] as? String, !scope.isEmpty else {
                throw BigSyncMutationJournalError.invalidAccountScope(recordName)
            }
            accountScopeIdentifier = scope
        } else {
            accountScopeIdentifier = nil
        }
        let mutation = realm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ) ?? BigSyncPendingMutation(
            recordName: recordName,
            entityType: entityType,
            objectIdentifier: objectIdentifier,
            accountScopeIdentifier: accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                mutationGeneration.replicaBindingGenerationIdentifier
        )
        guard mutation.entityType == entityType,
              mutation.objectIdentifier == objectIdentifier else {
            throw BigSyncMutationJournalError.witnessMismatch(recordName)
        }
        if let existingScope = mutation.accountScopeIdentifier,
           existingScope != accountScopeIdentifier,
           expectedJournalIdentity != nil || requiresAvailableIdentity
                || accountScopeIdentifier != nil {
            throw BigSyncMutationJournalError.accountScopeChanged(recordName)
        }
        if let accountScopeIdentifier {
            mutation.accountScopeIdentifier = accountScopeIdentifier
        }
        mutation.replicaBindingGenerationIdentifier =
            mutationGeneration.replicaBindingGenerationIdentifier
        mutation.generation = mutationGeneration.generation
        mutation.changedAt = timestamp
        realm.add(mutation, update: .modified)
        return expectedJournalIdentity.map { identity in
            BigSyncMutationJournalWitness(
                recordName: recordName,
                entityType: entityType,
                objectIdentifier: objectIdentifier,
                accountScopeIdentifier: accountScopeIdentifier,
                generation: mutationGeneration.generation,
                identity: identity
            )
        }
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
