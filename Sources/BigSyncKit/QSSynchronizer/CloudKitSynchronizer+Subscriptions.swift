//
//  CloudKitSynchronizer+Subscriptions.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation
import CloudKit
import CryptoKit

private struct CloudKitSubscriptionAccountFence: Sendable {
    let accountIdentifier: String
    let attemptID: UUID
    let runContext: CloudKitSynchronizer.RunContext?
}

private struct CloudKitSubscriptionOperationReservation: Sendable {
    let id: UUID
    let attemptID: UUID
    let runContext: CloudKitSynchronizer.RunContext?
}

/// CloudKit mutation APIs suspend and `BigSyncBackgroundActor` is reentrant.
/// Reserve an operation synchronously before any suspension so a later
/// Subscribe/Cancel intent cannot overtake an earlier opposite intent and then
/// be undone by the earlier transport response.
@BigSyncBackgroundActor
private final class CloudKitSubscriptionOperationGate {
    private var reservations = [UUID]()
    private var waiters = [UUID: CheckedContinuation<Void, Never>]()

    func reserve() -> UUID {
        let id = UUID()
        reservations.append(id)
        return id
    }

    func enter(_ id: UUID) async {
        precondition(
            reservations.contains(id),
            "Subscription operation entered without a reservation"
        )
        guard reservations.first != id else { return }
        await withCheckedContinuation { continuation in
            precondition(
                waiters[id] == nil,
                "Subscription operation waited more than once"
            )
            waiters[id] = continuation
        }
    }

    func leave(_ id: UUID) {
        precondition(
            reservations.first == id,
            "Subscription operations must leave in reservation order"
        )
        reservations.removeFirst()
        if let next = reservations.first,
           let continuation = waiters.removeValue(forKey: next) {
            continuation.resume()
        }
    }
}

@BigSyncBackgroundActor
private enum CloudKitSubscriptionOperationGates {
    private final class Entry {
        weak var owner: CloudKitSynchronizer?
        let gate: CloudKitSubscriptionOperationGate

        init(
            owner: CloudKitSynchronizer,
            gate: CloudKitSubscriptionOperationGate
        ) {
            self.owner = owner
            self.gate = gate
        }
    }

    private static var entries = [ObjectIdentifier: Entry]()

    static func gate(
        for synchronizer: CloudKitSynchronizer
    ) -> CloudKitSubscriptionOperationGate {
        entries = entries.filter { $0.value.owner != nil }
        let key = ObjectIdentifier(synchronizer)
        if let entry = entries[key],
           let owner = entry.owner,
           owner === synchronizer {
            return entry.gate
        }
        let entry = Entry(
            owner: synchronizer,
            gate: CloudKitSubscriptionOperationGate()
        )
        entries[key] = entry
        return entry.gate
    }
}

@available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
public extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    private func reserveSubscriptionOperation()
        -> CloudKitSubscriptionOperationReservation {
        let gate = CloudKitSubscriptionOperationGates.gate(for: self)
        return CloudKitSubscriptionOperationReservation(
            id: gate.reserve(),
            attemptID: synchronizationAttemptID,
            runContext: activeRunContext
        )
    }

    @BigSyncBackgroundActor
    private func enterSubscriptionOperation(
        _ reservation: CloudKitSubscriptionOperationReservation
    ) async throws -> CloudKitSubscriptionOperationGate {
        let gate = CloudKitSubscriptionOperationGates.gate(for: self)
        await gate.enter(reservation.id)
        do {
            try Task.checkCancellation()
            guard synchronizationAttemptID == reservation.attemptID else {
                throw CancellationError()
            }
            if let runContext = reservation.runContext {
                try checkRunContext(runContext)
            }
            return gate
        } catch {
            gate.leave(reservation.id)
            throw error
        }
    }

    @BigSyncBackgroundActor
    private func makeSubscriptionAccountFence(
        attemptID: UUID,
        runContext: CloudKitSynchronizer.RunContext?
    ) async throws -> CloudKitSubscriptionAccountFence {
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID else {
            throw CancellationError()
        }
        if let runContext {
            try checkRunContext(runContext)
        }
        try keyValueStore.bigSyncValidateDurability()
        let accountIdentifier = try await accountIdentifierProvider()
        try Task.checkCancellation()
        guard synchronizationAttemptID == attemptID else {
            throw CancellationError()
        }
        if let runContext {
            try checkRunContext(runContext)
            guard accountIdentifier == runContext.accountIdentifier else {
                throw OneOffRecordZoneResetError.cloudKitAccountChanged
            }
        }
        return CloudKitSubscriptionAccountFence(
            accountIdentifier: accountIdentifier,
            attemptID: attemptID,
            runContext: runContext
        )
    }

    @BigSyncBackgroundActor
    private func revalidateSubscriptionAccountFence(
        _ fence: CloudKitSubscriptionAccountFence
    ) async throws {
        try Task.checkCancellation()
        guard synchronizationAttemptID == fence.attemptID else {
            throw CancellationError()
        }
        if let runContext = fence.runContext {
            try await revalidateRunContext(runContext)
            return
        }
        let currentAccountIdentifier = try await accountIdentifierProvider()
        try Task.checkCancellation()
        guard synchronizationAttemptID == fence.attemptID else {
            throw CancellationError()
        }
        guard currentAccountIdentifier == fence.accountIdentifier else {
            throw OneOffRecordZoneResetError.cloudKitAccountChanged
        }
    }

    /// CloudKit subscriptions are shared by every client of this database.
    /// Never adopt an arbitrary subscription merely because it has the same
    /// type: it may belong to another feature/app and not request a
    /// content-available push.  Keep IDs deterministic so a reinstall can
    /// recover this synchronizer's own subscription without creating another.
    @BigSyncBackgroundActor
    private func ownedSubscriptionID(
        kind: String,
        zoneID: CKRecordZone.ID? = nil
    ) -> CKSubscription.ID {
        let zoneComponent: String
        if let zoneID {
            zoneComponent = "\(zoneID.ownerName)/\(zoneID.zoneName)"
        } else {
            zoneComponent = "database"
        }
        let source = [
            "BigSyncKit.Subscription.v2",
            identifier,
            containerIdentifier,
            String(database.databaseScope.rawValue),
            kind,
            zoneComponent,
        ].joined(separator: "|")
        let digest = SHA256.hash(data: Data(source.utf8))
            .map { String(format: "%02x", $0) }
            .joined()
        return "BigSyncKit.v2.\(kind).\(digest)"
    }

    /// Returns identifier for a registered `CKSubscription` to track changes.
    /// - Parameter zoneID: `CKRecordZoneID` that is being tracked with the subscription.
    /// - Returns: Identifier of an existing `CKSubscription` for the record zone, if there is one.
    @BigSyncBackgroundActor
    func subscriptionID(forRecordZoneID zoneID: CKRecordZone.ID) -> String? {
        return getStoredSubscriptionID(for: zoneID)
    }
    
    /// Returns identifier for a registered `CKSubscription` for this database.
    @BigSyncBackgroundActor
    func subscriptionIDForDatabaseSubscription() -> String? {
        return self.databaseSubscriptionID
    }
    
    /// Creates a database subscription so the application can receive change notifications.
    @BigSyncBackgroundActor
    func subscribeForChangesInDatabase(completion: ((Error?) -> ())?) {
        let reservation = reserveSubscriptionOperation()
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await subscribeForChangesInDatabase(
                    reservation: reservation
                )
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func subscribeForChangesInDatabase() async throws {
        let reservation = reserveSubscriptionOperation()
        try await subscribeForChangesInDatabase(reservation: reservation)
    }

    @BigSyncBackgroundActor
    private func subscribeForChangesInDatabase(
        reservation: CloudKitSubscriptionOperationReservation
    ) async throws {
        let operationGate = try await enterSubscriptionOperation(reservation)
        defer { operationGate.leave(reservation.id) }
        let expectedSubscriptionID = ownedSubscriptionID(kind: "database")
        let accountFence = try await makeSubscriptionAccountFence(
            attemptID: reservation.attemptID,
            runContext: reservation.runContext
        )
        if let storedSubscriptionID = subscriptionIDForDatabaseSubscription(),
           storedSubscriptionID != expectedSubscriptionID {
            // Do not perpetuate an older arbitrary ID: it may have been
            // adopted from another CloudKit client by pre-v2 code. Local
            // metadata is not proof that any server subscription still exists.
            try persistDatabaseSubscriptionID(nil)
        }
        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        if let existing {
            guard existing is CKDatabaseSubscription else {
                // A deterministic ID resolving to an incompatible server type
                // is not safe to overwrite or adopt as our registration.
                try persistDatabaseSubscriptionID(nil)
                throw CocoaError(.coderValueNotFound)
            }
            if existing.notificationInfo?.shouldSendContentAvailable == true {
                try persistDatabaseSubscriptionID(existing.subscriptionID)
                return
            }
        }

        let subscription = CKDatabaseSubscription(
            subscriptionID: expectedSubscriptionID
        )
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        let saved = try await subscriptionStore.save(subscription: subscription)
        try await revalidateSubscriptionAccountFence(accountFence)
        guard saved.subscriptionID == expectedSubscriptionID,
              saved is CKDatabaseSubscription,
              saved.notificationInfo?.shouldSendContentAvailable == true else {
            throw CocoaError(.coderValueNotFound)
        }
        try persistDatabaseSubscriptionID(expectedSubscriptionID)
    }
    
    /// Creates a record-zone subscription so the application can receive change notifications.
    @BigSyncBackgroundActor
    func subscribeForChanges(in zoneID: CKRecordZone.ID, completion: ((Error?)->())?) {
        let reservation = reserveSubscriptionOperation()
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await subscribeForChanges(
                    in: zoneID,
                    reservation: reservation
                )
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func subscribeForChanges(in zoneID: CKRecordZone.ID) async throws {
        let reservation = reserveSubscriptionOperation()
        try await subscribeForChanges(in: zoneID, reservation: reservation)
    }

    @BigSyncBackgroundActor
    private func subscribeForChanges(
        in zoneID: CKRecordZone.ID,
        reservation: CloudKitSubscriptionOperationReservation
    ) async throws {
        let operationGate = try await enterSubscriptionOperation(reservation)
        defer { operationGate.leave(reservation.id) }
        let expectedSubscriptionID = ownedSubscriptionID(
            kind: "zone",
            zoneID: zoneID
        )
        let accountFence = try await makeSubscriptionAccountFence(
            attemptID: reservation.attemptID,
            runContext: reservation.runContext
        )
        if let storedSubscriptionID = subscriptionID(forRecordZoneID: zoneID),
           storedSubscriptionID != expectedSubscriptionID {
            // See the database-subscription equivalent above. Clear only local
            // metadata; an unknown server subscription is not ours to delete.
            try persistSubscriptionID(nil, for: zoneID)
        }
        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        if let existing {
            guard let zoneSubscription = existing as? CKRecordZoneSubscription,
                  zoneSubscription.zoneID == zoneID else {
                try persistSubscriptionID(nil, for: zoneID)
                throw CocoaError(.coderValueNotFound)
            }
            if zoneSubscription.notificationInfo?.shouldSendContentAvailable == true {
                try persistSubscriptionID(zoneSubscription.subscriptionID, for: zoneID)
                return
            }
        }

        let subscription = CKRecordZoneSubscription(
            zoneID: zoneID,
            subscriptionID: expectedSubscriptionID
        )
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        let saved = try await subscriptionStore.save(subscription: subscription)
        try await revalidateSubscriptionAccountFence(accountFence)
        guard let saved = saved as? CKRecordZoneSubscription,
              saved.subscriptionID == expectedSubscriptionID,
              saved.zoneID == zoneID,
              saved.notificationInfo?.shouldSendContentAvailable == true else {
            throw CocoaError(.coderValueNotFound)
        }
        try persistSubscriptionID(expectedSubscriptionID, for: zoneID)
    }
    
    /// Deletes the database subscription to stop receiving notifications.
    @BigSyncBackgroundActor
    @objc func cancelSubscriptionForChangesInDatabase(completion: ((Error?)->())?) {
        let reservation = reserveSubscriptionOperation()
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await cancelSubscriptionForChangesInDatabase(
                    reservation: reservation
                )
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func cancelSubscriptionForChangesInDatabase() async throws {
        let reservation = reserveSubscriptionOperation()
        try await cancelSubscriptionForChangesInDatabase(
            reservation: reservation
        )
    }

    @BigSyncBackgroundActor
    private func cancelSubscriptionForChangesInDatabase(
        reservation: CloudKitSubscriptionOperationReservation
    ) async throws {
        let operationGate = try await enterSubscriptionOperation(reservation)
        defer { operationGate.leave(reservation.id) }
        let accountFence = try await makeSubscriptionAccountFence(
            attemptID: reservation.attemptID,
            runContext: reservation.runContext
        )
        let expectedSubscriptionID = ownedSubscriptionID(kind: "database")
        if let stored = subscriptionIDForDatabaseSubscription(),
           stored != expectedSubscriptionID {
            // Pre-v2 code may have persisted another client's ID. Clear only
            // our local pointer; never delete an unowned server subscription.
            try persistDatabaseSubscriptionID(nil)
        }

        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        guard let existing else {
            // Exact absence already satisfies cancellation. Clear stale local
            // registration without issuing a delete for a nonexistent object.
            try persistDatabaseSubscriptionID(nil)
            return
        }
        guard existing is CKDatabaseSubscription else {
            // Subscribe already treats an incompatible deterministic-ID object
            // as unowned. Cancellation must not become a destructive escape
            // hatch for that same collision.
            try persistDatabaseSubscriptionID(nil)
            throw CocoaError(.coderValueNotFound)
        }
        try await cancelSubscription(
            identifier: expectedSubscriptionID,
            accountFence: accountFence
        )
    }
    
    /// Deletes a record-zone subscription to stop receiving notifications.
    @BigSyncBackgroundActor
    @objc func cancelSubscriptionForChanges(in zoneID: CKRecordZone.ID, completion: ((Error?)->())?) {
        let reservation = reserveSubscriptionOperation()
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await cancelSubscriptionForChanges(
                    in: zoneID,
                    reservation: reservation
                )
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func cancelSubscriptionForChanges(
        in zoneID: CKRecordZone.ID
    ) async throws {
        let reservation = reserveSubscriptionOperation()
        try await cancelSubscriptionForChanges(
            in: zoneID,
            reservation: reservation
        )
    }

    @BigSyncBackgroundActor
    private func cancelSubscriptionForChanges(
        in zoneID: CKRecordZone.ID,
        reservation: CloudKitSubscriptionOperationReservation
    ) async throws {
        let operationGate = try await enterSubscriptionOperation(reservation)
        defer { operationGate.leave(reservation.id) }
        let accountFence = try await makeSubscriptionAccountFence(
            attemptID: reservation.attemptID,
            runContext: reservation.runContext
        )
        let expectedSubscriptionID = ownedSubscriptionID(
            kind: "zone",
            zoneID: zoneID
        )
        if let stored = subscriptionID(forRecordZoneID: zoneID),
           stored != expectedSubscriptionID {
            // A foreign local pointer is never deletion authority.
            try persistSubscriptionID(nil, for: zoneID)
        }

        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        guard let existing else {
            try persistSubscriptionID(nil, for: zoneID)
            return
        }
        guard let zoneSubscription = existing as? CKRecordZoneSubscription,
              zoneSubscription.zoneID == zoneID else {
            try persistSubscriptionID(nil, for: zoneID)
            throw CocoaError(.coderValueNotFound)
        }
        try await cancelSubscription(
            identifier: expectedSubscriptionID,
            accountFence: accountFence
        )
    }
    
    @BigSyncBackgroundActor
    fileprivate func cancelSubscription(
        identifier: String,
        accountFence: CloudKitSubscriptionAccountFence
    ) async throws {
        try await revalidateSubscriptionAccountFence(accountFence)
        do {
            try await subscriptionStore.deleteSubscription(withID: identifier)
        } catch {
            // A prior attempt (or another installation) may already have
            // deleted this deterministic ID. Its absence is the desired result,
            // not an error that should pin our cached registration forever.
            // An unrelated partial failure must not erase local retry state.
            guard subscriptionDeletionReportsMissingItem(
                error, identifier: identifier
            ) else { throw error }
        }
        // This also fences an unknownItem result: neither account replacement
        // nor cancellation may turn an old response into local success.
        try await revalidateSubscriptionAccountFence(accountFence)
        try persistRemovingSubscriptionID(identifier)
    }

    @BigSyncBackgroundActor
    private func subscriptionDeletionReportsMissingItem(
        _ error: Error,
        identifier: CKSubscription.ID
    ) -> Bool {
        guard let cloudKitError = error as? CKError else { return false }
        if cloudKitError.code == .unknownItem { return true }
        guard cloudKitError.code == .partialFailure,
              let failures = cloudKitError.userInfo[
                CKPartialErrorsByItemIDKey
              ] as? [AnyHashable: Error],
              failures.count == 1,
              let itemError = failures[identifier] as? CKError else {
            return false
        }
        return itemError.code == .unknownItem
    }
}
