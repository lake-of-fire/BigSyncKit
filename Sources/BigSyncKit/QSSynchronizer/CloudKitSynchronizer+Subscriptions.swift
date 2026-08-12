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
    let runContext: CloudKitSynchronizer.RunContext?
}

@available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
public extension CloudKitSynchronizer {
    @BigSyncBackgroundActor
    private func makeSubscriptionAccountFence() async throws
        -> CloudKitSubscriptionAccountFence {
        let runContext = activeRunContext
        let accountIdentifier = try await accountIdentifierProvider()
        if let runContext {
            try checkRunContext(runContext)
            guard accountIdentifier == runContext.accountIdentifier else {
                throw OneOffRecordZoneResetError.cloudKitAccountChanged
            }
        }
        return CloudKitSubscriptionAccountFence(
            accountIdentifier: accountIdentifier,
            runContext: runContext
        )
    }

    @BigSyncBackgroundActor
    private func revalidateSubscriptionAccountFence(
        _ fence: CloudKitSubscriptionAccountFence
    ) async throws {
        if let runContext = fence.runContext {
            try await revalidateRunContext(runContext)
            return
        }
        let currentAccountIdentifier = try await accountIdentifierProvider()
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
    
    /// Returns identifier for a registered `CKSubscription` to track changes in the synchronizer's database.
    /// - Returns: Identifier of an existing `CKSubscription` for this database, if there is one.
    @BigSyncBackgroundActor
    func subscriptionIDForDatabaseSubscription() -> String? {
        return self.databaseSubscriptionID
    }
    
    /**
     *  Creates a new database subscription with CloudKit so the application can receive notifications when new changes happen. The application is responsible for registering for remote notifications and initiating synchronization when a notification is received. @see `CKSubscription`
     *
     *  -Parameter completion Block that will be called after subscription is created, with an optional error.
     */
    
    
    /// Creates a new database subscription with CloudKit so the application can receive notifications when new changes happen. The application is responsible for registering for remote notifications and initiating synchronization when a notification is received. @see `CKSubscription`
    /// - Parameter completion: Block that will be called after subscription is created, with an optional error.
    @BigSyncBackgroundActor
    func subscribeForChangesInDatabase(completion: ((Error?) -> ())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await subscribeForChangesInDatabase()
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func subscribeForChangesInDatabase() async throws {
        let expectedSubscriptionID = ownedSubscriptionID(kind: "database")
        if let storedSubscriptionID = subscriptionIDForDatabaseSubscription() {
            guard storedSubscriptionID == expectedSubscriptionID else {
                // Do not perpetuate an older arbitrary ID: it may have been
                // adopted from another CloudKit client by pre-v2 code.
                databaseSubscriptionID = nil
                return try await subscribeForChangesInDatabase()
            }
            return
        }
        let accountFence = try await makeSubscriptionAccountFence()
        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        if let existing = existing as? CKDatabaseSubscription {
            databaseSubscriptionID = existing.subscriptionID
            return
        }

        let subscription = CKDatabaseSubscription(
            subscriptionID: expectedSubscriptionID
        )
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        let saved = try await subscriptionStore.save(subscription: subscription)
        try await revalidateSubscriptionAccountFence(accountFence)
        guard saved.subscriptionID == expectedSubscriptionID else {
            throw CocoaError(.coderValueNotFound)
        }
        databaseSubscriptionID = expectedSubscriptionID
    }
    
    /// Creates a new subscription with CloudKit so the application can receive notifications when new changes happen. The application is responsible for registering for remote notifications and initiating synchronization when a notification is received. @see `CKSubscription`
    /// - Parameters:
    ///   - zoneID: `CKRecordZoneID` to track for changes
    ///   - completion: Block that will be called after subscription is created, with an optional error.
    @BigSyncBackgroundActor
    func subscribeForChanges(in zoneID: CKRecordZone.ID, completion: ((Error?)->())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await subscribeForChanges(in: zoneID)
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func subscribeForChanges(in zoneID: CKRecordZone.ID) async throws {
        let expectedSubscriptionID = ownedSubscriptionID(
            kind: "zone",
            zoneID: zoneID
        )
        if let storedSubscriptionID = subscriptionID(forRecordZoneID: zoneID) {
            guard storedSubscriptionID == expectedSubscriptionID else {
                // See the database-subscription equivalent above. Clear only
                // local metadata; an unknown server subscription is not ours
                // to delete.
                clearStoredSubscriptionID(for: zoneID)
                return try await subscribeForChanges(in: zoneID)
            }
            return
        }
        let accountFence = try await makeSubscriptionAccountFence()
        let existing = try await subscriptionStore.subscription(
            withID: expectedSubscriptionID
        )
        try await revalidateSubscriptionAccountFence(accountFence)
        if let existing = existing as? CKRecordZoneSubscription,
           existing.zoneID == zoneID {
            storeSubscriptionID(existing.subscriptionID, for: zoneID)
            return
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
        guard saved.subscriptionID == expectedSubscriptionID else {
            throw CocoaError(.coderValueNotFound)
        }
        storeSubscriptionID(expectedSubscriptionID, for: zoneID)
    }
    
    /**
     *  Delete existing database subscription to stop receiving notifications.
     *
     *  -Parameter completion Block that will be called after subscription is deleted, with an optional error.
     */
    
    /// Delete existing database subscription to stop receiving notifications.
    /// - Parameter completion: Block that will be called after subscription is deleted, with an optional error.
    @BigSyncBackgroundActor
    @objc func cancelSubscriptionForChangesInDatabase(completion: ((Error?)->())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await cancelSubscriptionForChangesInDatabase()
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    func cancelSubscriptionForChangesInDatabase() async throws {
        let accountFence = try await makeSubscriptionAccountFence()
        let expectedSubscriptionID = ownedSubscriptionID(kind: "database")
        let subscriptionID: String?
        if let stored = subscriptionIDForDatabaseSubscription() {
            if stored == expectedSubscriptionID {
                subscriptionID = stored
            } else {
                // Pre-v2 code may have persisted another client's ID. Clear
                // only our local pointer; never delete an unowned server
                // subscription.
                databaseSubscriptionID = nil
                subscriptionID = try await subscriptionStore.subscription(
                    withID: expectedSubscriptionID
                ).flatMap { $0 is CKDatabaseSubscription ? $0.subscriptionID : nil }
                try await revalidateSubscriptionAccountFence(accountFence)
            }
        } else {
            subscriptionID = try await subscriptionStore.subscription(
                withID: expectedSubscriptionID
            ).flatMap { $0 is CKDatabaseSubscription ? $0.subscriptionID : nil }
            try await revalidateSubscriptionAccountFence(accountFence)
        }
        guard let subscriptionID else { return }
        try await cancelSubscription(
            identifier: subscriptionID,
            accountFence: accountFence
        )
    }
    
    /// Delete existing subscription to stop receiving notifications.
    /// - Parameters:
    ///   - zoneID: `CKRecordZoneID` to stop tracking for changes.
    ///   - completion: Block that will be called after subscription is deleted, with an optional error.
    @BigSyncBackgroundActor
    @objc func cancelSubscriptionForChanges(in zoneID: CKRecordZone.ID, completion: ((Error?)->())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await cancelSubscriptionForChanges(in: zoneID)
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
        let accountFence = try await makeSubscriptionAccountFence()
        let expectedSubscriptionID = ownedSubscriptionID(
            kind: "zone",
            zoneID: zoneID
        )
        let resolvedSubscriptionID: String?
        if let stored = subscriptionID(forRecordZoneID: zoneID) {
            if stored == expectedSubscriptionID {
                resolvedSubscriptionID = stored
            } else {
                clearStoredSubscriptionID(for: zoneID)
                resolvedSubscriptionID = try await subscriptionStore.subscription(
                    withID: expectedSubscriptionID
                ).flatMap { subscription in
                    guard let zoneSubscription = subscription as? CKRecordZoneSubscription,
                          zoneSubscription.zoneID == zoneID else { return nil }
                    return zoneSubscription.subscriptionID
                }
                try await revalidateSubscriptionAccountFence(accountFence)
            }
        } else {
            resolvedSubscriptionID = try await subscriptionStore.subscription(
                withID: expectedSubscriptionID
            ).flatMap { subscription in
                guard let zoneSubscription = subscription as? CKRecordZoneSubscription,
                      zoneSubscription.zoneID == zoneID else { return nil }
                return zoneSubscription.subscriptionID
            }
            try await revalidateSubscriptionAccountFence(accountFence)
        }
        guard let resolvedSubscriptionID else { return }
        try await cancelSubscription(
            identifier: resolvedSubscriptionID,
            accountFence: accountFence
        )
    }
    
    @BigSyncBackgroundActor
    fileprivate func cancelSubscription(identifier: String, completion: ((Error?)->())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                let accountFence = try await makeSubscriptionAccountFence()
                try await cancelSubscription(
                    identifier: identifier,
                    accountFence: accountFence
                )
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    fileprivate func cancelSubscription(
        identifier: String,
        accountFence: CloudKitSubscriptionAccountFence
    ) async throws {
        try await revalidateSubscriptionAccountFence(accountFence)
        try await subscriptionStore.deleteSubscription(withID: identifier)
        try await revalidateSubscriptionAccountFence(accountFence)
        clearSubscriptionID(identifier)
    }
}
