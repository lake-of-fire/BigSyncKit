//
//  CloudKitSynchronizer+Subscriptions.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation
import CloudKit

@available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
public extension CloudKitSynchronizer {
    private func fetchAllCloudKitSubscriptions() async throws
        -> [CKSubscription] {
        try await awaitCancellableCloudKitCallback { completion in
            database.fetchAllSubscriptions { subscriptions, error in
                if let error {
                    completion(.failure(error))
                } else {
                    completion(.success(subscriptions ?? []))
                }
            }
        }
    }

    private func saveCloudKitSubscription(
        _ subscription: CKSubscription
    ) async throws -> CKSubscription {
        try await awaitCancellableCloudKitCallback { completion in
            database.save(subscription: subscription) { subscription, error in
                if let error {
                    completion(.failure(error))
                } else if let subscription {
                    completion(.success(subscription))
                } else {
                    completion(.failure(CocoaError(.coderValueNotFound)))
                }
            }
        }
    }

    private func deleteCloudKitSubscription(
        identifier: String
    ) async throws {
        let _: Void = try await awaitCancellableCloudKitCallback { completion in
            database.delete(withSubscriptionID: identifier) { _, error in
                if let error {
                    completion(.failure(error))
                } else {
                    completion(.success(()))
                }
            }
        }
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
        guard subscriptionIDForDatabaseSubscription() == nil else { return }
        let context = activeRunContext
        let subscriptions = try await fetchAllCloudKitSubscriptions()
        if let context {
            try await revalidateRunContext(context)
        }
        if let existing = subscriptions.first(where: {
            $0 is CKDatabaseSubscription
        }) {
            databaseSubscriptionID = existing.subscriptionID
            return
        }

        let subscription = CKDatabaseSubscription()
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        let saved = try await saveCloudKitSubscription(subscription)
        if let context {
            try await revalidateRunContext(context)
        }
        databaseSubscriptionID = saved.subscriptionID
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
        guard subscriptionID(forRecordZoneID: zoneID) == nil else { return }
        let context = activeRunContext
        let subscriptions = try await fetchAllCloudKitSubscriptions()
        if let context {
            try await revalidateRunContext(context)
        }
        if let existing = subscriptions.compactMap({
            $0 as? CKRecordZoneSubscription
        }).first(where: { $0.zoneID == zoneID }) {
            storeSubscriptionID(existing.subscriptionID, for: zoneID)
            return
        }

        let subscription = CKRecordZoneSubscription(zoneID: zoneID)
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        let saved = try await saveCloudKitSubscription(subscription)
        if let context {
            try await revalidateRunContext(context)
        }
        storeSubscriptionID(saved.subscriptionID, for: zoneID)
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
        let subscriptionID: String?
        if let stored = subscriptionIDForDatabaseSubscription() {
            subscriptionID = stored
        } else {
            subscriptionID = try await fetchAllCloudKitSubscriptions()
                .first(where: { $0 is CKDatabaseSubscription })?
                .subscriptionID
        }
        guard let subscriptionID else { return }
        try await cancelSubscription(identifier: subscriptionID)
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
        let resolvedSubscriptionID: String?
        if let stored = subscriptionID(forRecordZoneID: zoneID) {
            resolvedSubscriptionID = stored
        } else {
            resolvedSubscriptionID = try await fetchAllCloudKitSubscriptions()
                .compactMap { $0 as? CKRecordZoneSubscription }
                .first(where: { $0.zoneID == zoneID })?
                .subscriptionID
        }
        guard let resolvedSubscriptionID else { return }
        try await cancelSubscription(identifier: resolvedSubscriptionID)
    }
    
    @BigSyncBackgroundActor
    fileprivate func cancelSubscription(identifier: String, completion: ((Error?)->())?) {
        Task { @BigSyncBackgroundActor [weak self] in
            guard let self else {
                completion?(CancellationError())
                return
            }
            do {
                try await cancelSubscription(identifier: identifier)
                completion?(nil)
            } catch {
                completion?(error)
            }
        }
    }

    @BigSyncBackgroundActor
    fileprivate func cancelSubscription(identifier: String) async throws {
        try await deleteCloudKitSubscription(identifier: identifier)
        clearSubscriptionID(identifier)
    }
}
