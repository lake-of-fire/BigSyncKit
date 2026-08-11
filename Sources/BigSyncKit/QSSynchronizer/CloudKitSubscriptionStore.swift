import CloudKit

/// The async subscription surface used by `CloudKitSynchronizer`.
///
/// This remains separate from the older database adapter because subscriptions
/// are lifecycle metadata, not upload work. It also lets tests model exact-ID
/// lookup and per-item mutation results without a CloudKit callback bridge.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitSubscriptionStore: Sendable {
    /// Returns the subscription for this exact ID, or `nil` when it does not
    /// exist. Implementations must not turn other CloudKit errors into `nil`.
    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription?

    /// Saves exactly one subscription and verifies its individual result.
    func save(subscription: CKSubscription) async throws -> CKSubscription

    /// Deletes exactly one subscription and verifies its individual result.
    func deleteSubscription(withID identifier: CKSubscription.ID) async throws
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitSubscriptionStore {
    private func subscriptionConfiguration() -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        configuration.timeoutIntervalForRequest = 60
        configuration.timeoutIntervalForResource = 180
        return configuration
    }

    public func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        do {
            return try await database.configuredWith(
                configuration: subscriptionConfiguration()
            ) { database in
                try await database.subscription(for: identifier)
            }
        } catch let error as CKError where error.code == .unknownItem {
            return nil
        }
    }

    public func save(subscription: CKSubscription) async throws -> CKSubscription {
        try await database.configuredWith(
            configuration: subscriptionConfiguration()
        ) { database in
            let results = try await database.modifySubscriptions(
                saving: [subscription],
                deleting: []
            )
            guard let result = results.saveResults[subscription.subscriptionID] else {
                throw CocoaError(.coderValueNotFound)
            }
            return try result.get()
        }
    }

    public func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        try await database.configuredWith(
            configuration: subscriptionConfiguration()
        ) { database in
            let results = try await database.modifySubscriptions(
                saving: [],
                deleting: [identifier]
            )
            guard let result = results.deleteResults[identifier] else {
                throw CocoaError(.coderValueNotFound)
            }
            try result.get()
        }
    }
}
