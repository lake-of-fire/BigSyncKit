import Foundation

/// Optional local persistence overrides for an isolated synchronizer client.
///
/// Production clients normally leave this nil. Integration tests can provide
/// a complete namespace without clearing or borrowing another client's device
/// identifier, tokens, tracking Realm, or materialized assets.
public struct BigSyncLocalStateConfiguration {
    public let trackingRealmDirectoryURL: URL
    public let keyValueStore: any KeyValueStore
    public let assetDirectoryURL: URL

    public init(
        trackingRealmDirectoryURL: URL,
        keyValueStore: any KeyValueStore,
        assetDirectoryURL: URL
    ) {
        self.trackingRealmDirectoryURL = trackingRealmDirectoryURL
        self.keyValueStore = keyValueStore
        self.assetDirectoryURL = assetDirectoryURL
    }
}
