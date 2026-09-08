import Foundation
import RealmSwift

/// Target-Realm postimages staged before cursor publication or waiting for the
/// application repair callback after it. This is projection debt, not upload
/// work.
final class BigSyncPendingInboundIdentityDelivery: Object {
    static let canonicalID = "pending-inbound-identity-delivery-v1"

    @Persisted(primaryKey: true) var id = canonicalID
    @Persisted var deliveryID = ""
    /// Compatibility aggregate written before page chunking. It precedes the
    /// current receipt's page batches when the delivery is decoded.
    @Persisted var encodedIdentities = Data()
    @Persisted var encodedIdentityPageBatches = List<Data>()
    @Persisted var queuedDeliveryID = ""
    /// Compatibility aggregate written before page chunking. It precedes the
    /// queued receipt's page batches when the delivery is decoded.
    @Persisted var queuedEncodedIdentities = Data()
    @Persisted var queuedEncodedIdentityPageBatches = List<Data>()
    /// Compatibility aggregate written before page chunking. It precedes the
    /// staged page batches when the debt crosses a cursor boundary.
    @Persisted var stagedEncodedIdentities = Data()
    @Persisted var stagedEncodedIdentityPageBatches = List<Data>()
}
