import Foundation

/// The existing default, record-level custom merge rule. It deliberately
/// orders modification metadata, not domain epochs or per-field causality.
/// A complete tie accepts the incoming server record. An absent target has no
/// local conflict clock: a newly constructed object's defaults are not user intent.
enum BigSyncRecordConflictPolicy {
    static func acceptsRemote(
        remoteExplicitlyModifiedAt: Date?,
        remoteModifiedAt: Date?,
        localExplicitlyModifiedAt: Date?,
        localModifiedAt: Date?,
        localObjectExists: Bool = true
    ) -> Bool {
        guard localObjectExists else { return true }
        let remoteExplicit = remoteExplicitlyModifiedAt ?? .distantPast
        let localExplicit = localExplicitlyModifiedAt ?? .distantPast
        if remoteExplicit > localExplicit { return true }
        if remoteExplicit == localExplicit {
            return (remoteModifiedAt ?? .distantPast) >= (localModifiedAt ?? .distantPast)
        }
        return false
    }
}
