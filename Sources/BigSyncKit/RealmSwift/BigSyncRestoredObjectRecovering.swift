import RealmSwift

/// Opt-in local admission for domain records copied from a backup. Retained
/// bytes are recovery material until a live server value is consumed. This is
/// not upload intent or deletion evidence and must not advance domain clocks.
/// Implementations store the flag as a local, non-synchronized property.
public protocol BigSyncRestoredObjectRecovering {
    var isAwaitingRecoveryEvidence: Bool { get }
    func retainForRestoreRecovery() throws
    func admitAfterServerEvidence() throws
}
