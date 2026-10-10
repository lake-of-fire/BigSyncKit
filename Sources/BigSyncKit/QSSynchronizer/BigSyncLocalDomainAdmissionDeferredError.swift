import Foundation

/// A domain's initial replica-binding admission cannot safely inspect its local
/// stores while another local transaction owns a required store. The callback
/// may throw this only for temporary local admission unavailability, before
/// publishing a dataset/head decision. Stale account, installation, binding or
/// selected authority must retain their original errors.
///
/// BigSync keeps the current synchronization drain's waiters and retries through
/// its existing delayed local-target retry path. Cancellation and replacement
/// retain the scheduler's existing attempt and account checks.
public struct BigSyncLocalDomainAdmissionDeferredError: Error, Equatable, Sendable {
    public init() {}
}
