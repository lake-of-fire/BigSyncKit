import Dispatch

/// One request's result race, not ownership of the shared synchronization.
/// The timer wakes a waiter; the immutable monotonic deadline decides whether
/// a completion can win. Scheduling the timer late never renews the budget.
actor BigSyncDeadlineRace<Value: Sendable> {
    enum Outcome: Sendable {
        case completed(Value)
        case timedOut
        case cancelled
    }

    typealias Now = @Sendable () -> UInt64
    typealias Sleep = @Sendable (UInt64) async throws -> Void

    private let deadline: UInt64
    private let now: Now
    private var outcome: Outcome?
    private var continuation: CheckedContinuation<Outcome, Never>?

    init(
        durationNanoseconds: UInt64,
        now: @escaping Now = { DispatchTime.now().uptimeNanoseconds }
    ) {
        self.now = now
        let sum = now().addingReportingOverflow(durationNanoseconds)
        deadline = sum.overflow ? UInt64.max : sum.partialValue
    }

    /// Safe on the worker actor before any admission side effect. This only
    /// inspects immutable timing inputs, never actor-owned result state.
    nonisolated var remainingNanoseconds: UInt64 {
        let instant = now()
        return instant < deadline ? deadline - instant : 0
    }

    @discardableResult
    func resolve(_ proposed: Outcome) -> Bool {
        guard outcome == nil else { return false }
        let accepted: Outcome
        switch proposed {
        case .completed:
            accepted = remainingNanoseconds > 0 ? proposed : .timedOut
        case .timedOut:
            // Defensive against an early wake; the timer loops until expiry.
            guard remainingNanoseconds == 0 else { return false }
            accepted = .timedOut
        case .cancelled:
            accepted = .cancelled
        }
        outcome = accepted
        continuation?.resume(returning: accepted)
        continuation = nil
        return true
    }

    func value() async -> Outcome {
        if let outcome { return outcome }
        precondition(continuation == nil, "A deadline race has one request waiter")
        return await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
    }

    /// All sleeps consume the same absolute budget, including a delayed start
    /// or early wake. Cancellation of this timer is not deadline expiration.
    nonisolated func waitUntilDeadline(
        sleep: Sleep = { try await Task.sleep(nanoseconds: $0) }
    ) async {
        while !Task.isCancelled {
            let remaining = remainingNanoseconds
            if remaining == 0 {
                await resolve(.timedOut)
                return
            }
            do { try await sleep(remaining) }
            catch { return }
        }
    }
}
