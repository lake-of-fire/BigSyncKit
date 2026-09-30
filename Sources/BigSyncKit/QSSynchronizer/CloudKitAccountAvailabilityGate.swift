import CloudKit
import Dispatch

enum CloudKitAccountAvailability: Equatable, Sendable {
    case available
    case unavailable(CKAccountStatus)
    case failed
}

struct CloudKitAccountAvailabilityGate: Sendable {
    typealias StatusProvider = @Sendable (String) async -> CloudKitAccountAvailability
    private let statusProvider: StatusProvider
    private let deadlineNanoseconds: UInt64
    private let now: @Sendable () -> UInt64
    private let sleep: @Sendable (UInt64) async throws -> Void

    static let defaultDeadlineNanoseconds: UInt64 = 20_000_000_000

    init() {
        self.init(statusProvider: { containerIdentifier in
            do {
                let configuration = CKOperation.Configuration()
                configuration.timeoutIntervalForRequest = 15
                configuration.timeoutIntervalForResource = 20
                let container = CKContainer(identifier: containerIdentifier)
                let status = try await container.configuredWith(
                    configuration: configuration
                ) { configuredContainer in
                    try await configuredContainer.accountStatus()
                }
                return status == .available ? .available : .unavailable(status)
            } catch {
                return .failed
            }
        })
    }

    init(
        statusProvider: @escaping StatusProvider,
        deadlineNanoseconds: UInt64 = Self.defaultDeadlineNanoseconds,
        now: @escaping @Sendable () -> UInt64 = { DispatchTime.now().uptimeNanoseconds },
        sleep: @escaping @Sendable (UInt64) async throws -> Void = {
            try await Task.sleep(nanoseconds: $0)
        }
    ) {
        self.statusProvider = statusProvider
        self.deadlineNanoseconds = deadlineNanoseconds
        self.now = now
        self.sleep = sleep
    }

    func availability(for containerIdentifier: String) async -> CloudKitAccountAvailability {
        guard !Task.isCancelled else { return .failed }
        // Capture the budget before scheduling either task. Use the same
        // settlement rule as worker deadlines, not a second timing protocol.
        let race = BigSyncDeadlineRace<CloudKitAccountAvailability>(
            durationNanoseconds: deadlineNanoseconds, now: now
        )
        guard race.remainingNanoseconds > 0 else { return .failed }
        let providerTask = Task {
            guard !Task.isCancelled else {
                await race.resolve(.cancelled)
                return
            }
            guard race.remainingNanoseconds > 0 else {
                await race.resolve(.timedOut)
                return
            }
            let value = await statusProvider(containerIdentifier)
            await race.resolve(.completed(value))
        }
        let deadlineTask = Task.detached { [race, sleep] in
            await race.waitUntilDeadline(sleep: sleep)
        }
        let outcome = await withTaskCancellationHandler {
            await race.value()
        } onCancel: {
            // Cancel only this logical provider, without joining a possibly
            // noncooperative Apple request or disturbing another caller.
            providerTask.cancel()
            Task { await race.resolve(.cancelled) }
        }
        providerTask.cancel()
        deadlineTask.cancel()
        // Settlement may precede cancellation while delivery is still queued.
        guard !Task.isCancelled else { return .failed }
        switch outcome {
        case .completed(let value): return value
        case .timedOut, .cancelled: return .failed
        }
    }
}
