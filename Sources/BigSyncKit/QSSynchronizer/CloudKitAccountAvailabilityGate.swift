import CloudKit

enum CloudKitAccountAvailability: Equatable, Sendable {
    case available
    case unavailable(CKAccountStatus)
    case failed
}

private actor CloudKitAccountAvailabilityRace {
    private var resolvedValue: CloudKitAccountAvailability?
    private var continuation:
        CheckedContinuation<CloudKitAccountAvailability, Never>?

    func resolve(_ value: CloudKitAccountAvailability) {
        guard resolvedValue == nil else { return }
        resolvedValue = value
        continuation?.resume(returning: value)
        continuation = nil
    }

    func value() async -> CloudKitAccountAvailability {
        if let resolvedValue { return resolvedValue }
        return await withCheckedContinuation { continuation in
            if let resolvedValue {
                continuation.resume(returning: resolvedValue)
            } else {
                self.continuation = continuation
            }
        }
    }
}

struct CloudKitAccountAvailabilityGate: Sendable {
    typealias StatusProvider = @Sendable (String) async -> CloudKitAccountAvailability
    private let statusProvider: StatusProvider
    private let deadlineNanoseconds: UInt64

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
        deadlineNanoseconds: UInt64 = Self.defaultDeadlineNanoseconds
    ) {
        self.statusProvider = statusProvider
        self.deadlineNanoseconds = deadlineNanoseconds
    }

    func availability(for containerIdentifier: String) async -> CloudKitAccountAvailability {
        let race = CloudKitAccountAvailabilityRace()
        let providerTask = Task {
            let value = await statusProvider(containerIdentifier)
            await race.resolve(value)
        }
        let deadlineTask = Task.detached { [race, deadlineNanoseconds] in
            do {
                try await Task.sleep(nanoseconds: deadlineNanoseconds)
            } catch {
                return
            }
            await race.resolve(.failed)
        }
        let value = await withTaskCancellationHandler {
            await race.value()
        } onCancel: {
            Task { await race.resolve(.failed) }
        }
        providerTask.cancel()
        deadlineTask.cancel()
        return value
    }
}
