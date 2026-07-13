import CloudKit

enum CloudKitAccountAvailability: Equatable, Sendable {
    case available
    case unavailable(CKAccountStatus)
    case failed
}

struct CloudKitAccountAvailabilityGate: Sendable {
    typealias StatusProvider = @Sendable (String) async -> CloudKitAccountAvailability

    private let statusProvider: StatusProvider

    init() {
        statusProvider = { containerIdentifier in
            await Self.liveStatus(for: containerIdentifier)
        }
    }

    init(statusProvider: @escaping StatusProvider) {
        self.statusProvider = statusProvider
    }

    func availability(for containerIdentifier: String) async -> CloudKitAccountAvailability {
        await statusProvider(containerIdentifier)
    }

    private static func liveStatus(for containerIdentifier: String) async -> CloudKitAccountAvailability {
        let container = CKContainer(identifier: containerIdentifier)
        return await withCheckedContinuation { continuation in
            container.accountStatus { status, error in
                if error != nil {
                    continuation.resume(returning: .failed)
                } else if status == .available {
                    continuation.resume(returning: .available)
                } else {
                    continuation.resume(returning: .unavailable(status))
                }
            }
        }
    }
}
