import CloudKit

enum CloudKitAccountAvailability: Equatable, Sendable {
    case available
    case unavailable(CKAccountStatus)
    case failed
}

struct CloudKitAccountAvailabilityGate: Sendable {
    typealias StatusProvider = @Sendable (String) async -> CloudKitAccountAvailability
    typealias AccountStatusProvider = @Sendable (
        String,
        @escaping @Sendable (CKAccountStatus, Error?) -> Void
    ) -> Void

    private let statusProvider: StatusProvider

    init() {
        self.init(accountStatusProvider: { containerIdentifier, completion in
            CKContainer(identifier: containerIdentifier).accountStatus(completionHandler: completion)
        })
    }

    init(statusProvider: @escaping StatusProvider) {
        self.statusProvider = statusProvider
    }

    init(accountStatusProvider: @escaping AccountStatusProvider) {
        statusProvider = { containerIdentifier in
            await Self.liveStatus(
                for: containerIdentifier,
                accountStatusProvider: accountStatusProvider
            )
        }
    }

    func availability(for containerIdentifier: String) async -> CloudKitAccountAvailability {
        await statusProvider(containerIdentifier)
    }

    private static func liveStatus(
        for containerIdentifier: String,
        accountStatusProvider: @escaping AccountStatusProvider
    ) async -> CloudKitAccountAvailability {
        do {
            return try await awaitCancellableCloudKitCallback { completion in
                accountStatusProvider(containerIdentifier) { status, error in
                    if error != nil {
                        completion(.success(.failed))
                    } else if status == .available {
                        completion(.success(.available))
                    } else {
                        completion(.success(.unavailable(status)))
                    }
                }
            }
        } catch is CancellationError {
            return .failed
        } catch {
            return .failed
        }
    }
}
