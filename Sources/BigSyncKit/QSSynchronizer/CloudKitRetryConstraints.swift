import CloudKit
import Foundation

/// Independent constraints on one failed CloudKit operation. A recovery action
/// (for example token rebuild or batch splitting) never grants permission to
/// ignore an account stop or a server-directed retry deadline.
struct CloudKitRetryConstraints {
    let codes: Set<CKError.Code>
    let serverMinimum: TimeInterval?
    let containsOnlySizeLimitFailures: Bool

    var blocksAccountOperations: Bool {
        !codes.isDisjoint(with: [.notAuthenticated, .accountTemporarilyUnavailable])
    }

    var requestsTokenRecovery: Bool { codes.contains(.changeTokenExpired) }

    var requiresDeferredRetry: Bool {
        serverMinimum != nil || !codes.isDisjoint(with: [
            .serviceUnavailable, .requestRateLimited, .zoneBusy,
            .networkFailure, .networkUnavailable,
        ])
    }

    init(_ error: Error) {
        let errors = cloudKitErrors(in: error)
        codes = Set(errors.map(\.code))
        serverMinimum = errors.compactMap {
            ($0.userInfo[CKErrorRetryAfterKey] as? NSNumber)?.doubleValue
        }.filter { $0.isFinite && $0 >= 0 }.max()
        containsOnlySizeLimitFailures = codes.contains(.limitExceeded)
            && Self.isSizeLimitFailureTree(error)
    }

    private static func isSizeLimitFailureTree(_ error: Error, depth: Int = 0) -> Bool {
        guard depth < 32, let error = error as? CKError else { return false }
        switch error.code {
        case .limitExceeded, .batchRequestFailed:
            return true
        case .partialFailure:
            guard let children = error.userInfo[CKPartialErrorsByItemIDKey]
                as? [AnyHashable: Error], !children.isEmpty else { return false }
            return children.values.allSatisfy {
                isSizeLimitFailureTree($0, depth: depth + 1)
            }
        default:
            return false
        }
    }
}

func cloudKitErrors(in error: Error, depth: Int = 0) -> [CKError] {
    guard depth < 32, let error = error as? CKError else { return [] }
    guard error.code == .partialFailure,
          let children = error.userInfo[CKPartialErrorsByItemIDKey]
            as? [AnyHashable: Error] else { return [error] }
    return [error] + children.values.flatMap {
        cloudKitErrors(in: $0, depth: depth + 1)
    }
}
