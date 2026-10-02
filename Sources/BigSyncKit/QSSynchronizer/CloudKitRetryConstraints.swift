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

    private enum SizeLimitVisit {
        case visiting
        case complete(height: Int?)
    }

    private static func isSizeLimitFailureTree(_ error: Error) -> Bool {
        // Retain the NSError whose address is memoized. Bridging a Swift Error
        // may create a temporary wrapper; a bare ObjectIdentifier set could
        // otherwise confuse a later wrapper reusing a released address.
        var memo = [ObjectIdentifier: (error: NSError, visit: SizeLimitVisit)]()
        func height(of error: NSError, depth: Int) -> Int? {
            guard depth < 32 else { return nil }
            let id = ObjectIdentifier(error)
            if let entry = memo[id] {
                switch entry.visit {
                case .visiting:
                    return nil // A cycle is not proof of a pure size failure.
                case .complete(let height):
                    // Shared DAG nodes are safe to reuse, but a longer path to
                    // the same node must still obey the depth ceiling.
                    guard let height, depth + height <= 32 else { return nil }
                    return height
                }
            }
            memo[id] = (error, .visiting)
            guard let cloudError = error as? CKError else {
                memo[id] = (error, .complete(height: nil))
                return nil
            }
            var children = [Error]()
            if let underlying = error.userInfo[NSUnderlyingErrorKey] as? Error {
                children.append(underlying)
            }
            switch cloudError.code {
            case .limitExceeded, .batchRequestFailed:
                break
            case .partialFailure:
                guard let partial = error.userInfo[CKPartialErrorsByItemIDKey]
                    as? [AnyHashable: Error], !partial.isEmpty else {
                    memo[id] = (error, .complete(height: nil))
                    return nil
                }
                children.append(contentsOf: partial.values)
            default:
                memo[id] = (error, .complete(height: nil))
                return nil
            }
            var maximumChildHeight = 0
            for child in children {
                guard let childHeight = height(of: child as NSError, depth: depth + 1) else {
                    memo[id] = (error, .complete(height: nil))
                    return nil
                }
                maximumChildHeight = max(maximumChildHeight, childHeight)
            }
            let result = maximumChildHeight + 1
            memo[id] = (error, .complete(height: result))
            return result
        }
        return height(of: error as NSError, depth: 0) != nil
    }
}

func cloudKitErrors(in error: Error, depth: Int = 0) -> [CKError] {
    guard depth < 32 else { return [] }
    // Breadth-first visitation finds each identity at its shallowest depth,
    // avoiding both repeated DAG fanout and a deep first path hiding evidence
    // that is also reachable by a shorter path. Keep identity objects alive.
    var visited = [ObjectIdentifier: NSError]()
    var queue = [(error: error as NSError, depth: depth)]
    var offset = 0
    var errors = [CKError]()
    while offset < queue.count {
        let item = queue[offset]
        offset += 1
        guard item.depth < 32 else { continue }
        let id = ObjectIdentifier(item.error)
        guard visited[id] == nil else { continue }
        visited[id] = item.error
        if let cloudError = item.error as? CKError {
            errors.append(cloudError)
            if cloudError.code == .partialFailure,
               let children = item.error.userInfo[CKPartialErrorsByItemIDKey]
                as? [AnyHashable: Error] {
                for child in children.values {
                    queue.append((child as NSError, item.depth + 1))
                }
            }
        }
        if let underlying = item.error.userInfo[NSUnderlyingErrorKey] as? Error {
            queue.append((underlying as NSError, item.depth + 1))
        }
    }
    return errors
}
