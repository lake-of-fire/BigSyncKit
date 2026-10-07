import CloudKit
import Foundation

/// Independent constraints on one failed CloudKit operation. A recovery action
/// (for example token rebuild or batch splitting) never grants permission to
/// ignore an account stop or a server-directed retry deadline.
struct CloudKitRetryConstraints {
    let codes: Set<CKError.Code>
    let serverMinimum: TimeInterval?
    let containsOnlySizeLimitFailures: Bool
    /// False when the bounded scan left a previously unseen cause unexamined.
    /// Absence of a discovered constraint then cannot authorize local repair.
    let isErrorGraphComplete: Bool

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
        let inspection = inspectCloudKitErrors(in: error)
        let errors = inspection.errors
        isErrorGraphComplete = inspection.isComplete
        codes = Set(errors.map(\.code))
        serverMinimum = errors.compactMap {
            ($0.userInfo[CKErrorRetryAfterKey] as? NSNumber)?.doubleValue
        }.filter { $0.isFinite && $0 >= 0 }.max()
        containsOnlySizeLimitFailures = isErrorGraphComplete && codes.contains(.limitExceeded)
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
            // Foundation combines NSUnderlyingErrorKey and
            // NSMultipleUnderlyingErrorsKey. A size-only proof must not
            // discard a local failure or constraint carried by either form.
            let info = error.userInfo
            let underlying = cloudKitUnderlyingErrors(in: info)
            guard underlying.isComplete else {
                memo[id] = (error, .complete(height: nil))
                return nil
            }
            var children = underlying.errors
            switch cloudError.code {
            case .limitExceeded, .batchRequestFailed:
                break
            case .partialFailure:
                let partial = cloudKitPartialErrors(in: info)
                guard partial.isComplete, !partial.entries.isEmpty else {
                    memo[id] = (error, .complete(height: nil))
                    return nil
                }
                children.append(contentsOf: partial.entries.map { $0.error })
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
    inspectCloudKitErrors(in: error, depth: depth).errors
}

private func inspectCloudKitErrors(
    in error: Error, depth: Int = 0
) -> (errors: [CKError], isComplete: Bool) {
    guard depth < 32 else { return ([], false) }
    // Breadth-first visitation finds each identity at its shallowest depth,
    // avoiding both repeated DAG fanout and a deep first path hiding evidence
    // that is also reachable by a shorter path. Keep identity objects alive.
    var visited = [ObjectIdentifier: NSError]()
    var queue = [(error: error as NSError, depth: depth)]
    var offset = 0
    var errors = [CKError]()
    var isComplete = true
    while offset < queue.count {
        let item = queue[offset]
        offset += 1
        let id = ObjectIdentifier(item.error)
        guard visited[id] == nil else { continue }
        // A deep alias already inspected on a shallower path is not missing
        // evidence. Only unseen nodes beyond the ceiling make the scan partial.
        guard item.depth < 32 else {
            isComplete = false
            continue
        }
        visited[id] = item.error
        let info = item.error.userInfo
        if let cloudError = item.error as? CKError {
            errors.append(cloudError)
            if cloudError.code == .partialFailure {
                let partial = cloudKitPartialErrors(in: info)
                isComplete = isComplete && partial.isComplete
                for child in partial.entries {
                    queue.append((child.error as NSError, item.depth + 1))
                }
            }
        }
        // Match the same Foundation edge set used by size-only validation.
        // Existing identity/depth guards also bound aggregate cycles and DAGs.
        let underlyingCauses = cloudKitUnderlyingErrors(in: info)
        isComplete = isComplete && underlyingCauses.isComplete
        for underlying in underlyingCauses.errors {
            queue.append((underlying as NSError, item.depth + 1))
        }
    }
    return (errors, isComplete)
}

/// Read containers entry by entry: a malformed sibling must not erase a known
/// stop/deadline, and valid children do not prove the unseen evidence safe.
func cloudKitUnderlyingErrors(in info: [String: Any])
    -> (errors: [Error], isComplete: Bool) {
    var errors = [Error]()
    var isComplete = true
    if let value = info[NSUnderlyingErrorKey] {
        if let error = value as? Error { errors.append(error) }
        else { isComplete = false }
    }
    if let value = info[NSMultipleUnderlyingErrorsKey] {
        if let values = value as? [Any] {
            for value in values {
                if let error = value as? Error { errors.append(error) }
                else { isComplete = false }
            }
        } else { isComplete = false }
    }
    return (errors, isComplete)
}

/// Keep item scope alongside each valid cause. A malformed dictionary or value
/// is incomplete knowledge, not an empty authoritative set of failures.
func cloudKitPartialErrors(in info: [String: Any])
    -> (entries: [(key: Any, error: Error)], isComplete: Bool) {
    guard let dictionary = info[CKPartialErrorsByItemIDKey] as? NSDictionary,
          dictionary.count > 0 else { return ([], false) }
    var entries = [(key: Any, error: Error)]()
    var isComplete = true
    for (key, value) in dictionary {
        if let error = value as? Error { entries.append((key, error)) }
        else { isComplete = false }
    }
    return (entries, isComplete)
}
