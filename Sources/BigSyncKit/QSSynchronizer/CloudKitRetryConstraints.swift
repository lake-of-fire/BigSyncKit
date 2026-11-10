import CloudKit
import Foundation

/// Independent constraints on one failed CloudKit operation. A recovery action
/// (for example token rebuild or batch splitting) never grants permission to
/// ignore an account stop or a server-directed retry deadline.
struct CloudKitRetryConstraints {
    let codes: Set<CKError.Code>
    let serverMinimum: TimeInterval?
    /// Even malformed retry metadata cannot authorize an immediate domain request.
    let hasRetryDirective: Bool
    let containsOnlySizeLimitFailures: Bool
    /// False when the bounded scan left a previously unseen cause unexamined.
    /// Absence of a discovered constraint then cannot authorize local repair.
    let isErrorGraphComplete: Bool
    /// The operation's account authority is obsolete, even if this local
    /// failure is nested alongside CloudKit errors with their own constraints.
    let containsAccountChange: Bool

    var blocksAccountOperations: Bool {
        containsAccountChange
            || !codes.isDisjoint(with: [.notAuthenticated, .accountTemporarilyUnavailable])
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
        containsAccountChange = inspection.containsAccountChange
        codes = Set(errors.map(\.code))
        hasRetryDirective = errors.contains { $0.userInfo[CKErrorRetryAfterKey] != nil }
        serverMinimum = errors.compactMap {
            ($0.userInfo[CKErrorRetryAfterKey] as? NSNumber)?.doubleValue
        }.filter { $0.isFinite && $0 >= 0 }.max()
        containsOnlySizeLimitFailures = isErrorGraphComplete && codes.contains(.limitExceeded)
            && Self.isFailureTree(error, allowing: [.limitExceeded, .batchRequestFailed])
    }

    private enum FailureTreeVisit {
        case visiting
        case complete(height: Int?)
    }

    fileprivate static func isFailureTree(
        _ error: Error,
        allowing leafCodes: Set<CKError.Code>,
        allowingDirectMissingRecordDetail: Bool = false
    ) -> Bool {
        // Retain the NSError whose address is memoized. Bridging a Swift Error
        // may create a temporary wrapper; a bare ObjectIdentifier set could
        // otherwise confuse a later wrapper reusing a released address.
        var memo = [ObjectIdentifier: (error: NSError, visit: FailureTreeVisit)]()
        func height(of error: NSError, depth: Int) -> Int? {
            guard depth < 32 else { return nil }
            let id = ObjectIdentifier(error)
            if let entry = memo[id] {
                switch entry.visit {
                case .visiting:
                    return nil // A cycle is not proof of a pure failure tree.
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
            // NSMultipleUnderlyingErrorsKey. A pure failure proof must not
            // discard a local failure or constraint carried by either form.
            let info = error.userInfo
            // Only partialFailure defines an item-error container. A leaf code
            // cannot hide independent failures in an unexpected container,
            // including empty or malformed payloads.
            if cloudError.code != .partialFailure,
               info[CKPartialErrorsByItemIDKey] != nil {
                memo[id] = (error, .complete(height: nil))
                return nil
            }
            let underlying = cloudKitUnderlyingErrors(in: info)
            guard underlying.isComplete else {
                memo[id] = (error, .complete(height: nil))
                return nil
            }
            var children = underlying.errors
            switch cloudError.code {
            case .partialFailure:
                let partial = cloudKitPartialErrors(in: info)
                guard partial.isComplete, !partial.entries.isEmpty else {
                    memo[id] = (error, .complete(height: nil))
                    return nil
                }
                children.append(contentsOf: partial.entries.map { $0.error })
            default:
                guard leafCodes.contains(cloudError.code) else {
                    memo[id] = (error, .complete(height: nil))
                    return nil
                }
            }
            var maximumChildHeight = 0
            for child in children {
                // This exception belongs to the direct public unknownItem edge,
                // never to a memoized node: a shared detail reached through a
                // nested path must still fail the ordinary pure-tree proof.
                if allowingDirectMissingRecordDetail, depth == 0,
                   cloudError.code == .unknownItem,
                   isObservedMissingRecordDetail(child as NSError) {
                    maximumChildHeight = max(maximumChildHeight, 1)
                    continue
                }
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

    private static func isObservedMissingRecordDetail(_ error: NSError) -> Bool {
        // A signed macOS single-record fetch returned public unknownItem with
        // this immediate SDK-private "Record not found" detail. The public
        // result grants absence authority; this undocumented detail alone does
        // not. Reject any additional causes/containers or retry metadata, even
        // malformed or empty payloads, rather than hiding independent failures.
        guard error.domain == "CKInternalErrorDomain", error.code == 2003 else { return false }
        let info = error.userInfo
        return info[NSUnderlyingErrorKey] == nil
            && info[NSMultipleUnderlyingErrorsKey] == nil
            && info[CKPartialErrorsByItemIDKey] == nil
            && info[CKErrorRetryAfterKey] == nil
    }
}

/// Read-only failure admission for domain-owned CloudKit operations.
/// Constrained failures must reach the synchronizer's existing account,
/// recovery and backoff policy without another immediate CloudKit request.
public enum BigSyncCloudKitRetryPolicy {
    public static func allowsImmediateRecordConflictRetry(_ error: Error) -> Bool {
        let constraints = CloudKitRetryConstraints(error)
        return constraints.isErrorGraphComplete
            && !constraints.requiresDeferredRetry
            && !constraints.hasRetryDirective
            && !constraints.blocksAccountOperations
            && !constraints.requestsTokenRecovery
            && constraints.codes.contains(.serverRecordChanged)
            && CloudKitRetryConstraints.isFailureTree(error, allowing: [.serverRecordChanged])
    }

    /// A single requested record may be treated as absent only when its own
    /// top-level unknownItem result has no independent failure or constraint.
    /// A partial batch is not record-bound proof of this record's absence.
    public static func allowsMissingRecordAsAbsence(_ error: Error) -> Bool {
        guard let cloudError = error as? CKError, cloudError.code == .unknownItem else { return false }
        let constraints = CloudKitRetryConstraints(error)
        return constraints.isErrorGraphComplete
            && !constraints.requiresDeferredRetry
            && !constraints.hasRetryDirective
            && !constraints.blocksAccountOperations
            && !constraints.requestsTokenRecovery
            && CloudKitRetryConstraints.isFailureTree(
                error, allowing: [.unknownItem], allowingDirectMissingRecordDetail: true)
    }
}

func cloudKitErrors(in error: Error, depth: Int = 0) -> [CKError] {
    inspectCloudKitErrors(in: error, depth: depth).errors
}

private func inspectCloudKitErrors(
    in error: Error, depth: Int = 0
) -> (errors: [CKError], isComplete: Bool, containsAccountChange: Bool) {
    guard depth < 32 else { return ([], false, false) }
    // Breadth-first visitation finds each identity at its shallowest depth,
    // avoiding both repeated DAG fanout and a deep first path hiding evidence
    // that is also reachable by a shorter path. Keep identity objects alive.
    var visited = [ObjectIdentifier: NSError]()
    var queue = [(error: error as NSError, depth: depth)]
    var offset = 0
    var errors = [CKError]()
    var isComplete = true
    var containsAccountChange = false
    // Use the enum's Foundation representation so both Swift errors and
    // NSError wrappers preserve the same account-stop meaning.
    let accountChange = OneOffRecordZoneResetError.cloudKitAccountChanged as NSError
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
        if item.error.domain == accountChange.domain,
           item.error.code == accountChange.code {
            containsAccountChange = true
        }
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
    return (errors, isComplete, containsAccountChange)
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
