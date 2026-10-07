import CloudKit
import Foundation

/// Classifies CloudKit failures which describe loss of a custom zone or its
/// encryption material.  CloudKit nests per-item failures below
/// `CKPartialErrorsByItemIDKey`, so callers must not classify only the outer
/// error when deciding whether a zone is recoverable or terminal.
///
/// This is deliberately transport-neutral.  Synchronization code supplies a
/// zone when an operation is intrinsically scoped to one, while mutation
/// failures retain the record/zone IDs CloudKit returned.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
enum CloudKitLossClassifier {
    enum ZoneDisposition: Equatable {
        /// The zone must not be recreated implicitly.  This includes a user
        /// deletion, purge, and an unrecognised database-history deletion.
        case terminal(CloudKitZoneDeletionKind)
        /// CloudKit's encrypted-data key was reset; recreate only through the
        /// fenced encrypted-reset recovery path.
        case encryptedDataReset
        /// A zone which was never established may be created during setup.
        case missing
    }

    struct Classification {
        var zoneDispositions = [CKRecordZone.ID: ZoneDisposition]()
        var affectedRecordIDs = Set<CKRecord.ID>()
        var transientCodes = Set<CKError.Code>()
        var accountCodes = Set<CKError.Code>()
        /// Partial observation can preserve a known terminal loss, but cannot
        /// prove that recreating or rebuilding a zone is safe.
        private(set) var isErrorGraphComplete = true

        var hasEncryptedDataReset: Bool {
            zoneDispositions.values.contains(.encryptedDataReset)
        }

        var isAccountTemporarilyUnavailable: Bool {
            accountCodes.contains(.accountTemporarilyUnavailable)
        }

        mutating func merge(_ other: Classification) {
            affectedRecordIDs.formUnion(other.affectedRecordIDs)
            transientCodes.formUnion(other.transientCodes)
            accountCodes.formUnion(other.accountCodes)
            for (zoneID, disposition) in other.zoneDispositions {
                set(disposition, for: zoneID)
            }
            if !other.isErrorGraphComplete { markIncomplete() }
        }

        mutating func set(_ disposition: ZoneDisposition, for zoneID: CKRecordZone.ID) {
            if !isErrorGraphComplete {
                guard case .terminal = disposition else { return }
            }
            guard let existing = zoneDispositions[zoneID] else {
                zoneDispositions[zoneID] = disposition
                return
            }
            // CloudKit database-history order is not a safety guarantee.  A
            // terminal deletion always wins over an encrypted reset or a
            // normal missing-zone error for the same zone.
            if priority(of: disposition) > priority(of: existing) {
                zoneDispositions[zoneID] = disposition
            }
        }

        mutating func markIncomplete() {
            guard isErrorGraphComplete else { return }
            isErrorGraphComplete = false
            // Missing/reset observations may have an unseen terminal sibling.
            // Do not turn that uncertainty into permission to recreate a zone.
            zoneDispositions = zoneDispositions.filter {
                if case .terminal = $0.value { return true }
                return false
            }
        }

        private func priority(of disposition: ZoneDisposition) -> Int {
            switch disposition {
            case .terminal(.purged): 5
            case .terminal(.deleted): 4
            case .terminal(.unknown): 3
            case .terminal(.encryptedDataReset): 2
            case .encryptedDataReset: 2
            case .missing: 1
            }
        }
    }

    /// Classifies an operation error.  Pass `defaultZoneID` for a zone-scoped
    /// fetch/setup operation whose CloudKit error carries no item key.
    static func classify(
        error: Error,
        defaultZoneID: CKRecordZone.ID? = nil
    ) -> Classification {
        var classification = Classification()
        // A shared NSError may be attached to two different record zones. Its
        // identity alone is not the identity of a classification observation.
        var visited = Set<ErrorScope>()
        var queue: [(error: NSError, zone: CKRecordZone.ID?, depth: Int)] = [
            (error as NSError, defaultZoneID, 0)
        ]
        var offset = 0
        while offset < queue.count {
            let item = queue[offset]
            offset += 1
            let scope = ErrorScope(error: ObjectIdentifier(item.error), zone: item.zone)
            guard !visited.contains(scope) else { continue }
            // The queue retains each NSError, including bridged Swift errors.
            // Check aliases first: an already inspected shallow observation
            // remains complete when another path reaches it at the limit.
            guard item.depth < 32 else {
                classification.markIncomplete()
                continue
            }
            visited.insert(scope)
            let info = item.error.userInfo
            if item.error.domain == CKErrorDomain {
                classifyCode(item.error.code, userInfo: info, zoneID: item.zone,
                             into: &classification)
                if item.error.code == CKError.partialFailure.rawValue {
                    for (key, nested) in partialErrors(in: info) {
                        var zone = item.zone
                        let identity = (key as? AnyHashable)?.base ?? key
                        if let recordID = identity as? CKRecord.ID {
                            classification.affectedRecordIDs.insert(recordID)
                            zone = recordID.zoneID
                        } else if let zoneID = identity as? CKRecordZone.ID {
                            zone = zoneID
                        }
                        queue.append((nested as NSError, zone, item.depth + 1))
                    }
                }
            }
            // Underlying causes are not limited to CloudKit-domain wrappers,
            // and partial-item errors do not replace a wrapper's other causes.
            for nested in cloudKitUnderlyingErrors(in: info) {
                queue.append((nested as NSError, item.zone, item.depth + 1))
            }
        }
        return classification
    }

    /// Classifies database-history deletions and applies the same conservative
    /// precedence used for nested operation errors.
    static func classify(deletions: [CloudKitZoneDeletion]) -> Classification {
        var classification = Classification()
        for deletion in deletions {
            switch deletion.kind {
            case .encryptedDataReset:
                classification.set(.encryptedDataReset, for: deletion.zoneID)
            case .deleted, .purged, .unknown:
                classification.set(.terminal(deletion.kind), for: deletion.zoneID)
            }
        }
        return classification
    }

    private struct ErrorScope: Hashable {
        let error: ObjectIdentifier
        let zone: CKRecordZone.ID?
    }

    private static func classifyCode(
        _ rawCode: Int,
        userInfo: [String: Any],
        zoneID: CKRecordZone.ID?,
        into classification: inout Classification
    ) {
        guard let code = CKError.Code(rawValue: rawCode) else { return }
        switch code {
        case .zoneNotFound:
            guard let zoneID else { return }
            if didResetEncryptedDataKey(userInfo) {
                classification.set(.encryptedDataReset, for: zoneID)
            } else {
                classification.set(.missing, for: zoneID)
            }
        case .userDeletedZone:
            if let zoneID {
                classification.set(.terminal(.deleted), for: zoneID)
            }
        case .accountTemporarilyUnavailable, .notAuthenticated:
            classification.accountCodes.insert(code)
        case .serviceUnavailable, .requestRateLimited, .zoneBusy,
                .networkUnavailable, .networkFailure:
            classification.transientCodes.insert(code)
        default:
            break
        }
    }

    private static func partialErrors(in userInfo: [String: Any]) -> [(Any, Error)] {
        guard let dictionary = userInfo[CKPartialErrorsByItemIDKey] as? NSDictionary else {
            return []
        }
        return dictionary.compactMap { key, value in
            guard let nestedError = value as? Error else { return nil }
            return (key, nestedError)
        }
    }

    private static func didResetEncryptedDataKey(_ userInfo: [String: Any]) -> Bool {
        if let value = userInfo[CKErrorUserDidResetEncryptedDataKey] as? NSNumber {
            return value.boolValue
        }
        return userInfo[CKErrorUserDidResetEncryptedDataKey] as? Bool == true
    }
}
