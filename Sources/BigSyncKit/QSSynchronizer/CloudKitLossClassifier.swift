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
        }

        mutating func set(_ disposition: ZoneDisposition, for zoneID: CKRecordZone.ID) {
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
        visit(error, inheritedZoneID: defaultZoneID, into: &classification)
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

    private static func visit(
        _ error: Error,
        inheritedZoneID: CKRecordZone.ID?,
        into classification: inout Classification
    ) {
        let nsError = error as NSError
        guard nsError.domain == CKErrorDomain else { return }

        let itemErrors = partialErrors(in: nsError)
        if nsError.code == CKError.partialFailure.rawValue, !itemErrors.isEmpty {
            for (item, nestedError) in itemErrors {
                var zoneID = inheritedZoneID
                if let recordID = item as? CKRecord.ID {
                    classification.affectedRecordIDs.insert(recordID)
                    zoneID = recordID.zoneID
                } else if let nestedRecordID = (item as? AnyHashable)?.base as? CKRecord.ID {
                    classification.affectedRecordIDs.insert(nestedRecordID)
                    zoneID = nestedRecordID.zoneID
                } else if let itemZoneID = item as? CKRecordZone.ID {
                    zoneID = itemZoneID
                } else if let nestedZoneID = (item as? AnyHashable)?.base as? CKRecordZone.ID {
                    zoneID = nestedZoneID
                }
                visit(nestedError, inheritedZoneID: zoneID, into: &classification)
            }
            return
        }

        guard let code = CKError.Code(rawValue: nsError.code) else { return }
        switch code {
        case .zoneNotFound:
            guard let zoneID = inheritedZoneID else { return }
            if didResetEncryptedDataKey(nsError) {
                classification.set(.encryptedDataReset, for: zoneID)
            } else {
                classification.set(.missing, for: zoneID)
            }
        case .userDeletedZone:
            if let zoneID = inheritedZoneID {
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

    private static func partialErrors(in error: NSError) -> [(Any, Error)] {
        guard let dictionary = error.userInfo[CKPartialErrorsByItemIDKey] as? NSDictionary else {
            return []
        }
        return dictionary.compactMap { key, value in
            guard let nestedError = value as? Error else { return nil }
            return (key, nestedError)
        }
    }

    private static func didResetEncryptedDataKey(_ error: NSError) -> Bool {
        if let value = error.userInfo[CKErrorUserDidResetEncryptedDataKey] as? NSNumber {
            return value.boolValue
        }
        return error.userInfo[CKErrorUserDidResetEncryptedDataKey] as? Bool == true
    }
}
