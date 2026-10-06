import Foundation
import RealmSwift

/// Public, read-only record identity construction for application admission
/// evidence. It uses the exact primary-key representation BigSync uses for
/// tracking and CloudKit record IDs, but never creates tracking state.
public enum BigSyncRecordIdentity {
    public static func recordName(for object: Object) -> String? {
        guard let primaryKey = type(of: object).primaryKey()
                ?? object.objectSchema.primaryKeyProperty?.name else {
            return nil
        }
        let objectID = object[primaryKey]
        let identifier: String
        if let value = objectID as? String {
            identifier = value
        } else if let value = objectID as? CustomStringConvertible {
            identifier = String(describing: value)
        } else {
            return nil
        }
        let recordName = object.objectSchema.className + "." + identifier
        do {
            try RealmSwiftAdapter.validateCloudKitRecordName(recordName)
            return recordName
        } catch {
            return nil
        }
    }
}

/// Transport strings are stored identities, not localized text comparisons.
/// Match the UTF-8 identity already used by record fingerprints; normalizing
/// here can hide a real scalar edit or collapse distinct collection members.
/// This helper does not change storage, wire encoding, or conflict policy.
enum BigSyncStringIdentity {
    static func equal(_ lhs: String, _ rhs: String) -> Bool {
        lhs.utf8.elementsEqual(rhs.utf8)
    }

    static func orderedValuesEqual<Left: Sequence, Right: Sequence>(
        _ lhs: Left, _ rhs: Right
    ) -> Bool where Left.Element == String, Right.Element == String {
        lhs.elementsEqual(rhs, by: equal)
    }

    static func unorderedValuesEqual<Left: Sequence, Right: Sequence>(
        _ lhs: Left, _ rhs: Right
    ) -> Bool where Left.Element == String, Right.Element == String {
        Set(lhs.map { Data($0.utf8) }) == Set(rhs.map { Data($0.utf8) })
    }
}
