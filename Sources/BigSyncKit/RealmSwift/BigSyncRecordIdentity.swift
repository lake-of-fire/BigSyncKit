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
    static func mapKeysAreUnambiguous<Keys: Sequence>(_ keys: Keys) -> Bool
        where Keys.Element == String {
        var seen = Set<String>()
        return keys.allSatisfy { seen.insert($0).inserted }
    }

    static func realmMapKeysAreUnambiguous(_ value: Any?) -> Bool {
        guard let collection = value as? RLMSwiftCollectionBase,
              let map = collection._rlmCollection as? RLMDictionary<AnyObject, AnyObject> else { return false }
        let rawKeys = map.allKeys
        let keys = rawKeys.compactMap { $0 as? String }
        return keys.count == rawKeys.count && mapKeysAreUnambiguous(keys)
    }

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

    static func mappedValuesEqual<Left: Sequence, Right: Sequence>(
        _ lhs: Left, _ rhs: Right
    ) -> Bool where Left.Element == (key: String, value: String),
                   Right.Element == (key: String, value: String) {
        // Do not first collect Realm entries in a String-keyed dictionary:
        // that would collapse canonically equivalent, byte-distinct keys.
        func identities<Entries: Sequence>(_ entries: Entries) -> [Data: Data]
            where Entries.Element == (key: String, value: String) {
            entries.reduce(into: [:]) { result, entry in
                result[Data(entry.key.utf8)] = Data(entry.value.utf8)
            }
        }
        return identities(lhs) == identities(rhs)
    }
}
