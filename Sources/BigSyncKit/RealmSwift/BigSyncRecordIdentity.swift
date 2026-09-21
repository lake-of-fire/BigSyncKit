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
