import CryptoKit
import Foundation
import Realm
import RealmSwift

/// Local comparison evidence, never an upload journal or CloudKit model. Keep
/// this in the target Realm so an import and its new base commit or roll back
/// together. Only one set of field digests is retained, not payload history.
public final class BigSyncRecordBaseline: Object {
    @Persisted(primaryKey: true) public var recordName = ""
    @Persisted public var namespace = ""
    @Persisted public var revision = ""
    @Persisted public var isComparisonInvalidated = false
    @Persisted public var serverChangeTag: String?
    @Persisted public var schemaSignature = ""
    @Persisted public var fields: Map<String, Data>
}

public extension BigSyncMutationPolicy {
    /// Call before opening every target Realm used by writers or the adapter.
    /// Undeclared records remain indivisible; models may declare independent
    /// fields or an epoch bundle without implementing reconciliation callbacks.
    static func enableRecordRebasing(in configuration: inout Realm.Configuration) {
        precondition(configuration.objectTypes != nil)
        for type in BigSyncLocalRecordEvidence.objectTypes where
            configuration.objectTypes?.contains(where: { $0.className() == type.className() }) != true {
            configuration.objectTypes?.append(type)
        }
    }
}

/// A schema declaration, not an application callback. A lifetime bundle keeps
/// every field together except explicitly independent fields. Newly added
/// fields therefore default to the bundle instead of silently escaping it.
public enum BigSyncRecordRebasePolicy: Sendable, Equatable {
    case disabled
    case atomicRecord
    case independentFields
    case lifetimeBundle(lifetimeField: String, independentFields: Set<String>)
}

public protocol BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { get }
}

public enum BigSyncRecordRebaseError: Error {
    case unsupportedField(String)
    case invalidPolicy
    case missingBaseline(String)
    case inconsistentReceipt(String)
    case invalidLifetime
    case lifetimeOverflow
}

/// Pure three-way selection. Sets/maps/lists are single fields; this is not a
/// collection CRDT. A genuine same-field collision uses the supplied existing
/// record-clock decision. A lifetime change wins over edits in the old lifetime;
/// concurrent lifetime changes use their opaque IDs as a shared deterministic
/// tie-break, independent of unrelated title/difficulty clocks on either row.
enum BigSyncRecordRebasePlanner {
    static func incomingFields(
        base: [String: Data], local: [String: Data], remote: [String: Data],
        policy: BigSyncRecordRebasePolicy,
        preferRemoteOnConflict: Bool,
        localLifetime: String?, remoteLifetime: String?
    ) throws -> Set<String> {
        guard Set(base.keys) == Set(local.keys),
              Set(local.keys) == Set(remote.keys) else {
            throw BigSyncRecordRebaseError.invalidPolicy
        }
        let keys = Set(local.keys)
        var incoming = Set<String>()
        for key in keys {
            if local[key] == base[key]
                || (remote[key] != base[key] && preferRemoteOnConflict) {
                incoming.insert(key)
            }
        }
        switch policy {
        case .disabled:
            return []
        case .atomicRecord:
            return local == base || (remote != base && preferRemoteOnConflict)
                ? keys : []
        case .independentFields:
            return incoming
        case let .lifetimeBundle(lifetimeField, independentFields):
            guard keys.contains(lifetimeField),
                  independentFields.isSubset(of: keys),
                  !independentFields.contains(lifetimeField) else {
                throw BigSyncRecordRebaseError.invalidPolicy
            }
            let bundle = keys.subtracting(independentFields)
            let useRemote: Bool
            if local[lifetimeField] != remote[lifetimeField] {
                if let ordered = try BigSyncLifetimeID.prefersIncoming(
                    local: localLifetime, incoming: remoteLifetime
                ) {
                    useRemote = ordered
                } else if local[lifetimeField] == base[lifetimeField] {
                    useRemote = true
                } else if remote[lifetimeField] == base[lifetimeField] {
                    useRemote = false
                } else {
                    // Comparing IDs is an arbitration rule, not chronology.
                    // It must be the same on all members of a lifetime bundle.
                    useRemote = (remoteLifetime ?? "") > (localLifetime ?? "")
                }
            } else if bundle.allSatisfy({ local[$0] == base[$0] }) {
                useRemote = true
            } else if bundle.allSatisfy({ remote[$0] == base[$0] }) {
                useRemote = false
            } else {
                useRemote = preferRemoteOnConflict
            }
            incoming.subtract(bundle)
            if useRemote { incoming.formUnion(bundle) }
            return incoming
        }
    }
}

/// Fingerprints are made from Realm's decoded representation, on its owning
/// executor. This avoids a second CloudKit decoder and normalizes URL/UUID,
/// unordered sets/maps, nil/empty collections and millisecond Date precision.
/// Relationships are deliberately outside this first capability: their deferred
/// materialization has a separate commit boundary and must not be fingerprinted
/// as though an unresolved target were an authored clear.
enum BigSyncRecordFingerprint {
    static let metadata: Set<String> = [
        "createdAt", "modifiedAt", "explicitlyModifiedAt",
    ]

    static func properties(of object: Object) -> [Property] {
        let skipped = (object as? SyncSkippablePropertiesModel)?
            .skipSyncingProperties() ?? []
        return object.objectSchema.properties.filter {
            $0.name != object.objectSchema.primaryKeyProperty?.name
                && $0.type != .linkingObjects
                && !skipped.contains($0.name)
                && !BigSyncRecordLifecycle.metadataFields(for: object).contains($0.name)
        }
    }

    static func supports(_ object: Object) -> Bool {
        properties(of: object).allSatisfy {
            switch $0.type {
            case .int, .bool, .float, .double, .string, .date, .data, .UUID:
                true
            default:
                false
            }
        }
    }

    static func fields(of object: Object) throws -> [String: Data] {
        try Dictionary(uniqueKeysWithValues: properties(of: object).map { property in
            let value = object[property.name]
            let digest: Data
            if property.isMap {
                let entries = try mapEntries(value, type: property.type)
                digest = frame(entries.sorted { $0.0 < $1.0 }.flatMap {
                    [Data($0.0.utf8), $0.1]
                })
            } else if property.isArray || property.isSet {
                guard let collection = value as? RLMSwiftCollectionBase else {
                    throw BigSyncRecordRebaseError.unsupportedField(property.name)
                }
                var elements = try (0..<collection._rlmCollection.count).map {
                    try scalar(collection._rlmCollection[$0], type: property.type)
                }
                if property.isSet { elements.sort { $0.lexicographicallyPrecedes($1) } }
                digest = frame(elements)
            } else {
                digest = try scalar(value, type: property.type)
            }
            return (property.name, digest)
        })
    }

    private static func frame(_ parts: [Data]) -> Data {
        var hash = SHA256()
        for part in parts {
            var length = UInt64(part.count).bigEndian
            withUnsafeBytes(of: &length) { hash.update(data: Data($0)) }
            hash.update(data: part)
        }
        return Data(hash.finalize())
    }

    private static func scalar(_ value: Any?, type: PropertyType) throws -> Data {
        guard let value, !(value is NSNull) else { return frame([Data([0])]) }
        let bytes: Data
        switch type {
        case .int:
            guard let number = value as? NSNumber else { throw unsupported(type) }
            bytes = Data(String(number.int64Value).utf8)
        case .bool:
            guard let number = value as? NSNumber else { throw unsupported(type) }
            bytes = Data([number.boolValue ? 1 : 0])
        case .float, .double:
            guard let number = value as? NSNumber else { throw unsupported(type) }
            let numberValue = type == .float ? Double(number.floatValue) : number.doubleValue
            guard numberValue.isFinite else { throw unsupported(type) }
            bytes = Data(String((numberValue == 0 ? 0.0 : numberValue).bitPattern).utf8)
        case .string:
            if let url = value as? URL { bytes = Data(url.absoluteString.utf8) }
            else if let text = value as? String { bytes = Data(text.utf8) }
            else { throw unsupported(type) }
        case .date:
            guard let date = value as? Date else { throw unsupported(type) }
            let milliseconds = (date.timeIntervalSinceReferenceDate * 1_000).rounded()
            guard milliseconds.isFinite,
                  let integer = Int64(exactly: milliseconds) else { throw unsupported(type) }
            bytes = Data(String(integer).utf8)
        case .data:
            guard let data = value as? Data else { throw unsupported(type) }
            bytes = data
        case .UUID:
            guard let uuid = value as? UUID else { throw unsupported(type) }
            bytes = Data(uuid.uuidString.lowercased().utf8)
        default:
            throw unsupported(type)
        }
        return frame([Data([1]), bytes])
    }

    private static func unsupported(_ type: PropertyType) -> BigSyncRecordRebaseError {
        .unsupportedField(String(describing: type))
    }

    private static func mapEntries(_ value: Any?, type: PropertyType) throws -> [(String, Data)] {
        func entries<T: RealmCollectionValue>(_ map: Map<String, T>?) throws -> [(String, Data)] {
            guard let map else { throw unsupported(type) }
            return try map.map { ($0.key, try scalar($0.value, type: type)) }
        }
        switch type {
        case .int: return try entries(value as? Map<String, Int>)
        case .bool: return try entries(value as? Map<String, Bool>)
        case .float: return try entries(value as? Map<String, Float>)
        case .double: return try entries(value as? Map<String, Double>)
        case .string: return try entries(value as? Map<String, String>)
        case .date: return try entries(value as? Map<String, Date>)
        case .data: return try entries(value as? Map<String, Data>)
        case .UUID: return try entries(value as? Map<String, UUID>)
        default: throw unsupported(type)
        }
    }
}


struct BigSyncRecordRebaseContext: Sendable, Equatable {
    let namespace: String
    let account: String
    let binding: String

    func validate(in realm: Realm) throws {
        guard let identity = BigSyncMutationTracking.currentJournalIdentity(
            verifyingPendingMutationsFor: [Object](), in: realm
        ), identity.replicaBindingGenerationIdentifier == binding else {
            throw CancellationError()
        }
    }
}

/// Immutable preparation-time evidence. A late receipt may advance this base
/// under a newer local edit, but never over a base installed by a newer import.
struct BigSyncPreparedRecordBase: Sendable {
    let context: BigSyncRecordRebaseContext
    let revision: String?
    let fields: [String: Data]
    let schemaSignature: String

    init(context: BigSyncRecordRebaseContext, revision: String?, fields: [String: Data],
         schemaSignature: String = "") {
        self.context = context
        self.revision = revision
        self.fields = fields
        self.schemaSignature = schemaSignature
    }
}

extension BigSyncRecordBaseline {
    var fieldDigests: [String: Data] {
        Dictionary(uniqueKeysWithValues: fields.map { ($0.key, $0.value) })
    }

    static func isEnabled(in realm: Realm) -> Bool {
        isEnabled(in: realm.configuration)
            && realm.schema.objectSchema.contains { $0.className == className() }
    }

    static func isEnabled(in configuration: Realm.Configuration) -> Bool {
        // Merely linking BigSyncKit can make the table visible to Realm's
        // automatic schema discovery. That is not consent to change merging.
        configuration.objectTypes?.contains { $0.className() == className() } == true
    }

    @discardableResult
    static func install(recordName: String, namespace: String,
                        fields: [String: Data], serverChangeTag: String? = nil,
                        schemaSignature: String = "", in realm: Realm) -> Bool {
        precondition(realm.isInWriteTransaction)
        let existing = realm.object(ofType: Self.self, forPrimaryKey: recordName)
        if existing?.isComparisonInvalidated == false, existing?.namespace == namespace,
           existing?.fieldDigests == fields, existing?.serverChangeTag == serverChangeTag,
           existing?.schemaSignature == schemaSignature { return false }
        let row = existing ?? Self()
        if existing == nil { row.recordName = recordName }
        row.namespace = namespace
        row.schemaSignature = schemaSignature
        row.isComparisonInvalidated = false
        row.serverChangeTag = serverChangeTag
        row.revision = UUID().uuidString
        row.fields.removeAll()
        for (name, digest) in fields { row.fields[name] = digest }
        realm.add(row, update: .modified)
        return true
    }

    static func invalidate(recordName: String, in realm: Realm) {
        precondition(realm.isInWriteTransaction)
        guard isEnabled(in: realm) else { return }
        let existing = realm.object(ofType: Self.self, forPrimaryKey: recordName)
        // Keep a revision even when the first upload has not established a base.
        // Removing the row would turn delete/resurrection back into nil and let
        // an older nil-based receipt install an ancestor from the previous life.
        let row = existing ?? Self()
        if existing == nil { row.recordName = recordName }
        row.isComparisonInvalidated = true
        row.serverChangeTag = nil
        row.revision = UUID().uuidString
        row.fields.removeAll()
        realm.add(row, update: .modified)
    }
}
