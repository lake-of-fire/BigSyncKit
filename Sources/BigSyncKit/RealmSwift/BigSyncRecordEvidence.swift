import CloudKit
import CryptoKit
import Foundation
import Realm
import RealmSwift

/// At most one unresolved submitted candidate per record and transport
/// namespace. This is evidence about the in-flight value, not a second outbox:
/// BigSyncPendingMutation still owns the latest authored value and generation.
public final class BigSyncRecordSubmission: Object {
    @Persisted(primaryKey: true) public var id = ""
    @Persisted(indexed: true) public var recordName = ""
    @Persisted public var namespace = ""
    @Persisted public var schemaSignature = ""
    @Persisted public var generation = ""
    @Persisted public var comparisonRevision: String?
    @Persisted public var fields: Map<String, Data>
    @Persisted public var payload = Data()
}

/// Explicit, local-only preservation of incomparable values. Retention is
/// bounded by admission/backpressure, never silent eviction of unresolved work.
public final class BigSyncRecordConflict: Object {
    @Persisted(primaryKey: true) public var id = ""
    @Persisted(indexed: true) public var recordName = ""
    @Persisted public var entityType = ""
    @Persisted public var namespace = ""
    @Persisted public var schemaSignature = ""
    @Persisted public var generation = ""
    @Persisted public var comparisonRevision: String?
    @Persisted public var reason = ""
    @Persisted public var localPayload = Data()
    @Persisted public var incomingPayload = Data()
    @Persisted public var createdAt = Date()
    @Persisted(indexed: true) public var isResolved = false
}

public struct BigSyncRecordConflictSnapshot: Sendable, Identifiable {
    public let id: String
    public let recordName: String
    public let entityType: String
    public let reason: String
    public let generation: String
    public let createdAt: Date
    public let localText: String?
    public let incomingText: String?
}

public enum BigSyncRecordConflictChoice: Sendable, Equatable {
    case keepLocal
    case useIncoming
}

public struct BigSyncUnresolvedRecordConflicts: Error, LocalizedError, Sendable {
    public let recordNames: [String]
    public var errorDescription: String? {
        "Sync preserved conflicting changes for \(recordNames.count) record(s). Resolve them before completing sync."
    }
}

/// The limits bound retained payload memory and disk work. Hitting a limit
/// aborts before discarding or overwriting target data. They are not an expiry
/// policy. Resolved conflict copies are retained until explicitly discarded.
enum BigSyncRecordEvidenceLimits {
    static let maximumRecordBytes = 64 * 1024 * 1024
    static let maximumRealmBytes = 256 * 1024 * 1024
    static let maximumConflictCount = 512

    static func admit(additionalBytes: Int, in realm: Realm) throws {
        var bytes = 0
        for row in realm.objects(BigSyncRecordSubmission.self) {
            bytes += row.payload.count
        }
        for row in realm.objects(BigSyncRecordConflict.self) {
            bytes += row.localPayload.count + row.incomingPayload.count
        }
        guard additionalBytes <= maximumRealmBytes - bytes else {
            throw BigSyncRecordContractError.evidenceCapacityExceeded
        }
    }
}

/// NSKeyedArchiver preserves the real CloudKit system fields. Asset contents
/// are embedded separately rather than retaining temporary paths. The archive
/// never treats an absent server lookup as evidence that a write never ran.
struct BigSyncRecordPayload: Codable {
    let recordArchive: Data
    let assets: [String: Data]

    static func encode(_ record: CKRecord) throws -> Data {
        guard let snapshot = record.copy() as? CKRecord else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(record.recordID.recordName)
        }
        var assets = [String: Data]()
        var size = 0
        for key in snapshot.allKeys() {
            if let asset = snapshot[key] as? CKAsset {
                guard let url = asset.fileURL else {
                    throw BigSyncRecordRebaseError.unsupportedField(key)
                }
                let resource = try url.resourceValues(forKeys: [.fileSizeKey])
                guard let count = resource.fileSize,
                      count <= BigSyncRecordEvidenceLimits.maximumRecordBytes - size else {
                    throw BigSyncRecordContractError.evidenceCapacityExceeded
                }
                let data = try Data(contentsOf: url)
                size += data.count
                assets[key] = data
                snapshot[key] = nil
            }
        }
        let archive = try NSKeyedArchiver.archivedData(withRootObject: snapshot,
                                                      requiringSecureCoding: true)
        let encoder = PropertyListEncoder()
        encoder.outputFormat = .binary
        let encoded = try encoder.encode(Self(recordArchive: archive, assets: assets))
        guard encoded.count <= BigSyncRecordEvidenceLimits.maximumRecordBytes else {
            throw BigSyncRecordContractError.evidenceCapacityExceeded
        }
        return encoded
    }

    static func decode(_ data: Data, assetManager: PersistentAssetManager? = nil) throws -> CKRecord {
        guard data.count <= BigSyncRecordEvidenceLimits.maximumRecordBytes else {
            throw BigSyncRecordContractError.evidenceCapacityExceeded
        }
        let payload = try PropertyListDecoder().decode(Self.self, from: data)
        guard let record = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self,
                                                                from: payload.recordArchive) else {
            throw CocoaError(.coderReadCorrupt)
        }
        for (key, bytes) in payload.assets {
            if let assetManager {
                record[key] = CKAsset(fileURL: try assetManager.store(data: bytes,
                    forRecordID: record.recordID.recordName, propertyName: key))
            } else {
                // The normal inbound decoder accepts inline Data for Data
                // properties too. Read-only conflict previews need no files.
                record[key] = bytes as CKRecordValue
            }
        }
        return record
    }

    static func systemFields(of record: CKRecord) throws -> Data {
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        record.encodeSystemFields(with: archiver)
        archiver.finishEncoding()
        return archiver.encodedData
    }

    static func record(systemFields: Data) throws -> CKRecord {
        let decoder = try NSKeyedUnarchiver(forReadingFrom: systemFields)
        decoder.requiresSecureCoding = true
        defer { decoder.finishDecoding() }
        guard let record = CKRecord(coder: decoder) else { throw CocoaError(.coderReadCorrupt) }
        return record
    }

    static func identity(_ parts: [String]) -> String {
        var hash = SHA256()
        for part in parts {
            let bytes = Data(part.utf8)
            var size = UInt64(bytes.count).bigEndian
            withUnsafeBytes(of: &size) { hash.update(data: Data($0)) }
            hash.update(data: bytes)
        }
        return hash.finalize().map { String(format: "%02x", $0) }.joined()
    }
}

extension BigSyncRecordPayload {
    /// The adopted scalar contract uses this same encoder for uploads and
    /// conflict preservation. That keeps recovery from inventing a second
    /// interpretation of Realm collections, UUIDs or encoded integer fields.
    static func record(
        from object: Object, recordID: CKRecord.ID, template: CKRecord? = nil,
        assetManager: PersistentAssetManager? = nil
    ) throws -> CKRecord {
        let record = template ?? CKRecord(recordType: object.objectSchema.className, recordID: recordID)
        guard record.recordID == recordID, record.recordType == object.objectSchema.className else {
            throw BigSyncRecordRebaseError.inconsistentReceipt(recordID.recordName)
        }
        let skipped = (object as? SyncSkippablePropertiesModel)?.skipSyncingProperties() ?? []
        let stringIntegers = (type(of: object) as? BigSyncStringEncodedIntegerModel.Type)?
            .bigSyncStringEncodedIntegerPropertyNames ?? []
        for property in object.objectSchema.properties {
            let key = property.name
            guard key != object.objectSchema.primaryKeyProperty?.name,
                  property.type != .linkingObjects else { continue }
            guard !skipped.contains(key) else { record[key] = nil; continue }
            let value = object[key]
            if property.isMap {
                let map: [String: Any]
                func dictionary<T: RealmCollectionValue>(_ value: Any?, _: T.Type) throws -> [String: Any] {
                    guard let typed = value as? Map<String, T> else {
                        throw BigSyncRecordRebaseError.unsupportedField(key)
                    }
                    return typed.reduce(into: [:]) { result, entry in
                        if let uuid = entry.value as? UUID { result[entry.key] = uuid.uuidString }
                        else { result[entry.key] = entry.value }
                    }
                }
                switch property.type {
                case .int: map = try dictionary(value, Int.self)
                case .bool: map = try dictionary(value, Bool.self)
                case .float: map = try dictionary(value, Float.self)
                case .double: map = try dictionary(value, Double.self)
                case .string: map = try dictionary(value, String.self)
                case .data: map = try dictionary(value, Data.self)
                case .date: map = try dictionary(value, Date.self)
                case .UUID: map = try dictionary(value, UUID.self)
                default: throw BigSyncRecordRebaseError.unsupportedField(key)
                }
                record[key] = map.isEmpty ? nil : try PropertyListSerialization.data(
                    fromPropertyList: map, format: .binary, options: 0) as CKRecordValue
            } else if property.isArray || property.isSet {
                guard let collection = value as? RLMSwiftCollectionBase else {
                    throw BigSyncRecordRebaseError.unsupportedField(key)
                }
                var values = [CKRecordValue]()
                for index in 0..<collection._rlmCollection.count {
                    let entry = collection._rlmCollection[index]
                    if let uuid = entry as? UUID { values.append(uuid.uuidString as CKRecordValue) }
                    else if let url = entry as? URL { values.append(url.absoluteString as CKRecordValue) }
                    else if let converted = entry as? CKRecordValue { values.append(converted) }
                    else { throw BigSyncRecordRebaseError.unsupportedField(key) }
                }
                record[key] = values.isEmpty ? nil : values as CKRecordValue
            } else if value == nil || value is NSNull {
                record[key] = nil
            } else if let uuid = value as? UUID {
                record[key] = uuid.uuidString as CKRecordValue
            } else if let url = value as? URL {
                record[key] = url.absoluteString as CKRecordValue
            } else if property.type == .int, let number = value as? NSNumber {
                record[key] = stringIntegers.contains(key)
                    ? BigSyncStringEncodedIntegerCodec.encode(number.int64Value) as CKRecordValue
                    : NSNumber(value: number.int64Value)
            } else if property.type == .data, let bytes = value as? Data, let assetManager {
                record[key] = CKAsset(fileURL: try assetManager.store(data: bytes,
                    forRecordID: recordID.recordName, propertyName: key))
            } else if let converted = value as? CKRecordValue {
                record[key] = converted
            } else {
                throw BigSyncRecordRebaseError.unsupportedField(key)
            }
        }
        return record
    }
}

struct BigSyncRecordEvidenceStore {
    let context: BigSyncRecordRebaseContext
    let realm: Realm

    func submission(recordName: String) -> BigSyncRecordSubmission? {
        guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordSubmission.className() }) else {
            return nil
        }
        return realm.object(ofType: BigSyncRecordSubmission.self,
            forPrimaryKey: BigSyncRecordPayload.identity([context.namespace, recordName]))
    }

    func stage(record: CKRecord, generation: String, proof: BigSyncPreparedRecordBase) throws {
        precondition(realm.isInWriteTransaction)
        try context.validate(in: realm)
        guard submission(recordName: record.recordID.recordName) == nil else { return }
        let payload = try BigSyncRecordPayload.encode(record)
        try BigSyncRecordEvidenceLimits.admit(additionalBytes: payload.count, in: realm)
        let row = BigSyncRecordSubmission()
        row.id = BigSyncRecordPayload.identity([context.namespace, record.recordID.recordName])
        row.recordName = record.recordID.recordName
        row.namespace = context.namespace
        row.schemaSignature = proof.schemaSignature
        row.generation = generation
        row.comparisonRevision = proof.revision
        row.payload = payload
        for (key, value) in proof.fields { row.fields[key] = value }
        realm.add(row)
    }

    func isBlocked(recordName: String) -> Bool {
        guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordConflict.className() }) else {
            return false
        }
        return !realm.objects(BigSyncRecordConflict.self).where {
            $0.namespace == context.namespace && $0.recordName == recordName && !$0.isResolved
        }.isEmpty
    }

    func preserveConflict(
        record: CKRecord, object: Object, generation: String, revision: String?,
        signature: String, incomingFields: [String: Data],
        reason: String = "missing-accepted-baseline"
    ) throws -> String {
        precondition(realm.isInWriteTransaction)
        try context.validate(in: realm)
        let localRecord = try BigSyncRecordPayload.record(from: object, recordID: record.recordID)
        let local = try BigSyncRecordPayload.encode(localRecord)
        let remote = try BigSyncRecordPayload.encode(record)
        let localFields = try BigSyncRecordFingerprint.fields(of: object)
        var identity = ["bigsync-conflict-v1", context.namespace, record.recordID.recordName,
                        signature, generation, record.recordChangeTag ?? "", reason]
        identity += localFields.keys.sorted().map { $0 + ":" + localFields[$0]!.base64EncodedString() }
        // A changed, tag-less injected/server representation must not overwrite
        // an earlier preserved candidate with the same local generation.
        identity += incomingFields.keys.sorted().map { "incoming:" + $0 + ":" + incomingFields[$0]!.base64EncodedString() }
        let id = BigSyncRecordPayload.identity(identity)
        if realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: id) != nil { return id }
        guard realm.objects(BigSyncRecordConflict.self).count < BigSyncRecordEvidenceLimits.maximumConflictCount else {
            throw BigSyncRecordContractError.evidenceCapacityExceeded
        }
        try BigSyncRecordEvidenceLimits.admit(additionalBytes: local.count + remote.count, in: realm)
        let row = BigSyncRecordConflict()
        row.id = id
        row.recordName = record.recordID.recordName
        row.entityType = record.recordType
        row.namespace = context.namespace
        row.schemaSignature = signature
        row.generation = generation
        row.comparisonRevision = revision
        row.reason = reason
        row.localPayload = local
        row.incomingPayload = remote
        realm.add(row)
        return id
    }

    func preserveNoteCopy(
        losingObject: Object, record: CKRecord, fieldNames: Set<String>
    ) throws {
        precondition(realm.isInWriteTransaction)
        let fields = try BigSyncRecordFingerprint.fields(of: losingObject)
        let parts = ["bigsync-note-conflict-copy-v1", context.preservationNamespace,
                     record.recordID.zoneID.ownerName, record.recordID.zoneID.zoneName,
                     record.recordID.recordName] + fieldNames.sorted().map {
            $0 + ":" + (fields[$0]?.base64EncodedString() ?? "")
        }
        let hash = BigSyncRecordPayload.identity(parts)
        var bytes = stride(from: 0, to: 32, by: 2).map { offset -> UInt8 in
            let start = hash.index(hash.startIndex, offsetBy: offset)
            return UInt8(hash[start..<hash.index(start, offsetBy: 2)], radix: 16)!
        }
        bytes[6] = (bytes[6] & 0x0f) | 0x80 // UUID v8: application-defined SHA-256 identity
        bytes[8] = (bytes[8] & 0x3f) | 0x80
        let hex = bytes.map { String(format: "%02x", $0) }.joined()
        let cuts = [0, 8, 12, 16, 20, 32]
        let uuid = UUID(uuidString: zip(cuts, cuts.dropFirst()).map { a, b in
            String(hex[hex.index(hex.startIndex, offsetBy: a)..<hex.index(hex.startIndex, offsetBy: b)])
        }.joined(separator: "-"))!
        let type = type(of: losingObject)
        guard let key = losingObject.objectSchema.primaryKeyProperty, key.type == .UUID else {
            throw BigSyncRecordContractError.invalidDeclaration(type.className())
        }
        if realm.object(ofType: type, forPrimaryKey: uuid) != nil {
            // The deterministic identity is the preservation receipt. The user
            // may have edited or deleted that ordinary recovery note since it
            // was created; redelivery cannot overwrite, revive or duplicate it.
            return
        }
        var values = [String: Any]()
        for property in losingObject.objectSchema.properties where property.type != .linkingObjects {
            values[property.name] = losingObject[property.name] ?? NSNull()
        }
        values[key.name] = uuid
        values["isDeleted"] = false
        let copy = realm.create(type, value: values)
        (copy as? ChangeMetadataRecordable)?.journalCurrentValuePreservingChangeMetadata(at: Date())
    }
}

struct BigSyncRecordConflictValidationFailure: BigSyncInboundSemanticValidationFailure {
    var bigSyncValidationCode: String { "record-comparison-unresolved" }
}

extension BigSyncRecordContractError: BigSyncInboundSemanticValidationFailure {
    public var bigSyncValidationCode: String {
        switch self {
        case .unexpectedPhysicalDeletion: return "retained-record-physically-deleted"
        default: return "record-contract-invalid"
        }
    }
}
