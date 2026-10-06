// Explicit test-only domain, adapter and transport collaborators. The complete
// production audit and comparison inspector are copied unchanged by run.sh.
import CloudKit
import Foundation
import RealmSwift

@globalActor public actor BigSyncBackgroundActor { public static let shared = BigSyncBackgroundActor() }
protocol SoftDeletable { var isDeleted: Bool { get } }
protocol BigSyncRecordContractProviding {}
struct BigSyncRecordRebaseContext: Equatable, Sendable {
    let namespace: String
    let account: String
    let binding: String
}
enum RealmSwiftAdapterError: Error { case setupUnavailable }
enum FixtureError: Error { case invalidRecord }
final class AuditNote: Object, SoftDeletable, BigSyncRecordContractProviding {
    var text: String { get { self["text"] as? String ?? "accepted" } set { self["text"] = newValue } }
    var owner: String { get { self["owner"] as? String ?? "account-1" } set { self["owner"] = newValue } }
    var isDeleted: Bool { get { self["isDeleted"] as? Bool ?? false } set { self["isDeleted"] = newValue } }
}
final class AuditOtherNote: Object {}
final class BigSyncPendingMutation: Object {
    override class func primaryKey() -> String? { "recordName" }
    var recordName: String { get { self["recordName"] as? String ?? "" } set { self["recordName"] = newValue } }
    var entityType: String { get { self["entityType"] as? String ?? AuditNote.className() } set { self["entityType"] = newValue } }
    var generation: String { get { self["generation"] as? String ?? "pending-1" } set { self["generation"] = newValue } }
    var replicaBindingGenerationIdentifier: String? { get { self["binding"] as? String } set { self["binding"] = newValue } }
    var accountScopeIdentifier: String? { get { self["account"] as? String } set { self["account"] = newValue } }
}
final class BigSyncRecordBaseline: Object {
    override class func primaryKey() -> String? { "recordName" }
    var recordName: String { get { self["recordName"] as? String ?? "" } set { self["recordName"] = newValue } }
    var namespace: String { get { self["namespace"] as? String ?? "namespace-1" } set { self["namespace"] = newValue } }
    var revision: String { get { self["revision"] as? String ?? "revision-1" } set { self["revision"] = newValue } }
    var schemaSignature: String { get { self["signature"] as? String ?? "signature-1" } set { self["signature"] = newValue } }
    var isComparisonInvalidated: Bool { get { self["invalid"] as? Bool ?? false } set { self["invalid"] = newValue } }
    var serverChangeTag: String? { get { self["tag"] as? String } set { self["tag"] = newValue } }
    var acceptedSystemFields: Data? { get { self["system"] as? Data } set { self["system"] = newValue } }
    var fieldDigests: [String: Data] { get { self["fields"] as? [String: Data] ?? [:] } set { self["fields"] = newValue } }
    var fields: [String: Data] { fieldDigests }
    static func isEnabled(in realm: Realm) -> Bool { realm.schema.objectSchema.contains { $0.className == className() } }
}
final class BigSyncRecordSubmission: Object {
    var recordName: String { get { self["recordName"] as? String ?? "" } set { self["recordName"] = newValue } }
    var namespace: String { get { self["namespace"] as? String ?? "namespace-1" } set { self["namespace"] = newValue } }
    var comparisonRevision: String? { get { self["comparisonRevision"] as? String ?? "revision-1" } set { self["comparisonRevision"] = newValue } }
}
final class BigSyncRecordConflict: Object {
    var namespace: String { get { self["namespace"] as? String ?? "namespace-1" } set { self["namespace"] = newValue } }
    var entityType: String { get { self["entityType"] as? String ?? AuditNote.className() } set { self["entityType"] = newValue } }
    var isResolved: Bool { get { self["resolved"] as? Bool ?? false } set { self["resolved"] = newValue } }
    var isPreservationReceipt: Bool { get { self["receipt"] as? Bool ?? false } set { self["receipt"] = newValue } }
}
enum SyncedEntityState: Int { case synced, changed, deletedLocally, new }
final class SyncedEntity: Object {
    override class func primaryKey() -> String? { "identifier" }
    var identifier: String { get { self["identifier"] as? String ?? "" } set { self["identifier"] = newValue } }
    var entityType: String { get { self["entityType"] as? String ?? AuditNote.className() } set { self["entityType"] = newValue } }
    var state: Int { get { self["state"] as? Int ?? 0 } set { self["state"] = newValue } }
    var entityState: SyncedEntityState { get { SyncedEntityState(rawValue: state)! } set { state = newValue.rawValue } }
    var pendingGeneration: String? { get { self["generation"] as? String } set { self["generation"] = newValue } }
    var record: CKRecord? { get { self["record"] as? CKRecord } set { self["record"] = newValue } }
}
final class PendingRelationship: Object {}
final class BigSyncInboundSemanticQuarantine: Object {
    var recordName: String { get { self["recordName"] as? String ?? "" } set { self["recordName"] = newValue } }
    var validationCode: String { get { self["validationCode"] as? String ?? "invalid" } set { self["validationCode"] = newValue } }
}
struct BigSyncCompiledRecordContract {
    let signature = "signature-1"
    static func compile(_ object: Object) throws -> Self? { object is BigSyncRecordContractProviding ? Self() : nil }
}
enum BigSyncRecordFingerprint {
    static func properties(of object: Object) -> [Property] { ["text", "owner", "isDeleted"].map(Property.init) }
    static func strings(of object: Object) -> [String: String] {
        guard let note = object as? AuditNote else { return [:] }
        return ["text": note.text, "owner": note.owner, "isDeleted": String(note.isDeleted)]
    }
    static func fields(of object: Object) throws -> [String: Data] {
        // Deterministic fixed-width comparison tokens, NOT production digests.
        strings(of: object).mapValues { value in
            var result = [UInt8](repeating: 0, count: 32)
            for (i, byte) in value.utf8.enumerated() { result[i % 32] &+= byte }
            return Data(result)
        }
    }
}
enum BigSyncRecordLifecycle { static func isPhysicalDeletion(_ object: Object) -> Bool { (object as? SoftDeletable)?.isDeleted == true } }
enum BigSyncRecordPayload {
    static func record(from object: Object, recordID: CKRecord.ID) throws -> CKRecord {
        let record = CKRecord(recordType: type(of: object).className(), recordID: recordID)
        record.fields = BigSyncRecordFingerprint.strings(of: object)
        return record
    }
    static func record(systemFields: Data) throws -> CKRecord { try JSONDecoder().decode(CKRecord.self, from: systemFields) }
}
final class RealmProvider {
    var persistenceRealm: Realm?
    var targetReaderRealms: [Realm]?
    var targetReaderRealmPerSchemaName: [String: Realm]
    init(target: Realm, tracking: Realm) {
        persistenceRealm = tracking; targetReaderRealms = [target]
        targetReaderRealmPerSchemaName = [AuditNote.className(): target, AuditOtherNote.className(): target]
    }
}
public final class RealmSwiftAdapter: @unchecked Sendable {
    var realmProvider: RealmProvider?
    var modelTypes: [String: Object.Type] = [AuditNote.className(): AuditNote.self, AuditOtherNote.className(): AuditOtherNote.self]
    var excludedClassNames: [String] = []
    var accountScopePropertyByClassName: [String: String] = [:]
    var activeAccountScopeIdentifier: String? = "account-1"
    var activeReplicaBindingGenerationIdentifier: String? = "binding-1"
    var recordRebaseContext: BigSyncRecordRebaseContext? = .init(namespace: "namespace-1", account: "account-1", binding: "binding-1")
    let recordZoneID = CKRecordZone.ID(zoneName: "zone")
    var beforeComparison: (@BigSyncBackgroundActor () -> Void)?
    init(target: Realm, tracking: Realm) { realmProvider = RealmProvider(target: target, tracking: tracking) }
    @BigSyncBackgroundActor func ensureSetup() async throws {}
    func realmObjectClass(name: String) -> Object.Type? { modelTypes[name] }
    func getObjectIdentifier(recordName: String, entityType: String) -> String? {
        let prefix = entityType + "."; guard recordName.hasPrefix(prefix) else { return nil }
        return String(recordName.dropFirst(prefix.count))
    }
    func getObjectIdentifier(for entity: SyncedEntity) -> String? {
        getObjectIdentifier(recordName: entity.identifier, entityType: entity.entityType)
    }
    static func getTargetObjectStringIdentifier(for object: Object, usingPrimaryKey: String) -> String { object[usingPrimaryKey] as? String ?? "" }
    func getRecord(for entity: SyncedEntity) -> CKRecord? { entity.record }
    @BigSyncBackgroundActor func serverDifferencePropertyNames(record: CKRecord, object: Object) -> [String] {
        let hook = beforeComparison; beforeComparison = nil; hook?()
        return BigSyncRecordFingerprint.strings(of: object).keys.filter {
            BigSyncRecordFingerprint.strings(of: object)[$0] != record.fields[$0]
        }.sorted()
    }
    func decodedComparisonObject(_ record: CKRecord, type: Object.Type) throws -> Object {
        let object = type.init()
        if let note = object as? AuditNote {
            guard let text = record.fields["text"], let owner = record.fields["owner"],
                  let deleted = record.fields["isDeleted"], let isDeleted = Bool(deleted) else { throw FixtureError.invalidRecord }
            note.text = text; note.owner = owner; note.isDeleted = isDeleted
        }
        return object
    }
    func validatedSubmissionRecord(_ submitted: BigSyncRecordSubmission, recordID: CKRecord.ID,
        type: Object.Type, context: BigSyncRecordRebaseContext) throws -> CKRecord {
        // Representation validation itself is outside this audit-read harness.
        CKRecord(recordType: type.className(), recordID: recordID)
    }
    @BigSyncBackgroundActor func activeInboundSemanticQuarantines(
        accountScopeIdentifier: String, in realm: Realm) -> Results<BigSyncInboundSemanticQuarantine> {
        Results(Array(realm.objects(BigSyncInboundSemanticQuarantine.self)).filter { $0["account"] as? String == accountScopeIdentifier })
    }

    // Unmodified eligibility bodies from RealmSwiftAdapter.swift at 32bf4338.
    @BigSyncBackgroundActor
    func syncedEntityIsEligibleForActiveAccount(
        _ syncedEntity: SyncedEntity
    ) -> Bool {
        guard accountScopePropertyByClassName[
            syncedEntity.entityType
        ] != nil else { return true }
        guard let objectClass = realmObjectClass(
            name: syncedEntity.entityType
        ), let objectIdentifier = getObjectIdentifier(for: syncedEntity),
        let object = realmProvider?.targetReaderRealmPerSchemaName[
            syncedEntity.entityType
        ]?.object(
            ofType: objectClass,
            forPrimaryKey: objectIdentifier
        ) else {
            return false
        }
        return objectIsEligibleForActiveAccount(
            object,
            entityType: syncedEntity.entityType
        )
    }

    @BigSyncBackgroundActor
    func pendingMutationIsEligibleForActiveTransport(
        _ mutation: BigSyncPendingMutation
    ) -> Bool {
        pendingMutationIsEligibleForActiveTransport(
            entityType: mutation.entityType,
            accountScopeIdentifier: mutation.accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                mutation.replicaBindingGenerationIdentifier
        )
    }

    @BigSyncBackgroundActor
    private func pendingMutationIsEligibleForActiveTransport(
        entityType: String,
        accountScopeIdentifier: String?,
        replicaBindingGenerationIdentifier: String?
    ) -> Bool {
        if accountScopePropertyByClassName[entityType] != nil {
            guard let activeAccountScopeIdentifier,
                  accountScopeIdentifier == activeAccountScopeIdentifier else {
                return false
            }
        }
        return replicaBindingGenerationIdentifier
            == activeReplicaBindingGenerationIdentifier
    }

    @BigSyncBackgroundActor
    func objectIsEligibleForActiveAccount(
        _ object: Object,
        entityType: String
    ) -> Bool {
        guard let propertyName =
                accountScopePropertyByClassName[entityType] else {
            return true
        }
        guard let activeAccountScopeIdentifier else { return false }
        return (object[propertyName] as? String)
            == activeAccountScopeIdentifier
    }
}
