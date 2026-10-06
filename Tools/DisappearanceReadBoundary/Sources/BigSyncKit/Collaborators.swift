import CloudKit
import Foundation
import RealmSwift

@globalActor public actor BigSyncBackgroundActor { public static let shared = BigSyncBackgroundActor() }
extension Realm {
    @BigSyncBackgroundActor
    public func asyncWritePreservingOwnership(_ operation: () throws -> Void) async throws {
        try Task.checkCancellation()
        try write { try operation(); try Task.checkCancellation() }
        DisappearanceCallbacks.afterWrite?(self)
    }
}
struct BigSyncRecordRebaseContext: Sendable, Equatable {
    let namespace: String
    var binding: String { "binding-1" }
    func validate(in realm: Realm) throws {
        precondition(realm.isInWriteTransaction)
        guard let identity = BigSyncMutationTrackingRegistry.currentMutationJournalIdentity(in: realm),
              !identity.installationIdentifier.isEmpty,
              identity.replicaBindingGenerationIdentifier == binding else { throw CancellationError() }
    }
}
enum DisappearanceCallbacks {
    @TaskLocal static var identity: (@Sendable () -> Void)?
    @TaskLocal static var compile: (@Sendable () throws -> Void)?
    @TaskLocal static var lifecycle: (@Sendable () -> Void)?
    @TaskLocal static var eligibility: (@Sendable () -> Void)?
    @TaskLocal static var afterWrite: (@Sendable (Realm) -> Void)?
    @TaskLocal static var journal: (@Sendable () -> Void)?
}
enum BigSyncMutationTrackingRegistry {
    struct Identity { let installationIdentifier: String; let replicaBindingGenerationIdentifier: String }
    static func currentMutationJournalIdentity(in realm: Realm) -> Identity? {
        DisappearanceCallbacks.identity?()
        return Identity(installationIdentifier: "installation-1", replicaBindingGenerationIdentifier: "binding-1")
    }
}
enum BigSyncRecordRebaseError: Error { case inconsistentReceipt(String) }
enum BigSyncRecordContractError: Error { case unexpectedPhysicalDeletion(String) }
enum RealmSwiftAdapterAcknowledgementError: Error { case recordWasNotPrepared }
struct RealmSwiftInboundTargetChangedError: Error { let recordName: String }
enum InboundDeletionDisposition: Equatable {
    case alreadyDeleted, ignoredExplicitAuthority, appliedTombstone
    case preservedNewerLive(generation: String)
}
protocol SoftDeletable: AnyObject { var isDeleted: Bool { get set } }
protocol ChangeMetadataRecordable { func journalCurrentValuePreservingChangeMetadata(at: Date) }
protocol BigSyncInboundSemanticDeletionValidating {
    static func validateInboundSemanticDeletion(_ recordID: CKRecord.ID, existingObject: Object?) throws
}
struct BigSyncCompiledRecordContract {
    enum Deletion { case physical }
    struct Declaration { let deletion: Deletion = .physical }
    let signature = "signature-1"
    let declaration = Declaration()
    static func compile(_ object: Object) throws -> Self? { try DisappearanceCallbacks.compile?(); return object is HarnessLegacyNote ? nil : Self() }
}
enum BigSyncRecordFingerprint { static func fields(of object: Object) throws -> [String: Data] { [:] } }
enum BigSyncRecordLifecycle { static func isPhysicalDeletion(_ object: Object) -> Bool { DisappearanceCallbacks.lifecycle?(); return (object as? SoftDeletable)?.isDeleted == true } }
final class BigSyncRecordBaseline: Object {
    var recordName = ""
    override var rowID: String { recordName }
    var namespace = "namespace-1"
    var schemaSignature = "signature-1"
    var revision = "revision-1"
    var isComparisonInvalidated = false
    var fields = [String: Data]()
    var serverChangeTag: String? = "accepted-tag"
    var acceptedSystemFields: Data? = Data([1])
    override func copyRow() -> Object {
        let r = BigSyncRecordBaseline(); r.recordName = recordName; r.namespace = namespace
        r.schemaSignature = schemaSignature; r.revision = revision; r.isComparisonInvalidated = isComparisonInvalidated
        r.fields = fields; r.serverChangeTag = serverChangeTag; r.acceptedSystemFields = acceptedSystemFields; return r
    }
    static func invalidate(recordName: String, in realm: Realm) {
        let r = realm.object(ofType: Self.self, forPrimaryKey: recordName) ?? Self()
        r.recordName = recordName; r.revision = UUID().uuidString; r.isComparisonInvalidated = true
        r.fields = [:]; r.serverChangeTag = nil; r.acceptedSystemFields = nil; realm.add(r)
    }
}
final class BigSyncRecordSubmission: Object {
    var id = ""; override var rowID: String { id }
    var recordName = ""; var namespace = "namespace-1"; var schemaSignature = "signature-1"
    var generation = "generation-1"; var comparisonRevision: String? = "revision-1"
    var candidateIdentity = "submission-1"; var payload = Data(); var fields = [String: Data]()
    override func copyRow() -> Object {
        let r = Self(); r.id = id; r.recordName = recordName; r.namespace = namespace
        r.schemaSignature = schemaSignature; r.generation = generation; r.comparisonRevision = comparisonRevision
        r.candidateIdentity = candidateIdentity; r.payload = payload; r.fields = fields; return r
    }
}
final class BigSyncPendingMutation: Object {
    var recordName = ""; override var rowID: String { recordName }
    var generation = "generation-1"; var replicaBindingGenerationIdentifier: String? = "binding-1"
    override func copyRow() -> Object {
        let r = Self(); r.recordName = recordName; r.generation = generation
        r.replicaBindingGenerationIdentifier = replicaBindingGenerationIdentifier; return r
    }
}
enum SyncedEntityState: Int { case new, changed, deletedLocally, deletedRemotely, synced }
final class SyncedEntity: Object {
    var entityType = ""; var identifier = ""; override var rowID: String { identifier }
    var state = SyncedEntityState.synced.rawValue
    var entityState: SyncedEntityState { get { SyncedEntityState(rawValue: state)! } set { state = newValue.rawValue } }
    var encodedRecord: Data? = Data([1]); var pendingGeneration: String?; var pendingReplicaBindingGenerationIdentifier: String?
    required init() { super.init() }
    init(entityType: String, identifier: String, state: Int) {
        self.entityType = entityType; self.identifier = identifier; self.state = state; super.init()
    }
    func setPendingMutation(generation: String, replicaBindingGenerationIdentifier: String?) {
        pendingGeneration = generation; pendingReplicaBindingGenerationIdentifier = replicaBindingGenerationIdentifier
    }
    func clearPendingMutation() { pendingGeneration = nil; pendingReplicaBindingGenerationIdentifier = nil }
    override func copyRow() -> Object {
        let r = Self(); r.entityType = entityType; r.identifier = identifier; r.state = state
        r.encodedRecord = encodedRecord; r.pendingGeneration = pendingGeneration
        r.pendingReplicaBindingGenerationIdentifier = pendingReplicaBindingGenerationIdentifier; return r
    }
}
final class HarnessLegacyNote: Object {}
final class HarnessNote: Object, SoftDeletable, ChangeMetadataRecordable {
    var id = "note"; override var rowID: String { id }
    var isDeleted = false; var account = "account-1"; var text = "committed"
    override func copyRow() -> Object {
        let r = Self(); r.id = id; r.isDeleted = isDeleted; r.account = account; r.text = text; return r
    }
    func journalCurrentValuePreservingChangeMetadata(at: Date) {
        let realm = realm!
        let r = BigSyncPendingMutation(); r.recordName = Self.className() + "." + id
        r.generation = UUID().uuidString; realm.add(r); DisappearanceCallbacks.journal?()
    }
}
struct ComparisonBase {
    let context: BigSyncRecordRebaseContext
    let revision: String?
    let submissionIdentity: String?
    let schemaSignature: String
    let fields: [String: Data]
}
public struct PreparedRecordUpload { let record: CKRecord; let generation: String; let comparisonBase: ComparisonBase? }
public struct PreparedRecordDeletion { let recordID: CKRecord.ID; let generation: String?; let evidence: BigSyncPreparedDeletionEvidence? }
final class RealmProvider { let persistenceRealm: Realm?; let targetReaderRealmPerSchemaName: [String: Realm]
    init(target: Realm, tracking: Realm) {
        persistenceRealm = tracking; targetReaderRealmPerSchemaName = [HarnessNote.className(): target]
    }
}
enum BigSyncRecordPayload {
    static func identity(_ parts: [String]) -> String { parts.joined(separator: "|") }
    static func decode(_ data: Data, assetManager: Int? = nil) throws -> CKRecord {
        let name = data.isEmpty ? HarnessNote.className() + ".note" : String(decoding: data, as: UTF8.self)
        let record = CKRecord(recordType: HarnessNote.className(), recordID: .init(recordName: name, zoneID: .init(zoneName: "zone")))
        record.recordChangeTag = "accepted-tag"; return record
    }
}
public final class RealmSwiftAdapter: @unchecked Sendable {
    var realmProvider: RealmProvider?
    let recordZoneID = CKRecordZone.ID(zoneName: "zone")
    let persistentAssetManager = 0
    var context = BigSyncRecordRebaseContext(namespace: "namespace-1")
    var cancellationGeneration: UInt64 = 0
    var cancelSync = false
    var comparisonEnabled = true
    var recordRebaseContext: BigSyncRecordRebaseContext? { comparisonEnabled ? context : nil }
    var legacyRequeues = [([CKRecord.ID], [String: String])]()
    var binding = "binding-1"; var account = "account-1"
    var _testAfterDisappearanceTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testAfterDisappearanceTrackingWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeRemoteDeletionTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeMissingServerTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    init(target: Realm, tracking: Realm) { realmProvider = RealmProvider(target: target, tracking: tracking) }
    func decodedComparisonObject(_ record: CKRecord, type: Object.Type) -> Object { type.init() }
    func getObjectIdentifier(recordName: String, entityType: String) -> String? {
        let prefix = entityType + "."; guard recordName.hasPrefix(prefix) else { return nil }
        return String(recordName.dropFirst(prefix.count))
    }
    func realmObjectClass(name: String) -> Object.Type? { name == HarnessNote.className() ? HarnessNote.self : (name == HarnessLegacyNote.className() ? HarnessLegacyNote.self : nil) }
    func matchingSubmission(recordName: String, context: BigSyncRecordRebaseContext, in realm: Realm) -> BigSyncRecordSubmission? {
        realm.object(ofType: BigSyncRecordSubmission.self, forPrimaryKey: BigSyncRecordPayload.identity([context.namespace, recordName]))
    }
    func pendingMutationIsEligibleForActiveTransport(_ mutation: BigSyncPendingMutation) -> Bool { mutation.replicaBindingGenerationIdentifier == binding }
    func objectIsEligibleForActiveAccount(_ object: Object, entityType: String) -> Bool { DisappearanceCallbacks.eligibility?(); return (object as? HarnessNote)?.account == account }
    @BigSyncBackgroundActor func requeueMissingServerRecords(_ ids: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws { legacyRequeues.append((ids, matchingPreparedGenerations)) }
    @BigSyncBackgroundActor func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor func updateHasChanges(realm: Realm) {}
}
