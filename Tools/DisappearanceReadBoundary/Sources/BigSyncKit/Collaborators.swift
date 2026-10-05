// Portable test collaborator only; this is not the native SDK or adapter implementation.
import CloudKit
import Foundation
import RealmSwift

@globalActor public actor BigSyncBackgroundActor { public static let shared = BigSyncBackgroundActor() }
extension Realm {
    @BigSyncBackgroundActor
    public func asyncWritePreservingOwnership(_ operation: () throws -> Void) async throws {
        try Task.checkCancellation()
        try write(operation)
    }
}
struct BigSyncRecordRebaseContext: Sendable, Equatable { let namespace: String }
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
    static func compile(_ object: Object) throws -> Self? { Self() }
}
enum BigSyncRecordFingerprint { static func fields(of object: Object) throws -> [String: Data] { [:] } }
enum BigSyncRecordLifecycle { static func isPhysicalDeletion(_ object: Object) -> Bool { (object as? SoftDeletable)?.isDeleted == true } }
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
final class HarnessNote: Object, SoftDeletable, ChangeMetadataRecordable {
    var id = "note"; override var rowID: String { id }
    var isDeleted = false; var account = "account-1"; var text = "committed"
    override func copyRow() -> Object {
        let r = Self(); r.id = id; r.isDeleted = isDeleted; r.account = account; r.text = text; return r
    }
    func journalCurrentValuePreservingChangeMetadata(at: Date) {
        let realm = realm!
        let r = BigSyncPendingMutation(); r.recordName = Self.className() + "." + id
        r.generation = UUID().uuidString; realm.add(r)
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
        let record = CKRecord(recordType: HarnessNote.className(), recordID: .init(recordName: HarnessNote.className() + ".note", zoneID: .init(zoneName: "zone")))
        record.recordChangeTag = "accepted-tag"; return record
    }
}
public final class RealmSwiftAdapter: @unchecked Sendable {
    let realmProvider: RealmProvider?
    let recordZoneID = CKRecordZone.ID(zoneName: "zone")
    let persistentAssetManager = 0
    var context = BigSyncRecordRebaseContext(namespace: "namespace-1")
    var cancellationGeneration: UInt64 = 0
    var binding = "binding-1"; var account = "account-1"
    var _testAfterDisappearanceTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeRemoteDeletionTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    var _testBeforeMissingServerTargetWrite: (@BigSyncBackgroundActor @Sendable () async throws -> Void)?
    init(target: Realm, tracking: Realm) { realmProvider = RealmProvider(target: target, tracking: tracking) }
    func decodedComparisonObject(_ record: CKRecord, type: Object.Type) -> Object { type.init() }
    func getObjectIdentifier(recordName: String, entityType: String) -> String? {
        let prefix = entityType + "."; guard recordName.hasPrefix(prefix) else { return nil }
        return String(recordName.dropFirst(prefix.count))
    }
    func realmObjectClass(name: String) -> Object.Type? { name == HarnessNote.className() ? HarnessNote.self : nil }
    @BigSyncBackgroundActor func currentRecordEvidenceCut() throws -> BigSyncRecordEvidenceCut {
        try Task.checkCancellation(); return .init(context: context, cancellationGeneration: cancellationGeneration)
    }
    @BigSyncBackgroundActor func validateRecordEvidenceCut(_ cut: BigSyncRecordEvidenceCut, in realm: Realm) throws {
        try Task.checkCancellation()
        guard cut.context == context && cut.cancellationGeneration == cancellationGeneration else { throw CancellationError() }
    }
    func matchingSubmission(recordName: String, context: BigSyncRecordRebaseContext, in realm: Realm) -> BigSyncRecordSubmission? {
        realm.object(ofType: BigSyncRecordSubmission.self, forPrimaryKey: BigSyncRecordPayload.identity([context.namespace, recordName]))
    }
    func pendingMutationIsEligibleForActiveTransport(_ mutation: BigSyncPendingMutation) -> Bool { mutation.replicaBindingGenerationIdentifier == binding }
    func objectIsEligibleForActiveAccount(_ object: Object, entityType: String) -> Bool { (object as? HarnessNote)?.account == account }
    @BigSyncBackgroundActor func requeueMissingServerRecords(_ ids: [CKRecord.ID], matchingPreparedGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor func didDelete(recordIDs: [CKRecord.ID], matchingGenerations: [String: String]) async throws {}
    @BigSyncBackgroundActor func updateHasChanges(realm: Realm) {}
}
