import CloudKit
import Foundation
import Logging
import RealmSwift
import RealmSwiftGaps
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

#if DEBUG
// A real Realm model exercising BigSync's existing hooks, not the Common model.
// Every test below uses the actual target/tracking Realms, journal and adapter.
private enum RA1RevisionError: Error, BigSyncInboundSemanticValidationFailure {
    case invalidPayload, changedIdentity, divergentEqualRevision, unexpectedIO
    var bigSyncValidationCode: String { "ra1-test-\(self)" }
}

private struct RA1DomainValue: Equatable, Sendable {
    var owner = "owner-a"
    var key = "article-a:page-0"
    var revision: Int64
    var segments: Set<String>
    var characters: Int64
    var activeMicroseconds: Int64 = 100
    var title = "Article"
    var deleted = false
    var id: String { owner + "|" + key }

    func validate() throws {
        guard !owner.isEmpty, !key.isEmpty, revision >= 0, characters >= 0,
              activeMicroseconds >= 0, segments.allSatisfy({ !$0.isEmpty }) else {
            throw RA1RevisionError.invalidPayload
        }
    }
}

@objc(BigSyncRA1OwnedRevisionObject)
private final class RA1OwnedRevisionObject: Object, ChangeMetadataRecordable,
    BigSyncStringEncodedIntegerModel, BigSyncInboundSemanticRecordValidating,
    BigSyncInboundSemanticReplacementValidating,
    BigSyncOutboundSemanticObjectValidating, BigSyncRetainsSyncedTombstone {
    static let bigSyncStringEncodedIntegerPropertyNames: Set<String> = [
        "stateRevision", "characters", "activeMicroseconds",
    ]
    @Persisted(primaryKey: true) var id = ""
    @Persisted var owner = ""
    @Persisted var logicalKey = ""
    @Persisted var stateRevision: Int64 = 0
    @Persisted var segments: MutableSet<String>
    @Persisted var characters: Int64 = 0
    @Persisted var activeMicroseconds: Int64 = 0
    @Persisted var title = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    var retainsSyncedTombstone: Bool { true }

    var domain: RA1DomainValue {
        RA1DomainValue(owner: owner, key: logicalKey, revision: stateRevision,
                       segments: Set(segments), characters: characters,
                       activeMicroseconds: activeMicroseconds, title: title,
                       deleted: isDeleted)
    }

    func assign(_ value: RA1DomainValue) {
        owner = value.owner
        logicalKey = value.key
        stateRevision = value.revision
        segments.removeAll()
        segments.insert(objectsIn: value.segments)
        characters = value.characters
        activeMicroseconds = value.activeMicroseconds
        title = value.title
        isDeleted = value.deleted
    }

    static func decode(_ record: CKRecord) throws -> RA1DomainValue {
        guard record.recordType == className(),
              let owner = record["owner"] as? String,
              let key = record["logicalKey"] as? String,
              let revision = BigSyncStringEncodedIntegerCodec.decode(record["stateRevision"]),
              let characters = BigSyncStringEncodedIntegerCodec.decode(record["characters"]),
              let duration = BigSyncStringEncodedIntegerCodec.decode(record["activeMicroseconds"]),
              let title = record["title"] as? String,
              let deleted = BigSyncCloudKitBooleanCodec.decode(record["isDeleted"]),
              record["segments"] == nil || record["segments"] is [String] else {
            throw RA1RevisionError.invalidPayload
        }
        let value = RA1DomainValue(owner: owner, key: key, revision: revision,
            segments: Set(record["segments"] as? [String] ?? []), characters: characters,
            activeMicroseconds: duration, title: title, deleted: deleted)
        try value.validate()
        guard record.recordID.recordName == className() + "." + value.id else {
            throw RA1RevisionError.changedIdentity
        }
        return value
    }

    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        _ = try decode(record)
    }

    static func validateInboundSemanticReplacement(
        _ record: CKRecord, existingObject: Object?
    ) throws {
        _ = try inboundSemanticReplacementDisposition(record, existingObject: existingObject)
    }

    static func inboundSemanticReplacementDisposition(
        _ record: CKRecord, existingObject: Object?
    ) throws -> BigSyncInboundSemanticReplacementDisposition {
        let incoming = try decode(record)
        guard let existing = existingObject as? RA1OwnedRevisionObject else {
            return .applyIncomingRecord
        }
        let local = existing.domain
        try local.validate()
        guard incoming.id == existing.id,
              incoming.owner == local.owner, incoming.key == local.key else {
            throw RA1RevisionError.changedIdentity
        }
        if incoming.revision > local.revision { return .preferIncomingRecord }
        if incoming.revision < local.revision { return .preferExistingObject }
        guard incoming == local else { throw RA1RevisionError.divergentEqualRevision }
        // Audit timestamps and CK system/transport fields do not change domain equality.
        return .preserveExistingObject
    }

    func validateOutboundSemanticObject(in realm: Realm) throws {
        try domain.validate()
        guard id == domain.id else { throw RA1RevisionError.changedIdentity }
        // A foreign-owned unchanged relay is allowed; it is not a new authored read.
    }
}

@BigSyncBackgroundActor
private final class RA1RealmFixture {
    let adapter: RealmSwiftAdapter
    let target: Realm
    let tracking: Realm
    var journalIdentity: BigSyncMutationJournalIdentity
    let scope: String
    let assets: URL

    private init(adapter: RealmSwiftAdapter, target: Realm, tracking: Realm, assets: URL) {
        self.adapter = adapter
        self.target = target
        self.tracking = tracking
        self.assets = assets
        self.scope = "ra1-test-account"
        self.journalIdentity = .init(installationIdentifier: "owner-a",
            replicaBindingGenerationIdentifier: String(repeating: "a", count: 64))
    }

    static func make() async throws -> RA1RealmFixture {
        let nonce = UUID().uuidString
        var persistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistence.inMemoryIdentifier = "ra1-tracking-" + nonce
        var configuration = Realm.Configuration()
        configuration.inMemoryIdentifier = "ra1-target-" + nonce
        configuration.objectTypes = [RA1OwnedRevisionObject.self, BigSyncPendingMutation.self]
        let assets = FileManager.default.temporaryDirectory.appendingPathComponent("ra1-assets-" + nonce)
        let adapter = RealmSwiftAdapter(persistenceRealmConfiguration: persistence,
            targetRealmConfigurations: [configuration], excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "ra1-" + nonce,
                                         ownerName: CKCurrentUserDefaultName),
            logger: Logger(label: "OwnedRecordRevisionTests"), startSetupTask: false,
            assetDirectoryURL: assets)
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens() // Tests explicitly forward the real journal, without timer races.
        let target = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let result = RA1RealmFixture(adapter: adapter, target: target, tracking: tracking, assets: assets)
        try await result.bind(result.journalIdentity, scope: result.scope)
        return result
    }

    func bind(_ identity: BigSyncMutationJournalIdentity, scope: String) async throws {
        journalIdentity = identity
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target.configuration],
            mutationJournalIdentityProvider: { identity })
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.ra1-test", databaseScope: .private)
        try await adapter.activateReplicaBinding(accountScopeIdentifier: scope,
            replicaBindingGenerationIdentifier: identity.replicaBindingGenerationIdentifier)
    }

    func record(_ value: RA1DomainValue, auditTime: TimeInterval = 1_000) -> CKRecord {
        let record = CKRecord(recordType: RA1OwnedRevisionObject.className(),
            recordID: .init(recordName: name(value.id), zoneID: adapter.recordZoneID))
        record["owner"] = value.owner as CKRecordValue
        record["logicalKey"] = value.key as CKRecordValue
        record["stateRevision"] = String(value.revision) as CKRecordValue
        record["segments"] = value.segments.sorted() as CKRecordValue
        record["characters"] = String(value.characters) as CKRecordValue
        record["activeMicroseconds"] = String(value.activeMicroseconds) as CKRecordValue
        record["title"] = value.title as CKRecordValue
        record["isDeleted"] = value.deleted as CKRecordValue
        record["createdAt"] = Date(timeIntervalSince1970: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSince1970: auditTime) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSince1970: auditTime) as CKRecordValue
        return record
    }

    func name(_ id: String = "owner-a|article-a:page-0") -> String {
        RA1OwnedRevisionObject.className() + "." + id
    }

    func value(_ id: String = "owner-a|article-a:page-0") throws -> RA1DomainValue {
        try XCTUnwrap(target.object(ofType: RA1OwnedRevisionObject.self, forPrimaryKey: id)).domain
    }

    func generation(_ id: String = "owner-a|article-a:page-0") -> String? {
        target.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name(id))?.generation
    }

    func refresh() async {
        await target.asyncRefresh()
        await tracking.asyncRefresh()
    }

    @discardableResult
    func author(_ value: RA1DomainValue, auditTime: TimeInterval = 1_000) async throws -> String {
        try await target.asyncWrite {
            let object = self.target.object(ofType: RA1OwnedRevisionObject.self, forPrimaryKey: value.id)
                ?? RA1OwnedRevisionObject()
            if object.realm == nil { object.id = value.id }
            object.assign(value)
            if object.realm == nil { self.target.add(object) }
            _ = try object.refreshChangeMetadata(explicitlyModified: true,
                at: Date(timeIntervalSince1970: auditTime), expectedJournalIdentity: self.journalIdentity)
        }
        return try XCTUnwrap(generation(value.id))
    }

    func prepared() async throws -> [PreparedRecordUpload] {
        try await adapter._test_forwardPendingMutations(in: target)
        return try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
    }

    func nextUpload() async throws -> PreparedRecordUpload {
        let uploads = try await prepared()
        XCTAssertEqual(uploads.count, 1)
        return try XCTUnwrap(uploads.first)
    }

    func acknowledge(_ prepared: PreparedRecordUpload) async throws {
        try await adapter.didUpload(savedRecords: [prepared.record],
            matchingGenerations: [prepared.record.recordID.recordName: try XCTUnwrap(prepared.generation)])
        await refresh()
    }
}

final class OwnedRecordRevisionTests: XCTestCase {
    private let mark = RA1DomainValue(revision: 42, segments: ["x", "y"], characters: 7)
    private let undo = RA1DomainValue(revision: 43, segments: [], characters: 0)

    @BigSyncBackgroundActor
    func testLatePopulatedDownloadRequeuesNewerEmptyValueWithoutReauthoring() async throws {
        for forceSave in [false, true] {
            for retainedTombstone in [false, true] {
                let f = try await RA1RealmFixture.make()
                var newer = undo
                newer.deleted = retainedTombstone
                _ = try await f.adapter.saveChanges(in: [f.record(newer)], forceSave: true)
                await f.refresh()
                XCTAssertNil(f.generation())
                _ = try await f.adapter.saveChanges(in: [f.record(mark, auditTime: 9_000)], forceSave: forceSave)
                await f.refresh()
                XCTAssertEqual(try f.value(), newer)
                XCTAssertNotNil(f.generation(), "Local preference must become ordinary upload work")
                let upload = try await f.nextUpload()
                XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), newer)
                XCTAssertEqual(upload.record["modifiedAt"] as? Date, Date(timeIntervalSince1970: 1_000))
                try await f.acknowledge(upload)
                XCTAssertNil(f.generation())
                XCTAssertEqual(try f.value(), newer, "Acknowledgement must retain the empty/tombstoned version")
            }
        }
    }

    @BigSyncBackgroundActor
    func testHigherIncomingRevisionReplacesPendingValueAndMintsNewGeneration() async throws {
        for forceSave in [false, true] {
            let f = try await RA1RealmFixture.make()
            let g1 = try await f.author(mark, auditTime: 9_000)
            let sent = try await f.nextUpload()
            _ = try await f.adapter.saveChanges(in: [f.record(undo, auditTime: 10)], forceSave: forceSave)
            await f.refresh()
            let g2 = try XCTUnwrap(f.generation())
            XCTAssertNotEqual(g1, g2)
            XCTAssertEqual(try f.value(), undo)
            try await f.acknowledge(sent)
            XCTAssertEqual(f.generation(), g2)
            let next = try await f.nextUpload()
            XCTAssertEqual(next.generation, g2)
            XCTAssertEqual(try RA1OwnedRevisionObject.decode(next.record), undo)
            try await f.acknowledge(next)
            XCTAssertNil(f.generation())
        }
    }

    @BigSyncBackgroundActor
    func testOlderOwnEchoAndG1AcknowledgementCannotRetireG2() async throws {
        for tombstone in [false, true] {
            let f = try await RA1RealmFixture.make()
            _ = try await f.author(mark)
            let sent = try await f.nextUpload()
            var newer = undo
            newer.deleted = tombstone
            let g2 = try await f.author(newer)
            // Intentionally do not forward G2 before G1 acknowledgement. The target
            // journal, not the tracking cache's last forwarded generation, is decisive.
            let results = try await f.adapter.validateAuthoritativeOwnUploadRecords([sent.record])
            XCTAssertEqual(results.first?.disposition, .validatedAuthoritativeOwnUpload)
            try await f.acknowledge(sent)
            XCTAssertEqual(try f.value(), newer)
            XCTAssertEqual(f.generation(), g2)
            let pending = try await f.nextUpload()
            XCTAssertEqual(pending.generation, g2)
            XCTAssertEqual(try RA1OwnedRevisionObject.decode(pending.record), newer)
        }
    }

    @BigSyncBackgroundActor
    func testClockRevisionThenUndoTransportsNewestCompletePayload() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.author(mark)
        var clock = mark
        clock.revision = 43
        clock.activeMicroseconds = 250
        clock.title = "Updated title"
        _ = try await f.author(clock)
        var reversed = clock
        reversed.revision = 44
        reversed.segments = []
        reversed.characters = 0
        _ = try await f.author(reversed)
        for older in [mark, clock] {
            _ = try await f.adapter.saveChanges(in: [f.record(older, auditTime: 99_000)], forceSave: true)
        }
        await f.refresh()
        XCTAssertEqual(try f.value(), reversed)
        let upload = try await f.nextUpload()
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), reversed)
        // This tests complete record transport, not Common's inverse or semantic guard.
    }

    @BigSyncBackgroundActor
    func testEqualRevisionReplayNormalizesSetsAndIgnoresAuditMetadata() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.adapter.saveChanges(in: [f.record(mark)], forceSave: true)
        let replay = f.record(mark, auditTime: 99_000)
        replay["segments"] = ["y", "x"] as CKRecordValue
        replay["createdAt"] = Date(timeIntervalSince1970: 500) as CKRecordValue
        _ = try await f.adapter.saveChanges(in: [replay], forceSave: true)
        await f.refresh()
        XCTAssertEqual(try f.value(), mark)
        XCTAssertNil(f.generation())
        XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        let object = try XCTUnwrap(f.target.object(ofType: RA1OwnedRevisionObject.self, forPrimaryKey: mark.id))
        XCTAssertEqual(object.modifiedAt, Date(timeIntervalSince1970: 1_000))
    }

    @BigSyncBackgroundActor
    func testEqualRevisionDivergenceIsQuarantinedWithoutLosingPendingWork() async throws {
        for forceSave in [false, true] {
            let f = try await RA1RealmFixture.make()
            let generation = try await f.author(mark)
            var different = mark
            different.activeMicroseconds += 1 // Clock fields are part of complete domain equality.
            let results = try await f.adapter.saveChanges(in: [f.record(different)], forceSave: forceSave)
            await f.refresh()
            XCTAssertEqual(results.count, 1)
            guard let result = results.first, case .quarantined = result.disposition else {
                XCTFail("Expected existing semantic quarantine diagnostics")
                return
            }
            XCTAssertEqual(try f.value(), mark)
            XCTAssertEqual(f.generation(), generation)
            XCTAssertEqual(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
            let upload = try await f.nextUpload()
            XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), mark)
        }
    }

    @BigSyncBackgroundActor
    func testOmittedEmptyMembershipIsTheSameDomainReplay() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.adapter.saveChanges(in: [f.record(undo)], forceSave: true)
        let omitted = f.record(undo, auditTime: 99_000)
        omitted["segments"] = nil
        _ = try await f.adapter.saveChanges(in: [omitted], forceSave: true)
        await f.refresh()
        XCTAssertEqual(try f.value(), undo)
        XCTAssertNil(f.generation())
        XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testChangedOwnerCannotAddressExistingRecordEvenWithHigherRevision() async throws {
        let f = try await RA1RealmFixture.make()
        let generation = try await f.author(mark)
        let forged = f.record(undo)
        forged["owner"] = "owner-b" as CKRecordValue
        _ = try await f.adapter.saveChanges(in: [forged], forceSave: true)
        await f.refresh()
        XCTAssertEqual(try f.value(), mark)
        XCTAssertEqual(f.generation(), generation)
        XCTAssertEqual(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testRequiredJournalIdentityFailureRollsBackDomainAndRevision() async throws {
        let f = try await RA1RealmFixture.make()
        let beforeGeneration = try await f.author(mark)
        let differentIdentity = BigSyncMutationJournalIdentity(installationIdentifier: "replacement",
            replicaBindingGenerationIdentifier: String(repeating: "b", count: 64))
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [f.target.configuration],
            mutationJournalIdentityProvider: { differentIdentity })
        do {
            _ = try await f.author(undo)
            XCTFail("An identity change must abort the owning Realm write")
        } catch is BigSyncMutationJournalError { }
        await f.refresh()
        XCTAssertEqual(try f.value(), mark)
        XCTAssertEqual(f.generation(), beforeGeneration)
    }

    @BigSyncBackgroundActor
    func testReseedKeepsOriginalOwnerAndRevisionUnderNewJournalIdentity() async throws {
        let f = try await RA1RealmFixture.make()
        let originalGeneration = try await f.author(undo)
        let replacement = BigSyncMutationJournalIdentity(installationIdentifier: "restored-installation",
            replicaBindingGenerationIdentifier: String(repeating: "b", count: 64))
        try await f.bind(replacement, scope: f.scope)
        let epoch = 4_000_000_001
        try await f.adapter.prepareChangeFeedReset(accountScopeIdentifier: f.scope, epoch: epoch,
                                                   mode: .localDatasetRebootstrap)
        try await f.adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: f.scope, epoch: epoch,
                                                            mode: .localDatasetRebootstrap)
        try await f.adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: f.scope,
                                                                     epoch: epoch, mode: .localDatasetRebootstrap)
        await f.refresh()
        XCTAssertEqual(try f.value(), undo)
        XCTAssertNotEqual(f.generation(), originalGeneration)
        let pending = try XCTUnwrap(f.target.object(ofType: BigSyncPendingMutation.self,
                                                   forPrimaryKey: f.name()))
        XCTAssertEqual(pending.replicaBindingGenerationIdentifier, replacement.replicaBindingGenerationIdentifier)
        let upload = try await f.nextUpload()
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), undo)
        // A controlled empty destination tests relay, not admission of a real backup.
    }

    @BigSyncBackgroundActor
    func testRealUploadLoopRetriesLowerServerConflictWithNewerLocalEmptyState() async throws {
        try await exerciseConflictLoop(local: undo, server: mark, expected: undo)
    }

    @BigSyncBackgroundActor
    func testRealUploadLoopReplacesStalePendingMarkWithNewerServerEmptyState() async throws {
        try await exerciseConflictLoop(local: mark, server: undo, expected: undo)
    }

    @BigSyncBackgroundActor
    private func exerciseConflictLoop(local: RA1DomainValue, server: RA1DomainValue,
                                      expected: RA1DomainValue) async throws {
        let f = try await RA1RealmFixture.make()
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("ra1-transport-" + UUID().uuidString)
        let io = RA1ScriptedIO(serverConflict: f.record(server, auditTime: 90_000))
        let store = RA1KeyValueStore()
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.ra1-test", database: io,
            recordZoneID: f.adapter.recordZoneID, keyValueStore: store,
            accountIdentifierProvider: { "ra1-cloud-account" }, accountStatusProvider: { .available },
            backupDetectionBaseURL: root, logger: Logger(label: "RA1ConflictLoop"))
        sync.addModelAdapter(f.adapter)
        // Same actual account/attempt setup used by the existing direct-upload tests.
        // Only remote IO is scripted; the adapter, journals and admission lease are real.
        try await sync._test_validateSynchronizationAccount()
        let account = try XCTUnwrap(store.object(forKey: sync.durableStateKey("CloudKitAccountIdentifier")) as? String)
        let lease = try XCTUnwrap(sync.accountScopeLease())
        let binding = try sync.activeReplicaBindingGenerationIdentifierForRun(accountScopeIdentifier: lease.accountScopeIdentifier)
        let installation = try XCTUnwrap(BackupDetection.installationIdentifier(
            namespace: sync.durableStateNamespace, sharedSentinelBaseURL: root))
        try await f.bind(.init(installationIdentifier: installation,
                              replicaBindingGenerationIdentifier: binding), scope: lease.accountScopeIdentifier)
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: account,
            accountScopeIdentifier: lease.accountScopeIdentifier,
            replicaBindingGenerationIdentifier: binding, accountInvalidationGeneration: lease.invalidationGeneration)
        _ = try await f.author(local)
        _ = try await f.prepared()
        try await sync.synchronizeAdapter(f.adapter)
        await f.refresh()
        let uploads = await io.uploadedValues()
        XCTAssertEqual(uploads, [local, expected])
        XCTAssertEqual(try f.value(), expected)
        XCTAssertNil(f.generation(), "Retry must reach real generation acknowledgement")
        XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        let transport = try sync.outboundQuiescenceSnapshot()
        XCTAssertTrue(transport.outstandingSubmissions.isEmpty)
    }
}

// Deliberately bounded remote script: exactly one serverRecordChanged followed
// by one successful retry. Unexpected operations throw rather than succeeding.
private actor RA1MutationScript {
    let conflict: CKRecord
    var uploads: [RA1DomainValue] = []
    init(conflict: CKRecord) { self.conflict = conflict }
    func modify(_ records: [CKRecord], deleting: [CKRecord.ID],
                savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) throws -> CloudKitRecordMutationResults {
        guard records.count == 1, deleting.isEmpty, !atomically,
              savePolicy == .ifServerRecordUnchanged, uploads.count < 2,
              records[0].recordID == conflict.recordID else { throw RA1RevisionError.unexpectedIO }
        let record = records[0]
        uploads.append(try RA1OwnedRevisionObject.decode(record))
        if uploads.count == 1 {
            let error = NSError(domain: CKErrorDomain, code: CKError.serverRecordChanged.rawValue,
                userInfo: [CKRecordChangedErrorServerRecordKey: conflict])
            return .init(saveResults: [record.recordID: .failure(error)], deleteResults: [:])
        }
        return .init(saveResults: [record.recordID: .success(record)], deleteResults: [:])
    }
}

private final class RA1ScriptedIO: NSObject, CloudKitDatabaseAdapter, CloudKitRecordStore,
    CloudKitChangeFeed, CloudKitZoneStore, CloudKitSubscriptionStore, @unchecked Sendable {
    let script: RA1MutationScript
    var databaseScope: CKDatabase.Scope { .private }
    init(serverConflict: CKRecord) { script = RA1MutationScript(conflict: serverConflict) }
    func uploadedValues() async -> [RA1DomainValue] { await script.uploads }
    func modifyRecords(saving records: [CKRecord], deleting ids: [CKRecord.ID],
                       savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) async throws -> CloudKitRecordMutationResults {
        try await script.modify(records, deleting: ids, savePolicy: savePolicy, atomically: atomically)
    }
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone { CKRecordZone(zoneID: id) }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone { throw RA1RevisionError.unexpectedIO }
    func deleteRecordZone(withID id: CKRecordZone.ID) async throws { throw RA1RevisionError.unexpectedIO }
    func subscription(withID id: CKSubscription.ID) async throws -> CKSubscription? { throw RA1RevisionError.unexpectedIO }
    func save(subscription: CKSubscription) async throws -> CKSubscription { throw RA1RevisionError.unexpectedIO }
    func deleteSubscription(withID id: CKSubscription.ID) async throws { throw RA1RevisionError.unexpectedIO }
    func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        throw RA1RevisionError.unexpectedIO
    }
    func recordZoneChanges(in id: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?,
                           desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        throw RA1RevisionError.unexpectedIO
    }
}

private final class RA1KeyValueStore: NSObject, KeyValueStore {
    // The synchronizer accesses its store on BigSyncBackgroundActor.
    private var values: [String: Any] = [:]
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value }
    func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
    func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    func synchronize() -> Bool { true }
}
#endif
