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
    BigSyncOutboundSemanticObjectValidating, BigSyncRetainsSyncedTombstone,
    BigSyncInboundSemanticDeletionValidating, BigSyncRestoredObjectRecovering,
    SyncSkippablePropertiesModel {
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
    @Persisted var isAwaitingRecoveryEvidence = false
    var retainsSyncedTombstone: Bool { true }
    func skipSyncingProperties() -> Set<String>? { ["isAwaitingRecoveryEvidence"] }

    func retainForRestoreRecovery() throws {
        guard realm?.isInWriteTransaction == true else {
            throw BigSyncMutationJournalError.writeTransactionRequired
        }
        isAwaitingRecoveryEvidence = true
    }

    func admitAfterServerEvidence() throws {
        guard realm?.isInWriteTransaction == true else {
            throw BigSyncMutationJournalError.writeTransactionRequired
        }
        isAwaitingRecoveryEvidence = false
    }

    static func validateInboundSemanticDeletion(
        _ recordID: CKRecord.ID, existingObject: Object?
    ) throws {
        // Retaining local tombstones alone does not reject an incoming hard delete.
        throw RA1RevisionError.invalidPayload
    }

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
        // Copied bytes are not current authored authority. This mirrors W3's
        // restore admission, including equal-revision server re-admission.
        if existing.isAwaitingRecoveryEvidence { return .preferIncomingRecord }
        if incoming.revision > local.revision { return .preferIncomingRecord }
        if incoming.revision < local.revision { return .preferExistingObject }
        guard incoming == local else { throw RA1RevisionError.divergentEqualRevision }
        // Audit timestamps and CK system/transport fields do not change domain equality.
        return .preserveExistingObject
    }

    func validateOutboundSemanticObject(in realm: Realm) throws {
        guard !isAwaitingRecoveryEvidence else {
            throw BigSyncSemanticAdmissionUnavailable(entityType: Self.className())
        }
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
        for realm in adapter.realmProvider?.targetReaderRealms ?? [] {
            await realm.asyncRefresh()
        }
    }

    @discardableResult
    func author(_ value: RA1DomainValue, auditTime: TimeInterval = 1_000) async throws -> String {
        guard value.owner == journalIdentity.installationIdentifier else {
            throw RA1RevisionError.changedIdentity
        }
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
    func testNewerLocalRevisionAtFinalWriteOverridesSelectionSnapshot() async throws {
        for forceSave in [false, true] {
            let f = try await RA1RealmFixture.make()
            _ = try await f.adapter.saveChanges(in: [f.record(mark)], forceSave: true)
            await f.refresh()
            var value = undo
            value.revision = 44
            let newest = value
            f.adapter._testBeforeImportedRecordTargetWrite = {
                // Same audit timestamp: only the final domain comparison can win.
                _ = try await f.author(newest)
            }
            defer { f.adapter._testBeforeImportedRecordTargetWrite = nil }
            _ = try await f.adapter.saveChanges(in: [f.record(undo)], forceSave: forceSave)
            await f.refresh()
            XCTAssertEqual(try f.value(), newest)
            let upload = try await f.nextUpload()
            XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), newest)
            XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testHigherRemoteRevisionStillWinsAfterLocalJournalChangesDuringSelection() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.author(mark)
        let sent = try await f.nextUpload()
        var clock = mark
        clock.revision = 43
        clock.activeMicroseconds = 200
        let concurrent = clock
        var value = undo
        value.revision = 44
        value.activeMicroseconds = 250
        let newest = value
        f.adapter._testBeforeImportedRecordTargetWrite = {
            _ = try await f.author(concurrent, auditTime: 99_000)
        }
        defer { f.adapter._testBeforeImportedRecordTargetWrite = nil }
        _ = try await f.adapter.saveChanges(in: [f.record(newest, auditTime: 10)], forceSave: false)
        await f.refresh()
        let generation = try XCTUnwrap(f.generation())
        XCTAssertEqual(try f.value(), newest)
        try await f.acknowledge(sent)
        XCTAssertEqual(f.generation(), generation)
        let upload = try await f.nextUpload()
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(upload.record), newest)
    }

    @BigSyncBackgroundActor
    func testEqualRevisionDivergenceAtFinalWriteCannotBeHiddenByNewPendingWork() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.adapter.saveChanges(in: [f.record(mark)], forceSave: true)
        await f.refresh()
        var value = undo
        value.activeMicroseconds = 200
        let concurrent = value
        f.adapter._testBeforeImportedRecordTargetWrite = { _ = try await f.author(concurrent) }
        defer { f.adapter._testBeforeImportedRecordTargetWrite = nil }
        let results = try await f.adapter.saveChanges(in: [f.record(undo)], forceSave: false)
        await f.refresh()
        guard let result = results.first, case .quarantined = result.disposition else {
            return XCTFail("Final-write equality must be checked before pending-local preservation")
        }
        XCTAssertEqual(try f.value(), concurrent)
        XCTAssertNotNil(f.generation())
        XCTAssertEqual(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).count, 1)
    }

    @BigSyncBackgroundActor
    func testSecondIncomingWinnerJournalFailureRollsBackBothTargets() async throws {
        let f = try await RA1RealmFixture.make()
        var other = mark
        other.key = "article-a:page-1"
        let firstGeneration = try await f.author(mark)
        let secondGeneration = try await f.author(other)
        _ = try await f.prepared()
        var otherUndo = undo
        otherUndo.key = other.key
        let failure = RA1IdentityFailure(identity: f.journalIdentity, failsOnCall: 2)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [f.target.configuration],
            mutationJournalIdentityProvider: { failure.next() })
        do {
            _ = try await f.adapter.saveChanges(in: [f.record(undo), f.record(otherUndo)], forceSave: true)
            XCTFail("The second required journal failure must escape the whole target write")
        } catch BigSyncMutationJournalError.identityUnavailable { }
        XCTAssertEqual(failure.callCount, 2)
        await f.refresh()
        XCTAssertEqual(try f.value(), mark)
        XCTAssertEqual(try f.value(other.id), other)
        XCTAssertEqual(f.generation(), firstGeneration)
        XCTAssertEqual(f.generation(other.id), secondGeneration)
        XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        try await f.bind(f.journalIdentity, scope: f.scope)
        _ = try await f.adapter.saveChanges(in: [f.record(undo), f.record(otherUndo)], forceSave: true)
        await f.refresh()
        XCTAssertEqual(try f.value(), undo)
        XCTAssertEqual(try f.value(other.id), otherUndo)
        XCTAssertNotEqual(f.generation(), firstGeneration)
        XCTAssertNotEqual(f.generation(other.id), secondGeneration)
    }

    @BigSyncBackgroundActor
    func testTrackingFailureAfterTargetCommitPreservesWinnerForReplayAndOldAck() async throws {
        let f = try await RA1RealmFixture.make()
        _ = try await f.author(mark)
        let sent = try await f.nextUpload()
        f.adapter._testBeforeImportedRecordPersistenceWrite = { throw RA1RevisionError.unexpectedIO }
        defer { f.adapter._testBeforeImportedRecordPersistenceWrite = nil }
        do {
            _ = try await f.adapter.saveChanges(in: [f.record(undo)], forceSave: true)
            XCTFail("Expected the failure between the two physical Realm commits")
        } catch RA1RevisionError.unexpectedIO { }
        await f.refresh()
        let winnerGeneration = try XCTUnwrap(f.generation())
        XCTAssertEqual(try f.value(), undo, "Target committed; tracking failure is not a target rollback")
        XCTAssertNotEqual(winnerGeneration, sent.generation)
        f.adapter._testBeforeImportedRecordPersistenceWrite = nil
        try await f.acknowledge(sent)
        XCTAssertEqual(f.generation(), winnerGeneration)
        _ = try await f.adapter.saveChanges(in: [f.record(undo)], forceSave: false)
        await f.refresh()
        XCTAssertEqual(f.generation(), winnerGeneration, "Equal replay must not mint another domain edit")
        let retry = try await f.nextUpload()
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(retry.record), undo)
        try await f.acknowledge(retry)
        XCTAssertNil(f.generation())
    }

    @BigSyncBackgroundActor
    func testPayloadAheadOfForwardedGenerationStillDrainsNewerJournal() async throws {
        let f = try await RA1RealmFixture.make()
        let g1 = try await f.author(mark)
        _ = try await f.nextUpload()
        let g2 = try await f.author(undo)
        // No fixture forward here: exercise the real materialization/ack window.
        let prepared = try await f.adapter.preparedRecordsToUpload(limit: 1, restrictedToEntityType: nil)
        let retry = try XCTUnwrap(prepared.first)
        XCTAssertEqual(retry.generation, g1)
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(retry.record), undo)
        try await f.acknowledge(retry)
        XCTAssertEqual(f.generation(), g2)
        let remaining = try await f.adapter.preparedRecordsToUpload(limit: 1, restrictedToEntityType: nil)
        let final = try XCTUnwrap(remaining.first)
        XCTAssertEqual(final.generation, g2)
        XCTAssertEqual(try RA1OwnedRevisionObject.decode(final.record), undo)
        try await f.acknowledge(final)
        XCTAssertNil(f.generation())
    }

    @BigSyncBackgroundActor
    func testDivergentOwnEchoIsQuarantinedWithoutApplyingOrAcknowledgingIt() async throws {
        let f = try await RA1RealmFixture.make()
        let generation = try await f.author(undo)
        var divergent = undo
        divergent.title = "Different authored title at the same revision"
        let results = try await f.adapter.validateAuthoritativeOwnUploadRecords([f.record(divergent)])
        await f.refresh()
        guard let result = results.first, case .quarantined = result.disposition else {
            return XCTFail("Own-echo validation must not bypass domain equality")
        }
        XCTAssertEqual(try f.value(), undo)
        XCTAssertEqual(f.generation(), generation)
    }

    @BigSyncBackgroundActor
    func testIncomingHardDeletionCannotEraseAcknowledgedEmptyRevision() async throws {
        for deleted in [false, true] {
            let f = try await RA1RealmFixture.make()
            var value = undo
            value.deleted = deleted
            _ = try await f.author(value)
            let upload = try await f.nextUpload()
            try await f.acknowledge(upload)
            let results = try await f.adapter.deleteRecords(with: [upload.record.recordID])
            await f.refresh()
            guard let result = results.first, case .quarantined = result.disposition else {
                return XCTFail("Retained version requires model deletion admission as well as a live upsert")
            }
            XCTAssertEqual(try f.value(), value)
            XCTAssertNil(f.generation())
            _ = try await f.adapter.saveChanges(in: [f.record(mark)], forceSave: true)
            await f.refresh()
            let repair = try await f.nextUpload()
            XCTAssertEqual(try RA1OwnedRevisionObject.decode(repair.record), value)
        }
    }

    @BigSyncBackgroundActor
    func testBackupRestoreWithholdsCopiedRowsUntilActualServerImport() async throws {
        var newer = undo
        newer.revision = 44
        let serverValues: [RA1DomainValue?] = [nil, mark, undo, newer]
        for server in serverValues {
            let f = try await RA1RealmFixture.make()
            _ = try await f.author(undo)
            _ = try await f.nextUpload()
            let replacement = BigSyncMutationJournalIdentity(installationIdentifier: "restored-installation",
                replicaBindingGenerationIdentifier: String(repeating: "b", count: 64))
            try await f.bind(replacement, scope: f.scope)
            let epoch = 4_000_000_002
            try await f.adapter.prepareChangeFeedReset(accountScopeIdentifier: f.scope, epoch: epoch,
                                                       mode: .backupRestore)
            await f.refresh()
            let copy = try XCTUnwrap(f.target.object(ofType: RA1OwnedRevisionObject.self, forPrimaryKey: undo.id))
            XCTAssertTrue(copy.isAwaitingRecoveryEvidence)
            XCTAssertEqual(copy.domain, undo)
            XCTAssertNil(f.generation(), "Copied upload work is not a new installation's intent")
            try await f.adapter.beginChangeFeedServerBootstrap(accountScopeIdentifier: f.scope, epoch: epoch,
                                                               mode: .backupRestore)
            if let server {
                _ = try await f.adapter.validateAuthoritativeOwnUploadRecords([f.record(server)])
                await f.refresh()
                XCTAssertTrue(copy.isAwaitingRecoveryEvidence, "Validation-only echoes cannot admit copied bytes")
                _ = try await f.adapter.saveChanges(in: [f.record(server)], forceSave: true)
            }
            try await f.adapter.reconcileAfterChangeFeedServerBootstrap(accountScopeIdentifier: f.scope,
                                                                         epoch: epoch, mode: .backupRestore)
            await f.refresh()
            XCTAssertEqual(copy.isAwaitingRecoveryEvidence, server == nil)
            XCTAssertEqual(copy.domain, server ?? undo)
            XCTAssertEqual(copy.owner, "owner-a")
            XCTAssertNil(f.generation())
            let uploads = try await f.prepared()
            XCTAssertTrue(uploads.isEmpty, "Restore neither reauthors original ownership nor resurrects absent data")
        }
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
        // This helper owns a direct adapter drain, not a background full sync.
        // Mirror the enclosing production drain's ownership so normal journal
        // delegate wakeups coalesce instead of starting a second attempt that
        // the deliberately upload-only transport cannot service.
        let attemptID = sync.synchronizationAttemptID
        sync.syncing = true
        sync.synchronizationDrainIsActive = true
        defer {
            sync.cancelSynchronization()
            try? FileManager.default.removeItem(at: root)
        }
        // Seed an unchanged foreign value through inbound replication, then make
        // it a normal repair upload. Do not author owner-a with this random
        // installation's identity merely to arrange the transport fixture.
        _ = try await f.adapter.saveChanges(in: [f.record(local)], forceSave: true)
        var stale = local
        stale.revision -= 1
        _ = try await f.adapter.saveChanges(in: [f.record(stale)], forceSave: true)
        await f.refresh()
        _ = try await f.prepared()
        // Keep journal forwarding paused. An incoming preference can replace G1
        // with G2 in the target while the tracking cache still knows only G1.
        // Refresh values at that boundary without manually forwarding G2.
        f.adapter._testBeforeImportedRecordPersistenceWrite = { await f.refresh() }
        defer { f.adapter._testBeforeImportedRecordPersistenceWrite = nil }
        try await sync.synchronizeAdapter(f.adapter)
        await f.refresh()
        let uploads = await io.uploadedValues()
        XCTAssertEqual(sync.synchronizationAttemptID, attemptID,
                       "A journal wakeup must not replace the owned direct drain")
        XCTAssertTrue(sync.syncing)
        XCTAssertNil(sync.synchronizationTask)
        XCTAssertEqual(uploads.first, local)
        XCTAssertTrue((2...3).contains(uploads.count))
        XCTAssertTrue(uploads.dropFirst().allSatisfy { $0 == expected },
                      "Every retry must transport the selected complete domain value")
        XCTAssertEqual(try f.value(), expected)
        XCTAssertNil(f.generation(), "Retry must reach real generation acknowledgement")
        XCTAssertTrue(f.tracking.objects(BigSyncInboundSemanticQuarantine.self).isEmpty)
        let transport = try sync.outboundQuiescenceSnapshot()
        XCTAssertTrue(transport.outstandingSubmissions.isEmpty)
    }
}

// One conflict, then at most two saves of the selected value. G1 may acknowledge
// a retry before the replacement G2 reaches tracking; G2 must then drain too.
// This is not a CloudKit change-tag/CAS simulator (records have no server tags).
private actor RA1MutationScript {
    let conflict: CKRecord
    var uploads: [RA1DomainValue] = []
    init(conflict: CKRecord) { self.conflict = conflict }
    func modify(_ records: [CKRecord], deleting: [CKRecord.ID],
                savePolicy: CKModifyRecordsOperation.RecordSavePolicy, atomically: Bool) throws -> CloudKitRecordMutationResults {
        guard records.count == 1, deleting.isEmpty, !atomically,
              savePolicy == .ifServerRecordUnchanged, uploads.count < 3,
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

// Faults only the existing journal identity provider, never Realm rollback.
private final class RA1IdentityFailure: @unchecked Sendable {
    private let lock = NSLock()
    private let identity: BigSyncMutationJournalIdentity
    private let failsOnCall: Int
    private var calls = 0

    init(identity: BigSyncMutationJournalIdentity, failsOnCall: Int) {
        self.identity = identity
        self.failsOnCall = failsOnCall
    }

    var callCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return calls
    }

    func next() -> BigSyncMutationJournalIdentity? {
        lock.lock()
        defer { lock.unlock() }
        calls += 1
        return calls == failsOnCall ? nil : identity
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
