import CloudKit
import Foundation
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(W1UploadSnapshotRow)
final class W1UploadSnapshotRow: Object, ChangeMetadataRecordable {
    override class func shouldIncludeInDefaultSchema() -> Bool { false }
    @Persisted(primaryKey: true) var id = "upload-snapshot"
    @Persisted var title = "committed"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 10)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@BigSyncBackgroundActor
private final class W1MissingUploadTargetSignal {
    private(set) var reached = false
    private var released = false
    private var waiter: CheckedContinuation<Void, Never>?

    func reach() {
        reached = true
        release()
    }

    func release() {
        released = true
        waiter?.resume()
        waiter = nil
    }

    func wait() async {
        guard !released else { return }
        await withCheckedContinuation { waiter = $0 }
    }
}

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    private func uploadSnapshotFixture() async throws
        -> (RealmSwiftAdapter, Realm, W1UploadSnapshotRow, String, String) {
        let (adapter, realm) = try await fixture()
        let row = W1UploadSnapshotRow()
        try realm.write {
            realm.add(row)
            row.refreshChangeMetadata(explicitlyModified: true, at: row.modifiedAt)
        }
        _ = try await adapter._test_forwardPendingMutations(in: realm)
        let name = W1UploadSnapshotRow.className() + "." + row.id
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                                   forPrimaryKey: name)?.generation)
        return (adapter, realm, row, name, generation)
    }

    @BigSyncBackgroundActor
    func testUncontractedUploadIgnoresProvisionalTargetPayloadAndDeletion() async throws {
        for removesTarget in [false, true] {
            for commits in [false, true] {
                let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
                realm.beginWrite()
                defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
                if removesTarget { realm.delete(row) }
                else {
                    row.title = "provisional"
                    row.refreshChangeMetadata(explicitlyModified: true,
                        at: Date(timeIntervalSinceReferenceDate: 20))
                }
                let selected = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(selected.count, 1)
                XCTAssertEqual(selected.first?.record["title"] as? String, "committed")
                XCTAssertEqual(selected.first?.generation, generation)
                XCTAssertTrue(realm.isInWriteTransaction, "Selection must preserve the independent owner")
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm?.object(
                    ofType: SyncedEntity.self, forPrimaryKey: name))
                XCTAssertEqual(tracking.entityState, .new,
                    "Provisional absence must not become a CloudKit deletion")
                if commits { try realm.commitWrite() } else { realm.cancelWrite() }
                let after = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                if removesTarget && commits {
                    XCTAssertTrue(after.isEmpty)
                    XCTAssertEqual(tracking.entityState, .deletedLocally)
                } else {
                    XCTAssertEqual(after.first?.record["title"] as? String,
                        commits ? "provisional" : "committed")
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testUncontractedUploadIgnoresProvisionalTrackingGenerationAndRemoval() async throws {
        for removesTracking in [false, true] {
            for commits in [false, true] {
                let (adapter, _, _, name, generation) = try await uploadSnapshotFixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let row = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if removesTracking { tracking.delete(row) }
                else { row.pendingGeneration = "provisional-generation" }
                let selected = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(selected.count, 1)
                XCTAssertEqual(selected.first?.generation, generation)
                XCTAssertEqual(selected.first?.record["title"] as? String, "committed")
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                let after = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                if removesTracking && commits { XCTAssertTrue(after.isEmpty) }
                else {
                    XCTAssertEqual(after.first?.generation,
                        commits ? "provisional-generation" : generation)
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testMissingUploadTargetSerializerDoesNotWriteTracking() async throws {
        let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
        try realm.write { realm.delete(row) }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        XCTAssertNil(try adapter.recordToUpload(syncedEntity: entity, isDummyRecord: false))
        XCTAssertTrue(tracking.isInWriteTransaction)
        XCTAssertEqual(entity.entityState, .new)
        XCTAssertEqual(entity.pendingGeneration, generation)
        tracking.cancelWrite()
        let selected = try await adapter.preparedRecordsToUpload(limit: 10,
            restrictedToEntityType: W1UploadSnapshotRow.className())
        XCTAssertTrue(selected.isEmpty)
        XCTAssertEqual(entity.entityState, .deletedLocally)
        XCTAssertEqual(entity.pendingGeneration, generation)
    }
    @BigSyncBackgroundActor
    func testMissingUploadTargetRechecksReappearanceAfterTrackingOwnershipWait() async throws {
        let (adapter, realm, row, name, generation) = try await uploadSnapshotFixture()
        try realm.write { realm.delete(row) }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        tracking.beginWrite()
        defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
        let signal = W1MissingUploadTargetSignal()
        adapter._testBeforeMissingUploadTargetTrackingWrite = { signal.reach() }
        defer { adapter._testBeforeMissingUploadTargetTrackingWrite = nil }
        let preparation = Task { @BigSyncBackgroundActor in
            defer { signal.release() }
            return try await adapter.preparedRecordsToUpload(limit: 10,
                restrictedToEntityType: W1UploadSnapshotRow.className())
        }
        await signal.wait()
        XCTAssertTrue(signal.reached, "The serializer must observe durable absence before the wait")
        XCTAssertTrue(tracking.isInWriteTransaction)
        let restored = W1UploadSnapshotRow()
        restored.title = "restored while tracking waited"
        try realm.write { realm.add(restored) }
        try tracking.commitWrite()
        let selected = try await preparation.value
        XCTAssertTrue(selected.isEmpty, "This selection began with an absent target")
        XCTAssertEqual(entity.entityState, .new, "A reappeared target must not become a deletion")
        XCTAssertEqual(entity.pendingGeneration, generation)
        let retry = try await adapter.preparedRecordsToUpload(limit: 10,
            restrictedToEntityType: W1UploadSnapshotRow.className())
        XCTAssertEqual(retry.first?.record["title"] as? String, restored.title)
    }

    @BigSyncBackgroundActor
    func testPhysicalDeletionIgnoresProvisionalTrackingStateAndInsertion() async throws {
        for insertsRow in [false, true] {
            for commits in [false, true] {
                let (adapter, _, _, name, generation) = try await uploadSnapshotFixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                if insertsRow {
                    try tracking.write {
                        tracking.delete(try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
                            forPrimaryKey: name)))
                    }
                }
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                let entity: SyncedEntity
                if insertsRow {
                    entity = SyncedEntity(entityType: W1UploadSnapshotRow.className(),
                        identifier: name, state: SyncedEntityState.deletedLocally.rawValue)
                    entity.setPendingMutation(generation: generation,
                        replicaBindingGenerationIdentifier: "w1-binding")
                    tracking.add(entity)
                } else {
                    entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self,
                        forPrimaryKey: name))
                    entity.entityState = .deletedLocally
                }
                let model: any ModelAdapter = adapter
                let during = try await model.preparedRecordDeletions(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertTrue(during.isEmpty, "A provisional delete must never become a transport request")
                let transport = W1ScriptedTransport()
                let provisionalRequest = try await transport.modifyRecords(saving: [],
                    deleting: during.map(\.recordID), savePolicy: .ifServerRecordUnchanged,
                    atomically: false)
                XCTAssertTrue(provisionalRequest.deleteResults.isEmpty)
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                let after = try await model.preparedRecordDeletions(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(after.count, commits ? 1 : 0)
                let committedRequest = try await transport.modifyRecords(saving: [],
                    deleting: after.map(\.recordID), savePolicy: .ifServerRecordUnchanged,
                    atomically: false)
                XCTAssertEqual(committedRequest.deleteResults.count, commits ? 1 : 0,
                    "The mutation fake must never receive a rollback-only deletion")
                if commits { XCTAssertEqual(after.first?.generation, generation) }
            }
        }
    }

    @BigSyncBackgroundActor
    func testPhysicalDeletionUsesCommittedGenerationDuringProvisionalReplacementOrRemoval() async throws {
        for removesRow in [false, true] {
            for commits in [false, true] {
                let (adapter, realm, target, name, generation) = try await uploadSnapshotFixture()
                try realm.write { realm.delete(target) }
                _ = try await adapter.preparedRecordsToUpload(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
                XCTAssertEqual(entity.entityState, .deletedLocally)
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if removesRow { tracking.delete(entity) }
                else { entity.pendingGeneration = "successor-deletion" }
                let model: any ModelAdapter = adapter
                let during = try await model.preparedRecordDeletions(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                XCTAssertEqual(during.first?.generation, generation)
                XCTAssertEqual(during.first?.recordID.recordName, name)
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                let after = try await model.preparedRecordDeletions(limit: 10,
                    restrictedToEntityType: W1UploadSnapshotRow.className())
                if removesRow && commits { XCTAssertTrue(after.isEmpty) }
                else {
                    XCTAssertEqual(after.first?.generation, commits ? "successor-deletion" : generation)
                    try await model.didDelete(recordIDs: during.map(\.recordID),
                        matchingGenerations: [name: generation])
                    let remaining = tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name)
                    if commits { XCTAssertEqual(remaining?.pendingGeneration, "successor-deletion") }
                    else {
                        XCTAssertEqual(remaining?.entityState, .deletedRemotely,
                            "The exact committed disappearance remains acknowledgeable")
                        XCTAssertNil(remaining?.pendingGeneration)
                    }
                }
            }
        }
    }

    @BigSyncBackgroundActor
    func testPhysicalDeletionIgnoresProvisionalTransportBinding() async throws {
        for commits in [false, true] {
            let (adapter, realm, target, name, generation) = try await uploadSnapshotFixture()
            try realm.write { realm.delete(target) }
            _ = try await adapter.preparedRecordsToUpload(limit: 10,
                restrictedToEntityType: W1UploadSnapshotRow.className())
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
            tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            entity.pendingReplicaBindingGenerationIdentifier = "foreign-binding"
            let during = try await adapter.preparedRecordDeletions(limit: 10,
                restrictedToEntityType: W1UploadSnapshotRow.className())
            XCTAssertEqual(during.first?.generation, generation)
            XCTAssertTrue(tracking.isInWriteTransaction)
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            let after = try await adapter.preparedRecordDeletions(limit: 10,
                restrictedToEntityType: W1UploadSnapshotRow.className())
            XCTAssertEqual(after.count, commits ? 0 : 1)
        }
    }

    /// Synthetic native CKRecord system fields, not signed cloud delivery.
    /// Verify both the SDK setter and the resulting archive round trip.
    @BigSyncBackgroundActor
    private func serverEvidenceRecord(
        entityType: String,
        recordName: String,
        zoneID: CKRecordZone.ID,
        tag: String = "evidence-accepted"
    ) throws -> CKRecord {
        let modifiedAt = Date(timeIntervalSinceReferenceDate: 30)
        let record = try tagged(CKRecord(
            recordType: entityType,
            recordID: .init(recordName: recordName, zoneID: zoneID)
        ), tag)
        let setter = NSSelectorFromString("setModificationDate:")
        guard record.responds(to: setter) else {
            throw NSError(domain: "W1NativeFixture", code: 2,
                userInfo: [NSLocalizedDescriptionKey:
                    "This CloudKit SDK cannot construct the server-date fixture"])
        }
        _ = record.perform(setter, with: modifiedAt as NSDate)
        XCTAssertEqual(record.modificationDate, modifiedAt)
        let decoded = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(record))
        XCTAssertEqual(decoded.recordChangeTag, tag)
        XCTAssertEqual(decoded.modificationDate, modifiedAt)
        return decoded
    }

    @BigSyncBackgroundActor
    private func semanticQuarantine(
        adapter: RealmSwiftAdapter
    ) -> BigSyncInboundSemanticQuarantine {
        let row = BigSyncInboundSemanticQuarantine()
        row.lineageID = "snapshot-quarantine"
        row.recordName = W1UploadSnapshotRow.className() + ".quarantined"
        row.entityType = W1UploadSnapshotRow.className()
        row.accountScopeIdentifier = "w1-account"
        row.semanticScopeIdentifier = "scope-a"
        row.containerIdentifier = "iCloud.test.w1-closeout"
        row.databaseScopeRawValue = CKDatabase.Scope.private.rawValue
        row.zoneOwnerName = adapter.recordZoneID.ownerName
        row.zoneName = adapter.recordZoneID.zoneName
        row.replicaActivationIdentifier = "w1-binding"
        row.changeFeedEpoch = 0
        row.validationCode = "snapshot-fixture"
        return row
    }

    @BigSyncBackgroundActor
    func testSemanticQuarantineIgnoresProvisionalInsertionAndRemoval() async throws {
        for initiallyPresent in [false, true] {
            for commits in [false, true] {
                let (adapter, _) = try await fixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let row = semanticQuarantine(adapter: adapter)
                if initiallyPresent { try tracking.write { tracking.add(row) } }
                func blocks(_ scope: String = "scope-a") throws -> Bool {
                    try adapter.hasInboundSemanticQuarantine(
                        entityType: W1UploadSnapshotRow.className(),
                        accountScopeIdentifier: "w1-account",
                        semanticScopeIdentifier: scope
                    )
                }
                XCTAssertEqual(try blocks(), initiallyPresent)
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if initiallyPresent { tracking.delete(row) }
                else { tracking.add(row) }
                XCTAssertEqual(try blocks(), initiallyPresent,
                    "A provisional quarantine change cannot grant or revoke durable admission")
                XCTAssertFalse(try blocks("scope-b"))
                XCTAssertFalse(try adapter.hasInboundSemanticQuarantine(
                    entityType: W1UploadSnapshotRow.className(),
                    accountScopeIdentifier: "another-account",
                    semanticScopeIdentifier: "scope-a"
                ))
                XCTAssertTrue(tracking.isInWriteTransaction)
                XCTAssertEqual(tracking.objects(BigSyncInboundSemanticQuarantine.self).count,
                    initiallyPresent ? 0 : 1, "Inspection must leave the owner's write intact")
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                XCTAssertEqual(try blocks(), commits ? !initiallyPresent : initiallyPresent)
            }
        }
    }

    @BigSyncBackgroundActor
    func testSemanticQuarantineUsesCommittedFeedEpoch() async throws {
        for commits in [false, true] {
            let (adapter, _) = try await fixture()
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let row = semanticQuarantine(adapter: adapter)
            row.semanticScopeIdentifier = nil
            let state = RebuildProvenanceState()
            state.accountScopeIdentifier = "w1-account"
            try tracking.write {
                tracking.add(row)
                tracking.add(state, update: .modified)
            }
            tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            state.epoch = 1
            XCTAssertTrue(try adapter.hasInboundSemanticQuarantine(
                entityType: W1UploadSnapshotRow.className(),
                accountScopeIdentifier: "w1-account",
                semanticScopeIdentifier: "any-scope"
            ), "A provisional feed reset must not hide a committed unscoped quarantine")
            XCTAssertTrue(tracking.isInWriteTransaction)
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            XCTAssertEqual(try adapter.hasInboundSemanticQuarantine(
                entityType: W1UploadSnapshotRow.className(),
                accountScopeIdentifier: "w1-account",
                semanticScopeIdentifier: "any-scope"
            ), !commits)
        }
    }

    @BigSyncBackgroundActor
    func testServerEvidenceIgnoresProvisionalAcknowledgement() async throws {
        for commits in [false, true] {
            let (adapter, realm, _, name, generation) = try await uploadSnapshotFixture()
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            let entity = try XCTUnwrap(tracking.object(
                ofType: SyncedEntity.self, forPrimaryKey: name
            ))
            let type = W1UploadSnapshotRow.className()
            let record = try serverEvidenceRecord(
                entityType: type, recordName: name, zoneID: adapter.recordZoneID
            )
            let expected = BigSyncServerRecordEvidence(
                recordName: name, entityType: type,
                recordChangeTag: "evidence-accepted",
                serverModifiedAt: Date(timeIntervalSinceReferenceDate: 30)
            )
            tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            try adapter.save(record: record, for: entity)
            entity.entityState = .synced
            entity.clearPendingMutation()
            XCTAssertNil(try adapter.serverRecordEvidence(
                recordName: name, expectedEntityType: type
            ))
            XCTAssertTrue(try adapter.serverRecordEvidence(entityTypes: [type]).isEmpty,
                "Uncommitted system fields must not prove current-account server membership")
            XCTAssertTrue(tracking.isInWriteTransaction)
            XCTAssertEqual(entity.entityState, .synced)
            XCTAssertNil(entity.pendingGeneration)
            XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: name)?.generation, generation)
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            XCTAssertEqual(try adapter.serverRecordEvidence(
                recordName: name, expectedEntityType: type
            ), commits ? expected : nil)
            XCTAssertEqual(try adapter.serverRecordEvidence(entityTypes: [type]),
                commits ? [expected] : [])
        }
    }

    @BigSyncBackgroundActor
    func testServerEvidenceIgnoresProvisionalRemovalAndForeignZoneReplacement() async throws {
        for removesRow in [false, true] {
            for commits in [false, true] {
                let (adapter, _, _, name, _) = try await uploadSnapshotFixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let entity = try XCTUnwrap(tracking.object(
                    ofType: SyncedEntity.self, forPrimaryKey: name
                ))
                let type = W1UploadSnapshotRow.className()
                let accepted = try serverEvidenceRecord(
                    entityType: type, recordName: name, zoneID: adapter.recordZoneID
                )
                let foreign = try serverEvidenceRecord(
                    entityType: type, recordName: name,
                    zoneID: .init(zoneName: "foreign-zone"), tag: "foreign-zone-tag"
                )
                try tracking.write {
                    try adapter.save(record: accepted, for: entity)
                    entity.entityState = .synced
                    entity.clearPendingMutation()
                }
                let expected = BigSyncServerRecordEvidence(
                    recordName: name, entityType: type,
                    recordChangeTag: "evidence-accepted",
                    serverModifiedAt: Date(timeIntervalSinceReferenceDate: 30)
                )
                XCTAssertEqual(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ), expected)
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if removesRow { tracking.delete(entity) }
                else { try adapter.save(record: foreign, for: entity) }
                XCTAssertEqual(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ), expected)
                XCTAssertEqual(try adapter.serverRecordEvidence(entityTypes: [type]), [expected])
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                XCTAssertEqual(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ), commits ? nil : expected)
                XCTAssertEqual(try adapter.serverRecordEvidence(entityTypes: [type]),
                    commits ? [] : [expected],
                    "A committed cache from another zone must not prove membership in this zone")
            }
        }
    }

    @BigSyncBackgroundActor
    func testServerEvidencePreservesExactAndCatalogStatePolicies() async throws {
        let (adapter, _, _, name, _) = try await uploadSnapshotFixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let entity = try XCTUnwrap(tracking.object(ofType: SyncedEntity.self, forPrimaryKey: name))
        let type = W1UploadSnapshotRow.className()
        let accepted = try serverEvidenceRecord(
            entityType: type, recordName: name, zoneID: adapter.recordZoneID
        )
        let expected = BigSyncServerRecordEvidence(
            recordName: name, entityType: type,
            recordChangeTag: "evidence-accepted",
            serverModifiedAt: Date(timeIntervalSinceReferenceDate: 30)
        )
        let states: [SyncedEntityState] = [
            .new, .synced, .changed, .deletedLocally, .deletedRemotely,
            .recreatingRemotely, .awaitingServerEvidence
        ]
        for state in states {
            for hasPendingGeneration in [false, true] {
                try tracking.write {
                    try adapter.save(record: accepted, for: entity)
                    entity.entityState = state
                    if hasPendingGeneration {
                        entity.setPendingMutation(generation: "pending-proof",
                            replicaBindingGenerationIdentifier: "w1-binding")
                    } else { entity.clearPendingMutation() }
                }
                XCTAssertEqual(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ), state == .synced && !hasPendingGeneration ? expected : nil)
                let membershipStates: [SyncedEntityState] = [.synced, .changed, .deletedLocally]
                XCTAssertEqual(try adapter.serverRecordEvidence(entityTypes: [type]),
                    membershipStates.contains(state) ? [expected] : [])
            }
        }
        XCTAssertNil(try adapter.serverRecordEvidence(
            recordName: name, expectedEntityType: W1RetainedArticle.className()
        ))
        XCTAssertTrue(try adapter.serverRecordEvidence(entityTypes: []).isEmpty)
    }

    @BigSyncBackgroundActor
    func testServerEvidenceUsesCommittedAccountScopeAcrossSharedTargetRealm() async throws {
        for commits in [false, true] {
            let types = [W1UploadSnapshotRow.className(), W1RetainedArticle.className()]
            let (adapter, realm) = try await fixture(
                accountScopePropertyByClassName: Dictionary(
                    uniqueKeysWithValues: types.map { ($0, "title") }
                )
            )
            let row = W1UploadSnapshotRow()
            row.title = "w1-account"
            let article = W1RetainedArticle()
            article.title = "w1-account"
            try realm.write {
                realm.add([row, article])
                row.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: 10))
                article.refreshChangeMetadata(explicitlyModified: true,
                    at: Date(timeIntervalSinceReferenceDate: 10))
            }
            _ = try await adapter._test_forwardPendingMutations(in: realm)
            let names = [types[0] + "." + row.id, types[1] + "." + article.id]
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            for (type, name) in zip(types, names) {
                let entity = try XCTUnwrap(tracking.object(
                    ofType: SyncedEntity.self, forPrimaryKey: name
                ))
                let record = try serverEvidenceRecord(
                    entityType: type, recordName: name, zoneID: adapter.recordZoneID
                )
                try tracking.write {
                    try adapter.save(record: record, for: entity)
                    entity.entityState = .synced
                    entity.clearPendingMutation()
                }
            }
            let expectedNames = Set(names)
            var originalGenerations = [String: String]()
            for name in names {
                let pending = try XCTUnwrap(realm.object(
                    ofType: BigSyncPendingMutation.self, forPrimaryKey: name
                ))
                XCTAssertEqual(pending.accountScopeIdentifier, "w1-account")
                originalGenerations[name] = pending.generation
            }
            func assertOriginalJournalIsUnchanged() throws {
                for name in names {
                    let pending = try XCTUnwrap(realm.object(
                        ofType: BigSyncPendingMutation.self, forPrimaryKey: name
                    ))
                    XCTAssertEqual(pending.accountScopeIdentifier, "w1-account")
                    XCTAssertEqual(pending.generation, originalGenerations[name])
                }
            }
            XCTAssertEqual(Set(try adapter.serverRecordEvidence(
                entityTypes: Set(types)
            ).map(\.recordName)), expectedNames)
            realm.beginWrite()
            defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
            row.title = "another-account"
            article.title = "another-account"
            // Deliberately model malformed provisional storage without invoking
            // the public mutation hook, which rejects immutable scope changes.
            try assertOriginalJournalIsUnchanged()
            for (type, name) in zip(types, names) {
                XCTAssertNotNil(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ))
            }
            XCTAssertEqual(Set(try adapter.serverRecordEvidence(
                entityTypes: Set(types)
            ).map(\.recordName)), expectedNames)
            try assertOriginalJournalIsUnchanged()
            XCTAssertTrue(realm.isInWriteTransaction)
            XCTAssertEqual(row.title, "another-account")
            XCTAssertEqual(article.title, "another-account")
            if commits { try realm.commitWrite() } else { realm.cancelWrite() }
            try assertOriginalJournalIsUnchanged()
            XCTAssertEqual(Set(try adapter.serverRecordEvidence(
                entityTypes: Set(types)
            ).map(\.recordName)), commits ? [] : expectedNames)
            for (type, name) in zip(types, names) {
                XCTAssertEqual(try adapter.serverRecordEvidence(
                    recordName: name, expectedEntityType: type
                ) != nil, !commits)
            }
            try assertOriginalJournalIsUnchanged()
        }
    }

    @BigSyncBackgroundActor
    func testBootstrapServerEvidenceIgnoresProvisionalTrackingMembership() async throws {
        for initiallyEstablished in [false, true] {
            for commits in [false, true] {
                let (adapter, _) = try await fixture()
                let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
                let entity = SyncedEntity(
                    entityType: W1UploadSnapshotRow.className(),
                    identifier: W1UploadSnapshotRow.className() + ".bootstrap",
                    state: (initiallyEstablished
                        ? SyncedEntityState.synced : SyncedEntityState.new).rawValue
                )
                try tracking.write { tracking.add(entity) }
                let before = try await adapter.hasChangeFeedEstablishedServerEvidence()
                XCTAssertEqual(before, initiallyEstablished)
                tracking.beginWrite()
                defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
                if initiallyEstablished { tracking.delete(entity) }
                else { entity.entityState = .synced }
                let during = try await adapter.hasChangeFeedEstablishedServerEvidence()
                XCTAssertEqual(during, initiallyEstablished)
                XCTAssertTrue(tracking.isInWriteTransaction)
                if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
                let after = try await adapter.hasChangeFeedEstablishedServerEvidence()
                XCTAssertEqual(after, commits ? !initiallyEstablished : initiallyEstablished)
            }
        }
    }

}
