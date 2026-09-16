import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(RebaseRow)
private final class RebaseRow: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    @Persisted(primaryKey: true) var id = "row"
    @Persisted var remoteField: String?
    @Persisted var localField = "local-v0"
    @Persisted var members: MutableSet<String>
    @Persisted var order: List<String>
    @Persisted var scores: Map<String, Int>
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(RebaseArticle)
private final class RebaseArticle: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        .lifetimeBundle(lifetimeField: "epoch", independentFields: ["title"])
    }
    @Persisted(primaryKey: true) var id = "article"
    @Persisted var title = "base title"
    @Persisted var epoch = "E0"
    @Persisted var count = 7
    @Persisted var duration = 70.0
    @Persisted var finished = true
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(RebaseControl)
private final class RebaseControl: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        .lifetimeBundle(lifetimeField: "epoch", independentFields: [])
    }
    @Persisted(primaryKey: true) var id = "control"
    @Persisted var epoch = "E0"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

final class SyncRecordRebaseTests: XCTestCase {
    private enum Fault: Error { case beforeTrackingCommit }

    @BigSyncBackgroundActor
    private func fixture() async throws -> (RealmSwiftAdapter, Realm) {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "rebase-target-" + nonce
        target.objectTypes = [RebaseRow.self, RebaseArticle.self, RebaseControl.self,
                              BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        // Explicit in the red baseline so the old adapter can open the fixture.
        let exclusions = [BigSyncRecordBaseline.className()]
        BigSyncMutationPolicy(excludedClassNames: exclusions).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
            }
        )
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "rebase-tracking-" + nonce
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target], excludedClassNames: exclusions,
            recordZoneID: .init(zoneName: "rebase"),
            logger: Logger(label: "SyncRecordRebaseTests"), startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "account", replicaBindingGenerationIdentifier: "binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.rebase", databaseScope: .private
        )
        return (adapter, try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first))
    }

    private func record(_ adapter: RealmSwiftAdapter, type: Object.Type = RebaseRow.self,
                        id: String = "row", at time: TimeInterval = 10) -> CKRecord {
        let record = CKRecord(recordType: type.className(), recordID: .init(
            recordName: type.className() + "." + id, zoneID: adapter.recordZoneID
        ))
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["explicitlyModifiedAt"] = Date(timeIntervalSinceReferenceDate: time) as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        return record
    }

    private func row(_ adapter: RealmSwiftAdapter, remote: String? = "remote-v0",
                     local: String = "local-v0", at time: TimeInterval = 10) -> CKRecord {
        let value = record(adapter, at: time)
        value["remoteField"] = remote as CKRecordValue?
        value["localField"] = local as CKRecordValue
        return value
    }

    private func epochRecords(_ adapter: RealmSwiftAdapter, epoch: String = "E0",
                              count: Int = 7, at time: TimeInterval = 10) -> [CKRecord] {
        let article = record(adapter, type: RebaseArticle.self, id: "article", at: time)
        article["title"] = "base title" as CKRecordValue
        article["epoch"] = epoch as CKRecordValue
        article["count"] = count as CKRecordValue
        article["duration"] = Double(count * 10) as CKRecordValue
        article["finished"] = (count > 0) as CKRecordValue
        let control = record(adapter, type: RebaseControl.self, id: "control", at: time)
        control["epoch"] = epoch as CKRecordValue
        return [control, article]
    }

    @BigSyncBackgroundActor
    private func deliver(_ records: [CKRecord], to adapter: RealmSwiftAdapter,
                         force: Bool = false) async throws {
        _ = try await adapter.saveChanges(in: records, forceSave: force)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }

    @BigSyncBackgroundActor
    private func edit(_ realm: Realm, at time: TimeInterval = 20,
                      _ update: (RebaseRow) -> Void) throws -> RebaseRow {
        let value = try XCTUnwrap(realm.object(ofType: RebaseRow.self, forPrimaryKey: "row"))
        try realm.write {
            update(value)
            value.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: time))
        }
        return value
    }

    private func baseline(_ realm: Realm) throws -> BigSyncRecordBaseline {
        try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                                  forPrimaryKey: RebaseRow.className() + ".row"))
    }

    private func generation(_ realm: Realm) throws -> String {
        try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self,
                                  forPrimaryKey: RebaseRow.className() + ".row")?.generation)
    }

    @BigSyncBackgroundActor
    func testDisjointFieldsPreserveBothEditsAndOldAcknowledgementCannotEraseRebase() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.localField = "local-v1" }
        try await adapter.didFinishImport()
        let old = try await adapter.prepareUploadBatch(limit: 10)
        let oldGeneration = try generation(realm)
        try await deliver([row(adapter, remote: "remote-v1", at: 30)], to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1", "REBASING: untouched local field lost the incoming edit")
        XCTAssertEqual(value.localField, "local-v1")
        XCTAssertEqual(value.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 20), "Do not launder the local edit's clock")
        let current = try generation(realm)
        XCTAssertNotEqual(current, oldGeneration)
        let revision = try baseline(realm).revision
        try await deliver([row(adapter, remote: "remote-v1", at: 30)], to: adapter)
        XCTAssertEqual(try generation(realm), current, "Redelivery must be idempotent")
        XCTAssertEqual(try baseline(realm).revision, revision)
        try await adapter.acknowledgeUploadedRecords(old.records, from: old)
        XCTAssertEqual(try generation(realm), current)
        XCTAssertEqual(try baseline(realm).revision, revision)
        let next = try await adapter.prepareUploadBatch(limit: 10)
        let outgoing = try XCTUnwrap(next.records.first)
        XCTAssertEqual(outgoing["remoteField"] as? String, "remote-v1")
        XCTAssertEqual(outgoing["localField"] as? String, "local-v1")
        XCTAssertEqual(next.records.count, 1, "The baseline table must not become upload work")
    }

    @BigSyncBackgroundActor
    func testNewerRemoteSameFieldWinsInBothNormalAndConflictDelivery() async throws {
        for force in [false, true] {
            let (adapter, realm) = try await fixture()
            try await deliver([row(adapter)], to: adapter)
            let value = try edit(realm) { $0.remoteField = "client at 20" }
            try await deliver([row(adapter, remote: "server at 30", at: 30)], to: adapter, force: force)
            realm.refresh()
            XCTAssertEqual(value.remoteField, "server at 30", "REBASING: pending status incorrectly outranked authored time")
            XCTAssertEqual(value.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 30))
        }
    }

    @BigSyncBackgroundActor
    func testNewerLocalSameFieldKeepsItsClockAcrossDuplicateServerDelivery() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm, at: 40) { $0.remoteField = "client at 40" }
        for _ in 0..<2 {
            try await deliver([row(adapter, remote: "server at 30", at: 30)], to: adapter)
            realm.refresh()
            XCTAssertEqual(value.remoteField, "client at 40")
            XCTAssertEqual(value.explicitlyModifiedAt, Date(timeIntervalSinceReferenceDate: 40))
        }
    }

    @BigSyncBackgroundActor
    func testExplicitNilAndIndependentRemoteEditBothSurvive() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.remoteField = nil }
        try await deliver([row(adapter, local: "remote localField edit", at: 30)], to: adapter)
        realm.refresh()
        XCTAssertNil(value.remoteField)
        XCTAssertEqual(value.localField, "remote localField edit", "REBASING: a clear cannot swallow a disjoint remote change")
    }

    @BigSyncBackgroundActor
    func testAcceptedFirstUploadEstablishesBaseForSubsequentDisjointEdits() async throws {
        let (adapter, realm) = try await fixture()
        let value = RebaseRow()
        value.remoteField = "remote-v0"
        try realm.write {
            realm.add(value)
            value.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 10))
        }
        try await adapter.didFinishImport()
        let first = try await adapter.prepareUploadBatch(limit: 10)
        try await adapter.acknowledgeUploadedRecords(first.records, from: first)
        XCTAssertFalse(try baseline(realm).revision.isEmpty)
        _ = try edit(realm) { $0.localField = "local-v1" }
        try await deliver([row(adapter, remote: "remote-v1", at: 30)], to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1")
        XCTAssertEqual(value.localField, "local-v1")
    }

    @BigSyncBackgroundActor
    func testOldUploadReceiptAdvancesBaseWithoutAcknowledgingNewerTyping() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.remoteField = "sent remote-v1"; $0.localField = "local-v1" }
        try await adapter.didFinishImport()
        let sent = try await adapter.prepareUploadBatch(limit: 10)
        _ = try edit(realm, at: 40) { $0.localField = "local-v2" }
        try await adapter.didFinishImport()
        let newer = try generation(realm)
        try await adapter.acknowledgeUploadedRecords(sent.records, from: sent)
        XCTAssertEqual(try generation(realm), newer)
        try await deliver([row(adapter, remote: "server remote-v2", local: "local-v1", at: 30)], to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "server remote-v2", "REBASING: an already accepted field was incorrectly treated as a new local edit")
        XCTAssertEqual(value.localField, "local-v2")
    }

    @BigSyncBackgroundActor
    func testNewLocalMutationBetweenSelectionAndCommitStillRebasesCurrentValue() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.localField = "local-v1" }
        adapter._testBeforeImportedRecordTargetWrite = { @BigSyncBackgroundActor in
            adapter._testBeforeImportedRecordTargetWrite = nil
            _ = try self.edit(realm, at: 40) { $0.localField = "local-v2" }
        }
        try await deliver([row(adapter, remote: "remote-v1", at: 30)], to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1", "REBASING: final-write fencing must not discard the incoming independent field")
        XCTAssertEqual(value.localField, "local-v2")
    }

    @BigSyncBackgroundActor
    func testTargetAndBaseSurviveTrackingFailureAndRedeliveryTogether() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.localField = "local-v1" }
        adapter._testBeforeImportedRecordPersistenceWrite = { throw Fault.beforeTrackingCommit }
        do {
            _ = try await adapter.saveChanges(in: [row(adapter, remote: "remote-v1", at: 30)], forceSave: false)
            XCTFail("Expected injected failure")
        } catch Fault.beforeTrackingCommit {}
        adapter._testBeforeImportedRecordPersistenceWrite = nil
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1", "REBASING: target must commit the merged value")
        let base = try baseline(realm).revision
        let pending = try generation(realm)
        try await deliver([row(adapter, remote: "remote-v1", at: 30)], to: adapter)
        XCTAssertEqual(try baseline(realm).revision, base)
        XCTAssertEqual(try generation(realm), pending)
        XCTAssertEqual(value.localField, "local-v1")
    }

    @BigSyncBackgroundActor
    func testResetRebasesWholeLifetimeBundleWithoutLosingIndependentTitleInEitherOrder() async throws {
        for remoteFirst in [false, true] {
            let (adapter, realm) = try await fixture()
            try await deliver(epochRecords(adapter), to: adapter)
            let article = try XCTUnwrap(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article"))
            if remoteFirst { try await deliver(epochRecords(adapter, epoch: "E1", count: 0, at: 30), to: adapter) }
            try realm.write {
                article.title = "local title"
                article.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
            }
            if !remoteFirst { try await deliver(Array(epochRecords(adapter, epoch: "E1", count: 0, at: 30).reversed()), to: adapter) }
            realm.refresh()
            XCTAssertEqual(article.title, "local title")
            XCTAssertEqual(article.epoch, "E1", "REBASING: title-only intent must not pin the old lifetime")
            XCTAssertEqual(article.count, 0)
            XCTAssertEqual(article.duration, 0)
            XCTAssertFalse(article.finished)
            XCTAssertEqual(realm.object(ofType: RebaseControl.self, forPrimaryKey: "control")?.epoch, article.epoch)
        }
    }

    @BigSyncBackgroundActor
    func testRemoteResetDoesNotRelabelLocallyEditedOldEpochCounters() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(epochRecords(adapter), to: adapter)
        let article = try XCTUnwrap(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article"))
        try realm.write {
            article.title = "keep title"
            article.count = 100
            article.duration = 999
            article.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 40))
        }
        try await deliver(epochRecords(adapter, epoch: "E1", count: 0, at: 30), to: adapter)
        realm.refresh()
        XCTAssertEqual(article.epoch, "E1", "REBASING: lifetime transition must replace the coupled old state")
        XCTAssertEqual(article.count, 0)
        XCTAssertEqual(article.duration, 0)
        XCTAssertEqual(article.title, "keep title")
    }

    @BigSyncBackgroundActor
    func testConcurrentLifetimeResetsChooseSameBundleDespiteUnrelatedRecordClocks() async throws {
        for localEpoch in ["E1", "E2"] {
            let remoteEpoch = localEpoch == "E1" ? "E2" : "E1"
            let (adapter, realm) = try await fixture()
            try await deliver(epochRecords(adapter), to: adapter)
            let article = try XCTUnwrap(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article"))
            let control = try XCTUnwrap(realm.object(ofType: RebaseControl.self, forPrimaryKey: "control"))
            try realm.write {
                article.epoch = localEpoch
                article.count = localEpoch == "E2" ? 2 : 1
                article.duration = Double(article.count * 10)
                article.title = "independent local title"
                article.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 90))
                control.epoch = localEpoch
                control.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
            }
            try await deliver(epochRecords(adapter, epoch: remoteEpoch, count: remoteEpoch == "E2" ? 2 : 1, at: 50), to: adapter)
            realm.refresh()
            XCTAssertEqual(article.epoch, "E2", "REBASING: lifetime arbitration cannot use the title clock")
            XCTAssertEqual(control.epoch, "E2")
            XCTAssertEqual(article.count, 2)
            XCTAssertEqual(article.duration, 20)
            XCTAssertEqual(article.title, "independent local title")
        }
    }

    @BigSyncBackgroundActor
    func testPrimitiveCollectionsAreAtomicFieldsAndUnorderedSetEncodingIsEquivalent() async throws {
        let (adapter, realm) = try await fixture()
        let initial = row(adapter)
        initial["members"] = ["a", "b"] as CKRecordValue
        initial["order"] = ["a", "b"] as CKRecordValue
        try await deliver([initial], to: adapter)
        let value = try edit(realm) { $0.localField = "local-v1" }
        let changed = row(adapter, remote: "remote-v1", at: 30)
        changed["members"] = ["b", "a"] as CKRecordValue
        changed["order"] = ["b", "a"] as CKRecordValue
        try await deliver([changed], to: adapter)
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v1")
        XCTAssertEqual(value.localField, "local-v1")
        XCTAssertEqual(Set(value.members), ["a", "b"])
        XCTAssertEqual(Array(value.order), ["b", "a"])
    }
}


extension SyncRecordRebaseTests {
    @BigSyncBackgroundActor
    func testLateFirstUploadReceiptCannotInstallPreDeletionAncestor() async throws {
        let (adapter, realm) = try await fixture()
        let value = RebaseRow()
        value.remoteField = "before deletion"
        try realm.write {
            realm.add(value)
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let old = try await adapter.prepareUploadBatch(limit: 10)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try realm.write {
            value.isDeleted = false
            value.remoteField = "new lifetime"
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let generationBefore = try generation(realm)
        try await adapter.acknowledgeUploadedRecords(old.records, from: old)
        realm.refresh()
        let retained = realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: RebaseRow.className() + ".row")
        XCTAssertTrue(retained == nil || retained!.fields.isEmpty,
            "LIFECYCLE: an old first-upload reply must not restore pre-deletion comparison evidence")
        XCTAssertEqual(try generation(realm), generationBefore)
        XCTAssertEqual(value.remoteField, "new lifetime")
    }

    @BigSyncBackgroundActor
    func testLateReceiptCannotOverwriteNewerImportedComparisonBase() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.localField = "sent" }
        try await adapter.didFinishImport()
        let old = try await adapter.prepareUploadBatch(limit: 10)
        try await deliver([row(adapter, remote: "received", at: 30)], to: adapter)
        let revision = try baseline(realm).revision
        let latestGeneration = try generation(realm)
        try await adapter.acknowledgeUploadedRecords(old.records, from: old)
        XCTAssertEqual(try baseline(realm).revision, revision)
        XCTAssertEqual(try generation(realm), latestGeneration)
        XCTAssertEqual(value.remoteField, "received")
        XCTAssertEqual(value.localField, "sent")
    }

    @BigSyncBackgroundActor
    func testComparisonBaseRollsBackWithItsTargetTransaction() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let old = try baseline(realm)
        let revision = old.revision, fields = old.fieldDigests, namespace = old.namespace
        do {
            try realm.write {
                BigSyncRecordBaseline.install(recordName: old.recordName, namespace: namespace,
                    fields: ["different": Data([1])], in: realm)
                throw Fault.beforeTrackingCommit
            }
        } catch Fault.beforeTrackingCommit {}
        XCTAssertEqual(try baseline(realm).revision, revision)
        XCTAssertEqual(try baseline(realm).fieldDigests, fields)
    }

    @BigSyncBackgroundActor
    func testAcknowledgedArticleAndPendingControlCannotSplitLifetimeWinner() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(epochRecords(adapter), to: adapter)
        let article = try XCTUnwrap(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article"))
        let control = try XCTUnwrap(realm.object(ofType: RebaseControl.self, forPrimaryKey: "control"))
        let newer = "bsk1:0000000000000002:00000000-0000-0000-0000-000000000001"
        let older = "bsk1:0000000000000001:ffffffff-ffff-ffff-ffff-ffffffffffff"
        try realm.write {
            article.epoch = newer; article.count = 2; article.duration = 20
            article.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
            control.epoch = newer
            control.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 20))
        }
        try await adapter.didFinishImport()
        let prepared = try await adapter.prepareUploadBatch(limit: 10)
        let articleReceipt = prepared.records.filter { $0.recordType == RebaseArticle.className() }
        XCTAssertEqual(articleReceipt.count, 1)
        try await adapter.acknowledgeUploadedRecords(articleReceipt, from: prepared)
        try await deliver(epochRecords(adapter, epoch: older, count: 1, at: 90), to: adapter)
        realm.refresh()
        XCTAssertEqual(article.epoch, newer, "LIFETIME: no journal does not make an older accepted reset newer")
        XCTAssertEqual(control.epoch, newer)
        XCTAssertEqual(article.count, 2)
        XCTAssertEqual(article.duration, 20)
    }

    @BigSyncBackgroundActor
    func testDelayedOlderResetCannotRegressAcceptedLifetimeWithoutPendingWork() async throws {
        let (adapter, realm) = try await fixture()
        let newer = "bsk1:0000000000000002:00000000-0000-0000-0000-000000000001"
        let older = "bsk1:0000000000000001:ffffffff-ffff-ffff-ffff-ffffffffffff"
        try await deliver(epochRecords(adapter, epoch: newer, count: 2, at: 20), to: adapter)
        try await deliver(epochRecords(adapter, epoch: older, count: 1, at: 90), to: adapter)
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article")?.epoch,
            newer, "LIFETIME: replay must not reverse a later reset")
        XCTAssertEqual(realm.object(ofType: RebaseControl.self, forPrimaryKey: "control")?.epoch, newer)
        XCTAssertEqual(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article")?.count, 2)
    }

    @BigSyncBackgroundActor
    func testIncomingOrderedResetWinsOverOldLifetimeEditsWithoutTimestampAuthority() async throws {
        let (adapter, realm) = try await fixture()
        let newer = "bsk1:0000000000000001:00000000-0000-0000-0000-000000000001"
        try await deliver(epochRecords(adapter), to: adapter)
        let article = try XCTUnwrap(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article"))
        try realm.write {
            article.title = "keep this title"
            article.count = 999
            article.refreshChangeMetadata(explicitlyModified: true, at: Date(timeIntervalSinceReferenceDate: 900))
        }
        try await deliver(epochRecords(adapter, epoch: newer, count: 0, at: 20), to: adapter)
        realm.refresh()
        XCTAssertEqual(article.epoch, newer)
        XCTAssertEqual(article.count, 0)
        XCTAssertEqual(article.title, "keep this title")
    }

    @BigSyncBackgroundActor
    func testVersionedLifetimeRejectsMalformedReservedIdentifier() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver(epochRecords(adapter), to: adapter)
        do {
            try await deliver(epochRecords(adapter, epoch: "bsk1:malformed", at: 30), to: adapter)
            XCTFail("LIFETIME: reserved malformed versions must not enter comparison accounting")
        } catch is BigSyncRecordRebaseError {}
        realm.refresh()
        XCTAssertEqual(realm.object(ofType: RebaseArticle.self, forPrimaryKey: "article")?.epoch, "E0")
    }
}
