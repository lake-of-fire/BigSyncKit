import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

@objc(IncomingRepresentationSemanticRecord)
private final class IncomingRepresentationSemanticRecord: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding, BigSyncInboundSemanticRecordValidating,
    BigSyncInboundSemanticReplacementValidating {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: []),
        incomingRepresentation: .init(
            identity: "semantic-defaults-v1",
            fields: [
                "count": .compatibilityDefault(.integer(0)),
                "enabled": .compatibilityDefault(.boolean(false)),
                "isDeleted": .compatibilityDefault(.boolean(false)),
            ]
        )
    )

    @Persisted(primaryKey: true) var id = ""
    @Persisted var epoch = "E0"
    @Persisted var payload = "constructor-payload"
    @Persisted var count = 37
    @Persisted var enabled = true
    @Persisted var optionalText: String?
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false

    enum Failure: Error { case invalid }

    static func validateInboundSemanticRecord(_ record: CKRecord) throws {
        guard record["epoch"] as? String == "E0",
              let payload = record["payload"] as? String,
              ["accepted", "accepted-next"].contains(payload) else {
            throw Failure.invalid
        }
    }

    static func validateInboundSemanticReplacement(
        _ record: CKRecord,
        existingObject: Object?
    ) throws {
        _ = try inboundSemanticReplacementDisposition(
            record,
            existingObject: existingObject
        )
    }

    static func inboundSemanticReplacementDisposition(
        _ record: CKRecord,
        existingObject: Object?
    ) throws -> BigSyncInboundSemanticReplacementDisposition {
        try validateInboundSemanticRecord(record)
        guard let existing = existingObject as? Self else {
            return .applyIncomingRecord
        }
        guard record.recordID.recordName == className() + "." + existing.id else {
            throw Failure.invalid
        }
        return .preferIncomingRecord
    }
}

final class IncomingRepresentationSemanticRecordTests: XCTestCase {
    private struct Fixture {
        let adapter: RealmSwiftAdapter
        let realm: Realm
        let zone: CKRecordZone.ID

        var recordName: String {
            IncomingRepresentationSemanticRecord.className() + ".semantic"
        }
    }

    @BigSyncBackgroundActor
    private func fixture() async throws -> Fixture {
        let nonce = UUID().uuidString
        var target = Realm.Configuration()
        target.inMemoryIdentifier = "incoming-semantic-target-" + nonce
        target.objectTypes = [
            IncomingRepresentationSemanticRecord.self,
            BigSyncPendingMutation.self,
        ]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(
            configurations: [target],
            mutationJournalIdentityProvider: {
                .init(
                    installationIdentifier: "incoming-semantic-local",
                    replicaBindingGenerationIdentifier: "incoming-semantic-binding"
                )
            }
        )

        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.inMemoryIdentifier = "incoming-semantic-tracking-" + nonce
        let zone = CKRecordZone.ID(
            zoneName: "incoming-semantic-" + nonce,
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking,
            targetRealmConfigurations: [target],
            excludedClassNames: [],
            recordZoneID: zone,
            logger: Logger(label: "IncomingRepresentationSemanticRecordTests"),
            startSetupTask: false
        )
        adapter.mergePolicy = .custom
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()
        try await adapter.activateReplicaBinding(
            accountScopeIdentifier: "incoming-semantic-account",
            replicaBindingGenerationIdentifier: "incoming-semantic-binding"
        )
        try await adapter.activateTransportNamespace(
            containerIdentifier: "iCloud.test.incoming-semantic",
            databaseScope: .private
        )
        return Fixture(
            adapter: adapter,
            realm: try XCTUnwrap(
                adapter.realmProvider?.targetReaderRealmPerSchemaName[
                    IncomingRepresentationSemanticRecord.className()
                ]
            ),
            zone: zone
        )
    }

    private func record(_ fixture: Fixture, time: TimeInterval = 10) -> CKRecord {
        let record = CKRecord(
            recordType: IncomingRepresentationSemanticRecord.className(),
            recordID: .init(recordName: fixture.recordName, zoneID: fixture.zone)
        )
        record["epoch"] = "E0" as CKRecordValue
        record["payload"] = "accepted" as CKRecordValue
        let date = Date(timeIntervalSinceReferenceDate: time)
        record["createdAt"] = Date(timeIntervalSinceReferenceDate: 1) as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        // count, enabled, optionalText and isDeleted are intentionally omitted.
        return record
    }

    @BigSyncBackgroundActor
    private func deliver(
        _ record: CKRecord,
        to adapter: RealmSwiftAdapter
    ) async throws {
        _ = try await adapter.saveChanges(in: [record], forceSave: false)
        try await adapter.persistImportedChanges()
        try await adapter.didFinishImport()
    }

    @BigSyncBackgroundActor
    func testPreferIncomingAppliesDeclaredDefaultsToExistingManagedValues() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }
        let incoming = record(fixture)

        try await deliver(incoming, to: fixture.adapter)
        let object = try XCTUnwrap(
            fixture.realm.object(
                ofType: IncomingRepresentationSemanticRecord.self,
                forPrimaryKey: "semantic"
            )
        )
        let baseline = try XCTUnwrap(
            fixture.realm.object(
                ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: fixture.recordName
            )
        )

        try await fixture.realm.asyncWrite {
            object.count = 37
            object.enabled = true
        }
        XCTAssertTrue(fixture.realm.objects(BigSyncPendingMutation.self).isEmpty)

        try await deliver(record(fixture, time: 20), to: fixture.adapter)
        fixture.realm.refresh()

        XCTAssertEqual(object.count, 0)
        XCTAssertFalse(object.enabled)
        XCTAssertNil(object.optionalText)
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(
            baseline.fieldDigests,
            try BigSyncRecordFingerprint.fields(of: object)
        )
        let decoded = try fixture.adapter.decodedComparisonObject(
            incoming,
            type: IncomingRepresentationSemanticRecord.self
        )
        XCTAssertEqual(
            baseline.fieldDigests,
            try BigSyncRecordFingerprint.fields(of: decoded)
        )
        XCTAssertTrue(fixture.realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(fixture.realm.objects(BigSyncRecordSubmission.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testPreferIncomingAdvancesAcceptedBaseAndRotatesPendingGeneration() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }

        try await deliver(record(fixture), to: fixture.adapter)
        let object = try XCTUnwrap(
            fixture.realm.object(
                ofType: IncomingRepresentationSemanticRecord.self,
                forPrimaryKey: "semantic"
            )
        )
        let baseline = try XCTUnwrap(
            fixture.realm.object(
                ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: fixture.recordName
            )
        )
        let originalRevision = baseline.revision

        try await fixture.realm.asyncWrite {
            object.payload = "local-pending"
            object.count = 37
            object.enabled = true
            object.refreshChangeMetadata(explicitlyModified: true, at: Date())
        }
        let originalGeneration = try XCTUnwrap(
            fixture.realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation
        )

        let incoming = record(fixture, time: 20)
        incoming["payload"] = "accepted-next" as CKRecordValue
        try await deliver(incoming, to: fixture.adapter)
        fixture.realm.refresh()

        XCTAssertEqual(object.payload, "accepted-next")
        XCTAssertEqual(object.count, 0)
        XCTAssertFalse(object.enabled)
        XCTAssertNotEqual(baseline.revision, originalRevision)
        XCTAssertEqual(baseline.fieldDigests, try BigSyncRecordFingerprint.fields(of: object))
        let decoded = try fixture.adapter.decodedComparisonObject(
            incoming,
            type: IncomingRepresentationSemanticRecord.self
        )
        XCTAssertEqual(baseline.fieldDigests, try BigSyncRecordFingerprint.fields(of: decoded))
        XCTAssertNotEqual(
            fixture.realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation,
            originalGeneration
        )
    }

    @BigSyncBackgroundActor
    func testMalformedFieldRollsBackCompatibilityDefaultsInSemanticWrite() async throws {
        let fixture = try await fixture()
        defer { fixture.adapter.invalidateTokens() }

        try await deliver(record(fixture), to: fixture.adapter)
        let object = try XCTUnwrap(
            fixture.realm.object(
                ofType: IncomingRepresentationSemanticRecord.self,
                forPrimaryKey: "semantic"
            )
        )
        try await fixture.realm.asyncWrite {
            object.count = 37
            object.enabled = true
        }
        let baseline = try XCTUnwrap(
            fixture.realm.object(
                ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: fixture.recordName
            )
        )
        let revision = baseline.revision
        let fields = baseline.fieldDigests

        let malformed = record(fixture, time: 20)
        malformed["optionalText"] = 42 as CKRecordValue
        do {
            try await deliver(malformed, to: fixture.adapter)
            XCTFail("A malformed field must reject the semantic target transaction")
        } catch {
            XCTAssertTrue(error is RealmSwiftRemoteRecordDecodingError)
        }
        fixture.realm.refresh()

        XCTAssertEqual(object.count, 37)
        XCTAssertTrue(object.enabled)
        XCTAssertEqual(baseline.revision, revision)
        XCTAssertEqual(baseline.fieldDigests, fields)
        XCTAssertTrue(fixture.realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertTrue(fixture.realm.objects(BigSyncRecordSubmission.self).isEmpty)
    }
}
