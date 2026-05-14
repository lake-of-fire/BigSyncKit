import XCTest
import CloudKit
import Logging
@testable import BigSyncKit
import RealmSwift

private final class DictionaryKeyValueStore: NSObject, KeyValueStore {
    private var storage = [String: Any]()

    func object(forKey defaultName: String) -> Any? { storage[defaultName] }
    func bool(forKey defaultName: String) -> Bool { storage[defaultName] as? Bool ?? false }
    func set(value: Any?, forKey defaultName: String) { storage[defaultName] = value }
    func set(boolValue: Bool, forKey defaultName: String) { storage[defaultName] = boolValue }
    func removeObject(forKey defaultName: String) { storage.removeValue(forKey: defaultName) }
}

private final class NoopAdapterProvider: NSObject, AdapterProvider {
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, modelAdapterForRecordZoneID zoneID: CKRecordZone.ID) -> ModelAdapter? { nil }
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID) async {}
}

private final class FakeCloudKitDatabase: NSObject, CloudKitDatabaseAdapter, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }

    func add(_ operation: CKDatabaseOperation) {
        if let modifyOperation = operation as? CKModifyRecordsOperation {
            let savedRecords = modifyOperation.recordsToSave ?? []
            let deletedRecordIDs = modifyOperation.recordIDsToDelete ?? []
            for record in savedRecords {
                modifyOperation.perRecordCompletionBlock?(record, nil)
            }
            modifyOperation.modifyRecordsCompletionBlock?(savedRecords, deletedRecordIDs, nil)
        }
    }

    func save(zone: CKRecordZone, completionHandler: @escaping (CKRecordZone?, Error?) -> Void) {
        completionHandler(zone, nil)
    }

    func fetch(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone?, Error?) -> Void) {
        completionHandler(CKRecordZone(zoneID: zoneID), nil)
    }

    func fetch(withRecordID recordID: CKRecord.ID, completionHandler: @escaping (CKRecord?, Error?) -> Void) {
        completionHandler(nil, nil)
    }

    func delete(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone.ID?, Error?) -> Void) {
        completionHandler(zoneID, nil)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func fetchAllSubscriptions(completionHandler: @escaping ([CKSubscription]?, Error?) -> Void) {
        completionHandler([], nil)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func save(subscription: CKSubscription, completionHandler: @escaping (CKSubscription?, Error?) -> Void) {
        completionHandler(subscription, nil)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func delete(withSubscriptionID subscriptionID: CKSubscription.ID, completionHandler: @escaping (String?, Error?) -> Void) {
        completionHandler(subscriptionID, nil)
    }
}

private final class FakeModelAdapter: NSObject, PrioritySyncCapableModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    let priorityEntityTypeNames: [String]
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server

    private(set) var events = [String]()
    private(set) var savedBatchSizes = [Int]()
    private var uploadedByEntity: [String: [CKRecord]]
    private var deletedByEntity: [String: [CKRecord.ID]]
    private var storedServerChangeToken: CKServerChangeToken?

    var hasChanges: Bool {
        uploadedByEntity.values.contains(where: { !$0.isEmpty }) ||
        deletedByEntity.values.contains(where: { !$0.isEmpty })
    }

    init(
        zoneID: CKRecordZone.ID,
        priorities: [String],
        uploadedByEntity: [String: [CKRecord]] = [:],
        deletedByEntity: [String: [CKRecord.ID]] = [:]
    ) {
        self.recordZoneID = zoneID
        self.priorityEntityTypeNames = priorities
        self.uploadedByEntity = uploadedByEntity
        self.deletedByEntity = deletedByEntity
    }

    func cleanUp() {}
    func resetSyncCaches() async throws {}
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { true }

    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {
        savedBatchSizes.append(records.count)
        let recordTypes = records.map { $0.recordType }.joined(separator: ",")
        events.append("save:\(recordTypes)")
    }

    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {
        let recordNames = recordIDs.map { $0.recordName }.joined(separator: ",")
        events.append("deleteRemote:\(recordNames)")
    }

    func persistImportedChanges() async throws {
        events.append("persist")
    }

    func recordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [CKRecord] {
        let target = restrictedToEntityType ?? nextEntityTypeWithPendingUploads()
        events.append("recordsToUpload:\(target ?? "*")")
        guard let target else { return [] }
        let allRecords = uploadedByEntity[target] ?? []
        let selectedRecords = Array(allRecords.prefix(limit))
        uploadedByEntity[target] = Array(allRecords.dropFirst(selectedRecords.count))
        return selectedRecords
    }

    func recordsToUpload(limit: Int) async throws -> [CKRecord] {
        try await recordsToUpload(limit: limit, restrictedToEntityType: nil)
    }

    func didUpload(savedRecords: [CKRecord]) async throws {
        let recordNames = savedRecords.map { $0.recordID.recordName }.joined(separator: ",")
        events.append("didUpload:\(recordNames)")
    }

    func recordIDsMarkedForDeletion(limit: Int, restrictedToEntityType: String?) async throws -> [CKRecord.ID] {
        let target = restrictedToEntityType ?? nextEntityTypeWithPendingDeletions()
        events.append("recordIDsMarkedForDeletion:\(target ?? "*")")
        guard let target else { return [] }
        let allRecordIDs = deletedByEntity[target] ?? []
        let selectedRecordIDs = Array(allRecordIDs.prefix(limit))
        deletedByEntity[target] = Array(allRecordIDs.dropFirst(selectedRecordIDs.count))
        return selectedRecordIDs
    }

    func recordIDsMarkedForDeletion(limit: Int) async throws -> [CKRecord.ID] {
        try await recordIDsMarkedForDeletion(limit: limit, restrictedToEntityType: nil)
    }

    func didDelete(recordIDs: [CKRecord.ID]) async {
        let recordNames = recordIDs.map { $0.recordName }.joined(separator: ",")
        events.append("didDelete:\(recordNames)")
    }

    var serverChangeToken: CKServerChangeToken? {
        get async { storedServerChangeToken }
    }

    func saveToken(_ token: CKServerChangeToken?) async {
        storedServerChangeToken = token
        events.append("saveToken")
    }

    func deleteChangeTracking(forRecordIDs: [CKRecord.ID]) async throws {}
    func didFinishImport() async {}
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}

    private func nextEntityTypeWithPendingUploads() -> String? {
        priorityEntityTypeNames.first(where: { !(uploadedByEntity[$0] ?? []).isEmpty }) ??
        uploadedByEntity.keys.sorted().first(where: { !(uploadedByEntity[$0] ?? []).isEmpty })
    }

    private func nextEntityTypeWithPendingDeletions() -> String? {
        priorityEntityTypeNames.first(where: { !(deletedByEntity[$0] ?? []).isEmpty }) ??
        deletedByEntity.keys.sorted().first(where: { !(deletedByEntity[$0] ?? []).isEmpty })
    }
}

@objc(BigSyncTrackedObject)
private final class BigSyncTrackedObject: Object, ChangeMetadataRecordable {
    @Persisted(primaryKey: true) var id: String
    @Persisted var createdAt: Date
    @Persisted var modifiedAt: Date
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false

    convenience init(id: String, createdAt: Date, modifiedAt: Date, explicitlyModifiedAt: Date?) {
        self.init()
        self.id = id
        self.createdAt = createdAt
        self.modifiedAt = modifiedAt
        self.explicitlyModifiedAt = explicitlyModifiedAt
    }
}

final class BigSyncKitTests: XCTestCase {
    @BigSyncBackgroundActor
    func testPrioritizedRemoteChangesAreProcessedInConfiguredOrderBeforeUnprioritized() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "priority-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(adapter)

        let processor = ChangeRequestProcessor.shared
        processor.clearErrors()
        processor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: makeRecord(type: "Article", id: "1", zoneID: zoneID), deletedRecordID: nil, adapter: adapter))
        processor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: nil, deletedRecordID: CKRecord.ID(recordName: "HistoryRecord.2", zoneID: zoneID), adapter: adapter))
        processor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: makeRecord(type: "Bookmark", id: "3", zoneID: zoneID), deletedRecordID: nil, adapter: adapter))

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertEqual(
            adapter.events.filter { $0.hasPrefix("save:") || $0.hasPrefix("deleteRemote:") },
            ["save:Bookmark", "deleteRemote:HistoryRecord.2", "save:Article"]
        )
    }

    @BigSyncBackgroundActor
    func testPriorityRemoteDeletionWinsBeforeLowerPriorityUploadAndUnprioritizedWork() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "deletion-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"],
            uploadedByEntity: [
                "HistoryRecord": [makeRecord(type: "HistoryRecord", id: "10", zoneID: zoneID)],
                "Article": [makeRecord(type: "Article", id: "20", zoneID: zoneID)],
            ]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(adapter)

        let processor = ChangeRequestProcessor.shared
        processor.clearErrors()
        processor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: nil, deletedRecordID: CKRecord.ID(recordName: "Bookmark.1", zoneID: zoneID), adapter: adapter))

        try await synchronizer.synchronizeAdapter(adapter)

        let deleteIndex = try XCTUnwrap(adapter.events.firstIndex(of: "deleteRemote:Bookmark.1"))
        let lowerPriorityUploadIndex = try XCTUnwrap(adapter.events.firstIndex(of: "recordsToUpload:HistoryRecord"))
        let unrestrictedUploadIndex = try XCTUnwrap(adapter.events.firstIndex(of: "recordsToUpload:Article"))
        XCTAssertLessThan(deleteIndex, lowerPriorityUploadIndex)
        XCTAssertLessThan(deleteIndex, unrestrictedUploadIndex)
    }

    @BigSyncBackgroundActor
    func testPrioritizedUploadsDrainOneEntityBeforeAdvancing() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "upload-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"],
            uploadedByEntity: [
                "Bookmark": [
                    makeRecord(type: "Bookmark", id: "1", zoneID: zoneID),
                    makeRecord(type: "Bookmark", id: "2", zoneID: zoneID),
                ],
                "HistoryRecord": [
                    makeRecord(type: "HistoryRecord", id: "3", zoneID: zoneID),
                    makeRecord(type: "HistoryRecord", id: "4", zoneID: zoneID),
                ],
            ]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.batchSize = 1
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        let uploads = adapter.events.filter { $0.hasPrefix("didUpload:") }
        XCTAssertEqual(uploads, [
            "didUpload:Bookmark.1",
            "didUpload:Bookmark.2",
            "didUpload:HistoryRecord.3,HistoryRecord.4",
        ])
    }

    @BigSyncBackgroundActor
    func testPrioritizedDeletionsDrainOneEntityBeforeAdvancing() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "delete-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"],
            deletedByEntity: [
                "Bookmark": [
                    CKRecord.ID(recordName: "Bookmark.1", zoneID: zoneID),
                    CKRecord.ID(recordName: "Bookmark.2", zoneID: zoneID),
                ],
                "HistoryRecord": [
                    CKRecord.ID(recordName: "HistoryRecord.3", zoneID: zoneID),
                    CKRecord.ID(recordName: "HistoryRecord.4", zoneID: zoneID),
                ],
            ]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.batchSize = 1
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        let deletions = adapter.events.filter { $0.hasPrefix("didDelete:") }
        XCTAssertEqual(deletions, [
            "didDelete:Bookmark.1,Bookmark.2",
            "didDelete:HistoryRecord.3,HistoryRecord.4",
        ])
    }

    @BigSyncBackgroundActor
    func testSkipsEmptyPriorityTypeAndProcessesNextAvailablePriority() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "next-priority-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(adapter)

        let processor = ChangeRequestProcessor.shared
        processor.clearErrors()
        processor.addFetchedChangeRequest(ChangeRequest(downloadedRecord: makeRecord(type: "HistoryRecord", id: "1", zoneID: zoneID), deletedRecordID: nil, adapter: adapter))

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertFalse(adapter.events.contains("save:Bookmark"))
        XCTAssertTrue(adapter.events.contains("save:HistoryRecord"))
    }

    @BigSyncBackgroundActor
    func testRecordsToUploadWrapperFallsBackToUnrestrictedBehaviorAfterPriorityWorkIsExhausted() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "wrapper-upload-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            uploadedByEntity: [
                "Article": [makeRecord(type: "Article", id: "1", zoneID: zoneID)],
            ]
        )
        let records = try await adapter.recordsToUpload(limit: 10)
        XCTAssertEqual(records.map(\.recordType), ["Article"])
        XCTAssertEqual(records.map(\.recordID.recordName), ["Article.1"])
    }

    @BigSyncBackgroundActor
    func testDeletionWrapperFallsBackToUnrestrictedBehaviorAfterPriorityWorkIsExhausted() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "wrapper-delete-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            deletedByEntity: [
                "Article": [CKRecord.ID(recordName: "Article.1", zoneID: zoneID)],
            ]
        )
        let recordIDs = try await adapter.recordIDsMarkedForDeletion(limit: 10)
        XCTAssertEqual(recordIDs.map(\.recordName), ["Article.1"])
    }

    @BigSyncBackgroundActor
    func testFetchedChangeProcessorUsesLargerBatchWithoutRepeatedHundredRecordSplits() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "batch-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(adapter)

        let processor = ChangeRequestProcessor.shared
        processor.clearErrors()
        processor.fetchedChangeBatchSize = 300

        for index in 0..<250 {
            processor.addFetchedChangeRequest(
                ChangeRequest(
                    downloadedRecord: makeRecord(type: "Article", id: "\(index)", zoneID: zoneID),
                    deletedRecordID: nil,
                    adapter: adapter
                )
            )
        }

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertEqual(adapter.savedBatchSizes, [250])
        XCTAssertEqual(adapter.events.filter { $0 == "persist" }.count, 1)
    }

    @BigSyncBackgroundActor
    func testEnqueuedLocalChangeAdvancesLastTrackedChangesAtFromProjectedTupleTimestamp() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let explicitDate = Date(timeIntervalSinceReferenceDate: 10_000)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(BigSyncTrackedObject(
                id: "local",
                createdAt: explicitDate,
                modifiedAt: explicitDate,
                explicitlyModifiedAt: explicitDate
            ))
        }

        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)

        let syncedEntityType = try XCTUnwrap(
            fixture.persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: BigSyncTrackedObject.className())
        )
        XCTAssertEqual(syncedEntityType.lastTrackedChangesAt, explicitDate)
        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".local"
            )
        )
    }

    @BigSyncBackgroundActor
    func testRemoteFetchedChangeStillAdvancesLastTrackedChangesAtWhenFilteredFromUploadQueue() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let explicitDate = Date(timeIntervalSinceReferenceDate: 20_000)
        let modifiedDate = Date(timeIntervalSinceReferenceDate: 20_001)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(BigSyncTrackedObject(
                id: "remote",
                createdAt: explicitDate,
                modifiedAt: modifiedDate,
                explicitlyModifiedAt: explicitDate
            ))
        }
        let storedModifiedDate = try XCTUnwrap(
            fixture.targetRealm.object(ofType: BigSyncTrackedObject.self, forPrimaryKey: "remote")?["modifiedAt"] as? Date
        )
        fixture.adapter._test_markRecentlyFetchedRecord(
            entityType: BigSyncTrackedObject.className(),
            identifier: "remote",
            modifiedAt: storedModifiedDate
        )

        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)

        let syncedEntityType = try XCTUnwrap(
            fixture.persistenceRealm.object(ofType: SyncedEntityType.self, forPrimaryKey: BigSyncTrackedObject.className())
        )
        XCTAssertEqual(syncedEntityType.lastTrackedChangesAt, explicitDate)
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".remote"
            )
        )
    }

    @BigSyncBackgroundActor
    private func makeSynchronizer() -> CloudKitSynchronizer {
        CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: FakeCloudKitDatabase(),
            adapterProvider: NoopAdapterProvider(),
            keyValueStore: DictionaryKeyValueStore(),
            logger: Logger(label: "BigSyncKitTests")
        )
    }

    private func makeRecord(type: String, id: String, zoneID: CKRecordZone.ID) -> CKRecord {
        CKRecord(recordType: type, recordID: CKRecord.ID(recordName: "\(type).\(id)", zoneID: zoneID))
    }

    @BigSyncBackgroundActor
    private func makeRealmAdapterFixture() async throws -> (
        adapter: RealmSwiftAdapter,
        persistenceRealm: Realm,
        targetRealm: Realm
    ) {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier = "persistence-\(identifier)"

        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "target-\(identifier)"
        targetConfiguration.objectTypes = [BigSyncTrackedObject.self]

        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "realm-adapter-zone", ownerName: CKCurrentUserDefaultName),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        adapter.invalidateTokens()

        let persistenceRealm = try await Realm.open(configuration: persistenceConfiguration)
        let targetRealm = try await Realm.open(configuration: targetConfiguration)
        return (adapter, persistenceRealm, targetRealm)
    }
}
