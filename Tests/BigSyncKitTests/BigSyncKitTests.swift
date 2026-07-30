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

private struct FakeZoneChangePage {
    let zoneID: CKRecordZone.ID
    let records: [CKRecord]
    let deletedRecordIDs: [CKRecord.ID]
    let moreComing: Bool
}

private final class FakeCloudKitDatabase: NSObject, CloudKitDatabaseAdapter, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    var deleteZoneError: Error?
    var completesModifyOperations = true
    var reportsDeletedRecordsAsUnknownItems = false
    var partialSaveErrorsByRecordID = [CKRecord.ID: NSError]()
    var accountIdentifier = "test-account"
    var accountIdentifierAfterNextZoneDeletion: String?
    var zoneChangePages = [FakeZoneChangePage]()
    private(set) var deletedZoneIDs = [CKRecordZone.ID]()
    private(set) var savedSubscriptionCount = 0
    private(set) var modifySubscriptionOperationCount = 0
    private(set) var fetchZoneChangesOperationCount = 0
    private var records = [CKRecord.ID: CKRecord]()
    private var conditionallyFetchedRecordIDs = Set<CKRecord.ID>()

    func add(_ operation: CKDatabaseOperation) {
        if let fetchOperation =
            operation as? CKFetchRecordZoneChangesOperation,
           !zoneChangePages.isEmpty {
            fetchZoneChangesOperationCount += 1
            let page = zoneChangePages.removeFirst()
            for record in page.records {
                fetchOperation.recordChangedBlock?(record)
            }
            for recordID in page.deletedRecordIDs {
                fetchOperation.recordWithIDWasDeletedBlock?(
                    recordID,
                    recordID.recordName.split(separator: ".", maxSplits: 1)
                        .first.map(String.init) ?? "Record"
                )
            }
            fetchOperation.recordZoneFetchCompletionBlock?(
                page.zoneID,
                nil,
                nil,
                page.moreComing,
                nil
            )
            fetchOperation.fetchRecordZoneChangesCompletionBlock?(nil)
            return
        }
        if operation is CKModifySubscriptionsOperation {
            modifySubscriptionOperationCount += 1
        }
        if let modifyOperation = operation as? CKModifyRecordsOperation {
            guard completesModifyOperations else { return }
            let savedRecords = modifyOperation.recordsToSave ?? []
            let deletedRecordIDs = modifyOperation.recordIDsToDelete ?? []
            let partialSaveErrors = partialSaveErrorsByRecordID.filter {
                savedRecords.map(\.recordID).contains($0.key)
            }
            if !partialSaveErrors.isEmpty {
                var successfullySaved = [CKRecord]()
                for record in savedRecords {
                    if let error = partialSaveErrors[record.recordID] {
                        modifyOperation.perRecordCompletionBlock?(record, error)
                    } else {
                        records[record.recordID] = record
                        successfullySaved.append(record)
                        modifyOperation.perRecordCompletionBlock?(record, nil)
                    }
                }
                modifyOperation.modifyRecordsCompletionBlock?(
                    successfullySaved,
                    [],
                    CKError(
                        .partialFailure,
                        userInfo: [CKPartialErrorsByItemIDKey: partialSaveErrors]
                    )
                )
                return
            }
            if reportsDeletedRecordsAsUnknownItems, !deletedRecordIDs.isEmpty {
                let partialErrors = Dictionary(
                    uniqueKeysWithValues: deletedRecordIDs.map {
                        (
                            $0,
                            NSError(
                                domain: CKErrorDomain,
                                code: CKError.unknownItem.rawValue
                            )
                        )
                    }
                )
                modifyOperation.modifyRecordsCompletionBlock?(
                    [],
                    [],
                    CKError(
                        .partialFailure,
                        userInfo: [CKPartialErrorsByItemIDKey: partialErrors]
                    )
                )
                return
            }
            if modifyOperation.savePolicy == .ifServerRecordUnchanged,
               let duplicateRecord = savedRecords.first(where: {
                   records[$0.recordID] != nil
                    && !conditionallyFetchedRecordIDs.contains($0.recordID)
               }) {
                let itemError = NSError(
                    domain: CKErrorDomain,
                    code: CKError.serverRecordChanged.rawValue
                )
                modifyOperation.modifyRecordsCompletionBlock?(
                    [],
                    [],
                    CKError(
                        .partialFailure,
                        userInfo: [
                            CKPartialErrorsByItemIDKey: [
                                duplicateRecord.recordID: itemError
                            ]
                        ]
                    )
                )
                return
            }
            for record in savedRecords {
                records[record.recordID] = record
                conditionallyFetchedRecordIDs.remove(record.recordID)
                modifyOperation.perRecordCompletionBlock?(record, nil)
            }
            for recordID in deletedRecordIDs {
                records.removeValue(forKey: recordID)
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
        if records[recordID] != nil {
            conditionallyFetchedRecordIDs.insert(recordID)
        }
        completionHandler(records[recordID], nil)
    }

    func setDate(_ date: Date, field: String, for recordID: CKRecord.ID) {
        records[recordID]?[field] = date as CKRecordValue
    }

    func seed(_ record: CKRecord) {
        records[record.recordID] = record
    }

    func delete(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone.ID?, Error?) -> Void) {
        deletedZoneIDs.append(zoneID)
        if let nextAccountIdentifier = accountIdentifierAfterNextZoneDeletion {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextZoneDeletion = nil
        }
        completionHandler(deleteZoneError == nil ? zoneID : nil, deleteZoneError)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func fetchAllSubscriptions(completionHandler: @escaping ([CKSubscription]?, Error?) -> Void) {
        completionHandler([], nil)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func save(subscription: CKSubscription, completionHandler: @escaping (CKSubscription?, Error?) -> Void) {
        savedSubscriptionCount += 1
        completionHandler(subscription, nil)
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func delete(withSubscriptionID subscriptionID: CKSubscription.ID, completionHandler: @escaping (String?, Error?) -> Void) {
        completionHandler(subscriptionID, nil)
    }
}

private final class FakeModelAdapterDelegate: ModelAdapterDelegate {
    private(set) var initialSetupCount = 0
    private(set) var uploadWakeupCount = 0

    func needsInitialSetup() async throws {
        initialSetupCount += 1
    }

    func hasChangesToUpload() async {
        uploadWakeupCount += 1
    }
}

private actor AsyncGate {
    private var continuation: CheckedContinuation<Void, Never>?

    func wait() async {
        await withCheckedContinuation {
            continuation = $0
        }
    }

    func open() {
        continuation?.resume()
        continuation = nil
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
    var didFinishImportHandler: (@Sendable () async -> Void)?

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

    func cleanUp() async throws {}
    func resetSyncCaches() async throws {
        events.append("resetSyncCaches")
    }
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

    func saveToken(_ token: CKServerChangeToken?) async throws {
        storedServerChangeToken = token
        events.append("saveToken")
    }

    func deleteChangeTracking(forRecordIDs: [CKRecord.ID]) async throws {
        let recordNames = forRecordIDs.map(\.recordName).joined(separator: ",")
        events.append("deleteTracking:\(recordNames)")
    }
    func didFinishImport() async {
        await didFinishImportHandler?()
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {
        events.append("unsetCancellation")
    }

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
    @Persisted var tags: List<String>
    @Persisted var urls: List<URL>
    @Persisted var scores: MutableSet<Int>
    @Persisted var attributes: Map<String, String>

    convenience init(id: String, createdAt: Date, modifiedAt: Date, explicitlyModifiedAt: Date?) {
        self.init()
        self.id = id
        self.createdAt = createdAt
        self.modifiedAt = modifiedAt
        self.explicitlyModifiedAt = explicitlyModifiedAt
    }
}

@objc(BigSyncRelationshipChild)
private final class BigSyncRelationshipChild: Object, ChangeMetadataRecordable,
SoftDeletable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(BigSyncRelationshipParent)
private final class BigSyncRelationshipParent: Object, ChangeMetadataRecordable,
SoftDeletable {
    @Persisted(primaryKey: true) var id = ""
    @Persisted var createdAt = Date()
    @Persisted var modifiedAt = Date()
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var children: List<BigSyncRelationshipChild>
    @Persisted var relatedChildren: MutableSet<BigSyncRelationshipChild>
    @Persisted var favoriteChild: BigSyncRelationshipChild?
}

final class BigSyncKitTests: XCTestCase {
    func testEarlyMutationTrackingInstallationJournalsBeforeAdapterSetup() throws {
        var configuration = Realm.Configuration()
        configuration.inMemoryIdentifier = "early-tracking-\(UUID().uuidString)"
        configuration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        BigSyncMutationTracking.install(
            configurations: [configuration],
            excludedClassNames: []
        )
        let realm = try Realm(configuration: configuration)
        let object = BigSyncTrackedObject(
            id: "created-before-adapter",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )

        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }

        let recordName = BigSyncTrackedObject.className()
            + ".created-before-adapter"
        XCTAssertNotNil(
            realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
    }

    func testCloudKitRecordNameValidationRejectsInvalidIdentifiers() throws {
        XCTAssertNoThrow(
            try RealmSwiftAdapter.validateCloudKitRecordName(
                String(repeating: "a", count: 255)
            )
        )
        XCTAssertThrowsError(
            try RealmSwiftAdapter.validateCloudKitRecordName("")
        ) {
            XCTAssertEqual($0 as? BigSyncCloudKitRecordNameError, .empty)
        }
        XCTAssertThrowsError(
            try RealmSwiftAdapter.validateCloudKitRecordName("_reserved")
        ) {
            XCTAssertEqual(
                $0 as? BigSyncCloudKitRecordNameError,
                .reservedPrefix
            )
        }
        XCTAssertThrowsError(
            try RealmSwiftAdapter.validateCloudKitRecordName("日本語")
        )
        XCTAssertThrowsError(
            try RealmSwiftAdapter.validateCloudKitRecordName(
                String(repeating: "a", count: 256)
            )
        )
    }

    func testPersistentAssetFilePrefixDoesNotExposeRecordNameAsAPath() {
        let recordName = "MediaTranscript.https://example.com/a/b?x=1"
        let prefix = PersistentAssetManager.fileNamePrefix(
            forRecordID: recordName
        )

        XCTAssertTrue(prefix.hasPrefix("record-"))
        XCTAssertFalse(prefix.contains(recordName))
        XCTAssertFalse(prefix.contains("/"))
        XCTAssertEqual(prefix.count, "record-".count + 64)
    }

    func testTrackingRealmPathIsNamespacedBeyondTheZoneName() {
        let zoneID = CKRecordZone.ID(
            zoneName: "shared-zone-name",
            ownerName: CKCurrentUserDefaultName
        )
        let first = DefaultRealmSwiftAdapterProvider.realmPath(
            appGroup: nil,
            zoneID: zoneID,
            persistenceNamespace: "container-a|sync-a|private"
        )
        let second = DefaultRealmSwiftAdapterProvider.realmPath(
            appGroup: nil,
            zoneID: zoneID,
            persistenceNamespace: "container-b|sync-a|private"
        )

        XCTAssertNotEqual(first, second)
        XCTAssertTrue(first.hasSuffix("-shared-zone-name.realm"))
        XCTAssertFalse(first.contains("container-a"))
    }

    @BigSyncBackgroundActor
    func testZoneChangesAreDeliveredAndCommittedOnePageAtATime() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "paged-zone",
            ownerName: CKCurrentUserDefaultName
        )
        database.zoneChangePages = [
            FakeZoneChangePage(
                zoneID: zoneID,
                records: [makeRecord(type: "Item", id: "first", zoneID: zoneID)],
                deletedRecordIDs: [],
                moreComing: true
            ),
            FakeZoneChangePage(
                zoneID: zoneID,
                records: [],
                deletedRecordIDs: [],
                moreComing: false
            ),
        ]
        var deliveredPages = [[String]]()
        var finishCount = 0
        let finished = expectation(description: "all pages")
        let operation = FetchZoneChangesOperation(
            database: database,
            zoneIDs: [zoneID],
            zoneChangeTokens: [:],
            modelVersion: 0,
            ignoreDeviceIdentifier: nil,
            desiredKeys: nil,
            completion: { results in
                deliveredPages.append(
                    results[zoneID]?.downloadedRecords.map(
                        \.recordID.recordName
                    ) ?? []
                )
            },
            didFinishPages: {
                finishCount += 1
                finished.fulfill()
            }
        )

        operation.start()
        await fulfillment(of: [finished], timeout: 1)

        XCTAssertEqual(deliveredPages, [["Item.first"], []])
        XCTAssertEqual(finishCount, 1)
        XCTAssertEqual(database.fetchZoneChangesOperationCount, 2)
        XCTAssertTrue(operation.isFinished)
    }

    @BigSyncBackgroundActor
    func testStaleOperationFailureCannotFailANewerSynchronizationAttempt() async {
        let synchronizer = makeSynchronizer()
        synchronizer.syncing = true
        let operation = CloudKitSynchronizerOperation()
        synchronizer.runOperation(operation)
        for _ in 0..<1_000 where !operation.isExecuting {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertTrue(operation.isExecuting)

        synchronizer.synchronizationAttemptID = UUID()
        operation.finish(
            error: NSError(
                domain: "BigSyncKitTests.StaleOperation",
                code: 1
            )
        )
        try? await Task.sleep(nanoseconds: 50_000_000)

        XCTAssertTrue(synchronizer.syncing)
        synchronizer.cancelSynchronization()
    }

    @BigSyncBackgroundActor
    func testSynchronizationRequestDuringActiveRunSchedulesOneTailRun()
    async {
        let synchronizer = makeSynchronizer()
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let firstAttemptID = synchronizer.synchronizationAttemptID

        synchronizer.beginSynchronization()
        synchronizer.beginSynchronization()
        XCTAssertTrue(synchronizer.synchronizationRequestedWhileRunning)

        await synchronizer.changesFinishedSynchronizing()

        XCTAssertTrue(synchronizer.syncing)
        XCTAssertFalse(synchronizer.synchronizationRequestedWhileRunning)
        XCTAssertNotEqual(
            synchronizer.synchronizationAttemptID,
            firstAttemptID
        )
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testAwaitableSynchronizationReturnsOnlyAtTerminalBoundary() async
    throws {
        let synchronizer = makeSynchronizer()
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where !synchronizer.syncing {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertTrue(synchronizer.syncing)
        synchronizer.synchronizationDrainDidImportChanges = true

        await synchronizer.changesFinishedSynchronizing()
        let result = try await synchronization.value

        XCTAssertEqual(
            result,
            CloudKitSynchronizer.SynchronizationResult(
                didImportChanges: true
            )
        )
        XCTAssertFalse(synchronizer.syncing)
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testCancellationBarrierResumesAwaitingSynchronization() async {
        let synchronizer = makeSynchronizer()
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where !synchronizer.syncing {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }

        await synchronizer.cancelSynchronizationAndWait()

        do {
            _ = try await synchronization.value
            XCTFail("Expected synchronization cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testFinishingAnOldAttemptCannotEndANewerAttemptAfterActorReentry() async {
        let synchronizer = makeSynchronizer()
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "finish-race-zone"),
            priorities: []
        )
        let gate = AsyncGate()
        let enteredFinish = expectation(description: "old attempt entered finish")
        adapter.didFinishImportHandler = {
            enteredFinish.fulfill()
            await gate.wait()
        }
        synchronizer.addModelAdapter(adapter)
        synchronizer.syncing = true

        let oldFinish = Task { @BigSyncBackgroundActor in
            await synchronizer.changesFinishedSynchronizing()
        }
        await fulfillment(of: [enteredFinish], timeout: 1)

        synchronizer.synchronizationAttemptID = UUID()
        synchronizer.syncing = true
        await gate.open()
        await oldFinish.value

        XCTAssertTrue(synchronizer.syncing)
        synchronizer.cancelSynchronization()
    }

    @BigSyncBackgroundActor
    func testDatabaseSubscriptionIsSavedExactlyOnce() async {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)

        await withCheckedContinuation { continuation in
            synchronizer.subscribeForChangesInDatabase { error in
                XCTAssertNil(error)
                continuation.resume()
            }
        }

        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertEqual(database.modifySubscriptionOperationCount, 0)
    }

    func testCancellingModifyOperationFinishesAndCompletesExactlyOnce() {
        let database = FakeCloudKitDatabase()
        database.completesModifyOperations = false
        let completed = expectation(description: "completion")
        completed.expectedFulfillmentCount = 1
        var completionCount = 0
        let operation = ModifyRecordsOperation(
            database: database,
            records: [],
            recordIDsToDelete: []
        ) { _, _, _, _, error in
            completionCount += 1
            XCTAssertTrue(error is CancellationError)
            completed.fulfill()
        }

        operation.start()
        operation.cancel()

        wait(for: [completed], timeout: 1)
        XCTAssertTrue(operation.isFinished)
        XCTAssertEqual(completionCount, 1)
    }

    @BigSyncBackgroundActor
    func testZoneResetDeletesManagedZoneThenRebuildsLocalTracking() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)
        let zoneID = CKRecordZone.ID(
            zoneName: "reset-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.deleteRecordZonesAndResetSyncCachesForReupload()

        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
        XCTAssertEqual(
            adapter.events.suffix(2),
            ["unsetCancellation", "resetSyncCaches"]
        )
    }

    @BigSyncBackgroundActor
    func testZoneResetTreatsAlreadyDeletedZoneAsSuccess() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneError = NSError(
            domain: CKErrorDomain,
            code: CKError.zoneNotFound.rawValue
        )
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(
                zoneName: "missing-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.deleteRecordZonesAndResetSyncCachesForReupload()

        XCTAssertTrue(adapter.events.contains("resetSyncCaches"))
    }

    @BigSyncBackgroundActor
    func testZoneResetDoesNotClearLocalTrackingWhenDeletionFails() async {
        let database = FakeCloudKitDatabase()
        database.deleteZoneError = NSError(
            domain: CKErrorDomain,
            code: CKError.networkFailure.rawValue
        )
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(
                zoneName: "failed-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)

        do {
            try await synchronizer.deleteRecordZonesAndResetSyncCachesForReupload()
            XCTFail("Expected zone deletion to fail")
        } catch {
            XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        }
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetDeletesSharedZoneOnlyOnceAcrossDevices() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "multi-device-reset-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let firstSynchronizer = makeSynchronizer(database: database)
        let firstAdapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        firstSynchronizer.addModelAdapter(firstAdapter)

        let firstResult = try await firstSynchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "test-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )

        let secondSynchronizer = makeSynchronizer(database: database)
        let secondAdapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        secondSynchronizer.addModelAdapter(secondAdapter)
        let secondResult = try await secondSynchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "test-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )

        XCTAssertEqual(firstResult, .performedCloudReset)
        XCTAssertEqual(secondResult, .cloudResetAlreadyCompleted)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
        XCTAssertTrue(firstAdapter.events.contains("resetSyncCaches"))
        XCTAssertTrue(secondAdapter.events.contains("resetSyncCaches"))
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetRetainsClaimForRetryAfterDeletionFailure() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneError = NSError(
            domain: CKErrorDomain,
            code: CKError.networkFailure.rawValue
        )
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(
                zoneName: "retry-reset-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "retry-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail("Expected the first zone deletion to fail")
        } catch {
            XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        }

        database.deleteZoneError = nil
        let result = try await synchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "retry-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )

        XCTAssertEqual(result, .performedCloudReset)
        XCTAssertTrue(adapter.events.contains("resetSyncCaches"))
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetDoesNotStealAnActiveClaim() async throws {
        let database = FakeCloudKitDatabase()
        let claimID = CKRecord.ID(
            recordName: "BigSyncKitMigration.active-lease-v1.claim"
        )
        let claim = CKRecord(recordType: "ExistingRecordType", recordID: claimID)
        claim["owner"] = "another-device" as CKRecordValue
        claim["lease"] = Date() as CKRecordValue
        database.seed(claim)

        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(
            FakeModelAdapter(
                zoneID: CKRecordZone.ID(
                    zoneName: "active-lease-zone",
                    ownerName: CKCurrentUserDefaultName
                ),
                priorities: []
            )
        )

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "active-lease-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 60
            )
            XCTFail("Expected the active claim to block this device")
        } catch OneOffRecordZoneResetError.migrationInProgress {
            XCTAssertTrue(database.deletedZoneIDs.isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetTakesOverAnExpiredClaimWithoutDeletingItFirst() async throws {
        let database = FakeCloudKitDatabase()
        let claimID = CKRecord.ID(
            recordName: "BigSyncKitMigration.expired-lease-v1.claim"
        )
        let claim = CKRecord(recordType: "ExistingRecordType", recordID: claimID)
        claim["owner"] = "offline-device" as CKRecordValue
        claim["lease"] = Date(timeIntervalSinceNow: -300) as CKRecordValue
        database.seed(claim)

        let zoneID = CKRecordZone.ID(
            zoneName: "expired-lease-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )

        let result = try await synchronizer.performOneOffRecordZoneResetAndReupload(
            migrationIdentifier: "expired-lease-v1",
            markerRecordType: "ExistingRecordType",
            markerOwnerField: "owner",
            markerLeaseDateField: "lease",
            leaseDuration: 60
        )

        XCTAssertEqual(result, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetStopsWhenCloudKitAccountChangesMidMigration() async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        database.accountIdentifierAfterNextZoneDeletion = "account-b"
        let zoneID = CKRecordZone.ID(
            zoneName: "account-change-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "account-change-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail("Expected the account fence to stop the migration")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
            XCTAssertEqual(database.deletedZoneIDs, [zoneID])
        }
    }

    @BigSyncBackgroundActor
    func testSynchronizationAccountSwitchRebuildsAdapterMetadata() async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(
                zoneName: "account-adoption",
                ownerName: CKCurrentUserDefaultName
            ),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)

        try await synchronizer._test_validateSynchronizationAccount()
        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()

        try await synchronizer._test_validateSynchronizationAccount()

        XCTAssertTrue(adapter.events.contains("resetSyncCaches"))
        XCTAssertFalse(synchronizer.cancelledDueToUnauthentication)
    }

    func testCloudKitAccountAvailabilityGateStartsImmediatelyWhenAvailable() async {
        let gate = CloudKitAccountAvailabilityGate { identifier in
            XCTAssertEqual(identifier, "iCloud.test")
            return .available
        }

        let availability = await gate.availability(for: "iCloud.test")
        XCTAssertEqual(availability, .available)
    }

    @BigSyncBackgroundActor
    func testDeletingUnknownCloudKitItemIsAcknowledgedAsAlreadyDeleted() async throws {
        let database = FakeCloudKitDatabase()
        database.reportsDeletedRecordsAsUnknownItems = true
        let zoneID = CKRecordZone.ID(
            zoneName: "unknown-deletion-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let recordID = CKRecord.ID(
            recordName: "Bookmark.missing",
            zoneID: zoneID
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            deletedByEntity: ["Bookmark": [recordID]]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertTrue(adapter.events.contains("didDelete:Bookmark.missing"))
    }

    @BigSyncBackgroundActor
    func testMixedPartialUploadFailureDoesNotDiscardTheUnresolvedError() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "mixed-partial-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let missing = makeRecord(type: "Bookmark", id: "missing", zoneID: zoneID)
        let unavailable = makeRecord(type: "Bookmark", id: "unavailable", zoneID: zoneID)
        database.partialSaveErrorsByRecordID = [
            missing.recordID: NSError(
                domain: CKErrorDomain,
                code: CKError.unknownItem.rawValue
            ),
            unavailable.recordID: NSError(
                domain: CKErrorDomain,
                code: CKError.networkFailure.rawValue
            ),
        ]
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            uploadedByEntity: ["Bookmark": [missing, unavailable]]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        do {
            try await synchronizer.synchronizeAdapter(adapter)
            XCTFail("Expected the unresolved per-record failure to propagate")
        } catch {
            XCTAssertTrue(
                adapter.events.contains("deleteTracking:Bookmark.missing")
            )
            XCTAssertFalse(
                adapter.events.contains(where: { $0.hasPrefix("didUpload:") })
            )
        }
    }

    @BigSyncBackgroundActor
    func testServerConflictIsImportedBeforeUploadRetry() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "conflict-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let clientRecord = makeRecord(type: "Bookmark", id: "conflict", zoneID: zoneID)
        let serverRecord = makeRecord(type: "Bookmark", id: "conflict", zoneID: zoneID)
        database.partialSaveErrorsByRecordID = [
            clientRecord.recordID: NSError(
                domain: CKErrorDomain,
                code: CKError.serverRecordChanged.rawValue,
                userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
            )
        ]
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            uploadedByEntity: ["Bookmark": [clientRecord]]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertTrue(adapter.events.contains("save:Bookmark"))
        XCTAssertTrue(adapter.events.contains("persist"))
    }

    func testCloudKitAccountAvailabilityGateDefersUnavailableAndFailedStatuses() async {
        let unavailableGate = CloudKitAccountAvailabilityGate { _ in .unavailable(.noAccount) }
        let failedGate = CloudKitAccountAvailabilityGate { _ in .failed }

        let unavailable = await unavailableGate.availability(for: "iCloud.test")
        let failed = await failedGate.availability(for: "iCloud.test")
        XCTAssertEqual(unavailable, .unavailable(.noAccount))
        XCTAssertEqual(failed, .failed)
    }

    func testCloudKitAccountAvailabilityGateReevaluatesEveryRequest() async {
        actor StatusSequence {
            var statuses: [CloudKitAccountAvailability] = [.unavailable(.couldNotDetermine), .available]

            func next() -> CloudKitAccountAvailability {
                statuses.removeFirst()
            }
        }

        let statuses = StatusSequence()
        let gate = CloudKitAccountAvailabilityGate { _ in await statuses.next() }

        let initiallyUnavailable = await gate.availability(for: "iCloud.test")
        let subsequentlyAvailable = await gate.availability(for: "iCloud.test")
        XCTAssertEqual(initiallyUnavailable, .unavailable(.couldNotDetermine))
        XCTAssertEqual(subsequentlyAvailable, .available)
    }

    @BigSyncBackgroundActor
    func testPrioritizedRemoteChangesAreProcessedInConfiguredOrderBeforeUnprioritized() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "priority-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark", "HistoryRecord"]
        )
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(adapter)

        let processor = synchronizer.changeRequestProcessor
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

        let processor = synchronizer.changeRequestProcessor
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

        let processor = synchronizer.changeRequestProcessor
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

        let processor = synchronizer.changeRequestProcessor
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
    func testFetchedChangeProcessorsDoNotShareQueues() async throws {
        let zoneID = CKRecordZone.ID(
            zoneName: "isolated-queue",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        let first = ChangeRequestProcessor()
        let second = ChangeRequestProcessor()
        first.addFetchedChangeRequest(
            ChangeRequest(
                downloadedRecord: makeRecord(type: "Article", id: "1", zoneID: zoneID),
                deletedRecordID: nil,
                adapter: adapter
            )
        )

        try await second.finishProcessing(for: adapter)
        XCTAssertTrue(adapter.savedBatchSizes.isEmpty)

        try await first.finishProcessing(for: adapter)
        XCTAssertEqual(adapter.savedBatchSizes, [1])
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
    func testEmptyRealmCollectionsRemoveCloudKitArrayFields() async throws {
        let fixture = try await makeRealmAdapterFixture()
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(BigSyncTrackedObject(
                id: "empty-collections",
                createdAt: Date(),
                modifiedAt: Date(),
                explicitlyModifiedAt: Date()
            ))
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)

        let records = try await fixture.adapter.recordsToUpload(limit: 10)
        let record = try XCTUnwrap(records.first {
            $0.recordID.recordName == BigSyncTrackedObject.className() + ".empty-collections"
        })

        XCTAssertNil(record["tags"])
        XCTAssertNil(record["scores"])
        XCTAssertFalse(record.allKeys().contains("tags"))
        XCTAssertFalse(record.allKeys().contains("scores"))
        let encodedMap = try XCTUnwrap(record["attributes"] as? Data)
        let decodedMap = try XCTUnwrap(
            PropertyListSerialization.propertyList(
                from: encodedMap,
                options: [],
                format: nil
            ) as? [String: String]
        )
        XCTAssertEqual(decodedMap, [:])
    }

    @BigSyncBackgroundActor
    func testRealmMapEncodingUsesCloudKitCompatibleBinaryPropertyList() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "map",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.attributes["z"] = "last"
        object.attributes["a"] = "first"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)

        let records = try await fixture.adapter.recordsToUpload(limit: 10)
        let record = try XCTUnwrap(records.first {
            $0.recordID.recordName == BigSyncTrackedObject.className() + ".map"
        })
        let encodedMap = try XCTUnwrap(record["attributes"] as? Data)
        let decodedMap = try XCTUnwrap(
            PropertyListSerialization.propertyList(
                from: encodedMap,
                options: [],
                format: nil
            ) as? [String: String]
        )
        XCTAssertEqual(decodedMap, ["a": "first", "z": "last"])
    }

    @BigSyncBackgroundActor
    func testMissingRemoteCollectionFieldClearsLocalCollection() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "existing",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.tags.append("keep")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        let property = try XCTUnwrap(object.objectSchema.properties.first { $0.name == "tags" })
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: "existing",
            zoneID: fixture.adapter.recordZoneID
        )

        try await fixture.targetRealm.asyncWrite {
            try fixture.adapter.applyChange(
                property: property,
                record: record,
                object: object,
                syncedEntityIdentifier: record.recordID.recordName
            )
        }

        XCTAssertTrue(object.tags.isEmpty)
    }

    @BigSyncBackgroundActor
    func testURLListRoundTripsThroughCloudKitStrings() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "url-list",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.urls.append(
            objectsIn: [
                URL(string: "https://example.com/first")!,
                URL(string: "file:///tmp/offline%20item")!,
            ]
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )

        let uploadedRecords = try await fixture.adapter.recordsToUpload(
            limit: 10
        )
        let record = try XCTUnwrap(
            uploadedRecords.first {
                $0.recordID.recordName
                    == BigSyncTrackedObject.className() + ".url-list"
            }
        )
        XCTAssertEqual(
            record["urls"] as? [String],
            object.urls.map(\.absoluteString)
        )

        record["urls"] = [
            "https://example.com/replaced",
            "file:///tmp/second",
        ] as CKRecordValue
        let property = try XCTUnwrap(
            object.objectSchema.properties.first { $0.name == "urls" }
        )
        try await fixture.targetRealm.asyncWrite {
            try fixture.adapter.applyChange(
                property: property,
                record: record,
                object: object,
                syncedEntityIdentifier: record.recordID.recordName
            )
        }
        XCTAssertEqual(
            object.urls.map(\.absoluteString),
            [
                "https://example.com/replaced",
                "file:///tmp/second",
            ]
        )
    }

    @BigSyncBackgroundActor
    func testRemoteNilClearsOptionalToOneRelationship() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let child = BigSyncRelationshipChild()
        child.id = "favorite"
        let parent = BigSyncRelationshipParent()
        parent.id = "parent-with-favorite"
        parent.favoriteChild = child
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add([child, parent], update: .modified)
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["modifiedAt"] = Date().addingTimeInterval(60) as CKRecordValue
        record["explicitlyModifiedAt"] =
            Date().addingTimeInterval(60) as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncRelationshipParent.self,
                forPrimaryKey: parent.id
            )?.favoriteChild
        )
    }

    @BigSyncBackgroundActor
    func testUnknownCloudKitItemIsRequeuedAsNewInsteadOfLosingTracking() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "missing-on-server",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)
        let recordName = BigSyncTrackedObject.className() + ".missing-on-server"
        let tracked = try XCTUnwrap(
            fixture.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordName)
        )
        try await fixture.persistenceRealm.asyncWrite {
            tracked.entityState = .changed
            tracked.encodedRecord = Data([1, 2, 3])
        }

        try await fixture.adapter.deleteChangeTracking(
            forRecordIDs: [CKRecord.ID(recordName: recordName, zoneID: fixture.adapter.recordZoneID)]
        )

        let requeued = try XCTUnwrap(
            fixture.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordName)
        )
        XCTAssertEqual(requeued.entityState, .new)
        XCTAssertNil(requeued.encodedRecord)
    }

    @BigSyncBackgroundActor
    func testUploadAcknowledgesOnlyTheGenerationThatWasPrepared() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "generation",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(in: fixture.targetRealm)
        let initialRecords = try await fixture.adapter.recordsToUpload(limit: 10)
        let initialRecord = try XCTUnwrap(initialRecords.first)
        try await fixture.adapter.didUpload(savedRecords: [initialRecord])

        try await fixture.targetRealm.asyncWrite {
            object.tags.append("first-edit")
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let firstMutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".generation"
            )
        )
        let firstGeneration = firstMutation.generation
        let forwardedCount = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        XCTAssertEqual(forwardedCount, 1)

        let inFlightTracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: initialRecord.recordID.recordName
            )
        )
        XCTAssertEqual(inFlightTracking.pendingGeneration, firstGeneration)

        try await fixture.targetRealm.asyncWrite {
            object.tags.append("second-edit")
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let secondGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: initialRecord.recordID.recordName
            )?.generation
        )
        XCTAssertNotEqual(secondGeneration, firstGeneration)

        try await fixture.adapter.didUpload(
            savedRecords: [initialRecord],
            matchingGenerations: [initialRecord.recordID.recordName: firstGeneration]
        )

        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: initialRecord.recordID.recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .changed)
        XCTAssertEqual(tracking.pendingGeneration, secondGeneration)
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: initialRecord.recordID.recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testDeletionAcknowledgesOnlyTheGenerationThatWasPrepared() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "deletion-generation",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let initialRecords = try await fixture.adapter.recordsToUpload(limit: 1)
        let initialRecord = try XCTUnwrap(initialRecords.first)
        try await fixture.adapter.didUpload(savedRecords: [initialRecord])

        let recordName = initialRecord.recordID.recordName
        let deletionGeneration = UUID().uuidString
        try await fixture.persistenceRealm.asyncWrite {
            let tracking = try XCTUnwrap(
                fixture.persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: recordName
                )
            )
            tracking.entityState = .deletedLocally
            tracking.pendingGeneration = deletionGeneration
        }

        let newerGeneration = UUID().uuidString
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(
                BigSyncPendingMutation(
                    recordName: recordName,
                    entityType: BigSyncTrackedObject.className(),
                    objectIdentifier: object.id,
                    generation: newerGeneration
                ),
                update: .modified
            )
        }

        try await fixture.adapter.didDelete(
            recordIDs: [initialRecord.recordID],
            matchingGenerations: [recordName: deletionGeneration]
        )

        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .new)
        XCTAssertEqual(tracking.pendingGeneration, newerGeneration)
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testMutationIsDurablyJournaledWhenRefreshFollowsAddInSameWrite() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "unmanaged",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }

        let forwardedCount = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        XCTAssertEqual(forwardedCount, 1)
        let recordName = BigSyncTrackedObject.className() + ".unmanaged"
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.entityState,
            .new
        )
    }

    @BigSyncBackgroundActor
    func testPendingLocalEditWinsOverConcurrentRemoteDeletion() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "locally-edited",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let uploadedRecords = try await fixture.adapter.recordsToUpload(
            limit: 1
        )
        let uploaded = try XCTUnwrap(uploadedRecords.first)
        try await fixture.adapter.didUpload(savedRecords: [uploaded])

        try await fixture.targetRealm.asyncWrite {
            object.tags.append("offline-edit")
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter.deleteRecords(with: [uploaded.recordID])
        await fixture.targetRealm.asyncRefresh()

        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(Array(object.tags), ["offline-edit"])
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .new)
        XCTAssertNil(tracking.encodedRecord)
        XCTAssertNotNil(tracking.pendingGeneration)
    }

    @BigSyncBackgroundActor
    func testRemoteDeletionWithoutExistingTrackingStillDeletesLocalObject() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "unknown-tracking",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        let recordName = BigSyncTrackedObject.className() + ".unknown-tracking"
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )

        try await fixture.adapter.deleteRecords(
            with: [
                CKRecord.ID(
                    recordName: recordName,
                    zoneID: fixture.adapter.recordZoneID
                )
            ]
        )

        await fixture.targetRealm.asyncRefresh()
        XCTAssertTrue(
            fixture.targetRealm.object(
                ofType: BigSyncTrackedObject.self,
                forPrimaryKey: "unknown-tracking"
            )?.isDeleted == true
        )
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.entityState,
            .deletedRemotely
        )
    }

    @BigSyncBackgroundActor
    func testRemoteRelationshipCollectionsReplaceAtomicallyAndPreserveListOrder()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let staleChild = BigSyncRelationshipChild()
        staleChild.id = "stale"
        let firstChild = BigSyncRelationshipChild()
        firstChild.id = "first"
        let secondChild = BigSyncRelationshipChild()
        secondChild.id = "second"
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        parent.children.append(staleChild)
        parent.relatedChildren.insert(staleChild)
        parent.favoriteChild = staleChild
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(
                [staleChild, firstChild, secondChild, parent],
                update: .modified
            )
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["children"] = [
            "\(BigSyncRelationshipChild.className()).second",
            "\(BigSyncRelationshipChild.className()).first",
        ] as CKRecordValue
        record["relatedChildren"] = [
            "\(BigSyncRelationshipChild.className()).first",
            "\(BigSyncRelationshipChild.className()).second",
        ] as CKRecordValue
        record["favoriteChild"] =
            "\(BigSyncRelationshipChild.className()).first" as CKRecordValue
        record["modifiedAt"] = Date().addingTimeInterval(60) as CKRecordValue
        record["explicitlyModifiedAt"] =
            Date().addingTimeInterval(60) as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        let refreshedParent = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncRelationshipParent.self,
                forPrimaryKey: parent.id
            )
        )
        XCTAssertEqual(refreshedParent.children.map(\.id), ["second", "first"])
        XCTAssertEqual(
            Set(refreshedParent.relatedChildren.map(\.id)),
            Set(["first", "second"])
        )
        XCTAssertEqual(refreshedParent.favoriteChild?.id, "first")
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            0
        )
    }

    @BigSyncBackgroundActor
    func testRemoteRelationshipCollectionWaitsForEveryTargetBeforeReplacement()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let staleChild = BigSyncRelationshipChild()
        staleChild.id = "stale"
        let availableChild = BigSyncRelationshipChild()
        availableChild.id = "available"
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        parent.children.append(staleChild)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(
                [staleChild, availableChild, parent],
                update: .modified
            )
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["children"] = [
            "\(BigSyncRelationshipChild.className()).available",
            "\(BigSyncRelationshipChild.className()).late",
        ] as CKRecordValue
        record["modifiedAt"] = Date().addingTimeInterval(60) as CKRecordValue
        record["explicitlyModifiedAt"] =
            Date().addingTimeInterval(60) as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncRelationshipParent.self,
                forPrimaryKey: parent.id
            )?.children.map(\.id),
            ["stale"]
        )
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            2
        )

        let lateChild = BigSyncRelationshipChild()
        lateChild.id = "late"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(lateChild)
        }
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncRelationshipParent.self,
                forPrimaryKey: parent.id
            )?.children.map(\.id),
            ["available", "late"]
        )
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            0
        )
    }

    @BigSyncBackgroundActor
    func testSetupDrainsJournalWithoutStartingSynchronization() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let delegate = FakeModelAdapterDelegate()
        fixture.adapter.modelAdapterDelegate = delegate
        try await fixture.targetRealm.asyncWrite {
            let object = BigSyncTrackedObject(
                id: "setup-journal",
                createdAt: Date(),
                modifiedAt: Date(),
                explicitlyModifiedAt: nil
            )
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }

        try await fixture.adapter._test_setup()

        XCTAssertEqual(delegate.uploadWakeupCount, 0)
        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".setup-journal"
            )
        )
    }

    @BigSyncBackgroundActor
    func testJournalDropsMutationsForUntrackedEntityTypes() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let recordName = "ExcludedThing.id"
        let mutation = BigSyncPendingMutation(
            recordName: recordName,
            entityType: "ExcludedThing",
            objectIdentifier: "id"
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(mutation)
        }

        let forwarded = try await fixture.adapter
            ._test_forwardPendingMutations(in: fixture.targetRealm)

        XCTAssertEqual(forwarded, 0)
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testVersionedRecoveryFindsPreJournalObjects() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let date = Date(timeIntervalSinceReferenceDate: 20_000)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(
                BigSyncTrackedObject(
                    id: "pre-journal",
                    createdAt: date,
                    modifiedAt: date,
                    explicitlyModifiedAt: date
                )
            )
        }
        try await fixture.persistenceRealm.asyncWrite {
            let recovery = fixture.persistenceRealm.object(
                ofType: SyncedEntityType.self,
                forPrimaryKey: "__BigSyncKitMutationJournalRecovery"
            )
            recovery?.recoveryVersion = 0
        }

        try await fixture.adapter._test_setup()

        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".pre-journal"
            )
        )
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntityType.self,
                forPrimaryKey: "__BigSyncKitMutationJournalRecovery"
            )?.recoveryVersion,
            1
        )
    }

    @BigSyncBackgroundActor
    func testNormalSetupDrainsJournalWithoutRepeatingBroadTimestampRecovery() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let date = Date(timeIntervalSinceReferenceDate: 30_000)
        let journaled = BigSyncTrackedObject(
            id: "journaled",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: nil
        )
        let unjournaled = BigSyncTrackedObject(
            id: "unjournaled",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: date
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(journaled)
            journaled.refreshChangeMetadata(explicitlyModified: true)
            fixture.targetRealm.add(unjournaled)
        }

        try await fixture.adapter._test_setup()

        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".journaled"
            )
        )
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".unjournaled"
            )
        )
    }

    @BigSyncBackgroundActor
    private func makeSynchronizer(
        database: CloudKitDatabaseAdapter = FakeCloudKitDatabase(),
        keyValueStore: KeyValueStore = DictionaryKeyValueStore(),
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
            "test-account"
        }
    ) -> CloudKitSynchronizer {
        CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: database,
            adapterProvider: NoopAdapterProvider(),
            keyValueStore: keyValueStore,
            accountIdentifierProvider: accountIdentifierProvider,
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
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncRelationshipChild.self,
            BigSyncRelationshipParent.self,
            BigSyncPendingMutation.self,
        ]

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

        guard let persistenceRealm = adapter.realmProvider?.persistenceRealm,
              let targetRealm = adapter.realmProvider?.targetReaderRealms?.first else {
            throw NSError(
                domain: "BigSyncKitTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Adapter Realm provider was not initialized"]
            )
        }
        return (adapter, persistenceRealm, targetRealm)
    }
}
