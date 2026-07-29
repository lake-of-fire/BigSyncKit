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
    var deleteZoneError: Error?
    var completesModifyOperations = true
    var reportsDeletedRecordsAsUnknownItems = false
    private(set) var deletedZoneIDs = [CKRecordZone.ID]()
    private var records = [CKRecord.ID: CKRecord]()

    func add(_ operation: CKDatabaseOperation) {
        if let modifyOperation = operation as? CKModifyRecordsOperation {
            guard completesModifyOperations else { return }
            let savedRecords = modifyOperation.recordsToSave ?? []
            let deletedRecordIDs = modifyOperation.recordIDsToDelete ?? []
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
        completionHandler(records[recordID], nil)
    }

    func delete(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone.ID?, Error?) -> Void) {
        deletedZoneIDs.append(zoneID)
        completionHandler(deleteZoneError == nil ? zoneID : nil, deleteZoneError)
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

    func saveToken(_ token: CKServerChangeToken?) async {
        storedServerChangeToken = token
        events.append("saveToken")
    }

    func deleteChangeTracking(forRecordIDs: [CKRecord.ID]) async throws {}
    func didFinishImport() async {}
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

final class BigSyncKitTests: XCTestCase {
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
                markerRecordType: "ExistingRecordType"
            )

        let secondSynchronizer = makeSynchronizer(database: database)
        let secondAdapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        secondSynchronizer.addModelAdapter(secondAdapter)
        let secondResult = try await secondSynchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "test-v1",
                markerRecordType: "ExistingRecordType"
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
                markerRecordType: "ExistingRecordType"
            )
            XCTFail("Expected the first zone deletion to fail")
        } catch {
            XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        }

        database.deleteZoneError = nil
        let result = try await synchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "retry-v1",
                markerRecordType: "ExistingRecordType"
            )

        XCTAssertEqual(result, .performedCloudReset)
        XCTAssertTrue(adapter.events.contains("resetSyncCaches"))
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
    func testEmptyRealmCollectionsAreEncodedAsExplicitEmptyCloudKitValues() async throws {
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

        XCTAssertEqual(record["tags"] as? [String], [])
        XCTAssertEqual(record["scores"] as? [Int], [])
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
    func testMissingRemoteCollectionFieldPreservesLocalCollection() async throws {
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

        XCTAssertEqual(Array(object.tags), ["keep"])
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
    func testUnmanagedRefreshIsJournaledAfterObjectIsAdded() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "unmanaged",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.refreshChangeMetadata(explicitlyModified: true)

        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
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
    private func makeSynchronizer(
        database: CloudKitDatabaseAdapter = FakeCloudKitDatabase()
    ) -> CloudKitSynchronizer {
        CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: database,
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
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
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
