import XCTest
import CloudKit
import Logging
@testable import BigSyncKit
import RealmSwift
import RealmSwiftGaps

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
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID) async throws {}
}

private enum TestSynchronizationError: Error {
    case initialSetupFailed
    case terminalForwardingFailed
    case deletedZoneResetFailed
    case restoredBackupResetFailed
    case importedPersistenceCacheFailed
}

private final class FailingDeletedZoneProvider: NSObject, AdapterProvider {
    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, modelAdapterForRecordZoneID zoneID: CKRecordZone.ID) -> ModelAdapter? { nil }

    func cloudKitSynchronizer(_ synchronizer: CloudKitSynchronizer, zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID) async throws {
        throw TestSynchronizationError.deletedZoneResetFailed
    }
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
    var completesFetchDatabaseChanges = true
    var completesRecordZoneFetches = true
    var recordZoneFetchHandler: (@Sendable () -> Void)?
    var completesSubscriptionFetches = true
    var reportsDeletedRecordsAsUnknownItems = false
    var partialSaveErrorsByRecordID = [CKRecord.ID: NSError]()
    var accountIdentifier = "test-account"
    var accountIdentifierAfterNextRecordFetch: String?
    // Review findings 3 and 4 test seams for account replacement at callback
    // and final CloudKit mutation boundaries.
    var accountIdentifierAfterNextDatabaseChangesFetch: String?
    var databaseDeletedZoneIDs = [CKRecordZone.ID]()
    var accountIdentifierAfterNextMigrationMarkerSave: String?
    var accountIdentifierAfterNextModifyRecords: String?
    var accountIdentifierAfterNextZoneDeletion: String?
    var migrationClaimFetchDelayNanoseconds: UInt64?
    var deleteZoneDelayNanoseconds: UInt64?
    var deleteZoneHandler: (@Sendable () -> Void)?
    var zoneChangePages = [FakeZoneChangePage]()
    private(set) var deletedZoneIDs = [CKRecordZone.ID]()
    private(set) var savedSubscriptionCount = 0
    private(set) var subscriptionFetchCount = 0
    private(set) var modifySubscriptionOperationCount = 0
    private(set) var modifyRecordsOperationCount = 0
    private(set) var fetchZoneChangesOperationCount = 0
    private(set) var recordZoneFetchCount = 0
    private(set) var modifyRecordsAtomicValues = [Bool]()
    private(set) var modifyRecordsSavePolicies = [CKModifyRecordsOperation.RecordSavePolicy]()
    private let recordsLock = NSLock()
    private var records = [CKRecord.ID: CKRecord]()
    private var conditionallyFetchedRecordIDs = Set<CKRecord.ID>()

    private func withRecordsLock<T>(_ operation: () throws -> T) rethrows -> T {
        recordsLock.lock()
        defer { recordsLock.unlock() }
        return try operation()
    }

    func add(_ operation: CKDatabaseOperation) {
        if let fetchOperation = operation as? CKFetchDatabaseChangesOperation {
            if completesFetchDatabaseChanges {
                for zoneID in databaseDeletedZoneIDs {
                    fetchOperation.recordZoneWithIDWasDeletedBlock?(zoneID)
                }
                if let nextAccountIdentifier =
                    accountIdentifierAfterNextDatabaseChangesFetch {
                    accountIdentifier = nextAccountIdentifier
                    accountIdentifierAfterNextDatabaseChangesFetch = nil
                }
                fetchOperation.fetchDatabaseChangesCompletionBlock?(nil, false, nil)
            }
            return
        }
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
            modifyRecordsOperationCount += 1
            modifyRecordsAtomicValues.append(modifyOperation.isAtomic)
            modifyRecordsSavePolicies.append(modifyOperation.savePolicy)
            guard completesModifyOperations else { return }
            let savedRecords = modifyOperation.recordsToSave ?? []
            let deletedRecordIDs = modifyOperation.recordIDsToDelete ?? []
            if let nextAccountIdentifier =
                accountIdentifierAfterNextModifyRecords {
                accountIdentifier = nextAccountIdentifier
                accountIdentifierAfterNextModifyRecords = nil
            }
            if let nextAccountIdentifier = accountIdentifierAfterNextMigrationMarkerSave,
               !savedRecords.isEmpty {
                accountIdentifier = nextAccountIdentifier
                accountIdentifierAfterNextMigrationMarkerSave = nil
            }
            let partialSaveErrors = partialSaveErrorsByRecordID.filter {
                savedRecords.map(\.recordID).contains($0.key)
            }
            if !partialSaveErrors.isEmpty {
                var successfullySaved = [CKRecord]()
                for record in savedRecords {
                    if let error = partialSaveErrors[record.recordID] {
                        modifyOperation.perRecordCompletionBlock?(record, error)
                    } else {
                        withRecordsLock {
                            records[record.recordID] = record
                        }
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
            let duplicateRecord = withRecordsLock {
                savedRecords.first(where: {
                    records[$0.recordID] != nil
                        && !conditionallyFetchedRecordIDs.contains($0.recordID)
                })
            }
            if modifyOperation.savePolicy == .ifServerRecordUnchanged,
               let duplicateRecord {
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
                withRecordsLock {
                    records[record.recordID] = record
                    conditionallyFetchedRecordIDs.remove(record.recordID)
                }
                modifyOperation.perRecordCompletionBlock?(record, nil)
            }
            for recordID in deletedRecordIDs {
                _ = withRecordsLock {
                    records.removeValue(forKey: recordID)
                }
            }
            modifyOperation.modifyRecordsCompletionBlock?(savedRecords, deletedRecordIDs, nil)
        }
    }

    func save(zone: CKRecordZone, completionHandler: @escaping (CKRecordZone?, Error?) -> Void) {
        completionHandler(zone, nil)
    }

    func fetch(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone?, Error?) -> Void) {
        recordZoneFetchCount += 1
        recordZoneFetchHandler?()
        guard completesRecordZoneFetches else { return }
        completionHandler(CKRecordZone(zoneID: zoneID), nil)
    }

    func fetch(withRecordID recordID: CKRecord.ID, completionHandler: @escaping (CKRecord?, Error?) -> Void) {
        let record = withRecordsLock {
            let record = records[recordID]
            if record != nil {
                conditionallyFetchedRecordIDs.insert(recordID)
            }
            return record
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextRecordFetch {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextRecordFetch = nil
        }
        if recordID.recordName.hasSuffix(".claim"),
           record != nil,
           let migrationClaimFetchDelayNanoseconds {
            Task {
                try? await Task.sleep(nanoseconds: migrationClaimFetchDelayNanoseconds)
                completionHandler(record, nil)
            }
        } else {
            completionHandler(record, nil)
        }
    }

    func setDate(_ date: Date, field: String, for recordID: CKRecord.ID) {
        withRecordsLock {
            records[recordID]?[field] = date as CKRecordValue
        }
    }

    func seed(_ record: CKRecord) {
        withRecordsLock {
            records[record.recordID] = record
        }
    }

    func delete(withRecordZoneID zoneID: CKRecordZone.ID, completionHandler: @escaping (CKRecordZone.ID?, Error?) -> Void) {
        deletedZoneIDs.append(zoneID)
        deleteZoneHandler?()
        if let nextAccountIdentifier = accountIdentifierAfterNextZoneDeletion {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextZoneDeletion = nil
        }
        let complete = { [deleteZoneError] in
            completionHandler(deleteZoneError == nil ? zoneID : nil, deleteZoneError)
        }
        if let deleteZoneDelayNanoseconds {
            Task {
                try? await Task.sleep(nanoseconds: deleteZoneDelayNanoseconds)
                complete()
            }
        } else {
            complete()
        }
    }

    @available(iOS 10.0, macOS 10.12, watchOS 6.0, *)
    func fetchAllSubscriptions(completionHandler: @escaping ([CKSubscription]?, Error?) -> Void) {
        subscriptionFetchCount += 1
        guard completesSubscriptionFetches else { return }
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
    // Review finding 1 test seam for throwing adapter readiness.
    var initialSetupHandler: (@Sendable () async throws -> Void)?

    func needsInitialSetup() async throws {
        initialSetupCount += 1
        try await initialSetupHandler?()
    }

    func hasChangesToUpload() async {
        uploadWakeupCount += 1
    }
}

private actor AsyncGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isOpen = false

    func wait() async {
        guard !isOpen else { return }
        await withCheckedContinuation {
            continuation = $0
        }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }

    func hasOpened() -> Bool {
        isOpen
    }
}

private actor AccountIdentifierSequence {
    private var identifiers: [String]

    init(_ identifiers: [String]) {
        self.identifiers = identifiers
    }

    func next() -> String {
        guard identifiers.count > 1 else {
            return identifiers[0]
        }
        return identifiers.removeFirst()
    }
}

private final class FakeModelAdapter: NSObject, PrioritySyncCapableModelAdapter, @unchecked Sendable {
    let recordZoneID: CKRecordZone.ID
    let priorityEntityTypeNames: [String]
    weak var modelAdapterDelegate: ModelAdapterDelegate?
    var mergePolicy: MergePolicy = .server

    private(set) var events = [String]()
    private(set) var savedBatchSizes = [Int]()
    // Review finding 2 test seam for the post-cleanup forwarding boundary.
    private(set) var didFinishImportCount = 0
    private var uploadedByEntity: [String: [CKRecord]]
    private var deletedByEntity: [String: [CKRecord.ID]]
    private var storedServerChangeToken: CKServerChangeToken?
    var didFinishImportHandler: (@Sendable () async throws -> Void)?
    var cleanUpHandler: (@Sendable () async throws -> Void)?
    var resetSyncCachesHandler: (@Sendable () async throws -> Void)?
    var saveChangesHandler: (@Sendable () async throws -> Void)?
    var recordsToUploadHandler: (@Sendable () async throws -> Void)?

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

    func cleanUp() async throws {
        events.append("cleanUp")
        try await cleanUpHandler?()
    }
    func resetSyncCaches() async throws {
        events.append("resetSyncCaches")
        try await resetSyncCachesHandler?()
    }
    func hasChanges(record: CKRecord, object: RealmSwift.Object) -> Bool { true }

    func saveChanges(in records: [CKRecord], forceSave: Bool) async throws {
        savedBatchSizes.append(records.count)
        let recordTypes = records.map { $0.recordType }.joined(separator: ",")
        events.append("save:\(recordTypes)")
        try await saveChangesHandler?()
    }

    func deleteRecords(with recordIDs: [CKRecord.ID]) async throws {
        let recordNames = recordIDs.map { $0.recordName }.joined(separator: ",")
        events.append("deleteRemote:\(recordNames)")
    }

    func persistImportedChanges() async throws {
        events.append("persist")
    }

    func recordsToUpload(limit: Int, restrictedToEntityType: String?) async throws -> [CKRecord] {
        try await recordsToUploadHandler?()
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
    func didFinishImport() async throws {
        didFinishImportCount += 1
        try await didFinishImportHandler?()
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
private final class BigSyncTrackedObject: Object, ChangeMetadataRecordable,
CloudKitInitialSyncEligibilityModel {
    @Persisted(primaryKey: true) var id: String
    @Persisted var createdAt: Date
    @Persisted var modifiedAt: Date
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    @Persisted var tags: List<String>
    @Persisted var urls: List<URL>
    @Persisted var scores: MutableSet<Int>
    @Persisted var attributes: Map<String, String>
    @Persisted var initialCloudKitSyncEligible = true

    static var initialCloudKitSyncEligibilityPredicate: NSPredicate {
        NSPredicate(format: "initialCloudKitSyncEligible == true")
    }

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
    func testRefreshChangeMetadataUsesSuppliedTimestampForJournalAndMetadata() throws {
        var configuration = Realm.Configuration()
        configuration.inMemoryIdentifier = "timestamped-tracking-\(UUID().uuidString)"
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
            id: "timestamped",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        let timestamp = Date(timeIntervalSinceReferenceDate: 42_000)

        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(
                explicitlyModified: true,
                at: timestamp
            )
        }

        XCTAssertEqual(object.modifiedAt, timestamp)
        XCTAssertEqual(object.explicitlyModifiedAt, timestamp)
        XCTAssertEqual(
            realm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".timestamped"
            )?.changedAt,
            timestamp
        )
    }

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
        await fulfillment(of: [finished], timeout: 5)

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
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where synchronizer.activeRunContext == nil {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertNotNil(synchronizer.activeRunContext)
        synchronizer.synchronizationDrainDidImportChanges = true

        await synchronizer.changesFinishedSynchronizing()
        let result = try await synchronization.value
        let expectedAccountScope =
            try await synchronizer.cloudKitAccountScopeIdentifier()

        XCTAssertTrue(result.didImportChanges)
        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(
            result.receipt?.accountScopeIdentifier,
            expectedAccountScope
        )
        XCTAssertFalse(synchronizer.syncing)
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testTerminalForwardingFailureWithholdsSynchronizationReceipt() async {
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "terminal-forwarding-failure"),
            priorities: []
        )
        adapter.didFinishImportHandler = {
            throw TestSynchronizationError.terminalForwardingFailed
        }
        synchronizer.addModelAdapter(adapter)

        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where synchronizer.activeRunContext == nil {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }

        await synchronizer.changesFinishedSynchronizing()

        do {
            _ = try await synchronization.value
            XCTFail("Expected final journal forwarding failure")
        } catch TestSynchronizationError.terminalForwardingFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertNil(synchronizer.activeReceiptAuthorizationID)
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testPostCleanupJournalForwardingFailureWithholdsReceipt() async {
        // Review finding 2: the second forwarding boundary is the only call that
        // sees a mutation committed while cleanup was running.
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "post-cleanup-forwarding"),
            priorities: []
        )
        adapter.didFinishImportHandler = { [weak adapter] in
            if adapter?.didFinishImportCount == 2 {
                throw TestSynchronizationError.terminalForwardingFailed
            }
        }
        synchronizer.addModelAdapter(adapter)

        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where synchronizer.activeRunContext == nil {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }

        await synchronizer.changesFinishedSynchronizing()

        do {
            _ = try await synchronization.value
            XCTFail("Expected post-cleanup journal forwarding failure")
        } catch TestSynchronizationError.terminalForwardingFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertGreaterThanOrEqual(adapter.didFinishImportCount, 2)
        XCTAssertTrue(adapter.events.contains("cleanUp"))
        XCTAssertNil(synchronizer.activeReceiptAuthorizationID)
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testDeletedZoneProviderFailureIsPropagatedBeforeTokenCommit() async {
        let database = FakeCloudKitDatabase()
        database.databaseDeletedZoneIDs = [
            CKRecordZone.ID(zoneName: "deleted-zone")
        ]
        let synchronizer = CloudKitSynchronizer(
            identifier: "deleted-zone-reset-failure",
            containerIdentifier: "iCloud.test",
            database: database,
            adapterProvider: FailingDeletedZoneProvider(),
            keyValueStore: DictionaryKeyValueStore(),
            accountIdentifierProvider: { "test-account" },
            logger: Logger(label: "BigSyncKitTests")
        )

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected deleted-zone cache reset failure")
        } catch TestSynchronizationError.deletedZoneResetFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertNil(synchronizer.storedDatabaseToken)
    }

    @BigSyncBackgroundActor
    func testAccountSwitchDuringCleanupPreventsSuccessfulReceipt() async
    throws {
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "cleanup-zone"),
            priorities: []
        )
        adapter.cleanUpHandler = {
            database.accountIdentifier = "different-account"
        }
        synchronizer.addModelAdapter(adapter)
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where synchronizer.activeRunContext == nil {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertNotNil(synchronizer.activeRunContext)

        await synchronizer.changesFinishedSynchronizing()

        do {
            _ = try await synchronization.value
            XCTFail("Expected account switch to fail terminal cleanup")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertTrue(adapter.events.contains("cleanUp"))
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testAccountSwitchBeforeDeletedZonePublicationSkipsProviderMutation()
    async {
        // Review finding 3: the provider callback can destructively reset Realm
        // tracking state, so it must not run for a stale account callback.
        final class RecordingProvider: NSObject, AdapterProvider {
            private(set) var deletedZoneCount = 0

            func cloudKitSynchronizer(
                _ synchronizer: CloudKitSynchronizer,
                modelAdapterForRecordZoneID zoneID: CKRecordZone.ID
            ) -> ModelAdapter? {
                nil
            }

            func cloudKitSynchronizer(
                _ synchronizer: CloudKitSynchronizer,
                zoneWasDeletedWithZoneID zoneID: CKRecordZone.ID
            ) async throws {
                deletedZoneCount += 1
            }
        }

        let database = FakeCloudKitDatabase()
        database.databaseDeletedZoneIDs = [
            CKRecordZone.ID(zoneName: "stale-deleted-zone")
        ]
        database.accountIdentifierAfterNextDatabaseChangesFetch = "account-b"
        let provider = RecordingProvider()
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: database,
            adapterProvider: provider,
            keyValueStore: DictionaryKeyValueStore(),
            accountIdentifierProvider: { database.accountIdentifier },
            logger: Logger(label: "BigSyncKitTests")
        )

        // Review finding 4: the account check runs inside a callback-owned Task.
        // Bound the assertion so a discarded thrown error is reported as a hang.
        let finished = expectation(description: "stale callback failed the run")
        var didComplete = false
        var observedError: Error?
        let synchronization = Task { @BigSyncBackgroundActor in
            defer {
                didComplete = true
                finished.fulfill()
            }
            do {
                _ = try await synchronizer.synchronize()
            } catch {
                observedError = error
            }
        }
        await fulfillment(of: [finished], timeout: 5)
        guard didComplete else {
            synchronization.cancel()
            await synchronizer.cancelSynchronizationAndWait()
            XCTFail("Stale database callback abandoned the synchronization run")
            return
        }
        guard let resetError = observedError as? OneOffRecordZoneResetError else {
            if let observedError {
                XCTFail("Unexpected error: \(observedError)")
            } else {
                XCTFail("Expected account replacement to fail the stale callback")
            }
            return
        }
        guard case .cloudKitAccountChanged = resetError else {
            XCTFail("Unexpected reset error: \(resetError)")
            return
        }
        XCTAssertEqual(provider.deletedZoneCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountSwitchDuringUploadPreparationSkipsCloudKitMutation()
    async {
        // Review finding 3: adapter preparation can suspend after the run's last
        // account check. The mutating CloudKit operation must perform its own
        // final preflight.
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "upload-account-preflight",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: [
                "Item": [makeRecord(type: "Item", id: "1", zoneID: zoneID)]
            ]
        )
        adapter.recordsToUploadHandler = {
            database.accountIdentifier = "account-b"
        }
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected account replacement before upload")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountSwitchInUploadCallbackFailsRunInsteadOfHanging() async {
        // Review finding 4: CloudKit can complete after the account changes. The
        // callback's final validation must resolve the awaiting upload bridge.
        let database = FakeCloudKitDatabase()
        database.accountIdentifierAfterNextModifyRecords = "account-b"
        let zoneID = CKRecordZone.ID(
            zoneName: "upload-callback-account-switch",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: [
                "Item": [makeRecord(type: "Item", id: "1", zoneID: zoneID)]
            ]
        )
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(adapter)

        let finished = expectation(description: "upload callback failed the run")
        var didComplete = false
        var observedError: Error?
        let synchronization = Task { @BigSyncBackgroundActor in
            defer {
                didComplete = true
                finished.fulfill()
            }
            do {
                _ = try await synchronizer.synchronize()
            } catch {
                observedError = error
            }
        }
        await fulfillment(of: [finished], timeout: 5)
        guard didComplete else {
            synchronization.cancel()
            await synchronizer.cancelSynchronizationAndWait()
            XCTFail("Upload callback abandoned the synchronization run")
            return
        }
        guard let resetError = observedError as? OneOffRecordZoneResetError else {
            if let observedError {
                XCTFail("Unexpected error: \(observedError)")
            } else {
                XCTFail("Expected account replacement during upload callback")
            }
            return
        }
        guard case .cloudKitAccountChanged = resetError else {
            XCTFail("Unexpected reset error: \(resetError)")
            return
        }
        XCTAssertEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testAccountSwitchInDeletionCallbackFailsRunInsteadOfHanging() async {
        // Review finding 4: the deletion callback has the same unstructured-task
        // boundary as uploads and must resolve its awaiting attempt on failure.
        let database = FakeCloudKitDatabase()
        database.accountIdentifierAfterNextModifyRecords = "account-b"
        let zoneID = CKRecordZone.ID(
            zoneName: "deletion-callback-account-switch",
            ownerName: CKCurrentUserDefaultName
        )
        let recordID = CKRecord.ID(
            recordName: "Item.1",
            zoneID: zoneID
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            deletedByEntity: ["Item": [recordID]]
        )
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(adapter)

        let finished = expectation(
            description: "deletion callback failed the run"
        )
        var didComplete = false
        var observedError: Error?
        let synchronization = Task { @BigSyncBackgroundActor in
            defer {
                didComplete = true
                finished.fulfill()
            }
            do {
                _ = try await synchronizer.synchronize()
            } catch {
                observedError = error
            }
        }
        await fulfillment(of: [finished], timeout: 5)
        guard didComplete else {
            synchronization.cancel()
            await synchronizer.cancelSynchronizationAndWait()
            XCTFail("Deletion callback abandoned the synchronization run")
            return
        }
        guard let resetError = observedError as? OneOffRecordZoneResetError else {
            if let observedError {
                XCTFail("Unexpected error: \(observedError)")
            } else {
                XCTFail("Expected account replacement during deletion callback")
            }
            return
        }
        guard case .cloudKitAccountChanged = resetError else {
            XCTFail("Unexpected reset error: \(resetError)")
            return
        }
        XCTAssertEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testCancellationBarrierResumesAwaitingSynchronization() async {
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
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
    func testPublicCacheResetCannotBypassCancellationBarrier() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)
        let gate = AsyncGate()
        let enteredTerminalImport = expectation(
            description: "terminal import entered"
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "public-reset-barrier"),
            priorities: []
        )
        adapter.didFinishImportHandler = {
            enteredTerminalImport.fulfill()
            await gate.wait()
        }
        synchronizer.addModelAdapter(adapter)
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        await fulfillment(of: [enteredTerminalImport], timeout: 1)

        let reset = Task { @BigSyncBackgroundActor in
            try await synchronizer.resetSyncCaches(
                cancelSynchronization: false,
                includingAdapters: true
            )
        }
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertFalse(adapter.events.contains("resetSyncCaches"))

        await gate.open()
        try await reset.value
        XCTAssertTrue(adapter.events.contains("resetSyncCaches"))
        do {
            _ = try await synchronization.value
            XCTFail("Expected synchronization cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @BigSyncBackgroundActor
    func testRestoredBackupResetFailureRetainsPendingMarker() async {
        let store = DictionaryKeyValueStore()
        store.set(
            boolValue: true,
            forKey: BackupDetection.restoreResetRequiredStoreKey
        )
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "restore-reset-failure"),
            priorities: []
        )
        adapter.resetSyncCachesHandler = {
            throw TestSynchronizationError.restoredBackupResetFailed
        }
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected restored-backup cache reset to fail")
        } catch TestSynchronizationError.restoredBackupResetFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(BackupDetection.restoreResetIsRequired(store: store))
        XCTAssertEqual(database.subscriptionFetchCount, 0)
        XCTAssertEqual(
            adapter.events.filter { $0 == "resetSyncCaches" }.count,
            1
        )
    }

    @BigSyncBackgroundActor
    func testRestoredBackupResetCompletesBeforeCloudKitFetch() async throws {
        let store = DictionaryKeyValueStore()
        store.set(
            boolValue: true,
            forKey: BackupDetection.restoreResetRequiredStoreKey
        )
        let database = FakeCloudKitDatabase()
        database.completesSubscriptionFetches = false
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "restore-reset-success"),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }

        for _ in 0..<1_000 where database.subscriptionFetchCount == 0 {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }

        XCTAssertEqual(database.subscriptionFetchCount, 1)
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(store: store))
        XCTAssertEqual(
            adapter.events.filter { $0 == "resetSyncCaches" }.count,
            1
        )

        await synchronizer.cancelSynchronizationAndWait()
        do {
            _ = try await synchronization.value
            XCTFail("Expected synchronization cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @BigSyncBackgroundActor
    func testRestoredBackupResetRetainsMarkerAcrossAccountReplacement() async {
        let store = DictionaryKeyValueStore()
        store.set(
            boolValue: true,
            forKey: BackupDetection.restoreResetRequiredStoreKey
        )
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "restore-reset-account-swap"),
            priorities: []
        )
        adapter.resetSyncCachesHandler = {
            database.accountIdentifier = "replacement-account"
        }
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected restored-backup reset to reject the account swap")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(BackupDetection.restoreResetIsRequired(store: store))
        XCTAssertEqual(database.subscriptionFetchCount, 0)
        XCTAssertEqual(
            adapter.events.filter { $0 == "resetSyncCaches" }.count,
            1
        )
    }

    @BigSyncBackgroundActor
    func testCancellationBarrierDoesNotWaitForSubscriptionCallback() async {
        let database = FakeCloudKitDatabase()
        database.completesSubscriptionFetches = false
        let synchronizer = makeSynchronizer(database: database)
        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<1_000 where database.subscriptionFetchCount == 0 {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertEqual(database.subscriptionFetchCount, 1)

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

        for _ in 0..<2 {
            await withCheckedContinuation { continuation in
                synchronizer.subscribeForChangesInDatabase { error in
                    XCTAssertNil(error)
                    continuation.resume()
                }
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
    func testObsoleteZoneDeletionStopsAfterAccountSwitch() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let expectedAccountScope =
            try await synchronizer.cloudKitAccountScopeIdentifier()
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: expectedAccountScope
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )
        database.accountIdentifier = "different-account"

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                CKRecordZone.ID(zoneName: "obsolete-zone"),
                using: receipt
            )
            XCTFail("Expected account switch to stop zone deletion")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
    }

    @BigSyncBackgroundActor
    func testObsoleteZoneDeletionReportsAccountSwitchDuringDeletion() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let expectedAccountScope =
            try await synchronizer.cloudKitAccountScopeIdentifier()
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: expectedAccountScope
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )
        database.accountIdentifierAfterNextZoneDeletion = "different-account"

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                CKRecordZone.ID(zoneName: "obsolete-zone"),
                using: receipt
            )
            XCTFail("Expected account switch to prevent migration completion")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.deletedZoneIDs.count, 1)
    }

    @BigSyncBackgroundActor
    func testObsoleteZoneDeletionReceiptCanRetryAfterTransientFailure() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier:
                    try await synchronizer.cloudKitAccountScopeIdentifier()
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )
        let zoneID = CKRecordZone.ID(zoneName: "transient-obsolete-zone")
        database.deleteZoneError = NSError(
            domain: CKErrorDomain,
            code: CKError.networkFailure.rawValue
        )

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                zoneID,
                using: receipt
            )
            XCTFail("Expected the transient delete failure")
        } catch {
            XCTAssertEqual((error as NSError).code, CKError.networkFailure.rawValue)
        }

        database.deleteZoneError = nil
        let deleted = try await synchronizer.deleteRecordZoneIfPresent(
            zoneID,
            using: receipt
        )
        XCTAssertTrue(deleted)
        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                zoneID,
                using: receipt
            )
            XCTFail("Expected a successfully consumed receipt to reject replay")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }
    }

    @BigSyncBackgroundActor
    func testObsoleteZoneDeletionReceiptRejectsConcurrentReplay() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneDelayNanoseconds = 100_000_000
        let enteredDeletion = expectation(description: "obsolete deletion entered")
        database.deleteZoneHandler = { enteredDeletion.fulfill() }
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier:
                    try await synchronizer.cloudKitAccountScopeIdentifier()
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )
        let zoneID = CKRecordZone.ID(zoneName: "concurrent-obsolete-zone")
        let firstDeletion = Task { @BigSyncBackgroundActor in
            try await synchronizer.deleteRecordZoneIfPresent(
                zoneID,
                using: receipt
            )
        }
        await fulfillment(of: [enteredDeletion], timeout: 1)

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                zoneID,
                using: receipt
            )
            XCTFail("Expected concurrent receipt replay to be rejected")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }

        let firstDeleted = try await firstDeletion.value
        XCTAssertTrue(firstDeleted)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testObsoleteZoneDeletionRejectsReceiptAfterRunInvalidation() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier:
                    try await synchronizer.cloudKitAccountScopeIdentifier()
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )

        // Models account A -> B -> A: the public account string is the same,
        // but cancellation invalidates the old run's one-shot authorization.
        synchronizer.cancelSynchronization()

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                CKRecordZone.ID(zoneName: "obsolete-zone"),
                using: receipt
            )
            XCTFail("Expected the stale receipt to be rejected")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }
        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
    }

    @BigSyncBackgroundActor
    func testObsoleteZoneDeletionRejectsAnActiveAdapterZone() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let activeZoneID = CKRecordZone.ID(zoneName: "active-zone")
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: activeZoneID, priorities: [])
        )
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier:
                    try await synchronizer.cloudKitAccountScopeIdentifier()
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )

        do {
            _ = try await synchronizer.deleteRecordZoneIfPresent(
                activeZoneID,
                using: receipt
            )
            XCTFail("Expected active zone deletion to be rejected")
        } catch OneOffRecordZoneResetError.activeRecordZoneCannotBeDeleted {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
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
    func testOneOffZoneResetRevalidatesAccountBeforeUsingLocalCompletion()
    async throws {
        let keyValueStore = DictionaryKeyValueStore()
        let accounts = AccountIdentifierSequence(["account-a", "account-b"])
        let synchronizer = makeSynchronizer(
            keyValueStore: keyValueStore,
            accountIdentifierProvider: { await accounts.next() }
        )
        let migrationIdentifier = "local-completion-v1"
        let accountKey = Data("account-a".utf8)
            .base64EncodedString()
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "+", with: "-")
        keyValueStore.set(
            boolValue: true,
            forKey: "\(synchronizer.identifier).BigSyncKitMigration."
                + migrationIdentifier + ".\(accountKey).completed"
        )

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: migrationIdentifier,
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail(
                "Expected the local completion fast path to reject the account swap"
            )
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
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
    func testOneOffZoneResetRejectsInvalidLeaseDurationsBeforeCloudKitWork() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)

        for leaseDuration in [0, -1, .infinity, .nan] {
            do {
                _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                    migrationIdentifier: "invalid-lease-\(leaseDuration)",
                    markerRecordType: "ExistingRecordType",
                    markerOwnerField: "owner",
                    markerLeaseDateField: "lease",
                    leaseDuration: leaseDuration
                )
                XCTFail("Expected invalid lease duration to fail")
            } catch OneOffRecordZoneResetError.invalidLeaseDuration {
                // Expected.
            }
        }

        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
        XCTAssertTrue(database.modifyRecordsAtomicValues.isEmpty)
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetRenewsClaimDuringLongReplacementUpload() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "long-reset-zone")
        let firstSynchronizer = makeSynchronizer(database: database)
        let firstAdapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        firstAdapter.didFinishImportHandler = {
            try? await Task.sleep(nanoseconds: 300_000_000)
        }
        firstSynchronizer.addModelAdapter(firstAdapter)

        let firstReset = Task { @BigSyncBackgroundActor in
            try await firstSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "long-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
        }

        try await Task.sleep(nanoseconds: 160_000_000)

        let secondSynchronizer = makeSynchronizer(database: database)
        secondSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        do {
            _ = try await secondSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "long-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
            XCTFail("Expected the renewed claim to remain owned by the first device")
        } catch OneOffRecordZoneResetError.migrationInProgress {
        }

        let firstResult = try await firstReset.value
        XCTAssertEqual(firstResult, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetRenewsClaimDuringSlowZoneDeletion() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneDelayNanoseconds = 300_000_000
        let zoneID = CKRecordZone.ID(zoneName: "slow-delete-reset-zone")
        let enteredDeletion = expectation(description: "zone deletion entered")
        database.deleteZoneHandler = { enteredDeletion.fulfill() }
        let firstSynchronizer = makeSynchronizer(database: database)
        firstSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )

        let firstReset = Task { @BigSyncBackgroundActor in
            try await firstSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "slow-delete-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
        }
        await fulfillment(of: [enteredDeletion], timeout: 1)
        try await Task.sleep(nanoseconds: 160_000_000)

        let secondSynchronizer = makeSynchronizer(database: database)
        secondSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        do {
            _ = try await secondSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "slow-delete-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
            XCTFail("Expected the slow deletion claim to remain owned")
        } catch OneOffRecordZoneResetError.migrationInProgress {
        }

        let firstResult = try await firstReset.value
        XCTAssertEqual(firstResult, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetDoesNotCompeteWithItsClaimHeartbeat() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneDelayNanoseconds = 80_000_000
        database.migrationClaimFetchDelayNanoseconds = 100_000_000
        let zoneID = CKRecordZone.ID(zoneName: "single-writer-reset-zone")
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )

        let result = try await synchronizer.performOneOffRecordZoneResetAndReupload(
            migrationIdentifier: "single-writer-reset-v1",
            markerRecordType: "ExistingRecordType",
            markerOwnerField: "owner",
            markerLeaseDateField: "lease",
            leaseDuration: 0.09
        )

        XCTAssertEqual(result, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetRejectsActorLocalConcurrentInvocation() async throws {
        let database = FakeCloudKitDatabase()
        database.deleteZoneDelayNanoseconds = 100_000_000
        let enteredDeletion = expectation(description: "reset deletion entered")
        database.deleteZoneHandler = { enteredDeletion.fulfill() }
        let zoneID = CKRecordZone.ID(zoneName: "single-flight-reset-zone")
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        let firstReset = Task { @BigSyncBackgroundActor in
            try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "single-flight-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
        }
        await fulfillment(of: [enteredDeletion], timeout: 1)

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "single-flight-v2",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail("Expected the actor-local reset to remain single flight")
        } catch OneOffRecordZoneResetError.migrationInProgress {
        }

        let firstResult = try await firstReset.value
        XCTAssertEqual(firstResult, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testLongOneOffResetIdentifiersWithCommonPrefixDoNotCollide() async throws {
        let database = FakeCloudKitDatabase()
        let commonPrefix = String(repeating: "same-prefix-", count: 12)
        XCTAssertGreaterThan(commonPrefix.count, 120)
        let zoneID = CKRecordZone.ID(zoneName: "long-identifier-zone")

        let firstSynchronizer = makeSynchronizer(database: database)
        firstSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        let firstResult = try await firstSynchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: commonPrefix + "first",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )

        let secondSynchronizer = makeSynchronizer(database: database)
        secondSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        let secondResult = try await secondSynchronizer
            .performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: commonPrefix + "second",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )

        XCTAssertEqual(firstResult, .performedCloudReset)
        XCTAssertEqual(secondResult, .performedCloudReset)
        XCTAssertEqual(database.deletedZoneIDs, [zoneID, zoneID])
    }

    @BigSyncBackgroundActor
    func testCancelledOneOffResetKeepsClaimUntilUploadDrainStops() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "cancelled-reset-zone")
        let uploadGate = AsyncGate()
        let enteredUpload = expectation(description: "replacement upload entered")
        let firstSynchronizer = makeSynchronizer(database: database)
        let firstAdapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        firstAdapter.didFinishImportHandler = {
            enteredUpload.fulfill()
            await uploadGate.wait()
        }
        firstSynchronizer.addModelAdapter(firstAdapter)

        let firstReset = Task { @BigSyncBackgroundActor in
            try await firstSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "cancelled-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
        }
        await fulfillment(of: [enteredUpload], timeout: 1)

        firstReset.cancel()
        try await Task.sleep(nanoseconds: 160_000_000)

        let secondSynchronizer = makeSynchronizer(database: database)
        secondSynchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        do {
            _ = try await secondSynchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "cancelled-reset-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
            XCTFail("Expected the cancelled reset to retain its claim until the drain stops")
        } catch OneOffRecordZoneResetError.migrationInProgress {
        }

        await uploadGate.open()
        do {
            _ = try await firstReset.value
            XCTFail("Expected reset cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
    }

    @BigSyncBackgroundActor
    func testSuspendedUploadPreparationCannotJoinANewerAttempt() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "stale-upload-preparation")
        let record = CKRecord(
            recordType: "Item",
            recordID: CKRecord.ID(
                recordName: "Item.stale",
                zoneID: zoneID
            )
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: ["Item": [record]]
        )
        let gate = AsyncGate()
        let enteredPreparation = expectation(
            description: "old upload preparation suspended"
        )
        adapter.recordsToUploadHandler = {
            enteredPreparation.fulfill()
            await gate.wait()
        }
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        let oldUpload = Task { @BigSyncBackgroundActor in
            try await synchronizer.uploadRecordsIfNeeded(
                adapter: adapter,
                restrictedToEntityType: nil
            )
        }
        await fulfillment(of: [enteredPreparation], timeout: 1)

        synchronizer.cancelSynchronization()
        synchronizer.cancelSync = false
        await gate.open()

        do {
            try await oldUpload.value
            XCTFail("Expected the stale upload attempt to be rejected")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertTrue(database.modifyRecordsAtomicValues.isEmpty)
    }

    @BigSyncBackgroundActor
    func testCancelledOneOffResetDoesNotWaitForMissingZoneFetchCallback()
    async throws {
        let database = FakeCloudKitDatabase()
        database.completesRecordZoneFetches = false
        let enteredZoneFetch = expectation(
            description: "record-zone fetch entered"
        )
        database.recordZoneFetchHandler = {
            enteredZoneFetch.fulfill()
        }
        let zoneID = CKRecordZone.ID(zoneName: "missing-zone-callback")
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(
            FakeModelAdapter(zoneID: zoneID, priorities: [])
        )
        let finished = expectation(description: "cancelled reset returned")
        let reset = Task { @BigSyncBackgroundActor in
            defer { finished.fulfill() }
            return try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "missing-zone-callback-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease",
                leaseDuration: 0.09
            )
        }
        await fulfillment(of: [enteredZoneFetch], timeout: 1)
        XCTAssertEqual(database.recordZoneFetchCount, 1)

        reset.cancel()
        await fulfillment(of: [finished], timeout: 5)
        do {
            _ = try await reset.value
            XCTFail("Expected reset cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertEqual(database.deletedZoneIDs, [zoneID])
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
    func testOneOffZoneResetStopsWhenAccountChangesAfterCompletionMarkerFetch()
    async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        database.accountIdentifierAfterNextRecordFetch = "account-b"
        let zoneID = CKRecordZone.ID(zoneName: "completion-fetch-account-change")
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "completion-fetch-account-change-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail("Expected the account fence to reject the reset")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
            XCTAssertTrue(database.deletedZoneIDs.isEmpty)
            XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        }
    }

    @BigSyncBackgroundActor
    func testOneOffZoneResetStopsWhenAccountChangesDuringInitialClaimSave()
    async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        database.accountIdentifierAfterNextMigrationMarkerSave = "account-b"
        let zoneID = CKRecordZone.ID(zoneName: "claim-save-account-change")
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.performOneOffRecordZoneResetAndReupload(
                migrationIdentifier: "claim-save-account-change-v1",
                markerRecordType: "ExistingRecordType",
                markerOwnerField: "owner",
                markerLeaseDateField: "lease"
            )
            XCTFail("Expected the account fence to reject the reset")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
            XCTAssertTrue(database.deletedZoneIDs.isEmpty)
            XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
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

    @BigSyncBackgroundActor
    func testAccountChangeDuringCacheResetLeavesValidationRequired()
    async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(
                zoneName: "account-reset-race",
                ownerName: CKCurrentUserDefaultName
            ),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)
        try await synchronizer._test_validateSynchronizationAccount()

        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()

        let gate = AsyncGate()
        let enteredReset = expectation(description: "entered cache reset")
        adapter.resetSyncCachesHandler = {
            enteredReset.fulfill()
            await gate.wait()
        }
        let validation = Task { @BigSyncBackgroundActor in
            try await synchronizer._test_validateSynchronizationAccount()
        }
        await fulfillment(of: [enteredReset], timeout: 1)

        database.accountIdentifier = "account-c"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()
        await gate.open()

        do {
            try await validation.value
            XCTFail("Expected the superseded validation to be cancelled")
        } catch is CancellationError {
        }

        adapter.resetSyncCachesHandler = nil
        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertEqual(
            adapter.events.filter { $0 == "resetSyncCaches" }.count,
            2
        )
    }

    @BigSyncBackgroundActor
    func testSynchronizationAccountSwitchRecreatesDatabaseSubscription()
    async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )

        try await synchronizer._test_validateSynchronizationAccount()
        try await synchronizer.subscribeForChangesInDatabase()
        XCTAssertEqual(database.savedSubscriptionCount, 1)

        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()
        synchronizer.beginSynchronization()

        for _ in 0..<1_000 where database.savedSubscriptionCount < 2 {
            try await Task.sleep(nanoseconds: 1_000_000)
        }

        XCTAssertEqual(database.savedSubscriptionCount, 2)
        await synchronizer.cancelSynchronizationAndWait()
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
        let successfulRecord = makeRecord(
            type: "Bookmark",
            id: "successful",
            zoneID: zoneID
        )
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
            uploadedByEntity: [
                "Bookmark": [clientRecord, successfulRecord]
            ]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertTrue(adapter.events.contains("save:Bookmark"))
        XCTAssertTrue(adapter.events.contains("persist"))
        XCTAssertTrue(
            adapter.events.contains("didUpload:Bookmark.successful")
        )
        XCTAssertEqual(database.modifyRecordsAtomicValues, [false])
        XCTAssertEqual(
            database.modifyRecordsSavePolicies,
            [.ifServerRecordUnchanged]
        )
    }

    @BigSyncBackgroundActor
    func testExplicitSynchronizationCancelsDelayedStartupSynchronization() async {
        let backgroundActor = BigSyncBackgroundActor()
        backgroundActor._test_scheduleDormantInitialSynchronization()

        XCTAssertTrue(
            backgroundActor._test_hasScheduledInitialSynchronization
        )
        let result = await backgroundActor.synchronizeCloudKit()
        XCTAssertNil(result)
        XCTAssertFalse(
            backgroundActor._test_hasScheduledInitialSynchronization
        )
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
    func testCancelledFetchedBatchCannotReenterNewRun() async throws {
        let zoneID = CKRecordZone.ID(
            zoneName: "cancelled-batch",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        let processor = ChangeRequestProcessor()
        let oldRunID = await processor.beginRun()
        let enteredSave = expectation(description: "old run entered save")
        let gate = AsyncGate()
        adapter.saveChangesHandler = {
            enteredSave.fulfill()
            await gate.wait()
            try Task.checkCancellation()
        }
        processor.addFetchedChangeRequest(
            ChangeRequest(
                downloadedRecord: makeRecord(
                    type: "OldAccountRecord",
                    id: "old",
                    zoneID: zoneID
                ),
                deletedRecordID: nil,
                adapter: adapter,
                runID: oldRunID
            )
        )

        let oldProcessing = Task { @BigSyncBackgroundActor in
            try await processor.finishProcessing(for: adapter)
        }
        await fulfillment(of: [enteredSave], timeout: 1)

        let newRun = Task { @BigSyncBackgroundActor in
            await processor.beginRun()
        }
        await Task.yield()
        await gate.open()
        let newRunID = await newRun.value
        processor.addFetchedChangeRequest(
            ChangeRequest(
                downloadedRecord: makeRecord(
                    type: "NewAccountRecord",
                    id: "new",
                    zoneID: zoneID
                ),
                deletedRecordID: nil,
                adapter: adapter,
                runID: newRunID
            )
        )
        _ = try? await oldProcessing.value
        adapter.saveChangesHandler = nil

        try await processor.finishProcessing(for: adapter)
        XCTAssertEqual(
            adapter.events.filter { $0 == "save:OldAccountRecord" }.count,
            1
        )
        XCTAssertEqual(
            adapter.events.filter { $0 == "save:NewAccountRecord" }.count,
            1
        )
        XCTAssertFalse(processor.hasPendingChangeRequests(for: adapter))
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
    func testRemoteMapRequiresACompleteValidPayload() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "remote-map-validation",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.attributes["local"] = "value"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        let property = try XCTUnwrap(
            object.objectSchema.properties.first { $0.name == "attributes" }
        )
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["attributes"] = Data([0, 1, 2]) as CKRecordValue

        do {
            try await fixture.targetRealm.asyncWrite {
                try fixture.adapter.applyChange(
                    property: property,
                    record: record,
                    object: object,
                    syncedEntityIdentifier: record.recordID.recordName
                )
            }
            XCTFail("Expected malformed map decoding to fail")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }
        XCTAssertEqual(object.attributes["local"], "value")

        record["attributes"] = try PropertyListSerialization.data(
            fromPropertyList: ["remote": "accepted"],
            format: .binary,
            options: 0
        ) as CKRecordValue
        try await fixture.targetRealm.asyncWrite {
            try fixture.adapter.applyChange(
                property: property,
                record: record,
                object: object,
                syncedEntityIdentifier: record.recordID.recordName
            )
        }
        XCTAssertEqual(
            object.attributes.reduce(into: [String: String]()) {
                $0[$1.key] = $1.value
            },
            ["remote": "accepted"]
        )
    }

    @BigSyncBackgroundActor
    func testMalformedRemoteChunkDoesNotPublishPersistenceCacheBeforeCorrectedRedelivery() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let malformedObject = BigSyncTrackedObject(
            id: "malformed-chunk",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        malformedObject.attributes["local"] = "malformed"
        let validObject = BigSyncTrackedObject(
            id: "valid-chunk",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        validObject.attributes["local"] = "valid"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(malformedObject)
            fixture.targetRealm.add(validObject)
        }

        func remoteRecord(id: String, attributes: CKRecordValue) -> CKRecord {
            let record = makeRecord(
                type: BigSyncTrackedObject.className(),
                id: id,
                zoneID: fixture.adapter.recordZoneID
            )
            let remoteDate = Date().addingTimeInterval(60)
            record["modifiedAt"] = remoteDate as CKRecordValue
            record["explicitlyModifiedAt"] = remoteDate as CKRecordValue
            record["attributes"] = attributes
            return record
        }
        let malformedRecord = remoteRecord(
            id: malformedObject.id,
            attributes: Data([0, 1, 2]) as CKRecordValue
        )
        let validMap = try PropertyListSerialization.data(
            fromPropertyList: ["remote": "valid"],
            format: .binary,
            options: 0
        )
        let validRecord = remoteRecord(
            id: validObject.id,
            attributes: validMap as CKRecordValue
        )

        do {
            try await fixture.adapter.saveChanges(
                in: [validRecord, malformedRecord],
                forceSave: true
            )
            XCTFail("Expected the malformed record to reject the complete chunk")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }

        XCTAssertEqual(malformedObject.attributes["local"], "malformed")
        XCTAssertEqual(validObject.attributes["local"], "valid")
        for record in [malformedRecord, validRecord] {
            XCTAssertNil(
                fixture.persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: record.recordID.recordName
                ),
                "A failed target chunk published system-field cache state"
            )
        }

        malformedRecord["attributes"] = try PropertyListSerialization.data(
            fromPropertyList: ["remote": "corrected"],
            format: .binary,
            options: 0
        ) as CKRecordValue
        try await fixture.adapter.saveChanges(
            in: [validRecord, malformedRecord],
            forceSave: true
        )

        XCTAssertEqual(malformedObject.attributes["remote"], "corrected")
        XCTAssertEqual(validObject.attributes["remote"], "valid")
        for record in [malformedRecord, validRecord] {
            XCTAssertNotNil(
                fixture.persistenceRealm.object(
                    ofType: SyncedEntity.self,
                    forPrimaryKey: record.recordID.recordName
                )?.encodedRecord
            )
        }
    }

    @BigSyncBackgroundActor
    func testRedeliveryFinalizesPersistenceAfterTargetCommitInterruption() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "interrupted-finalization",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.attributes["local"] = "value"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        let remoteDate = Date().addingTimeInterval(60)
        record["modifiedAt"] = remoteDate as CKRecordValue
        record["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        record["attributes"] = try PropertyListSerialization.data(
            fromPropertyList: ["remote": "value"],
            format: .binary,
            options: 0
        ) as CKRecordValue
        fixture.adapter._testBeforeImportedRecordPersistenceWrite = {
            throw CancellationError()
        }

        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected interruption after target publication")
        } catch is CancellationError {
            // Expected.
        }
        fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil

        XCTAssertEqual(object.attributes["remote"], "value")
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )

        // The committed inbound target write must retain its suppression
        // marker instead of being re-journaled as a local upload.
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )

        // CloudKit has not advanced its token, so the same record is
        // redelivered. Even though the target now matches, cache finalization
        // must run and publish the system fields.
        try await fixture.adapter.saveChanges(in: [record], forceSave: false)
        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )?.encodedRecord
        )
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
    func testMalformedPresentCollectionRollsBackWholeRemoteRecord() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let originalDate = Date(timeIntervalSinceReferenceDate: 12_000)
        let object = BigSyncTrackedObject(
            id: "malformed-collection",
            createdAt: originalDate,
            modifiedAt: originalDate,
            explicitlyModifiedAt: originalDate
        )
        object.tags.append("local")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }

        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        let remoteDate = originalDate.addingTimeInterval(60)
        record["createdAt"] = originalDate as CKRecordValue
        record["modifiedAt"] = remoteDate as CKRecordValue
        record["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["tags"] = [42] as CKRecordValue

        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected malformed collection decoding to fail")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(object.modifiedAt, originalDate)
        XCTAssertEqual(Array(object.tags), ["local"])
    }

    @BigSyncBackgroundActor
    func testMalformedRelationshipDoesNotClearExistingTargets() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let child = BigSyncRelationshipChild()
        child.id = "existing-child"
        let parent = BigSyncRelationshipParent()
        parent.id = "malformed-relationship-parent"
        parent.children.append(child)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add([child, parent], update: .modified)
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        let remoteDate = Date().addingTimeInterval(60)
        record["createdAt"] = parent.createdAt as CKRecordValue
        record["modifiedAt"] = remoteDate as CKRecordValue
        record["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["children"] = ["not-a-cloudkit-record-name"] as CKRecordValue

        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected malformed relationship decoding to fail")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(parent.children.map(\.id), [child.id])
    }

    @BigSyncBackgroundActor
    func testRelationshipListRejectsWrongEntityType() async throws {
        try await assertMalformedRelationshipDoesNotApply(
            field: "children",
            value: ["UnexpectedType.existing-child"] as CKRecordValue
        )
    }

    @BigSyncBackgroundActor
    func testRelationshipSetRejectsWrongEntityType() async throws {
        try await assertMalformedRelationshipDoesNotApply(
            field: "relatedChildren",
            value: ["UnexpectedType.existing-child"] as CKRecordValue
        )
    }

    @BigSyncBackgroundActor
    func testToOneRelationshipRejectsWrongEntityType() async throws {
        try await assertMalformedRelationshipDoesNotApply(
            field: "favoriteChild",
            value: "UnexpectedType.existing-child" as CKRecordValue
        )
    }

    @BigSyncBackgroundActor
    func testRelationshipReferenceRejectsAnotherZone() async throws {
        let otherZone = CKRecordZone.ID(
            zoneName: "another-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let reference = CKRecord.Reference(
            recordID: CKRecord.ID(
                recordName: BigSyncRelationshipChild.className() + ".existing-child",
                zoneID: otherZone
            ),
            action: .none
        )
        try await assertMalformedRelationshipDoesNotApply(
            field: "favoriteChild",
            value: reference
        )
    }

    @BigSyncBackgroundActor
    func testRelationshipReferenceCollectionsRejectAnotherZone() async throws {
        let otherZone = CKRecordZone.ID(
            zoneName: "another-collection-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let reference = CKRecord.Reference(
            recordID: CKRecord.ID(
                recordName: BigSyncRelationshipChild.className() + ".existing-child",
                zoneID: otherZone
            ),
            action: .none
        )

        try await assertMalformedRelationshipDoesNotApply(
            field: "children",
            value: [reference] as CKRecordValue
        )
        try await assertMalformedRelationshipDoesNotApply(
            field: "relatedChildren",
            value: [reference] as CKRecordValue
        )
    }

    @BigSyncBackgroundActor
    private func assertMalformedRelationshipDoesNotApply(
        field: String,
        value: CKRecordValue
    ) async throws {
        let fixture = try await makeRealmAdapterFixture()
        let child = BigSyncRelationshipChild()
        child.id = "existing-child"
        let parent = BigSyncRelationshipParent()
        parent.id = "wrong-relationship-type-\(field)"
        parent.children.append(child)
        parent.relatedChildren.insert(child)
        parent.favoriteChild = child
        let originalDate = parent.modifiedAt
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add([child, parent], update: .modified)
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        let remoteDate = originalDate.addingTimeInterval(60)
        record["createdAt"] = parent.createdAt as CKRecordValue
        record["modifiedAt"] = remoteDate as CKRecordValue
        record["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record[field] = value

        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected invalid relationship reference to fail")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(parent.children.map(\.id), [child.id])
        XCTAssertEqual(Set(parent.relatedChildren.map(\.id)), Set([child.id]))
        XCTAssertEqual(parent.favoriteChild?.id, child.id)
        XCTAssertEqual(parent.modifiedAt, originalDate)
        XCTAssertTrue(fixture.persistenceRealm.objects(PendingRelationship.self).isEmpty)
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
    func testUnknownCloudKitItemWithMatchingGenerationIsRequeuedAsNew() async throws {
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
        let preparedGeneration = try XCTUnwrap(tracked.pendingGeneration)
        try await fixture.persistenceRealm.asyncWrite {
            tracked.entityState = .changed
            tracked.encodedRecord = Data([1, 2, 3])
        }

        try await fixture.adapter.requeueMissingServerRecords(
            [CKRecord.ID(recordName: recordName, zoneID: fixture.adapter.recordZoneID)],
            matchingPreparedGenerations: [recordName: preparedGeneration]
        )

        let requeued = try XCTUnwrap(
            fixture.persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: recordName)
        )
        XCTAssertEqual(requeued.entityState, .new)
        XCTAssertNil(requeued.encodedRecord)
        XCTAssertEqual(requeued.pendingGeneration, preparedGeneration)
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
        let initialPrepared = try await fixture.adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let initial = try XCTUnwrap(initialPrepared.first)
        let initialRecord = initial.record
        try await fixture.adapter.didUpload(savedRecords: [initialRecord], matchingGenerations: [initialRecord.recordID.recordName: try XCTUnwrap(initial.generation)])

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
        let forwardedMutationCount = try await fixture.adapter
            ._test_forwardPendingMutations(in: fixture.targetRealm)
        XCTAssertEqual(forwardedMutationCount, 1)

        try await fixture.adapter.requeueMissingServerRecords(
            [initialRecord.recordID],
            matchingPreparedGenerations: [initialRecord.recordID.recordName: firstGeneration]
        )
        let afterMissingServerRecovery = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: initialRecord.recordID.recordName
            )
        )
        XCTAssertEqual(afterMissingServerRecovery.entityState, .changed)
        XCTAssertEqual(afterMissingServerRecovery.pendingGeneration, secondGeneration)

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
        let initialPrepared = try await fixture.adapter.preparedRecordsToUpload(
            limit: 1,
            restrictedToEntityType: nil
        )
        let initial = try XCTUnwrap(initialPrepared.first)
        let initialRecord = initial.record
        try await fixture.adapter.didUpload(savedRecords: [initialRecord], matchingGenerations: [initialRecord.recordID.recordName: try XCTUnwrap(initial.generation)])

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
    func testOpaquePreparedUploadBatchSupportsPartialAcknowledgement() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "partial-upload",
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
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )

        let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        let acknowledged = try XCTUnwrap(batch.records.first)

        try await fixture.adapter.acknowledgeUploadedRecords([], from: batch)
        XCTAssertNotEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: acknowledged.recordID.recordName
            )?.entityState,
            .synced
        )

        try await fixture.adapter.acknowledgeUploadedRecords(
            [acknowledged],
            from: batch
        )

        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: acknowledged.recordID.recordName
            )?.entityState,
            .synced
        )
    }

    @BigSyncBackgroundActor
    func testOpaquePreparedUploadBatchRejectsAnotherAdapter() async throws {
        let firstFixture = try await makeRealmAdapterFixture()
        let secondFixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "wrong-adapter-batch",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await firstFixture.targetRealm.asyncWrite {
            firstFixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await firstFixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: firstFixture.targetRealm
        )
        let batch = try await firstFixture.adapter.prepareUploadBatch(limit: 1)

        do {
            try await secondFixture.adapter.acknowledgeUploadedRecords(
                batch.records,
                from: batch
            )
            XCTFail("Expected an adapter ownership error")
        } catch RealmSwiftAdapterAcknowledgementError.batchBelongsToAnotherAdapter {
            // Expected.
        }

        let preparedRecord = try XCTUnwrap(batch.records.first)
        let foreignRecord = CKRecord(
            recordType: preparedRecord.recordType,
            recordID: CKRecord.ID(
                recordName: preparedRecord.recordID.recordName,
                zoneID: CKRecordZone.ID(zoneName: "other-zone")
            )
        )
        do {
            try await firstFixture.adapter.acknowledgeUploadedRecords(
                [foreignRecord],
                from: batch
            )
            XCTFail("Expected an unprepared-record error")
        } catch RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared {
            // Expected.
        }
    }

    @BigSyncBackgroundActor
    func testOpaqueUploadBatchReplayDoesNotOverwriteNewerGeneration() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "replayed-upload",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let batch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let record = try XCTUnwrap(batch.records.first)
        try await fixture.adapter.acknowledgeUploadedRecords([record], from: batch)

        try await fixture.targetRealm.asyncWrite {
            object.tags.append("newer-edit")
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )
        let newerGeneration = try XCTUnwrap(tracking.pendingGeneration)
        let encodedBeforeReplay = tracking.encodedRecord
        record["tags"] = ["stale-response"] as CKRecordValue

        try await fixture.adapter.acknowledgeUploadedRecords([record], from: batch)

        XCTAssertEqual(tracking.entityState, .changed)
        XCTAssertEqual(tracking.pendingGeneration, newerGeneration)
        XCTAssertEqual(tracking.encodedRecord, encodedBeforeReplay)
    }

    @BigSyncBackgroundActor
    func testOpaqueDeletionBatchValidatesFullIDAndFailsClosed() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "opaque-deletion",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let uploadBatch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let uploaded = try XCTUnwrap(uploadBatch.records.first)
        try await fixture.adapter.acknowledgeUploadedRecords(
            [uploaded],
            from: uploadBatch
        )
        try await fixture.targetRealm.asyncWrite {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let forwardedCount = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        XCTAssertEqual(forwardedCount, 1)
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
        let deletionGeneration = try XCTUnwrap(tracking.pendingGeneration)
        XCTAssertEqual(tracking.entityState, .deletedLocally)
        let deletionBatch = try await fixture.adapter.prepareDeletionBatch(limit: 1)
        let preparedID = try XCTUnwrap(deletionBatch.recordIDs.first)
        let foreignID = CKRecord.ID(
            recordName: preparedID.recordName,
            zoneID: CKRecordZone.ID(zoneName: "other-zone")
        )

        do {
            try await fixture.adapter.acknowledgeDeletedRecordIDs(
                [foreignID],
                from: deletionBatch
            )
            XCTFail("Expected an unprepared-record error")
        } catch RealmSwiftAdapterAcknowledgementError.recordWasNotPrepared {
            // Expected.
        }
        await fixture.adapter.didDelete(recordIDs: [preparedID])
        XCTAssertEqual(tracking.entityState, .deletedLocally)
        XCTAssertEqual(tracking.pendingGeneration, deletionGeneration)

        try await fixture.adapter.acknowledgeDeletedRecordIDs(
            [preparedID],
            from: deletionBatch
        )
        XCTAssertEqual(tracking.entityState, .deletedRemotely)
        XCTAssertNil(tracking.pendingGeneration)
    }

    @BigSyncBackgroundActor
    func testGenerationlessUploadAcknowledgementFailsClosed() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "generationless-ack",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let batch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let record = try XCTUnwrap(batch.records.first)

        do {
            try await fixture.adapter.didUpload(savedRecords: [record])
            XCTFail("Expected a prepared-generation error")
        } catch RealmSwiftAdapterAcknowledgementError.preparedGenerationRequired {
            // Expected.
        }

        XCTAssertNotEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )?.entityState,
            .synced
        )
    }

    @BigSyncBackgroundActor
    func testGenerationlessIdentifierDeletionAcknowledgementFailsClosed() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let identifier = "BigSyncTrackedObject.generationless-delete"
        let tracking = SyncedEntity(
            entityType: BigSyncTrackedObject.className(),
            identifier: identifier,
            state: SyncedEntityState.deletedLocally.rawValue
        )
        tracking.pendingGeneration = "current-generation"
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(tracking)
        }

        do {
            try await fixture.adapter.didDelete(identifiers: [identifier])
            XCTFail("Expected a prepared-generation error")
        } catch RealmSwiftAdapterAcknowledgementError.preparedGenerationRequired {
            // Expected.
        }

        let preserved = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: identifier
            )
        )
        XCTAssertEqual(preserved.entityState, .deletedLocally)
        XCTAssertEqual(preserved.pendingGeneration, "current-generation")
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
    func testCreateThenDeleteBeforeFirstForwardProducesDurableTombstone() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "deleted-before-first-forward",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
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

        let forwarded = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )

        XCTAssertEqual(forwarded, 1)
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .deletedLocally)
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
        let deletionIDs = try await fixture.adapter
            .prepareDeletionBatch(limit: 10).recordIDs
        XCTAssertEqual(
            deletionIDs,
            [CKRecord.ID(recordName: recordName, zoneID: fixture.adapter.recordZoneID)]
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
        let uploadBatch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let uploaded = try XCTUnwrap(uploadBatch.records.first)
        try await fixture.adapter.acknowledgeUploadedRecords(
            [uploaded],
            from: uploadBatch
        )

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
    func testLocalEditCommittedAtRemoteImportBoundaryWins() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let baselineDate = Date(timeIntervalSinceReferenceDate: 10_000)
        let object = BigSyncTrackedObject(
            id: "import-boundary-edit",
            createdAt: baselineDate,
            modifiedAt: baselineDate,
            explicitlyModifiedAt: baselineDate
        )
        object.tags.append("baseline")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }

        let remoteDate = baselineDate.addingTimeInterval(120)
        let remoteRecord = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        remoteRecord["createdAt"] = baselineDate as CKRecordValue
        remoteRecord["modifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["isDeleted"] = false as CKRecordValue
        remoteRecord["tags"] = ["remote"] as CKRecordValue

        let localDate = baselineDate.addingTimeInterval(60)
        fixture.adapter._testBeforeImportedRecordTargetWrite = {
            try await fixture.targetRealm.asyncWrite {
                object.tags.removeAll()
                object.tags.append("local")
                object.refreshChangeMetadata(
                    explicitlyModified: true,
                    at: localDate
                )
            }
        }

        try await fixture.adapter.saveChanges(
            in: [remoteRecord],
            forceSave: true
        )
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(Array(object.tags), ["local"])
        XCTAssertEqual(object.modifiedAt, localDate)
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .changed)
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
    }

    @BigSyncBackgroundActor
    func testForwardedLocalMutationAtRemoteImportBoundaryKeepsItsTrackingGeneration()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let baselineDate = Date(timeIntervalSinceReferenceDate: 11_000)
        let localDate = baselineDate.addingTimeInterval(60)
        let object = BigSyncTrackedObject(
            id: "import-forwarded-boundary-edit",
            createdAt: baselineDate,
            modifiedAt: baselineDate,
            explicitlyModifiedAt: baselineDate
        )
        object.tags.append("baseline")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }

        let remoteRecord = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        let remoteDate = baselineDate.addingTimeInterval(120)
        remoteRecord["createdAt"] = baselineDate as CKRecordValue
        remoteRecord["modifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["isDeleted"] = false as CKRecordValue
        remoteRecord["tags"] = ["remote"] as CKRecordValue

        fixture.adapter._testBeforeImportedRecordTargetWrite = {
            try await fixture.targetRealm.asyncWrite {
                object.tags.removeAll()
                object.tags.append("local")
                object.refreshChangeMetadata(
                    explicitlyModified: true,
                    at: localDate
                )
            }
            _ = try await fixture.adapter._test_forwardPendingMutations(
                in: fixture.targetRealm
            )
        }

        try await fixture.adapter.saveChanges(in: [remoteRecord], forceSave: true)

        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .new)
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
        let upload = try await fixture.adapter.prepareUploadBatch(limit: 1)
        XCTAssertEqual(upload.records.map(\.recordID), [remoteRecord.recordID])
    }

    @BigSyncBackgroundActor
    func testNormalRedeliveryRepairsSystemFieldsAfterImportedPersistenceFailure()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let date = Date(timeIntervalSinceReferenceDate: 12_000)
        let object = BigSyncTrackedObject(
            id: "import-persistence-retry",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: date
        )
        object.tags.append("remote")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }

        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["createdAt"] = date as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["tags"] = ["remote"] as CKRecordValue

        fixture.adapter._testBeforeImportedRecordPersistenceWrite = {
            throw TestSynchronizationError.importedPersistenceCacheFailed
        }
        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected persistence-cache publication to fail")
        } catch TestSynchronizationError.importedPersistenceCacheFailed {
            // Expected. The target Realm has already committed, but no
            // tracking/system-field cache publication is allowed.
        }
        fixture.adapter._testBeforeImportedRecordPersistenceWrite = nil

        let recordName = record.recordID.recordName
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )

        try await fixture.adapter.saveChanges(in: [record], forceSave: false)

        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertNotNil(tracking.encodedRecord)
        XCTAssertEqual(Array(object.tags), ["remote"])
    }

    @BigSyncBackgroundActor
    func testLocalEditCommittedAtRemoteDeletionBoundaryWins() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "deletion-boundary-edit",
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
        let uploadBatch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let uploaded = try XCTUnwrap(uploadBatch.records.first)
        try await fixture.adapter.acknowledgeUploadedRecords(
            [uploaded],
            from: uploadBatch
        )

        fixture.adapter._testBeforeRemoteDeletionTargetWrite = {
            try await fixture.targetRealm.asyncWrite {
                object.tags.append("committed-at-boundary")
                object.refreshChangeMetadata(explicitlyModified: true)
            }
        }
        try await fixture.adapter.deleteRecords(with: [uploaded.recordID])
        await fixture.targetRealm.asyncRefresh()

        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(Array(object.tags), ["committed-at-boundary"])
        let mutation = try XCTUnwrap(
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
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
    }

    @BigSyncBackgroundActor
    func testLocalEditCommittedAtCleanupBoundaryPreventsHardDeletion()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "cleanup-boundary-edit",
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
        let uploadBatch = try await fixture.adapter.prepareUploadBatch(limit: 1)
        let uploaded = try XCTUnwrap(uploadBatch.records.first)
        try await fixture.adapter.acknowledgeUploadedRecords(
            [uploaded],
            from: uploadBatch
        )
        try await fixture.adapter.deleteRecords(with: [uploaded.recordID])
        XCTAssertTrue(object.isDeleted)

        fixture.adapter._testBeforeCleanupTargetWrite = {
            try await fixture.targetRealm.asyncWrite {
                object.isDeleted = false
                object.tags.append("resurrected-at-boundary")
                object.refreshChangeMetadata(explicitlyModified: true)
            }
        }
        try await fixture.adapter.cleanUp()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncTrackedObject.self,
                forPrimaryKey: object.id
            )
        )
        XCTAssertFalse(object.isDeleted)
        XCTAssertEqual(Array(object.tags), ["resurrected-at-boundary"])
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: uploaded.recordID.recordName
            )?.entityState,
            .new
        )
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
    func testDeferredRemoteRelationshipCannotOverwriteNewerLocalEdit()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let originalChild = BigSyncRelationshipChild()
        originalChild.id = "original"
        let availableRemoteChild = BigSyncRelationshipChild()
        availableRemoteChild.id = "available"
        let localChild = BigSyncRelationshipChild()
        localChild.id = "local"
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        parent.children.append(originalChild)
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(
                [originalChild, availableRemoteChild, localChild, parent],
                update: .modified
            )
        }

        let remoteDate = Date(timeIntervalSinceReferenceDate: 30_000)
        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["children"] = [
            "\(BigSyncRelationshipChild.className()).available",
            "\(BigSyncRelationshipChild.className()).late",
        ] as CKRecordValue
        record["modifiedAt"] = remoteDate as CKRecordValue
        record["explicitlyModifiedAt"] = remoteDate as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        try await fixture.adapter.persistImportedChanges()
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            2
        )

        let localEditDate = remoteDate.addingTimeInterval(60)
        try await fixture.targetRealm.asyncWrite {
            parent.children.removeAll()
            parent.children.append(localChild)
            parent.refreshChangeMetadata(
                explicitlyModified: true,
                at: localEditDate
            )
        }
        let lateRemoteChild = BigSyncRelationshipChild()
        lateRemoteChild.id = "late"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(lateRemoteChild)
        }

        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(parent.children.map(\.id), ["local"])
        XCTAssertEqual(parent.modifiedAt, localEditDate)
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            0
        )
    }

    @BigSyncBackgroundActor
    func testTerminalImportUsesCompletedSetupAndWakesForNewJournalEntry()
    async throws {
        // Review finding 1: rerunning setup here forwards with notifyDelegate=false
        // and suppresses the only wakeup for this durable mutation.
        let fixture = try await makeRealmAdapterFixture()
        let delegate = FakeModelAdapterDelegate()
        fixture.adapter.modelAdapterDelegate = delegate
        let object = BigSyncTrackedObject(
            id: "terminal-ready-journal",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className()
            + ".terminal-ready-journal"
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )

        try await fixture.adapter.didFinishImport()

        XCTAssertEqual(delegate.uploadWakeupCount, 1)
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.pendingGeneration,
            generation
        )
    }

    @BigSyncBackgroundActor
    func testJournalForwardingPreservesEveryGenerationAcrossPages() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let recordCount = 1_001
        let objects = (0..<recordCount).map { index in
            BigSyncTrackedObject(
                id: "paged-journal-\(index)",
                createdAt: Date(),
                modifiedAt: Date(),
                explicitlyModifiedAt: nil
            )
        }
        try await fixture.targetRealm.asyncWrite {
            for object in objects {
                fixture.targetRealm.add(object)
                object.refreshChangeMetadata(explicitlyModified: true)
            }
        }

        let forwardedCount = try await fixture.adapter
            ._test_forwardPendingMutations(in: fixture.targetRealm)

        XCTAssertEqual(forwardedCount, recordCount)
        XCTAssertEqual(
            fixture.persistenceRealm.objects(SyncedEntity.self).where {
                $0.pendingGeneration != nil
            }.count,
            recordCount
        )
    }

    @BigSyncBackgroundActor
    func testInitialSetupFailureRemainsRetryable() async throws {
        // Review finding 1: a setup callback failure must not leave a non-nil
        // provider that later passes readiness checks as an empty adapter.
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "retryable-setup-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "retryable-setup-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "retryable-setup"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let delegate = FakeModelAdapterDelegate()
        delegate.initialSetupHandler = {
            throw TestSynchronizationError.initialSetupFailed
        }
        adapter.modelAdapterDelegate = delegate

        do {
            try await adapter.unsetCancellation()
            XCTFail("Expected initial setup failure")
        } catch TestSynchronizationError.initialSetupFailed {
        }
        XCTAssertEqual(delegate.initialSetupCount, 1)

        delegate.initialSetupHandler = nil
        try await adapter.unsetCancellation()

        XCTAssertEqual(delegate.initialSetupCount, 2)
        XCTAssertNotNil(adapter.realmProvider?.persistenceRealm)
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
    func testCancelledDebouncedJournalForwardingRetainsDurableWakeup()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "cancelled-journal-forward",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )

        fixture.adapter.cancelSynchronization()
        fixture.adapter._test_enqueueObservedJournalRecordNames([recordName])
        do {
            try await fixture.adapter._test_processObservedRealmChanges()
            XCTFail("Expected forwarding cancellation")
        } catch is CancellationError {
        }
        XCTAssertTrue(
            fixture.adapter._test_hasPendingObservedRealmChanges()
        )
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )

        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter._test_processObservedRealmChanges()

        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
        XCTAssertFalse(
            fixture.adapter._test_hasPendingObservedRealmChanges()
        )
    }

    @BigSyncBackgroundActor
    func testBeginBarrierRestartsQueuedInitialSetup() async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "queued-setup-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "queued-setup-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(
                zoneName: "queued-setup-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            logger: Logger(label: "BigSyncKitTests")
        )

        // The actor cannot start the queued bootstrap task until this test
        // suspends, so cancellation deterministically precedes setup entry.
        // This mirrors the normal begin-synchronization barrier: it owns the
        // queued bootstrap task even though the adapter was not first marked
        // cancelled by a destructive reset.
        await adapter.waitForCancellation()
        for _ in 0..<20 {
            await Task.yield()
        }

        XCTAssertNil(adapter.realmProvider)

        try await adapter.unsetCancellation()
        XCTAssertNotNil(adapter.realmProvider)
    }

    @BigSyncBackgroundActor
    func testLegacyObservationDrainsInsideOwnedProcessorTask() async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "legacy-observation-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "legacy-observation-target-\(identifier)"
        targetConfiguration.objectTypes = [BigSyncTrackedObject.self]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(
                zoneName: "legacy-observation-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        let targetRealm = try XCTUnwrap(
            adapter.realmProvider?.targetReaderRealms?.first
        )
        let persistenceRealm = try XCTUnwrap(
            adapter.realmProvider?.persistenceRealm
        )
        adapter.invalidateTokens()
        let changedAt = Date(timeIntervalSinceReferenceDate: 34_000)
        try await targetRealm.asyncWrite {
            targetRealm.add(
                BigSyncTrackedObject(
                    id: "legacy-observed",
                    createdAt: changedAt,
                    modifiedAt: changedAt,
                    explicitlyModifiedAt: changedAt
                )
            )
        }

        adapter._test_enqueueObservedLegacyRealmIndex()
        try await adapter._test_processObservedRealmChanges()

        XCTAssertNotNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey:
                    BigSyncTrackedObject.className() + ".legacy-observed"
            )
        )
    }

    @BigSyncBackgroundActor
    func testCacheResetWaitsForObservedJournalForwardingCancellation()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "reset-journal-barrier",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        let expectedGeneration = mutation.generation
        let enteredForwarding = AsyncGate()
        let releaseForwarding = AsyncGate()
        let finishedReset = AsyncGate()
        fixture.adapter._testBeforePendingMutationTrackingWrite = {
            await enteredForwarding.open()
            await releaseForwarding.wait()
        }
        fixture.adapter._test_enqueueObservedJournalRecordNames([recordName])
        fixture.adapter._test_startObservedRealmChangesTaskIfNeeded()
        await enteredForwarding.wait()

        let reset = Task { @BigSyncBackgroundActor in
            fixture.adapter.cancelSynchronization()
            await fixture.adapter.waitForCancellation()
            try await fixture.adapter.resetSyncCaches()
            await finishedReset.open()
        }
        for _ in 0..<20 {
            await Task.yield()
        }
        let resetCompletedBeforeRelease = await finishedReset.hasOpened()
        XCTAssertFalse(resetCompletedBeforeRelease)

        await releaseForwarding.open()
        try await reset.value
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )

        try await fixture.adapter.unsetCancellation()
        let tracking = try XCTUnwrap(
            fixture.adapter.realmProvider?.persistenceRealm?.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.pendingGeneration, expectedGeneration)
    }

    @BigSyncBackgroundActor
    func testResetRequeuesJournalForwardedBeforeCancellation() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "reset-after-forward-journal",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        let enteredPostWrite = AsyncGate()
        let releasePostWrite = AsyncGate()
        fixture.adapter._testAfterPendingMutationTrackingWrite = {
            await enteredPostWrite.open()
            await releasePostWrite.wait()
        }
        fixture.adapter._test_enqueueObservedJournalRecordNames([recordName])
        fixture.adapter._test_startObservedRealmChangesTaskIfNeeded()
        await enteredPostWrite.wait()

        let reset = Task { @BigSyncBackgroundActor in
            fixture.adapter.cancelSynchronization()
            await fixture.adapter.waitForCancellation()
            try await fixture.adapter.resetSyncCaches()
        }
        for _ in 0..<20 {
            await Task.yield()
        }
        await releasePostWrite.open()
        try await reset.value

        XCTAssertTrue(fixture.adapter._test_hasPendingObservedRealmChanges())
        try await fixture.adapter.unsetCancellation()
        try await fixture.adapter._test_processObservedRealmChanges()

        let tracking = try XCTUnwrap(
            fixture.adapter.realmProvider?.persistenceRealm?.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
        XCTAssertFalse(fixture.adapter._test_hasPendingObservedRealmChanges())
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
            for recovery in fixture.persistenceRealm.objects(SyncedEntityType.self)
            where recovery.entityType.hasPrefix(
                "__BigSyncKitMutationJournalRecovery.v2."
            ) {
                recovery.recoveryVersion = 0
            }
        }

        try await fixture.adapter._test_setup()

        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".pre-journal"
            )
        )
        XCTAssertEqual(
            fixture.persistenceRealm.objects(SyncedEntityType.self)
                .first(where: {
                    $0.entityType.hasPrefix(
                        "__BigSyncKitMutationJournalRecovery.v2."
                    )
                })?.recoveryVersion,
            1
        )
    }

    @BigSyncBackgroundActor
    func testInitialSetupSkipsObjectsThatOptOutOfInitialCloudKitSync() async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier = "persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncRelationshipChild.self,
            BigSyncRelationshipParent.self,
            BigSyncPendingMutation.self,
        ]
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let date = Date(timeIntervalSinceReferenceDate: 25_000)
        let eligible = BigSyncTrackedObject(
            id: "eligible",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: date
        )
        let cacheOnly = BigSyncTrackedObject(
            id: "cache-only",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: date
        )
        cacheOnly.initialCloudKitSyncEligible = false
        try await targetRealm.asyncWrite {
            targetRealm.add(eligible)
            targetRealm.add(cacheOnly)
        }
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(
                zoneName: "initial-eligibility-zone",
                ownerName: CKCurrentUserDefaultName
            ),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )

        try await adapter._test_setup()
        let persistenceRealm = try XCTUnwrap(
            adapter.realmProvider?.persistenceRealm
        )

        XCTAssertNotNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".eligible"
            )
        )
        XCTAssertNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".cache-only"
            )
        )
    }

    @BigSyncBackgroundActor
    func testRecoveryScanSkipsObjectsThatOptOutOfInitialCloudKitSync() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let date = Date(timeIntervalSinceReferenceDate: 26_000)
        let cacheOnly = BigSyncTrackedObject(
            id: "recovery-cache-only",
            createdAt: date,
            modifiedAt: date,
            explicitlyModifiedAt: date
        )
        cacheOnly.initialCloudKitSyncEligible = false
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(cacheOnly)
        }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(
                SyncedEntity(
                    entityType: "Existing",
                    identifier: "Existing.anchor",
                    state: SyncedEntityState.synced.rawValue
                ),
                update: .modified
            )
            let recovery = fixture.persistenceRealm.object(
                ofType: SyncedEntityType.self,
                forPrimaryKey: "__BigSyncKitMutationJournalRecovery"
            )
            recovery?.recoveryVersion = 0
        }

        try await fixture.adapter._test_setup()

        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className() + ".recovery-cache-only"
            )
        )
    }

    @BigSyncBackgroundActor
    func testJournaledMutationIsNotFilteredByInitialSyncEligibility() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "edited-cache-row",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.initialCloudKitSyncEligible = false
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }

        try await fixture.adapter._test_setup()

        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: BigSyncTrackedObject.className()
                    + ".edited-cache-row"
            )
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
