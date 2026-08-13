import XCTest
import CloudKit
import Logging
@testable import BigSyncKit
import RealmSwift
import RealmSwiftGaps

private final class DictionaryKeyValueStore: NSObject, KeyValueStore {
    private var storage = [String: Any]()
    var synchronizesDurably = true

    var propertyListEntries: [String: [String: Any]] {
        storage.compactMapValues { $0 as? [String: Any] }
    }

    func object(forKey defaultName: String) -> Any? { storage[defaultName] }
    func bool(forKey defaultName: String) -> Bool { storage[defaultName] as? Bool ?? false }
    func set(value: Any?, forKey defaultName: String) { storage[defaultName] = value }
    func set(boolValue: Bool, forKey defaultName: String) { storage[defaultName] = boolValue }
    func removeObject(forKey defaultName: String) { storage.removeValue(forKey: defaultName) }
    func synchronize() -> Bool { synchronizesDurably }
}

private enum TestSynchronizationError: Error {
    case initialSetupFailed
    case terminalForwardingFailed
    case importedPersistenceCacheFailed
    case subscriptionMutationFailed
}

@BigSyncBackgroundActor
private final class TerminalCallbackObservation: NSObject {
    private let synchronizer: CloudKitSynchronizer
    private(set) var sawCompletedDrain = false

    init(synchronizer: CloudKitSynchronizer) {
        self.synchronizer = synchronizer
    }

    @objc func synchronizerDidSynchronize(_ notification: Notification) {
        guard notification.object as? CloudKitSynchronizer === synchronizer else {
            return
        }
        sawCompletedDrain = !synchronizer.synchronizationDrainIsActive
        synchronizer.beginSynchronization()
    }
}

private struct FakeZoneChangePage {
    let zoneID: CKRecordZone.ID
    let records: [CKRecord]
    let deletedRecordIDs: [CKRecord.ID]
    let moreComing: Bool
}

private struct FakeDatabaseChangePage {
    let changedZoneIDs: [CKRecordZone.ID]
    let deletions: [CloudKitZoneDeletion]
    let moreComing: Bool
}

private final class FakeCloudKitDatabase: NSObject, CloudKitDatabaseAdapter, @unchecked Sendable {
    var databaseScope: CKDatabase.Scope { .private }
    var deleteZoneError: Error?
    var completesModifyOperations = true
    var completesFetchDatabaseChanges = true
    var completesRecordZoneFetches = true
    var completesRecordZoneSaves = true
    // Allows a zero-zone token probe to reach its terminal callback.
    var completesEmptyZoneChangeOperation = false
    var zoneExists = true
    var recordZoneFetchHandler: (@Sendable () -> Void)?
    var completesSubscriptionFetches = true
    var subscriptionLookupError: Error?
    var subscriptionSaveError: Error?
    var subscriptionDeleteError: Error?
    var accountIdentifierAfterNextSubscriptionLookup: String?
    var accountIdentifierAfterNextSubscriptionSave: String?
    var accountIdentifierAfterNextSubscriptionDelete: String?
    var reportsDeletedRecordsAsUnknownItems = false
    var partialSaveErrorsByRecordID = [CKRecord.ID: NSError]()
    var partialSaveErrorsOnceByRecordID = [CKRecord.ID: NSError]()
    var partialDeleteErrorsByRecordID = [CKRecord.ID: NSError]()
    var partialDeleteErrorsOnceByRecordID = [CKRecord.ID: NSError]()
    var recordMutationTopLevelError: Error?
    /// Test-only one-shot transport failure. A real CloudKit retry receives a
    /// fresh operation, so leaving the injected error installed would model a
    /// permanently failing server rather than an encrypted-key reset event.
    var recordMutationTopLevelErrorOnce: Error?
    var accountIdentifier = "test-account"
    var accountIdentifierAfterNextRecordFetch: String?
    var accountIdentifierAfterNextZoneFetch: String?
    var accountIdentifierAfterNextZoneSave: String?
    // Review findings 3 and 4 test seams for account replacement at callback
    // and final CloudKit mutation boundaries.
    var accountIdentifierAfterNextDatabaseChangesFetch: String?
    var databaseDeletedZoneIDs = [CKRecordZone.ID]()
    var fetchedSubscriptions = [CKSubscription]()
    var accountIdentifierAfterNextMigrationMarkerSave: String?
    var accountIdentifierAfterNextModifyRecords: String?
    var accountIdentifierAfterNextZoneDeletion: String?
    var migrationClaimFetchDelayNanoseconds: UInt64?
    var deleteZoneDelayNanoseconds: UInt64?
    var deleteZoneHandler: (@Sendable () -> Void)?
    var zoneChangePages = [FakeZoneChangePage]()
    var databaseChangePages = [FakeDatabaseChangePage]()
    var nextDatabaseChangesError: Error?
    var nextRecordZoneChangesError: Error?
    private(set) var deletedZoneIDs = [CKRecordZone.ID]()
    private(set) var savedSubscriptionCount = 0
    private(set) var savedSubscriptions = [CKSubscription]()
    private(set) var deletedSubscriptionIDs = [CKSubscription.ID]()
    private(set) var subscriptionFetchCount = 0
    private(set) var modifySubscriptionOperationCount = 0
    private(set) var modifyRecordsOperationCount = 0
    private(set) var recordZoneFetchCount = 0
    private(set) var databaseChangeFetchCount = 0
    private(set) var recordZoneChangeFetchCount = 0
    private(set) var savedZoneCount = 0
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

    func record(for recordID: CKRecord.ID) -> CKRecord? {
        withRecordsLock { records[recordID] }
    }

    func removeAllRecords() {
        withRecordsLock {
            records.removeAll()
            conditionallyFetchedRecordIDs.removeAll()
        }
    }

}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension FakeCloudKitDatabase: CloudKitSubscriptionStore {
    func subscription(withID identifier: CKSubscription.ID) async throws
        -> CKSubscription? {
        subscriptionFetchCount += 1
        if let subscriptionLookupError {
            throw subscriptionLookupError
        }
        guard completesSubscriptionFetches else {
            return try await withCheckedThrowingContinuation {
                (_: CheckedContinuation<CKSubscription?, Error>) in
            }
        }
        let subscription = fetchedSubscriptions.first {
            $0.subscriptionID == identifier
        }
        if let nextAccountIdentifier =
            accountIdentifierAfterNextSubscriptionLookup {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextSubscriptionLookup = nil
        }
        return subscription
    }

    func save(subscription: CKSubscription) async throws -> CKSubscription {
        savedSubscriptionCount += 1
        savedSubscriptions.append(subscription)
        if let subscriptionSaveError {
            throw subscriptionSaveError
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextSubscriptionSave {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextSubscriptionSave = nil
        }
        return subscription
    }

    func deleteSubscription(withID identifier: CKSubscription.ID) async throws {
        deletedSubscriptionIDs.append(identifier)
        if let subscriptionDeleteError {
            throw subscriptionDeleteError
        }
        if let nextAccountIdentifier =
            accountIdentifierAfterNextSubscriptionDelete {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextSubscriptionDelete = nil
        }
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension FakeCloudKitDatabase: CloudKitZoneStore {
    func recordZone(withID identifier: CKRecordZone.ID) async throws
        -> CKRecordZone {
        recordZoneFetchCount += 1
        recordZoneFetchHandler?()
        guard completesRecordZoneFetches else {
            try await Task.sleep(nanoseconds: .max)
            throw CancellationError()
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextZoneFetch {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextZoneFetch = nil
        }
        guard zoneExists else {
            throw CKError(.zoneNotFound)
        }
        return CKRecordZone(zoneID: identifier)
    }

    func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        savedZoneCount += 1
        guard completesRecordZoneSaves else {
            try await Task.sleep(nanoseconds: .max)
            throw CancellationError()
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextZoneSave {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextZoneSave = nil
        }
        zoneExists = true
        return recordZone
    }

    func deleteRecordZone(withID identifier: CKRecordZone.ID) async throws {
        deletedZoneIDs.append(identifier)
        deleteZoneHandler?()
        if let nextAccountIdentifier = accountIdentifierAfterNextZoneDeletion {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextZoneDeletion = nil
        }
        if let deleteZoneDelayNanoseconds {
            try await Task.sleep(nanoseconds: deleteZoneDelayNanoseconds)
        }
        if let deleteZoneError {
            throw deleteZoneError
        }
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension FakeCloudKitDatabase: CloudKitRecordStore {
    func modifyRecords(
        saving recordsToSave: [CKRecord],
        deleting recordIDsToDelete: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        modifyRecordsOperationCount += 1
        modifyRecordsAtomicValues.append(atomically)
        modifyRecordsSavePolicies.append(savePolicy)
        guard completesModifyOperations else {
            try await Task.sleep(nanoseconds: .max)
            throw CancellationError()
        }
        if let recordMutationTopLevelErrorOnce {
            self.recordMutationTopLevelErrorOnce = nil
            throw recordMutationTopLevelErrorOnce
        }
        if let recordMutationTopLevelError {
            throw recordMutationTopLevelError
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextModifyRecords {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextModifyRecords = nil
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextMigrationMarkerSave,
           !recordsToSave.isEmpty {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextMigrationMarkerSave = nil
        }

        var saveResults = [CKRecord.ID: Result<CKRecord, Error>]()
        for record in recordsToSave {
            if let error = partialSaveErrorsOnceByRecordID.removeValue(
                forKey: record.recordID
            ) ?? partialSaveErrorsByRecordID[record.recordID] {
                saveResults[record.recordID] = .failure(error)
                continue
            }
            let existing = withRecordsLock { records[record.recordID] }
            let wasConditionallyFetched = withRecordsLock {
                conditionallyFetchedRecordIDs.contains(record.recordID)
            }
            if savePolicy == .ifServerRecordUnchanged,
               let existing,
               !wasConditionallyFetched {
                saveResults[record.recordID] = .failure(
                    CKError(
                        .serverRecordChanged,
                        userInfo: [CKRecordChangedErrorServerRecordKey: existing]
                    )
                )
                continue
            }
            withRecordsLock {
                records[record.recordID] = record
                conditionallyFetchedRecordIDs.remove(record.recordID)
            }
            saveResults[record.recordID] = .success(record)
        }

        var deleteResults = [CKRecord.ID: Result<Void, Error>]()
        for recordID in recordIDsToDelete {
            if let error = partialDeleteErrorsOnceByRecordID.removeValue(
                forKey: recordID
            ) ?? partialDeleteErrorsByRecordID[recordID] {
                deleteResults[recordID] = .failure(error)
            } else if reportsDeletedRecordsAsUnknownItems {
                deleteResults[recordID] = .failure(CKError(.unknownItem))
            } else {
                _ = withRecordsLock { records.removeValue(forKey: recordID) }
                deleteResults[recordID] = .success(())
            }
        }
        return CloudKitRecordMutationResults(
            saveResults: saveResults,
            deleteResults: deleteResults
        )
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension FakeCloudKitDatabase: CloudKitChangeFeed {
    func databaseChanges(since cursor: DatabaseChangeCursor?, resultsLimit: Int?) async throws -> CloudKitDatabaseChangePage {
        if let nextDatabaseChangesError {
            self.nextDatabaseChangesError = nil
            throw nextDatabaseChangesError
        }
        guard completesFetchDatabaseChanges else {
            return try await withCheckedThrowingContinuation { (_: CheckedContinuation<CloudKitDatabaseChangePage, Error>) in }
        }
        if let nextAccountIdentifier = accountIdentifierAfterNextDatabaseChangesFetch {
            accountIdentifier = nextAccountIdentifier
            accountIdentifierAfterNextDatabaseChangesFetch = nil
        }
        databaseChangeFetchCount += 1
        let page = databaseChangePages.isEmpty
            ? FakeDatabaseChangePage(
                changedZoneIDs: [],
                deletions: databaseDeletedZoneIDs.map {
                    CloudKitZoneDeletion(zoneID: $0, kind: .deleted)
                },
                moreComing: false
            )
            : databaseChangePages.removeFirst()
        return CloudKitDatabaseChangePage(
            cursor: DatabaseChangeCursor(
                serializedData: Data("database-\(databaseChangeFetchCount)".utf8)
            ),
            changedZoneIDs: page.changedZoneIDs,
            deletions: page.deletions,
            moreComing: page.moreComing
        )
    }

    func recordZoneChanges(in zoneID: CKRecordZone.ID, since cursor: RecordZoneChangeCursor?, desiredKeys: [CKRecord.FieldKey]?, resultsLimit: Int?) async throws -> CloudKitRecordZoneChangePage {
        if let nextRecordZoneChangesError {
            self.nextRecordZoneChangesError = nil
            throw nextRecordZoneChangesError
        }
        guard zoneExists else {
            throw CKError(.zoneNotFound)
        }
        guard !zoneChangePages.isEmpty || completesEmptyZoneChangeOperation else {
            return try await withCheckedThrowingContinuation { (_: CheckedContinuation<CloudKitRecordZoneChangePage, Error>) in }
        }
        recordZoneChangeFetchCount += 1
        let page = zoneChangePages.isEmpty
            ? FakeZoneChangePage(zoneID: zoneID, records: [], deletedRecordIDs: [], moreComing: false)
            : zoneChangePages.removeFirst()
        return CloudKitRecordZoneChangePage(
            cursor: RecordZoneChangeCursor(serializedData: Data("zone-\(recordZoneChangeFetchCount)".utf8)),
            records: page.records,
            deletedRecordIDs: page.deletedRecordIDs,
            moreComing: page.moreComing
        )
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

private actor GatedAccountIdentifierSequence {
    private var identifiers: [String]
    private var callCount = 0
    private let gatedCall: Int
    private let enteredGate: AsyncGate
    private let releaseGate: AsyncGate

    init(
        _ identifiers: [String],
        gatedCall: Int,
        enteredGate: AsyncGate,
        releaseGate: AsyncGate
    ) {
        self.identifiers = identifiers
        self.gatedCall = gatedCall
        self.enteredGate = enteredGate
        self.releaseGate = releaseGate
    }

    func next() async -> String {
        callCount += 1
        let identifier: String
        if identifiers.count > 1 {
            identifier = identifiers.removeFirst()
        } else {
            identifier = identifiers[0]
        }
        if callCount == gatedCall {
            await enteredGate.open()
            await releaseGate.wait()
        }
        return identifier
    }
}

private final class FakeModelAdapter:
    NSObject,
    ModelAdapter,
    TerminalSynchronizationStateModelAdapter,
    @unchecked Sendable {
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
    private var storedServerChangeToken: RecordZoneChangeCursor?
    var didFinishImportHandler: (@Sendable () async throws -> Void)?
    var cleanUpHandler: (@Sendable () async throws -> Void)?
    var resetSyncCachesHandler: (@Sendable () async throws -> Void)?
    var saveChangesHandler: (@Sendable () async throws -> Void)?
    var recordsToUploadHandler: (@Sendable () async throws -> Void)?
    var terminalPendingChanges = false
    var repeatsPreparedUploads = false
    var repeatsPreparedDeletions = false

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

    func preparedRecordsToUpload(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordUpload] {
        try await recordsToUploadHandler?()
        let target = restrictedToEntityType ?? nextEntityTypeWithPendingUploads()
        events.append("recordsToUpload:\(target ?? "*")")
        guard let target else { return [] }
        let allRecords = uploadedByEntity[target] ?? []
        let selectedRecords = Array(allRecords.prefix(limit))
        if !repeatsPreparedUploads {
            uploadedByEntity[target] = Array(
                allRecords.dropFirst(selectedRecords.count)
            )
        }
        return selectedRecords.map {
            PreparedRecordUpload(record: $0, generation: nil)
        }
    }

    func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {
        let recordNames = savedRecords.map { $0.recordID.recordName }.joined(separator: ",")
        events.append("didUpload:\(recordNames)")
    }

    func preparedRecordDeletions(
        limit: Int,
        restrictedToEntityType: String?
    ) async throws -> [PreparedRecordDeletion] {
        let target = restrictedToEntityType ?? nextEntityTypeWithPendingDeletions()
        events.append("recordIDsMarkedForDeletion:\(target ?? "*")")
        guard let target else { return [] }
        let allRecordIDs = deletedByEntity[target] ?? []
        let selectedRecordIDs = Array(allRecordIDs.prefix(limit))
        if !repeatsPreparedDeletions {
            deletedByEntity[target] = Array(allRecordIDs.dropFirst(selectedRecordIDs.count))
        }
        return selectedRecordIDs.map {
            PreparedRecordDeletion(recordID: $0, generation: nil)
        }
    }

    func didDelete(
        recordIDs: [CKRecord.ID],
        matchingGenerations: [String: String]
    ) async throws {
        let recordNames = recordIDs.map { $0.recordName }.joined(separator: ",")
        events.append("didDelete:\(recordNames)")
        if repeatsPreparedDeletions {
            let acknowledged = Set(recordIDs)
            for entityType in deletedByEntity.keys {
                deletedByEntity[entityType]?.removeAll { acknowledged.contains($0) }
            }
        }
    }

    func requeueMissingServerRecords(
        _ recordIDs: [CKRecord.ID],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        let recordNames = recordIDs.map(\.recordName).joined(separator: ",")
        events.append("deleteTracking:\(recordNames)")
    }

    @BigSyncBackgroundActor
    func rebasePendingDeletionMetadata(
        using serverRecords: [CKRecord],
        matchingPreparedGenerations: [String: String]
    ) async throws {
        // This fake has no target model values or durable generations to
        // overwrite. Recording the same import/persist boundary models a
        // tombstone-preserving adapter for the mutation-drain behavior tests.
        _ = matchingPreparedGenerations
        try await saveChanges(in: serverRecords, forceSave: true)
        try await persistImportedChanges()
    }

    var serverChangeToken: RecordZoneChangeCursor? {
        get async { storedServerChangeToken }
    }

    func saveToken(_ token: RecordZoneChangeCursor?) async throws {
        storedServerChangeToken = token
        events.append("saveToken")
    }

    func didFinishImport() async throws {
        didFinishImportCount += 1
        try await didFinishImportHandler?()
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {
        events.append("unsetCancellation")
    }

    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        events.append("terminalPendingState")
        return terminalPendingChanges || hasChanges
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
    @Persisted var payload: Data?
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

    @BigSyncBackgroundActor
    func testSyncHealthPersistsAcrossSynchronizerRecreation() async throws {
        let identifier = "sync-health-\(UUID().uuidString)"
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(identifier).plist")
        let first = makeSynchronizer(
            keyValueStore: FileKeyValueStore(fileURL: fileURL),
            identifier: identifier,
            accountIdentifierProvider: { "health-account" }
        )
        let successAt = Date(timeIntervalSinceReferenceDate: 123_456)
        try first._test_recordSyncHealth(
            .succeeded,
            accountIdentifier: "health-account",
            now: successAt
        )

        let reopened = CloudKitSynchronizer(
            identifier: identifier,
            containerIdentifier: "iCloud.test",
            database: FakeCloudKitDatabase(),
            recordZoneID: first.recordZoneID,
            keyValueStore: FileKeyValueStore(fileURL: fileURL),
            accountIdentifierProvider: { "health-account" },
            logger: Logger(label: "BigSyncKitTests")
        )
        let snapshot = try await reopened.syncHealthSnapshot()
        XCTAssertEqual(snapshot?.category, .succeeded)
        XCTAssertEqual(snapshot?.lastSuccessAt, successAt)
        XCTAssertNil(snapshot?.lastFailureAt)
    }

    @BigSyncBackgroundActor
    func testSyncHealthDoesNotCrossAccountScopes() async throws {
        let store = DictionaryKeyValueStore()
        let identifier = "sync-health-\(UUID().uuidString)"
        let synchronizer = makeSynchronizer(
            keyValueStore: store,
            identifier: identifier,
            accountIdentifierProvider: { "first-account" }
        )
        try synchronizer._test_recordSyncHealth(
            .failed,
            accountIdentifier: "first-account",
            now: Date(timeIntervalSinceReferenceDate: 100)
        )
        let differentAccount = makeSynchronizer(
            keyValueStore: store,
            identifier: identifier,
            accountIdentifierProvider: { "second-account" }
        )
        let snapshot = try await differentAccount.syncHealthSnapshot()
        XCTAssertNil(snapshot)
    }

    @BigSyncBackgroundActor
    func testSyncHealthMapsTerminalCategoriesAndPreservesRetryDate() async throws {
        let synchronizer = makeSynchronizer()
        XCTAssertEqual(
            synchronizer.syncHealthCategory(for: CloudKitSynchronizer.SyncError.notAuthenticated),
            .notAuthenticated
        )
        XCTAssertEqual(
            synchronizer.syncHealthCategory(for: CloudKitSynchronizer.SyncError.higherModelVersionFound),
            .higherModelVersion
        )
        XCTAssertEqual(
            synchronizer.syncHealthCategory(
                for: ChangeFeedMigrationError.establishedZoneUnavailable(
                    CKRecordZone.ID(zoneName: "health-terminal"),
                    .unknown
                )
            ),
            .terminalZoneUnavailable
        )
        XCTAssertEqual(
            synchronizer.syncHealthCategory(for: TestSynchronizationError.initialSetupFailed),
            .failed
        )
        XCTAssertEqual(
            synchronizer.syncHealthCategory(for: CancellationError()),
            .idle
        )

        let retryNotBefore = Date(timeIntervalSinceReferenceDate: 456_789)
        try synchronizer._test_recordSyncHealth(
            .transientRetry,
            accountIdentifier: "test-account",
            retryNotBefore: retryNotBefore,
            now: Date(timeIntervalSinceReferenceDate: 456_700)
        )
        let snapshot = try await synchronizer.syncHealthSnapshot()
        XCTAssertEqual(snapshot?.retryNotBefore, retryNotBefore)
    }
    func testFileKeyValueStorePersistsPropertyListValuesAndRemovals() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncKitTests-\(UUID().uuidString)")
            .appendingPathComponent("state.plist")
        let firstStore = FileKeyValueStore(fileURL: fileURL)
        firstStore.set(value: "value", forKey: "string")
        firstStore.set(value: Data([1, 2, 3]), forKey: "data")
        firstStore.set(value: ["key": "value"], forKey: "dictionary")
        firstStore.set(boolValue: true, forKey: "flag")
        XCTAssertTrue(firstStore.synchronize())

        let reopenedStore = FileKeyValueStore(fileURL: fileURL)
        XCTAssertEqual(reopenedStore.object(forKey: "string") as? String, "value")
        XCTAssertEqual(reopenedStore.object(forKey: "data") as? Data, Data([1, 2, 3]))
        XCTAssertEqual(
            reopenedStore.object(forKey: "dictionary") as? [String: String],
            ["key": "value"]
        )
        XCTAssertTrue(reopenedStore.bool(forKey: "flag"))

        reopenedStore.removeObject(forKey: "string")
        XCTAssertNil(FileKeyValueStore(fileURL: fileURL).object(forKey: "string"))
    }

    func testFileKeyValueStoreDoesNotShareStateAcrossFileURLs() {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncKitTests-\(UUID().uuidString)")
        let firstStore = FileKeyValueStore(
            fileURL: rootURL.appendingPathComponent("first.plist")
        )
        let secondStore = FileKeyValueStore(
            fileURL: rootURL.appendingPathComponent("second.plist")
        )

        firstStore.set(value: "first", forKey: "client")

        XCTAssertEqual(firstStore.object(forKey: "client") as? String, "first")
        XCTAssertNil(secondStore.object(forKey: "client"))
    }

    func testFileKeyValueStoreCanPersistWithoutAtomicReplacement() {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncKitTests-\(UUID().uuidString)")
            .appendingPathComponent("state.plist")
        let firstStore = FileKeyValueStore(
            fileURL: fileURL,
            writesAtomically: false
        )
        firstStore.set(value: "value", forKey: "key")

        XCTAssertFalse(firstStore.writesAtomically)
        XCTAssertEqual(
            FileKeyValueStore(fileURL: fileURL).object(forKey: "key") as? String,
            "value"
        )
    }

    func testServerComparisonTreatsOmittedEmptyCollectionsAsEqual() {
        let adapter = makeRelationshipComparisonAdapter()
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        parent.createdAt = Date(timeIntervalSinceReferenceDate: 1_000)
        parent.modifiedAt = Date(timeIntervalSinceReferenceDate: 2_000)
        let record = makeRelationshipParentRecord(
            parent: parent,
            zoneID: adapter.recordZoneID
        )

        XCTAssertFalse(adapter.hasChanges(record: record, object: parent))
    }

    func testServerComparisonAcceptsRecordNameRelationships() {
        let adapter = makeRelationshipComparisonAdapter()
        let child = BigSyncRelationshipChild()
        child.id = "child"
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        parent.createdAt = Date(timeIntervalSinceReferenceDate: 1_000)
        parent.modifiedAt = Date(timeIntervalSinceReferenceDate: 2_000)
        parent.children.append(child)
        parent.relatedChildren.insert(child)
        parent.favoriteChild = child
        let record = makeRelationshipParentRecord(
            parent: parent,
            zoneID: adapter.recordZoneID,
            childRecordNames: [BigSyncRelationshipChild.className() + ".child"]
        )

        XCTAssertFalse(adapter.hasChanges(record: record, object: parent))
        record["favoriteChild"] = BigSyncRelationshipChild.className()
            + ".different" as CKRecordValue
        XCTAssertTrue(adapter.hasChanges(record: record, object: parent))
    }

    func testAdapterProviderUsesExplicitTrackingDirectory() {
        let identifier = UUID().uuidString
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("BigSyncKit-isolation-\(identifier)")
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let provider = DefaultRealmSwiftAdapterProvider(
            targetConfigurations: [targetConfiguration],
            excludedClassNames: [],
            zoneID: CKRecordZone.ID(zoneName: "isolated-zone"),
            persistenceNamespace: "isolated-client",
            persistenceDirectoryURL: directory,
            assetDirectoryURL: directory.appendingPathComponent("assets"),
            logger: Logger(label: "BigSyncKitTests")
        )

        XCTAssertEqual(
            provider.persistenceConfiguration.fileURL?
                .deletingLastPathComponent().standardizedFileURL,
            directory.standardizedFileURL
        )
        XCTAssertEqual(
            provider.adapter.persistenceRealmConfiguration.fileURL,
            provider.persistenceConfiguration.fileURL
        )
    }

    @BigSyncBackgroundActor
    func testSynchronizationAuditAcceptsCleanImportedRecord() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: "audited",
            zoneID: fixture.adapter.recordZoneID
        )
        let date = Date(timeIntervalSinceReferenceDate: 9_000)
        record["createdAt"] = date as CKRecordValue
        record["modifiedAt"] = date as CKRecordValue
        record["explicitlyModifiedAt"] = date as CKRecordValue
        record["isDeleted"] = false as CKRecordValue
        record["initialCloudKitSyncEligible"] = true as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        let audit = try await fixture.adapter.auditSynchronizationState(
            serverRecords: [record]
        )

        XCTAssertTrue(audit.isClean, audit.issues.joined(separator: "\n"))
        XCTAssertEqual(audit.ownedServerRecordCount, 1)
        XCTAssertEqual(audit.localObjectCount, 1)
        XCTAssertEqual(audit.trackingRecordCount, 1)
    }

    @BigSyncBackgroundActor
    func testSynchronizationAuditFlagsServerRecordWithoutTracking() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: "untracked-server-record",
            zoneID: fixture.adapter.recordZoneID
        )

        let audit = try await fixture.adapter.auditSynchronizationState(
            serverRecords: [record]
        )
        let recordName = BigSyncTrackedObject.className() + ".untracked-server-record"

        XCTAssertFalse(audit.isClean)
        XCTAssertTrue(audit.issues.contains("server-record-missing-locally:\(recordName)"))
        XCTAssertTrue(audit.issues.contains("server-record-missing-tracking:\(recordName)"))
    }

    @BigSyncBackgroundActor
    func testSynchronizationAuditFlagsSurplusTrackingRecord() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let recordName = BigSyncTrackedObject.className() + ".surplus-tracking"
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(
                SyncedEntity(
                    entityType: BigSyncTrackedObject.className(),
                    identifier: recordName,
                    state: SyncedEntityState.synced.rawValue
                )
            )
        }
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: "surplus-tracking",
            zoneID: fixture.adapter.recordZoneID
        )

        let audit = try await fixture.adapter.auditSynchronizationState(
            serverRecords: [record]
        )

        XCTAssertFalse(audit.isClean)
        XCTAssertTrue(audit.issues.contains("server-record-missing-locally:\(recordName)"))
        XCTAssertTrue(audit.issues.contains("tracking-record-missing-locally:\(recordName)"))
    }

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

    @BigSyncBackgroundActor
    func testMalformedInboundRecordIdentifierFailsWithoutPublishingTrackingState() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let malformedRecordName = "WrongType.remote-object"
        let record = CKRecord(
            recordType: BigSyncTrackedObject.className(),
            recordID: CKRecord.ID(
                recordName: malformedRecordName,
                zoneID: fixture.adapter.recordZoneID
            )
        )

        do {
            try await fixture.adapter.saveChanges(
                in: [record],
                forceSave: true
            )
            XCTFail("Expected the malformed record identifier to fail")
        } catch RealmSwiftAdapterError.malformedRecordIdentifier(
            let recordName,
            let entityType
        ) {
            XCTAssertEqual(recordName, malformedRecordName)
            XCTAssertEqual(entityType, BigSyncTrackedObject.className())
        }

        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: malformedRecordName
            )
        )
        XCTAssertTrue(
            fixture.targetRealm.objects(BigSyncTrackedObject.self).isEmpty
        )
    }

    @BigSyncBackgroundActor
    func testExcludedRecordTypeIsNotImportedFromCloudKit() async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "excluded-import-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "excluded-import-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [BigSyncTrackedObject.className()],
            recordZoneID: CKRecordZone.ID(zoneName: "excluded-import"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await adapter.resetSyncCaches()
        let record = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: "remote",
            zoneID: adapter.recordZoneID
        )

        try await adapter.saveChanges(in: [record], forceSave: true)

        let targetRealm = try XCTUnwrap(
            adapter.realmProvider?.targetReaderRealms?.first
        )
        let persistenceRealm = try XCTUnwrap(
            adapter.realmProvider?.persistenceRealm
        )
        XCTAssertTrue(targetRealm.objects(BigSyncTrackedObject.self).isEmpty)
        XCTAssertNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testSetupRetiresTrackingForNewlyExcludedTypeAndFailsClosed()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "excluded-transition-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "excluded-transition-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]

        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let recordName = BigSyncTrackedObject.className() + ".excluded"
        let trackedObject = BigSyncTrackedObject(
            id: "excluded",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await targetRealm.asyncWrite {
            targetRealm.add(trackedObject)
            targetRealm.add(
                BigSyncPendingMutation(
                    recordName: recordName,
                    entityType: BigSyncTrackedObject.className(),
                    objectIdentifier: trackedObject.id
                )
            )
        }
        try await persistenceRealm.asyncWrite {
            let staleEntities = (0..<3).map { index in
                let entity = SyncedEntity(
                    entityType: BigSyncTrackedObject.className(),
                    identifier: index == 0
                        ? recordName
                        : recordName + "-\(index)",
                    state: SyncedEntityState.changed.rawValue
                )
                entity.pendingGeneration = UUID().uuidString
                persistenceRealm.add(entity)
                return entity
            }
            let relationship = PendingRelationship()
            relationship.relationshipName = "favoriteChild"
            relationship.targetIdentifier = "unused"
            relationship.forSyncedEntity = staleEntities[0]
            persistenceRealm.add(relationship)
        }
        XCTAssertEqual(persistenceRealm.objects(SyncedEntity.self).count, 3)
        XCTAssertEqual(
            persistenceRealm.objects(PendingRelationship.self).count,
            1
        )

        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [BigSyncTrackedObject.className()],
            recordZoneID: CKRecordZone.ID(
                zoneName: "excluded-transition"
            ),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await adapter.unsetCancellation()
        persistenceRealm.refresh()
        targetRealm.refresh()

        XCTAssertNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertTrue(persistenceRealm.objects(SyncedEntity.self).isEmpty)
        XCTAssertTrue(
            persistenceRealm.objects(PendingRelationship.self).isEmpty
        )
        XCTAssertNil(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )

        // Preparation also fails closed if corrupt or externally authored
        // tracking rows appear after the versioned setup reconciliation.
        try await persistenceRealm.asyncWrite {
            let upload = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName + "-upload",
                state: SyncedEntityState.changed.rawValue
            )
            upload.pendingGeneration = UUID().uuidString
            persistenceRealm.add(upload)
            let deletion = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName + "-deletion",
                state: SyncedEntityState.deletedLocally.rawValue
            )
            deletion.pendingGeneration = UUID().uuidString
            persistenceRealm.add(deletion)
        }

        let preparedUploads = try await adapter.preparedRecordsToUpload(
            limit: 100,
            restrictedToEntityType: nil
        )
        let preparedDeletions = try await adapter.preparedRecordDeletions(
            limit: 100,
            restrictedToEntityType: nil
        )
        XCTAssertTrue(preparedUploads.isEmpty)
        XCTAssertTrue(preparedDeletions.isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
    }

    @BigSyncBackgroundActor
    func testSetupRetiresRemovedTypeBeforeDeterminingInitialState()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "removed-transition-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "removed-transition-target-\(identifier)"
        targetConfiguration.objectTypes = [BigSyncPendingMutation.self]

        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let recordName = BigSyncTrackedObject.className() + ".removed"
        try await targetRealm.asyncWrite {
            targetRealm.add(
                BigSyncPendingMutation(
                    recordName: recordName,
                    entityType: BigSyncTrackedObject.className(),
                    objectIdentifier: "removed"
                )
            )
        }
        try await persistenceRealm.asyncWrite {
            let staleEntity = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.changed.rawValue
            )
            staleEntity.pendingGeneration = UUID().uuidString
            persistenceRealm.add(staleEntity)
            let relationship = PendingRelationship()
            relationship.relationshipName = "children"
            relationship.targetIdentifier = "unused"
            relationship.forSyncedEntity = staleEntity
            persistenceRealm.add(relationship)
        }
        XCTAssertEqual(persistenceRealm.objects(SyncedEntity.self).count, 1)
        XCTAssertEqual(
            persistenceRealm.objects(PendingRelationship.self).count,
            1
        )

        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "removed-transition"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let delegate = FakeModelAdapterDelegate()
        adapter.modelAdapterDelegate = delegate
        try await adapter.unsetCancellation()
        persistenceRealm.refresh()
        targetRealm.refresh()

        XCTAssertEqual(delegate.initialSetupCount, 1)
        XCTAssertTrue(persistenceRealm.objects(SyncedEntity.self).isEmpty)
        XCTAssertTrue(
            persistenceRealm.objects(PendingRelationship.self).isEmpty
        )
        XCTAssertTrue(
            targetRealm.objects(BigSyncPendingMutation.self).isEmpty
        )
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
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

    func testTerminalAssetCleanupRemovesSupersededPendingRecordVersions()
    throws {
        let manager = PersistentAssetManager(identifier: UUID().uuidString)
        let firstURL = try manager.store(
            data: Data("first".utf8),
            forRecordID: "AssetRecord.pending",
            propertyName: "payload"
        )
        let secondURL = try manager.store(
            data: Data("second".utf8),
            forRecordID: "AssetRecord.pending",
            propertyName: "payload"
        )
        XCTAssertTrue(FileManager.default.fileExists(atPath: firstURL.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: secondURL.path))

        manager.clearAssetFiles()

        XCTAssertFalse(FileManager.default.fileExists(atPath: firstURL.path))
        XCTAssertFalse(FileManager.default.fileExists(atPath: secondURL.path))
    }

    @BigSyncBackgroundActor
    func testPendingAssetRematerializesAfterTerminalAttemptCleanup()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        try await fixture.adapter._test_setup()
        let expectedData = Data("durable-asset-payload".utf8)
        let object = BigSyncTrackedObject(
            id: "asset-retry",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.payload = expectedData
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        // The live journal observer may already have forwarded this generation;
        // explicitly drain once more so the assertion is insensitive to that
        // scheduling race while still exercising the production boundary.
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        XCTAssertNotNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertNotNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )
        )

        let firstPreparedRecords = try await fixture.adapter.preparedRecordsToUpload(
            limit: 1,
            restrictedToEntityType: nil
        )
        let first = try XCTUnwrap(firstPreparedRecords.first)
        let firstAsset = try XCTUnwrap(first.record["payload"] as? CKAsset)
        let firstURL = try XCTUnwrap(firstAsset.fileURL)
        XCTAssertEqual(try Data(contentsOf: firstURL), expectedData)

        try await fixture.adapter.didFinishImport()
        XCTAssertFalse(FileManager.default.fileExists(atPath: firstURL.path))

        let retryPreparedRecords = try await fixture.adapter.preparedRecordsToUpload(
            limit: 1,
            restrictedToEntityType: nil
        )
        let retry = try XCTUnwrap(retryPreparedRecords.first)
        let retryAsset = try XCTUnwrap(retry.record["payload"] as? CKAsset)
        let retryURL = try XCTUnwrap(retryAsset.fileURL)
        XCTAssertEqual(retry.generation, first.generation)
        XCTAssertEqual(try Data(contentsOf: retryURL), expectedData)
    }

    @BigSyncBackgroundActor
    func testUnreadableInboundAssetRejectsChunkWithoutLocalMutation()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let localData = Data("local-value".utf8)
        let object = BigSyncTrackedObject(
            id: "missing-inbound-asset",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.payload = localData
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
        let missingURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        record["payload"] = CKAsset(fileURL: missingURL)

        do {
            try await fixture.adapter.saveChanges(
                in: [record],
                forceSave: true
            )
            XCTFail("Expected unreadable CKAsset rejection")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }

        XCTAssertEqual(object.payload, localData)
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )
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
    func testSynchronizationRequestDuringActiveRunSchedulesOneTailRun()
    async {
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "tail-run"),
            priorities: []
        ))
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
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "awaitable-terminal"),
            priorities: []
        ))
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
    func testTerminalNotificationReentrancyStartsAnIndependentDrain() async {
        let synchronizer = makeSynchronizer()
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "terminal-reentrancy"),
            priorities: []
        ))
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let firstAttemptID = synchronizer.synchronizationAttemptID
        let observation = TerminalCallbackObservation(synchronizer: synchronizer)
        NotificationCenter.default.addObserver(
            observation,
            selector: #selector(observation.synchronizerDidSynchronize(_:)),
            name: .SynchronizerDidSynchronize,
            object: synchronizer
        )
        defer { NotificationCenter.default.removeObserver(observation) }

        await synchronizer.changesFinishedSynchronizing()

        XCTAssertTrue(observation.sawCompletedDrain)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertTrue(synchronizer.synchronizationDrainIsActive)
        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, firstAttemptID)
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
    func testLocalWorkForwardedDuringTerminalFailureStartsOneTailDrain() async {
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "failure-tail-local-work"),
            priorities: []
        )
        synchronizer.addModelAdapter(adapter)
        adapter.didFinishImportHandler = { [weak adapter] in
            await adapter?.modelAdapterDelegate?.hasChangesToUpload()
        }
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let failedAttemptID = synchronizer.synchronizationAttemptID

        await synchronizer.failSynchronization(
            error: TestSynchronizationError.terminalForwardingFailed
        )

        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, failedAttemptID)
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertFalse(synchronizer.synchronizationRequestedWhileRunning)
        await synchronizer.cancelSynchronizationAndWait()
    }

    func testCloudKitRetryBackoffNeverUndercutsServerRetryAfter() {
        XCTAssertEqual(
            CloudKitRetryBackoff.delay(
                serverMinimum: 120,
                consecutiveFailures: 1,
                randomUnit: 0
            ),
            120
        )
        XCTAssertGreaterThanOrEqual(
            CloudKitRetryBackoff.delay(
                serverMinimum: 120,
                consecutiveFailures: 20,
                randomUnit: 1
            ),
            120
        )
    }

    func testCloudKitRetryBackoffGrowsAndCapsWithoutServerDirection() {
        XCTAssertEqual(
            CloudKitRetryBackoff.delay(
                serverMinimum: nil,
                consecutiveFailures: 1
            ),
            5
        )
        XCTAssertEqual(
            CloudKitRetryBackoff.delay(
                serverMinimum: nil,
                consecutiveFailures: 2
            ),
            10
        )
        XCTAssertEqual(
            CloudKitRetryBackoff.delay(
                serverMinimum: nil,
                consecutiveFailures: 99
            ),
            300
        )
    }

    @BigSyncBackgroundActor
    func testTransientRetryDeadlinePersistsAndIsAccountScoped() async throws {
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        let identifier = "retry-state-\(UUID().uuidString)"
        func make() -> CloudKitSynchronizer {
            CloudKitSynchronizer(
                identifier: identifier,
                containerIdentifier: "iCloud.test",
                database: database,
                recordZoneID: CloudKitSynchronizer.defaultCustomZoneID,
                keyValueStore: store,
                accountIdentifierProvider: { database.accountIdentifier },
                logger: Logger(label: "BigSyncKitTests")
            )
        }
        let first = make()
        let notBefore = Date().addingTimeInterval(600)
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: "test-account",
            accountScopeIdentifier: "scope-a"
        )
        first.persistTransientRetryState(
            context: context,
            notBefore: notBefore,
            consecutiveFailures: 3
        )

        let relaunched = make()
        let persistedNotBefore = try XCTUnwrap(
            relaunched._test_persistedTransientRetryNotBefore(
                accountScopeIdentifier: "scope-a"
            )
        )
        XCTAssertEqual(
            persistedNotBefore.timeIntervalSince1970,
            notBefore.timeIntervalSince1970,
            accuracy: 0.001
        )

        let otherAccount = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: "other-account",
            accountScopeIdentifier: "scope-b"
        )
        try await relaunched.waitForPersistedTransientRetryIfNeeded(
            context: otherAccount
        )
        XCTAssertNil(
            relaunched._test_persistedTransientRetryNotBefore(
                accountScopeIdentifier: "scope-a"
            )
        )
    }

    @BigSyncBackgroundActor
    func testCompletedSynchronizationResetsTransientRetryBackoff() async {
        let synchronizer = makeSynchronizer()
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        synchronizer.consecutiveTransientCloudKitFailures = 4

        await synchronizer.changesFinishedSynchronizing()

        XCTAssertEqual(synchronizer.consecutiveTransientCloudKitFailures, 0)
    }

    @BigSyncBackgroundActor
    func testServerDirectedRetryIsScheduledNoEarlierThanCloudKitMinimumAndCancellationClearsIt() async {
        let synchronizer = makeSynchronizer()
        synchronizer.syncing = true
        let startedAt = Date()
        await synchronizer.failSynchronization(
            error: CKError(
                .requestRateLimited,
                userInfo: [CKErrorRetryAfterKey: 1.0]
            )
        )

        XCTAssertGreaterThanOrEqual(
            synchronizer.retrySleepUntil ?? .distantPast,
            startedAt.addingTimeInterval(1)
        )
        XCTAssertEqual(synchronizer.consecutiveTransientCloudKitFailures, 1)

        await synchronizer.cancelSynchronizationAndWait()

        XCTAssertNil(synchronizer.retrySleepUntil)
        XCTAssertEqual(synchronizer.consecutiveTransientCloudKitFailures, 0)
    }

    @BigSyncBackgroundActor
    func testTerminalPendingStateSchedulesTailRunAndWithholdsReceipt() async {
        // Zero newly-forwarded journal rows do not prove the adapter is empty.
        let database = FakeCloudKitDatabase()
        database.completesFetchDatabaseChanges = false
        let synchronizer = makeSynchronizer(database: database)
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "terminal-pending-state"),
            priorities: []
        )
        adapter.didFinishImportHandler = { [weak adapter] in
            if adapter?.didFinishImportCount == 2 {
                adapter?.terminalPendingChanges = true
            }
        }
        synchronizer.addModelAdapter(adapter)
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let firstAttemptID = synchronizer.synchronizationAttemptID

        await synchronizer.changesFinishedSynchronizing()

        XCTAssertEqual(adapter.didFinishImportCount, 2)
        XCTAssertTrue(adapter.events.contains("terminalPendingState"))
        XCTAssertTrue(synchronizer.syncing)
        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, firstAttemptID)
        XCTAssertNil(synchronizer.activeReceiptAuthorizationID)
        await synchronizer.cancelSynchronizationAndWait()
    }

    @BigSyncBackgroundActor
    func testDeletedZoneIsTerminalBeforeProviderCanResetTracking() async {
        let database = FakeCloudKitDatabase()
        database.databaseDeletedZoneIDs = [
            CKRecordZone.ID(zoneName: "deleted-zone")
        ]
        let deletedZoneID = CKRecordZone.ID(zoneName: "deleted-zone")
        let synchronizer = CloudKitSynchronizer(
            identifier: "deleted-zone-reset-failure",
            containerIdentifier: "iCloud.test",
            database: database,
            recordZoneID: deletedZoneID,
            keyValueStore: DictionaryKeyValueStore(),
            accountIdentifierProvider: { "test-account" },
            accountStatusProvider: { .available },
            logger: Logger(label: "BigSyncKitTests")
        )
#if DEBUG
        synchronizer._allowRecordZoneRebindingForTesting()
#endif
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: deletedZoneID,
            priorities: []
        ))

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected deleted zone to become terminal")
        } catch ChangeFeedMigrationError.establishedZoneUnavailable(
            let zoneID,
            let kind
        ) {
            XCTAssertEqual(zoneID.zoneName, "deleted-zone")
            XCTAssertEqual(kind, .deleted)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
        XCTAssertNil(synchronizer.storedDatabaseToken)
    }

    @BigSyncBackgroundActor
    func testUnrelatedDatabaseZoneDeletionNeverAffectsSoleAdapter()
    async throws {
        let configuredZoneID = CKRecordZone.ID(
            zoneName: "configured-private-zone"
        )
        let unrelatedZoneID = CKRecordZone.ID(
            zoneName: "unrelated-private-zone"
        )
        let database = FakeCloudKitDatabase()
        database.databaseDeletedZoneIDs = [unrelatedZoneID]
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: database,
            recordZoneID: configuredZoneID,
            keyValueStore: DictionaryKeyValueStore(),
            accountIdentifierProvider: { "test-account" },
            accountStatusProvider: { .available },
            logger: Logger(label: "BigSyncKitTests")
        )
#if DEBUG
        synchronizer._allowRecordZoneRebindingForTesting()
#endif
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: configuredZoneID,
            priorities: []
        ))

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertFalse(synchronizer.configuredZoneIsTerminal(configuredZoneID))
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
    func testAccountSwitchBeforeDeletedZonePublicationSkipsDelegateNotification()
    async {
        final class RecordingDelegate: CloudKitSynchronizerDelegate {
            private(set) var deletedZoneCount = 0
            func synchronizerWillFetchChanges(
                _ synchronizer: CloudKitSynchronizer,
                in recordZone: CKRecordZone.ID
            ) {}
            func synchronizerWillUploadChanges(
                _ synchronizer: CloudKitSynchronizer,
                to recordZone: CKRecordZone.ID
            ) {}
            func synchronizerDidSync(_ synchronizer: CloudKitSynchronizer) {}
            func synchronizerDidfailToSync(
                _ synchronizer: CloudKitSynchronizer,
                error: Error
            ) {}
            func synchronizer(
                _ synchronizer: CloudKitSynchronizer,
                zoneIDWasDeleted zoneID: CKRecordZone.ID
            ) {
                deletedZoneCount += 1
            }
        }

        let database = FakeCloudKitDatabase()
        database.databaseDeletedZoneIDs = [
            CKRecordZone.ID(zoneName: "stale-deleted-zone")
        ]
        database.accountIdentifierAfterNextDatabaseChangesFetch = "account-b"
        let delegate = RecordingDelegate()
        let staleDeletedZoneID = CKRecordZone.ID(
            zoneName: "stale-deleted-zone"
        )
        let synchronizer = CloudKitSynchronizer(
            identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test",
            database: database,
            recordZoneID: staleDeletedZoneID,
            keyValueStore: DictionaryKeyValueStore(),
            accountIdentifierProvider: { database.accountIdentifier },
            accountStatusProvider: { .available },
            logger: Logger(label: "BigSyncKitTests")
        )
#if DEBUG
        synchronizer._allowRecordZoneRebindingForTesting()
#endif
        synchronizer.delegate = delegate
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: staleDeletedZoneID,
            priorities: []
        ))

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
        XCTAssertEqual(delegate.deletedZoneCount, 0)
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
    func testCancellingStalledAsyncRecordMutationReleasesSynchronization()
    async {
        let database = FakeCloudKitDatabase()
        database.completesModifyOperations = false
        let zoneID = CKRecordZone.ID(
            zoneName: "stalled-async-record-mutation",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: [
                "Item": [makeRecord(type: "Item", id: "1", zoneID: zoneID)]
            ]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        for _ in 0..<5_000 where database.modifyRecordsOperationCount == 0 {
            try? await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertEqual(database.modifyRecordsOperationCount, 1)

        await synchronizer.cancelSynchronizationAndWait()
        do {
            _ = try await synchronization.value
            XCTFail("Expected cancellation while the record mutation was stalled")
        } catch is CancellationError {
            // Expected: the async CloudKit mutation belongs to the cancelled
            // synchronization task and cannot publish a stale acknowledgement.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertFalse(
            adapter.events.contains(where: { $0.hasPrefix("didUpload:") })
        )
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
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "cancellation-barrier"),
            priorities: []
        ))
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
        // This scenario needs synchronization to reach the terminal import
        // barrier before reset is requested.  The fake database deliberately
        // leaves an empty zone-change operation open unless a test opts into
        // its completion, so the barrier test must close that operation here.
        database.completesEmptyZoneChangeOperation = true
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
        await fulfillment(of: [enteredTerminalImport], timeout: 5)

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
    func testRestoredBackupUsesProvenanceMigrationAndDoesNotResurrectMissingServerRecord()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let identifier = "restore-provenance-\(UUID().uuidString)"
        let installedBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let restoredBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let targetObject = BigSyncTrackedObject(
            id: "server-deleted-after-backup",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + targetObject.id
        let copiedOutboxObject = BigSyncTrackedObject(
            id: "acknowledged-after-backup",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        copiedOutboxObject.tags.append("historical-edit")
        let copiedOutboxRecordName = BigSyncTrackedObject.className()
            + "." + copiedOutboxObject.id
        let copiedGeneration = "copied-backup-generation"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(targetObject)
            fixture.targetRealm.add(copiedOutboxObject)
            fixture.targetRealm.add(BigSyncPendingMutation(
                recordName: copiedOutboxRecordName,
                entityType: BigSyncTrackedObject.className(),
                objectIdentifier: copiedOutboxObject.id,
                generation: copiedGeneration,
                changedAt: .distantFuture
            ))
        }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
            let copiedTracking = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: copiedOutboxRecordName,
                state: SyncedEntityState.changed.rawValue
            )
            copiedTracking.pendingGeneration = copiedGeneration
            fixture.persistenceRealm.add(copiedTracking)
        }

        // The installed client writes the backed-up marker and an excluded
        // sentinel. The restored client sees the marker in the same store but
        // no sentinel in its restored filesystem root.
        let installed = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: installedBase
        )
        let installedSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: installedBase
        )
        let restoredSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: restoredBase
        )
        let restoredMarker = BackupDetection.markerURL(
            sentinelURL: restoredSentinel
        )
        try FileManager.default.createDirectory(
            at: restoredMarker.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: installedSentinel),
            to: restoredMarker
        )
        let restored = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: restoredBase
        )
        let scope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )
        try restored.markConfiguredZoneTerminal(
            fixture.adapter.recordZoneID,
            kind: .purged,
            accountScopeIdentifier: scope
        )
        restored.persistTransientRetryState(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: scope
            ),
            notBefore: Date().addingTimeInterval(600),
            consecutiveFailures: 2
        )
        restored.addModelAdapter(fixture.adapter)

        let result = try await restored.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self,
            forPrimaryKey: targetObject.id
        ))
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self,
            forPrimaryKey: copiedOutboxObject.id
        ))
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: copiedOutboxRecordName
        ))
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: recordName
        ))
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: copiedOutboxRecordName
        ))
        XCTAssertNil(restored._test_persistedTransientRetryNotBefore(
            accountScopeIdentifier: scope
        ))
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: restored.durableStateNamespace,
            sharedSentinelBaseURL: restoredBase
        ))
    }

    @BigSyncBackgroundActor
    func testRestoredBackupKeepsAndUploadsMutationCreatedByCurrentProcess()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "restore-current-mutation-\(UUID().uuidString)"
        let installedBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let restoredBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)

        let installed = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: installedBase
        )
        let installedSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: installedBase
        )
        let restoredSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: restoredBase
        )
        let restoredMarker = BackupDetection.markerURL(
            sentinelURL: restoredSentinel
        )
        try FileManager.default.createDirectory(
            at: restoredMarker.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: installedSentinel),
            to: restoredMarker
        )

        // This edit happened after the restored process launched. Its opaque
        // generation carries the current process identity and must not be
        // mistaken for an outbox entry copied from the old installation.
        let object = BigSyncTrackedObject(
            id: "edited-after-restore-launch",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.tags.append("current-process-edit")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let currentGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        XCTAssertTrue(
            BigSyncPendingMutation.wasCreatedInCurrentProcess(
                currentGeneration
            )
        )

        let restored = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: restoredBase
        )
        restored.addModelAdapter(fixture.adapter)

        let result = try await restored.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertGreaterThanOrEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertEqual(
            database.record(for: CKRecord.ID(
                recordName: recordName,
                zoneID: fixture.adapter.recordZoneID
            ))?["tags"] as? [String],
            ["current-process-edit"]
        )
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: restored.durableStateNamespace,
            sharedSentinelBaseURL: restoredBase
        ))
    }

    @BigSyncBackgroundActor
    func testRestoredBackupKeepsPostDetectionMutationFromAnotherProcessSession()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "restore-shared-host-mutation-\(UUID().uuidString)"
        let installedBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let restoredBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let installed = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: installedBase
        )
        let installedSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: installedBase
        )
        let restoredSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: restoredBase
        )
        let restoredMarker = BackupDetection.markerURL(
            sentinelURL: restoredSentinel
        )
        try FileManager.default.createDirectory(
            at: restoredMarker.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: installedSentinel),
            to: restoredMarker
        )

        // Constructing the restored synchronizer publishes the restore event
        // and a fresh backup-excluded installation identity. Model a mutation
        // written afterwards by another process in the same app group: its
        // process differs, but its installation identity is shared.
        let restored = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            backupDetectionBaseURL: restoredBase
        )
        let restoredInstallationIdentifier = try XCTUnwrap(
            BackupDetection.installationIdentifier(
            namespace: restored.durableStateNamespace,
            sharedSentinelBaseURL: restoredBase
            )
        )
        BigSyncMutationTracking.install(
            configurations: [fixture.targetRealm.configuration],
            excludedClassNames: [],
            installationIdentifier: restoredInstallationIdentifier
        )
        let object = BigSyncTrackedObject(
            id: "shared-host-post-restore-edit",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        object.tags.append("shared-host-edit")
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let generation = BigSyncPendingMutation.makeGeneration(
            installationIdentifier: restoredInstallationIdentifier
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            fixture.targetRealm.add(BigSyncPendingMutation(
                recordName: recordName,
                entityType: BigSyncTrackedObject.className(),
                objectIdentifier: object.id,
                generation: generation,
                changedAt: .distantPast
            ))
        }
        restored.addModelAdapter(fixture.adapter)

        let result = try await restored.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertGreaterThanOrEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertEqual(
            database.record(for: CKRecord.ID(
                recordName: recordName,
                zoneID: fixture.adapter.recordZoneID
            ))?["tags"] as? [String],
            ["shared-host-edit"]
        )
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))
    }

    @BigSyncBackgroundActor
    func testInitialImportDiscoversPreexistingUnjournaledObjectOnce()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "preexisting-before-bigsync",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.tags.append("initial-import")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))

        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(
            database: database,
            recordZoneID: fixture.adapter.recordZoneID
        )
        synchronizer.addModelAdapter(fixture.adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertGreaterThanOrEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertEqual(
            database.record(for: CKRecord.ID(
                recordName: recordName,
                zoneID: fixture.adapter.recordZoneID
            ))?["tags"] as? [String],
            ["initial-import"]
        )
    }

    @BigSyncBackgroundActor
    func testDeviceIdentifierIsProcessSessionScopedNotRestoredMetadata()
    async throws {
        let store = DictionaryKeyValueStore()
        let identifier = "device-session-\(UUID().uuidString)"
        let first = makeSynchronizer(
            keyValueStore: store,
            identifier: identifier
        )
        let firstIdentifier = first.deviceIdentifier
        // Model a pre-cutover backup containing the old persisted optimization
        // tag. A fresh synchronizer must not adopt it and suppress records
        // written by the original installation.
        first.deviceUUID = firstIdentifier

        let restored = makeSynchronizer(
            keyValueStore: store,
            identifier: identifier
        )

        XCTAssertNotEqual(restored.deviceIdentifier, firstIdentifier)
        XCTAssertEqual(first.deviceUUID, firstIdentifier)
    }

    @BigSyncBackgroundActor
    func testFreshRestoreEventSupersedesCopiedPendingRestoreEnvelope()
    throws {
        let store = DictionaryKeyValueStore()
        let synchronizer = makeSynchronizer(keyValueStore: store)
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: "restore-account",
            accountScopeIdentifier: CloudKitSynchronizer
                .accountScopeIdentifier(for: "restore-account")
        )
        let firstEventIdentifier = UUID().uuidString
        let secondEventIdentifier = UUID().uuidString
        try synchronizer.requestChangeFeedRecovery(
            context: context,
            mode: .backupRestore,
            backupRestoreEventIdentifier: firstEventIdentifier
        )
        let first = try XCTUnwrap(store.propertyListEntries.first(where: {
            $0.key.contains("ChangeFeedMigration.v3")
        })?.value)
        let firstEpoch = try XCTUnwrap(
            (first["epoch"] as? NSNumber)?.intValue
        )

        try synchronizer.requestChangeFeedRecovery(
            context: context,
            mode: .backupRestore,
            backupRestoreEventIdentifier: secondEventIdentifier
        )
        let replaced = try XCTUnwrap(store.propertyListEntries.first(where: {
            $0.key.contains("ChangeFeedMigration.v3")
        })?.value)

        XCTAssertEqual(
            replaced["backupRestoreEventIdentifier"] as? String,
            secondEventIdentifier
        )
        XCTAssertEqual(
            (replaced["epoch"] as? NSNumber)?.intValue,
            firstEpoch + 1
        )
        XCTAssertEqual(replaced["phase"] as? String, "requested")
    }

    @BigSyncBackgroundActor
    func testRestoredBackupRetainsEventAndTerminalFenceWhenRecoveryEnvelopeIsNotDurable()
    async throws {
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        let identifier = "restore-undurable-\(UUID().uuidString)"
        let zoneID = CKRecordZone.ID(
            zoneName: "restore-undurable-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let installedBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let restoredBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let installed = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: zoneID,
            backupDetectionBaseURL: installedBase
        )
        let installedSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: installedBase
        )
        let restoredSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace,
            sharedBaseURL: restoredBase
        )
        let restoredMarker = BackupDetection.markerURL(
            sentinelURL: restoredSentinel
        )
        try FileManager.default.createDirectory(
            at: restoredMarker.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: installedSentinel),
            to: restoredMarker
        )
        let restored = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: zoneID,
            backupDetectionBaseURL: restoredBase
        )
        restored.addModelAdapter(FakeModelAdapter(
            zoneID: zoneID,
            priorities: []
        ))
        let scope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )
        try restored.markConfiguredZoneTerminal(
            zoneID,
            kind: .purged,
            accountScopeIdentifier: scope
        )
        store.synchronizesDurably = false

        do {
            _ = try await restored.synchronize()
            XCTFail("Expected restore recovery durability failure")
        } catch let error as ChangeFeedMigrationPersistenceError {
            XCTAssertEqual(error, .stateNotDurable)
        }

        XCTAssertTrue(BackupDetection.restoreResetIsRequired(
            namespace: restored.durableStateNamespace,
            sharedSentinelBaseURL: restoredBase
        ))
        XCTAssertTrue(restored.configuredZoneIsTerminal(zoneID))
        XCTAssertEqual(database.subscriptionFetchCount, 0)
        XCTAssertEqual(database.databaseChangeFetchCount, 0)
        XCTAssertEqual(database.recordZoneChangeFetchCount, 0)
        XCTAssertEqual(database.recordZoneFetchCount, 0)
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testAccountReplacementReconcilesPriorServerProofWithoutUploadingIt()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-b"
        database.completesEmptyZoneChangeOperation = true
        let object = BigSyncTrackedObject(
            id: "account-a-server-object",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store,
            recordZoneID: fixture.adapter.recordZoneID,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )
        synchronizer.addModelAdapter(fixture.adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self,
            forPrimaryKey: object.id
        ))
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: recordName
        ))
    }

    @BigSyncBackgroundActor
    func testAccountReplacementFollowedBySchemaRevisionDoesNotRediscoverPriorAccountObject()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-b"
        database.completesEmptyZoneChangeOperation = true
        let object = BigSyncTrackedObject(
            id: "account-a-retained-across-schema-revision",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
        }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store,
            recordZoneID: fixture.adapter.recordZoneID,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        store.set(
            value: "account-a",
            forKey: synchronizer.durableStateKey(
                "CloudKitAccountIdentifier"
            )
        )
        synchronizer.addModelAdapter(fixture.adapter)
        _ = try await synchronizer.synchronize()
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: recordName
        ))

        var revisedTargetConfiguration = fixture.targetRealm.configuration
        revisedTargetConfiguration.schemaVersion += 1
        let replacement = RealmSwiftAdapter(
            persistenceRealmConfiguration:
                fixture.persistenceRealm.configuration,
            targetRealmConfigurations: [revisedTargetConfiguration],
            excludedClassNames: [],
            recordZoneID: fixture.adapter.recordZoneID,
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await replacement._test_setup()
        let reopenedPersistenceRealm = try XCTUnwrap(
            replacement.realmProvider?.persistenceRealm
        )

        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self,
            forPrimaryKey: object.id
        ))
        XCTAssertNil(reopenedPersistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: recordName
        ))
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testCancellationBarrierDoesNotWaitForSubscriptionCallback() async {
        let database = FakeCloudKitDatabase()
        database.completesSubscriptionFetches = false
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "subscription-cancellation"),
            priorities: []
        ))
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

    @BigSyncBackgroundActor
    func testSubscriptionSaveFailureDoesNotPublishLocalIdentifier() async {
        let database = FakeCloudKitDatabase()
        database.subscriptionSaveError =
            TestSynchronizationError.subscriptionMutationFailed
        let synchronizer = makeSynchronizer(database: database)

        do {
            try await synchronizer.subscribeForChangesInDatabase()
            XCTFail("Expected subscription save failure")
        } catch TestSynchronizationError.subscriptionMutationFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
        XCTAssertEqual(database.savedSubscriptionCount, 1)
    }

    @BigSyncBackgroundActor
    func testSubscriptionDeleteFailureRetainsLocalIdentifier() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)
        try await synchronizer.subscribeForChangesInDatabase()
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        database.subscriptionDeleteError =
            TestSynchronizationError.subscriptionMutationFailed

        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("Expected subscription deletion failure")
        } catch TestSynchronizationError.subscriptionMutationFailed {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(),
            identifier
        )
        XCTAssertEqual(database.deletedSubscriptionIDs, [identifier])
    }

    @BigSyncBackgroundActor
    func testAccountReplacementAfterSubscriptionLookupPreventsDeletion()
    async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        try await synchronizer.subscribeForChangesInDatabase()
        database.fetchedSubscriptions = database.savedSubscriptions
        synchronizer.databaseSubscriptionID = nil
        database.accountIdentifierAfterNextSubscriptionLookup = "account-b"

        do {
            try await synchronizer.cancelSubscriptionForChangesInDatabase()
            XCTFail("Expected account replacement after subscription lookup")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertTrue(database.deletedSubscriptionIDs.isEmpty)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testAccountReplacementAfterSubscriptionDeletionRetainsLocalFence()
    async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "subscription-delete-fence")
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        try await synchronizer.subscribeForChanges(in: zoneID)
        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        database.accountIdentifierAfterNextSubscriptionDelete = "account-b"

        do {
            try await synchronizer.cancelSubscriptionForChanges(in: zoneID)
            XCTFail("Expected account replacement after subscription deletion")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.deletedSubscriptionIDs, [identifier])
        XCTAssertEqual(
            synchronizer.subscriptionID(forRecordZoneID: zoneID),
            identifier
        )
    }

    @BigSyncBackgroundActor
    func testAccountReplacementAfterSubscriptionSaveDoesNotPublishIdentifier()
    async {
        let database = FakeCloudKitDatabase()
        database.accountIdentifierAfterNextSubscriptionSave = "replacement-account"
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "subscription-account-replacement"),
            priorities: []
        ))

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected account replacement to fail synchronization")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertNil(synchronizer.subscriptionIDForDatabaseSubscription())
    }

    @BigSyncBackgroundActor
    func testDatabaseSubscriptionDoesNotAdoptAnUnrelatedSubscription() async throws {
        let database = FakeCloudKitDatabase()
        database.fetchedSubscriptions = [
            CKDatabaseSubscription(subscriptionID: "foreign-database-subscription")
        ]
        let synchronizer = makeSynchronizer(database: database)

        try await synchronizer.subscribeForChangesInDatabase()

        let saved = try XCTUnwrap(
            database.savedSubscriptions.first as? CKDatabaseSubscription
        )
        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertNotEqual(saved.subscriptionID, "foreign-database-subscription")
        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(),
            saved.subscriptionID
        )
        XCTAssertTrue(saved.notificationInfo?.shouldSendContentAvailable ?? false)
    }

    @BigSyncBackgroundActor
    func testZoneSubscriptionDoesNotAdoptAnUnrelatedZoneSubscription()
    async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "subscription-ownership-zone")
        database.fetchedSubscriptions = [
            CKRecordZoneSubscription(
                zoneID: zoneID,
                subscriptionID: "foreign-zone-subscription"
            )
        ]
        let synchronizer = makeSynchronizer(database: database)

        try await synchronizer.subscribeForChanges(in: zoneID)

        let saved = try XCTUnwrap(
            database.savedSubscriptions.first as? CKRecordZoneSubscription
        )
        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertEqual(saved.zoneID, zoneID)
        XCTAssertNotEqual(saved.subscriptionID, "foreign-zone-subscription")
        XCTAssertEqual(
            synchronizer.subscriptionID(forRecordZoneID: zoneID),
            saved.subscriptionID
        )
        XCTAssertTrue(saved.notificationInfo?.shouldSendContentAvailable ?? false)
    }

    @BigSyncBackgroundActor
    func testSubscriptionUpgradeRejectsStoredArbitraryIDAndNeverDeletesForeignSubscription()
    async throws {
        let database = FakeCloudKitDatabase()
        database.fetchedSubscriptions = [
            CKDatabaseSubscription(subscriptionID: "foreign-database-subscription")
        ]
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.databaseSubscriptionID = "old-arbitrary-subscription-id"

        try await synchronizer.subscribeForChangesInDatabase()

        let saved = try XCTUnwrap(
            database.savedSubscriptions.first as? CKDatabaseSubscription
        )
        XCTAssertNotEqual(saved.subscriptionID, "old-arbitrary-subscription-id")
        XCTAssertEqual(
            synchronizer.subscriptionIDForDatabaseSubscription(),
            saved.subscriptionID
        )

        let unrelatedOnlyDatabase = FakeCloudKitDatabase()
        unrelatedOnlyDatabase.fetchedSubscriptions = [
            CKDatabaseSubscription(subscriptionID: "foreign-only-subscription")
        ]
        let unrelatedOnlySynchronizer = makeSynchronizer(
            database: unrelatedOnlyDatabase
        )
        unrelatedOnlySynchronizer.databaseSubscriptionID =
            "foreign-only-subscription"
        try await unrelatedOnlySynchronizer.cancelSubscriptionForChangesInDatabase()
        XCTAssertTrue(unrelatedOnlyDatabase.deletedSubscriptionIDs.isEmpty)

        let zoneID = CKRecordZone.ID(zoneName: "foreign-cancel-zone")
        let foreignZoneID = "foreign-zone-subscription"
        let zoneDatabase = FakeCloudKitDatabase()
        zoneDatabase.fetchedSubscriptions = [
            CKRecordZoneSubscription(
                zoneID: zoneID,
                subscriptionID: foreignZoneID
            )
        ]
        let zoneSynchronizer = makeSynchronizer(database: zoneDatabase)
        zoneSynchronizer.storeSubscriptionID(foreignZoneID, for: zoneID)
        try await zoneSynchronizer.cancelSubscriptionForChanges(in: zoneID)
        XCTAssertTrue(zoneDatabase.deletedSubscriptionIDs.isEmpty)
    }

    func testCorruptDatabaseAndZoneCursorsDoNotDowngradeToNil() {
        let corruptData = Data("not-a-keyed-archive".utf8)

        XCTAssertThrowsError(
            try DatabaseChangeCursor(serializedData: corruptData).token()
        ) { error in
            XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
        }
        XCTAssertThrowsError(
            try RecordZoneChangeCursor(serializedData: corruptData).token()
        ) { error in
            XCTAssertEqual(error as? CloudKitChangeFeedError, .corruptCursor)
        }
    }

    @BigSyncBackgroundActor
    func testDisposableClientDeletesItsSingleActiveZoneUsingReceipt() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let activeZoneID = CKRecordZone.ID(zoneName: "disposable-active-zone")
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

        let deleted = try await synchronizer.deleteActiveRecordZoneForDisposableClient(
            using: receipt
        )
        XCTAssertTrue(deleted)
        XCTAssertEqual(database.deletedZoneIDs, [activeZoneID])
    }

    @BigSyncBackgroundActor
    func testDisposableClientZoneDeletionRejectsWrongReceiptAndAccount() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        synchronizer.addModelAdapter(
            FakeModelAdapter(
                zoneID: CKRecordZone.ID(zoneName: "disposable-receipt-zone"),
                priorities: []
            )
        )
        let authorizationID = UUID()
        synchronizer.activeReceiptAuthorizationID = authorizationID
        let accountScopeIdentifier =
            try await synchronizer.cloudKitAccountScopeIdentifier()
        let receipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: accountScopeIdentifier
            ),
            issuerID: synchronizer.synchronizationReceiptIssuerID,
            authorizationID: authorizationID
        )
        let wrongReceipt = CloudKitSynchronizer.SynchronizationReceipt(
            context: .init(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: accountScopeIdentifier
            ),
            issuerID: UUID(),
            authorizationID: authorizationID
        )

        do {
            _ = try await synchronizer.deleteActiveRecordZoneForDisposableClient(
                using: wrongReceipt
            )
            XCTFail("Expected foreign receipt rejection")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }
        XCTAssertTrue(database.deletedZoneIDs.isEmpty)

        database.accountIdentifier = "different-account"
        do {
            _ = try await synchronizer.deleteActiveRecordZoneForDisposableClient(
                using: receipt
            )
            XCTFail("Expected account replacement rejection")
        } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
        }
        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
    }

    @BigSyncBackgroundActor
    func testSynchronizerRefusesASecondActiveZoneAtRegistration() async throws {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)
        let first = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "disposable-first-zone"),
            priorities: []
        )
        let second = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "disposable-second-zone"),
            priorities: []
        )
        synchronizer.addModelAdapter(first)
        XCTAssertTrue(synchronizer.canAddModelAdapter(first))
        XCTAssertFalse(synchronizer.canAddModelAdapter(second))
        XCTAssertEqual(synchronizer.modelAdapters.count, 1)
        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
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
    func testSynchronizationAccountSwitchDurablyRequestsAdapterReconciliation()
    async throws {
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        let store = DictionaryKeyValueStore()
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store,
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

        // Account validation must not erase tracking outside the provenance
        // migration. It durably requests server-first reconciliation; the
        // next synchronization attempt performs the adapter reset/capture.
        XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        let migration = try XCTUnwrap(store.propertyListEntries.first(where: {
            $0.key.contains("ChangeFeedMigration.v3")
        })?.value)
        XCTAssertEqual(migration["phase"] as? String, "requested")
        XCTAssertEqual(migration["mode"] as? String, "serverReconciliation")
        XCTAssertFalse(synchronizer.cancelledDueToUnauthentication)
    }

    @BigSyncBackgroundActor
    func testAccountChangeDuringReplacementConfirmationLeavesValidationRequired()
    async throws {
        let database = FakeCloudKitDatabase()
        let enteredConfirmation = AsyncGate()
        let releaseConfirmation = AsyncGate()
        let identifiers = GatedAccountIdentifierSequence(
            ["account-a", "account-b", "account-b", "account-c"],
            gatedCall: 3,
            enteredGate: enteredConfirmation,
            releaseGate: releaseConfirmation
        )
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { await identifiers.next() }
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

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()

        let validation = Task { @BigSyncBackgroundActor in
            try await synchronizer._test_validateSynchronizationAccount()
        }
        while !(await enteredConfirmation.hasOpened()) {
            await Task.yield()
        }

        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()
        await releaseConfirmation.open()

        do {
            try await validation.value
            XCTFail("Expected the superseded validation to be cancelled")
        } catch is CancellationError {
        }

        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertFalse(adapter.events.contains("resetSyncCaches"))
        XCTAssertEqual(
            synchronizer.keyValueStore.object(
                forKey: synchronizer.durableStateKey(
                    "CloudKitAccountIdentifier"
                )
            ) as? String,
            "account-c"
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
        synchronizer.addModelAdapter(FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "account-subscription-rebuild"),
            priorities: []
        ))

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
    func testDeleteServerConflictImportsServerRecordThenRetriesTombstone() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "delete-conflict-zone")
        let recordID = CKRecord.ID(recordName: "Bookmark.conflict", zoneID: zoneID)
        let serverRecord = CKRecord(recordType: "Bookmark", recordID: recordID)
        database.partialDeleteErrorsOnceByRecordID[recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            deletedByEntity: ["Bookmark": [recordID]]
        )
        adapter.repeatsPreparedDeletions = true
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertTrue(adapter.events.contains("save:Bookmark"))
        XCTAssertTrue(adapter.events.contains("persist"))
        XCTAssertTrue(adapter.events.contains("didDelete:Bookmark.conflict"))
        XCTAssertEqual(database.modifyRecordsOperationCount, 2)
        XCTAssertEqual(database.modifyRecordsAtomicValues, [false, false])
        XCTAssertEqual(
            database.modifyRecordsSavePolicies,
            [.ifServerRecordUnchanged, .ifServerRecordUnchanged]
        )
    }

    @BigSyncBackgroundActor
    func testRepeatedDeleteServerConflictStopsAfterBoundedRetries() async {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "bounded-delete-conflict-zone")
        let recordID = CKRecord.ID(recordName: "Bookmark.conflict", zoneID: zoneID)
        let serverRecord = CKRecord(recordType: "Bookmark", recordID: recordID)
        database.partialDeleteErrorsByRecordID[recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            deletedByEntity: ["Bookmark": [recordID]]
        )
        adapter.repeatsPreparedDeletions = true
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        do {
            try await synchronizer.synchronizeAdapter(adapter)
            XCTFail("Expected the repeated deletion conflict retry ceiling")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.modifyRecordsOperationCount, 6)
        XCTAssertEqual(adapter.events.filter { $0 == "save:Bookmark" }.count, 5)
        XCTAssertFalse(adapter.events.contains(where: { $0.hasPrefix("didDelete:") }))
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
        // Pin the per-record result contract: successful records must be
        // acknowledged independently from failures in the same batch.
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
    func testRepeatedServerConflictStopsAfterBoundedRetries() async {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "bounded-conflict-zone")
        let clientRecord = makeRecord(
            type: "Bookmark",
            id: "conflict",
            zoneID: zoneID
        )
        let serverRecord = makeRecord(
            type: "Bookmark",
            id: "conflict",
            zoneID: zoneID
        )
        database.partialSaveErrorsByRecordID[clientRecord.recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: ["Bookmark": [clientRecord]]
        )
        adapter.repeatsPreparedUploads = true
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        do {
            try await synchronizer.synchronizeAdapter(adapter)
            XCTFail("Expected the repeated conflict retry ceiling")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.modifyRecordsOperationCount, 6)
        XCTAssertEqual(
            adapter.events.filter { $0 == "save:Bookmark" }.count,
            5
        )
    }

    @BigSyncBackgroundActor
    func testRepeatedServerConflictCannotRestartAnOuterUploadLoop() async {
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let zoneID = CKRecordZone.ID(zoneName: "bounded-full-sync-conflict")
        let clientRecord = makeRecord(
            type: "Bookmark",
            id: "conflict",
            zoneID: zoneID
        )
        let serverRecord = makeRecord(
            type: "Bookmark",
            id: "conflict",
            zoneID: zoneID
        )
        database.partialSaveErrorsByRecordID[clientRecord.recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: ["Bookmark": [clientRecord]]
        )
        adapter.repeatsPreparedUploads = true
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected the bounded conflict to fail the run")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        XCTAssertEqual(database.modifyRecordsOperationCount, 6)
        XCTAssertFalse(synchronizer.syncing)
    }

    @BigSyncBackgroundActor
    func testRepeatedConflictPreservesDurableJournalGeneration() async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "bounded-real-journal-conflict"
        )
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let recordID = CKRecord.ID(
            recordName: fixture.recordName,
            zoneID: fixture.adapter.recordZoneID
        )
        let serverRecord = CKRecord(
            recordType: BigSyncTrackedObject.className(),
            recordID: recordID
        )
        let now = Date()
        serverRecord["createdAt"] = now as CKRecordValue
        serverRecord["modifiedAt"] = now as CKRecordValue
        serverRecord["explicitlyModifiedAt"] = now as CKRecordValue
        serverRecord["isDeleted"] = false as CKRecordValue
        serverRecord["tags"] = ["stale-server"] as CKRecordValue
        database.partialSaveErrorsByRecordID[recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected bounded conflict failure")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        await fixture.targetRealm.asyncRefresh()
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation,
            fixture.generation
        )
        XCTAssertEqual(database.modifyRecordsOperationCount, 6)
    }

    @BigSyncBackgroundActor
    func testRepeatedDeleteConflictPreservesJournaledTombstoneGeneration() async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "bounded-journaled-delete-conflict"
        )
        let objectID = "bounded-journaled-delete-conflict"
        try await fixture.targetRealm.asyncWrite {
            let object = try XCTUnwrap(
                fixture.targetRealm.object(
                    ofType: BigSyncTrackedObject.self,
                    forPrimaryKey: objectID
                )
            )
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let tombstoneGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation
        )
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let recordID = CKRecord.ID(
            recordName: fixture.recordName,
            zoneID: fixture.adapter.recordZoneID
        )
        let serverRecord = CKRecord(
            recordType: BigSyncTrackedObject.className(),
            recordID: recordID
        )
        let now = Date()
        serverRecord["createdAt"] = now as CKRecordValue
        serverRecord["modifiedAt"] = now as CKRecordValue
        serverRecord["explicitlyModifiedAt"] = now as CKRecordValue
        serverRecord["isDeleted"] = false as CKRecordValue
        database.partialDeleteErrorsByRecordID[recordID] = NSError(
            domain: CKErrorDomain,
            code: CKError.serverRecordChanged.rawValue,
            userInfo: [CKRecordChangedErrorServerRecordKey: serverRecord]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected bounded deletion conflict failure")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        await fixture.targetRealm.asyncRefresh()
        XCTAssertTrue(
            fixture.targetRealm.object(
                ofType: BigSyncTrackedObject.self,
                forPrimaryKey: objectID
            )?.isDeleted == true
        )
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation,
            tombstoneGeneration
        )
        XCTAssertEqual(database.modifyRecordsOperationCount, 6)
    }

    @BigSyncBackgroundActor
    func testDeleteConflictRebasesOnlyTombstoneSystemMetadata() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "delete-conflict-metadata-rebase",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let initialUpload = try await fixture.adapter.preparedRecordsToUpload(
            limit: 1,
            restrictedToEntityType: nil
        )
        let uploaded = try XCTUnwrap(initialUpload.first)
        try await fixture.adapter.didUpload(
            savedRecords: [uploaded.record],
            matchingGenerations: [
                uploaded.record.recordID.recordName:
                    try XCTUnwrap(uploaded.generation),
            ]
        )

        try await fixture.targetRealm.asyncWrite {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let preparedDeletions = try await fixture.adapter
            .preparedRecordDeletions(
                limit: 1,
                restrictedToEntityType: nil
            )
        let deletion = try XCTUnwrap(preparedDeletions.first)
        let generation = try XCTUnwrap(deletion.generation)
        let persistenceRealm = try XCTUnwrap(
            fixture.adapter.realmProvider?.persistenceRealm
        )
        try await persistenceRealm.asyncWrite {
            let tracked = try XCTUnwrap(persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: deletion.recordID.recordName
            ))
            tracked.encodedRecord = nil
        }
        let serverRecord = CKRecord(
            recordType: BigSyncTrackedObject.className(),
            recordID: deletion.recordID
        )

        try await fixture.adapter.rebasePendingDeletionMetadata(
            using: [serverRecord],
            matchingPreparedGenerations: [
                deletion.recordID.recordName: generation,
            ]
        )

        persistenceRealm.refresh()
        await fixture.targetRealm.asyncRefresh()
        let tracked = try XCTUnwrap(persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: deletion.recordID.recordName
        ))
        XCTAssertNotNil(tracked.encodedRecord)
        XCTAssertEqual(tracked.entityState, .deletedLocally)
        XCTAssertEqual(tracked.pendingGeneration, generation)
        XCTAssertTrue(object.isDeleted)
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: deletion.recordID.recordName
            )?.generation,
            generation
        )
    }

    @BigSyncBackgroundActor
    func testSuccessfulFullBatchGrowsOnlyOncePerDrain() async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "single-batch-growth-zone")
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: [],
            uploadedByEntity: [
                "Bookmark": [
                    makeRecord(type: "Bookmark", id: "one", zoneID: zoneID)
                ]
            ]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.batchSize = 1
        synchronizer.addModelAdapter(adapter)

        try await synchronizer.synchronizeAdapter(adapter)

        XCTAssertEqual(synchronizer.batchSize, 2)
        XCTAssertEqual(database.modifyRecordsOperationCount, 1)
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

    @BigSyncBackgroundActor
    func testFailedAccountPreflightSchedulesADeferredRetry() async {
        let gate = CloudKitAccountAvailabilityGate { _ in .failed }
        let actor = BigSyncBackgroundActor(accountAvailabilityGate: gate)
        actor._test_installSynchronizer(makeSynchronizer())

        let result = await actor.synchronizeCloudKit()

        XCTAssertNil(result)
        XCTAssertTrue(actor._test_hasScheduledAccountAvailabilityRetry)

        await actor.cancelSynchronization()
        XCTAssertFalse(actor._test_hasScheduledAccountAvailabilityRetry)
    }

    @BigSyncBackgroundActor
    func testTemporarilyUnavailableAccountWaitsForAccountChangeWithoutPolling()
    async {
        let gate = CloudKitAccountAvailabilityGate { _ in
            .unavailable(.temporarilyUnavailable)
        }
        let actor = BigSyncBackgroundActor(accountAvailabilityGate: gate)
        actor._test_installSynchronizer(makeSynchronizer())

        let result = await actor.synchronizeCloudKit()

        XCTAssertNil(result)
        XCTAssertFalse(actor._test_hasScheduledAccountAvailabilityRetry)
        await actor.cancelSynchronization()
    }

    @BigSyncBackgroundActor
    func testLifecycleSynchronizationReturnsAtHardDeadline() async {
        let gate = CloudKitAccountAvailabilityGate { _ in
            try? await Task.sleep(nanoseconds: 60_000_000_000)
            return .available
        }
        let actor = BigSyncBackgroundActor(accountAvailabilityGate: gate)
        actor._test_installSynchronizer(makeSynchronizer())

        let startedAt = ContinuousClock.now
        let result = await actor.synchronizeCloudKit(
            deadlineNanoseconds: 1_000_000
        )

        XCTAssertNil(result)
        XCTAssertLessThan(startedAt.duration(to: .now), .seconds(1))
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
            "didDelete:Bookmark.1",
            "didDelete:Bookmark.2",
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
    func testPreparedUploadsUseUnrestrictedBehaviorAfterPriorityWorkIsExhausted() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "wrapper-upload-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            uploadedByEntity: [
                "Article": [makeRecord(type: "Article", id: "1", zoneID: zoneID)],
            ]
        )
        let prepared = try await adapter.preparedRecordsToUpload(
            limit: 10,
            restrictedToEntityType: nil
        )
        XCTAssertEqual(prepared.map(\.record.recordType), ["Article"])
        XCTAssertEqual(prepared.map(\.record.recordID.recordName), ["Article.1"])
    }

    @BigSyncBackgroundActor
    func testPreparedDeletionsUseUnrestrictedBehaviorAfterPriorityWorkIsExhausted() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "wrapper-delete-zone", ownerName: CKCurrentUserDefaultName)
        let adapter = FakeModelAdapter(
            zoneID: zoneID,
            priorities: ["Bookmark"],
            deletedByEntity: [
                "Article": [CKRecord.ID(recordName: "Article.1", zoneID: zoneID)],
            ]
        )
        let prepared = try await adapter.preparedRecordDeletions(
            limit: 10,
            restrictedToEntityType: nil
        )
        XCTAssertEqual(prepared.map(\.recordID.recordName), ["Article.1"])
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
    func testEmptyRealmCollectionsUseAbsentCloudKitFields() async throws {
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

        let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
        let record = try XCTUnwrap(batch.records.first {
            $0.recordID.recordName == BigSyncTrackedObject.className() + ".empty-collections"
        })

        XCTAssertNil(record["tags"])
        XCTAssertNil(record["scores"])
        XCTAssertNil(record["attributes"])
        XCTAssertFalse(record.allKeys().contains("tags"))
        XCTAssertFalse(record.allKeys().contains("scores"))
        XCTAssertFalse(record.allKeys().contains("attributes"))

        let object = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncTrackedObject.self,
                forPrimaryKey: "empty-collections"
            )
        )
        XCTAssertFalse(fixture.adapter.hasChanges(record: record, object: object))

        record["attributes"] = try PropertyListSerialization.data(
            fromPropertyList: [String: String](),
            format: .binary,
            options: 0
        ) as CKRecordValue
        XCTAssertFalse(fixture.adapter.hasChanges(record: record, object: object))
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

        let batch = try await fixture.adapter.prepareUploadBatch(limit: 10)
        let record = try XCTUnwrap(batch.records.first {
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
    func testMalformedPresentScalarRollsBackWholeRemoteRecord() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let originalDate = Date(timeIntervalSinceReferenceDate: 12_500)
        let object = BigSyncTrackedObject(
            id: "malformed-scalar",
            createdAt: originalDate,
            modifiedAt: originalDate,
            explicitlyModifiedAt: originalDate
        )
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
        record["initialCloudKitSyncEligible"] = "not-a-boolean" as CKRecordValue

        do {
            try await fixture.adapter.saveChanges(in: [record], forceSave: true)
            XCTFail("Expected malformed scalar decoding to fail")
        } catch is RealmSwiftRemoteRecordDecodingError {
            // Expected.
        }
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(object.modifiedAt, originalDate)
        XCTAssertTrue(object.initialCloudKitSyncEligible)
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
    func testRelationshipReferenceRejectsMalformedRecordName() async throws {
        let reference = CKRecord.Reference(
            recordID: CKRecord.ID(
                recordName: "missing-type-prefix",
                zoneID: CKRecordZone.ID(
                    zoneName: "realm-adapter-zone",
                    ownerName: CKCurrentUserDefaultName
                )
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

        let uploadBatch = try await fixture.adapter.prepareUploadBatch(
            limit: 10
        )
        let record = try XCTUnwrap(
            uploadBatch.records.first {
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
    func testPendingLocalTombstoneRemainsADeletionAfterConcurrentRemoteDeletion()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "locally-deleted",
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
            [uploaded], from: uploadBatch
        )

        try await fixture.targetRealm.asyncWrite {
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let mutation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )

        try await fixture.adapter.deleteRecords(with: [uploaded.recordID])

        let tracking = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
        XCTAssertEqual(tracking.entityState, .deletedLocally)
        XCTAssertEqual(tracking.pendingGeneration, mutation.generation)
        let deletionBatch = try await fixture.adapter.prepareDeletionBatch(limit: 1)
        XCTAssertEqual(deletionBatch.recordIDs, [uploaded.recordID])
        try await fixture.adapter.acknowledgeDeletedRecordIDs(
            [uploaded.recordID], from: deletionBatch
        )
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: uploaded.recordID.recordName
            )
        )
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
    func testNewerRemoteEmptyRelationshipReplacesMissingTargetIntent()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let parent = BigSyncRelationshipParent()
        parent.id = "parent"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(parent)
        }
        let remoteDate = Date(timeIntervalSinceReferenceDate: 31_000)
        let relationshipRecord = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        relationshipRecord["children"] = [
            "\(BigSyncRelationshipChild.className()).late"
        ] as CKRecordValue
        relationshipRecord["modifiedAt"] = remoteDate as CKRecordValue
        relationshipRecord["explicitlyModifiedAt"] = remoteDate as CKRecordValue

        try await fixture.adapter.saveChanges(
            in: [relationshipRecord],
            forceSave: true
        )
        try await fixture.adapter.persistImportedChanges()
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self)
                .where { $0.relationshipName == "children" }.count,
            1
        )

        // CloudKit omits empty relationship collections. Keep identical
        // metadata to prove correctness does not depend on timestamp ordering.
        let clearedRecord = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        clearedRecord["modifiedAt"] = remoteDate as CKRecordValue
        clearedRecord["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        try await fixture.adapter.saveChanges(
            in: [clearedRecord],
            forceSave: true
        )
        try await fixture.adapter.persistImportedChanges()
        XCTAssertTrue(parent.children.isEmpty)
        XCTAssertTrue(
            fixture.persistenceRealm.objects(PendingRelationship.self)
                .where { $0.relationshipName == "children" }.isEmpty
        )

        let lateChild = BigSyncRelationshipChild()
        lateChild.id = "late"
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(lateChild)
        }
        try await fixture.adapter.persistImportedChanges()

        XCTAssertTrue(parent.children.isEmpty)
        XCTAssertTrue(
            fixture.persistenceRealm.objects(PendingRelationship.self).isEmpty
        )
    }

    @BigSyncBackgroundActor
    func testCleanupRemovesDeferredRelationshipsWithRetiredRemoteDeletion()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let parent = BigSyncRelationshipParent()
        parent.id = "remotely-deleted-parent"
        let parentID = parent.id
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(parent)
        }

        let record = makeRecord(
            type: BigSyncRelationshipParent.className(),
            id: parent.id,
            zoneID: fixture.adapter.recordZoneID
        )
        record["children"] = [
            "\(BigSyncRelationshipChild.className()).missing"
        ] as CKRecordValue
        record["modifiedAt"] = Date().addingTimeInterval(60) as CKRecordValue
        record["explicitlyModifiedAt"] =
            Date().addingTimeInterval(60) as CKRecordValue

        try await fixture.adapter.saveChanges(in: [record], forceSave: true)
        try await fixture.adapter.persistImportedChanges()
        try await fixture.persistenceRealm.asyncWrite {
            let alreadyOrphaned = PendingRelationship()
            alreadyOrphaned.relationshipName = "favoriteChild"
            alreadyOrphaned.targetIdentifier =
                BigSyncRelationshipChild.className() + ".also-missing"
            fixture.persistenceRealm.add(alreadyOrphaned)
        }
        XCTAssertEqual(
            fixture.persistenceRealm.objects(PendingRelationship.self).count,
            2
        )

        try await fixture.adapter.deleteRecords(with: [record.recordID])
        try await fixture.adapter.cleanUp()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncRelationshipParent.self,
                forPrimaryKey: parentID
            )
        )
        XCTAssertNil(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: record.recordID.recordName
            )
        )
        XCTAssertTrue(
            fixture.persistenceRealm.objects(PendingRelationship.self).isEmpty
        )
    }

    @BigSyncBackgroundActor
    func testRealmTerminalBoundaryDetectsDurablePendingMutation() async throws {
        // The receipt cut must see the durable target journal before its
        // debounced observer becomes the upload wakeup.
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "terminal-boundary-journal",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }

        XCTAssertTrue(
            try fixture.adapter.hasPendingChangesAtTerminalBoundary()
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
    func testJournalForwardingRechecksGenerationAfterPageSuspension()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "reentrant-journal-generation",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let firstGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        let enteredFirstForward = AsyncGate()
        let releaseFirstForward = AsyncGate()
        fixture.adapter._testBeforePendingMutationTrackingWrite = {
            guard !(await enteredFirstForward.hasOpened()) else { return }
            await enteredFirstForward.open()
            await releaseFirstForward.wait()
        }

        let firstForward = Task { @BigSyncBackgroundActor in
            try await fixture.adapter._test_forwardPendingMutations(
                in: fixture.targetRealm
            )
        }
        await enteredFirstForward.wait()

        try await fixture.targetRealm.asyncWrite {
            object.tags.append("newer")
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let secondGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        XCTAssertNotEqual(firstGeneration, secondGeneration)

        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.pendingGeneration,
            secondGeneration
        )

        await releaseFirstForward.open()
        _ = try await firstForward.value
        fixture.adapter._testBeforePendingMutationTrackingWrite = nil

        XCTAssertEqual(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.pendingGeneration,
            secondGeneration
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
    func testSetupRejectsOneTrackedTypeOwnedByMultipleTargetRealms()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "duplicate-owner-persistence-\(identifier)"
        var firstTarget = Realm.Configuration()
        firstTarget.inMemoryIdentifier = "duplicate-owner-first-\(identifier)"
        firstTarget.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        var secondTarget = Realm.Configuration()
        secondTarget.inMemoryIdentifier = "duplicate-owner-second-\(identifier)"
        secondTarget.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [firstTarget, secondTarget],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "duplicate-owner"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )

        do {
            try await adapter.unsetCancellation()
            XCTFail("Expected duplicate target-Realm ownership to fail setup")
        } catch let error as RealmSwiftAdapterError {
            guard case let .duplicateTrackedEntityType(entityType) = error else {
                XCTFail("Unexpected adapter error: \(error)")
                return
            }
            XCTAssertEqual(entityType, BigSyncTrackedObject.className())
        }
        XCTAssertNil(adapter.realmProvider)
    }

    @BigSyncBackgroundActor
    func testSetupAssignsEveryMissingGenerationWithoutMutatingLiveResults()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "missing-generation-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "missing-generation-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]

        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let recordCount = 1_001
        try await persistenceRealm.asyncWrite {
            for index in 0..<recordCount {
                persistenceRealm.add(
                    SyncedEntity(
                        entityType: BigSyncTrackedObject.className(),
                        identifier: "\(BigSyncTrackedObject.className()).legacy-\(index)",
                        state: SyncedEntityState.changed.rawValue
                    )
                )
            }
        }

        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "missing-generations"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )

        try await adapter.unsetCancellation()
        persistenceRealm.refresh()

        XCTAssertEqual(
            persistenceRealm.objects(SyncedEntity.self).where {
                $0.state == SyncedEntityState.changed.rawValue
                    && $0.pendingGeneration != nil
            }.count,
            recordCount
        )
    }

    @BigSyncBackgroundActor
    func testSaveTokenWaitsForCompletedRealmSetupAndPropagatesFailure()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "token-readiness-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "token-readiness-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "token-readiness"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let delegate = FakeModelAdapterDelegate()
        delegate.initialSetupHandler = {
            throw TestSynchronizationError.initialSetupFailed
        }
        adapter.modelAdapterDelegate = delegate

        do {
            try await adapter.saveToken(nil)
            XCTFail("Expected setup failure before token publication")
        } catch TestSynchronizationError.initialSetupFailed {
        }
        XCTAssertEqual(delegate.initialSetupCount, 1)
        XCTAssertEqual(
            adapter.realmProvider?.persistenceRealm?
                .objects(ServerToken.self).count,
            0
        )

        delegate.initialSetupHandler = nil
        try await adapter.saveToken(nil)

        XCTAssertEqual(delegate.initialSetupCount, 2)
        XCTAssertEqual(
            adapter.realmProvider?.persistenceRealm?
                .objects(ServerToken.self).count,
            1
        )
    }

    @BigSyncBackgroundActor
    func testRealmAdapterPersistsOpaqueRecordZoneCursorBytes() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let cursor = RecordZoneChangeCursor(
            serializedData: Data([0, 255, 17, 42])
        )

        try await fixture.adapter.saveToken(cursor)

        let persistedCursor = await fixture.adapter.serverChangeToken
        XCTAssertEqual(persistedCursor, cursor)
        XCTAssertEqual(
            fixture.persistenceRealm.objects(ServerToken.self).first?.token,
            cursor.serializedData
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
    func testPreSetupCacheResetClearsTrackingAndPreservesTargetJournal()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "pre-setup-reset-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "pre-setup-reset-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]

        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "pre-setup-reset"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )

        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let object = BigSyncTrackedObject(
            id: "journal-survives-reset",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await targetRealm.asyncWrite {
            targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let expectedGeneration = try XCTUnwrap(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        try await persistenceRealm.asyncWrite {
            let staleEntity = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: "BigSyncTrackedObject.stale-account",
                state: SyncedEntityState.changed.rawValue
            )
            staleEntity.pendingGeneration = UUID().uuidString
            persistenceRealm.add(staleEntity)
            persistenceRealm.add(
                SyncedEntityType(
                    entityType: BigSyncTrackedObject.className(),
                    lastTrackedChangesAt: Date()
                )
            )
            let pendingRelationship = PendingRelationship()
            pendingRelationship.relationshipName = "favoriteChild"
            pendingRelationship.targetIdentifier = "stale-target"
            pendingRelationship.forSyncedEntity = staleEntity
            persistenceRealm.add(pendingRelationship)
            persistenceRealm.add(ServerToken())
        }
        adapter.cancelSynchronization()
        try await adapter.resetSyncCaches()
        persistenceRealm.refresh()
        targetRealm.refresh()

        XCTAssertNil(adapter.realmProvider)
        XCTAssertTrue(persistenceRealm.objects(SyncedEntity.self).isEmpty)
        XCTAssertTrue(
            persistenceRealm.objects(SyncedEntityType.self).isEmpty
        )
        XCTAssertTrue(
            persistenceRealm.objects(PendingRelationship.self).isEmpty
        )
        XCTAssertTrue(persistenceRealm.objects(ServerToken.self).isEmpty)
        XCTAssertEqual(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation,
            expectedGeneration
        )
        XCTAssertNotNil(
            targetRealm.object(
                ofType: BigSyncTrackedObject.self,
                forPrimaryKey: object.id
            )
        )

        try await adapter.unsetCancellation()
        persistenceRealm.refresh()
        targetRealm.refresh()

        XCTAssertEqual(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.pendingGeneration,
            expectedGeneration
        )
        XCTAssertEqual(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation,
            expectedGeneration
        )
    }

    @BigSyncBackgroundActor
    func testChangeFeedRebuildKeepsJournalAuthorityAndDoesNotUploadUnjournaledTombstone()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier = "change-feed-rebuild-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "change-feed-rebuild-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "change-feed-rebuild-zone"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let localOnly = BigSyncTrackedObject(
            id: "local-only",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let tombstone = BigSyncTrackedObject(
            id: "acknowledged-tombstone",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        tombstone.isDeleted = true
        let journaled = BigSyncTrackedObject(
            id: "journal-wins",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let priorServerObject = BigSyncTrackedObject(
            id: "await-server-evidence",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        try await targetRealm.asyncWrite {
            targetRealm.add(localOnly)
            targetRealm.add(tombstone)
            targetRealm.add(journaled)
            targetRealm.add(priorServerObject)
            journaled.refreshChangeMetadata(explicitlyModified: true)
        }
        let journalRecordName = BigSyncTrackedObject.className() + ".journal-wins"
        let expectedGeneration = try XCTUnwrap(
            targetRealm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: journalRecordName)?.generation
        )

        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "account-a",
            epoch: 1,
            mode: .initialImport
        )
        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: "account-a",
            epoch: 1,
            mode: .initialImport
        )
        let persistenceRealm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let priorServerRecordName = BigSyncTrackedObject.className() + ".await-server-evidence"
        // This row represents the valid prior server proof captured before a
        // real reset. It must suppress resurrection when the nil-token fetch
        // supplies neither a record nor an explicit deletion.
        try await persistenceRealm.asyncWrite {
            let provenance = RebuildProvenance()
            provenance.identifier = priorServerRecordName
            provenance.entityType = BigSyncTrackedObject.className()
            provenance.hadValidServerRecord = true
            provenance.accountScopeIdentifier = "account-a"
            provenance.epoch = 1
            persistenceRealm.add(provenance, update: .modified)
        }
        // No remote pages are applied: this exercises exactly the post-nil-
        // token reconciliation boundary.
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: "account-a",
            epoch: 1,
            mode: .initialImport
        )
        let localOnlyRecordName = BigSyncTrackedObject.className() + ".local-only"
        let tombstoneRecordName = BigSyncTrackedObject.className() + ".acknowledged-tombstone"

        XCTAssertEqual(
            persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: localOnlyRecordName)?.entityState,
            .new
        )
        XCTAssertNil(
            persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: tombstoneRecordName)
        )
        XCTAssertEqual(
            persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: journalRecordName)?.pendingGeneration,
            expectedGeneration
        )
        XCTAssertEqual(
            persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: priorServerRecordName)?.entityState,
            .awaitingServerEvidence
        )
        try await adapter.finishChangeFeedReset(
            accountScopeIdentifier: "account-a",
            epoch: 1,
            mode: .initialImport
        )
        XCTAssertNil(
            persistenceRealm.object(ofType: SyncedEntity.self, forPrimaryKey: priorServerRecordName)
        )
        XCTAssertNotNil(
            targetRealm.object(ofType: BigSyncTrackedObject.self, forPrimaryKey: priorServerObject.id)
        )

        // A later explicit local edit is authoritative and recreates normal
        // upload tracking; completed rebuild state must not rediscover the
        // stale object before that journal boundary.
        try await targetRealm.asyncWrite {
            priorServerObject.modifiedAt = Date()
            priorServerObject.refreshChangeMetadata(explicitlyModified: true)
        }
        let laterGeneration = try XCTUnwrap(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: priorServerRecordName
            )?.generation
        )
        try await adapter.resetSyncCaches()
        let resumedPersistenceRealm = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertEqual(
            resumedPersistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: priorServerRecordName
            )?.pendingGeneration,
            laterGeneration
        )
        XCTAssertNotNil(targetRealm.object(ofType: BigSyncTrackedObject.self, forPrimaryKey: tombstone.id))
    }

    @BigSyncBackgroundActor
    func testAsyncChangeFeedConsumesEveryDatabaseAndZonePageBeforeReceipt()
    async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "multipage-change-feed")
        let firstRecord = makeRecord(type: "FirstPage", id: "one", zoneID: zoneID)
        let secondRecord = makeRecord(type: "SecondPage", id: "two", zoneID: zoneID)
        database.databaseChangePages = [
            FakeDatabaseChangePage(
                changedZoneIDs: [zoneID], deletions: [], moreComing: true
            ),
            FakeDatabaseChangePage(
                changedZoneIDs: [], deletions: [], moreComing: false
            ),
        ]
        database.zoneChangePages = [
            FakeZoneChangePage(
                zoneID: zoneID, records: [firstRecord], deletedRecordIDs: [],
                moreComing: true
            ),
            FakeZoneChangePage(
                zoneID: zoneID, records: [secondRecord], deletedRecordIDs: [],
                moreComing: false
            ),
        ]
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        // The terminal post-upload poll is a third, empty database page. The
        // receipt therefore cannot be issued at the first terminal page of
        // the original database listing.
        XCTAssertEqual(database.databaseChangeFetchCount, 3)
        XCTAssertEqual(database.recordZoneChangeFetchCount, 2)
        XCTAssertEqual(
            synchronizer.storedDatabaseToken?.serializedData,
            Data("database-3".utf8)
        )
        let storedZoneCursor = await adapter.serverChangeToken
        XCTAssertEqual(storedZoneCursor?.serializedData, Data("zone-2".utf8))
        XCTAssertGreaterThanOrEqual(
            adapter.events.filter { $0 == "saveToken" }.count,
            2
        )
        XCTAssertEqual(
            adapter.events.filter { $0.hasPrefix("save:") },
            ["save:FirstPage", "save:SecondPage"]
        )
    }

    @BigSyncBackgroundActor
    func testAsyncChangeFeedImportFailureDoesNotAdvanceZoneOrDatabaseCursor()
    async throws {
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(zoneName: "token-last-import-failure")
        let record = makeRecord(type: "FailsToImport", id: "one", zoneID: zoneID)
        database.databaseChangePages = [
            FakeDatabaseChangePage(
                changedZoneIDs: [zoneID], deletions: [], moreComing: false
            )
        ]
        database.zoneChangePages = [
            FakeZoneChangePage(
                zoneID: zoneID, records: [record], deletedRecordIDs: [],
                moreComing: false
            )
        ]
        let adapter = FakeModelAdapter(zoneID: zoneID, priorities: [])
        adapter.saveChangesHandler = {
            throw TestSynchronizationError.importedPersistenceCacheFailed
        }
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected import failure")
        } catch TestSynchronizationError.importedPersistenceCacheFailed {
        }

        XCTAssertEqual(database.databaseChangeFetchCount, 1)
        XCTAssertEqual(database.recordZoneChangeFetchCount, 1)
        XCTAssertNil(synchronizer.storedDatabaseToken)
        let storedZoneCursor = await adapter.serverChangeToken
        XCTAssertNil(storedZoneCursor)
        XCTAssertFalse(adapter.events.contains("saveToken"))
    }

    @BigSyncBackgroundActor
    func testChangeFeedResetPrepareIsIdempotentAfterCrash() async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter.defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier = "change-feed-resume-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "change-feed-resume-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let zoneID = CKRecordZone.ID(zoneName: "change-feed-resume-zone")
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: zoneID,
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let object = BigSyncTrackedObject(
            id: "previously-on-server",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await targetRealm.asyncWrite {
            // Deliberately unjournaled: this represents an acknowledged local
            // cache of a record previously known to CloudKit.
            targetRealm.add(object)
        }
        try await persistenceRealm.asyncWrite {
            persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }

        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "resume-account", epoch: 7
        )
        persistenceRealm.refresh()
        XCTAssertTrue(persistenceRealm.objects(SyncedEntity.self).isEmpty)
        let captured = try XCTUnwrap(
            persistenceRealm.object(
                ofType: RebuildProvenance.self,
                forPrimaryKey: recordName
            )
        )
        // A synced state remains prior-server evidence even when the optional
        // cached system fields are unavailable or corrupt.
        XCTAssertTrue(captured.hadValidServerRecord)
        let expectedAccount = captured.accountScopeIdentifier
        let expectedEpoch = captured.epoch
        let expectedState = captured.priorState

        // Simulate relaunch: the synchronizer repeats prepare for the same
        // durable migration. Original proof must survive unchanged.
        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "resume-account", epoch: 7
        )
        persistenceRealm.refresh()
        XCTAssertTrue(persistenceRealm.objects(SyncedEntity.self).isEmpty)
        let resumed = try XCTUnwrap(
            persistenceRealm.object(
                ofType: RebuildProvenance.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertTrue(resumed.hadValidServerRecord)
        XCTAssertEqual(resumed.accountScopeIdentifier, expectedAccount)
        XCTAssertEqual(resumed.epoch, expectedEpoch)
        XCTAssertEqual(resumed.priorState, expectedState)

        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: "resume-account", epoch: 7
        )
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: "resume-account", epoch: 7
        )
        XCTAssertEqual(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: recordName
            )?.entityState,
            .awaitingServerEvidence
        )
        XCTAssertFalse(adapter.hasChanges)
    }

    @BigSyncBackgroundActor
    func testChangeFeedBootstrapTreatsNeverCreatedZoneAsEmptyAndUploadsJournal() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "never-created-zone-local-edit",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        object.tags.append("local")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )

        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(database: database)
        // A database cursor proves only that this client has seen database
        // history. It must not falsely establish every newly configured zone.
        synchronizer.storedDatabaseToken = DatabaseChangeCursor(
            serializedData: Data("unrelated-database-history".utf8)
        )
        synchronizer.addModelAdapter(fixture.adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.savedZoneCount, 1)
        XCTAssertGreaterThanOrEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertTrue(database.deletedZoneIDs.isEmpty)
        XCTAssertFalse(synchronizer.configuredZoneIsTerminal(fixture.adapter.recordZoneID))
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )
        )
        XCTAssertEqual(
            database.record(for: CKRecord.ID(
                recordName: recordName,
                zoneID: fixture.adapter.recordZoneID
            ))?["tags"] as? [String],
            ["local"]
        )
        XCTAssertNotEqual(generation, "")
    }

    @BigSyncBackgroundActor
    func testExpiredOrCorruptCursorRebuildsTrackingWithoutLosingJournal()
    async throws {
        let recoveryErrors: [Error] = [
            CKError(.changeTokenExpired),
            CloudKitChangeFeedError.corruptCursor,
        ]

        for (index, recoveryError) in recoveryErrors.enumerated() {
            let fixture = try await makeRealmAdapterFixture()
            let database = FakeCloudKitDatabase()
            database.completesEmptyZoneChangeOperation = true
            let synchronizer = makeSynchronizer(database: database)
            synchronizer.addModelAdapter(fixture.adapter)

            // Complete the one-time transport migration first. The injected
            // cursor failure below must request a *new* server-first epoch,
            // not rely on first-install behavior.
            _ = try await synchronizer.synchronize()

            let object = BigSyncTrackedObject(
                id: "cursor-recovery-\(index)",
                createdAt: Date(),
                modifiedAt: Date(),
                explicitlyModifiedAt: nil
            )
            object.tags.append("durable-local")
            try await fixture.targetRealm.asyncWrite {
                fixture.targetRealm.add(object)
                object.refreshChangeMetadata(explicitlyModified: true)
            }
            let recordName = BigSyncTrackedObject.className() + "." + object.id
            let generation = try XCTUnwrap(
                fixture.targetRealm.object(
                    ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: recordName
                )?.generation
            )
            database.nextDatabaseChangesError = recoveryError

            let result = try await synchronizer.synchronize()

            XCTAssertNotNil(result.receipt)
            XCTAssertNil(
                fixture.targetRealm.object(
                    ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: recordName
                )
            )
            XCTAssertEqual(
                database.record(
                    for: CKRecord.ID(
                        recordName: recordName,
                        zoneID: fixture.adapter.recordZoneID
                    )
                )?["tags"] as? [String],
                ["durable-local"]
            )
            XCTAssertFalse(generation.isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testCancellingAStalledAsyncZoneLookupReleasesSynchronization()
    async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "stalled-zone-lookup"
        )
        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        database.completesEmptyZoneChangeOperation = true
        database.completesRecordZoneFetches = false
        let enteredLookup = AsyncGate()
        database.recordZoneFetchHandler = {
            Task { await enteredLookup.open() }
        }
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        let synchronization = Task { @BigSyncBackgroundActor in
            try await synchronizer.synchronize()
        }
        await enteredLookup.wait()
        await synchronizer.cancelSynchronizationAndWait()

        do {
            _ = try await synchronization.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {
            // Expected: the caller is released even if CloudKit was suspended.
        }
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation,
            fixture.generation
        )
    }

    @BigSyncBackgroundActor
    func testZoneLifecycleAccountReplacementNeverPublishesUpload()
    async throws {
        for boundary in ["fetch", "save"] {
            let fixture = try await makeJournaledZoneFixture(
                id: "zone-account-switch-\(boundary)"
            )
            let database = FakeCloudKitDatabase()
            database.completesEmptyZoneChangeOperation = true
            if boundary == "fetch" {
                database.zoneExists = true
                database.accountIdentifierAfterNextZoneFetch = "other-account"
            } else {
                database.zoneExists = false
                database.accountIdentifierAfterNextZoneSave = "other-account"
            }
            let synchronizer = makeSynchronizer(
                database: database,
                accountIdentifierProvider: { database.accountIdentifier }
            )
            synchronizer.addModelAdapter(fixture.adapter)

            // A normal fresh fetch persists its first zone cursor before the
            // upload phase, so an already-existing zone does not subsequently
            // require lifecycle setup. Exercise the lifecycle boundary itself
            // with the same active account context the synchronization run
            // installs before any CloudKit work.
            let attemptID = synchronizer.synchronizationAttemptID
            synchronizer.activeRunContext = CloudKitSynchronizer.RunContext(
                attemptID: attemptID,
                runID: synchronizer.synchronizationRunID,
                accountIdentifier: "test-account",
                accountScopeIdentifier: "test-account-scope"
            )

            do {
                try await synchronizer.setupRecordZoneID(
                    fixture.adapter.recordZoneID,
                    attemptID: attemptID
                ) { error in
                    if let error {
                        throw error
                    }
                }
                XCTFail("Expected account replacement at \(boundary)")
            } catch OneOffRecordZoneResetError.cloudKitAccountChanged {
                // Expected.
            }

            XCTAssertEqual(database.modifyRecordsOperationCount, 0)
            XCTAssertEqual(
                fixture.targetRealm.object(
                    ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: fixture.recordName
                )?.generation,
                fixture.generation
            )
        }
    }

    @BigSyncBackgroundActor
    func testChangeFeedBootstrapNeverRecreatesAnEstablishedMissingZone() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "established-zone-local-edit",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        object.tags.append("preserve-me")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )

        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        let synchronizer = makeSynchronizer(
            database: database,
            recordZoneID: fixture.adapter.recordZoneID
        )
        try synchronizer.markConfiguredZoneEstablished(
            fixture.adapter.recordZoneID,
            accountScopeIdentifier:
                CloudKitSynchronizer.accountScopeIdentifier(
                    for: database.accountIdentifier
                )
        )
        synchronizer.addModelAdapter(fixture.adapter)

        for _ in 0..<2 {
            do {
                _ = try await synchronizer.synchronize()
                XCTFail("Expected the established missing zone to block sync")
            } catch ChangeFeedMigrationError.establishedZoneUnavailable(
                let zoneID,
                _
            ) {
                XCTAssertEqual(zoneID, fixture.adapter.recordZoneID)
            } catch {
                XCTFail("Unexpected error: \(error)")
            }
        }

        await fixture.targetRealm.asyncRefresh()
        XCTAssertEqual(Array(object.tags), ["preserve-me"])
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation,
            generation
        )
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testChangeFeedBootstrapTreatsDatabaseZoneDeletionAsTerminalEvidence()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: "database-deleted-zone-local-edit",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.tags.append("preserve-me")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )

        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        database.databaseDeletedZoneIDs = [fixture.adapter.recordZoneID]
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected server deletion history to block zone recreation")
        } catch ChangeFeedMigrationError.establishedZoneUnavailable(
            let zoneID,
            _
        ) {
            XCTAssertEqual(zoneID, fixture.adapter.recordZoneID)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        await fixture.targetRealm.asyncRefresh()
        XCTAssertTrue(
            synchronizer.configuredZoneIsEstablished(
                fixture.adapter.recordZoneID
            )
        )
        XCTAssertTrue(
            synchronizer.configuredZoneIsTerminal(
                fixture.adapter.recordZoneID
            )
        )
        XCTAssertEqual(Array(object.tags), ["preserve-me"])
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation,
            generation
        )
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testPurgedZoneNeverReuploadsAndSurfacesDurableReason()
    async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "purged-zone-local-edit"
        )
        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        database.databaseChangePages = [FakeDatabaseChangePage(
            changedZoneIDs: [],
            deletions: [CloudKitZoneDeletion(
                zoneID: fixture.adapter.recordZoneID,
                kind: .purged
            )],
            moreComing: false
        )]
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected purged CloudKit zone to stop synchronization")
        } catch ChangeFeedMigrationError.establishedZoneUnavailable(
            let zoneID,
            let kind
        ) {
            XCTAssertEqual(zoneID, fixture.adapter.recordZoneID)
            XCTAssertEqual(kind, .purged)
        }

        await fixture.targetRealm.asyncRefresh()
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )?.generation,
            fixture.generation
        )
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertEqual(
            synchronizer.configuredZoneTerminalState(
                fixture.adapter.recordZoneID
            )?.deletionKind,
            .purged
        )
        let health = try await synchronizer.syncHealthSnapshot()
        XCTAssertEqual(health?.category, .terminalZoneUnavailable)
        XCTAssertEqual(health?.terminalZoneDeletionKind, .purged)

        // A later caller must receive the durable terminal reason immediately;
        // it must not enqueue a waiter behind the deliberately quiescent zone.
        do {
            _ = try await synchronizer.synchronize()
            XCTFail("Expected the purged zone to remain terminal")
        } catch ChangeFeedMigrationError.establishedZoneUnavailable(
            let zoneID,
            let kind
        ) {
            XCTAssertEqual(zoneID, fixture.adapter.recordZoneID)
            XCTAssertEqual(kind, .purged)
        }
        XCTAssertEqual(database.savedZoneCount, 0)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
    }

    @BigSyncBackgroundActor
    func testEncryptedDataResetRebuildsJournalAndPreservesTargetRealm()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let now = Date()
        let live = BigSyncTrackedObject(
            id: "encrypted-reset-live",
            createdAt: now,
            modifiedAt: now,
            explicitlyModifiedAt: now
        )
        live.tags.append("local-live")
        let tombstone = BigSyncTrackedObject(
            id: "encrypted-reset-tombstone",
            createdAt: now,
            modifiedAt: now,
            explicitlyModifiedAt: nil
        )
        tombstone.tags.append("local-tombstone")
        let tombstoneID = tombstone.id
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(live)
            fixture.targetRealm.add(tombstone)
            tombstone.isDeleted = true
            tombstone.refreshChangeMetadata(
                explicitlyModified: true,
                at: now
            )
        }
        let liveRecordName = BigSyncTrackedObject.className()
            + "." + live.id
        let tombstoneRecordName = BigSyncTrackedObject.className()
            + "." + tombstone.id
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: liveRecordName
        ))
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: tombstoneRecordName
        ))

        let database = FakeCloudKitDatabase()
        database.zoneExists = false
        database.completesEmptyZoneChangeOperation = true
        database.databaseChangePages = [FakeDatabaseChangePage(
            changedZoneIDs: [],
            deletions: [CloudKitZoneDeletion(
                zoneID: fixture.adapter.recordZoneID,
                kind: .encryptedDataReset
            )],
            moreComing: false
        )]
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.savedZoneCount, 1)
        XCTAssertEqual(
            database.record(for: CKRecord.ID(
                recordName: liveRecordName,
                zoneID: fixture.adapter.recordZoneID
            ))?["tags"] as? [String],
            ["local-live"]
        )
        XCTAssertNil(database.record(for: CKRecord.ID(
            recordName: tombstoneRecordName,
            zoneID: fixture.adapter.recordZoneID
        )))
        await fixture.targetRealm.asyncRefresh()
        XCTAssertEqual(Array(live.tags), ["local-live"])
        // The reset never deletes target data. After the recovered upload is
        // acknowledged, ordinary tombstone cleanup may physically remove an
        // object the user had already deleted.
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self,
            forPrimaryKey: tombstoneID
        ))
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: liveRecordName
        ))
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: tombstoneRecordName
        ))
        XCTAssertFalse(
            synchronizer.configuredZoneIsTerminal(
                fixture.adapter.recordZoneID
            )
        )
        let health = try await synchronizer.syncHealthSnapshot()
        XCTAssertEqual(health?.category, .succeeded)
        XCTAssertNil(health?.terminalZoneDeletionKind)
    }

    @BigSyncBackgroundActor
    func testRejectedRemoteConflictCreatesDurableJournalGeneration()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let localDate = Date()
        let remoteDate = localDate.addingTimeInterval(-60)
        let object = BigSyncTrackedObject(
            id: "rejected-remote-conflict",
            createdAt: localDate,
            modifiedAt: localDate,
            explicitlyModifiedAt: localDate
        )
        object.tags.append("local")
        try await fixture.targetRealm.asyncWrite {
            // Deliberately model pre-journal local state. The conflict decision
            // below must durably claim it before retaining local values.
            fixture.targetRealm.add(object)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self,
            forPrimaryKey: recordName
        ))

        let remoteRecord = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        remoteRecord["createdAt"] = remoteDate as CKRecordValue
        remoteRecord["modifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["explicitlyModifiedAt"] = remoteDate as CKRecordValue
        remoteRecord["isDeleted"] = false as CKRecordValue
        remoteRecord["tags"] = ["remote"] as CKRecordValue

        fixture.adapter.mergePolicy = .custom
        try await fixture.adapter.saveChanges(
            in: [remoteRecord],
            forceSave: true
        )
        try await fixture.adapter.persistImportedChanges()
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(Array(object.tags), ["local"])
        let mutationGeneration = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        try await fixture.adapter.didFinishImport()
        let prepared = try await fixture.adapter.preparedRecordsToUpload(
            limit: 10,
            restrictedToEntityType: nil
        )
        XCTAssertEqual(prepared.count, 1)
        XCTAssertEqual(prepared.first?.record.recordID.recordName, recordName)
        XCTAssertEqual(prepared.first?.generation, mutationGeneration)
        XCTAssertEqual(
            prepared.first?.record["tags"] as? [String],
            ["local"]
        )
    }

    @BigSyncBackgroundActor
    func testRejectsRealmAdaptersForDifferentZonesThatShareTrackingRealm() async throws {
        let nonce = UUID().uuidString
        var sharedPersistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        sharedPersistence.inMemoryIdentifier = "shared-tracking-\(nonce)"

        func makeAdapter(
            persistence: Realm.Configuration,
            zoneName: String,
            targetIdentifier: String
        ) -> RealmSwiftAdapter {
            var target = Realm.Configuration()
            target.inMemoryIdentifier = targetIdentifier
            target.objectTypes = [
                BigSyncTrackedObject.self,
                BigSyncPendingMutation.self,
            ]
            return RealmSwiftAdapter(
                persistenceRealmConfiguration: persistence,
                targetRealmConfigurations: [target],
                excludedClassNames: [],
                recordZoneID: CKRecordZone.ID(zoneName: zoneName),
                logger: Logger(label: "BigSyncKitTests"),
                startSetupTask: false
            )
        }

        let first = makeAdapter(
            persistence: sharedPersistence,
            zoneName: "shared-tracking-first-\(nonce)",
            targetIdentifier: "shared-tracking-target-first-\(nonce)"
        )
        let conflicting = makeAdapter(
            persistence: sharedPersistence,
            zoneName: "shared-tracking-second-\(nonce)",
            targetIdentifier: "shared-tracking-target-second-\(nonce)"
        )
        var distinctPersistence = RealmSwiftAdapter.defaultPersistenceConfiguration()
        distinctPersistence.inMemoryIdentifier = "distinct-tracking-\(nonce)"
        let distinct = makeAdapter(
            persistence: distinctPersistence,
            zoneName: "distinct-tracking-\(nonce)",
            targetIdentifier: "distinct-tracking-target-\(nonce)"
        )

        let synchronizer = makeSynchronizer()
        XCTAssertTrue(synchronizer.canAddModelAdapter(first))
        synchronizer.addModelAdapter(first)
        XCTAssertTrue(synchronizer.canAddModelAdapter(first))
        XCTAssertFalse(synchronizer.canAddModelAdapter(conflicting))
        let replacement = makeAdapter(
            persistence: distinctPersistence,
            zoneName: first.recordZoneID.zoneName,
            targetIdentifier: "replacement-target-\(nonce)"
        )
        XCTAssertFalse(synchronizer.canAddModelAdapter(replacement))
        XCTAssertFalse(synchronizer.canAddModelAdapter(distinct))
        XCTAssertEqual(synchronizer.modelAdapters.count, 1)
    }

    @BigSyncBackgroundActor
    func testChangeFeedBootstrapSameDeviceRecordPreservesJournaledPayloadAndGeneration()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let baseline = Date(timeIntervalSinceReferenceDate: 40_000)
        let object = BigSyncTrackedObject(
            id: "bootstrap-local-wins",
            createdAt: baseline,
            modifiedAt: baseline,
            explicitlyModifiedAt: baseline
        )
        object.tags.append("local")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )

        try await fixture.adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "bootstrap-account", epoch: 1
        )
        try await fixture.adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: "bootstrap-account", epoch: 1
        )
        let olderSameDeviceRecord = makeRecord(
            type: BigSyncTrackedObject.className(),
            id: object.id,
            zoneID: fixture.adapter.recordZoneID
        )
        olderSameDeviceRecord["createdAt"] = baseline as CKRecordValue
        olderSameDeviceRecord["modifiedAt"] = baseline.addingTimeInterval(-60) as CKRecordValue
        olderSameDeviceRecord["explicitlyModifiedAt"] = baseline.addingTimeInterval(-60) as CKRecordValue
        olderSameDeviceRecord["isDeleted"] = false as CKRecordValue
        olderSameDeviceRecord["tags"] = ["server"] as CKRecordValue
        olderSameDeviceRecord[cloudKitSynchronizerDeviceUUIDKey] = "this-device" as CKRecordValue

        try await fixture.adapter.saveChanges(
            in: [olderSameDeviceRecord], forceSave: true
        )
        try await fixture.adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: "bootstrap-account", epoch: 1
        )
        await fixture.targetRealm.asyncRefresh()

        XCTAssertEqual(Array(object.tags), ["local"])
        XCTAssertEqual(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation,
            generation
        )
        let prepared = try await fixture.adapter.preparedRecordsToUpload(
            limit: 1, restrictedToEntityType: nil
        )
        let upload = try XCTUnwrap(prepared.first)
        XCTAssertEqual(upload.generation, generation)
        XCTAssertEqual(upload.record.recordID.recordName, recordName)
        XCTAssertEqual(upload.record["tags"] as? [String], ["local"])
    }

    @BigSyncBackgroundActor
    func testPreSetupCacheResetPropagatesTrackingRealmOpenFailure()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.fileURL = URL(
            fileURLWithPath: NSTemporaryDirectory(),
            isDirectory: true
        ).appendingPathComponent(
            "missing-read-only-reset-\(identifier).realm"
        )
        persistenceConfiguration.shouldCompactOnLaunch = nil
        persistenceConfiguration.readOnly = true
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "failed-pre-setup-reset-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "failed-pre-setup-reset"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        let targetRealm = try await Realm(
            configuration: targetConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let mutation = BigSyncPendingMutation(
            recordName: "\(BigSyncTrackedObject.className()).durable",
            entityType: BigSyncTrackedObject.className(),
            objectIdentifier: "durable"
        )
        try await targetRealm.asyncWrite {
            targetRealm.add(mutation)
        }
        adapter.cancelSynchronization()

        do {
            try await adapter.resetSyncCaches()
            XCTFail("Expected the tracking Realm open to fail")
        } catch {
            XCTAssertNil(adapter.realmProvider)
        }

        targetRealm.refresh()
        XCTAssertEqual(
            targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: mutation.recordName
            )?.generation,
            mutation.generation
        )
    }

    @BigSyncBackgroundActor
    func testAccountSwitchDefersTrackingResetUntilProvenancePreparation()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "pre-setup-account-reset-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "pre-setup-account-reset-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let persistenceRealm = try await Realm(
            configuration: persistenceConfiguration,
            actor: BigSyncBackgroundActor.shared
        )
        let staleRecordName = "\(BigSyncTrackedObject.className()).account-a"
        try await persistenceRealm.asyncWrite {
            let staleEntity = SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: staleRecordName,
                state: SyncedEntityState.synced.rawValue
            )
            persistenceRealm.add(staleEntity)
            let pendingRelationship = PendingRelationship()
            pendingRelationship.relationshipName = "children"
            pendingRelationship.targetIdentifier = "account-a-target"
            pendingRelationship.forSyncedEntity = staleEntity
            persistenceRealm.add(pendingRelationship)
            persistenceRealm.add(ServerToken())
        }

        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-a"
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(
            database: database,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "pre-setup-account-reset"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        synchronizer.addModelAdapter(adapter)

        try await synchronizer._test_validateSynchronizationAccount()
        XCTAssertNil(adapter.realmProvider)

        database.accountIdentifier = "account-b"
        try await synchronizer._test_validateSynchronizationAccount()
        persistenceRealm.refresh()

        // Validation only publishes a durable server-first recovery request.
        // Tracking must remain untouched until the adapter is set up and can
        // capture provenance transactionally.
        XCTAssertNotNil(
            persistenceRealm.object(
                ofType: SyncedEntity.self,
                forPrimaryKey: staleRecordName
            )
        )
        XCTAssertFalse(persistenceRealm.objects(PendingRelationship.self).isEmpty)
        XCTAssertFalse(persistenceRealm.objects(ServerToken.self).isEmpty)
        XCTAssertNil(adapter.realmProvider)

        let migration = try XCTUnwrap(
            (synchronizer.keyValueStore as? DictionaryKeyValueStore)?
                .propertyListEntries.first(where: {
                    $0.key.contains("ChangeFeedMigration.v3")
                })?.value
        )
        let epoch = try XCTUnwrap(
            (migration["epoch"] as? NSNumber)?.intValue
        )
        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: CloudKitSynchronizer
                .accountScopeIdentifier(for: "account-b"),
            epoch: epoch,
            mode: .serverReconciliation
        )
        persistenceRealm.refresh()

        XCTAssertNil(persistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: staleRecordName
        ))
        XCTAssertTrue(persistenceRealm.objects(PendingRelationship.self).isEmpty)
        XCTAssertTrue(persistenceRealm.objects(ServerToken.self).isEmpty)
        let provenance = try XCTUnwrap(persistenceRealm.object(
            ofType: RebuildProvenance.self,
            forPrimaryKey: staleRecordName
        ))
        XCTAssertTrue(provenance.hadValidServerRecord)
        XCTAssertNotNil(adapter.realmProvider)
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
    func testResetRecoversJournalForwardedBeforeCancellationFromDurableState()
    async throws {
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

        // Cache reset deliberately discards transient observer work. The
        // target-Realm journal remains authoritative and setup must recover it
        // without relying on an account-agnostic in-memory queue.
        XCTAssertFalse(fixture.adapter._test_hasPendingObservedRealmChanges())
        try await fixture.adapter.unsetCancellation()

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
    func testVersionedRecoveryDoesNotRediscoverUnjournaledObjects() async throws {
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

        XCTAssertNil(
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
            2
        )
    }

    @BigSyncBackgroundActor
    func testCompletedResetAtomicallyMarksRecoveryWithoutRediscoveringOnNewSignature()
    async throws {
        let identifier = UUID().uuidString
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier =
            "completed-reset-signature-persistence-\(identifier)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier =
            "completed-reset-signature-target-\(identifier)"
        targetConfiguration.objectTypes = [
            BigSyncTrackedObject.self,
            BigSyncPendingMutation.self,
        ]
        let zoneID = CKRecordZone.ID(
            zoneName: "completed-reset-signature-zone",
            ownerName: CKCurrentUserDefaultName
        )
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: zoneID,
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )

        try await adapter.prepareChangeFeedReset(
            accountScopeIdentifier: "signature-account",
            epoch: 31
        )
        try await adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: "signature-account",
            epoch: 31
        )
        try await adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: "signature-account",
            epoch: 31
        )
        let persistenceRealm = try XCTUnwrap(
            adapter.realmProvider?.persistenceRealm
        )
        let recoveryMarkerPrefix =
            "__BigSyncKitMutationJournalRecovery.v2."

        adapter._testBeforeChangeFeedResetCompletionMarkerWrite = {
            throw NSError(
                domain: "BigSyncKitTests.ChangeFeedCompletion",
                code: 1
            )
        }
        do {
            try await adapter.finishChangeFeedReset(
                accountScopeIdentifier: "signature-account",
                epoch: 31
            )
            XCTFail("Expected the completion transaction to roll back")
        } catch {
            persistenceRealm.refresh()
            let state = try XCTUnwrap(persistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            ))
            XCTAssertTrue(state.isActive)
            XCTAssertNotEqual(state.phase, "complete")
            XCTAssertFalse(
                persistenceRealm.objects(SyncedEntityType.self).contains {
                    $0.entityType.hasPrefix(recoveryMarkerPrefix)
                }
            )
        }

        adapter._testBeforeChangeFeedResetCompletionMarkerWrite = nil
        try await adapter.finishChangeFeedReset(
            accountScopeIdentifier: "signature-account",
            epoch: 31
        )
        persistenceRealm.refresh()
        let completedState = try XCTUnwrap(persistenceRealm.object(
            ofType: RebuildProvenanceState.self,
            forPrimaryKey: RebuildProvenanceState.primaryKeyValue
        ))
        XCTAssertFalse(completedState.isActive)
        XCTAssertEqual(completedState.phase, "complete")
        let completedMarkerIDs = Set(
            persistenceRealm.objects(SyncedEntityType.self)
                .filter { $0.entityType.hasPrefix(recoveryMarkerPrefix) }
                .map(\.entityType)
        )
        XCTAssertEqual(completedMarkerIDs.count, 1)
        XCTAssertTrue(
            persistenceRealm.objects(SyncedEntityType.self)
                .filter { completedMarkerIDs.contains($0.entityType) }
                .allSatisfy { $0.recoveryVersion == 2 }
        )

        let targetRealm = try XCTUnwrap(
            adapter.realmProvider?.targetReaderRealms?.first
        )
        let unjournaled = BigSyncTrackedObject(
            id: "discovered-by-new-signature",
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: Date()
        )
        try await targetRealm.asyncWrite {
            targetRealm.add(unjournaled)
        }

        // The target schema is unchanged, but its configured schema version
        // is part of the recovery signature. This models a later configuration
        // revision without relying on source-shape assertions.
        var revisedTargetConfiguration = targetConfiguration
        revisedTargetConfiguration.schemaVersion += 1
        let replacement = RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [revisedTargetConfiguration],
            excludedClassNames: [],
            recordZoneID: zoneID,
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
        try await replacement._test_setup()
        let reopenedPersistenceRealm = try XCTUnwrap(
            replacement.realmProvider?.persistenceRealm
        )
        XCTAssertNil(reopenedPersistenceRealm.object(
            ofType: SyncedEntity.self,
            forPrimaryKey: BigSyncTrackedObject.className()
                + ".discovered-by-new-signature"
        ))
        let revisedMarkerIDs = Set(
            reopenedPersistenceRealm.objects(SyncedEntityType.self)
                .filter { $0.entityType.hasPrefix(recoveryMarkerPrefix) }
                .map(\.entityType)
        )
        XCTAssertEqual(revisedMarkerIDs.subtracting(completedMarkerIDs).count, 1)
        XCTAssertEqual(
            reopenedPersistenceRealm.object(
                ofType: RebuildProvenanceState.self,
                forPrimaryKey: RebuildProvenanceState.primaryKeyValue
            )?.phase,
            "complete"
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

        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(
            database: database,
            recordZoneID: adapter.recordZoneID
        )
        synchronizer.addModelAdapter(adapter)
        _ = try await synchronizer.synchronize()
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
        XCTAssertNotNil(database.record(for: CKRecord.ID(
            recordName: BigSyncTrackedObject.className() + ".eligible",
            zoneID: adapter.recordZoneID
        )))
        XCTAssertNil(database.record(for: CKRecord.ID(
            recordName: BigSyncTrackedObject.className() + ".cache-only",
            zoneID: adapter.recordZoneID
        )))
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
        identifier: String = UUID().uuidString,
        recordZoneID: CKRecordZone.ID = CKRecordZone.ID(
            zoneName: "BigSyncKit",
            ownerName: CKCurrentUserDefaultName
        ),
        backupDetectionBaseURL: URL? = nil,
        accountIdentifierProvider: @escaping CloudKitSynchronizer.AccountIdentifierProvider = {
            "test-account"
        }
    ) -> CloudKitSynchronizer {
        let synchronizer = CloudKitSynchronizer(
            identifier: identifier,
            containerIdentifier: "iCloud.test",
            database: database,
            recordZoneID: recordZoneID,
            keyValueStore: keyValueStore,
            accountIdentifierProvider: accountIdentifierProvider,
            accountStatusProvider: { .available },
            backupDetectionBaseURL: backupDetectionBaseURL,
            logger: Logger(label: "BigSyncKitTests")
        )
#if DEBUG
        synchronizer._enableDisposableZoneDeletionForTesting()
        synchronizer._allowRecordZoneRebindingForTesting()
#endif
        return synchronizer
    }

    private func makeRecord(type: String, id: String, zoneID: CKRecordZone.ID) -> CKRecord {
        CKRecord(recordType: type, recordID: CKRecord.ID(recordName: "\(type).\(id)", zoneID: zoneID))
    }

    private func makeRelationshipComparisonAdapter() -> RealmSwiftAdapter {
        var persistenceConfiguration = RealmSwiftAdapter
            .defaultPersistenceConfiguration()
        persistenceConfiguration.inMemoryIdentifier = "comparison-persistence-\(UUID().uuidString)"
        var targetConfiguration = Realm.Configuration()
        targetConfiguration.inMemoryIdentifier = "comparison-target-\(UUID().uuidString)"
        targetConfiguration.objectTypes = [
            BigSyncRelationshipChild.self,
            BigSyncRelationshipParent.self,
            BigSyncPendingMutation.self,
        ]
        return RealmSwiftAdapter(
            persistenceRealmConfiguration: persistenceConfiguration,
            targetRealmConfigurations: [targetConfiguration],
            excludedClassNames: [],
            recordZoneID: CKRecordZone.ID(zoneName: "comparison-zone"),
            logger: Logger(label: "BigSyncKitTests"),
            startSetupTask: false
        )
    }

    private func makeRelationshipParentRecord(
        parent: BigSyncRelationshipParent,
        zoneID: CKRecordZone.ID,
        childRecordNames: [String] = []
    ) -> CKRecord {
        let record = CKRecord(
            recordType: BigSyncRelationshipParent.className(),
            recordID: CKRecord.ID(
                recordName: BigSyncRelationshipParent.className() + "." + parent.id,
                zoneID: zoneID
            )
        )
        record["createdAt"] = parent.createdAt as CKRecordValue
        record["modifiedAt"] = parent.modifiedAt as CKRecordValue
        record["isDeleted"] = parent.isDeleted as CKRecordValue
        if !childRecordNames.isEmpty {
            record["children"] = childRecordNames as CKRecordValue
            record["relatedChildren"] = childRecordNames as CKRecordValue
            record["favoriteChild"] = childRecordNames[0] as CKRecordValue
        }
        return record
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
            startSetupTask: false,
            assetDirectoryURL: FileManager.default.temporaryDirectory
                .appendingPathComponent(
                    "BigSyncKitTests-assets-\(identifier)",
                    isDirectory: true
                )
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

    @BigSyncBackgroundActor
    private func makeJournaledZoneFixture(id: String) async throws -> (
        adapter: RealmSwiftAdapter,
        targetRealm: Realm,
        recordName: String,
        generation: String
    ) {
        let fixture = try await makeRealmAdapterFixture()
        let object = BigSyncTrackedObject(
            id: id,
            createdAt: Date(),
            modifiedAt: Date(),
            explicitlyModifiedAt: nil
        )
        object.tags.append("local")
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let recordName = BigSyncTrackedObject.className() + "." + id
        let generation = try XCTUnwrap(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            )?.generation
        )
        return (fixture.adapter, fixture.targetRealm, recordName, generation)
    }

    func testCloudKitLossClassifierRecognizesTopLevelEncryptedDataReset() {
        let zoneID = CKRecordZone.ID(zoneName: "encrypted-reset", ownerName: CKCurrentUserDefaultName)
        let error = CKError(
            .zoneNotFound,
            userInfo: [CKErrorUserDidResetEncryptedDataKey: true]
        )

        let classification = CloudKitLossClassifier.classify(
            error: error,
            defaultZoneID: zoneID
        )

        XCTAssertTrue(classification.hasEncryptedDataReset)
        XCTAssertEqual(
            classification.zoneDispositions[zoneID],
            .encryptedDataReset
        )
    }

    func testCloudKitLossClassifierFindsNestedPartialEncryptedDataResetAndRecordID() {
        let zoneID = CKRecordZone.ID(zoneName: "nested-reset", ownerName: CKCurrentUserDefaultName)
        let recordID = CKRecord.ID(recordName: "record", zoneID: zoneID)
        let reset = CKError(
            .zoneNotFound,
            userInfo: [CKErrorUserDidResetEncryptedDataKey: true]
        )
        let nested = CKError(
            .partialFailure,
            userInfo: [CKPartialErrorsByItemIDKey: [recordID: reset as NSError]]
        )
        let outer = CKError(
            .partialFailure,
            userInfo: [CKPartialErrorsByItemIDKey: [recordID: nested as NSError]]
        )

        let classification = CloudKitLossClassifier.classify(error: outer)

        XCTAssertEqual(classification.zoneDispositions[zoneID], .encryptedDataReset)
        XCTAssertEqual(classification.affectedRecordIDs, [recordID])
    }

    func testCloudKitLossClassifierTerminalDeletionWinsOverEncryptedReset() {
        let zoneID = CKRecordZone.ID(zoneName: "terminal-wins", ownerName: CKCurrentUserDefaultName)
        let resetRecordID = CKRecord.ID(recordName: "reset", zoneID: zoneID)
        let deletedRecordID = CKRecord.ID(recordName: "deleted", zoneID: zoneID)
        let reset = CKError(
            .zoneNotFound,
            userInfo: [CKErrorUserDidResetEncryptedDataKey: true]
        )
        let terminal = CKError(.userDeletedZone)
        let partial = CKError(
            .partialFailure,
            userInfo: [
                CKPartialErrorsByItemIDKey: [
                    resetRecordID: reset as NSError,
                    deletedRecordID: terminal as NSError,
                ],
            ]
        )

        var classification = CloudKitLossClassifier.classify(error: partial)
        classification.merge(
            CloudKitLossClassifier.classify(
                deletions: [CloudKitZoneDeletion(zoneID: zoneID, kind: .purged)]
            )
        )

        XCTAssertEqual(classification.zoneDispositions[zoneID], .terminal(.purged))
        XCTAssertFalse(classification.hasEncryptedDataReset)
    }

    @BigSyncBackgroundActor
    func testEncryptedResetDoesNotPublishTerminalFenceWhenRecoveryEnvelopeIsNotDurable()
    async throws {
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        let zoneID = CKRecordZone.ID(
            zoneName: "undurable-encrypted-reset",
            ownerName: CKCurrentUserDefaultName
        )
        let synchronizer = makeSynchronizer(
            database: database,
            keyValueStore: store,
            recordZoneID: zoneID
        )
        let accountScope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )
        let context = CloudKitSynchronizer.RunContext(
            attemptID: synchronizer.synchronizationAttemptID,
            runID: synchronizer.synchronizationRunID,
            accountIdentifier: database.accountIdentifier,
            accountScopeIdentifier: accountScope
        )
        synchronizer.activeRunContext = context
        store.synchronizesDurably = false

        let error = synchronizer.applyCloudKitLoss(
            .encryptedDataReset,
            zoneID: zoneID,
            context: context
        )

        XCTAssertEqual(
            error as? ChangeFeedMigrationPersistenceError,
            .stateNotDurable
        )
        XCTAssertFalse(synchronizer.configuredZoneIsTerminal(zoneID))
        XCTAssertTrue(store.propertyListEntries.keys.allSatisfy {
            !$0.contains("ChangeFeedMigration.v3")
        })
    }

    @BigSyncBackgroundActor
    func testEncryptedResetRecordMutationFailuresResumeAndReuploadDurableJournal()
    async throws {
        for failureKind in ["top-level", "partial"] {
            let fixture = try await makeJournaledZoneFixture(
                id: "mutation-reset-\(failureKind)"
            )
            let database = FakeCloudKitDatabase()
            database.completesEmptyZoneChangeOperation = true
            let reset = CKError(
                .zoneNotFound,
                userInfo: [CKErrorUserDidResetEncryptedDataKey: true]
            )
            let recordID = CKRecord.ID(
                recordName: fixture.recordName,
                zoneID: fixture.adapter.recordZoneID
            )
            if failureKind == "top-level" {
                database.recordMutationTopLevelErrorOnce = reset
            } else {
                database.partialSaveErrorsOnceByRecordID[recordID] = reset as NSError
            }
            let synchronizer = makeSynchronizer(database: database)
            synchronizer.addModelAdapter(fixture.adapter)

            let result = try await synchronizer.synchronize()

            XCTAssertNotNil(result.receipt, failureKind)
            XCTAssertGreaterThanOrEqual(
                database.modifyRecordsOperationCount,
                2,
                failureKind
            )
            XCTAssertEqual(
                database.record(for: recordID)?["tags"] as? [String],
                ["local"],
                failureKind
            )
            await fixture.targetRealm.asyncRefresh()
            XCTAssertNil(
                fixture.targetRealm.object(
                    ofType: BigSyncPendingMutation.self,
                    forPrimaryKey: fixture.recordName
                ),
                failureKind
            )
            XCTAssertFalse(
                synchronizer.configuredZoneIsTerminal(
                    fixture.adapter.recordZoneID
                ),
                failureKind
            )
        }
    }

    @BigSyncBackgroundActor
    func testEncryptedResetZoneFetchFailureResumesAndReuploadsDurableJournal()
    async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "zone-fetch-encrypted-reset"
        )
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        // The reset signal is one-shot. On the recovery attempt CloudKit may
        // report only an ordinary missing zone; the fenced encrypted reset is
        // what authorizes creating it again.
        database.zoneExists = false
        database.nextRecordZoneChangesError = CKError(
            .zoneNotFound,
            userInfo: [CKErrorUserDidResetEncryptedDataKey: true]
        )
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        let result = try await synchronizer.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertGreaterThanOrEqual(database.recordZoneChangeFetchCount, 1)
        XCTAssertGreaterThanOrEqual(database.modifyRecordsOperationCount, 1)
        XCTAssertFalse(
            synchronizer.configuredZoneIsTerminal(
                fixture.adapter.recordZoneID
            )
        )
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testEncryptedTerminalWithPersistedEnvelopeResumesAfterReopen()
    async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "encrypted-terminal-reopen"
        )
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "encrypted-terminal-reopen-\(UUID().uuidString)"
        let first = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        first.addModelAdapter(fixture.adapter)
        let accountScope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(),
            runID: UUID(),
            accountIdentifier: database.accountIdentifier,
            accountScopeIdentifier: accountScope
        )

        // This is the process-death prefix that used to wedge: recovery
        // intent is durable first, then the terminal marker is observed.
        try first.requestChangeFeedRecovery(
            context: context,
            mode: .encryptedDataReset
        )
        try first.markConfiguredZoneTerminal(
            fixture.adapter.recordZoneID,
            kind: .encryptedDataReset,
            accountScopeIdentifier: accountScope
        )
        let envelopes = store.propertyListEntries.filter {
            $0.key.contains("ChangeFeedMigration.v3")
        }
        XCTAssertEqual(envelopes.count, 1)
        let envelope = try XCTUnwrap(envelopes.values.first)
        XCTAssertEqual(envelope["mode"] as? String, "encryptedDataReset")
        XCTAssertEqual(envelope["phase"] as? String, "requested")
        XCTAssertNotNil(envelope["epoch"])

        // A fresh synchronizer instance models process relaunch. The envelope
        // authorizes the terminal fence, rebuilds tracking from the durable
        // journal, and completes the re-upload rather than throwing terminal.
        let reopened = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        reopened.addModelAdapter(fixture.adapter)
        let result = try await reopened.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertFalse(
            reopened.configuredZoneIsTerminal(fixture.adapter.recordZoneID)
        )
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testOrphanEncryptedTerminalRepairsRecoveryEnvelopeAfterReopen()
    async throws {
        let fixture = try await makeJournaledZoneFixture(
            id: "orphan-encrypted-terminal"
        )
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "orphan-terminal-reopen-\(UUID().uuidString)"
        let first = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        first.addModelAdapter(fixture.adapter)
        let accountScope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )

        // Model the historical/crash prefix in which the terminal marker was
        // durable but recovery intent was not. A fresh process must repair the
        // encrypted-reset envelope rather than failing behind the fence.
        try first.markConfiguredZoneTerminal(
            fixture.adapter.recordZoneID,
            kind: .encryptedDataReset,
            accountScopeIdentifier: accountScope
        )
        XCTAssertTrue(store.propertyListEntries.keys.allSatisfy {
            !$0.contains("ChangeFeedMigration.v3")
        })

        let reopened = makeSynchronizer(
            database: database,
            keyValueStore: store,
            identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        reopened.addModelAdapter(fixture.adapter)
        let result = try await reopened.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertFalse(
            reopened.configuredZoneIsTerminal(fixture.adapter.recordZoneID)
        )
        XCTAssertNil(
            fixture.targetRealm.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: fixture.recordName
            )
        )
    }

    @BigSyncBackgroundActor
    func testEncryptedRecoveryEnvelopeResumesFromEveryPersistedPhaseAfterReopen()
    async throws {
        for phase in ["requested", "prepared", "serverBootstrap", "finishing"] {
            let fixture = try await makeJournaledZoneFixture(
                id: "encrypted-phase-reopen-\(phase)"
            )
            let store = DictionaryKeyValueStore()
            let database = FakeCloudKitDatabase()
            database.completesEmptyZoneChangeOperation = true
            let identifier = "encrypted-phase-reopen-\(phase)-\(UUID().uuidString)"
            let first = makeSynchronizer(
                database: database,
                keyValueStore: store,
                identifier: identifier,
                recordZoneID: fixture.adapter.recordZoneID
            )
            first.addModelAdapter(fixture.adapter)
            let accountScope = CloudKitSynchronizer.accountScopeIdentifier(
                for: database.accountIdentifier
            )
            let context = CloudKitSynchronizer.RunContext(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: accountScope
            )

            try first.requestChangeFeedRecovery(
                context: context,
                mode: .encryptedDataReset
            )
            let entry = try XCTUnwrap(store.propertyListEntries.first(where: {
                $0.key.contains("ChangeFeedMigration.v3")
            }))
            var envelope = entry.value
            let epoch = try XCTUnwrap((envelope["epoch"] as? NSNumber)?.intValue)

            // Make the adapter's durable provenance match the envelope prefix
            // a process could have observed before it died. `finishing` is
            // post-upload, so its adapter has already completed independently.
            switch phase {
            case "prepared":
                try await fixture.adapter.prepareChangeFeedReset(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
            case "serverBootstrap":
                try await fixture.adapter.prepareChangeFeedReset(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
                try await fixture.adapter.beginChangeFeedServerBootstrap(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
            case "finishing":
                try await fixture.adapter.prepareChangeFeedReset(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
                try await fixture.adapter.beginChangeFeedServerBootstrap(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
                try await fixture.adapter.reconcileAfterChangeFeedServerBootstrap(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
                try await fixture.adapter.finishChangeFeedReset(
                    accountScopeIdentifier: accountScope,
                    epoch: epoch,
                    mode: .encryptedDataReset
                )
            default:
                break
            }
            envelope["phase"] = phase
            store.set(value: envelope, forKey: entry.key)
            try first.markConfiguredZoneTerminal(
                fixture.adapter.recordZoneID,
                kind: .encryptedDataReset,
                accountScopeIdentifier: accountScope
            )

            let reopened = makeSynchronizer(
                database: database,
                keyValueStore: store,
                identifier: identifier,
                recordZoneID: fixture.adapter.recordZoneID
            )
            reopened.addModelAdapter(fixture.adapter)
            let result = try await reopened.synchronize()

            XCTAssertNotNil(result.receipt, phase)
            XCTAssertFalse(
                reopened.configuredZoneIsTerminal(fixture.adapter.recordZoneID),
                phase
            )
            let finalEnvelope = try XCTUnwrap(store.object(forKey: entry.key) as? [String: Any])
            XCTAssertEqual(finalEnvelope["phase"] as? String, "completed", phase)
        }
    }

    @BigSyncBackgroundActor
    func testRepeatedEncryptedResetAfterCompletionUsesFreshEpoch()
    async throws {
        let fixture = try await makeJournaledZoneFixture(id: "encrypted-fresh-epoch")
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(database: database, keyValueStore: store)
        synchronizer.addModelAdapter(fixture.adapter)
        let accountScope = CloudKitSynchronizer.accountScopeIdentifier(
            for: database.accountIdentifier
        )

        for resetIndex in 1...2 {
            // An encrypted-data reset removes the server zone. Keep the fake
            // server faithful to that contract so each recovery validates a
            // true empty-zone rebuild rather than conflicting with an
            // impossible record retained across the reset.
            database.removeAllRecords()
            let context = CloudKitSynchronizer.RunContext(
                attemptID: UUID(),
                runID: UUID(),
                accountIdentifier: database.accountIdentifier,
                accountScopeIdentifier: accountScope
            )
            try synchronizer.requestChangeFeedRecovery(
                context: context,
                mode: .encryptedDataReset
            )
            try synchronizer.markConfiguredZoneTerminal(
                fixture.adapter.recordZoneID,
                kind: .encryptedDataReset,
                accountScopeIdentifier: accountScope
            )
            let result = try await synchronizer.synchronize()
            XCTAssertNotNil(result.receipt, "reset \(resetIndex)")
            XCTAssertFalse(
                synchronizer.configuredZoneIsTerminal(fixture.adapter.recordZoneID),
                "reset \(resetIndex)"
            )
        }

        let envelope = try XCTUnwrap(store.propertyListEntries.first(where: {
            $0.key.contains("ChangeFeedMigration.v3")
        })?.value)
        XCTAssertEqual(envelope["phase"] as? String, "completed")
        XCTAssertEqual(
            (envelope["epoch"] as? NSNumber)?.intValue,
            3_000_000_002
        )
    }

    @BigSyncBackgroundActor
    func testServerReconciliationReopensFromPersistedReconciledPhaseWithoutResurrection()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "reconciled-reopen-\(UUID().uuidString)"
        let object = BigSyncTrackedObject(
            id: "previous-server-object",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite { fixture.targetRealm.add(object) }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(),
                identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }
        let first = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        let scope = CloudKitSynchronizer.accountScopeIdentifier(for: database.accountIdentifier)
        let context = CloudKitSynchronizer.RunContext(
            attemptID: UUID(), runID: UUID(),
            accountIdentifier: database.accountIdentifier,
            accountScopeIdentifier: scope
        )
        try first.requestChangeFeedRecovery(context: context)
        let entry = try XCTUnwrap(store.propertyListEntries.first(where: {
            $0.key.contains("ChangeFeedMigration.v3")
        }))
        let epoch = try XCTUnwrap((entry.value["epoch"] as? NSNumber)?.intValue)
        try await fixture.adapter.prepareChangeFeedReset(
            accountScopeIdentifier: scope, epoch: epoch, mode: .serverReconciliation
        )
        try await fixture.adapter.beginChangeFeedServerBootstrap(
            accountScopeIdentifier: scope, epoch: epoch, mode: .serverReconciliation
        )
        try await fixture.adapter.reconcileAfterChangeFeedServerBootstrap(
            accountScopeIdentifier: scope, epoch: epoch, mode: .serverReconciliation
        )
        var reconciledEnvelope = entry.value
        reconciledEnvelope["phase"] = "reconciled"
        store.set(value: reconciledEnvelope, forKey: entry.key)

        let reopened = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID
        )
        reopened.addModelAdapter(fixture.adapter)
        let result = try await reopened.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncTrackedObject.self, forPrimaryKey: object.id
        ))
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        ))
    }

    @BigSyncBackgroundActor
    func testRestoreRecoveryRequestSurvivesReopenBeforeCompletion() async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let identifier = "restore-request-reopen-\(UUID().uuidString)"
        let installedBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let restoredBase = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let object = BigSyncTrackedObject(
            id: "restore-stale-server-object",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite { fixture.targetRealm.add(object) }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(), identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }
        let installed = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID, backupDetectionBaseURL: installedBase
        )
        let installedSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace, sharedBaseURL: installedBase
        )
        let restoredSentinel = BackupDetection.defaultSentinelURL(
            namespace: installed.durableStateNamespace, sharedBaseURL: restoredBase
        )
        let marker = BackupDetection.markerURL(sentinelURL: restoredSentinel)
        try FileManager.default.createDirectory(
            at: marker.deletingLastPathComponent(), withIntermediateDirectories: true
        )
        try FileManager.default.copyItem(
            at: BackupDetection.markerURL(sentinelURL: installedSentinel), to: marker
        )
        let first = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID, backupDetectionBaseURL: restoredBase
        )
        let scope = CloudKitSynchronizer.accountScopeIdentifier(for: database.accountIdentifier)
        try first.requestChangeFeedRecovery(context: .init(
            attemptID: UUID(), runID: UUID(), accountIdentifier: database.accountIdentifier,
            accountScopeIdentifier: scope
        ))
        XCTAssertTrue(BackupDetection.restoreResetIsRequired(
            namespace: first.durableStateNamespace, sharedSentinelBaseURL: restoredBase
        ))

        let reopened = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID, backupDetectionBaseURL: restoredBase
        )
        reopened.addModelAdapter(fixture.adapter)
        let result = try await reopened.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        ))
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(
            namespace: reopened.durableStateNamespace, sharedSentinelBaseURL: restoredBase
        ))
    }

    @BigSyncBackgroundActor
    func testAccountReplacementRecoveryRequestSurvivesReopenBeforeAccountPublication()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let store = DictionaryKeyValueStore()
        let database = FakeCloudKitDatabase()
        database.accountIdentifier = "account-b"
        database.completesEmptyZoneChangeOperation = true
        let identifier = "account-request-reopen-\(UUID().uuidString)"
        let object = BigSyncTrackedObject(
            id: "account-a-stale-server-object",
            createdAt: Date(), modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite { fixture.targetRealm.add(object) }
        try await fixture.persistenceRealm.asyncWrite {
            fixture.persistenceRealm.add(SyncedEntity(
                entityType: BigSyncTrackedObject.className(), identifier: recordName,
                state: SyncedEntityState.synced.rawValue
            ))
        }
        let first = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        store.set(value: "account-a", forKey: first.durableStateKey("CloudKitAccountIdentifier"))
        let scope = CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
        try first.requestChangeFeedRecovery(context: .init(
            attemptID: UUID(), runID: UUID(), accountIdentifier: "account-b",
            accountScopeIdentifier: scope
        ))

        let reopened = makeSynchronizer(
            database: database, keyValueStore: store, identifier: identifier,
            recordZoneID: fixture.adapter.recordZoneID,
            accountIdentifierProvider: { database.accountIdentifier }
        )
        reopened.addModelAdapter(fixture.adapter)
        let result = try await reopened.synchronize()

        XCTAssertNotNil(result.receipt)
        XCTAssertEqual(database.modifyRecordsOperationCount, 0)
        XCTAssertEqual(
            store.object(forKey: reopened.durableStateKey("CloudKitAccountIdentifier")) as? String,
            "account-b"
        )
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        ))
    }

    @BigSyncBackgroundActor
    func testMixedDeletePartialFailureAcknowledgesOnlySuccessfulGeneration()
    async throws {
        let fixture = try await makeRealmAdapterFixture()
        let first = BigSyncTrackedObject(
            id: "delete-partial-success", createdAt: Date(),
            modifiedAt: Date(), explicitlyModifiedAt: Date()
        )
        let second = BigSyncTrackedObject(
            id: "delete-partial-unresolved", createdAt: Date(),
            modifiedAt: Date(), explicitlyModifiedAt: Date()
        )
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(first)
            fixture.targetRealm.add(second)
        }
        try await fixture.adapter._test_enqueueCreatedAndModifiedAndProcess(
            in: fixture.targetRealm
        )
        let initial = try await fixture.adapter.preparedRecordsToUpload(
            limit: 10, restrictedToEntityType: nil
        )
        let initialGenerations = Dictionary(uniqueKeysWithValues: initial.compactMap { prepared in
            prepared.generation.map { generation in
                (prepared.record.recordID.recordName, generation)
            }
        })
        try await fixture.adapter.didUpload(
            savedRecords: initial.map(\.record), matchingGenerations: initialGenerations
        )
        try await fixture.targetRealm.asyncWrite {
            first.isDeleted = true
            first.refreshChangeMetadata(explicitlyModified: true)
            second.isDeleted = true
            second.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await fixture.adapter._test_forwardPendingMutations(
            in: fixture.targetRealm
        )
        let successfulName = BigSyncTrackedObject.className() + "." + first.id
        let unresolvedName = BigSyncTrackedObject.className() + "." + second.id
        let unresolvedGeneration = try XCTUnwrap(
            fixture.persistenceRealm.object(
                ofType: SyncedEntity.self, forPrimaryKey: unresolvedName
            )?.pendingGeneration
        )
        let database = FakeCloudKitDatabase()
        let unresolvedID = CKRecord.ID(
            recordName: unresolvedName, zoneID: fixture.adapter.recordZoneID
        )
        database.partialDeleteErrorsByRecordID[unresolvedID] = CKError(.networkFailure) as NSError
        let synchronizer = makeSynchronizer(database: database)
        synchronizer.addModelAdapter(fixture.adapter)

        do {
            try await synchronizer.synchronizeAdapter(fixture.adapter)
            XCTFail("Expected unresolved per-record delete failure")
        } catch let error as CKError {
            XCTAssertEqual(error.code, .partialFailure)
        }

        let successful = try XCTUnwrap(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: successfulName
        ))
        XCTAssertEqual(successful.entityState, .deletedRemotely)
        XCTAssertNil(successful.pendingGeneration)
        XCTAssertNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: successfulName
        ))
        let unresolved = try XCTUnwrap(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: unresolvedName
        ))
        XCTAssertEqual(unresolved.entityState, .deletedLocally)
        XCTAssertEqual(unresolved.pendingGeneration, unresolvedGeneration)
        XCTAssertNotNil(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: unresolvedName
        ))
    }
}
