from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


q = "Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift"
replace_once(
    q,
    '''/// Bounded recovery identity for one actual CloudKit mutation request.
/// It deliberately contains no field values, assets, retry payloads or
/// acknowledgement authority; the Realm journal remains authoritative.
public struct BigSyncOutboundSubmissionRecoveryDescriptor: Codable, Equatable, Sendable {
    public let version: Int
    public let items: [BigSyncOutboundSubmissionItem]

    public init(items: [BigSyncOutboundSubmissionItem]) {
        version = 1
        self.items = items.sorted(by: Self.canonicalOrder)
    }
''',
    '''public struct BigSyncOutboundSubmissionTransportIdentity: Codable, Equatable, Sendable {
    /// CloudKit echoes this value from the record-zone change stream after
    /// it has received the corresponding modify request.
    public let clientChangeTokenData: Data
    /// Exact CKOperation.ID for the long-lived modify operation. A relaunch
    /// can ask the same CKContainer for this operation and replay callbacks.
    public let longLivedOperationID: String

    public init(clientChangeTokenData: Data, longLivedOperationID: String) {
        self.clientChangeTokenData = clientChangeTokenData
        self.longLivedOperationID = longLivedOperationID
    }
}

/// Bounded recovery identity for one actual CloudKit mutation request.
/// It deliberately contains no field values, assets, retry payloads or
/// acknowledgement authority; the Realm journal remains authoritative.
public struct BigSyncOutboundSubmissionRecoveryDescriptor: Codable, Equatable, Sendable {
    public let version: Int
    public let items: [BigSyncOutboundSubmissionItem]
    public let transportIdentity: BigSyncOutboundSubmissionTransportIdentity?

    public init(
        items: [BigSyncOutboundSubmissionItem],
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity? = nil
    ) {
        version = 1
        self.items = items.sorted(by: Self.canonicalOrder)
        self.transportIdentity = transportIdentity
    }
''',
)
replace_once(
    q,
    '''        let keys = descriptor.items.map {
            RecoveryRecordKey(
                recordName: $0.recordName,
                zoneName: $0.zoneName,
                zoneOwnerName: $0.zoneOwnerName
            )
        }
        guard Set(keys).count == keys.count else { return false }
        return descriptor.items.allSatisfy { item in
''',
    '''        let keys = descriptor.items.map {
            RecoveryRecordKey(
                recordName: $0.recordName,
                zoneName: $0.zoneName,
                zoneOwnerName: $0.zoneOwnerName
            )
        }
        guard Set(keys).count == keys.count,
              descriptor.transportIdentity.map(validTransportIdentity) != false else {
            return false
        }
        return descriptor.items.allSatisfy { item in
''',
)
replace_once(
    q,
    '''    private func validRecoveryComponent(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.count <= 4_096
    }

    private func validBarrier(_ barrier: BigSyncOutboundBarrier) -> Bool {
''',
    '''    private func validRecoveryComponent(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.count <= 4_096
    }

    private func validTransportIdentity(
        _ identity: BigSyncOutboundSubmissionTransportIdentity
    ) -> Bool {
        !identity.clientChangeTokenData.isEmpty
            && identity.clientChangeTokenData.count <= 1_024
            && validRecoveryComponent(identity.longLivedOperationID)
    }

    private func validBarrier(_ barrier: BigSyncOutboundBarrier) -> Bool {
''',
)

Path("Sources/BigSyncKit/QSSynchronizer/CloudKitRecordStore.swift").write_text(r'''import CloudKit
import Foundation

/// Per-record results from one non-atomic CloudKit mutation request.
///
/// BigSync consumes every item result independently so a successful record is
/// acknowledged only for the journal generation that produced it, while a
/// conflict, missing item, or transient failure remains explicit. Each save
/// value and server-conflict record must identify the requested item, including
/// its zone; save values also retain its record type. A result dictionary key
/// alone is not authority to acknowledge a differently identified value.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public struct CloudKitRecordMutationResults {
    public let saveResults: [CKRecord.ID: Result<CKRecord, Error>]
    public let deleteResults: [CKRecord.ID: Result<Void, Error>]

    public init(
        saveResults: [CKRecord.ID: Result<CKRecord, Error>],
        deleteResults: [CKRecord.ID: Result<Void, Error>]
    ) {
        self.saveResults = saveResults
        self.deleteResults = deleteResults
    }
}

/// Structured-concurrency record mutation surface used by BigSyncKit.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
public protocol CloudKitRecordStore: Sendable {
    func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal final class CloudKitPreparedRecordMutation: @unchecked Sendable {
    let operation: CKModifyRecordsOperation
    let transportIdentity: BigSyncOutboundSubmissionTransportIdentity
    let expectedSaveIDs: Set<CKRecord.ID>
    let expectedDeleteIDs: Set<CKRecord.ID>

    init(
        operation: CKModifyRecordsOperation,
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity,
        expectedSaveIDs: Set<CKRecord.ID>,
        expectedDeleteIDs: Set<CKRecord.ID>
    ) {
        self.operation = operation
        self.transportIdentity = transportIdentity
        self.expectedSaveIDs = expectedSaveIDs
        self.expectedDeleteIDs = expectedDeleteIDs
    }
}

/// Internal optional capability. Custom/test stores keep the existing public
/// protocol and are never forced to manufacture CloudKit operation identity.
@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
internal protocol CloudKitRecoverableRecordStore: CloudKitRecordStore {
    func prepareRecoverableModifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) -> CloudKitPreparedRecordMutation

    func executeRecoverableModifyRecords(
        _ prepared: CloudKitPreparedRecordMutation
    ) async throws -> CloudKitRecordMutationResults
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
private final class CloudKitMutationResultCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var saves = [CKRecord.ID: Result<CKRecord, Error>]()
    private var deletes = [CKRecord.ID: Result<Void, Error>]()

    func recordSave(_ recordID: CKRecord.ID, _ result: Result<CKRecord, Error>) {
        lock.lock()
        saves[recordID] = result
        lock.unlock()
    }

    func recordDelete(_ recordID: CKRecord.ID, _ result: Result<Void, Error>) {
        lock.lock()
        deletes[recordID] = result
        lock.unlock()
    }

    func snapshot() -> CloudKitRecordMutationResults {
        lock.lock()
        defer { lock.unlock() }
        return CloudKitRecordMutationResults(
            saveResults: saves,
            deleteResults: deletes
        )
    }
}

@available(iOS 15.0, macOS 12.0, watchOS 8.0, *)
extension DefaultCloudKitDatabaseAdapter: CloudKitRecordStore, CloudKitRecoverableRecordStore {
    private func recordMutationConfiguration(longLived: Bool = false) -> CKOperation.Configuration {
        let configuration = CKOperation.Configuration()
        configuration.timeoutIntervalForRequest = 60
        // Asset-backed uploads observed in the Development sandbox can make
        // steady progress for well over a minute. Bound the whole resource
        // without turning a healthy transfer into a false timeout.
        configuration.timeoutIntervalForResource = 600
        configuration.isLongLived = longLived
        if let container {
            configuration.container = container
        }
        return configuration
    }

    public func modifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) async throws -> CloudKitRecordMutationResults {
        try await database.configuredWith(
            configuration: recordMutationConfiguration()
        ) { database in
            let results = try await database.modifyRecords(
                saving: records,
                deleting: recordIDs,
                savePolicy: savePolicy,
                atomically: atomically
            )
            return CloudKitRecordMutationResults(
                saveResults: results.saveResults,
                deleteResults: results.deleteResults
            )
        }
    }

    internal func prepareRecoverableModifyRecords(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        savePolicy: CKModifyRecordsOperation.RecordSavePolicy,
        atomically: Bool
    ) -> CloudKitPreparedRecordMutation {
        let operation = CKModifyRecordsOperation(
            recordsToSave: records,
            recordIDsToDelete: recordIDs
        )
        let clientChangeTokenData = Data(UUID().uuidString.utf8)
        operation.clientChangeTokenData = clientChangeTokenData
        operation.savePolicy = savePolicy
        operation.isAtomic = atomically
        operation.configuration = recordMutationConfiguration(longLived: true)
        let identity = BigSyncOutboundSubmissionTransportIdentity(
            clientChangeTokenData: clientChangeTokenData,
            longLivedOperationID: operation.operationID
        )
        return CloudKitPreparedRecordMutation(
            operation: operation,
            transportIdentity: identity,
            expectedSaveIDs: Set(records.map(\.recordID)),
            expectedDeleteIDs: Set(recordIDs)
        )
    }

    internal func executeRecoverableModifyRecords(
        _ prepared: CloudKitPreparedRecordMutation
    ) async throws -> CloudKitRecordMutationResults {
        let collector = CloudKitMutationResultCollector()
        return try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<CloudKitRecordMutationResults, Error>) in
            prepared.operation.perRecordSaveBlock = { recordID, result in
                collector.recordSave(recordID, result)
            }
            prepared.operation.perRecordDeleteBlock = { recordID, result in
                collector.recordDelete(recordID, result)
            }
            prepared.operation.modifyRecordsResultBlock = { result in
                let collected = collector.snapshot()
                let hasEveryPerItemResult =
                    Set(collected.saveResults.keys) == prepared.expectedSaveIDs
                    && Set(collected.deleteResults.keys) == prepared.expectedDeleteIDs
                switch result {
                case .success:
                    continuation.resume(returning: collected)
                case .failure(let error):
                    // Non-atomic partial failure still carries authoritative
                    // per-item outcomes. Preserve those instead of collapsing
                    // them into one operation-level error.
                    if hasEveryPerItemResult {
                        continuation.resume(returning: collected)
                    } else {
                        continuation.resume(throwing: error)
                    }
                }
            }
            database.add(prepared.operation)
        }
    }
}
''')

d = "Sources/BigSyncKit/QSSynchronizer/CloudKitDatabase.swift"
replace_once(
    d,
    '''    /// The `CKDatabase` used by this adapter
    public let database: CKDatabase
    
    /// Initialize a `DefaultCloudKitDatabaseAdapter` with a given `CKDatabase`. All calls to the adapter methods will be forwarded to the database instance.
    /// - Parameter database:
    public init(database: CKDatabase) {
        self.database = database
    }
''',
    '''    /// The `CKDatabase` used by this adapter
    public let database: CKDatabase
    /// Owning container when the caller can supply it. Production Realm
    /// setup does so, enabling exact long-lived operation recovery after
    /// process death without changing the public database adapter protocol.
    public let container: CKContainer?
    
    /// Initialize a `DefaultCloudKitDatabaseAdapter` with a given `CKDatabase`.
    public init(database: CKDatabase) {
        self.database = database
        self.container = nil
    }

    public init(database: CKDatabase, container: CKContainer) {
        self.database = database
        self.container = container
    }
''',
)

f = "Sources/BigSyncKit/RealmSwift/QSCloudKitSynchronizer+RealmSwift.swift"
replace_once(
    f,
    '''        let database = DefaultCloudKitDatabaseAdapter(
            database: container.privateCloudDatabase
        )
''',
    '''        let database = DefaultCloudKitDatabaseAdapter(
            database: container.privateCloudDatabase,
            container: container
        )
''',
)

o = "Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+OutboundQuiescence.swift"
replace_once(
    o,
    '''    private func outboundRecoveryDescriptor(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String]
    ) -> BigSyncOutboundSubmissionRecoveryDescriptor {
''',
    '''    private func outboundRecoveryDescriptor(
        saving records: [CKRecord],
        deleting recordIDs: [CKRecord.ID],
        preparedGenerations: [String: String],
        transportIdentity: BigSyncOutboundSubmissionTransportIdentity?
    ) -> BigSyncOutboundSubmissionRecoveryDescriptor {
''',
)
replace_once(
    o,
    '''        return BigSyncOutboundSubmissionRecoveryDescriptor(items: saves + deletes)
    }

    internal func modifyRecordsHoldingOutboundLease(
''',
    '''        return BigSyncOutboundSubmissionRecoveryDescriptor(
            items: saves + deletes,
            transportIdentity: transportIdentity
        )
    }

    internal func modifyRecordsHoldingOutboundLease(
''',
)
replace_once(
    o,
    '''        let recoveryDescriptor = outboundRecoveryDescriptor(
            saving: records,
            deleting: recordIDs,
            preparedGenerations: preparedGenerations
        )
        try await outbound.willSubmitCooperatively(
            recoveryDescriptor: recoveryDescriptor
        )
''',
    '''        let recoverableStore = recordStore as? any CloudKitRecoverableRecordStore
        let preparedTransport = recoverableStore?.prepareRecoverableModifyRecords(
            saving: records,
            deleting: recordIDs,
            savePolicy: .ifServerRecordUnchanged,
            atomically: false
        )
        let recoveryDescriptor = outboundRecoveryDescriptor(
            saving: records,
            deleting: recordIDs,
            preparedGenerations: preparedGenerations,
            transportIdentity: preparedTransport?.transportIdentity
        )
        try await outbound.willSubmitCooperatively(
            recoveryDescriptor: recoveryDescriptor
        )
''',
)
replace_once(
    o,
    '''        let results: CloudKitRecordMutationResults
        do {
            results = try await recordStore.modifyRecords(
                saving: records,
                deleting: recordIDs,
                savePolicy: .ifServerRecordUnchanged,
                atomically: false
            )
        } catch {
''',
    '''        let results: CloudKitRecordMutationResults
        do {
            if let recoverableStore, let preparedTransport {
                results = try await recoverableStore.executeRecoverableModifyRecords(
                    preparedTransport
                )
            } else {
                results = try await recordStore.modifyRecords(
                    saving: records,
                    deleting: recordIDs,
                    savePolicy: .ifServerRecordUnchanged,
                    atomically: false
                )
            }
        } catch {
''',
)

# Normalize cosmetic indentation from the preceding source increment.
r = Path("Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+RecordMutations.swift")
text = r.read_text()
text = text.replace(
    '''            let mutationResults = try await modifyRecordsHoldingOutboundLease(
                outbound,
                    attemptID: attemptID,
                    saving: records,
                    deleting: [],
                    preparedGenerations: generations
                )
''',
    '''            let mutationResults = try await modifyRecordsHoldingOutboundLease(
                outbound,
                attemptID: attemptID,
                saving: records,
                deleting: [],
                preparedGenerations: generations
            )
''',
)
text = text.replace(
    '''            let mutationResults = try await modifyRecordsHoldingOutboundLease(
                outbound,
                    attemptID: attemptID,
                    saving: [],
                    deleting: recordIDs,
                    preparedGenerations: generations
                )
''',
    '''            let mutationResults = try await modifyRecordsHoldingOutboundLease(
                outbound,
                attemptID: attemptID,
                saving: [],
                deleting: recordIDs,
                preparedGenerations: generations
            )
''',
)
r.write_text(text)
