    var nextDatabaseChangesError: Error?
    var zoneChangeHandler: (@Sendable () async throws -> CloudKitRecordZoneChangePage)?
    private(set) var subscriptionFetchCount = 0

    func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        zoneChangeFetchCount += 1
        if let zoneChangeHandler { return try await zoneChangeHandler() }
        return .init(
            cursor: RecordZoneChangeCursor(serializedData: Data("account-fencing-zone".utf8)),
            records: [],
            deletedRecordIDs: [],
            moreComing: false
        )
    }

    func saveChanges(
        in records: [CKRecord],
        forceSave: Bool
    ) async throws -> [InboundLiveResult] {
        if let fetchedRecordFailure { throw fetchedRecordFailure }
        return records.enumerated().map {
            .init(
                event: .init(
                    ordinal: $0.offset,
                    entityType: $0.element.recordType,
                    recordID: $0.element.recordID
                ),
                disposition: .applied
            )
        }
    }

    var cursorLoadingProvider: (@Sendable () async -> RecordZoneChangeCursor?)?
    var fetchedRecordFailure: Error?
    var serverChangeToken: RecordZoneChangeCursor? { get async { await cursorLoadingProvider?() } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}

        XCTAssertEqual(reasonsAfterRetry, [.accountChanged])
        let newLease = try XCTUnwrap(synchronizer.accountScopeLease())
        XCTAssertGreaterThan(newLease.invalidationGeneration, oldLease.invalidationGeneration)
        XCTAssertThrowsError(try synchronizer.validateAccountScopeLease(oldLease))
        XCTAssertGreaterThan(transport.operationCount, 0)
    }
}

// Cursor staging uses the existing account-fencing test fixture and actor.
// No CloudKit request, target mutation, or durable cursor write is needed.
extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    private func cursorLoadingFixture() -> (
        sync: CloudKitSynchronizer,
        adapter: AccountFencingModelAdapter,
        transport: AccountFencingTransport
    ) {
        let transport = AccountFencingTransport()
        let sync = makeSynchronizer(
            transport: transport,
            recordZoneID: makeZoneID(),
            accountIdentifierProvider: {
                XCTFail("Loading persisted cursors must not add an account request")
                return "account-a"
            }
        )
        let adapter = AccountFencingModelAdapter(zoneID: sync.recordZoneID)
        sync.addModelAdapter(adapter)
        sync.activeRunContext = .init(
            attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID,
            accountIdentifier: "account-a",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        sync.activeZoneTokens[sync.recordZoneID] = cursorLoadingToken("prior")
        addTeardownBlock { @BigSyncBackgroundActor in
            adapter.cursorLoadingProvider = nil
            await sync.cancelSynchronizationAndWait()
        }
        return (sync, adapter, transport)
    }

    private func cursorLoadingToken(_ value: String) -> RecordZoneChangeCursor {
        RecordZoneChangeCursor(serializedData: Data(value.utf8))
    }

    @BigSyncBackgroundActor
    private func assertCursorLoadCancelled(
        _ result: Result<[CKRecordZone.ID], Error>,
        file: StaticString = #filePath, line: UInt = #line
    ) {
        guard case .failure(let error) = result else {
            return XCTFail("Retired cursor load succeeded", file: file, line: line)
        }
        XCTAssertTrue(error is CancellationError, "Unexpected error: \(error)", file: file, line: line)
    }

    @BigSyncBackgroundActor
    func testCursorLoadStagesMapUntilRegisteredReadCompletes() async throws {
        let (sync, adapter, transport) = cursorLoadingFixture()
        let entered = expectation(description: "persisted cursor read held")
        let release = ClosureRestorationGate()
        let loaded = cursorLoadingToken("loaded")
        adapter.cursorLoadingProvider = { entered.fulfill(); await release.wait(); return loaded }
        let request = Task { @BigSyncBackgroundActor in
            try await sync.loadTokens(for: [sync.recordZoneID])
        }
        addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData,
                       Data("prior".utf8), "Do not expose partially rebuilt state")
        await release.open()
        let zones = try await request.value
        XCTAssertEqual(zones, [sync.recordZoneID])
        XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, loaded.serializedData)
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testCursorLoadCannotAdoptANewerCallerAttemptAtEntry() async {
        let (sync, adapter, transport) = cursorLoadingFixture()
        adapter.cursorLoadingProvider = { XCTFail("Retired caller read the adapter"); return nil }
        do {
            _ = try await sync.loadTokens(for: [sync.recordZoneID], attemptID: UUID())
            XCTFail("Explicit old caller adopted current attempt")
        } catch { XCTAssertTrue(error is CancellationError) }
        XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("prior".utf8))
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testPrecancelledCursorLoadLeavesStateAndAdapterUnread() async {
        for empty in [false, true] {
            let (sync, adapter, transport) = cursorLoadingFixture()
            adapter.cursorLoadingProvider = { XCTFail("Cancelled loader invoked adapter"); return nil }
            let request = Task { @BigSyncBackgroundActor in
                withUnsafeCurrentTask { $0?.cancel() }
                return try await sync.loadTokens(for: empty ? [] : [sync.recordZoneID])
            }
            assertCursorLoadCancelled(await request.result)
            XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("prior".utf8))
            XCTAssertEqual(transport.operationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testLateCursorAndNilCannotOverwriteSuccessorPageState() async {
        for returnsNil in [false, true] {
            let (sync, adapter, transport) = cursorLoadingFixture()
            let entered = expectation(description: "old cursor held")
            let release = ClosureRestorationGate()
            let oldToken = returnsNil ? nil : cursorLoadingToken("old")
            adapter.cursorLoadingProvider = { entered.fulfill(); await release.wait(); return oldToken }
            let request = Task { @BigSyncBackgroundActor in
                try await sync.loadTokens(for: [sync.recordZoneID])
            }
            addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
            await fulfillment(of: [entered], timeout: 2)
            // A real public cancellation rotates the attempt. The marker below
            // represents page state already published by a later owner, not a
            // claim that this fixture executed another full CloudKit drain.
            sync.cancelSynchronization()
            sync.activeZoneTokens[sync.recordZoneID] = cursorLoadingToken("successor")
            await release.open()
            assertCursorLoadCancelled(await request.result)
            XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("successor".utf8))
            XCTAssertEqual(transport.operationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testCursorLoadRetainsExactOriginalRunAndOptionalContext() async {
        for replacement in 0..<3 {
            let (sync, adapter, transport) = cursorLoadingFixture()
            let entered = expectation(description: "read before run/context replacement")
            let release = ClosureRestorationGate()
            let token = cursorLoadingToken("stale")
            adapter.cursorLoadingProvider = { entered.fulfill(); await release.wait(); return token }
            let request = Task { @BigSyncBackgroundActor in
                try await sync.loadTokens(for: [sync.recordZoneID])
            }
            addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
            await fulfillment(of: [entered], timeout: 2)
            switch replacement {
            case 0: sync.synchronizationRunID = UUID()
            case 1: sync.activeRunContext = nil
            default:
                sync.activeRunContext = .init(
                    attemptID: sync.synchronizationAttemptID, runID: sync.synchronizationRunID,
                    accountIdentifier: "account-b",
                    accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "account-b")
                )
            }
            await release.open()
            assertCursorLoadCancelled(await request.result)
            XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("prior".utf8))
            XCTAssertEqual(transport.operationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testCursorLoadRejectsRemovedOrReplacedRegisteredAdapter() async {
        for removesAdapter in [false, true] {
            let (sync, adapter, transport) = cursorLoadingFixture()
            let entered = expectation(description: "read before adapter replacement")
            let release = ClosureRestorationGate()
            let token = cursorLoadingToken("stale")
            adapter.cursorLoadingProvider = { entered.fulfill(); await release.wait(); return token }
            let request = Task { @BigSyncBackgroundActor in
                try await sync.loadTokens(for: [sync.recordZoneID])
            }
            addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
            await fulfillment(of: [entered], timeout: 2)
            if removesAdapter {
                sync.modelAdapterDictionary.removeValue(forKey: sync.recordZoneID)
            } else {
                let replacement = AccountFencingModelAdapter(zoneID: sync.recordZoneID)
                replacement.cursorLoadingProvider = { XCTFail("Old loader adopted replacement adapter"); return nil }
                sync.modelAdapterDictionary[sync.recordZoneID] = replacement
            }
            await release.open()
            assertCursorLoadCancelled(await request.result)
            XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("prior".utf8))
            XCTAssertEqual(transport.operationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testCancelledCursorLoadDoesNotPoisonIndependentLaterLoad() async throws {
        let (sync, adapter, transport) = cursorLoadingFixture()
        let entered = expectation(description: "cancelled cursor read held")
        let release = ClosureRestorationGate()
        adapter.cursorLoadingProvider = { entered.fulfill(); await release.wait(); return nil }
        let request = Task { @BigSyncBackgroundActor in
            try await sync.loadTokens(for: [sync.recordZoneID])
        }
        addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
        await fulfillment(of: [entered], timeout: 2)
        request.cancel()
        await release.open()
        assertCursorLoadCancelled(await request.result)
        XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, Data("prior".utf8))
        let current = cursorLoadingToken("fresh")
        adapter.cursorLoadingProvider = { current }
        let zones = try await sync.loadTokens(for: [sync.recordZoneID])
        XCTAssertEqual(zones, [sync.recordZoneID])
        XCTAssertEqual(sync.activeZoneTokens[sync.recordZoneID]?.serializedData, current.serializedData)
        XCTAssertEqual(transport.operationCount, 0)
    }

    @BigSyncBackgroundActor
    func testCurrentNilUnknownAndEmptyCursorLoadsPreserveExistingContract() async throws {
        let (sync, adapter, transport) = cursorLoadingFixture()
        let unrelated = makeZoneID()
        let zones = try await sync.loadTokens(for: [unrelated, sync.recordZoneID])
        XCTAssertEqual(zones, [sync.recordZoneID], "Registered nil cursor still requires bootstrap fetch")
        XCTAssertTrue(sync.activeZoneTokens.isEmpty)
        sync.activeZoneTokens[sync.recordZoneID] = cursorLoadingToken("prior")
        adapter.cursorLoadingProvider = { XCTFail("Empty load must not read adapter"); return nil }
        let empty = try await sync.loadTokens(for: [])
        XCTAssertTrue(empty.isEmpty)
        XCTAssertTrue(sync.activeZoneTokens.isEmpty)
        XCTAssertEqual(transport.operationCount, 0)
    }
}

// The old network reply remains held while the real processor records a new
// run's adapter failure. Old unwinding must not erase that newer error.
extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    private func cursorCleanupFixture() async -> (
        CloudKitSynchronizer, AccountFencingModelAdapter, AccountFencingTransport
    ) {
        let transport = AccountFencingTransport()
        let sync = makeSynchronizer(transport: transport, recordZoneID: makeZoneID())
        let adapter = AccountFencingModelAdapter(zoneID: sync.recordZoneID)
        sync.addModelAdapter(adapter)
        let runID = await sync.changeRequestProcessor.beginRun()
        sync.synchronizationRunID = runID
        sync.activeRunContext = .init(
            attemptID: sync.synchronizationAttemptID, runID: runID,
            accountIdentifier: "account-a",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        addTeardownBlock { @BigSyncBackgroundActor in
            adapter.cursorLoadingProvider = nil
            adapter.fetchedRecordFailure = nil
            transport.zoneChangeHandler = nil
            await sync.cancelSynchronizationAndWait()
        }
        return (sync, adapter, transport)
    }

    @BigSyncBackgroundActor
    private func recordCursorCleanupFailure(
        _ failure: NSError, sync: CloudKitSynchronizer, adapter: AccountFencingModelAdapter
    ) async throws {
        adapter.fetchedRecordFailure = failure
        let record = CKRecord(recordType: "AccountFencingObject",
                              recordID: CKRecord.ID(recordName: "cursor-cleanup", zoneID: sync.recordZoneID))
        sync.changeRequestProcessor.addFetchedChangeRequest(ChangeRequest(
            downloadedRecord: record, deletedRecordID: nil, adapter: adapter,
            runID: sync.synchronizationRunID
        ))
        _ = try await sync.changeRequestProcessor.finishProcessing(for: adapter)
        let recorded = sync.changeRequestProcessor.getErrors()
        XCTAssertEqual(recorded.count, 1)
        XCTAssertTrue((recorded.first as NSError?) === failure)
    }

    @BigSyncBackgroundActor
    func testRetiredZoneFetchCannotClearSuccessorProcessingErrors() async throws {
        for replaceAttempt in [false, true] {
            let (sync, adapter, transport) = await cursorCleanupFixture()
            let entered = expectation(description: "old zone fetch held")
            let release = ClosureRestorationGate()
            let oldError = NSError(domain: "OldHeldZoneFetch", code: 1)
            transport.zoneChangeHandler = {
                entered.fulfill(); await release.wait(); throw oldError
            }
            let request = Task { @BigSyncBackgroundActor in
                try await sync.fetchZoneChanges([sync.recordZoneID])
            }
            addTeardownBlock { request.cancel(); await release.open(); _ = await request.result }
            await fulfillment(of: [entered], timeout: 2)
            if replaceAttempt { sync.synchronizationAttemptID = UUID() }
            let newRun = await sync.changeRequestProcessor.beginRun()
            sync.synchronizationRunID = newRun
            sync.activeRunContext = .init(
                attemptID: sync.synchronizationAttemptID, runID: newRun,
                accountIdentifier: "account-a",
                accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
            )
            let newer = NSError(domain: "SuccessorImportFailure", code: 2)
            try await recordCursorCleanupFailure(newer, sync: sync, adapter: adapter)
            await release.open()
            // The old run must fail; it must not settle or erase the current
            // processor's error on its way out. The exact outer error is not
            // the contract under test (network error vs cancellation fence).
            do { try await request.value; XCTFail("Obsolete fetch succeeded") } catch { }
            let errors = sync.changeRequestProcessor.getErrors()
            XCTAssertEqual(errors.count, 1)
            XCTAssertTrue((errors.first as NSError?) === newer)
            XCTAssertEqual(transport.recordMutationCount, 0)
        }
    }

    @BigSyncBackgroundActor
    func testCurrentZoneFetchStillRetiresItsOwnProcessingErrors() async throws {
        let (sync, adapter, transport) = await cursorCleanupFixture()
        let processingError = NSError(domain: "CurrentImportFailure", code: 3)
        try await recordCursorCleanupFailure(processingError, sync: sync, adapter: adapter)
        let networkError = NSError(domain: "CurrentZoneFetch", code: 4)
        transport.zoneChangeHandler = { throw networkError }
        do { try await sync.fetchZoneChanges([sync.recordZoneID]); XCTFail("Expected fetch failure") }
        catch { XCTAssertTrue((error as NSError) === networkError) }
        XCTAssertTrue(sync.changeRequestProcessor.getErrors().isEmpty)
        XCTAssertEqual(transport.recordMutationCount, 0)
    }
}
