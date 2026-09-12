from pathlib import Path

def replace(path, old, new):
    path = Path(path)
    value = path.read_text()
    assert value.count(old) == 1, (str(path), value.count(old))
    path.write_text(value.replace(old, new))

adapter = 'Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift'
replace(adapter, '    private var cancelSync: Bool = false\n', '''    private var cancelSync: Bool = false
    // New runs may prepare provenance after cancellation without enabling
    // normal setup or asynchronous journal forwarding ahead of that barrier.
    private var isPreparingFencedMigration = false
''')
replace(adapter, '''    @BigSyncBackgroundActor
    public func unsetCancellation() async throws {
        //        debugPrint("# unset cancel")
        cancelSync = false''', '''    /// The owning synchronizer has awaited callback/adapter quiescence and
    /// revalidated its run before calling this synchronous preparation hook.
    /// Unlike unsetCancellation(), it must not open the target Realm, restart
    /// setup, or drain observed journals before migration provenance exists.
    @BigSyncBackgroundActor
    func prepareForFencedMigrationAfterCancellation() throws {
        try Task.checkCancellation()
        isPreparingFencedMigration = true
        cancelSync = false
    }

    @BigSyncBackgroundActor
    public func unsetCancellation() async throws {
        // Normal setup and observers become eligible only after migration
        // preparation has installed the run's recovery/provenance boundary.
        isPreparingFencedMigration = false
        cancelSync = false''')
replace(adapter, '''        guard !cancelSync, observedRealmChangesTask == nil else { return }''', '''        guard !cancelSync, !isPreparingFencedMigration,
              observedRealmChangesTask == nil else { return }''')
replace(adapter, '''                if !cancelSync,
                   !observedJournalRecordNames.isEmpty {''', '''                if !cancelSync, !isPreparingFencedMigration,
                   !observedJournalRecordNames.isEmpty {''')
replace('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift', '''                try await beginChangeFeedMigrationIfNeeded(context: context)''', '''                // A cancelled partial/download-only migration must be able
                // to resume its persisted phase. Reset the Realm adapter's
                // cancellation gate without starting normal discovery or
                // journal observation before provenance preparation.
                for adapter in modelAdapters {
                    await adapter.waitForCancellation()
                    try checkRunContext(context)
                    if let realmAdapter = adapter as? RealmSwiftAdapter {
                        try realmAdapter.prepareForFencedMigrationAfterCancellation()
                    }
                }
                try await beginChangeFeedMigrationIfNeeded(context: context)''')

tests = 'Tests/BigSyncKitTests/BigSyncKitTests.swift'
replace(tests, '''        first.cancel()
        second.cancel()
        await synchronizer.cancelSynchronizationAndWait()
        await first.value''', '''        if synchronizer.syncing || synchronizer.synchronizationDrainIsActive {
            first.cancel()
            second.cancel()
            await synchronizer.cancelSynchronizationAndWait()
        }
        await first.value''')
# The low-level transport test must model an actual active owning drain. With
# syncing=false, its concurrent real journal write legitimately starts another
# synchronization via the adapter delegate and fences the tested operation.
replace(tests, '''        var observedError: Error?
        do {
            try await sync.synchronizeAdapter(adapter)''', '''        sync.syncing = true
        sync.synchronizationDrainIsActive = true
        sync.activeRunContext = reviewContext(sync)
        var observedError: Error?
        do {
            try await sync.synchronizeAdapter(adapter)''')
replace(tests, '''        let error = try XCTUnwrap(observedError)
        XCTAssertFalse(sync.shouldRetryUpload(for: error as NSError))''', '''        let error = try XCTUnwrap(observedError)
        XCTAssertEqual((error as? CKError)?.code, .partialFailure)
        XCTAssertFalse(sync.shouldRetryUpload(for: error as NSError))''')

extra = '''
    @BigSyncBackgroundActor
    func testClosureMigrationPreparationKeepsObservedJournalsQueued() async throws {
        let fixture = try await reviewJournalBatch(count: 1)
        let recordName = BigSyncTrackedObject.className() + "." + fixture.2[0].id
        fixture.0.cancelSynchronization()
        await fixture.0.waitForCancellation()
        try fixture.0.prepareForFencedMigrationAfterCancellation()
        fixture.0._test_enqueueObservedJournalRecordNames([recordName])
        fixture.0._test_startObservedRealmChangesTaskIfNeeded()
        for _ in 0..<20 { await Task.yield() }
        XCTAssertTrue(fixture.0._test_hasPendingObservedRealmChanges(),
                      "Migration preparation must not restart ordinary journal observation")
        fixture.1.refresh()
        XCTAssertEqual(fixture.1.objects(BigSyncPendingMutation.self).count, 1)
        try await fixture.0.unsetCancellation()
        try await fixture.0.didFinishImport()
        fixture.0.cancelSynchronization()
        await fixture.0.waitForCancellation()
    }

    @BigSyncBackgroundActor
    func testClosureExplicitDownloadCancellationResumesUnfinishedMigration() async throws {
        let fixture = try await reviewJournalBatch(count: 1)
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(database: database, recordZoneID: fixture.0.recordZoneID)
        synchronizer.addModelAdapter(fixture.0)
        synchronizer.syncMode = .downloadOnly
        let inbound = try await synchronizer.synchronize()
        XCTAssertEqual(inbound.completionScope, .downloadOnly)
        XCTAssertNil(inbound.receipt)
        await synchronizer.cancelSynchronizationAndWait()
        synchronizer.syncMode = .sync
        let full = try await synchronizer.synchronize()
        XCTAssertNotNil(full.receipt)
        fixture.1.refresh()
        XCTAssertTrue(fixture.1.objects(BigSyncPendingMutation.self).isEmpty)
        await synchronizer.cancelSynchronizationAndWait()
    }
'''
replace(tests, 'final class BigSyncKitTests: XCTestCase {\n',
        'final class BigSyncKitTests: XCTestCase {\n' + extra)
print('Applied fenced migration preparation, real recovery controls and active transport fixture')
