from pathlib import Path

def replace(path, old, new):
    path = Path(path)
    value = path.read_text()
    assert value.count(old) == 1, (str(path), value.count(old))
    path.write_text(value.replace(old, new))

replace('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift',
'''    public func didFinishImport() async throws {
        try await ensureSetup()''',
'''    public func didFinishImport() async throws {
        // Failure cleanup can arrive before migration has installed recovery
        // provenance. Keep the target journal untouched at this boundary;
        // normal setup/forwarding resumes through unsetCancellation only
        // after the owning run completes preparation successfully.
        guard !isPreparingFencedMigration else { return }
        try await ensureSetup()''')

test = '''
    @BigSyncBackgroundActor
    func testClosureMigrationFailureCannotForwardJournalBeforePreparation() async throws {
        let fixture = try await makeRealmAdapterFixture()
        fixture.adapter.cancelSynchronization()
        await fixture.adapter.waitForCancellation()
        let object = BigSyncTrackedObject(
            id: "closure-failed-preparation", createdAt: Date(),
            modifiedAt: Date(), explicitlyModifiedAt: nil
        )
        let recordName = BigSyncTrackedObject.className() + "." + object.id
        try await fixture.targetRealm.asyncWrite {
            fixture.targetRealm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let generation = try XCTUnwrap(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName
        )?.generation)
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        ))
        let database = FakeCloudKitDatabase()
        database.completesEmptyZoneChangeOperation = true
        let synchronizer = makeSynchronizer(database: database,
                                             recordZoneID: fixture.adapter.recordZoneID)
        synchronizer.addModelAdapter(fixture.adapter)
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        synchronizer.activeRunContext = reviewContext(synchronizer)
        try fixture.adapter.prepareForFencedMigrationAfterCancellation()
        // Exercise the real failure handler's explicit flush, not just the
        // debounced observer. Preparation itself has not committed provenance.
        await synchronizer.failSynchronization(error: TestSynchronizationError.initialSetupFailed)
        fixture.persistenceRealm.refresh()
        fixture.targetRealm.refresh()
        XCTAssertNil(fixture.persistenceRealm.object(
            ofType: SyncedEntity.self, forPrimaryKey: recordName
        ), "Failure cleanup must not forward a preparation-phase journal")
        XCTAssertEqual(fixture.targetRealm.object(
            ofType: BigSyncPendingMutation.self, forPrimaryKey: recordName
        )?.generation, generation)
        await synchronizer.cancelSynchronizationAndWait()
        let recovered = try await synchronizer.synchronize()
        XCTAssertNotNil(recovered.receipt)
        fixture.targetRealm.refresh()
        XCTAssertTrue(fixture.targetRealm.objects(BigSyncPendingMutation.self).isEmpty)
        await synchronizer.cancelSynchronizationAndWait()
    }
'''
replace('Tests/BigSyncKitTests/BigSyncKitTests.swift',
        'final class BigSyncKitTests: XCTestCase {\n',
        'final class BigSyncKitTests: XCTestCase {\n' + test)

script = Path('.github/qualify_hotfix_closure.py')
value = script.read_text()
anchor = "full = run('full', ['swift', 'test'], env=dict(os.environ, BIGSYNC_RUN_MUTATION_BENCHMARK='1'))"
assert value.count(anchor) == 1
negative = '''    source = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
    original = source.read_text()
    start = original.index('    public func didFinishImport() async throws {')
    end = original.index('    func hasPendingChangesAtTerminalBoundary()', start)
    segment = original[start:end]
    guard = '        guard !isPreparingFencedMigration else { return }\\n'
    assert segment.count(guard) == 1
    try:
        source.write_text(original[:start] + segment.replace(guard, '') + original[end:])
        run('negative-preparation-flush', ['swift', 'test', '--filter',
            'testClosureMigrationFailureCannotForwardJournalBeforePreparation'],
            negative_marker='Failure cleanup must not forward a preparation-phase journal')
    finally:
        source.write_text(original)
'''
value = value.replace(anchor, negative + anchor)
assert value.count('len(results) == 14') == 1
script.write_text(value.replace('len(results) == 14', 'len(results) == 15'))
print('Gated failure-path journal forwarding during fenced migration preparation')
