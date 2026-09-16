
    @BigSyncBackgroundActor
    private func acknowledgeAndRequireQuietSecondDrain(_ adapter: RealmSwiftAdapter, realm: Realm) async throws {
        let batch = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(batch.records.count, 1)
        // An injected successful reply exercises local receipt accounting;
        // it is not a simulation of server-assigned CloudKit system fields.
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await adapter.didFinishImport()
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
        let baseline = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
            forPrimaryKey: RebaseIntegrationBoundaryRow.className() + ".article"))
        let revision = baseline.revision
        try await adapter.didFinishImport()
        let secondUpload = try await adapter.prepareUploadBatch(limit: 10)
        let secondDeletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertTrue(secondUpload.records.isEmpty)
        XCTAssertTrue(secondDeletion.recordIDs.isEmpty)
        XCTAssertFalse(try adapter.hasPendingChangesAtTerminalBoundary())
        XCTAssertEqual(baseline.revision, revision)
    }

    @BigSyncBackgroundActor
    func testForwardedSuccessorFinishesWithAQuietSecondDrain() async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        try await deliver(record(adapter, epoch: old, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let deletion = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.count, 1)
        try await deliver(record(adapter, epoch: next), to: adapter)
        try await adapter.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await acknowledgeAndRequireQuietSecondDrain(adapter, realm: realm)
        XCTAssertFalse(value.isDeleted)
        XCTAssertEqual(value.epoch, next)
    }

    @BigSyncBackgroundActor
    private func openPersistedBoundaryAdapter(
        target: Realm.Configuration, tracking: Realm.Configuration
    ) async throws -> RealmSwiftAdapter {
        let adapter = RealmSwiftAdapter(
            persistenceRealmConfiguration: tracking, targetRealmConfigurations: [target],
            excludedClassNames: [], recordZoneID: .init(zoneName: "integration-boundary"),
            logger: Logger(label: "PersistedBoundaryTests"), startSetupTask: false
        )
        adapter.mergePolicy = .custom
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "account",
            replicaBindingGenerationIdentifier: "binding")
        try await adapter.activateTransportNamespace(containerIdentifier: "iCloud.test.integration-boundary",
            databaseScope: .private)
        // Do not call resetSyncCaches: that would erase the tracking state
        // whose persistence/reopen behavior this test is meant to exercise.
        try await adapter.ensureSetup()
        adapter.invalidateTokens()
        return adapter
    }

    @BigSyncBackgroundActor
    private func persistForwardedDeletion(
        target: Realm.Configuration, tracking: Realm.Configuration, epoch: String
    ) async throws -> String {
        let adapter = try await openPersistedBoundaryAdapter(target: target, tracking: tracking)
        let realm = try XCTUnwrap(adapter.realmProvider?.targetReaderRealms?.first)
        try await deliver(record(adapter, epoch: epoch, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let batch = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(batch.recordIDs.count, 1)
        let generation = try pending(in: realm).generation
        adapter.cancelSynchronization()
        await adapter.waitForCancellation()
        adapter.invalidateTokens()
        return generation
    }

    @BigSyncBackgroundActor
    func testFileBackedTrackingReopenStillAdmitsOrderedSuccessor() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("bigsync-forwarded-reopen-" + UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        var target = Realm.Configuration()
        target.fileURL = directory.appendingPathComponent("target.realm")
        target.objectTypes = [RebaseIntegrationBoundaryRow.self, BigSyncPendingMutation.self]
        BigSyncMutationPolicy.enableRecordRebasing(in: &target)
        BigSyncMutationPolicy(excludedClassNames: []).install(configurations: [target],
            mutationJournalIdentityProvider: {
                .init(installationIdentifier: "local", replicaBindingGenerationIdentifier: "binding")
            })
        var tracking = RealmSwiftAdapter.defaultPersistenceConfiguration()
        tracking.fileURL = directory.appendingPathComponent("tracking.realm")
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        let oldGeneration = try await persistForwardedDeletion(target: target, tracking: tracking, epoch: old)
        let reopened = try await openPersistedBoundaryAdapter(target: target, tracking: tracking)
        let realm = try XCTUnwrap(reopened.realmProvider?.targetReaderRealms?.first)
        XCTAssertEqual(try pending(in: realm).generation, oldGeneration)
        let deletion = try await reopened.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletion.recordIDs.count, 1)
        try await deliver(record(reopened, epoch: next), to: reopened)
        realm.refresh()
        XCTAssertFalse(try object(in: realm).isDeleted)
        XCTAssertEqual(try object(in: realm).epoch, next)
        XCTAssertNotEqual(try pending(in: realm).generation, oldGeneration)
        try await reopened.acknowledgeDeletedRecordIDs(deletion.recordIDs, from: deletion)
        try await reopened.cleanUp()
        try await acknowledgeAndRequireQuietSecondDrain(reopened, realm: realm)
        reopened.cancelSynchronization()
        await reopened.waitForCancellation()
        reopened.invalidateTokens()
    }
