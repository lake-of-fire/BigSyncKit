
    /// Production forwarding is deliberately part of the precondition. The
    /// target journal alone does not exercise the tracking admission gate.
    @BigSyncBackgroundActor
    private func checkForwardedDeletion(incomingKind: Int) async throws {
        let (adapter, realm) = try await fixture()
        let old = try BigSyncLifetimeID.next(after: nil, nonce: lowerNonce)
        let next = try BigSyncLifetimeID.next(after: old, nonce: higherNonce)
        let localEpoch = incomingKind == -1 ? next : old
        try await deliver(record(adapter, epoch: localEpoch, count: 7), to: adapter)
        let value = try object(in: realm)
        try realm.write {
            value.isDeleted = true
            value.refreshChangeMetadata(explicitlyModified: true,
                                        at: Date(timeIntervalSinceReferenceDate: 900))
        }
        let deletionGeneration = try pending(in: realm).generation
        try await adapter.didFinishImport()
        let deletionBatch = try await adapter.prepareDeletionBatch(limit: 10)
        XCTAssertEqual(deletionBatch.recordIDs.count, 1,
                       "The deletion must actually reach tracking before inbound admission")
        let incomingEpoch = incomingKind == 1 ? next : old
        let received = record(adapter, epoch: incomingEpoch, count: 0, time: 20)
        try await deliver(received, to: adapter)
        realm.refresh()
        if incomingKind == 1 {
            XCTAssertFalse(value.isDeleted)
            XCTAssertEqual(value.epoch, next)
            XCTAssertEqual(value.count, 0)
            let successorGeneration = try pending(in: realm).generation
            XCTAssertNotEqual(successorGeneration, deletionGeneration)
            let proof = try XCTUnwrap(realm.object(ofType: BigSyncRecordBaseline.self,
                forPrimaryKey: received.recordID.recordName))
            XCTAssertFalse(proof.isComparisonInvalidated)
            try await adapter.acknowledgeDeletedRecordIDs(
                deletionBatch.recordIDs, from: deletionBatch
            )
            try await adapter.cleanUp()
            realm.refresh()
            XCTAssertFalse(try object(in: realm).isDeleted)
            XCTAssertEqual(try pending(in: realm).generation, successorGeneration)
            let remainingDeletes = try await adapter.prepareDeletionBatch(limit: 10)
            XCTAssertTrue(remainingDeletes.recordIDs.isEmpty)
            let upload = try await adapter.prepareUploadBatch(limit: 10)
            XCTAssertEqual(upload.records.count, 1)
            XCTAssertEqual(upload.records.first?["epoch"] as? String, next)
            XCTAssertEqual(upload.records.first?["isDeleted"] as? Bool, false)
        } else {
            XCTAssertTrue(value.isDeleted)
            XCTAssertEqual(value.epoch, localEpoch)
            XCTAssertEqual(try pending(in: realm).generation, deletionGeneration)
            let stillPending = try await adapter.prepareDeletionBatch(limit: 10)
            XCTAssertEqual(stillPending.recordIDs, deletionBatch.recordIDs)
        }
    }

    @BigSyncBackgroundActor
    func testForwardedPredecessorDeletionAdmitsSuccessorAndRejectsLateDeleteReceipt() async throws {
        try await checkForwardedDeletion(incomingKind: 1)
    }

    @BigSyncBackgroundActor
    func testForwardedNewerDeletionPreservesItsGenerationAgainstOldLiveRecord() async throws {
        try await checkForwardedDeletion(incomingKind: -1)
    }

    @BigSyncBackgroundActor
    func testForwardedSameLifetimeDeletionPreservesItsGeneration() async throws {
        try await checkForwardedDeletion(incomingKind: 0)
    }
