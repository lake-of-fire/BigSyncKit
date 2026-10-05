extension SyncRetainedRecordContractTests {
    private enum RetainedAcknowledgementRetryGuard { case wrongTag, wrongContext, newerGeneration }

    @BigSyncBackgroundActor
    private func exerciseRetainedAcknowledgementRefresh(
        mode: CleanupRefreshAuthority.Mode,
        retryGuard: RetainedAcknowledgementRetryGuard? = nil
    ) async throws {
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let saved = try XCTUnwrap(prepared.first?.record)
        let results = try await adapter.deleteRecords(with: [saved.recordID])
        guard case .quarantined(let lineage) = try XCTUnwrap(results.first).disposition else {
            return XCTFail("A real retained physical deletion must create quarantine evidence")
        }

        adapter._testAfterAcceptedRetainedDeletionTrackingAdmission = {
            XCTAssertTrue(tracking.isInWriteTransaction)
            XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
            guard authority.armOnce() else { return }
            // The actual acknowledgement and initial cleanup candidate
            // selection have finished. Commit only a local metadata signal;
            // the tracking transaction then refreshes the real target Realm.
            try writerQueue.sync {
                let writer = try Realm(configuration: configuration, queue: writerQueue)
                try writer.write {

        XCTAssertEqual(request.isCancelled, mode == .cancelTask)
        XCTAssertTrue(authority.didObserve, "Must deliver a real refresh after terminal acknowledgement")
        XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
        XCTAssertNil(tracking.object(ofType: SyncedEntity.self,
            forPrimaryKey: saved.recordID.recordName)?.pendingGeneration)
        XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
        if mode != .live {
            XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineage))

            if retryGuard == .newerGeneration {
                let current = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
                try await adapter.didUpload(savedRecords: current.map(\.record), matchingPreparedUploads: current)
            } else {
                // Replaying the same public acknowledgement retains the accepted
                // receipt and gives the cleanup a fresh authority generation.
                try await adapter.didUpload(savedRecords: [saved], matchingPreparedUploads: prepared)
            }
        }
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
            forPrimaryKey: lineage))
        XCTAssertNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: receiptID))
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundPageReceipt.self,
            forPrimaryKey: BigSyncInboundPageReceipt.canonicalID))
        XCTAssertTrue(try value(target).isDeleted)
        XCTAssertEqual(try value(target).epoch, epoch)
        XCTAssertEqual(try value(target).title, expectedTitle)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testAcceptedRetainedDeletionRefreshGenerationRevocationPreservesReceiptsAndRetries() async throws {
