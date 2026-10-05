extension SyncRetainedRecordContractTests {
    private enum RetainedAcknowledgementRetryGuard { case wrongTag, wrongContext, newerGeneration }

    @BigSyncBackgroundActor
    private func exerciseRetainedAcknowledgementRefresh(
        mode: CleanupRefreshAuthority.Mode,
        retryGuard: RetainedAcknowledgementRetryGuard? = nil
    ) async throws {
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
        let sentGeneration = try XCTUnwrap(prepared.first?.generation)
        func acceptedReply(_ input: CKRecord, tag: String) throws -> CKRecord {
            // A server reply owns independent bytes and a real version tag;
            // an untagged prepared record is not accepted-server evidence.
            let copy = try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(input))
            guard copy.responds(to: NSSelectorFromString("setRecordChangeTag:")) else {
                XCTFail("CloudKit SDK cannot construct a tagged response fixture")
                throw CocoaError(.coderReadCorrupt)
            }
            _ = copy.perform(NSSelectorFromString("setRecordChangeTag:"), with: tag as NSString)
            return try BigSyncRecordPayload.decode(BigSyncRecordPayload.encode(copy))
        }
        let saved = try acceptedReply(XCTUnwrap(prepared.first?.record), tag: "retained-atomic-accepted")
        let results = try await adapter.deleteRecords(with: [saved.recordID])
        guard case .quarantined(let lineage) = try XCTUnwrap(results.first).disposition else {
            return XCTFail("A real retained physical deletion must create quarantine evidence")
        }

        adapter._testAfterAcceptedRetainedDeletionTrackingAdmission = {
            XCTAssertTrue(tracking.isInWriteTransaction)
            XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: saved.recordID.recordName)?.generation, sentGeneration)
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
            guard authority.armOnce() else { return }
            // Tracking acknowledgement is still provisional and the sent
            // journal remains the retry input. Commit only a metadata signal;
            // finalization then refreshes the real target in this transaction.
            try writerQueue.sync {
                let writer = try Realm(configuration: configuration, queue: writerQueue)
                try writer.write {

        XCTAssertEqual(request.isCancelled, mode == .cancelTask)
        XCTAssertTrue(authority.didObserve, "Must deliver a real refresh inside atomic acknowledgement")
        if mode == .live {
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.entityState, .synced)
            XCTAssertNil(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.pendingGeneration)
            XCTAssertTrue(target.objects(BigSyncPendingMutation.self).isEmpty)
        } else {
            // Cleanup rejection rolls back its tracking acknowledgement too.
            // The prior target comparison may already be durable; its journal
            // remains available for ordinary generation-matched retry.
            XCTAssertEqual(tracking.object(ofType: SyncedEntity.self,
                forPrimaryKey: saved.recordID.recordName)?.pendingGeneration, sentGeneration)
            XCTAssertEqual(target.object(ofType: BigSyncPendingMutation.self,
                forPrimaryKey: saved.recordID.recordName)?.generation, sentGeneration)
        }
        if mode != .live {
            XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineage))

            if retryGuard == .newerGeneration {
                let current = try await adapter.preparedRecordsToUpload(limit: 10, restrictedToEntityType: nil)
                let replies = try current.map {
                    try acceptedReply($0.record, tag: "retained-successor-accepted")
                }
                try await adapter.didUpload(savedRecords: replies, matchingPreparedUploads: current)
            } else {
                // The failed atomic tracking write preserved the ordinary
                // pending-generation retry input. No cleanup-only replay.
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
