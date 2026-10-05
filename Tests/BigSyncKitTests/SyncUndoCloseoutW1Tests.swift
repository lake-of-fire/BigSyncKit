    @BigSyncBackgroundActor
    func testAcceptedRetainedTombstoneRetiresOnlyItsPhysicalDeletionQuarantine() async throws {
        let (adapter, realm) = try await fixture()
        let object = W1RetainedArticle()
        try realm.write {
            realm.add(object)
            object.epoch = "E0"
            object.isDeleted = true
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let prepared = try await adapter.preparedRecordsToUpload(
            limit: 50,
            restrictedToEntityType: nil
        )
        let record = try XCTUnwrap(prepared.first?.record)
        let deletion = try await adapter.deleteRecords(with: [record.recordID])
        guard case let .quarantined(lineageID) = try XCTUnwrap(
            deletion.first
        ).disposition else {
            return XCTFail("Expected retained physical deletion quarantine")
        }
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        XCTAssertNotNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        let unrelated = try XCTUnwrap(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        let unrelatedLineageID = "unrelated-retained-deletion"
        let unrelatedCopy = BigSyncInboundSemanticQuarantine()
        unrelatedCopy.lineageID = unrelatedLineageID
        unrelatedCopy.recordName = "W1RetainedArticle.unrelated"
        unrelatedCopy.entityType = unrelated.entityType
        unrelatedCopy.accountScopeIdentifier = unrelated.accountScopeIdentifier
        unrelatedCopy.semanticScopeIdentifier =
            "retained-physical-deletion:" + unrelatedCopy.recordName
        unrelatedCopy.containerIdentifier = unrelated.containerIdentifier
        unrelatedCopy.databaseScopeRawValue = unrelated.databaseScopeRawValue
        unrelatedCopy.zoneOwnerName = unrelated.zoneOwnerName
        unrelatedCopy.zoneName = unrelated.zoneName
        unrelatedCopy.eventKind = unrelated.eventKind
        unrelatedCopy.validationCode = unrelated.validationCode
        unrelatedCopy.replicaActivationIdentifier =
            unrelated.replicaActivationIdentifier
        unrelatedCopy.changeFeedEpoch = unrelated.changeFeedEpoch
        try tracking.write { tracking.add(unrelatedCopy) }

        // The prepared retained tombstone is the exact server-restoring
        // disposition. Its acknowledgement may retire only this record's
        // quarantine; unrelated evidence must remain an audit blocker.
        let saved = try prepared.map {
            try tagged(BigSyncRecordPayload.decode(BigSyncRecordPayload.encode($0.record)),
                       "retained-tombstone-accepted")
        }
        try await adapter.didUpload(
            savedRecords: saved,
            matchingPreparedUploads: prepared
        )
        try await adapter.cleanUp()
        XCTAssertNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: lineageID
            )
        )
        XCTAssertNotNil(
            tracking.object(
                ofType: BigSyncInboundSemanticQuarantine.self,
                forPrimaryKey: unrelatedLineageID
            )
        )
        let audit = try await adapter.auditSynchronizationState(
            serverRecords: saved
        )
        XCTAssertFalse(audit.isClean)
        XCTAssertTrue(
            audit.issues.contains {
                $0.contains("W1RetainedArticle.unrelated")
            },
            audit.issues.joined(separator: ",")
        )
        try await quiet(adapter, realm: realm)
    }

    @BigSyncBackgroundActor
    func testTerminalLocalDeleteRetiresItsSupersededStagedSave() async throws {
