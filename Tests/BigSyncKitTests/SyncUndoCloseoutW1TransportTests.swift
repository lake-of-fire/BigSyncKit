        try await quiet(adapter, realm: reopened)
        try await synchronizer.synchronizeAdapter(adapter)
        let second = await transport.history()
        XCTAssertEqual(second.count, 1)
        _ = wakeups
    }
}

// These histories exercise the real tracking Realm, not a snapshot stand-in.
// The transactions are deliberately held across the read to model another
// independently owned writer. Only this fixture's ServerToken rows are changed.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testCursorReadIgnoresProvisionalUpdateThenObservesCommitOrRollback() async throws {
        for commits in [false, true] {
            let (adapter, _, _, _) = try await acceptedNote()
            let original = RecordZoneChangeCursor(serializedData: Data("committed-before".utf8))
            try await adapter.saveToken(original)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            try tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            let token = try XCTUnwrap(tracking.objects(ServerToken.self).first)
            token.token = Data("provisional-next".utf8)
            let during = await adapter.serverChangeToken
            XCTAssertEqual(during?.serializedData, original.serializedData)
            XCTAssertTrue(tracking.isInWriteTransaction, "A read must not settle another writer")
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            let after = await adapter.serverChangeToken
            XCTAssertEqual(after?.serializedData,
                           commits ? Data("provisional-next".utf8) : original.serializedData)
        }
    }

    @BigSyncBackgroundActor
    func testCursorReadIgnoresProvisionalRemovalThenObservesCommittedAbsence() async throws {
        for commits in [false, true] {
            let (adapter, _, _, _) = try await acceptedNote()
            let original = RecordZoneChangeCursor(serializedData: Data("before-removal".utf8))
            try await adapter.saveToken(original)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            try tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            tracking.delete(tracking.objects(ServerToken.self))
            let during = await adapter.serverChangeToken
            XCTAssertEqual(during?.serializedData, original.serializedData)
            XCTAssertTrue(tracking.isInWriteTransaction)
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            let after = await adapter.serverChangeToken
            XCTAssertEqual(after?.serializedData, commits ? nil : original.serializedData)
        }
    }

    @BigSyncBackgroundActor
    func testFirstProvisionalCursorDoesNotBecomeBootstrapEvidence() async throws {
        for commits in [false, true] {
            let (adapter, _, _, _) = try await acceptedNote()
            try await adapter.saveToken(nil)
            let absent = await adapter.serverChangeToken
            XCTAssertNil(absent)
            let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
            try tracking.beginWrite()
            defer { if tracking.isInWriteTransaction { tracking.cancelWrite() } }
            let token = tracking.objects(ServerToken.self).first ?? ServerToken()
            if token.realm == nil { tracking.add(token) }
            token.token = Data("first-provisional".utf8)
            let during = await adapter.serverChangeToken
            XCTAssertNil(during, "Only a committed cursor may advance bootstrap")
            XCTAssertTrue(tracking.isInWriteTransaction)
            if commits { try tracking.commitWrite() } else { tracking.cancelWrite() }
            let after = await adapter.serverChangeToken
            XCTAssertEqual(after?.serializedData, commits ? Data("first-provisional".utf8) : nil)
        }
    }
}
