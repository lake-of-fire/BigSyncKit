        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage),
                     "An authorized retry must retire B before discarding its resolution evidence")
        XCTAssertNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id))
        XCTAssertEqual(second.title, "second local")
    }

}

// Real Realm notification reentry at the public archive-cleanup boundary.
// The signal write changes only fixture-owned local archive metadata; it does
// not fabricate a submitted/accepted record or mutate a pending journal.
private final class CleanupRefreshAuthority: @unchecked Sendable {
    enum Mode: Sendable { case live, revokeLease, cancelTask }
    enum Failure: Error { case revoked, holdTrackingCleanup }
    let mode: Mode
    private let lock = NSLock()
    private var seeded = false
    private var observed = false
    private var armed = false

    init(_ mode: Mode) { self.mode = mode }
    func armOnce() -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard !seeded else { return false }
        seeded = true; armed = true
        return true
    }
    func receiveChange() {
        lock.lock()
        let shouldObserve = armed && !observed
        if shouldObserve { observed = true }
        lock.unlock()
        if shouldObserve, mode == .cancelTask {
            withUnsafeCurrentTask { $0?.cancel() }
        }
    }
    var didObserve: Bool {
        lock.lock(); defer { lock.unlock() }
        return observed
    }
    func validate() throws {
        if mode == .revokeLease && didObserve { throw Failure.revoked }
    }
}

extension SyncRetainedRecordContractTests {
    @BigSyncBackgroundActor
    private struct CleanupRefreshFixture {
        let adapter: RealmSwiftAdapter
        let target: Realm
        let tracking: Realm
        let conflictID: String
        let lineageID: String
        let originalTitle: String
        let pendingGeneration: String?
    }

    @BigSyncBackgroundActor
    private func cleanupRefreshFixture() async throws -> CleanupRefreshFixture {
        let (adapter, target, object, conflict) = try await unbasedRecoveryFixture()
        let tracking = try XCTUnwrap(adapter.realmProvider?.persistenceRealm)
        let lineage = try XCTUnwrap(tracking.objects(BigSyncInboundSemanticQuarantine.self).first).lineageID
        do {
            try await adapter.resolveRecordConflict(
                id: conflict.id, expectedGeneration: conflict.generation,
                choice: .keepLocal, validateAuthority: {
                    // Preserve the actual committed target / unretired tracking
                    // prefix. This predicate is tied to state, not call count.
                    if !target.isInWriteTransaction,
                       target.object(ofType: BigSyncRecordConflict.self,
                                     forPrimaryKey: conflict.id)?.isResolved == true {
                        throw CleanupRefreshAuthority.Failure.holdTrackingCleanup
                    }
                }
            )
            XCTFail("Expected the tracking phase to remain pending")
        } catch CleanupRefreshAuthority.Failure.holdTrackingCleanup { }
        XCTAssertTrue(try XCTUnwrap(target.object(
            ofType: BigSyncRecordConflict.self, forPrimaryKey: conflict.id)).isResolved)
        XCTAssertNotNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                       forPrimaryKey: lineage))
        return .init(adapter: adapter, target: target, tracking: tracking,
                     conflictID: conflict.id, lineageID: lineage,
                     originalTitle: object.title,
                     pendingGeneration: target.objects(BigSyncPendingMutation.self).first?.generation)
    }

    @BigSyncBackgroundActor
    private func exerciseCleanupRefresh(
        mode: CleanupRefreshAuthority.Mode, retry: Bool = false
    ) async throws {
        let fixture = try await cleanupRefreshFixture()
        let authority = CleanupRefreshAuthority(mode)
        let writerQueue = DispatchQueue(label: "test.cleanup-refresh." + UUID().uuidString)
        let configuration = fixture.target.configuration
        let conflictID = fixture.conflictID
        let priorAutorefresh = fixture.target.autorefresh
        fixture.target.autorefresh = false
        let observation = fixture.target.observe { notification, _ in
            if case .didChange = notification { authority.receiveChange() }
        }
        defer {
            observation.invalidate()
            fixture.target.autorefresh = priorAutorefresh
        }
        let request = Task { @BigSyncBackgroundActor in
            try await fixture.adapter.discardResolvedRecordConflictArchives(validateAuthority: {
                try authority.validate()
                guard fixture.tracking.isInWriteTransaction, authority.armOnce() else { return }
                // The private cleanup has selected its initial candidates and
                // holds the *tracking* writer. A separate scheduler commits the
                // target metadata; the ensuing target refresh delivers the real
                // notification which revokes the caller or cancels this task.
                try writerQueue.sync {
                    let writer = try Realm(configuration: configuration, queue: writerQueue)
                    try writer.write {
                        let row = try XCTUnwrap(writer.object(
                            ofType: BigSyncRecordConflict.self, forPrimaryKey: conflictID))
                        row.createdAt = row.createdAt.addingTimeInterval(1)
                    }
                }
            })
        }
        addTeardownBlock { request.cancel(); _ = await request.result }
        let outcome = await request.result
        XCTAssertTrue(authority.didObserve, "Must exercise real target refresh notification delivery")
        if mode == .live {
            try outcome.get()
            XCTAssertNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                 forPrimaryKey: fixture.lineageID))
            XCTAssertNil(fixture.target.object(ofType: BigSyncRecordConflict.self,
                                               forPrimaryKey: fixture.conflictID))
        } else {
            switch outcome {
            case .success: XCTFail("Revoked cleanup must not publish success")
            case .failure(let error):
                if mode == .cancelTask { XCTAssertTrue(error is CancellationError) }
                else { XCTAssertTrue(error is CleanupRefreshAuthority.Failure) }
            }
            XCTAssertEqual(request.isCancelled, mode == .cancelTask)
            XCTAssertNotNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                    forPrimaryKey: fixture.lineageID))
            XCTAssertTrue(try XCTUnwrap(fixture.target.object(
                ofType: BigSyncRecordConflict.self, forPrimaryKey: fixture.conflictID)).isResolved,
                "Reject cleanup, not the previously committed target decision")
            if retry {
                observation.invalidate()
                try await fixture.adapter.discardResolvedRecordConflictArchives()
                XCTAssertNil(fixture.tracking.object(ofType: BigSyncInboundSemanticQuarantine.self,
                                                     forPrimaryKey: fixture.lineageID))
                XCTAssertNil(fixture.target.object(ofType: BigSyncRecordConflict.self,
                                                   forPrimaryKey: fixture.conflictID))
            }
        }
        XCTAssertEqual(try value(fixture.target).title, fixture.originalTitle)
        XCTAssertEqual(fixture.target.objects(BigSyncPendingMutation.self).first?.generation,
                       fixture.pendingGeneration)
    }

    @BigSyncBackgroundActor
    func testRefreshRevocationCannotRetireResolvedQuarantineOrArchive() async throws {
        try await exerciseCleanupRefresh(mode: .revokeLease)
    }

    @BigSyncBackgroundActor
    func testRefreshTaskCancellationCannotRetireResolvedQuarantineOrArchive() async throws {
        try await exerciseCleanupRefresh(mode: .cancelTask)
    }

    @BigSyncBackgroundActor
    func testCurrentRefreshCleanupStillRetiresQuarantineAndArchive() async throws {
        try await exerciseCleanupRefresh(mode: .live)
    }

    @BigSyncBackgroundActor
    func testRefreshRejectedCleanupCanRetryWithoutChangingCommittedDecision() async throws {
        try await exerciseCleanupRefresh(mode: .revokeLease, retry: true)
    }
}

extension SyncRetainedRecordContractTests {
    @BigSyncBackgroundActor
    private func exportedConflictValues(_ adapter: RealmSwiftAdapter) throws -> [[String: Any]] {
        let bytes = try adapter.exportPreservedRecordConflicts()
        let archive = try XCTUnwrap(PropertyListSerialization.propertyList(
            from: bytes, format: nil) as? [String: Any])
        XCTAssertEqual(archive["format"] as? String, "BigSyncPreservedConflicts-v1")
        return try XCTUnwrap(archive["records"] as? [[String: Any]])
    }

    @BigSyncBackgroundActor
    func testConflictPreviewIgnoresAnotherOwnersProvisionalResolution() async throws {
        for commits in [false, true] {
            let (adapter, realm, object, original) = try await unbasedRecoveryFixture()
            let pending = realm.objects(BigSyncPendingMutation.self).first?.generation
            let archived = try XCTUnwrap(realm.object(
                ofType: BigSyncRecordConflict.self, forPrimaryKey: original.id))
            try realm.beginWrite()
            defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
            // This local fixture transition models another owner's pending
            // archive update, not an acknowledged conflict-resolution command.
            archived.isResolved = true
            let during = try adapter.unresolvedRecordConflicts()
            XCTAssertEqual(during.map(\.id), [original.id])
            XCTAssertEqual(during.first?.localTitle, original.localTitle)
            XCTAssertEqual(during.first?.incomingTitle, original.incomingTitle)
            XCTAssertEqual(during.first?.generation, original.generation)
            XCTAssertTrue(realm.isInWriteTransaction)
            if commits { try realm.commitWrite() } else { realm.cancelWrite() }
            let after = try adapter.unresolvedRecordConflicts()
            XCTAssertEqual(after.map(\.id), commits ? [] : [original.id])
            XCTAssertEqual(object.title, "mine")
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
        }
    }

    @BigSyncBackgroundActor
    func testConflictExportExcludesProvisionalPayloadAndRemoval() async throws {
        for removesRow in [false, true] {
            for commits in [false, true] {
                let (adapter, realm, object, original) = try await unbasedRecoveryFixture()
                let before = try XCTUnwrap(try exportedConflictValues(adapter).first)
                let pending = realm.objects(BigSyncPendingMutation.self).first?.generation
                let archived = try XCTUnwrap(realm.object(
                    ofType: BigSyncRecordConflict.self, forPrimaryKey: original.id))
                let provisional = Data("private provisional archive bytes".utf8)
                try realm.beginWrite()
                defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
                if removesRow { realm.delete(archived) }
                else { archived.localPayload = provisional }
                let during = try exportedConflictValues(adapter)
                XCTAssertEqual(during.count, 1)
                XCTAssertEqual(NSDictionary(dictionary: try XCTUnwrap(during.first)),
                               NSDictionary(dictionary: before))
                XCTAssertTrue(realm.isInWriteTransaction, "Export must not settle the held owner")
                if commits { try realm.commitWrite() } else { realm.cancelWrite() }
                let after = try exportedConflictValues(adapter)
                if removesRow && commits {
                    XCTAssertTrue(after.isEmpty)
                } else {
                    XCTAssertEqual(after.first?["localPayload"] as? Data,
                                   commits ? provisional : before["localPayload"] as? Data)
                }
                XCTAssertEqual(object.title, "mine")
                XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
            }
        }
    }
}
