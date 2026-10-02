    func testCycleTerminatesAndFailsClosedWithoutLeakingItsRoot() {
        weak var released: ReceiptCyclicError?
        autoreleasepool {
            let cyclic = ReceiptCyclicError()
            released = cyclic
            XCTAssertEqual(cloudKitErrors(in: cyclic).count, 2)
            let constraints = CloudKitRetryConstraints(cyclic)
            XCTAssertTrue(constraints.codes.contains(.limitExceeded))
            XCTAssertFalse(constraints.containsOnlySizeLimitFailures)
        }
        XCTAssertNil(released)
    }
}

// Completion-delivery regressions share this already registered native file.
// Their no-op drains send no record mutations; every account/zone API is injected.
private actor ReceiptCompletionAccountProbe {
    private(set) var calls = 0
    func identity() -> String { calls += 1; return "completion-account" }
}

private actor ReceiptCompletionZoneStore: CloudKitZoneStore {
    let missing: Bool
    let fetchError: Error?
    private(set) var fetches = 0
    private(set) var saves = 0
    init(missing: Bool, fetchError: Error? = nil) {
        self.missing = missing
        self.fetchError = fetchError
    }
    func recordZone(withID id: CKRecordZone.ID) async throws -> CKRecordZone {
        fetches += 1
        if let fetchError { throw fetchError }
        if missing { throw CKError(.zoneNotFound) }
        return CKRecordZone(zoneID: id)
    }
    func save(recordZone: CKRecordZone) async throws -> CKRecordZone {
        saves += 1
        return recordZone
    }
    func deleteRecordZone(withID id: CKRecordZone.ID) async throws {
        throw NSError(domain: "UnexpectedCompletionZoneDeletion", code: 1)
    }
}

@BigSyncBackgroundActor
private final class ReceiptCompletionObservation {
    var errors: [Error?] = []
}

extension SyncMutationReceiptFailureCompositionTests {
    private enum CompletionRoute: CaseIterable {
        case allUploads, records, deletions, setupAndUpload, existingZone, createdZone
    }

    @BigSyncBackgroundActor
    private struct CompletionFixture {
        let sync: CloudKitSynchronizer
        let adapter: ReceiptFailureAdapter
        let account: ReceiptCompletionAccountProbe
        let zone: ReceiptCompletionZoneStore
    }

    @BigSyncBackgroundActor
    private func makeCompletionFixture(
        _ route: CompletionRoute, fetchError: Error? = nil
    ) -> CompletionFixture {
        // Uploads see no upload candidates on a delete-only adapter, and
        // deletions see none on the ordinary upload fixture. Do not fabricate
        // receipts or acknowledge pending generations to obtain an empty drain.
        let phase: ReceiptFailurePhase = route == .deletions ? .none : .deleteAcknowledgement
        let adapter = ReceiptFailureAdapter(phase)
        let account = ReceiptCompletionAccountProbe()
        let zone = ReceiptCompletionZoneStore(missing: route == .createdZone, fetchError: fetchError)
        let transport = ReceiptFailureTransport(phase: phase, sibling: CKError(.networkFailure),
            account: ReceiptAccountProbe(failAfterResult: false))
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let sync = CloudKitSynchronizer(identifier: UUID().uuidString,
            containerIdentifier: "iCloud.test.completion-delivery", database: ReceiptDatabaseIdentity(),
            recordZoneID: adapter.recordZoneID, keyValueStore: ReceiptKeyValueStore(),
            accountIdentifierProvider: { await account.identity() }, accountStatusProvider: { .available },
            changeFeed: transport, subscriptionStore: transport, zoneStore: zone,
            recordStore: transport, backupDetectionBaseURL: directory,
            logger: Logger(label: "CompletionDelivery"))
        sync.activeRunContext = .init(attemptID: sync.synchronizationAttemptID,
            runID: sync.synchronizationRunID, accountIdentifier: "completion-account",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "completion-account"))
        addTeardownBlock { @BigSyncBackgroundActor in
            await sync.cancelSynchronizationAndWait()
            try? FileManager.default.removeItem(at: directory)
        }
        return .init(sync: sync, adapter: adapter, account: account, zone: zone)
    }

    @BigSyncBackgroundActor
    private func runCompletionRoute(
        _ route: CompletionRoute, fixture: CompletionFixture,
        completion: @escaping @Sendable @BigSyncBackgroundActor (Error?) async throws -> Void
    ) async throws {
        let sync = fixture.sync
        switch route {
        case .allUploads:
            try await sync.uploadChanges(completion: completion)
        case .records:
            try await sync.uploadRecordsUsingAsyncStore(adapter: fixture.adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .deletions:
            try await sync.uploadDeletionsUsingAsyncStore(adapter: fixture.adapter, restrictedToEntityType: nil,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .setupAndUpload:
            try await sync.setupZoneAndUploadRecords(adapter: fixture.adapter,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        case .existingZone, .createdZone:
            try await sync.setupRecordZoneID(fixture.adapter.recordZoneID,
                attemptID: sync.synchronizationAttemptID, completion: completion)
        }
    }

    @BigSyncBackgroundActor
    func testThrowingSuccessfulCompletionIsNotDeliveredAgain() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = NSError(domain: "CompletionConsumerFailure", code: 501)
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    throw original
                }
                XCTFail("Expected completion failure: \(route)")
            } catch { XCTAssertTrue((error as NSError) === original) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertNil(seen.errors.first ?? nil)
        }
    }

    @BigSyncBackgroundActor
    func testCompletionErrorCannotBeSwallowedByAnotherDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = NSError(domain: "CompletionConsumerFailure", code: 502)
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    if $0 == nil { throw original }
                }
                XCTFail("Second callback consumed the first callback's error: \(route)")
            } catch { XCTAssertTrue((error as NSError) === original) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
        }
    }

    @BigSyncBackgroundActor
    func testZoneShapedCompletionErrorDoesNotRestartZoneRecovery() async throws {
        for route in [CompletionRoute.existingZone, .createdZone, .setupAndUpload] {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            let original = CKError(.zoneNotFound)
            var readsAtFirstDelivery: Int?
            var savesAtFirstDelivery: Int?
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    if readsAtFirstDelivery == nil {
                        readsAtFirstDelivery = await fixture.account.calls
                        savesAtFirstDelivery = await fixture.zone.saves
                    }
                    throw original
                }
                XCTFail("Expected completion failure")
            } catch { XCTAssertEqual((error as? CKError)?.code, .zoneNotFound) }
            let finalReads = await fixture.account.calls
            let finalSaves = await fixture.zone.saves
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertEqual(finalReads, readsAtFirstDelivery)
            XCTAssertEqual(finalSaves, savesAtFirstDelivery)
            XCTAssertEqual(finalSaves, route == .createdZone ? 1 : 0)
        }
    }

    @BigSyncBackgroundActor
    func testSuspendedThrowingCompletionIsStillSingleDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            do {
                try await runCompletionRoute(route, fixture: fixture) {
                    seen.errors.append($0)
                    await Task.yield()
                    throw CancellationError()
                }
                XCTFail("Completion cancellation was swallowed")
            } catch { XCTAssertTrue(error is CancellationError) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
        }
    }

    @BigSyncBackgroundActor
    func testAccountStopAndThrowingDeliveryKeepOneOriginalOperationOutcome() async throws {
        let fixture = makeCompletionFixture(.existingZone, fetchError: CKError(.notAuthenticated))
        let seen = ReceiptCompletionObservation()
        let original = NSError(domain: "CompletionConsumerFailure", code: 503)
        do {
            try await runCompletionRoute(.existingZone, fixture: fixture) {
                seen.errors.append($0)
                throw original
            }
            XCTFail("Expected consumer error")
        } catch { XCTAssertTrue((error as NSError) === original) }
        XCTAssertEqual(seen.errors.count, 1)
        XCTAssertEqual(((seen.errors.first ?? nil) as? CKError)?.code, .notAuthenticated)
        let reads = await fixture.account.calls
        let saves = await fixture.zone.saves
        XCTAssertEqual(reads, 1, "No fresh account request after the returned stop")
        XCTAssertEqual(saves, 0)
    }

    @BigSyncBackgroundActor
    func testSuccessfulNoOpAndZoneCreationCompletionsRemainSingleDelivery() async throws {
        for route in CompletionRoute.allCases {
            let fixture = makeCompletionFixture(route)
            let seen = ReceiptCompletionObservation()
            try await runCompletionRoute(route, fixture: fixture) { seen.errors.append($0) }
            XCTAssertEqual(seen.errors.count, 1, "\(route)")
            XCTAssertNil(seen.errors.first ?? nil)
            let saves = await fixture.zone.saves
            XCTAssertEqual(saves, route == .createdZone ? 1 : 0)
        }
    }

    @BigSyncBackgroundActor
    func testMutationStageCancellationStillDeliversExactlyOneFailure() async throws {
        for deletes in [false, true] {
            let fixture = makeCompletionFixture(deletes ? .deletions : .records)
            let seen = ReceiptCompletionObservation()
            let completion: @Sendable @BigSyncBackgroundActor (Error?) async throws -> Void = {
                seen.errors.append($0)
            }
            let retiredAttempt = UUID()
            if deletes {
                try await fixture.sync.uploadDeletionsUsingAsyncStore(adapter: fixture.adapter,
                    restrictedToEntityType: nil, attemptID: retiredAttempt, completion: completion)
            } else {
                try await fixture.sync.uploadRecordsUsingAsyncStore(adapter: fixture.adapter,
                    restrictedToEntityType: nil, attemptID: retiredAttempt, completion: completion)
            }
            XCTAssertEqual(seen.errors.count, 1)
            XCTAssertTrue((seen.errors.first ?? nil) is CancellationError)
        }
    }
}
