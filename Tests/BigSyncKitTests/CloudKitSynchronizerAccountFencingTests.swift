final class CloudKitSynchronizerAccountFencingTests: XCTestCase {
#if DEBUG
    @BigSyncBackgroundActor
    private final class FailureObserverSnapshotCapture {
        var failures: [BigSyncSynchronizationFailure] = []
    }

    @BigSyncBackgroundActor
    func testReentrantFailureObserversPreserveOneSettlementSnapshot() async throws {
        for cancelsSettlementTask in [false, true] {
            let entered = expectation(description: "first account read held")
            let release = ClosureRestorationGate()
            let synchronizer = makeSynchronizer(
                transport: AccountFencingTransport(),
                accountIdentifierProvider: {
                    entered.fulfill()
                    await release.wait()
                    return "account-a"
                }
            )
            synchronizer.addModelAdapter(AccountFencingModelAdapter(zoneID: synchronizer.recordZoneID))
            let captured = FailureObserverSnapshotCapture()
            let handler: BigSyncSynchronizationFailureHandler = { failure in
                captured.failures.append(failure)
                guard captured.failures.count == 1 else { return }
                if cancelsSettlementTask {
                    withUnsafeCurrentTask { $0?.cancel() }
                } else {
                    // Legal synchronous reentry must not erase the remaining
                    // waiter's already-originated progress evidence.
                    synchronizer.cancelSynchronization()
                }
            }
            let first = Task { @BigSyncBackgroundActor in
                try await synchronizer.synchronize(failureHandler: handler)
            }
            await fulfillment(of: [entered], timeout: 2)
            let second = Task { @BigSyncBackgroundActor in
                try await synchronizer.synchronize(failureHandler: handler)
            }
            addTeardownBlock { @BigSyncBackgroundActor in
                first.cancel()
                second.cancel()
                await release.open()
                await synchronizer.cancelSynchronizationAndWait()
                _ = await first.result
                _ = await second.result
            }
            // The first request is held above; this existing actor-owned flag
            // proves the second request reached admission. No test-only runtime
            // mutation or new production hook is needed.
            let deadline = ContinuousClock.now.advanced(by: .seconds(2))
            while !synchronizer.synchronizationRequestedWhileRunning,
                  ContinuousClock.now < deadline {
                await Task.yield()
            }
            guard synchronizer.synchronizationRequestedWhileRunning else {
                XCTFail("Second request never reached the held drain")
                continue
            }
            let attempt = synchronizer.synchronizationAttemptID
            let run = synchronizer.activeRunContext?.runID
            synchronizer.reportProgress("failure-observer-snapshot")
            let settlement = Task { @BigSyncBackgroundActor in
                synchronizer.cancelSynchronization()
            }
            await settlement.value
            await release.open()
            for request in [first, second] {
                do {
                    _ = try await request.value
                    XCTFail("Cancellation returned synchronization success")
                } catch {
                    XCTAssertTrue(error is CancellationError)
                }
            }
            XCTAssertEqual(captured.failures.count, 2)
            XCTAssertEqual(Set(captured.failures.map(\.requestIdentifier)).count, 2)
            for failure in captured.failures {
                XCTAssertEqual(failure.category, .explicitSynchronizationCancellation)
                XCTAssertEqual(failure.attemptIdentifier, attempt)
                XCTAssertEqual(failure.runIdentifier, run)
                XCTAssertEqual(failure.lastProgressStage, "failure-observer-snapshot")
                XCTAssertFalse(failure.settlementTaskIsCancelled)
            }
            XCTAssertEqual(settlement.isCancelled, cancelsSettlementTask)
        }
    }

    @BigSyncBackgroundActor
    func testE2EWorkerPreservesOriginatingTransportFailure() async throws {
        let transport = AccountFencingTransport()
        transport.nextDatabaseChangesError = NSError(
            domain: "qualification.origin", code: 73,
            userInfo: [NSLocalizedDescriptionKey: "injected database failure"]
        )
        let synchronizer = makeSynchronizer(transport: transport)
        synchronizer.addModelAdapter(AccountFencingModelAdapter(zoneID: synchronizer.recordZoneID))
        let worker = BigSyncBackgroundActor()
        worker._test_installSynchronizer(synchronizer, performsAccountAvailabilityPreflight: false)
        addTeardownBlock { @BigSyncBackgroundActor in await worker.cancelSynchronization() }

        let outcome = await worker.cloudKitE2ESynchronizeCloudKit(
            untilUptimeNanoseconds: UInt64.max
        )
        guard case .failed(let failure) = outcome else {
            return XCTFail("Originating failure was erased: \(outcome)")
        }
        XCTAssertEqual(failure.category, .failed)
        XCTAssertEqual(failure.errorDomain, "qualification.origin")
        XCTAssertEqual(failure.errorCode, 73)
        XCTAssertEqual(failure.attemptIdentifier, synchronizer.synchronizationAttemptID)
        XCTAssertEqual(failure.runIdentifier, synchronizer.synchronizationRunID)
        XCTAssertEqual(failure.lastProgressStage, "database-fetch-start")
        XCTAssertFalse(failure.settlementTaskIsCancelled)

        // A completed later request must not inherit this request's failure.
        let recovery = await worker.cloudKitE2ESynchronizeCloudKit(
            untilUptimeNanoseconds: UInt64.max
        )
        guard case .completed(let result) = recovery else {
            return XCTFail("Old request failure leaked into recovery: \(recovery)")
        }
        XCTAssertNotNil(result?.receipt)
    }

