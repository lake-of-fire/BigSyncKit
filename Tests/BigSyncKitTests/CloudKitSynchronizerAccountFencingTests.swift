final class CloudKitSynchronizerAccountFencingTests: XCTestCase {
#if DEBUG
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

