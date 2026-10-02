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
