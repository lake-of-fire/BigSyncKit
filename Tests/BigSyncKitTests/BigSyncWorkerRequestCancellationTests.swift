    func testOverflowSaturatesInsteadOfExpiringImmediately() async {
        let clock = DeadlineTestClock(UInt64.max - 20)
        let race = Race(durationNanoseconds: 100, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 20)
        clock.set(UInt64.max - 1)
        XCTAssertEqual(race.remainingNanoseconds, 1)
        await race.resolve(.completed(7))
        guard case .completed(7) = await race.value() else { return XCTFail("Overflow wrapped deadline") }
    }
