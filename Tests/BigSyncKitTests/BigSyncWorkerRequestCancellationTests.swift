    func testOverflowSaturatesInsteadOfExpiringImmediately() async {
        let clock = DeadlineTestClock(100)
        let race = Race(durationNanoseconds: 50, now: { clock.read() })
        XCTAssertEqual(race.remainingNanoseconds, 50)
        await race.resolve(.completed(7))
        guard case .completed(7) = await race.value() else { return XCTFail("Overflow wrapped deadline") }
    }
