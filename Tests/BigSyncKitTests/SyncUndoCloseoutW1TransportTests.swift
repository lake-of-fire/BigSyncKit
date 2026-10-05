        try await quiet(adapter, realm: reopened)
        try await synchronizer.synchronizeAdapter(adapter)
        let second = await transport.history()
        XCTAssertEqual(second.count, 1)
        _ = wakeups
    }
}
