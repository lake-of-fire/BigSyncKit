        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNil(tracking.object(ofType: BigSyncInboundSemanticQuarantine.self, forPrimaryKey: lineage),
                     "An authorized retry must retire B before discarding its resolution evidence")
        XCTAssertNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: secondConflict.id))
        XCTAssertEqual(second.title, "second local")
    }

}
