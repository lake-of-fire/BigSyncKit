from pathlib import Path
p = Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift')
s = p.read_text()
old = '''        XCTAssertEqual(sent.records.count, 1)
        try realm.write { object.title = "V2"; object.refreshChangeMetadata(explicitlyModified: true) }'''
new = '''        XCTAssertEqual(sent.records.count, 1)
        // Capture a separate server-owned copy before upload temporary files
        // are retired. A fetched CloudKit asset has independent file storage.
        let accepted = try BigSyncRecordPayload.encode(try XCTUnwrap(sent.records.first))
        try realm.write { object.title = "V2"; object.refreshChangeMetadata(explicitlyModified: true) }'''
assert s.count(old) == 1
s = s.replace(old, new)
old = '''        _ = try await deliver(sent.records, to: adapter)
        XCTAssertEqual(object.title, "V2")'''
new = '''        let serverCopy = try BigSyncRecordPayload.decode(accepted)
        _ = try await deliver([serverCopy], to: adapter)
        XCTAssertEqual(object.title, "V2")'''
assert s.count(old) == 1
s = s.replace(old, new)
p.write_text(s)
