from pathlib import Path
import sys
r=Path(sys.argv[1])
p=r/'Sources/BigSyncKit/QSSynchronizer/BigSyncOutboundQuiescence.swift'
s=p.read_text(); old='cutoff.owner.url.deletingLastPathComponent().standardizedFileURL == directoryURL,'; assert s.count(old)==1
s=s.replace(old,'cutoff.owner.url.deletingLastPathComponent().standardizedFileURL.path == directoryURL.path,'); p.write_text(s)
p=r/'Tests/BigSyncKitTests/CloudKitTerminalReceiptTests.swift'
s=p.read_text(); old='''        _ = try await fixture.drain()
        await assertRejected { try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed) }
'''; assert s.count(old)==1
s=s.replace(old,'''        // A successor run invalidates the old completed capability, but
        // cannot bypass its still-closed outbound fence even with no records.
        await assertRejected { _ = try await fixture.synchronizer.synchronize() }
        await assertRejected { try await fixture.synchronizer.revalidatePostBarrierDrainPrincipal(completed) }
''')
s+='''

extension CloudKitTerminalReceiptTests {
    func testOutboundSettlementRequiresExactCompleteResponseIdentities() {
        let a = CKRecord(recordType: "Item", recordID: CKRecord.ID(recordName: "A"))
        let b = CKRecord(recordType: "Item", recordID: CKRecord.ID(recordName: "B"))
        XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [:], deleteResults: [:])
            .hasSettledOutboundOutcome(saving: [a], deleting: []))
        XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [a.recordID: .success(b)], deleteResults: [:])
            .hasSettledOutboundOutcome(saving: [a], deleting: []))
        XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [a.recordID: .success(a), b.recordID: .success(b)], deleteResults: [:])
            .hasSettledOutboundOutcome(saving: [a], deleting: []))
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [a.recordID: .success(a)], deleteResults: [:])
            .hasSettledOutboundOutcome(saving: [a], deleting: []))
    }

    func testOutboundSettlementDoesNotInferSuccessFromNetworkOrCancellationErrors() {
        let record = CKRecord(recordType: "Item", recordID: CKRecord.ID(recordName: "A"))
        let errors: [Error] = [CKError(.networkFailure), CKError(.operationCancelled), CancellationError()]
        for error in errors {
            XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [record.recordID: .failure(error)], deleteResults: [:])
                .hasSettledOutboundOutcome(saving: [record], deleting: []))
        }
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [record.recordID: .failure(CKError(.serverRecordChanged))], deleteResults: [:])
            .hasSettledOutboundOutcome(saving: [record], deleting: []))
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [:], deleteResults: [record.recordID: .failure(CKError(.unknownItem))])
            .hasSettledOutboundOutcome(saving: [], deleting: [record.recordID]))
    }
}
''';p.write_text(s)
p=r/'Tests/BigSyncKitTests/BigSyncOutboundQuiescenceTests.swift'
s=p.read_text(); i=s.index('    func testSeparateClientsDoNotFenceEachOther')
s=s[:i]+'''    func testDirectoryFlagSpellingDoesNotRejectTheSameNamespace() throws {
        let a = fixture()
        let peer = BigSyncOutboundCoordinator(clientNamespace: a.clientNamespace,
            directoryURL: URL(fileURLWithPath: a.directoryURL.path, isDirectory: true))
        let cutoff = try begin(peer)
        try peer.acquireQuiescence(cutoff)
        try a.validateQuiescence(cutoff, principal: principal)
        XCTAssertThrowsError(try a.admit(principal: principal))
    }

    func testReplacingALockInodeInvalidatesTheLiveCapability() throws {
        for name in ["owner.lock", "activity.lock"] {
            let a = fixture(), cutoff = try begin(a)
            try a.acquireQuiescence(cutoff)
            let url = a.directoryURL.appendingPathComponent(name)
            try FileManager.default.removeItem(at: url)
            try Data().write(to: url)
            XCTAssertThrowsError(try a.validateQuiescence(cutoff, principal: principal))
            XCTAssertNotNil(try a.snapshot().authorizationID)
        }
    }

'''+s[i:];p.write_text(s)
