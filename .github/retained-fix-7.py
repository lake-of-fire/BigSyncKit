from pathlib import Path
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift');s=p.read_text()
s=s.replace('    @Persisted(indexed: true) public var isResolved = false','''    @Persisted(indexed: true) public var isResolved = false
    /// A small local deduplication receipt survives removal of the ordinary
    /// recovery note. It contains no losing text or payload history.
    @Persisted public var isPreservationReceipt = false''',1)
s=s.replace('    static let maximumConflictCount = 512','    static let maximumConflictCount = 512\n    static let maximumPreservationReceiptCount = 4096',1)
s=s.replace('guard realm.objects(BigSyncRecordConflict.self).count < BigSyncRecordEvidenceLimits.maximumConflictCount else {','guard realm.objects(BigSyncRecordConflict.self).where({ !$0.isPreservationReceipt }).count < BigSyncRecordEvidenceLimits.maximumConflictCount else {',1)
a='''        precondition(realm.isInWriteTransaction)
        let fields = try BigSyncRecordFingerprint.fields(of: losingObject)'''
b='''        precondition(realm.isInWriteTransaction)
        try context.validate(in: realm)
        let fields = try BigSyncRecordFingerprint.fields(of: losingObject)''';assert s.count(a)==1;s=s.replace(a,b)
a='''        if realm.object(ofType: type, forPrimaryKey: uuid) != nil {
            // The deterministic identity is the preservation receipt. The user
            // may have edited or deleted that ordinary recovery note since it
            // was created; redelivery cannot overwrite, revive or duplicate it.
            return
        }'''
b='''        let receiptID = "note-preservation:" + hash
        if let receipt = realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: receiptID) {
            guard receipt.isPreservationReceipt, receipt.isResolved,
                  receipt.recordName == record.recordID.recordName,
                  receipt.entityType == type.className() else {
                throw BigSyncRecordContractError.staleConflict
            }
            return
        }
        guard realm.objects(BigSyncRecordConflict.self).where({ $0.isPreservationReceipt }).count
                < BigSyncRecordEvidenceLimits.maximumPreservationReceiptCount else {
            throw BigSyncRecordContractError.evidenceCapacityExceeded
        }
        let receipt = BigSyncRecordConflict()
        receipt.id = receiptID
        receipt.namespace = context.namespace
        receipt.recordName = record.recordID.recordName
        receipt.entityType = type.className()
        receipt.reason = "note-copy-preserved"
        receipt.isResolved = true
        receipt.isPreservationReceipt = true
        realm.add(receipt)
        if realm.object(ofType: type, forPrimaryKey: uuid) != nil {
            // Keep an existing edited/deleted copy and backfill its durable
            // receipt. Normal physical cleanup may remove that copy later.
            return
        }''';assert s.count(a)==1;s=s.replace(a,b);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift');s=p.read_text()
s=s.replace('$0.namespace == context.namespace && $0.isResolved', '$0.namespace == context.namespace && $0.isResolved && !$0.isPreservationReceipt')
s=s.replace('for row in realm.objects(BigSyncRecordConflict.self).where({ $0.namespace == context.namespace }) {','for row in realm.objects(BigSyncRecordConflict.self).where({ $0.namespace == context.namespace && !$0.isPreservationReceipt }) {')
p.write_text(s)
p=Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift');s=p.read_text();i=s.rfind('\n}');extra='''
    @BigSyncBackgroundActor
    func testDeletedRecoveryNoteCannotBeRecreatedAfterPhysicalCleanup() async throws {
        let (adapter, realm) = try await fixture()
        let note = ContractRecoveryNote()
        try realm.write { realm.add(note); note.text = "losing"; note.refreshChangeMetadata(explicitlyModified: true) }
        let source = CKRecord(recordType: ContractRecoveryNote.className(),
            recordID: .init(recordName: ContractRecoveryNote.className() + "." + note.id.uuidString,
                            zoneID: adapter.recordZoneID))
        let context = BigSyncRecordRebaseContext(namespace: "test", account: "account", binding: "binding")
        try realm.write {
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        let copy = try XCTUnwrap(realm.objects(ContractRecoveryNote.self).first { $0.id != note.id })
        let receipt = try XCTUnwrap(realm.objects(BigSyncRecordConflict.self).where { $0.isPreservationReceipt }.first)
        let receiptID = receipt.id
        // The source row remains live. Only the user's recovery copy is gone,
        // as it would be after a successful ordinary note-deletion cleanup.
        try realm.write { realm.delete(copy) }
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertNotNil(realm.object(ofType: BigSyncRecordConflict.self, forPrimaryKey: receiptID))
        try realm.write {
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).where { $0.isPreservationReceipt }.count, 1)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
    }
''';s=s[:i]+extra+s[i:];p.write_text(s)
