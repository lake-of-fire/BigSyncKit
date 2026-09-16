from pathlib import Path
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordBaseline.swift');s=p.read_text()
s=s.replace('    @Persisted public var schemaSignature = ""','''    @Persisted public var schemaSignature = ""
    /// The CAS template belongs to the accepted base, not a separately
    /// published tracking cache. No user payload or historical versions.
    @Persisted public var acceptedSystemFields: Data?''',1)
s=s.replace('schemaSignature: String = "", in realm: Realm) -> Bool {','schemaSignature: String = "", systemFields: Data? = nil, in realm: Realm) -> Bool {')
s=s.replace('existing?.schemaSignature == schemaSignature { return false }','''existing?.schemaSignature == schemaSignature {
            // Backfill metadata without inventing another semantic revision.
            if existing?.acceptedSystemFields == nil, let systemFields {
                existing?.acceptedSystemFields = systemFields
            }
            return false
        }''')
s=s.replace('row.serverChangeTag = serverChangeTag\n','row.serverChangeTag = serverChangeTag\n        row.acceptedSystemFields = systemFields\n',1)
s=s.replace('row.serverChangeTag = nil\n','row.serverChangeTag = nil\n        row.acceptedSystemFields = nil\n',1)
p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift');s=p.read_text();anchor='''    static func identity(_ parts: [String]) -> String {''';assert s.count(anchor)==1;s=s.replace(anchor,'''    static func systemFields(of record: CKRecord) throws -> Data {
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        record.encodeSystemFields(with: archiver)
        archiver.finishEncoding()
        return archiver.encodedData
    }

    static func record(systemFields: Data) throws -> CKRecord {
        let decoder = try NSKeyedUnarchiver(forReadingFrom: systemFields)
        decoder.requiresSecureCoding = true
        defer { decoder.finishDecoding() }
        guard let record = CKRecord(coder: decoder) else { throw CocoaError(.coderReadCorrupt) }
        return record
    }

'''+anchor);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift');s=p.read_text();old='''            _ = contract
            return try BigSyncRecordPayload.record(from: object, recordID: record.recordID,
                template: record, assetManager: forceDataTypeInsteadOfAsset ? nil : persistentAssetManager)''';new='''            let template: CKRecord
            if targetZoneID == nil,
               let base = object.realm?.object(ofType: BigSyncRecordBaseline.self,
                    forPrimaryKey: syncedEntity.identifier),
               base.namespace == recordRebaseContext?.namespace, !base.isComparisonInvalidated,
               base.schemaSignature == contract.signature, let systemFields = base.acceptedSystemFields {
                template = try BigSyncRecordPayload.record(systemFields: systemFields)
                guard template.recordID == record.recordID,
                      template.recordType == record.recordType,
                      template.recordChangeTag == base.serverChangeTag else {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(syncedEntity.identifier)
                }
            } else {
                template = record
            }
            return try BigSyncRecordPayload.record(from: object, recordID: record.recordID,
                template: template, assetManager: forceDataTypeInsteadOfAsset ? nil : persistentAssetManager)''';assert s.count(old)==1;s=s.replace(old,new)
old='''                schemaSignature: try BigSyncCompiledRecordContract.compile(object)?.signature ?? "", in: realm
            )''';new='''                schemaSignature: try BigSyncCompiledRecordContract.compile(object)?.signature ?? "",
                systemFields: try BigSyncRecordPayload.systemFields(of: record), in: realm
            )''';assert s.count(old)==1;s=s.replace(old,new)
old='''                            schemaSignature: proof.schemaSignature, in: realm)''';new='''                            schemaSignature: proof.schemaSignature,
                            systemFields: try BigSyncRecordPayload.systemFields(of: saved), in: realm)''';assert s.count(old)==1;s=s.replace(old,new)
s += '''
public extension RealmSwiftAdapter {
    @BigSyncBackgroundActor
    func exportPreservedRecordConflicts() throws -> Data {
        guard let context = recordRebaseContext else { throw CancellationError() }
        var snapshots = [[String: Any]]()
        var identities = Set<String>()
        for realm in realmProvider?.targetReaderRealms ?? [] {
            guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordConflict.className() }) else { continue }
            realm.refresh()
            for row in realm.objects(BigSyncRecordConflict.self).where({ $0.namespace == context.namespace }) {
                guard identities.insert(row.id).inserted else { continue }
                snapshots.append(["id": row.id, "recordName": row.recordName,
                    "entityType": row.entityType, "generation": row.generation,
                    "schemaSignature": row.schemaSignature, "reason": row.reason,
                    "createdAt": row.createdAt, "resolved": row.isResolved,
                    "localPayload": row.localPayload, "incomingPayload": row.incomingPayload])
            }
        }
        return try PropertyListSerialization.data(fromPropertyList:
            ["format": "BigSyncPreservedConflicts-v1", "records": snapshots], format: .binary, options: 0)
    }

    /// Explicit archive cleanup. Unresolved values are never evicted to make
    /// room, and no pending submission or mutation generation is touched.
    @BigSyncBackgroundActor
    func discardResolvedRecordConflictArchives() async throws {
        guard let context = recordRebaseContext else { throw CancellationError() }
        try await retireResolvedRecordConflictQuarantines()
        for realm in realmProvider?.targetReaderRealms ?? [] {
            guard realm.schema.objectSchema.contains(where: { $0.className == BigSyncRecordConflict.className() }) else { continue }
            try await realm.asyncWrite {
                guard recordRebaseContext == context else { throw CancellationError() }
                try context.validate(in: realm)
                realm.delete(realm.objects(BigSyncRecordConflict.self).where {
                    $0.namespace == context.namespace && $0.isResolved
                })
            }
        }
    }
}
''';p.write_text(s)
p=Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift');s=p.read_text();extra='''
    @BigSyncBackgroundActor
    func testAcceptedCASSystemFieldsCommitWithTheBaseline() async throws {
        let (adapter, realm) = try await fixture()
        let incoming = record(adapter)
        _ = try await deliver([incoming], to: adapter)
        let row = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        let archived = try XCTUnwrap(row.acceptedSystemFields)
        let restored = try BigSyncRecordPayload.record(systemFields: archived)
        XCTAssertEqual(restored.recordID, incoming.recordID)
        XCTAssertEqual(restored.recordType, incoming.recordType)
        XCTAssertEqual(restored.recordChangeTag, row.serverChangeTag)
        XCTAssertTrue(restored.allKeys().isEmpty, "Accepted CAS evidence must not retain payload history")
        let revision = row.revision
        _ = try await deliver([incoming], to: adapter)
        XCTAssertEqual(row.revision, revision)
    }

    @BigSyncBackgroundActor
    func testConflictArchivePruningCannotDiscardUnresolvedWork() async throws {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "private local work"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "remote work")], to: adapter)
        let conflict = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        let archive = try adapter.exportPreservedRecordConflicts()
        XCTAssertFalse(archive.isEmpty)
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().count, 1)
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
        XCTAssertEqual(object.title, "private local work")
        try await adapter.discardResolvedRecordConflictArchives()
        XCTAssertTrue(realm.objects(BigSyncRecordConflict.self).isEmpty)
        XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }
''';i=s.rfind('\n}');s=s[:i]+extra+s[i:];p.write_text(s)
