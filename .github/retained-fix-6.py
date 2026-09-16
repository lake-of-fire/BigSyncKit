from pathlib import Path
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift');s=p.read_text();a='''    public let incomingText: String?
}''';b='''    public let incomingText: String?
    public let localTitle: String?
    public let incomingTitle: String?
    public let localLifetime: String?
    public let incomingLifetime: String?
    public let localIsDeleted: Bool
    public let incomingIsDeleted: Bool
    /// A record-level user choice cannot reverse an ordered lifecycle. Both
    /// related members must continue to use the same successor order.
    public let requiredChoice: BigSyncRecordConflictChoice?
}''';assert s.count(a)==1;s=s.replace(a,b);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift');s=p.read_text();a='''                result.append(.init(id: row.id, recordName: row.recordName, entityType: row.entityType,
                    reason: row.reason, generation: row.generation, createdAt: row.createdAt,
                    localText: local["text"] as? String, incomingText: incoming["text"] as? String))''';b='''                let type = realmObjectClass(name: row.entityType)
                let policy = type.flatMap { $0 as? BigSyncRecordContractProviding.Type }?.bigSyncRecordContract.policy
                let lifetimes = conflictLifetimes(local: local, incoming: incoming, policy: policy)
                result.append(.init(id: row.id, recordName: row.recordName, entityType: row.entityType,
                    reason: row.reason, generation: row.generation, createdAt: row.createdAt,
                    localText: local["text"] as? String, incomingText: incoming["text"] as? String,
                    localTitle: local["title"] as? String, incomingTitle: incoming["title"] as? String,
                    localLifetime: lifetimes.local, incomingLifetime: lifetimes.incoming,
                    localIsDeleted: BigSyncCloudKitBooleanCodec.decode(local["isDeleted"]) == true,
                    incomingIsDeleted: BigSyncCloudKitBooleanCodec.decode(incoming["isDeleted"]) == true,
                    requiredChoice: try requiredLifecycleChoice(lifetimes: lifetimes)))''';assert s.count(a)==1;s=s.replace(a,b)
a='''                let local = try BigSyncRecordFingerprint.fields(of: object)
                let remote = try BigSyncRecordFingerprint.fields(of: decodedComparisonObject(incoming, type: type))
                _ = try applyComparisonFields(choice == .useIncoming ? Set(remote.keys) : [],''';b='''                let currentRecord = try BigSyncRecordPayload.record(from: object, recordID: incoming.recordID)
                let lifetimes = conflictLifetimes(local: currentRecord, incoming: incoming,
                    policy: contract.declaration.policy)
                if let required = try requiredLifecycleChoice(lifetimes: lifetimes), required != choice {
                    throw BigSyncRecordContractError.staleConflict
                }
                let local = try BigSyncRecordFingerprint.fields(of: object)
                let remote = try BigSyncRecordFingerprint.fields(of: decodedComparisonObject(incoming, type: type))
                _ = try applyComparisonFields(choice == .useIncoming ? Set(remote.keys) : [],''';assert s.count(a)==1;s=s.replace(a,b)
a='''    @BigSyncBackgroundActor
    public func unresolvedRecordConflicts()''';b='''    private func conflictLifetimes(local: CKRecord, incoming: CKRecord,
        policy: BigSyncRecordRebasePolicy?) -> (local: String?, incoming: String?) {
        guard case let .lifetimeBundle(field, _) = policy else { return (nil, nil) }
        return (local[field] as? String, incoming[field] as? String)
    }

    private func requiredLifecycleChoice(lifetimes: (local: String?, incoming: String?)) throws
        -> BigSyncRecordConflictChoice? {
        guard lifetimes.local != lifetimes.incoming,
              let incoming = try BigSyncLifetimeID.prefersIncoming(local: lifetimes.local,
                  incoming: lifetimes.incoming) else { return nil }
        return incoming ? .useIncoming : .keepLocal
    }

'''+a;assert s.count(a)==1;s=s.replace(a,b);p.write_text(s)
p=Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift');s=p.read_text();i=s.rfind('\n}');extra='''
    @BigSyncBackgroundActor
    func testUnbasedRecoveryCannotReverseAnOrderedLifetime() async throws {
        let (adapter, realm) = try await fixture()
        let initial = RetainedContractRow()
        let successor = try BigSyncLifetimeID.next(after: initial.epoch)
        try realm.write {
            realm.add(initial)
            initial.title = "local title without an accepted base"
            initial.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, epoch: successor, deleted: true)], to: adapter)
        let conflict = try XCTUnwrap(try adapter.unresolvedRecordConflicts().first)
        XCTAssertEqual(conflict.requiredChoice, .useIncoming)
        XCTAssertEqual(conflict.incomingLifetime, successor)
        XCTAssertTrue(conflict.incomingIsDeleted)
        do {
            try await adapter.resolveRecordConflict(id: conflict.id,
                expectedGeneration: conflict.generation, choice: .keepLocal)
            XCTFail("A record choice cannot roll back the shared lifecycle order")
        } catch BigSyncRecordContractError.staleConflict { }
        XCTAssertEqual(initial.epoch, "E0")
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .useIncoming)
        XCTAssertEqual(initial.epoch, successor)
        XCTAssertTrue(initial.isDeleted)
        XCTAssertTrue(try adapter.unresolvedRecordConflicts().isEmpty)
        XCTAssertFalse(try adapter.exportPreservedRecordConflicts().isEmpty)
    }
''';s=s[:i]+extra+s[i:];p.write_text(s)
