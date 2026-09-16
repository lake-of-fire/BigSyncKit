from pathlib import Path
import sys

stage = sys.argv[1]
path = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
s = path.read_text()
def replace(old, new):
    global s
    assert s.count(old) == 1, (stage, old[:160], s.count(old))
    s = s.replace(old, new)

if stage == 'tests':
    replace('''        id: String, expectedGeneration: String, choice: BigSyncRecordConflictChoice
    ) async throws {''', '''        id: String, expectedGeneration: String, choice: BigSyncRecordConflictChoice,
        validateAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void = {}
    ) async throws {''')
    replace('''    public func refreshRecordConflict(_ conflictID: String) async throws {''', '''    public func refreshRecordConflict(
        _ conflictID: String,
        validateAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void = {}
    ) async throws {''')
    replace('''    func discardResolvedRecordConflictArchives() async throws {''', '''    func discardResolvedRecordConflictArchives(
        validateAuthority: @BigSyncBackgroundActor @Sendable () throws -> Void = {}
    ) async throws {''')
    tests = Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift')
    t = tests.read_text()
    assert t.rstrip().endswith('}')
    additions = '''
    @BigSyncBackgroundActor
    private final class RecoveryAuthority {
        enum Failure: Error { case revoked }
        var calls = 0
        let revokeAtTransaction: Bool
        init(revokeAtTransaction: Bool = true) {
            self.revokeAtTransaction = revokeAtTransaction
        }
        func validate() throws {
            calls += 1
            if revokeAtTransaction && calls >= 2 { throw Failure.revoked }
        }
    }

    @BigSyncBackgroundActor
    private func unbasedRecoveryFixture() async throws -> (RealmSwiftAdapter, Realm, RetainedContractRow, BigSyncRecordConflictSnapshot) {
        let (adapter, realm) = try await fixture()
        let object = RetainedContractRow()
        try realm.write {
            realm.add(object)
            object.title = "mine"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        _ = try await deliver([record(adapter, title: "theirs")], to: adapter)
        return (adapter, realm, object, try XCTUnwrap(try adapter.unresolvedRecordConflicts().first))
    }

    @BigSyncBackgroundActor
    func testRevokedResolutionAuthorityCannotCommitEitherChoice() async throws {
        for choice: BigSyncRecordConflictChoice in [.keepLocal, .useIncoming] {
            let (adapter, realm, object, conflict) = try await unbasedRecoveryFixture()
            let authority = RecoveryAuthority()
            let pending = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first).generation
            do {
                try await adapter.resolveRecordConflict(id: conflict.id,
                    expectedGeneration: conflict.generation, choice: choice,
                    validateAuthority: { try authority.validate() })
                XCTFail("Authority was revoked after entry and before the target transaction")
            } catch RecoveryAuthority.Failure.revoked { }
            XCTAssertEqual(authority.calls, 2)
            XCTAssertEqual(object.title, "mine")
            XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
            XCTAssertEqual(try adapter.unresolvedRecordConflicts().map(\\.id), [conflict.id])
            XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
        }
    }

    @BigSyncBackgroundActor
    func testRevokedRefreshAuthorityCannotRetireOrReplaceEvidence() async throws {
        let (adapter, realm, object, conflict) = try await unbasedRecoveryFixture()
        try realm.write {
            object.title = "new typing"
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let pending = try XCTUnwrap(realm.objects(BigSyncPendingMutation.self).first).generation
        let authority = RecoveryAuthority()
        do {
            try await adapter.refreshRecordConflict(conflict.id,
                validateAuthority: { try authority.validate() })
            XCTFail("Refreshing evidence must revalidate authority inside its transaction")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertEqual(authority.calls, 2)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().first?.id, conflict.id)
        XCTAssertEqual(try adapter.unresolvedRecordConflicts().first?.generation, conflict.generation)
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).first?.generation, pending)
        XCTAssertEqual(object.title, "new typing")
    }

    @BigSyncBackgroundActor
    func testRevokedPruneAuthorityCannotDeleteRetainedArchives() async throws {
        let (adapter, realm, _, conflict) = try await unbasedRecoveryFixture()
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal)
        let before = try XCTUnwrap(realm.objects(BigSyncRecordConflict.self).first).localPayload
        let authority = RecoveryAuthority()
        do {
            try await adapter.discardResolvedRecordConflictArchives(
                validateAuthority: { try authority.validate() })
            XCTFail("Archive cleanup must revalidate authority inside its transaction")
        } catch RecoveryAuthority.Failure.revoked { }
        XCTAssertEqual(authority.calls, 2)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).count, 1)
        XCTAssertEqual(realm.objects(BigSyncRecordConflict.self).first?.localPayload, before)
        XCTAssertFalse(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testCurrentRecoveryAuthorityCanResolveAndDrain() async throws {
        let (adapter, _, object, conflict) = try await unbasedRecoveryFixture()
        let authority = RecoveryAuthority(revokeAtTransaction: false)
        try await adapter.resolveRecordConflict(id: conflict.id,
            expectedGeneration: conflict.generation, choice: .keepLocal,
            validateAuthority: { try authority.validate() })
        XCTAssertEqual(authority.calls, 2)
        XCTAssertEqual(object.title, "mine")
        let batch = try await adapter.prepareUploadBatch(limit: 20)
        try await adapter.acknowledgeUploadedRecords(batch.records, from: batch)
        try await requireQuiet(adapter)
    }
'''
    tests.write_text(t.rstrip()[:-1] + additions + '}\n')
elif stage == 'fix':
    # Recheck the caller's lease after the Realm write wait. Never hold an
    # account lock across an await or acquire a session lock in the callback.
    start = s.index('    public func resolveRecordConflict(')
    end = s.index('\n}\n\nextension RealmSwiftAdapter {', start)
    block = s[start:end]
    old = '    ) async throws {\n        guard let context'
    assert block.count(old) == 1
    block = block.replace(old, '    ) async throws {\n        try validateAuthority()\n        guard let context')
    old = '            try await realm.asyncWrite {\n                try context.validate(in: realm)'
    assert block.count(old) == 1
    block = block.replace(old, '            try await realm.asyncWrite {\n                try validateAuthority()\n                try context.validate(in: realm)')
    s = s[:start] + block + s[end:]
    for function in ['refreshRecordConflict', 'discardResolvedRecordConflictArchives']:
        start = s.index(('    public func ' if function == 'refreshRecordConflict' else '    func ') + function + '(')
        end = s.index('\n    }\n}', start) + len('\n    }')
        block = s[start:end]
        old = '    ) async throws {\n'
        assert block.count(old) == 1
        block = block.replace(old, old + '        try validateAuthority()\n')
        old = '            try await realm.asyncWrite {\n'
        assert block.count(old) == 1
        block = block.replace(old, old + '                try validateAuthority()\n')
        s = s[:start] + block + s[end:]
    old = '    public func resolveRecordConflict(\n'
    assert s.count(old) == 1
    s = s.replace(old, '''    /// Application callers supply their captured account-lease validator.
    /// It is checked at entry and again inside the final target transaction,
    /// after any Realm write wait. Adapter namespace/generation checks remain
    /// independent; this callback must not suspend or acquire a session lock.
    public func resolveRecordConflict(
''')
else:
    raise ValueError(stage)
path.write_text(s)
