#!/usr/bin/env python3
from pathlib import Path
import re
import subprocess

OTHER = 'b85d775d1c27b36acadc5f122477dba50a0fc39c'
BRANCH = 'fix/hotfix-findings-union-20260912'


def run(*args, check=True, capture=False):
    return subprocess.run(
        ['git', *args], check=check,
        text=True, capture_output=capture
    )


def text_at(ref, path):
    return subprocess.check_output(['git', 'show', f'{ref}:{path}'], text=True)


def replace_once(text, old, new, label):
    if text.count(old) != 1:
        raise RuntimeError(f'{label}: expected one source boundary, found {text.count(old)}')
    return text.replace(old, new, 1)


run('config', 'user.name', 'github-actions[bot]')
run('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
run('fetch', 'origin', OTHER)

# Preserve normal ancestry: the final source has both previously qualified hotfix
# lines as parents. Resolve divergent repair files in favor of the sync-closure
# parent, then explicitly compose the restoration-delivery addition below.
if run('merge-base', '--is-ancestor', OTHER, 'HEAD', check=False).returncode != 0:
    result = run('merge', '--no-commit', '--no-ff', OTHER, check=False)
    if result.returncode != 0:
        conflicts = subprocess.check_output(
            ['git', 'diff', '--name-only', '--diff-filter=U'], text=True
        ).splitlines()
        for path in conflicts:
            if path.startswith('.github/') or path.startswith('Sources/') or path.startswith('Tests/'):
                run('checkout', '--ours', '--', path)
                run('add', '--', path)
            else:
                raise RuntimeError('Unreviewed merge conflict: ' + path)

    # The sync-closure branch already has correct current-run cancellation and
    # interrupted-migration preparation. Do not replace them with the divergent
    # register-closure implementations. Only compose the missing restoration
    # delivery fence and its behavioral tests.
    cancellation = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Cancellation.swift')
    if cancellation.exists():
        run('rm', '-f', '--', str(cancellation))

    restoration_path = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+PublicationRestoration.swift')
    restoration_path.write_text(text_at(OTHER, str(restoration_path)))
    run('add', '--', str(restoration_path))

    actor_path = Path('Sources/BigSyncKit/QSSynchronizer/BigSyncBackgroundActor.swift')
    actor = actor_path.read_text()
    old = '''                    let evidence = try await synchronizer
                        .restoredDurablePublicationEvidence()
#if DEBUG
                    self.cloudKitE2ELastRestoredPublicationEvidence = evidence
#endif
                    try await restorationHandler(evidence)'''
    new = '''                    try await synchronizer.restoreDurablePublicationEvidence(
                        deliveringTo: { evidence in
#if DEBUG
                            await self.recordRestoredPublicationEvidence(evidence)
#endif
                            try await restorationHandler(evidence)
                        }
                    )'''
    if old in actor:
        actor = replace_once(actor, old, new, 'restoration worker delivery')
    helper_anchor = '''    /// Waits only for configuration's pre-sync restoration task. It does not'''
    helper = '''    @BigSyncBackgroundActor
    private func recordRestoredPublicationEvidence(
        _ evidence: BigSyncDurablePublicationEvidence?
    ) {
        cloudKitE2ELastRestoredPublicationEvidence = evidence
    }

'''
    if 'private func recordRestoredPublicationEvidence(' not in actor:
        actor = replace_once(actor, helper_anchor, helper + helper_anchor,
                             'restoration debug recorder')
    actor_path.write_text(actor)
    run('add', '--', str(actor_path))

    fencing_path = Path('Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift')
    fencing = fencing_path.read_text()
    other_fencing = text_at(OTHER, str(fencing_path))
    marker = 'private actor ClosureRestorationGate {'
    if marker not in fencing:
        suffix = other_fencing[other_fencing.index(marker):]
        fencing = fencing.rstrip() + '\n\n' + suffix
        fencing_path.write_text(fencing)
        run('add', '--', str(fencing_path))

    run('commit', '-m', 'merge: compose restoration delivery with migration-safe hotfix closure')

# Automatic default conflict selection must retransmit the already-authored local
# value without turning that retransmission into a fresh user edit timestamp.
protocol_path = Path('Sources/BigSyncKit/RealmSwift/SyncedEntityProtocol.swift')
protocol = protocol_path.read_text()
if 'journalCurrentValuePreservingChangeMetadata' not in protocol:
    anchor = '''    func refreshChangeMetadata(explicitlyModified: Bool, at timestamp: Date) {
        modifiedAt = timestamp
        if explicitlyModified {
            explicitlyModifiedAt = timestamp
            recordBigSyncMutation(at: timestamp)
        }
    }
'''
    replacement = anchor + '''
    /// BigSync has already selected the complete existing value as the winner.
    /// Queue that value for retransmission without manufacturing a later user
    /// edit clock. Custom delegates that mutate the object continue to use the
    /// ordinary explicit refresh path in RealmSwiftAdapter.
    internal func journalCurrentValuePreservingChangeMetadata(at timestamp: Date) {
        recordBigSyncMutation(at: timestamp)
    }
'''
    protocol = replace_once(protocol, anchor, replacement,
                            'preserving journal helper')
    protocol_path.write_text(protocol)

adapter_path = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
adapter = adapter_path.read_text()
old = '''                changeMetadata.refreshChangeMetadata(
                    explicitlyModified: true,
                    at: Date()
                )'''
new = '''                if delegate != nil {
                    // A custom delegate may have intentionally changed the
                    // local object while deciding to keep it. Preserve the
                    // historical authoring behavior for that model-owned merge.
                    changeMetadata.refreshChangeMetadata(
                        explicitlyModified: true,
                        at: Date()
                    )
                } else {
                    // The built-in timestamp policy selected an unchanged,
                    // already-authored local value. Retransmit it under a new
                    // journal generation without inventing a new conflict clock.
                    changeMetadata.journalCurrentValuePreservingChangeMetadata(
                        at: Date()
                    )
                }'''
if old in adapter:
    adapter = replace_once(adapter, old, new, 'automatic local winner clock')
adapter_path.write_text(adapter)

review_path = Path('Tests/BigSyncKitTests/WorkerReviewReconciliationTests.swift')
review = review_path.read_text()
if 'testReviewAutomaticLocalWinnerPreservesConflictClockAndLaterRemoteEditWins' not in review:
    review += r'''

extension WorkerReviewReconciliationTests {
    @BigSyncBackgroundActor
    func testReviewAutomaticLocalWinnerPreservesConflictClockAndLaterRemoteEditWins() async throws {
        let (adapter, realm) = try await fixture()
        let t1 = Date(timeIntervalSinceReferenceDate: 1_000)
        let t2 = Date(timeIntervalSinceReferenceDate: 2_000)
        let t3 = Date(timeIntervalSinceReferenceDate: 3_000)
        let local = WorkerReviewReceiver()
        local.id = "automatic-local-winner"
        local.payload = "local-t2"
        local.createdAt = t2
        local.modifiedAt = t2
        local.explicitlyModifiedAt = t2
        try await realm.asyncWrite {
            realm.add(local)
            local.refreshChangeMetadata(explicitlyModified: true, at: t2)
        }
        try await adapter.didFinishImport()
        let authored = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(authored.records.count, 1)
        try await adapter.acknowledgeUploadedRecords(authored.records, from: authored)
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)

        func incoming(_ payload: String, at timestamp: Date) -> CKRecord {
            let record = CKRecord(
                recordType: WorkerReviewReceiver.className(),
                recordID: .init(
                    recordName: WorkerReviewReceiver.className() + "." + local.id,
                    zoneID: adapter.recordZoneID
                )
            )
            record["payload"] = payload as CKRecordValue
            record["createdAt"] = t1 as CKRecordValue
            record["modifiedAt"] = timestamp as CKRecordValue
            record["explicitlyModifiedAt"] = timestamp as CKRecordValue
            record["isDeleted"] = false as CKRecordValue
            return record
        }

        _ = try await adapter.saveChanges(
            in: [incoming("remote-t1", at: t1)],
            forceSave: true
        )
        realm.refresh()
        XCTAssertEqual(local.payload, "local-t2")
        XCTAssertEqual(local.modifiedAt, t2,
                       "Automatic retransmission must not mint a newer modifiedAt")
        XCTAssertEqual(local.explicitlyModifiedAt, t2,
                       "Automatic retransmission must not mint a newer explicit edit clock")
        XCTAssertEqual(realm.objects(BigSyncPendingMutation.self).count, 1)

        try await adapter.didFinishImport()
        let retransmission = try await adapter.prepareUploadBatch(limit: 10)
        let retransmitted = try XCTUnwrap(retransmission.records.first)
        XCTAssertEqual(retransmitted["payload"] as? String, "local-t2")
        XCTAssertEqual(retransmitted["modifiedAt"] as? Date, t2)
        XCTAssertEqual(retransmitted["explicitlyModifiedAt"] as? Date, t2)
        try await adapter.acknowledgeUploadedRecords(
            retransmission.records, from: retransmission
        )
        realm.refresh()
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)

        _ = try await adapter.saveChanges(
            in: [incoming("remote-t3", at: t3)],
            forceSave: true
        )
        realm.refresh()
        XCTAssertEqual(local.payload, "remote-t3",
                       "A genuinely later edit must outrank the original local T2 authoring clock")
        XCTAssertEqual(local.modifiedAt, t3)
        XCTAssertEqual(local.explicitlyModifiedAt, t3)
        XCTAssertTrue(realm.objects(BigSyncPendingMutation.self).isEmpty)
    }
}
'''
    review_path.write_text(review)

run('diff', '--check')
run('add', '--', str(protocol_path), str(adapter_path), str(review_path))
if run('diff', '--cached', '--quiet', check=False).returncode != 0:
    run('commit', '-m', 'fix: preserve automatic local-winner conflict clocks')

# Qualification uses a negative control for the newly repaired timestamp boundary.
qual_path = Path('.github/qualify_hotfix_closure.py')
qual = qual_path.read_text()
if "negative-clock-inflation" not in qual:
    marker = "full = run('full', ['swift', 'test'], env=dict(os.environ, BIGSYNC_RUN_MUTATION_BENCHMARK='1'))"
    injection = r'''if focused:
    source = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
    original = source.read_text()
    good = ''' + '"""' + '''                    changeMetadata.journalCurrentValuePreservingChangeMetadata(
                        at: Date()
                    )''' + '"""' + r'''
    bad = ''' + '"""' + '''                    changeMetadata.refreshChangeMetadata(
                        explicitlyModified: true,
                        at: Date()
                    )''' + '"""' + r'''
    assert original.count(good) == 1
    try:
        source.write_text(original.replace(good, bad))
        run('negative-clock-inflation', ['swift', 'test', '--filter',
            'testReviewAutomaticLocalWinnerPreservesConflictClockAndLaterRemoteEditWins'],
            negative_marker='Automatic retransmission must not mint')
    finally:
        source.write_text(original)
'''
    qual = replace_once(qual, marker, injection + marker,
                        'clock negative control insertion')
    qual = qual.replace(
        "sys.exit(0 if len(results) == 15 and all(result['passed'] for result in results) else 1)",
        "sys.exit(0 if len(results) == 16 and all(result['passed'] for result in results) else 1)"
    )
    qual_path.write_text(qual)
    run('add', '--', str(qual_path))
    run('commit', '-m', 'test: qualify composed restoration and conflict-clock repairs')

run('diff', '--exit-code')
run('push', 'origin', 'HEAD:refs/heads/' + BRANCH)
print(subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
