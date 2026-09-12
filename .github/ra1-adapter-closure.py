#!/usr/bin/env python3
"""Apply the reviewed adapter delta once, preserving RA-1's selected source.

Preparation commits before qualification. This script is temporary and must not
be retained in the final candidate. It neither edits integration/default refs nor
runs the app or CloudKit service.
"""
from pathlib import Path
import hashlib
import subprocess

DONOR = 'f0a2851217e97502166959971c4dbb3dd134e941'
path = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
raw = path.read_bytes()
assert hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == '34f11c2c76c7a70ce24511152137c9442abb775e'
s = raw.decode()

def once(old, new):
    global s
    if s.count(old) != 1:
        raise RuntimeError('Changed adapter preimage: ' + old[:100])
    s = s.replace(old, new, 1)

once('enum RealmSwiftAdapterError: Error, LocalizedError {', '''/// A downloaded ordinary record changed without a durable local generation.
/// Fail the page for replay; never acknowledge unapplied payload as unchanged.
struct RealmSwiftInboundTargetChangedError: Error, Equatable, Sendable {
    let recordName: String
}

enum RealmSwiftAdapterError: Error, LocalizedError {''')
once('''                if property.type == PropertyType.object {
                    if let target = object[property.name] as? Object {''', '''                if property.type == PropertyType.object,
                   !property.isArray, !property.isSet, !property.isMap {
                    if let target = object[property.name] as? Object {''')
once('''            } else if value != nil {
                throw malformed("a UUID string")
            }
        } else if let asset = value as? CKAsset {''', '''            } else if value != nil {
                throw malformed("a UUID string")
            } else if property.isOptional {
                // Zone-change records are complete. An absent optional UUID
                // is a cleared value; required older-schema fields still keep
                // their compatibility default when absent.
                try Task.checkCancellation()
                object.setValue(nil, forKey: key)
            }
        } else if let asset = value as? CKAsset {''')
once(r'''                        logger.warning("Warning: Unsupported recordToUpload map property type \(property.type) for \(String(describing: type(of: object)))")''', '''                        throw RealmSwiftRemoteRecordDecodingError.malformedField(
                            recordName: syncedEntity.identifier,
                            propertyName: property.name,
                            expected: "a supported scalar Realm map for upload"
                        )''')
anchor = r'''                                            logger.info(
                                                "QSCloudKitSynchronizer >> Skipped downloaded record after a newer local mutation: \(candidate.syncedEntityID)"
                                            )'''
once(anchor, '''                                            guard currentMutationGeneration != nil else {
                                                // The ordinary target changed without a
                                                // journaled local winner. Replay instead of
                                                // persisting system fields/cursor over an
                                                // unapplied payload. RA-1 semantic winners
                                                // already bypass this selection branch.
                                                throw RealmSwiftInboundTargetChangedError(
                                                    recordName: candidate.syncedEntityID
                                                )
                                            }
''' + anchor)
path.write_text(s)

def donor(path):
    return subprocess.check_output(['git', 'show', DONOR + ':' + path])

review_tests = 'Tests/BigSyncKitTests/WorkerReviewReconciliationTests.swift'
assert not Path(review_tests).exists()
Path(review_tests).write_bytes(donor(review_tests))
path = Path('Tests/BigSyncKitTests/BigSyncKitTests.swift')
s = path.read_text()
d = donor(str(path)).decode()
for name in ['testReevaluationNonJournaledImportCollisionRetainsPageForReplay',
             'testReevaluationObjectCollectionsEncodeAndRoundTrip']:
    assert 'func ' + name not in s
    start = d.index('    @BigSyncBackgroundActor\n    func ' + name)
    end = d.find('\n    @BigSyncBackgroundActor', start + 30)
    assert end > start
    body = d[start:end].rstrip()
    assert body.endswith('}')
    s += '\n\nextension BigSyncKitTests {\n' + body + '\n}\n'
path.write_text(s)
subprocess.run(['git', 'diff', '--check'], check=True)
