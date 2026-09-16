from pathlib import Path
p = Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift')
s = p.read_text()
assert s.count('.resolveRecordConflict(conflict.id,') == 2
assert s.count('.resolveRecordConflict(refreshed.id,') == 1
s = s.replace('.resolveRecordConflict(conflict.id,', '.resolveRecordConflict(id: conflict.id,')
s = s.replace('.resolveRecordConflict(refreshed.id,', '.resolveRecordConflict(id: refreshed.id,')
p.write_text(s)
