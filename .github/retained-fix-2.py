from pathlib import Path
p = Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift')
s = p.read_text()
assert s.count('.resolveRecordConflict(') == 3
assert '.resolveRecordConflict(id:' not in s
s = s.replace('.resolveRecordConflict(', '.resolveRecordConflict(id: ')
p.write_text(s)
