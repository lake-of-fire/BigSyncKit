from pathlib import Path
p = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
s = p.read_text()
old = '''        try receipt.context.validate(in: realm)
        if !realm.isInWriteTransaction { realm.refresh() }
'''
new = '''        if realm.isInWriteTransaction {
            try receipt.context.validate(in: realm)
        } else {
            // The tracking phase observes the target Realm; it must not call
            // the public write-transaction-only mutation verification API.
            realm.refresh()
            guard let identity = BigSyncMutationTrackingRegistry.currentMutationJournalIdentity(in: realm),
                  !identity.installationIdentifier.isEmpty,
                  identity.replicaBindingGenerationIdentifier == receipt.context.binding else {
                throw CancellationError()
            }
        }
'''
assert s.count(old) == 1
p.write_text(s.replace(old, new))
