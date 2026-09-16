from pathlib import Path

p = Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift')
s = p.read_text()
anchor = '/// Record identifiers prepared for one deletion attempt.'
s = s.replace(anchor, '''/// A comparison receipt admitted by the target transaction. Carry its exact
/// revision into both remaining acknowledgement phases; a rejected comparison
/// must never fall through to generation-only acknowledgement.
private struct RealmSwiftAcceptedComparisonReceipt: Sendable {
    let context: BigSyncRecordRebaseContext
    let revision: String
}

''' + anchor, 1)
old = '''    public func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {
        guard let realmProvider,
'''
new = '''    public func didUpload(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String]
    ) async throws {
        // Comparison-enabled records must retain their preparation evidence all
        // the way through acknowledgement. A legacy call cannot manufacture it
        // from current values (which may already contain a newer local edit).
        for record in savedRecords {
            if let realm = realmProvider?.targetReaderRealmPerSchemaName[record.recordType],
               BigSyncRecordBaseline.isEnabled(in: realm),
               recordRebaseContext != nil,
               let type = realmObjectClass(name: record.recordType),
               try recordRebasePolicy(for: type.init()) != .disabled {
                throw BigSyncRecordRebaseError.inconsistentReceipt(record.recordID.recordName)
            }
        }
        try await acknowledgeUploadReceipts(
            savedRecords: savedRecords, matchingGenerations: matchingGenerations,
            comparisonReceipts: [:]
        )
    }

    @BigSyncBackgroundActor
    private func acknowledgeUploadReceipts(
        savedRecords: [CKRecord],
        matchingGenerations: [String: String],
        comparisonReceipts: [String: RealmSwiftAcceptedComparisonReceipt]
    ) async throws {
        guard let realmProvider,
'''
assert s.count(old) == 1
s = s.replace(old, new, 1)
old = '''                    try Task.checkCancellation()
                    try save(record: record, for: syncedEntity)
                    syncedEntity.state = SyncedEntityState.synced.rawValue
'''
new = '''                    if let receipt = comparisonReceipts[record.recordID.recordName] {
                        guard let target = realmProvider.targetReaderRealmPerSchemaName[syncedEntity.entityType],
                              try comparisonReceiptIsCurrent(receipt,
                                recordName: record.recordID.recordName, in: target) else { continue }
                    }
                    try Task.checkCancellation()
                    try save(record: record, for: syncedEntity)
                    syncedEntity.state = SyncedEntityState.synced.rawValue
'''
assert s.count(old) == 1
s = s.replace(old, new, 1)
pos = s.index('    private func acknowledgeUploadReceipts(')
end = s.index('    public func preparedRecordDeletions(', pos)
section = s[pos:end]
old = '                            targetReaderRealm.delete(mutation)\n'
new = '''                            if let receipt = comparisonReceipts[recordName] {
                                guard try comparisonReceiptIsCurrent(receipt,
                                    recordName: recordName, in: targetReaderRealm) else { continue }
                            }
                            targetReaderRealm.delete(mutation)
'''
assert section.count(old) == 1
section = section.replace(old, new, 1)
s = s[:pos] + section + s[end:]
start = s.index('    @BigSyncBackgroundActor\n    public func didUpload(savedRecords: [CKRecord], matchingPreparedUploads prepared:')
assert s[start:].endswith('\n}\n')
s = s[:start] + '''    @BigSyncBackgroundActor
    private func comparisonReceiptIsCurrent(
        _ receipt: RealmSwiftAcceptedComparisonReceipt,
        recordName: String, in realm: Realm
    ) throws -> Bool {
        guard recordRebaseContext == receipt.context,
              BigSyncRecordBaseline.isEnabled(in: realm) else { return false }
        try receipt.context.validate(in: realm)
        if !realm.isInWriteTransaction { realm.refresh() }
        guard let current = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: recordName),
              !current.isComparisonInvalidated,
              current.namespace == receipt.context.namespace,
              current.revision == receipt.revision else { return false }
        return true
    }

    @BigSyncBackgroundActor
    public func didUpload(savedRecords: [CKRecord], matchingPreparedUploads prepared: [PreparedRecordUpload]) async throws {
        // Validate the complete response before any target/tracking mutation.
        // Name alone is not identity: zone and record type must also match.
        var preparedByID = [CKRecord.ID: PreparedRecordUpload]()
        for item in prepared {
            guard item.record.recordID.zoneID == recordZoneID,
                  preparedByID.updateValue(item, forKey: item.record.recordID) == nil else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(item.record.recordID.recordName)
            }
        }
        var seen = Set<CKRecord.ID>()
        var generations = [String: String]()
        var admittedRecords = [CKRecord]()
        var accepted = [String: RealmSwiftAcceptedComparisonReceipt]()
        typealias ReceiptItem = (saved: CKRecord, type: Object.Type, proof: BigSyncPreparedRecordBase)
        var groups = [String: (realm: Realm, items: [ReceiptItem])]()
        for saved in savedRecords {
            guard seen.insert(saved.recordID).inserted,
                  let item = preparedByID[saved.recordID],
                  item.record.recordType == saved.recordType else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            guard let proof = item.comparisonBase else {
                // Only genuinely non-comparison records use the old path.
                // A missing proof on an enabled model is not opt-out consent.
                if let realm = realmProvider?.targetReaderRealmPerSchemaName[saved.recordType],
                   BigSyncRecordBaseline.isEnabled(in: realm), recordRebaseContext != nil,
                   let type = realmObjectClass(name: saved.recordType),
                   try recordRebasePolicy(for: type.init()) != .disabled {
                    throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
                }
                generations[saved.recordID.recordName] = item.generation
                admittedRecords.append(saved)
                continue
            }
            // A superseded binding's receipt has no authority in this adapter.
            // In particular it must not be passed to the legacy fallback.
            guard proof.context == recordRebaseContext else { continue }
            guard let realm = realmProvider?.targetReaderRealmPerSchemaName[saved.recordType],
                  let type = realmObjectClass(name: saved.recordType),
                  BigSyncRecordBaseline.isEnabled(in: realm) else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            let savedFields = try BigSyncRecordFingerprint.fields(of: decodedComparisonObject(saved, type: type))
            guard savedFields == proof.fields else {
                throw BigSyncRecordRebaseError.inconsistentReceipt(saved.recordID.recordName)
            }
            let key = BigSyncMutationTrackingRegistry.identity(for: realm.configuration)
            groups[key, default: (realm, [])].items.append((saved, type, proof))
        }
        for key in groups.keys.sorted() {
            guard let group = groups[key] else { continue }
            let realm = group.realm
            for chunk in group.items.chunks(ofCount: 500) {
                try await realm.asyncWrite {
                    for item in chunk {
                        try Task.checkCancellation()
                        let proof = item.proof, saved = item.saved
                        let name = saved.recordID.recordName
                        guard !cancelSync, recordRebaseContext == proof.context else { throw CancellationError() }
                        try proof.context.validate(in: realm)
                        let base = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
                        // Resume the exact accepted receipt if the target base
                        // committed before tracking acknowledgement. Tag and
                        // payload must both match; a newer base is not a match.
                        let alreadyInstalled = base?.isComparisonInvalidated == false
                            && base?.namespace == proof.context.namespace
                            && base?.fieldDigests == proof.fields
                            && base?.serverChangeTag == saved.recordChangeTag
                        guard base?.revision == proof.revision || alreadyInstalled,
                              let pending = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name),
                              pendingMutationIsEligibleForActiveTransport(pending),
                              let id = getObjectIdentifier(recordName: name, entityType: item.type.className()),
                              let object = realm.object(ofType: item.type, forPrimaryKey: id),
                              (object as? SoftDeletable)?.isDeleted != true else { continue }
                        BigSyncRecordBaseline.install(recordName: name, namespace: proof.context.namespace,
                            fields: proof.fields, serverChangeTag: saved.recordChangeTag, in: realm)
                        guard let current = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name) else {
                            throw BigSyncRecordRebaseError.inconsistentReceipt(name)
                        }
                        accepted[name] = .init(context: proof.context, revision: current.revision)
                        generations[name] = preparedByID[saved.recordID]?.generation
                        admittedRecords.append(saved)
                    }
                }
            }
        }
        // Do not pass rejected records with a merely matching generation. Both
        // subsequent commit phases recheck the admitted comparison revision.
        try await acknowledgeUploadReceipts(
            savedRecords: admittedRecords, matchingGenerations: generations,
            comparisonReceipts: accepted
        )
    }
}
'''
p.write_text(s)
