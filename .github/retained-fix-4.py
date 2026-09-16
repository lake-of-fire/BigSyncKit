from pathlib import Path
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordBaseline.swift')
s=p.read_text();old='''    let binding: String

    func validate''';new='''    let binding: String
    let preservationNamespace: String

    init(namespace: String, account: String, binding: String,
         preservationNamespace: String? = nil) {
        self.namespace = namespace
        self.account = account
        self.binding = binding
        self.preservationNamespace = preservationNamespace ?? namespace
    }

    func validate''';assert s.count(old)==1;s=s.replace(old,new);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift');s=p.read_text();s=s.replace('''let parts = ["bigsync-note-conflict-copy-v1", context.account,''','''let parts = ["bigsync-note-conflict-copy-v1", context.preservationNamespace,''');old='''        if let existing = realm.object(ofType: type, forPrimaryKey: uuid) {
            let prior = try BigSyncRecordFingerprint.fields(of: existing)
            guard fieldNames.allSatisfy({ prior[$0] == fields[$0] }) else {
                throw BigSyncRecordContractError.staleConflict
            }
            // Never revive a recovery note the user already removed.
            return
        }''';new='''        if realm.object(ofType: type, forPrimaryKey: uuid) != nil {
            // The deterministic identity is the preservation receipt. The user
            // may have edited or deleted that ordinary recovery note since it
            // was created; redelivery cannot overwrite, revive or duplicate it.
            return
        }''';assert s.count(old)==1;s=s.replace(old,new);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift');s=p.read_text();old='''            account: account, binding: binding
        )''';new='''            account: account, binding: binding,
            preservationNamespace: parts.dropLast().map { "\($0.utf8.count):\($0)" }.joined()
        )''';assert s.count(old)==1;s=s.replace(old,new)
old='''            guard BigSyncRecordBaseline.isEnabled(in: object.realm!) else {
                throw BigSyncRecordContractError.missingEvidenceSchema(object.objectSchema.className)
            }
            _ = contract''';new='''            try requireRecordEvidenceSchema(in: object.realm!, entityType: object.objectSchema.className)
            guard recordRebaseContext != nil else {
                throw BigSyncRecordContractError.invalidDeclaration(object.objectSchema.className)
            }
            _ = contract''';assert s.count(old)==1;s=s.replace(old,new)
old='''                let semanticScopeIdentifier = (objectClass as?
                    BigSyncInboundSemanticRecordValidating.Type)?''';new='''                if objectClass is BigSyncRecordContractProviding.Type {
                    guard recordRebaseContext != nil,
                          let target = realmProvider.targetReaderRealmPerSchemaName[record.recordType] else {
                        throw BigSyncRecordContractError.invalidDeclaration(record.recordType)
                    }
                    try requireRecordEvidenceSchema(in: target, entityType: record.recordType)
                }
                let semanticScopeIdentifier = (objectClass as?
                    BigSyncInboundSemanticRecordValidating.Type)?''';assert s.count(old)==1;s=s.replace(old,new)
old='''        if let validator = objectType as? BigSyncInboundSemanticReplacementValidating.Type {
            guard (try? validator.inboundSemanticReplacementDisposition(
                record, existingObject: existingObject
            )) == .applyIncomingRecord else { return .notAdopted }
        }
        let remoteObject''';new='''        var preservesValidatedPredecessor = false
        if let validator = objectType as? BigSyncInboundSemanticReplacementValidating.Type {
            do {
                guard try validator.inboundSemanticReplacementDisposition(
                    record, existingObject: existingObject
                ) == .applyIncomingRecord else { return .notAdopted }
            } catch {
                guard pending != nil, let existingObject,
                      let predecessor = objectType as? BigSyncInboundPendingSemanticReplacementValidating.Type else {
                    throw error
                }
                try predecessor.validateInboundSemanticPredecessorOfPendingMutation(
                    record, existingObject: existingObject)
                preservesValidatedPredecessor = true
            }
        }
        let remoteObject''';assert s.count(old)==1;s=s.replace(old,new)
old='''        let decision = try BigSyncRecordReconciliationPlanner.plan(''';new='''        if preservesValidatedPredecessor {
            // This validated exception admits the observed server baseline,
            // never property-level replacement of a pending bound catalog.
            // Retiring its uncertain candidate also prevents a fetch/reprepare
            // loop that could otherwise starve unrelated upload work.
            _ = try applyComparisonFields([], record: record, object: object,
                isNew: false, objectIdentifier: objectIdentifier, local: local,
                remote: remote, pending: true, context: context, in: realm)
            if let submitted = matchingSubmission(recordName: name, context: context, in: realm) {
                realm.delete(submitted)
            }
            return .committed
        }
        let decision = try BigSyncRecordReconciliationPlanner.plan(''';assert s.count(old)==1;s=s.replace(old,new)
p.write_text(s)
p=Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+RecordMutations.swift');s=p.read_text();old='''                if !observations.isEmpty {
                    let outcomes: [InboundLiveResult]''';new='''                if !observations.isEmpty {
                    for candidate in uncertain where observations.contains(where: {
                        $0.recordID == candidate.record.recordID
                    }) {
                        try retryBudget.register(.init(recordID: candidate.record.recordID,
                            generation: candidate.generation),
                            maximumPerGeneration: Self.maximumHandledRecordRetries,
                            maximumPerDrain: Self.maximumHandledRetriesPerDrain)
                    }
                    let outcomes: [InboundLiveResult]''';assert s.count(old)==1;s=s.replace(old,new);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordContract.swift');s=p.read_text();s += '''
public extension BigSyncRecordContract {
    /// Validate declarations at canonical configuration creation, before any
    /// Realm is opened. This never installs an account or mutation identity.
    static func validate(configuration: Realm.Configuration) throws {
        guard let types = configuration.objectTypes else {
            throw BigSyncRecordContractError.missingEvidenceSchema("explicit objectTypes")
        }
        let names = Set(types.map { $0.className() })
        for type in types.compactMap({ $0 as? Object.Type })
            where type is BigSyncRecordContractProviding.Type {
            guard names.contains(BigSyncPendingMutation.className()),
                  BigSyncLocalRecordEvidence.objectTypes.allSatisfy({ names.contains($0.className()) }) else {
                throw BigSyncRecordContractError.missingEvidenceSchema(type.className())
            }
            let object = type.init()
            guard BigSyncRecordFingerprint.supports(object) else {
                throw BigSyncRecordContractError.invalidDeclaration(type.className())
            }
            _ = try BigSyncCompiledRecordContract.compile(object)
        }
    }
}
''';p.write_text(s)
p=Path('Tests/BigSyncKitTests/SyncRetainedRecordContractTests.swift');s=p.read_text();s=s.replace('''config.objectTypes = [RetainedContractRow.self, ContractRecoveryNote.self,''','''config.objectTypes = [RetainedContractRow.self, BoundContractControl.self, ContractRecoveryNote.self,''')
anchor='''final class SyncRetainedRecordContractTests: XCTestCase {''';model='''@objc(BoundContractControl)
private final class BoundContractControl: Object, ChangeMetadataRecordable,
    BigSyncRecordContractProviding, BigSyncInboundSemanticReplacementValidating,
    BigSyncInboundPendingSemanticReplacementValidating {
    static let bigSyncRecordContract = BigSyncRecordContract(
        policy: .lifetimeBundle(lifetimeField: "epoch", independentFields: []), deletion: .retained)
    @Persisted(primaryKey: true) var id = "control"
    @Persisted var epoch = "E0"
    @Persisted var digest = ""
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
    static func validateInboundSemanticReplacement(_ record: CKRecord, existingObject: Object?) throws {
        guard let local = existingObject as? BoundContractControl else { return }
        if record["epoch"] as? String == local.epoch, !local.digest.isEmpty,
           record["digest"] as? String != local.digest { throw CocoaError(.coderReadCorrupt) }
    }
    static func validateInboundSemanticPredecessorOfPendingMutation(
        _ record: CKRecord, existingObject: Object
    ) throws {
        guard let local = existingObject as? BoundContractControl,
              record["epoch"] as? String == local.epoch,
              record["digest"] as? String == "", !local.digest.isEmpty else {
            throw CocoaError(.coderReadCorrupt)
        }
    }
}

''';assert s.count(anchor)==1;s=s.replace(anchor,model+anchor)
extra='''
    @BigSyncBackgroundActor
    func testAdoptedInboundCannotSilentlyUseWrongMergePolicy() async throws {
        let (adapter, realm) = try await fixture()
        adapter.mergePolicy = .server
        do {
            _ = try await deliver([record(adapter)], to: adapter)
            XCTFail("An adopted model cannot silently use legacy merging")
        } catch BigSyncRecordContractError.invalidDeclaration { }
        XCTAssertTrue(realm.objects(RetainedContractRow.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testCanonicalContractValidationRequiresEveryEvidenceTable() async throws {
        let (_, realm) = try await fixture()
        try BigSyncRecordContract.validate(configuration: realm.configuration)
        var partial = realm.configuration
        partial.objectTypes = partial.objectTypes?.filter { $0.className() != BigSyncRecordSubmission.className() }
        XCTAssertThrowsError(try BigSyncRecordContract.validate(configuration: partial))
    }

    @BigSyncBackgroundActor
    func testBoundPendingPredecessorAcceptsOnlyBaselineAndRetiresUncertainty() async throws {
        let (adapter, realm) = try await fixture()
        let control = BoundContractControl()
        try realm.write { realm.add(control); control.refreshChangeMetadata(explicitlyModified: true) }
        try await adapter.didFinishImport()
        let first = try await adapter.prepareUploadBatch(limit: 10)
        let unbound = try BigSyncRecordPayload.encode(try XCTUnwrap(first.records.first))
        try realm.write { control.digest = "bound"; control.refreshChangeMetadata(explicitlyModified: true) }
        _ = try await deliver([BigSyncRecordPayload.decode(unbound)], to: adapter)
        XCTAssertEqual(control.digest, "bound")
        XCTAssertTrue(realm.objects(BigSyncRecordSubmission.self).isEmpty)
        let bound = try await adapter.prepareUploadBatch(limit: 10)
        XCTAssertEqual(bound.records.first?["digest"] as? String, "bound")
        try await adapter.acknowledgeUploadedRecords(bound.records, from: bound)
        try await requireQuiet(adapter)
    }

    @BigSyncBackgroundActor
    func testRecoveryCopyRedeliveryPreservesUserEditsToTheCopy() async throws {
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
        try realm.write {
            copy.text = "user refined recovery copy"
            try BigSyncRecordEvidenceStore(context: context, realm: realm)
                .preserveNoteCopy(losingObject: note, record: source, fieldNames: ["text"])
        }
        XCTAssertEqual(copy.text, "user refined recovery copy")
        XCTAssertEqual(realm.objects(ContractRecoveryNote.self).count, 2)
    }
''';i=s.rfind('\n}');s=s[:i]+extra+s[i:];p.write_text(s)
