
@objc(RebaseUnsupportedRow)
private final class RebaseUnsupportedRow: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    @Persisted(primaryKey: true) var id = "unsupported"
    @Persisted var related: RebaseRow?
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(RebaseInvalidLifetimeDeclaration)
private final class RebaseInvalidLifetimeDeclaration: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy {
        .lifetimeBundle(lifetimeField: "epoch", independentFields: ["epoch"])
    }
    @Persisted(primaryKey: true) var id = "invalid"
    @Persisted var epoch = "E0"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

@objc(RebaseInvalidSemanticDeclaration)
private final class RebaseInvalidSemanticDeclaration: Object, ChangeMetadataRecordable,
    BigSyncRecordRebasePolicyProviding, BigSyncInboundSemanticRecordValidating {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
    static func validateInboundSemanticRecord(_ record: CKRecord) throws {}
    @Persisted(primaryKey: true) var id = "semantic"
    @Persisted var text = "valid"
    @Persisted var createdAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var modifiedAt = Date(timeIntervalSinceReferenceDate: 1)
    @Persisted var explicitlyModifiedAt: Date?
    @Persisted var isDeleted = false
}

extension SyncRecordRebaseTests {
    @BigSyncBackgroundActor
    func testDeclaredRebasingCannotSilentlyFallBackForRelationships() async throws {
        let (adapter, realm) = try await fixture(extraTypes: [RebaseUnsupportedRow.self])
        do {
            try await deliver([record(adapter, type: RebaseUnsupportedRow.self, id: "unsupported")], to: adapter)
            XCTFail("ADMISSION: explicit unsupported rebasing must not fall back to whole-record replacement")
        } catch BigSyncRecordRebaseError.unsupportedField("related") {}
        realm.refresh()
        XCTAssertTrue(realm.objects(RebaseUnsupportedRow.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testInvalidLifetimeDeclarationFailsBeforeFirstImportCreatesBaseline() async throws {
        let (adapter, realm) = try await fixture(extraTypes: [RebaseInvalidLifetimeDeclaration.self])
        let incoming = record(adapter, type: RebaseInvalidLifetimeDeclaration.self, id: "invalid")
        incoming["epoch"] = "E1" as CKRecordValue
        do {
            try await deliver([incoming], to: adapter)
            XCTFail("ADMISSION: the lifetime field cannot also be independent, even before a local edit")
        } catch BigSyncRecordRebaseError.invalidPolicy {}
        realm.refresh()
        XCTAssertTrue(realm.objects(RebaseInvalidLifetimeDeclaration.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testSemanticPartialMergeDeclarationFailsRatherThanAppearingEnabled() async throws {
        let (adapter, realm) = try await fixture(extraTypes: [RebaseInvalidSemanticDeclaration.self])
        let incoming = record(adapter, type: RebaseInvalidSemanticDeclaration.self, id: "semantic")
        incoming["text"] = "valid incoming" as CKRecordValue
        do {
            try await deliver([incoming], to: adapter)
            XCTFail("ADMISSION: invalid semantic merge declaration must be visible to the integrator")
        } catch BigSyncRecordRebaseError.invalidPolicy {}
        realm.refresh()
        XCTAssertTrue(realm.objects(RebaseInvalidSemanticDeclaration.self).isEmpty)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testUnsupportedDeclaredUploadDoesNotReachTransportOrLoseJournal() async throws {
        let (adapter, realm) = try await fixture(extraTypes: [RebaseUnsupportedRow.self])
        let object = RebaseUnsupportedRow()
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        try await adapter.didFinishImport()
        let name = RebaseUnsupportedRow.className() + ".unsupported"
        let generation = try XCTUnwrap(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation)
        do {
            _ = try await adapter.prepareUploadBatch(limit: 10)
            XCTFail("ADMISSION: declared unsupported schema must not produce an unprotected upload")
        } catch BigSyncRecordRebaseError.unsupportedField("related") {}
        XCTAssertEqual(realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)?.generation, generation)
    }

    @BigSyncBackgroundActor
    func testUnknownBaseWithDifferentPendingPayloadFailsWithoutInventingAnAncestor() async throws {
        let (adapter, realm) = try await fixture()
        let object = RebaseRow()
        object.remoteField = "work never accepted here"
        try realm.write {
            realm.add(object)
            object.refreshChangeMetadata(explicitlyModified: true)
        }
        let before = try generation(realm)
        do {
            try await deliver([row(adapter, remote: "server value")], to: adapter)
            XCTFail("MISSING BASE: an absent comparison base cannot authorize losing unsent work")
        } catch BigSyncRecordRebaseError.missingBaseline(_) {}
        realm.refresh()
        XCTAssertEqual(object.remoteField, "work never accepted here")
        XCTAssertEqual(try generation(realm), before)
        XCTAssertTrue(realm.objects(BigSyncRecordBaseline.self).isEmpty)
    }

    @BigSyncBackgroundActor
    func testDifferentAccountCannotUseAStoredComparisonBase() async throws {
        let (adapter, realm) = try await fixture()
        try await deliver([row(adapter)], to: adapter)
        let value = try edit(realm) { $0.localField = "local retained" }
        let before = try generation(realm)
        let revision = try baseline(realm).revision
        try await adapter.activateReplicaBinding(accountScopeIdentifier: "other", replicaBindingGenerationIdentifier: "other-binding")
        do {
            try await deliver([row(adapter, remote: "other account record")], to: adapter)
            XCTFail("ACCOUNT: comparison evidence must remain bound to its writer context")
        } catch {}
        realm.refresh()
        XCTAssertEqual(value.remoteField, "remote-v0")
        XCTAssertEqual(value.localField, "local retained")
        XCTAssertEqual(try generation(realm), before)
        XCTAssertEqual(try baseline(realm).revision, revision)
    }
}
