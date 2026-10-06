    @BigSyncBackgroundActor
    struct PublicationRestorationInspection {
        fileprivate let adapter: RealmSwiftAdapter
        fileprivate let persistenceConfiguration: Realm.Configuration
        fileprivate let targetConfigurations: [Realm.Configuration]

        /// Non-suspending final inspection, after account revalidation. These
        /// views never become the operational adapter's setup-completion flag.
        func matches(
            _ evidence: BigSyncDurablePublicationEvidence,
            containerIdentifier: String,
            databaseScope: CKDatabase.Scope
        ) throws -> Bool {
            // Drain Objective-C autoreleases here as well as Swift references.
            // Inspection must not retain old snapshots or prolong Realm
            // handles beyond the non-suspending local cutoff.
            return try autoreleasepool {
                try Task.checkCancellation()
                guard adapter.recordZoneID.ownerName == evidence.zoneOwnerName,
                      adapter.recordZoneID.zoneName == evidence.zoneName else {
                    return false
                }
                // Recheck existence/version after suspended account work, before
                // any open. No operational adapter setup, migration callbacks,
                // compaction, seed copying, or model mutations occur here.
                for configuration in [persistenceConfiguration] + targetConfigurations {
                    if configuration.inMemoryIdentifier == nil {
                        guard let url = configuration.fileURL,
                              FileManager.default.fileExists(atPath: url.path),
                              (try? schemaVersionAtURL(url)) == configuration.schemaVersion
                        else { return false }
                    }
                }
                let livePersistenceRealm = try Realm(configuration: persistenceConfiguration)
                for configuration in targetConfigurations {
                    let liveRealm = try Realm(configuration: configuration)
                    // A coordinated open can reuse a handle held by another
                    // writer. Read its committed base, not provisional debt.
                    // An immutable read-only configuration is already stable.
                    let realm = configuration.readOnly ? liveRealm
                        : adapter.committedRealmReadSnapshot(in: liveRealm)
                    if let binding = evidence.replicaBindingGenerationIdentifier {
                        let parts = [evidence.accountScopeIdentifier, containerIdentifier, String(databaseScope.rawValue),
                                     evidence.zoneOwnerName, evidence.zoneName, binding]
                        let context = BigSyncRecordRebaseContext(
                            namespace: parts.map { "\($0.utf8.count):\($0)" }.joined(),
                            account: evidence.accountScopeIdentifier, binding: binding,
                            preservationNamespace: parts.dropLast().map { "\($0.utf8.count):\($0)" }.joined())
                        let inspection = try adapter.inspectRecordEvidence(in: realm, context: context)
                        guard inspection.isConsistent, !inspection.hasSubmissionDebt else { return false }
                    } else if adapter.modelTypes.contains(where: { entry in
                        entry.value is BigSyncRecordContractProviding.Type
                            && !adapter.excludedClassNames.contains(entry.key)
                            && realm.schema.objectSchema.contains(where: { $0.className == entry.key })
                    }) {
                        // Adopted comparison evidence always has a concrete
                        // binding. An unbound legacy receipt cannot certify it.
                        return false
                    }
                    for mutation in realm.objects(BigSyncPendingMutation.self) {
                        guard adapter.isOwnedEntityType(mutation.entityType),
                              mutation.replicaBindingGenerationIdentifier
                                == evidence.replicaBindingGenerationIdentifier
                        else { continue }
                        if adapter.accountScopePropertyByClassName[mutation.entityType] == nil
                            || mutation.accountScopeIdentifier == evidence.accountScopeIdentifier {
                            return false
                        }
                    }
                }
                // Cursor, epoch and pending tracking state must come from one
                // committed version, including after refresh callback reentry.
                let persistenceRealm = persistenceConfiguration.readOnly
                    ? livePersistenceRealm
                    : adapter.committedRealmReadSnapshot(in: livePersistenceRealm)
                let rebuild = persistenceRealm.object(
                    ofType: RebuildProvenanceState.self,
                    forPrimaryKey: RebuildProvenanceState.primaryKeyValue
                )
                guard rebuild?.isActive != true,
                      (rebuild?.epoch ?? 0) == evidence.changeFeedEpoch else {
                    return false
                }
                for entity in persistenceRealm.objects(SyncedEntity.self) {
                    guard adapter.isOwnedEntityType(entity.entityType),
                          entity.pendingReplicaBindingGenerationIdentifier
                            == evidence.replicaBindingGenerationIdentifier else { continue }
                    switch entity.entityState {
                    case .new, .changed, .deletedLocally:
                        return false
                    default:
                        break
                    }
                }
                guard let token = persistenceRealm.objects(ServerToken.self).first?.token
                else { return false }
                return CloudKitSynchronizer.makeConsumedServerBoundaryIdentifier(
                    containerIdentifier: containerIdentifier,
                    databaseScope: databaseScope,
                    accountScopeIdentifier: evidence.accountScopeIdentifier,
                    replicaBindingGenerationIdentifier:
                        evidence.replicaBindingGenerationIdentifier,
                    recordZoneID: adapter.recordZoneID,
                    changeFeedEpoch: evidence.changeFeedEpoch,
                    cursorData: token
                ) == evidence.consumedServerBoundaryIdentifier
            }
        }
    }
