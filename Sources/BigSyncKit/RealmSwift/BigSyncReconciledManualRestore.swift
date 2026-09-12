import Foundation
import RealmSwift

public extension BigSyncClientIdentity {
    /// Completes a caller-reconciled manual restore under the existing exclusive
    /// identity lease. `replacement` must install the final Realm files and, for
    /// every listed type, preserve only independently known current values with
    /// isAwaitingRecoveryEvidence == false. Backup-only values must be withheld.
    ///
    /// This does not compare backups or select history. Only the caller possessing
    /// the preserved originals can establish that distinction. Do not use this
    /// overload for an automatic/raw backup, or before the final copy normalizer.
    /// Listed types must implement the existing restore and outbound validators.
    ///
    /// After the new installation/binding is published, admitted values acquire
    /// ordinary unchanged-repair journals before the durable restore intent is
    /// released. The actual backupRestore adapter then preserves those generations
    /// while withholding copied values and retiring copied journals as before.
    /// Failure retains the exact handoff for retry with the same transaction ID.
    @discardableResult
    func withReconciledManualBackupRestore(
        transactionIdentifier: UUID,
        configurations: [Realm.Configuration],
        reconciledObjectTypes: [Object.Type],
        _ replacement: () throws -> Void,
        rollback: () throws -> Void = {}
    ) throws -> BigSyncManualBackupRestoreReceipt {
        guard !configurations.isEmpty, !reconciledObjectTypes.isEmpty else {
            throw BigSyncMutationJournalError.objectUnavailable
        }
        for type in reconciledObjectTypes {
            guard type is BigSyncRestoredObjectRecovering.Type,
                  type is BigSyncOutboundSemanticObjectValidating.Type,
                  type is ChangeMetadataRecordable.Type else {
                throw BigSyncMutationJournalError.unregisteredModel(type.className())
            }
        }
        return try withManualBackupRestore(
            transactionIdentifier: transactionIdentifier,
            replacement,
            rollback: rollback,
            sentinelPublisher: nil,
            beforeCompletingHandoff: { receipt in
                let binding = try prepareReplicaBindingGenerationIdentifier(
                    forPublishedInstallation: receipt.newInstallationIdentifier
                )
                let identity = BigSyncMutationJournalIdentity(
                    installationIdentifier: receipt.newInstallationIdentifier,
                    replicaBindingGenerationIdentifier: binding
                )
                // This synchronous closure still owns the exclusive process
                // lease and durable intent. Permit only its nested journal
                // provider reads; all other processes remain fenced. On failure
                // the cache must not bypass the still-pending durable intent.
                BigSyncClientIdentityLeaseRegistry.publishInstallationIdentifier(
                    identity.installationIdentifier, at: leaseURL
                )
                defer {
                    BigSyncClientIdentityLeaseRegistry.invalidateInstallationIdentifier(at: leaseURL)
                }
                var opened = Set<String>()
                var foundTypes = Set<String>()
                let timestamp = Date()
                for configuration in configurations {
                    let configurationID = BigSyncMutationTrackingRegistry.identity(for: configuration)
                    guard opened.insert(configurationID).inserted else { continue }
                    let realm = try Realm(configuration: configuration)
                    let types = reconciledObjectTypes.filter { type in
                        realm.schema.objectSchema.contains { $0.className == type.className() }
                    }
                    guard !types.isEmpty else { continue }
                    var processedTypes = Set<String>()
                    try realm.write {
                        guard try BigSyncMutationTracking.requireCurrentJournalIdentity(in: realm) == identity else {
                            throw BigSyncMutationJournalError.identityChanged
                        }
                        for type in types where processedTypes.insert(type.className()).inserted {
                            foundTypes.insert(type.className())
                            for object in realm.objects(type) {
                                try Task.checkCancellation()
                                guard let recovering = object as? BigSyncRestoredObjectRecovering,
                                      let validator = object as? BigSyncOutboundSemanticObjectValidating,
                                      let metadata = object as? ChangeMetadataRecordable else {
                                    throw BigSyncMutationJournalError.unregisteredModel(type.className())
                                }
                                guard !recovering.isAwaitingRecoveryEvidence else { continue }
                                try validator.validateOutboundSemanticObject(in: realm)
                                try metadata.journalCurrentValuePreservingChangeMetadata(
                                    at: timestamp, expectedJournalIdentity: identity
                                )
                            }
                        }
                        guard try BigSyncMutationTracking.requireCurrentJournalIdentity(in: realm) == identity else {
                            throw BigSyncMutationJournalError.identityChanged
                        }
                    }
                }
                for type in reconciledObjectTypes where !foundTypes.contains(type.className()) {
                    throw BigSyncMutationJournalError.unregisteredModel(type.className())
                }
            }
        )
    }
}
