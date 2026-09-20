import CloudKit
import Foundation
import RealmSwift

/// Shared read-only inspection for terminal admission, publication recovery and
/// external inventory audits. An empty journal is not evidence of no debt.
struct BigSyncRecordEvidenceInspection: Sendable {
    var unresolvedSubmissionCount = 0
    var acceptedBaselineCount = 0
    var invalidatedBaselineCount = 0
    var resolvedPreservationReceiptCount = 0
    var issues = [String]()

    var hasSubmissionDebt: Bool { unresolvedSubmissionCount != 0 }
    var isConsistent: Bool { issues.isEmpty }
}

public struct BigSyncComparisonEvidenceError: Error, LocalizedError, Sendable {
    public let issues: [String]
    public var errorDescription: String? {
        "Synchronization comparison evidence is inconsistent: " + issues.joined(separator: ", ")
    }
}

extension RealmSwiftAdapter {
    /// An explicit context also permits restoration inspection before adapter
    /// activation. Do not infer a namespace from all rows in a shared Realm.
    @BigSyncBackgroundActor
    func inspectRecordEvidence(
        in realm: Realm, context: BigSyncRecordRebaseContext
    ) throws -> BigSyncRecordEvidenceInspection {
        var result = BigSyncRecordEvidenceInspection()
        guard BigSyncRecordBaseline.isEnabled(in: realm) else { return result }
        let schemaNames = Set(realm.schema.objectSchema.map(\.className))
        let registeredContracts = modelTypes.filter {
            !excludedClassNames.contains($0.key)
                && schemaNames.contains($0.key)
                && $0.value is BigSyncRecordContractProviding.Type
        }
        guard schemaNames.contains(BigSyncRecordSubmission.className()),
              schemaNames.contains(BigSyncPendingMutation.className()),
              schemaNames.contains(BigSyncRecordConflict.className()) else {
            if !registeredContracts.isEmpty { result.issues.append("comparison-evidence-schema-missing") }
            return result
        }
        func ownedType(for name: String) -> (String, Object.Type)? {
            guard let separator = name.firstIndex(of: ".") else { return nil }
            let entityType = String(name[..<separator])
            guard let type = registeredContracts[entityType] else { return nil }
            return (entityType, type)
        }
        func eligible(_ mutation: BigSyncPendingMutation, entityType: String) -> Bool {
            mutation.entityType == entityType
                && mutation.replicaBindingGenerationIdentifier == context.binding
                && (accountScopePropertyByClassName[entityType] == nil
                    || mutation.accountScopeIdentifier == context.account)
        }
        func target(_ name: String, type: Object.Type, entityType: String) -> Object? {
            guard let id = getObjectIdentifier(recordName: name, entityType: entityType),
                  let object = realm.object(ofType: type, forPrimaryKey: id) else { return nil }
            if let accountProperty = accountScopePropertyByClassName[entityType],
               object[accountProperty] as? String != context.account { return nil }
            return object
        }
        for baseline in realm.objects(BigSyncRecordBaseline.self)
            where baseline.namespace == context.namespace {
            let name = baseline.recordName
            guard let (entityType, type) = ownedType(for: name) else { continue }
            if baseline.isComparisonInvalidated {
                result.invalidatedBaselineCount += 1
                if baseline.revision.isEmpty || !baseline.fields.isEmpty
                    || baseline.acceptedSystemFields != nil || baseline.serverChangeTag != nil {
                    result.issues.append("invalidated-comparison-evidence-inconsistent:\(name)")
                }
                // A fence may outlive a physically deleted note. It is not
                // missing-object debt and never grants mutation authority.
                continue
            }
            result.acceptedBaselineCount += 1
            do {
                guard let compiled = try BigSyncCompiledRecordContract.compile(type.init()),
                      baseline.schemaSignature == compiled.signature,
                      !baseline.revision.isEmpty,
                      let systemFields = baseline.acceptedSystemFields else {
                    result.issues.append("accepted-comparison-identity-inconsistent:\(name)")
                    continue
                }
                let recordID = CKRecord.ID(recordName: name, zoneID: recordZoneID)
                let template = try BigSyncRecordPayload.record(systemFields: systemFields)
                let expectedKeys = Set(BigSyncRecordFingerprint.properties(of: type.init()).map(\.name))
                guard template.recordID == recordID, template.recordType == entityType,
                      template.recordChangeTag == baseline.serverChangeTag,
                      template.allKeys().isEmpty,
                      Set(baseline.fieldDigests.keys) == expectedKeys,
                      baseline.fieldDigests.values.allSatisfy({ $0.count == 32 }) else {
                    result.issues.append("accepted-comparison-representation-inconsistent:\(name)")
                    continue
                }
                guard let object = target(name, type: type, entityType: entityType),
                      !BigSyncRecordLifecycle.isPhysicalDeletion(object) else {
                    result.issues.append("accepted-comparison-target-missing:\(name)")
                    continue
                }
                let mutation = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
                if mutation.map({ eligible($0, entityType: entityType) }) != true,
                   try BigSyncRecordFingerprint.fields(of: object) != baseline.fieldDigests {
                    result.issues.append("accepted-comparison-unexplained-local-value:\(name)")
                }
            } catch {
                result.issues.append("accepted-comparison-undecodable:\(name)")
            }
        }
        for submission in realm.objects(BigSyncRecordSubmission.self)
            where submission.namespace == context.namespace {
            let name = submission.recordName
            result.unresolvedSubmissionCount += 1
            guard let (entityType, type) = ownedType(for: name) else {
                result.issues.append("active-submission-unknown-contract:\(name)")
                continue
            }
            let mutation = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
            if mutation.map({ eligible($0, entityType: entityType) }) != true {
                result.issues.append("orphaned-active-submission:\(name)")
            }
            do {
                _ = try validatedSubmissionRecord(submission,
                    recordID: .init(recordName: name, zoneID: recordZoneID), type: type, context: context)
                let base = realm.object(ofType: BigSyncRecordBaseline.self, forPrimaryKey: name)
                if submission.comparisonRevision != base?.revision,
                   !(base?.isComparisonInvalidated == true
                     && target(name, type: type, entityType: entityType).map(BigSyncRecordLifecycle.isPhysicalDeletion) == true
                     && mutation.map({ eligible($0, entityType: entityType) }) == true) {
                    result.issues.append("active-submission-comparison-superseded:\(name)")
                }
            } catch {
                result.issues.append("active-submission-representation-inconsistent:\(name)")
            }
            // Staged V1 and journal V2 are legitimate until V1 is resolved.
        }
        result.resolvedPreservationReceiptCount = realm.objects(BigSyncRecordConflict.self)
            .filter { $0.namespace == context.namespace && $0.isResolved && $0.isPreservationReceipt
                && registeredContracts[$0.entityType] != nil }.count
        return result
    }
}
