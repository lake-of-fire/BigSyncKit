import CloudKit
import Foundation
import RealmSwift

/// A read-only view shared by terminal receipt admission, publication recovery
/// and the external inventory audit. A journal-empty state is not necessarily
/// terminal: an uncertain submission still needs an acceptance decision.
struct BigSyncRecordEvidenceInspection: Sendable {
    var unresolvedSubmissionCount = 0
    var acceptedBaselineCount = 0
    var invalidatedBaselineCount = 0
    var resolvedPreservationReceiptCount = 0
    var issues = [String]()

    var hasSubmissionDebt: Bool { unresolvedSubmissionCount != 0 }
    var isConsistent: Bool { issues.isEmpty }
}

extension RealmSwiftAdapter {
    /// This deliberately accepts an explicit context. Read-only publication
    /// restoration uses it before operational setup/account activation, and
    /// must not open an account-wide or other-binding evidence namespace.
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
        guard !registeredContracts.isEmpty else { return result }
        guard schemaNames.contains(BigSyncRecordSubmission.className()),
              schemaNames.contains(BigSyncPendingMutation.className()),
              schemaNames.contains(BigSyncRecordConflict.className()) else {
            result.issues.append("comparison-evidence-schema-missing")
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
                    || baseline.acceptedSystemFields != nil || baseline.serverChangeTag != nil
                    || baseline.acceptedSubmissionIdentity != nil {
                    result.issues.append("invalidated-comparison-evidence-inconsistent:\(name)")
                }
                // The revision fence outlives a physically removed note. It is
                // neither a missing object nor an unresolved server submission.
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
                // An owned V2 is allowed to differ from the accepted V1. It is
                // the existing journal, not the baseline, that explains V2.
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
            guard let (entityType, type) = ownedType(for: name) else { continue }
            result.unresolvedSubmissionCount += 1
            let mutation = realm.object(ofType: BigSyncPendingMutation.self, forPrimaryKey: name)
            if mutation.map({ eligible($0, entityType: entityType) }) != true {
                result.issues.append("orphaned-active-submission:\(name)")
            }
            do {
                _ = try validatedSubmissionRecord(submission,
                    recordID: .init(recordName: name, zoneID: recordZoneID),
                    type: type, context: context)
            } catch {
                result.issues.append("active-submission-representation-inconsistent:\(name)")
            }
            // No generation-equality requirement: a staged V1 can legitimately
            // coexist with the journal's V2 until V1's uncertainty is resolved.
        }
        result.resolvedPreservationReceiptCount = realm.objects(BigSyncRecordConflict.self)
            .filter { $0.namespace == context.preservationNamespace
                && $0.isResolved && $0.isPreservationReceipt
                && registeredContracts[$0.entityType] != nil }.count
        return result
    }
}
