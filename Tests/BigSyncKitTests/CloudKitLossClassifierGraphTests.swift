import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

/// A bounded fallback keeps original recursive implementations from crashing
/// the test process. The production classifier must detect identity cycles,
/// rather than relying on this fixture's eventual removal of its cyclic edge.
private final class LossGraphCycle: NSError, @unchecked Sendable {
    private let lock = NSLock()
    private var reads = 0
    private let useUnderlying: Bool
    private let terminal: NSError
    private let maximumReads: Int

    init(useUnderlying: Bool, maximumReads: Int = 8) {
        self.useUnderlying = useUnderlying
        self.maximumReads = maximumReads
        terminal = CKError(.userDeletedZone) as NSError
        super.init(domain: useUnderlying ? "LocalEnvelope" : CKErrorDomain,
                   code: CKError.partialFailure.rawValue, userInfo: nil)
    }

    required init?(coder: NSCoder) { fatalError("Not an archived fixture") }

    var readCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return reads
    }

    override var userInfo: [String: Any] {
        lock.lock()
        reads += 1
        let includeCycle = reads < maximumReads
        lock.unlock()
        if useUnderlying {
            return [NSMultipleUnderlyingErrorsKey: includeCycle ? [self, terminal] : [terminal]]
        }
        var children: [String: NSError] = ["terminal": terminal]
        if includeCycle { children["cycle"] = self }
        return [CKPartialErrorsByItemIDKey: children]
    }
}

final class CloudKitLossClassifierGraphTests: XCTestCase {
    private let zoneA = CKRecordZone.ID(zoneName: "loss-a", ownerName: "owner-a")
    private let zoneB = CKRecordZone.ID(zoneName: "loss-b", ownerName: "owner-b")

    private func loss(_ code: CKError.Code, info: [String: Any] = [:]) -> NSError {
        CKError(code, userInfo: info) as NSError
    }

    private func wrapper(_ child: NSError) -> NSError {
        NSError(domain: "LocalEnvelope", code: 11, userInfo: [NSUnderlyingErrorKey: child])
    }

    private func aggregate(_ children: [NSError]) -> NSError {
        NSError(domain: "LocalEnvelope", code: 12,
                userInfo: [NSMultipleUnderlyingErrorsKey: children])
    }

    private func partial(_ children: [AnyHashable: NSError],
                         info: [String: Any] = [:]) -> NSError {
        var info = info
        info[CKPartialErrorsByItemIDKey] = children
        return loss(.partialFailure, info: info)
    }

    private func chain(_ child: NSError, wrappers: Int) -> NSError {
        (0..<wrappers).reduce(child) { value, _ in wrapper(value) }
    }

    private func classify(_ error: Error, zone: CKRecordZone.ID? = nil)
        -> CloudKitLossClassifier.Classification {
        CloudKitLossClassifier.classify(error: error, defaultZoneID: zone)
    }

    func testDirectMissingStillRequiresAnExplicitZone() {
        XCTAssertEqual(classify(loss(.zoneNotFound), zone: zoneA).zoneDispositions,
                       [zoneA: .missing])
        XCTAssertTrue(classify(loss(.zoneNotFound)).zoneDispositions.isEmpty)
    }

    func testDirectEncryptedResetKeepsOriginalFlagInterpretation() {
        for flag: Any in [true, NSNumber(value: true)] {
            let result = classify(loss(.zoneNotFound,
                info: [CKErrorUserDidResetEncryptedDataKey: flag]), zone: zoneA)
            XCTAssertEqual(result.zoneDispositions, [zoneA: .encryptedDataReset])
            XCTAssertTrue(result.hasEncryptedDataReset)
        }
        for info: [String: Any] in [[:], [CKErrorUserDidResetEncryptedDataKey: false],
                                    [CKErrorUserDidResetEncryptedDataKey: "true"]] {
            XCTAssertEqual(classify(loss(.zoneNotFound, info: info), zone: zoneA)
                .zoneDispositions, [zoneA: .missing])
        }
    }

    func testDirectUserDeletionStillWinsOverMissingObservation() {
        let result = classify(partial([
            "missing": loss(.zoneNotFound), "deleted": loss(.userDeletedZone)
        ]), zone: zoneA)
        XCTAssertEqual(result.zoneDispositions, [zoneA: .terminal(.deleted)])
    }

    func testRecordKeysOverrideDefaultZoneWithoutLosingAffectedIdentities() {
        let recordA = CKRecord.ID(recordName: "same", zoneID: zoneA)
        let recordB = CKRecord.ID(recordName: "same", zoneID: zoneB)
        let result = classify(partial([recordA: loss(.zoneNotFound),
                                       recordB: loss(.userDeletedZone)]), zone: zoneA)
        XCTAssertEqual(result.zoneDispositions, [zoneA: .missing, zoneB: .terminal(.deleted)])
        XCTAssertEqual(result.affectedRecordIDs, [recordA, recordB])
    }

    func testNestedZoneKeysOverrideInheritedRecordZone() {
        let record = CKRecord.ID(recordName: "outer", zoneID: zoneA)
        let result = classify(partial([record: partial([zoneB: loss(.userDeletedZone)])]))
        XCTAssertEqual(result.zoneDispositions, [zoneB: .terminal(.deleted)])
        XCTAssertEqual(result.affectedRecordIDs, [record])
    }

    func testEqualZoneNamesWithDifferentOwnersStayIndependent() {
        let second = CKRecordZone.ID(zoneName: zoneA.zoneName, ownerName: "other-owner")
        let result = classify(partial([zoneA: loss(.zoneNotFound),
                                       second: loss(.userDeletedZone)]))
        XCTAssertEqual(result.zoneDispositions, [zoneA: .missing, second: .terminal(.deleted)])
    }

    func testNonCloudNumericCodeDoesNotInventAZoneLoss() {
        let error = NSError(domain: "LocalEnvelope", code: CKError.Code.userDeletedZone.rawValue)
        let result = classify(error, zone: zoneA)
        XCTAssertTrue(result.zoneDispositions.isEmpty)
        XCTAssertTrue(result.affectedRecordIDs.isEmpty)
    }

    func testWrappedUserDeletionIsNotDropped() {
        XCTAssertEqual(classify(wrapper(loss(.userDeletedZone)), zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
    }

    func testWrappedEncryptedResetRetainsItsOwnMetadata() {
        let error = wrapper(loss(.zoneNotFound,
            info: [CKErrorUserDidResetEncryptedDataKey: true]))
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .encryptedDataReset])
    }

    func testUnderlyingDeletionDominatesOuterMissingZone() {
        let error = loss(.zoneNotFound, info: [NSUnderlyingErrorKey: loss(.userDeletedZone)])
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
    }

    func testAggregateDeletionDominatesOuterEncryptedReset() {
        let error = loss(.zoneNotFound, info: [
            CKErrorUserDidResetEncryptedDataKey: true,
            NSMultipleUnderlyingErrorsKey: [loss(.userDeletedZone)],
        ])
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
    }

    func testPartialItemsDoNotHideWrapperUnderlyingTerminalCause() {
        let error = partial(["item": loss(.zoneNotFound)],
                            info: [NSUnderlyingErrorKey: loss(.userDeletedZone)])
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
    }

    func testWrapperCausesKeepDefaultZoneRatherThanLastItemZone() {
        let record = CKRecord.ID(recordName: "other", zoneID: zoneB)
        let error = partial([record: loss(.zoneNotFound)], info: [
            NSMultipleUnderlyingErrorsKey: [loss(.userDeletedZone)],
        ])
        let result = classify(error, zone: zoneA)
        XCTAssertEqual(result.zoneDispositions, [zoneA: .terminal(.deleted), zoneB: .missing])
        XCTAssertEqual(result.affectedRecordIDs, [record])
    }

    func testPartialNonCloudWrapperKeepsInheritedRecordZone() {
        let record = CKRecord.ID(recordName: "wrapped", zoneID: zoneB)
        let result = classify(partial([record: wrapper(loss(.userDeletedZone))]), zone: zoneA)
        XCTAssertEqual(result.zoneDispositions, [zoneB: .terminal(.deleted)])
        XCTAssertEqual(result.affectedRecordIDs, [record])
    }

    func testSharedUnderlyingCauseIsVisitedForEachInheritedZone() {
        let shared = wrapper(loss(.userDeletedZone))
        let result = classify(partial([zoneA: shared, zoneB: shared]))
        XCTAssertEqual(result.zoneDispositions,
                       [zoneA: .terminal(.deleted), zoneB: .terminal(.deleted)])
    }

    func testPreviouslyUnscopedCauseIsRevisitedWithARecordZone() {
        let shared = loss(.userDeletedZone)
        let record = CKRecord.ID(recordName: "scoped-later", zoneID: zoneB)
        let error = aggregate([shared, partial([record: shared])])
        XCTAssertEqual(classify(error).zoneDispositions, [zoneB: .terminal(.deleted)])
    }

    func testSharedPartialSubgraphRetainsExplicitNestedScope() {
        let record = CKRecord.ID(recordName: "explicit", zoneID: zoneB)
        let shared = partial([record: loss(.zoneNotFound)])
        let result = classify(partial([zoneA: shared, zoneB: shared]))
        XCTAssertEqual(result.zoneDispositions, [zoneB: .missing])
        XCTAssertEqual(result.affectedRecordIDs, [record])
    }

    func testSingularAndAggregateCausesBothContributeAccountAndTransientCodes() {
        let error = loss(.zoneNotFound, info: [
            NSUnderlyingErrorKey: loss(.notAuthenticated),
            NSMultipleUnderlyingErrorsKey: [loss(.accountTemporarilyUnavailable),
                                           loss(.networkFailure), loss(.requestRateLimited)],
        ])
        let result = classify(error, zone: zoneA)
        XCTAssertEqual(result.accountCodes, [.notAuthenticated, .accountTemporarilyUnavailable])
        XCTAssertTrue(result.isAccountTemporarilyUnavailable)
        XCTAssertEqual(result.transientCodes, [.networkFailure, .requestRateLimited])
        XCTAssertEqual(result.zoneDispositions, [zoneA: .missing])
    }

    func testAggregateTerminalPrecedenceDoesNotDependOnChildOrder() {
        let missing = loss(.zoneNotFound)
        let reset = loss(.zoneNotFound, info: [CKErrorUserDidResetEncryptedDataKey: true])
        let deleted = loss(.userDeletedZone)
        for children in [[missing, reset, deleted], [deleted, reset, missing],
                         [reset, missing, deleted], [missing, deleted, reset]] {
            XCTAssertEqual(classify(aggregate(children), zone: zoneA).zoneDispositions,
                           [zoneA: .terminal(.deleted)])
        }
    }

    func testDatabaseHistoryPrecedenceAndMergeRemainUnchanged() {
        let values: [CloudKitZoneDeletion] = [
            .init(zoneID: zoneA, kind: .encryptedDataReset),
            .init(zoneID: zoneA, kind: .unknown),
            .init(zoneID: zoneA, kind: .deleted),
            .init(zoneID: zoneA, kind: .purged),
            .init(zoneID: zoneB, kind: .encryptedDataReset),
        ]
        for values in [values, Array(values.reversed())] {
            let result = CloudKitLossClassifier.classify(deletions: values)
            XCTAssertEqual(result.zoneDispositions, [zoneA: .terminal(.purged), zoneB: .encryptedDataReset])
        }
        var first = classify(loss(.zoneNotFound), zone: zoneA)
        first.merge(CloudKitLossClassifier.classify(deletions: values))
        XCTAssertEqual(first.zoneDispositions, [zoneA: .terminal(.purged), zoneB: .encryptedDataReset])
    }

    func testPartialIdentityCycleTerminatesWithoutRepeatedMetadataReads() {
        let cycle = LossGraphCycle(useUnderlying: false)
        XCTAssertEqual(classify(cycle, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
        XCTAssertEqual(cycle.readCount, 1)
    }

    func testUnderlyingIdentityCycleKeepsItsTerminalSibling() {
        let cycle = LossGraphCycle(useUnderlying: true)
        XCTAssertEqual(classify(cycle, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
        XCTAssertEqual(cycle.readCount, 1)
    }

    func testRepeatedSharedNodeDoesNotMultiplyMetadataReadsInOneZone() {
        let shared = LossGraphCycle(useUnderlying: false)
        let error = partial(["a": shared, "b": shared, "c": shared])
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
        XCTAssertEqual(shared.readCount, 1)
    }

    func testSharedCycleIsStillInspectedInTwoDistinctZoneContexts() {
        let shared = LossGraphCycle(useUnderlying: false)
        let result = classify(partial([zoneA: shared, zoneB: shared]))
        XCTAssertEqual(result.zoneDispositions,
                       [zoneA: .terminal(.deleted), zoneB: .terminal(.deleted)])
        XCTAssertEqual(shared.readCount, 2)
    }

    func testUnexaminedDeepCauseCannotAuthorizeZoneCreation() {
        let error = loss(.zoneNotFound,
            info: [NSUnderlyingErrorKey: chain(loss(.userDeletedZone), wrappers: 32)])
        XCTAssertTrue(classify(error, zone: zoneA).zoneDispositions.isEmpty)
    }

    func testUnexaminedDeepCauseCannotAuthorizeEncryptedResetRecovery() {
        let error = loss(.zoneNotFound, info: [
            CKErrorUserDidResetEncryptedDataKey: true,
            NSUnderlyingErrorKey: chain(loss(.userDeletedZone), wrappers: 32),
        ])
        XCTAssertTrue(classify(error, zone: zoneA).zoneDispositions.isEmpty)
    }

    func testObservedTerminalLossSurvivesUnexaminedDeeperCauses() {
        let error = loss(.userDeletedZone,
            info: [NSUnderlyingErrorKey: chain(loss(.zoneNotFound), wrappers: 32)])
        XCTAssertEqual(classify(error, zone: zoneA).zoneDispositions,
                       [zoneA: .terminal(.deleted)])
    }

    func testAlreadyInspectedDeepAliasRetainsCompleteMissingZoneProof() {
        let leaf = loss(.zoneNotFound)
        // Both paths reach the same error in the same inherited scope. The
        // long path's final edge is depth 32, but its shallow alias was read.
        let result = classify(aggregate([leaf, chain(leaf, wrappers: 31)]), zone: zoneA)
        XCTAssertEqual(result.zoneDispositions, [zoneA: .missing])
    }

    func testDepthBoundaryWithUnseenLeafIsNotTreatedAsACompleteProof() {
        let leaf = loss(.zoneNotFound)
        XCTAssertEqual(classify(chain(leaf, wrappers: 31), zone: zoneA).zoneDispositions,
                       [zoneA: .missing])
        XCTAssertTrue(classify(chain(leaf, wrappers: 32), zone: zoneA).zoneDispositions.isEmpty)
    }

    func testDeepAliasInAnotherZoneIsNotMistakenForAnInspectedObservation() {
        let shared = loss(.zoneNotFound)
        let error = partial([zoneA: shared, zoneB: chain(shared, wrappers: 31)])
        // The cause under B was not inspected. Missing-zone proof under A
        // cannot certify that unobserved branch or manufacture a B result.
        XCTAssertTrue(classify(error).zoneDispositions.isEmpty)
    }

    func testMergingIncompleteObservationCannotRestoreRecoveryPermission() {
        let unknown = classify(chain(loss(.zoneNotFound), wrappers: 32), zone: zoneA)
        var valid = classify(loss(.zoneNotFound), zone: zoneB)
        valid.merge(unknown)
        XCTAssertTrue(valid.zoneDispositions.isEmpty)
        var reverse = unknown
        reverse.merge(classify(loss(.zoneNotFound), zone: zoneB))
        XCTAssertTrue(reverse.zoneDispositions.isEmpty)
        reverse.merge(classify(loss(.userDeletedZone), zone: zoneA))
        XCTAssertEqual(reverse.zoneDispositions, [zoneA: .terminal(.deleted)])
    }

    func testMalformedPartialValuesDoNotInventRecordLoss() {
        let record = CKRecord.ID(recordName: "malformed", zoneID: zoneA)
        let error = loss(.partialFailure, info: [CKPartialErrorsByItemIDKey: [record: "not an error"]])
        let result = classify(error, zone: zoneA)
        XCTAssertTrue(result.zoneDispositions.isEmpty)
        XCTAssertTrue(result.affectedRecordIDs.isEmpty)
    }
}
