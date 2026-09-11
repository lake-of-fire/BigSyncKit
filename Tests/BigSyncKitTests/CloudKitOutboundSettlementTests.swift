import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

final class CloudKitOutboundSettlementTests: XCTestCase {
    func testExactSuccessSettlesButMissingOrForeignResultDoesNot() {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let wrong = CKRecord(recordType: "Other", recordID: record.recordID)
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [record.recordID: .success(record)], deleteResults: [:])
            .provesDefinitiveSettlement(saving: [record], deleting: []))
        XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [:], deleteResults: [:])
            .provesDefinitiveSettlement(saving: [record], deleting: []))
        XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [record.recordID: .success(wrong)], deleteResults: [:])
            .provesDefinitiveSettlement(saving: [record], deleting: []))
    }

    func testUnknownTransportOutcomeOrCancellationNeverSettles() {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let errors: [Error] = [CKError(.networkFailure), CKError(.networkUnavailable),
                               CKError(.operationCancelled), CKError(.internalError), CancellationError()]
        for error in errors {
            XCTAssertFalse(CloudKitRecordMutationResults(saveResults: [record.recordID: .failure(error)], deleteResults: [:])
                .provesDefinitiveSettlement(saving: [record], deleting: []))
        }
    }

    func testDefinitiveRejectionIsNotAClaimThatTheMutationWasAcknowledged() {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [record.recordID: .failure(CKError(.serverRecordChanged))], deleteResults: [:])
            .provesDefinitiveSettlement(saving: [record], deleting: []))
        XCTAssertTrue(CloudKitRecordMutationResults(saveResults: [:], deleteResults: [record.recordID: .failure(CKError(.unknownItem))])
            .provesDefinitiveSettlement(saving: [], deleting: [record.recordID]))
    }

    func testWholeOperationRejectionCannotBorrowPerItemSettlement() {
        XCTAssertTrue(CloudKitRecordMutationResults.isDefinitiveOperationRejection(CKError(.limitExceeded)))
        XCTAssertFalse(CloudKitRecordMutationResults.isDefinitiveOperationRejection(CKError(.assetFileModified)))
        XCTAssertFalse(CloudKitRecordMutationResults.isDefinitiveOperationRejection(CKError(.partialFailure)))
        XCTAssertFalse(CloudKitRecordMutationResults.isDefinitiveOperationRejection(CKError(.networkFailure)))
    }

    func testMixedBatchKeepsUncertaintyForOneUnknownItem() {
        let first = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let second = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.two"))
        let results = CloudKitRecordMutationResults(saveResults: [
            first.recordID: .success(first), second.recordID: .failure(CKError(.networkFailure))
        ], deleteResults: [:])
        XCTAssertFalse(results.provesDefinitiveSettlement(saving: [first, second], deleting: []))
    }
}

/// Native CKOperation callbacks without adding the operation to a database.
/// These exercise the production callback installer/collector, not CloudKit
/// networking, long-lived replay or generation-matched Realm acknowledgement.
private final class PreparedMutationCapture: @unchecked Sendable {
    private let lock = NSLock()
    private var values = [Result<CloudKitRecordMutationResults, Error>]()

    func receive(_ value: Result<CloudKitRecordMutationResults, Error>) {
        lock.lock()
        defer { lock.unlock() }
        values.append(value)
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return values.count
    }

    func result() throws -> CloudKitRecordMutationResults {
        lock.lock()
        defer { lock.unlock() }
        return try XCTUnwrap(values.first).get()
    }
}

private final class PreparedMutationLifetimeSentinel: Sendable {}

extension CloudKitOutboundSettlementTests {
    private func prepared(
        saving records: [CKRecord] = [], deleting ids: [CKRecord.ID] = []
    ) -> CloudKitPreparedRecordMutation {
        let operation = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: ids)
        return .init(operation: operation,
            transportIdentity: .init(clientChangeTokenData: Data("test-token".utf8),
                longLivedOperationID: operation.operationID),
            expectedSaveIDs: Set(records.map(\.recordID)), expectedDeleteIDs: Set(ids))
    }

    func testPreparedWrapperIsNotRetainedByItsOperationCallbacks() throws {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        var request: CloudKitPreparedRecordMutation? = prepared(saving: [record])
        weak var weakRequest = request
        let operation = try XCTUnwrap(request).operation
        let capture = PreparedMutationCapture()
        try request?.installResultHandlers { capture.receive($0) }
        request = nil
        XCTAssertNil(weakRequest)
        operation.perRecordSaveBlock?(record.recordID, .success(record))
        operation.modifyRecordsResultBlock?(.success(()))
        XCTAssertEqual(capture.count, 1)
        XCTAssertTrue(try capture.result().provesDefinitiveSettlement(saving: [record], deleting: []))
    }

    func testPreparedOperationAndWrapperCanBothDeallocateWithoutTerminalDelivery() throws {
        var request: CloudKitPreparedRecordMutation? = prepared()
        weak var weakRequest = request
        weak var weakOperation = request?.operation
        try request?.installResultHandlers { _ in }
        request = nil
        XCTAssertNil(weakRequest)
        XCTAssertNil(weakOperation)
    }

    func testTerminalDeliveryReleasesCallerCaptureWhileOperationIsStillRetained() throws {
        let request = prepared()
        var sentinel: PreparedMutationLifetimeSentinel? = .init()
        weak var weakSentinel = sentinel
        try request.installResultHandlers { [sentinel] _ in
            withExtendedLifetime(sentinel) {}
        }
        sentinel = nil
        XCTAssertNotNil(weakSentinel)
        request.operation.modifyRecordsResultBlock?(.success(()))
        XCTAssertNil(weakSentinel)
        withExtendedLifetime(request) {}
    }

    func testIncompleteSaveCallbacksPreserveKnownOutcomeDespiteWholeOperationError() throws {
        let first = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let missing = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.two"))
        for code in [CKError.Code.limitExceeded, .networkFailure, .partialFailure] {
            let request = prepared(saving: [first, missing]), capture = PreparedMutationCapture()
            try request.installResultHandlers { capture.receive($0) }
            request.operation.perRecordSaveBlock?(first.recordID, .success(first))
            request.operation.modifyRecordsResultBlock?(.failure(CKError(code)))
            let result = try capture.result()
            XCTAssertEqual(try result.saveResults[first.recordID]?.get().recordID, first.recordID)
            XCTAssertNil(result.saveResults[missing.recordID])
            XCTAssertFalse(result.provesDefinitiveSettlement(saving: [first, missing], deleting: []))
        }
    }

    func testIncompleteDeleteCallbacksDoNotSynthesizeSettlementForMissingDeletion() throws {
        let first = CKRecord.ID(recordName: "Item.one"), missing = CKRecord.ID(recordName: "Item.two")
        let request = prepared(deleting: [first, missing]), capture = PreparedMutationCapture()
        try request.installResultHandlers { capture.receive($0) }
        request.operation.perRecordDeleteBlock?(first, .success(()))
        request.operation.modifyRecordsResultBlock?(.failure(CKError(.limitExceeded)))
        let result = try capture.result()
        XCTAssertNotNil(result.deleteResults[first])
        XCTAssertNil(result.deleteResults[missing])
        XCTAssertFalse(result.provesDefinitiveSettlement(saving: [], deleting: [first, missing]))
    }

    func testNoPerItemCallbackRetainsOriginalOperationError() throws {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        for code in [CKError.Code.networkFailure, .limitExceeded] {
            let request = prepared(saving: [record]), capture = PreparedMutationCapture()
            try request.installResultHandlers { capture.receive($0) }
            request.operation.modifyRecordsResultBlock?(.failure(CKError(code)))
            XCTAssertThrowsError(try capture.result()) { error in
                XCTAssertEqual((error as? CKError)?.code, code)
            }
            XCTAssertEqual(capture.count, 1)
        }
    }

    func testCompletePartialFailureKeepsEachDefinitiveItemResult() throws {
        let saved = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let deleted = CKRecord.ID(recordName: "Item.two")
        let request = prepared(saving: [saved], deleting: [deleted]), capture = PreparedMutationCapture()
        try request.installResultHandlers { capture.receive($0) }
        request.operation.perRecordSaveBlock?(saved.recordID, .success(saved))
        request.operation.perRecordDeleteBlock?(deleted, .failure(CKError(.unknownItem)))
        request.operation.modifyRecordsResultBlock?(.failure(CKError(.partialFailure)))
        let result = try capture.result()
        XCTAssertEqual(result.saveResults.count, 1)
        XCTAssertEqual(result.deleteResults.count, 1)
        XCTAssertTrue(result.provesDefinitiveSettlement(saving: [saved], deleting: [deleted]))
    }

    func testOperationSuccessWithoutRequiredItemCallbackRemainsUnsettled() throws {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let request = prepared(saving: [record]), capture = PreparedMutationCapture()
        try request.installResultHandlers { capture.receive($0) }
        request.operation.modifyRecordsResultBlock?(.success(()))
        XCTAssertFalse(try capture.result().provesDefinitiveSettlement(saving: [record], deleting: []))
    }

    func testSecondExecutionCannotReplaceTheFirstCompletion() throws {
        let request = prepared(), first = PreparedMutationCapture(), second = PreparedMutationCapture()
        try request.installResultHandlers { first.receive($0) }
        XCTAssertThrowsError(try request.installResultHandlers { second.receive($0) }) { error in
            XCTAssertEqual(error as? CloudKitPreparedRecordMutationError, .alreadyExecuted)
        }
        request.operation.modifyRecordsResultBlock?(.success(()))
        XCTAssertEqual(first.count, 1)
        XCTAssertEqual(second.count, 0)
    }

    func testDuplicateTerminalAndLateItemCallbacksCannotDeliverAgain() throws {
        let record = CKRecord(recordType: "Item", recordID: .init(recordName: "Item.one"))
        let request = prepared(saving: [record]), capture = PreparedMutationCapture()
        try request.installResultHandlers { capture.receive($0) }
        request.operation.modifyRecordsResultBlock?(.success(()))
        request.operation.perRecordSaveBlock?(record.recordID, .success(record))
        request.operation.modifyRecordsResultBlock?(.failure(CKError(.networkFailure)))
        XCTAssertEqual(capture.count, 1)
        XCTAssertTrue(try capture.result().saveResults.isEmpty)
    }
}
