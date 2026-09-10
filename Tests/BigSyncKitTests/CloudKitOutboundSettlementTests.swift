import CloudKit
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
