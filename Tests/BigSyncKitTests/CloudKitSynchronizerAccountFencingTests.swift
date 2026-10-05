    var nextDatabaseChangesError: Error?
    private(set) var subscriptionFetchCount = 0

    func recordZoneChanges(
        in zoneID: CKRecordZone.ID,
        since cursor: RecordZoneChangeCursor?,
        desiredKeys: [CKRecord.FieldKey]?,
        resultsLimit: Int?
    ) async throws -> CloudKitRecordZoneChangePage {
        zoneChangeFetchCount += 1
        return .init(
            cursor: RecordZoneChangeCursor(serializedData: Data("account-fencing-zone".utf8)),
            records: [],
            deletedRecordIDs: [],
            moreComing: false
        )
    }

    func saveChanges(
        in records: [CKRecord],
        forceSave: Bool
    ) async throws -> [InboundLiveResult] {
        records.enumerated().map {
            .init(
                event: .init(
                    ordinal: $0.offset,
                    entityType: $0.element.recordType,
                    recordID: $0.element.recordID
                ),
                disposition: .applied
            )
        }
    }

    var serverChangeToken: RecordZoneChangeCursor? { get async { nil } }
    func saveToken(_ token: RecordZoneChangeCursor?) async throws {}

        XCTAssertEqual(reasonsAfterRetry, [.accountChanged])
        let newLease = try XCTUnwrap(synchronizer.accountScopeLease())
        XCTAssertGreaterThan(newLease.invalidationGeneration, oldLease.invalidationGeneration)
        XCTAssertThrowsError(try synchronizer.validateAccountScopeLease(oldLease))
        XCTAssertGreaterThan(transport.operationCount, 0)
    }
}
