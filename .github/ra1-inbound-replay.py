#!/usr/bin/env python3
from pathlib import Path

source = Path('Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Sync.swift')
text = source.read_text()
needle = """        if let migrationError = error as? ChangeFeedMigrationError,
           migrationError.deletionKind == .encryptedDataReset {"""
replacement = """        if error is RealmSwiftInboundTargetChangedError {
            // A non-journaled local write invalidated an inbound selection.
            // Its page cursor did not commit, so replay the ordinary fetch
            // after a bounded delay rather than acknowledging or quarantining it.
            shouldRetry = true
            retryDelay = 1
        } else if let migrationError = error as? ChangeFeedMigrationError,
           migrationError.deletionKind == .encryptedDataReset {"""
if text.count(needle) != 1:
    raise SystemExit(
        f'expected exactly one inbound-replay insertion point, got {text.count(needle)}'
    )
if 'error is RealmSwiftInboundTargetChangedError' in text:
    raise SystemExit('selected source already classifies inbound target races')
source.write_text(text.replace(needle, replacement, 1))

tests = Path('Tests/BigSyncKitTests/BigSyncKitTests.swift')
test_text = tests.read_text()
marker = 'testRA1InboundTargetChangedFailureSchedulesReplay'
if marker in test_text:
    raise SystemExit('focused RA-1 replay test already exists')
test_text += r'''

extension BigSyncKitTests {
    @BigSyncBackgroundActor
    func testRA1InboundTargetChangedFailureSchedulesReplay() async throws {
        let synchronizer = makeSynchronizer()
        synchronizer.syncing = true
        synchronizer.synchronizationDrainIsActive = true
        let attemptID = synchronizer.synchronizationAttemptID
        let started = Date()

        await synchronizer.failSynchronization(
            error: RealmSwiftInboundTargetChangedError(
                recordName: "RA1ParityRecord.collision"
            )
        )

        XCTAssertEqual(synchronizer.synchronizationAttemptID, attemptID)
        XCTAssertTrue(synchronizer.syncing)
        let retryAt = try XCTUnwrap(synchronizer.retrySleepUntil)
        XCTAssertGreaterThanOrEqual(
            retryAt,
            started.addingTimeInterval(0.9),
            "A transient non-journaled target race must replay through a delayed ordinary sync"
        )

        synchronizer.cancelSynchronization()
        await synchronizer.cancelSynchronizationAndWait()
    }
}
'''
tests.write_text(test_text)
