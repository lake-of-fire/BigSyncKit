from pathlib import Path
import subprocess

BASE = 'b1cde24b2e22e68a5cd056b865bc53f7fd491e2a'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == BASE

def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    assert text.count(old) == 1, (path, text.count(old))
    p.write_text(text.replace(old, new, 1))

path = 'Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift'
old = '''                recordZoneID: adapter.recordZoneID
            )
        }
        allowsRecordZoneRebindingForTesting = false'''
new = '''                recordZoneID: adapter.recordZoneID
            )
            // The initializer created backup/installation state for the original
            // zone. This legacy DEBUG fixture hook must initialize the rebound
            // namespace too; ordinary outbound admission still requires its
            // real installation sentinel and must never bypass that proof.
            do {
                let result = try BackupDetection.run(
                    store: keyValueStore,
                    namespace: durableStateNamespace,
                    sharedSentinelBaseURL: backupDetectionBaseURL
                )
                refreshBackupRestoreRequirement()
                if result == .restoredFromBackup || backupRestoreDetected {
                    accountScopeAuthorityFence.poison()
                    clearDeviceIdentifier()
                    try invalidateAccountScopeLeaseDurably()
                    queueAccountScopeInvalidation(.restoreDetected)
                }
            } catch {
                accountScopeAuthorityFence.poison()
                backupDetectionError = error
                logger.error("QSCloudKitSynchronizer >> Rebound fixture backup detection failed: \\(error)")
            }
        }
        allowsRecordZoneRebindingForTesting = false'''
replace_once(path, old, new)

path = 'Tests/BigSyncKitTests/BigSyncKitTests.swift'
anchor = '''    @BigSyncBackgroundActor
    private func makeSynchronizer(
'''
addition = '''    @BigSyncBackgroundActor
    func testReboundFixtureInitializesItsActualOutboundNamespace() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("rebound-outbound-" + UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let synchronizer = makeSynchronizer(backupDetectionBaseURL: root)
        let initialNamespace = synchronizer.durableStateNamespace
        let initialInstallation = try XCTUnwrap(BackupDetection.installationIdentifier(
            namespace: initialNamespace, sharedSentinelBaseURL: root))
        let adapter = FakeModelAdapter(
            zoneID: CKRecordZone.ID(zoneName: "rebound-outbound-zone"), priorities: [])
        synchronizer.addModelAdapter(adapter)
        let selectedNamespace = synchronizer.durableStateNamespace
        XCTAssertNotEqual(selectedNamespace, initialNamespace)
        let selectedInstallation = try XCTUnwrap(BackupDetection.installationIdentifier(
            namespace: selectedNamespace, sharedSentinelBaseURL: root))
        XCTAssertEqual(BackupDetection.installationIdentifier(
            namespace: initialNamespace, sharedSentinelBaseURL: root), initialInstallation)
        try await prepareDirectOutboundAuthority(synchronizer)
        let context = try XCTUnwrap(synchronizer.activeRunContext)
        let principal = try synchronizer.currentOutboundPrincipal(for: context)
        XCTAssertEqual(principal.installationIdentifier, selectedInstallation)
        XCTAssertEqual(principal.durableStateNamespace, selectedNamespace)
        // The fix initializes real evidence; it must not relax its admission.
        try FileManager.default.removeItem(at: root)
        XCTAssertThrowsError(try synchronizer.currentOutboundPrincipal(for: context)) {
            XCTAssertEqual($0 as? BigSyncOutboundQuiescenceError, .staleAuthority)
        }
    }

'''
replace_once(path, anchor, addition + anchor)

path = 'Tests/BigSyncKitTests/OwnedRecordRevisionTests.swift'
anchor = '''        // Seed an unchanged foreign value through inbound replication, then make
'''
addition = '''        // This helper owns a direct adapter drain, not a background full sync.
        // Mirror the enclosing production drain's ownership so normal journal
        // delegate wakeups coalesce instead of starting a second attempt that
        // the deliberately upload-only transport cannot service.
        let attemptID = sync.synchronizationAttemptID
        sync.syncing = true
        sync.synchronizationDrainIsActive = true
        defer {
            sync.cancelSynchronization()
            try? FileManager.default.removeItem(at: root)
        }
'''
replace_once(path, anchor, addition + anchor)
old = '''        let uploads = await io.uploadedValues()
        XCTAssertEqual(uploads.first, local)'''
new = '''        let uploads = await io.uploadedValues()
        XCTAssertEqual(sync.synchronizationAttemptID, attemptID,
                       "A journal wakeup must not replace the owned direct drain")
        XCTAssertTrue(sync.syncing)
        XCTAssertNil(sync.synchronizationTask)
        XCTAssertEqual(uploads.first, local)'''
replace_once(path, old, new)
subprocess.run(['git', 'diff', '--check'], check=True)
