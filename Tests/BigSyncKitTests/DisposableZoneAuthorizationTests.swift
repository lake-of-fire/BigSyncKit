#if DEBUG
import CloudKit
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

final class DisposableZoneAuthorizationTests: XCTestCase {
    func testArbitraryZoneCannotAcquireDisposableDeletionCapability() {
        let runID = UUID()
        var configuration = makeConfiguration(
            runID: runID,
            zoneName: "ImportantProductionZone"
        )

        XCTAssertFalse(
            configuration.authorizeDisposableZoneDeletionForE2E(runID: runID)
        )
    }

    func testIsolatedPerRunE2EZoneCanAcquireDisposableDeletionCapability() {
        let runID = UUID()
        let canonicalRunID = runID.uuidString.lowercased()
        var configuration = makeConfiguration(
            runID: runID,
            zoneName: "ManabiPlatform.e2e.v2.\(canonicalRunID)"
        )

        XCTAssertTrue(
            configuration.authorizeDisposableZoneDeletionForE2E(runID: runID)
        )
    }

    private func makeConfiguration(
        runID: UUID,
        zoneName: String
    ) -> BigSyncBackgroundWorkerConfiguration {
        let canonicalRunID = runID.uuidString.lowercased()
        let phaseRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("CloudKitE2E", isDirectory: true)
            .appendingPathComponent(canonicalRunID, isDirectory: true)
            .appendingPathComponent("cleanup", isDirectory: true)
        let realmConfiguration = Realm.Configuration(
            inMemoryIdentifier: UUID().uuidString,
            objectTypes: [BigSyncPendingMutation.self]
        )
        return BigSyncBackgroundWorkerConfiguration(
            synchronizerName: "\(zoneName).cleanup",
            containerName: "iCloud.io.manabi.ManabiPlatform.v2",
            configurations: [realmConfiguration],
            mutationPolicy: BigSyncMutationPolicy(
                excludedClassNames: [BigSyncPendingMutation.className()]
            ),
            recordZoneID: CKRecordZone.ID(
                zoneName: zoneName,
                ownerName: CKCurrentUserDefaultName
            ),
            localState: BigSyncLocalStateConfiguration(
                trackingRealmDirectoryURL: phaseRoot
                    .appendingPathComponent("tracking", isDirectory: true),
                keyValueStore: FileKeyValueStore(
                    fileURL: phaseRoot.appendingPathComponent("state.plist")
                ),
                assetDirectoryURL: phaseRoot
                    .appendingPathComponent("assets", isDirectory: true)
            ),
            logger: Logger(label: "DisposableZoneAuthorizationTests")
        )
    }
}
#endif
