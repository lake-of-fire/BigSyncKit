import Foundation
import XCTest
@testable import BigSyncKit

final class BackupDetectionTests: XCTestCase {
    private final class Store: NSObject, KeyValueStore {
        private var values = [String: Any]()

        func object(forKey key: String) -> Any? { values[key] }
        func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
        func set(value: Any?, forKey key: String) { values[key] = value }
        func set(boolValue: Bool, forKey key: String) { values[key] = boolValue }
        func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    }

    func testFirstRunRegularLaunchAndRestoreAreDistinguished() throws {
        let store = Store()
        let testRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let installedSentinel = testRoot.appendingPathComponent("installed/sentinel")
        let restoredSentinel = testRoot.appendingPathComponent("restored/sentinel")

        XCTAssertEqual(
            try BackupDetection.run(store: store, sentinelURL: installedSentinel),
            .firstRun
        )
        XCTAssertTrue(store.bool(forKey: BackupDetection.storeKey))
        XCTAssertTrue(FileManager.default.fileExists(atPath: installedSentinel.path))

        XCTAssertEqual(
            try BackupDetection.run(store: store, sentinelURL: installedSentinel),
            .regularLaunch
        )

        // A restored defaults store retains its marker while the excluded
        // filesystem sentinel is absent on the restored installation.
        XCTAssertEqual(
            try BackupDetection.run(store: store, sentinelURL: restoredSentinel),
            .restoredFromBackup
        )
        XCTAssertTrue(FileManager.default.fileExists(atPath: restoredSentinel.path))
        XCTAssertTrue(BackupDetection.restoreResetIsRequired(store: store))

        // Recreating the synchronizer after a crash still requires the cache
        // reset even though the replacement sentinel now exists.
        XCTAssertEqual(
            try BackupDetection.run(store: store, sentinelURL: restoredSentinel),
            .regularLaunch
        )
        XCTAssertTrue(BackupDetection.restoreResetIsRequired(store: store))

        BackupDetection.markRestoreResetCompleted(store: store)
        XCTAssertFalse(BackupDetection.restoreResetIsRequired(store: store))
    }

    func testExistingSentinelRepairsMissingBackedUpMarker() throws {
        let store = Store()
        let sentinel = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
            .appendingPathComponent("sentinel")
        try FileManager.default.createDirectory(
            at: sentinel.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data().write(to: sentinel)

        XCTAssertEqual(
            try BackupDetection.run(store: store, sentinelURL: sentinel),
            .regularLaunch
        )
        XCTAssertTrue(store.bool(forKey: BackupDetection.storeKey))
    }
}
