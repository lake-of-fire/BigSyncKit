import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit

private final class AccountFencingStore:
    NSObject,
    KeyValueStore,
    @unchecked Sendable {
    private var values = [String: Any]()
    var synchronizesDurably = true
    var undurableKeySubstring: String?
    private var lastMutatedKey: String?

    func object(forKey defaultName: String) -> Any? { values[defaultName] }
    func bool(forKey defaultName: String) -> Bool {
        values[defaultName] as? Bool ?? false
    }
    @BigSyncBackgroundActor
    func changeFeedEpoch() throws -> Int? { feedEpoch }
    func didFinishImport() async throws {
        if requestsOneUploadWakeupOnFinish {
            requestsOneUploadWakeupOnFinish = false
            await modelAdapterDelegate?.hasChangesToUpload()
        }
    }
    func cancelSynchronization() {}
    func unsetCancellation() async throws {}
    @BigSyncBackgroundActor
    func hasPendingChangesAtTerminalBoundary() throws -> Bool {
        hasPendingTerminalChanges
    }
}
