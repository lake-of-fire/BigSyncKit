import CloudKit
import Foundation
import XCTest
@testable import BigSyncKit

final class InjectedBindingStoreIdentityTests: XCTestCase {
    func testInjectedStoreIsTheOnlyPreparedBindingStore() throws {
        try withResources { identity, store, root in
            let installation = try identity.prepareInstallation()
            let binding = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let snapshot = try XCTUnwrap(identity.currentMutationJournalIdentity(store: store))
            XCTAssertEqual(snapshot.installationIdentifier, installation)
            XCTAssertEqual(snapshot.replicaBindingGenerationIdentifier, binding)
            XCTAssertNil(identity.currentMutationJournalIdentity())
            XCTAssertTrue(FileManager.default.fileExists(atPath: root.appendingPathComponent("bigsync-state.plist").path))
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: store), binding)
        }
    }

    func testExistingProviderObservesPendingReplacementAndActivation() throws {
        try withResources { identity, store, _ in
            let original = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let provider = identity.makeMutationJournalIdentityProvider(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount("account-a", store: store, key: key)
            let pending = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "account-a", destinationAccountScopeIdentifier: "account-b",
                store: store, key: key
            )
            XCTAssertNotEqual(pending.bindingGenerationIdentifier, original)
            XCTAssertEqual(provider()?.replicaBindingGenerationIdentifier, pending.bindingGenerationIdentifier)
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: store), pending.bindingGenerationIdentifier)
            _ = try BigSyncReplicaBindingStateStore.activatePort(pending, store: store, key: key)
            XCTAssertEqual(provider()?.replicaBindingGenerationIdentifier, pending.bindingGenerationIdentifier)
        }
    }

    func testFreshStoreAndProviderResumeTheSameDurableBinding() throws {
        try withResources { identity, store, root in
            let binding = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let reopened = FileKeyValueStore(fileURL: root.appendingPathComponent("bigsync-state.plist"), writesAtomically: true)
            try reopened.prepareForUse()
            XCTAssertEqual(identity.currentMutationJournalIdentity(store: reopened)?.replicaBindingGenerationIdentifier, binding)
            XCTAssertEqual(try identity.prepareReplicaBindingGenerationIdentifier(store: reopened), binding)
        }
    }

    func testMalformedBindingFailsClosedWithoutPreparingAnotherStore() throws {
        try withResources { identity, store, _ in
            _ = try identity.prepareReplicaBindingGenerationIdentifier(store: store)
            let key = identity.durableStateNamespace + ".ReplicaBinding.v1"
            try store.bigSyncSetDurably(value: ["version": 999], forKey: key)
            XCTAssertNil(identity.currentMutationJournalIdentity(store: store))
            XCTAssertThrowsError(try identity.prepareReplicaBindingGenerationIdentifier(store: store))
            XCTAssertNil(identity.currentMutationJournalIdentity())
        }
    }

    private func withResources(
        _ body: (BigSyncClientIdentity, FileKeyValueStore, URL) throws -> Void
    ) throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("InjectedBindingStoreIdentityTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let identity = BigSyncClientIdentity(
            synchronizerName: "isolated-client",
            containerName: "iCloud.example.injected-store-tests",
            recordZoneID: CKRecordZone.ID(zoneName: "isolated-zone", ownerName: CKCurrentUserDefaultName),
            sharedStateBaseURL: root.appendingPathComponent("identity", isDirectory: true)
        )
        let store = FileKeyValueStore(fileURL: root.appendingPathComponent("bigsync-state.plist"), writesAtomically: true)
        try body(identity, store, root)
    }
}
