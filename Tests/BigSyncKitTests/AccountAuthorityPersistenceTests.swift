import Foundation
import XCTest
@testable import BigSyncKit

// Injected property-list storage. These exercise the actual account codecs
// and state transitions, not disk durability or genuine Apple-account changes.
private final class AuthorityPersistenceStore: NSObject, DurableKeyValueStore {
    var values = [String: Any]()
    var writes = 0
    var readError: Error?
    func object(forKey key: String) -> Any? { values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { values[key] = value; writes += 1 }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { values.removeValue(forKey: key); writes += 1 }
    func synchronize() -> Bool { true }
    func prepareForUse() throws {}
    func validateDurability() throws { if let readError { throw readError } }
    func durableObject(forKey key: String) throws -> Any? {
        try validateDurability(); return values[key]
    }
    func setDurably(value: Any?, forKey key: String) throws { set(value: value, forKey: key) }
    func removeDurably(forKey key: String) throws { removeObject(forKey: key) }
}

final class AccountAuthorityPersistenceTests: XCTestCase {
    private let key = "binding"
    private enum StoreFailure: Error { case unavailable }

    private func preparedStore(owner: String? = "owner-A", restored: Bool = false) throws -> AuthorityPersistenceStore {
        let store = AuthorityPersistenceStore()
        _ = try BigSyncReplicaBindingStateStore.prepare(store: store, key: key, installationIdentifier: "install-A")
        if let owner {
            _ = try BigSyncReplicaBindingStateStore.bindInitialAccount(owner, store: store, key: key)
        }
        if restored {
            _ = try BigSyncReplicaBindingStateStore.prepare(store: store, key: key, installationIdentifier: "install-B")
        }
        store.writes = 0
        return store
    }

    private func alter(_ store: AuthorityPersistenceStore, _ field: String, _ value: Any?) {
        var raw = store.values[key] as! [String: Any]
        raw[field] = value
        store.values[key] = raw
    }

    private func validLease() -> [String: Any] {
        ["version": 1, "generation": NSNumber(value: Int64(7)), "isValid": true,
         "accountScopeIdentifier": "owner-A", "validatedAt": Date(timeIntervalSinceReferenceDate: 10)]
    }

    func testAbsentReplicaStateRemainsAbsent() throws {
        let store = AuthorityPersistenceStore()
        XCTAssertNil(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        XCTAssertEqual(store.writes, 0)
    }

    func testUnboundAndSameAccountBindingRemainIdempotent() throws {
        let store = try preparedStore(owner: nil)
        let initial = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
        XCTAssertNil(initial.datasetOwnerAccountScopeIdentifier)
        let bound = try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-A", store: store, key: key)
        XCTAssertEqual(bound.activeGenerationIdentifier, initial.activeGenerationIdentifier)
        XCTAssertEqual(store.writes, 1)
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-A", store: store, key: key), bound)
        XCTAssertEqual(store.writes, 1)
    }

    func testExistingOwnerRequiresPort() throws {
        let store = try preparedStore()
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-B", store: store, key: key)) {
            XCTAssertEqual($0 as? BigSyncReplicaBindingError, .accountMismatch)
        }
        XCTAssertEqual(store.writes, 0)
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.load(store: store, key: key)?.datasetOwnerAccountScopeIdentifier, "owner-A")
    }

    func testRestoredOwnerCannotBeReboundUntilAdmitted() throws {
        let store = try preparedStore(restored: true)
        let state = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
        XCTAssertNil(state.activeAccountScopeIdentifier)
        XCTAssertEqual(state.restoredDatasetOwnerAccountScopeIdentifier, "owner-A")
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-B", store: store, key: key))
        XCTAssertEqual(store.writes, 0)
        let rebound = try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-A", store: store, key: key)
        XCTAssertEqual(rebound.activeAccountScopeIdentifier, "owner-A")
        XCTAssertNil(rebound.restoredDatasetOwnerAccountScopeIdentifier)
        XCTAssertEqual(rebound.activeGenerationIdentifier, state.activeGenerationIdentifier)
    }

    func testPendingPortRoundTripsActivationAndCancellation() throws {
        for activate in [false, true] {
            let store = try preparedStore()
            let original = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
            let pending = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-B", store: store, key: key)
            XCTAssertEqual(try BigSyncReplicaBindingStateStore.load(store: store, key: key)?.pendingPort, pending)
            let result = try activate
                ? BigSyncReplicaBindingStateStore.activatePort(pending, store: store, key: key)
                : BigSyncReplicaBindingStateStore.cancelPort(pending, store: store, key: key)
            XCTAssertNil(result.pendingPort)
            XCTAssertEqual(result.activeAccountScopeIdentifier, activate ? "owner-B" : "owner-A")
            XCTAssertEqual(result.activeGenerationIdentifier, activate ? pending.bindingGenerationIdentifier : original.activeGenerationIdentifier)
        }
    }

    func testRepeatedPortRequestRetainsOriginalTransition() throws {
        let store = try preparedStore()
        let pending = try BigSyncReplicaBindingStateStore.requirePort(
            sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-B", store: store, key: key)
        let count = store.writes
        let repeated = try BigSyncReplicaBindingStateStore.requirePort(
            sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-C", store: store, key: key)
        XCTAssertEqual(repeated, pending)
        XCTAssertEqual(store.writes, count)
    }

    func testMalformedActiveOwnerCannotBecomeFirstBinding() throws {
        for malformed in [7, true, Data([1]), ["owner-A"], NSNull()] as [Any] {
            let store = try preparedStore()
            alter(store, "activeAccountScopeIdentifier", malformed)
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-B", store: store, key: key))
            XCTAssertEqual(store.writes, 0, "Malformed owner must not be overwritten by initial binding")
        }
    }

    func testMalformedRestoredOwnerCannotBecomeFirstBinding() throws {
        for malformed in [7, true, Data([1]), ["owner-A"], NSNull()] as [Any] {
            let store = try preparedStore(restored: true)
            alter(store, "restoredDatasetOwnerAccountScopeIdentifier", malformed)
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-B", store: store, key: key))
            XCTAssertEqual(store.writes, 0)
        }
    }

    func testInstallationRestoreCannotEraseMalformedOwnership() throws {
        for restored in [false, true] {
            let store = try preparedStore(restored: restored)
            alter(store, restored ? "restoredDatasetOwnerAccountScopeIdentifier" : "activeAccountScopeIdentifier", 7)
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.prepare(store: store, key: key, installationIdentifier: "install-C"))
            XCTAssertEqual(store.writes, 0)
        }
    }

    func testMalformedBindingVersionCannotPrepareOrBind() throws {
        for malformed in [true, 1.75, -0.5, "1", NSNull(), NSNumber(value: UInt64.max)] as [Any] {
            let store = try preparedStore(owner: nil)
            alter(store, "version", malformed)
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.prepare(store: store, key: key, installationIdentifier: "install-A"))
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("owner-A", store: store, key: key))
            XCTAssertEqual(store.writes, 0)
        }
    }

    func testIncompletePendingEnvelopeFailsClosed() throws {
        for missing in ["pendingTransitionID", "pendingBindingGenerationIdentifier", "pendingSourceAccountScopeIdentifier",
                        "pendingDestinationAccountScopeIdentifier", "pendingDetectedAt"] {
            let store = try preparedStore()
            _ = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-B", store: store, key: key)
            alter(store, missing, nil); store.writes = 0
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertEqual(store.writes, 0)
        }
    }

    func testPendingPortRejectsNonfiniteDates() throws {
        for value in [Double.infinity, -.infinity, .nan] {
            let store = try preparedStore()
            _ = try BigSyncReplicaBindingStateStore.requirePort(
                sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-B", store: store, key: key)
            alter(store, "pendingDetectedAt", Date(timeIntervalSinceReferenceDate: value)); store.writes = 0
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertEqual(store.writes, 0)
        }
    }

    func testPendingPortCannotNameTheSameSourceAndDestination() throws {
        let store = AuthorityPersistenceStore()
        store.values[key] = [
            "version": 1, "installationIdentityDigest": String(repeating: "b", count: 64),
            "activeGenerationIdentifier": String(repeating: "a", count: 64), "activeAccountScopeIdentifier": "owner-A",
            "pendingTransitionID": "a0000000-0000-0000-0000-000000000001",
            "pendingBindingGenerationIdentifier": "b132e7df24a44f46fc556df6caaaa9459225474541314302c510e9c32d5daa66",
            "pendingSourceAccountScopeIdentifier": "owner-A", "pendingDestinationAccountScopeIdentifier": "owner-A",
            "pendingDetectedAt": Date(timeIntervalSinceReferenceDate: 10),
        ]
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        XCTAssertEqual(store.writes, 0)
    }

    func testPendingPortStillRejectsWrongBoundDigest() throws {
        let store = try preparedStore()
        _ = try BigSyncReplicaBindingStateStore.requirePort(
            sourceAccountScopeIdentifier: "owner-A", destinationAccountScopeIdentifier: "owner-B", store: store, key: key)
        alter(store, "pendingBindingGenerationIdentifier", String(repeating: "a", count: 64))
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
    }

    func testBothOwnerFieldsAreNeverAdmitted() throws {
        let store = try preparedStore()
        alter(store, "restoredDatasetOwnerAccountScopeIdentifier", "owner-A")
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        XCTAssertEqual(store.writes, 0)
    }

    func testBinaryAndXMLBindingRoundTripPreservesOriginalBytes() throws {
        let owner = "e\u{301}"
        let store = try preparedStore(owner: owner)
        let original = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
            let data = try PropertyListSerialization.data(fromPropertyList: store.values[key]!, format: format, options: 0)
            store.values[key] = try PropertyListSerialization.propertyList(from: data, options: [], format: nil)
            let decoded = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertEqual(decoded, original)
            XCTAssertEqual(Data(try XCTUnwrap(decoded.activeAccountScopeIdentifier).utf8), Data(owner.utf8))
        }
        XCTAssertEqual(store.writes, 0)
    }

    func testUnknownBindingFieldsRemainCompatible() throws {
        let store = try preparedStore()
        let original = try BigSyncReplicaBindingStateStore.load(store: store, key: key)
        alter(store, "futureUninterpretedField", Data([1, 2]))
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.load(store: store, key: key), original)
        XCTAssertEqual(store.writes, 0)
    }

    func testExactLeaseGenerationBoundsRemainAccepted() throws {
        for generation in [Int64(0), 1, Int64.max] {
            var raw = validLease(); raw["generation"] = NSNumber(value: generation)
            let decoded = try BigSyncPersistedAccountScopeLease(persistedValue: raw)
            XCTAssertEqual(decoded.generation, generation)
            XCTAssertEqual(decoded.lease?.invalidationGeneration, generation)
        }
    }

    func testLeaseNumericFieldsRejectBooleanFractionAndOverflow() throws {
        for field in ["version", "generation"] {
            for invalid in [true, 1.75, -0.5, "1", NSNumber(value: UInt64.max), Double.infinity, Double.nan, NSNull()] as [Any] {
                var raw = validLease(); raw[field] = invalid
                XCTAssertThrowsError(try BigSyncPersistedAccountScopeLease(persistedValue: raw), "\(field) must be exact nonnegative integer")
            }
        }
    }

    func testLeaseValidityRequiresPersistedBoolean() throws {
        for invalid in [0, 1, 0.0, 1.0, NSNumber(value: 0), NSNumber(value: 1), "true", NSNull()] as [Any] {
            var raw = validLease(); raw["isValid"] = invalid
            XCTAssertThrowsError(try BigSyncPersistedAccountScopeLease(persistedValue: raw))
        }
    }

    func testLiveLeaseRejectsNonfiniteValidationDates() throws {
        for value in [Double.infinity, -.infinity, .nan] {
            var raw = validLease(); raw["validatedAt"] = Date(timeIntervalSinceReferenceDate: value)
            XCTAssertThrowsError(try BigSyncPersistedAccountScopeLease(persistedValue: raw))
        }
    }

    func testLiveLeaseRequiresCompleteIdentityAndDate() throws {
        for field in ["version", "generation", "isValid", "accountScopeIdentifier", "validatedAt"] {
            var raw = validLease(); raw.removeValue(forKey: field)
            XCTAssertThrowsError(try BigSyncPersistedAccountScopeLease(persistedValue: raw))
        }
        var raw = validLease(); raw["accountScopeIdentifier"] = ""
        XCTAssertThrowsError(try BigSyncPersistedAccountScopeLease(persistedValue: raw))
    }

    func testInvalidatedLeaseRetainsGenerationWithoutRevivingOldPayload() throws {
        var raw = validLease()
        raw["isValid"] = false; raw["accountScopeIdentifier"] = 7; raw["validatedAt"] = "stale"
        let decoded = try BigSyncPersistedAccountScopeLease(persistedValue: raw)
        XCTAssertEqual(decoded.generation, 7)
        XCTAssertNil(decoded.lease)
        raw.removeValue(forKey: "accountScopeIdentifier"); raw.removeValue(forKey: "validatedAt")
        XCTAssertNil(try BigSyncPersistedAccountScopeLease(persistedValue: raw).lease)
    }

    func testLeaseBinaryAndXMLRoundTripPreservesBooleanAndGeneration() throws {
        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
            let raw = validLease()
            let data = try PropertyListSerialization.data(fromPropertyList: raw, format: format, options: 0)
            let read = try PropertyListSerialization.propertyList(from: data, options: [], format: nil)
            let decoded = try BigSyncPersistedAccountScopeLease(persistedValue: read)
            XCTAssertEqual(decoded.generation, 7)
            XCTAssertEqual(decoded.lease?.accountScopeIdentifier, "owner-A")
        }
    }

    func testDurableReadErrorsCannotBecomeMissingBinding() throws {
        let store = try preparedStore()
        store.readError = StoreFailure.unavailable
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key)) {
            XCTAssertTrue($0 is StoreFailure)
        }
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.prepare(store: store, key: key, installationIdentifier: "install-A"))
        XCTAssertEqual(store.writes, 0)
    }
}
