import CryptoKit
import Foundation
import XCTest
@testable import BigSyncKit

/// Deterministic faults at the existing store interface, not a filesystem or
/// Realm substitute. Instances are confined to one test's sequential operations.
final class AccountAuthorityTestStore: NSObject, DurableKeyValueStore, @unchecked Sendable {
    enum Fault: Error, Equatable {
        case read, beforeWrite, afterWrite, readAfterWrite, discardWrite, replaceWrite
    }
    var values = [String: Any]()
    var fault: Fault?
    var faultKey: String?
    var replacement: Any?
    private(set) var writes = 0
    private(set) var reads = 0
    private(set) var legacyReads = 0
    private(set) var legacyWrites = 0
    private var acceptedWrite = false

    func arm(_ fault: Fault?, forKey key: String) {
        self.fault = fault
        faultKey = key
        acceptedWrite = false
    }
    func object(forKey key: String) -> Any? { legacyReads += 1; return values[key] }
    func bool(forKey key: String) -> Bool { values[key] as? Bool ?? false }
    func set(value: Any?, forKey key: String) { legacyWrites += 1; values[key] = value }
    func set(boolValue: Bool, forKey key: String) { set(value: boolValue, forKey: key) }
    func removeObject(forKey key: String) { set(value: nil, forKey: key) }
    func synchronize() -> Bool { true }
    func prepareForUse() throws {}
    func validateDurability() throws {}
    func durableObject(forKey key: String) throws -> Any? {
        reads += 1
        if key == faultKey {
            if fault == .read { throw Fault.read }
            if fault == .readAfterWrite && acceptedWrite { throw Fault.readAfterWrite }
        }
        return values[key]
    }
    func setDurably(value: Any?, forKey key: String) throws {
        writes += 1
        if key == faultKey {
            if fault == .beforeWrite { throw Fault.beforeWrite }
            if fault == .discardWrite { return }
            if fault == .replaceWrite { values[key] = replacement; return }
        }
        values[key] = value
        acceptedWrite = true
        if key == faultKey, fault == .afterWrite { throw Fault.afterWrite }
    }
    func removeDurably(forKey key: String) throws { try setDurably(value: nil, forKey: key) }
}

final class AccountAuthorityPersistenceTests: XCTestCase {
    private let key = "binding"
    private let date = Date(timeIntervalSince1970: 1_700_000_000)

    private func prepared(_ store: AccountAuthorityTestStore) throws -> BigSyncReplicaBindingSnapshot {
        try BigSyncReplicaBindingStateStore.prepare(store: store, key: key,
                                                    installationIdentifier: "installation")
    }
    private func owned(_ store: AccountAuthorityTestStore) throws -> BigSyncReplicaBindingSnapshot {
        _ = try prepared(store)
        return try BigSyncReplicaBindingStateStore.bindInitialAccount("A", store: store, key: key)
    }
    private func port(_ store: AccountAuthorityTestStore) throws -> BigSyncCloudAccountPortRequirement {
        try BigSyncReplicaBindingStateStore.requirePort(sourceAccountScopeIdentifier: "A",
                                                         destinationAccountScopeIdentifier: "B",
                                                         store: store, key: key)
    }
    private func fields(_ store: AccountAuthorityTestStore) throws -> [String: Any] {
        try XCTUnwrap(store.values[key] as? [String: Any])
    }
    private func roundTrip(_ value: [String: Any], _ format: PropertyListSerialization.PropertyListFormat) throws -> [String: Any] {
        let data = try PropertyListSerialization.data(fromPropertyList: value, format: format, options: 0)
        return try XCTUnwrap(PropertyListSerialization.propertyList(from: data, format: nil) as? [String: Any])
    }
    private func assertCorruptBinding(_ value: [String: Any], file: StaticString = #filePath, line: UInt = #line) {
        let store = AccountAuthorityTestStore()
        store.values[key] = value
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key), file: file, line: line) {
            XCTAssertEqual($0 as? BigSyncReplicaBindingError, .corrupt, file: file, line: line)
        }
        XCTAssertThrowsError(try prepared(store), file: file, line: line)
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("unrelated", store: store, key: key), file: file, line: line)
        XCTAssertEqual(store.writes, 0, file: file, line: line)
        XCTAssertEqual(store.legacyReads, 0, file: file, line: line)
        XCTAssertTrue(NSDictionary(dictionary: value).isEqual(NSDictionary(dictionary: store.values[key] as? [String: Any] ?? [:])), file: file, line: line)
    }

    func testBindingLifecycleKeepsExactOwnerAndPendingGenerationAcrossReloads() throws {
        let store = AccountAuthorityTestStore()
        XCTAssertNil(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        let first = try prepared(store)
        let initiallyOwned = try owned(store)
        XCTAssertEqual(initiallyOwned.activeGenerationIdentifier, first.activeGenerationIdentifier)
        let pending = try port(store)
        let writes = store.writes
        XCTAssertEqual(try port(store), pending)
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.requirePort(
            sourceAccountScopeIdentifier: "A", destinationAccountScopeIdentifier: "C", store: store, key: key), pending)
        XCTAssertEqual(store.writes, writes, "A third account cannot replace the first pending generation")
        let pendingState = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
        let raw = try fields(store)
        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
            // XML stores whole-second dates. Match that supported representation
            // explicitly rather than claiming fractional XML time is lossless.
            var value = raw
            value["pendingDetectedAt"] = date
            store.values[key] = try roundTrip(value, format)
            let decoded = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
            XCTAssertEqual(decoded.mutationGenerationIdentifier, pendingState.mutationGenerationIdentifier)
            XCTAssertEqual(decoded.pendingPort?.detectedAt, date)
        }
        store.values[key] = raw
        let activeB = try BigSyncReplicaBindingStateStore.activatePort(pending, store: store, key: key)
        XCTAssertEqual(activeB.activeAccountScopeIdentifier, "B")
        XCTAssertEqual(activeB.activeGenerationIdentifier, pending.bindingGenerationIdentifier)
        XCTAssertNil(activeB.pendingPort)
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.cancelPort(pending, store: store, key: key))
        let restored = try BigSyncReplicaBindingStateStore.prepare(store: store, key: key,
                                                                  installationIdentifier: "restored-installation")
        XCTAssertNil(restored.activeAccountScopeIdentifier)
        XCTAssertEqual(restored.restoredDatasetOwnerAccountScopeIdentifier, "B")
        XCTAssertNotEqual(restored.activeGenerationIdentifier, activeB.activeGenerationIdentifier)
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("A", store: store, key: key))
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.bindInitialAccount("B", store: store, key: key).activeAccountScopeIdentifier, "B")
        XCTAssertEqual(store.legacyReads, 0)
        XCTAssertEqual(store.legacyWrites, 0)
    }

    func testMalformedPresentOwnerCannotBecomeUnownedInitialBinding() throws {
        let store = AccountAuthorityTestStore()
        _ = try prepared(store)
        let initial = try fields(store)
        for name in ["activeAccountScopeIdentifier", "restoredDatasetOwnerAccountScopeIdentifier"] {
            for invalid in [NSNull(), true, false, 7, Data(), ["A"], ["account": "A"], ""] as [Any] {
                var value = initial
                value[name] = invalid
                assertCorruptBinding(value)
            }
        }
        var both = initial
        both["activeAccountScopeIdentifier"] = "A"
        both["restoredDatasetOwnerAccountScopeIdentifier"] = "A"
        assertCorruptBinding(both)
        // Genuinely absent optional owners still denote a fresh binding.
        store.values[key] = initial
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.bindInitialAccount("A", store: store, key: key).activeAccountScopeIdentifier, "A")
    }

    func testBindingVersionAndPendingFieldsMustRetainTheirExactMeaning() throws {
        let store = AccountAuthorityTestStore()
        _ = try owned(store)
        _ = try port(store)
        let original = try fields(store)
        for bad in [1.5, -1, true, false, "1", NSNull(), Double.nan, Double.infinity, UInt64.max] as [Any] {
            var value = original
            value["version"] = bad
            assertCorruptBinding(value)
        }
        for number in [NSNumber(value: Int8(1)), NSNumber(value: Int64(1)), NSNumber(value: UInt64(1)), NSNumber(value: 1.0)] {
            var value = original
            value["version"] = number
            store.values[key] = value
            XCTAssertNotNil(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        }
        for field in ["pendingTransitionID", "pendingBindingGenerationIdentifier", "pendingSourceAccountScopeIdentifier", "pendingDestinationAccountScopeIdentifier", "pendingDetectedAt"] {
            var value = original
            value.removeValue(forKey: field)
            assertCorruptBinding(value)
        }
        for invalidDate in [Date(timeIntervalSinceReferenceDate: .nan), Date(timeIntervalSinceReferenceDate: .infinity), Date(timeIntervalSinceReferenceDate: -.infinity)] {
            var value = original
            value["pendingDetectedAt"] = invalidDate
            // NaN is not reflexively equal, so assert the admission result only.
            store.values[key] = value
            XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.load(store: store, key: key))
        }
        var sameAccount = original
        sameAccount["pendingDestinationAccountScopeIdentifier"] = "A"
        let transition = try XCTUnwrap(UUID(uuidString: original["pendingTransitionID"] as? String ?? ""))
        let inputs = ["pending-replica-binding", try XCTUnwrap(original["activeGenerationIdentifier"] as? String), transition.uuidString.lowercased(), "A", "A"]
        var bytes = Data()
        for input in inputs {
            var count = UInt64(input.utf8.count).bigEndian
            withUnsafeBytes(of: &count) { bytes.append(contentsOf: $0) }
            bytes.append(contentsOf: input.utf8)
        }
        sameAccount["pendingBindingGenerationIdentifier"] = SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
        assertCorruptBinding(sameAccount)
    }

    func testBindingWriteRequiresReadbackAndDoesNotCompensateAnAcceptedSuccessor() throws {
        for fault in [AccountAuthorityTestStore.Fault.beforeWrite, .afterWrite, .readAfterWrite, .discardWrite] {
            let store = AccountAuthorityTestStore()
            let old = try owned(store)
            let oldWrites = store.writes
            store.arm(fault, forKey: key)
            XCTAssertThrowsError(try port(store))
            XCTAssertEqual(store.writes, oldWrites + 1, "An uncertain write must not be rolled back with a second write")
            let acceptedRaw = try fields(store)
            store.arm(nil, forKey: key)
            let after = try XCTUnwrap(BigSyncReplicaBindingStateStore.load(store: store, key: key))
            let retry = try port(store)
            if fault == .afterWrite || fault == .readAfterWrite {
                XCTAssertEqual(retry, after.pendingPort)
                XCTAssertEqual(store.writes, oldWrites + 1, "Accepted identity must be reused on retry")
                XCTAssertTrue(NSDictionary(dictionary: acceptedRaw).isEqual(NSDictionary(dictionary: try fields(store))))
            } else {
                XCTAssertEqual(after, old)
                XCTAssertEqual(store.writes, oldWrites + 2)
            }
        }
        let store = AccountAuthorityTestStore()
        let initial = try prepared(store)
        store.replacement = try fields(store) // Return a valid but different (unbound) value.
        store.arm(.replaceWrite, forKey: key)
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.bindInitialAccount("A", store: store, key: key)) {
            XCTAssertTrue($0 is DurableKeyValueStoreError)
        }
        store.arm(nil, forKey: key)
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.load(store: store, key: key), initial)
    }

    func testUnreadableBindingDoesNotBecomeFirstUseAndNoOpsDoNotWrite() throws {
        let store = AccountAuthorityTestStore()
        let initial = try prepared(store)
        let writes = store.writes
        store.arm(.read, forKey: key)
        XCTAssertThrowsError(try prepared(store)) { XCTAssertEqual($0 as? AccountAuthorityTestStore.Fault, .read) }
        XCTAssertEqual(store.writes, writes)
        store.arm(nil, forKey: key)
        XCTAssertEqual(try prepared(store), initial)
        XCTAssertEqual(store.writes, writes)
        _ = try owned(store)
        let ownedWrites = store.writes
        _ = try BigSyncReplicaBindingStateStore.bindInitialAccount("A", store: store, key: key)
        XCTAssertEqual(store.writes, ownedWrites)
        XCTAssertEqual(store.legacyReads, 0)
    }

    func testLeaseValidAndInvalidatedShapesRoundTripWithoutChangingGeneration() throws {
        for generation in [Int64(0), 1, 42, Int64.max] {
            for active in [false, true] {
                let value = try BigSyncAccountScopeLeaseState(generation: generation,
                    accountScopeIdentifier: active ? "A" : nil, validatedAt: active ? date : nil)
                for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
                    let decoded = try BigSyncAccountScopeLeaseState(propertyList: roundTrip(value.propertyList, format))
                    XCTAssertEqual(decoded, value)
                    XCTAssertEqual(decoded.lease?.invalidationGeneration, active ? generation : nil)
                }
                let store = AccountAuthorityTestStore()
                try value.persist(store: store, key: "lease")
                XCTAssertEqual(try BigSyncAccountScopeLeaseState.load(store: store, key: "lease"), value)
                XCTAssertEqual(store.legacyReads, 0)
                XCTAssertEqual(store.legacyWrites, 0)
            }
        }
        let store = AccountAuthorityTestStore()
        let missing = try BigSyncAccountScopeLeaseState.load(store: store, key: "lease")
        XCTAssertEqual(missing.generation, 0)
        XCTAssertNil(missing.lease)
        XCTAssertEqual(store.writes, 0)
        // Missing storage is equivalent to the initial *read* state, but it
        // must not count as readback proof that a requested write persisted.
        store.arm(.discardWrite, forKey: "lease")
        XCTAssertThrowsError(try missing.persist(store: store, key: "lease")) {
            XCTAssertEqual($0 as? DurableKeyValueStoreError, .mutationNotDurable)
        }
        XCTAssertNil(store.values["lease"])
    }

    func testLeaseRejectsConvertedGenerationsFlagsAndContradictoryInvalidation() throws {
        let valid = try BigSyncAccountScopeLeaseState(generation: 4, accountScopeIdentifier: "A", validatedAt: date).propertyList
        for bad in [4.9, -1, true, false, "4", NSNull(), Double.nan, Double.infinity, UInt64.max] as [Any] {
            var fields = valid
            fields["generation"] = bad
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields))
        }
        for bad in [1.5, true, false, "1", NSNull(), Double.nan, Double.infinity, UInt64.max] as [Any] {
            var fields = valid
            fields["version"] = bad
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields))
        }
        for bad in [0, 1, 2, -1, "true", NSNull(), 1.0] as [Any] {
            var fields = valid
            fields["isValid"] = bad
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields), "A numeric flag is not a property-list Boolean")
        }
        for field in ["accountScopeIdentifier", "validatedAt"] {
            var fields = valid
            fields.removeValue(forKey: field)
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields))
            fields = try BigSyncAccountScopeLeaseState(generation: 4, accountScopeIdentifier: nil, validatedAt: nil).propertyList
            fields[field] = valid[field]
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields))
        }
        for bad in [Date(timeIntervalSinceReferenceDate: .nan), Date(timeIntervalSinceReferenceDate: .infinity)] {
            var fields = valid
            fields["validatedAt"] = bad
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(propertyList: fields))
        }
        var exact = valid
        exact["generation"] = NSNumber(value: 4.0)
        exact["version"] = NSNumber(value: 1.0)
        XCTAssertEqual(try BigSyncAccountScopeLeaseState(propertyList: exact).generation, 4)
        XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(generation: -1, accountScopeIdentifier: nil, validatedAt: nil))
        XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(generation: 1, accountScopeIdentifier: "", validatedAt: date))
        XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(generation: 1, accountScopeIdentifier: "A", validatedAt: nil))
        XCTAssertThrowsError(try BigSyncAccountScopeLeaseState(generation: 1, accountScopeIdentifier: nil, validatedAt: date))
    }

    func testLeaseUncertainInvalidationRetainsAcceptedGenerationAndReadErrorsPropagate() throws {
        let old = try BigSyncAccountScopeLeaseState(generation: 6, accountScopeIdentifier: "A", validatedAt: date)
        let next = try BigSyncAccountScopeLeaseState(generation: 7, accountScopeIdentifier: nil, validatedAt: nil)
        for fault in [AccountAuthorityTestStore.Fault.beforeWrite, .afterWrite, .readAfterWrite, .discardWrite, .replaceWrite] {
            let store = AccountAuthorityTestStore()
            try old.persist(store: store, key: "lease")
            store.replacement = old.propertyList
            let writes = store.writes
            store.arm(fault, forKey: "lease")
            XCTAssertThrowsError(try next.persist(store: store, key: "lease"))
            XCTAssertEqual(store.writes, writes + 1)
            store.arm(nil, forKey: "lease")
            let actual = try BigSyncAccountScopeLeaseState.load(store: store, key: "lease")
            XCTAssertEqual(actual, fault == .afterWrite || fault == .readAfterWrite ? next : old)
            store.arm(.read, forKey: "lease")
            XCTAssertThrowsError(try BigSyncAccountScopeLeaseState.load(store: store, key: "lease")) {
                XCTAssertEqual($0 as? AccountAuthorityTestStore.Fault, .read)
            }
            XCTAssertEqual(store.writes, writes + 1)
            XCTAssertEqual(store.legacyReads, 0)
        }
    }

    func testDistinctStoreKeysDoNotBorrowOwnerOrLeaseAndStalePortCannotCancelNewOne() throws {
        let store = AccountAuthorityTestStore()
        _ = try owned(store)
        let first = try port(store)
        _ = try BigSyncReplicaBindingStateStore.cancelPort(first, store: store, key: key)
        let next = try port(store)
        XCTAssertNotEqual(next.bindingGenerationIdentifier, first.bindingGenerationIdentifier)
        let writes = store.writes
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.cancelPort(first, store: store, key: key))
        XCTAssertThrowsError(try BigSyncReplicaBindingStateStore.activatePort(first, store: store, key: key))
        XCTAssertEqual(store.writes, writes)
        XCTAssertEqual(try BigSyncReplicaBindingStateStore.load(store: store, key: key)?.pendingPort, next)
        XCTAssertNil(try BigSyncReplicaBindingStateStore.load(store: store, key: "other"))
        XCTAssertNil(try BigSyncAccountScopeLeaseState.load(store: store, key: "other").lease)
    }
}
