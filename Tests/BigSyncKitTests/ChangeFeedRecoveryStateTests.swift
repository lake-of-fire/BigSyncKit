import Foundation
import XCTest
@testable import BigSyncKit

/// Exercises the actual durable envelope and request policy. No Realm or
/// CloudKit substitute is involved; orchestration has separate native tests.
final class ChangeFeedRecoveryStateTests: XCTestCase {
    private let eventA = "00000000-0000-4000-8000-000000000001"
    private let eventB = "00000000-0000-4000-8000-000000000002"
    private let modes: [ChangeFeedResetMode] = [
        .initialImport, .serverReconciliation, .backupRestore,
        .encryptedDataReset, .localDatasetRebootstrap,
    ]
    private let phases: [ChangeFeedMigrationState.Phase] = [
        .requested, .prepared, .serverBootstrap, .reconciled, .finishing, .completed,
    ]

    private func state(_ mode: ChangeFeedResetMode,
                       phase: ChangeFeedMigrationState.Phase = .serverBootstrap,
                       event: String? = nil,
                       handoff: BigSyncReplicaJournalHandoff? = nil) -> ChangeFeedMigrationState {
        .init(key: "recovery-key", accountScopeIdentifier: "account",
              zoneName: "zone", zoneOwnerName: "owner",
              epoch: ChangeFeedMigrationState.initialEpoch + 7,
              mode: mode, phase: phase,
              backupRestoreEventIdentifier: mode == .backupRestore ? (event ?? eventA) : event,
              replicaJournalHandoff: handoff)
    }

    private func request(_ mode: ChangeFeedResetMode,
                         over current: ChangeFeedMigrationState?,
                         event: String? = nil,
                         handoff: BigSyncReplicaJournalHandoff? = nil) throws -> ChangeFeedMigrationState {
        try .requesting(current: current, key: "recovery-key",
                        accountScopeIdentifier: "account", zoneName: "zone", zoneOwnerName: "owner",
                        mode: mode, backupRestoreEventIdentifier: event,
                        replicaJournalHandoff: handoff)
    }

    private func decode(_ fields: [String: Any]) -> ChangeFeedMigrationState? {
        .init(key: "recovery-key", propertyList: fields,
              accountScopeIdentifier: "account", zoneName: "zone", zoneOwnerName: "owner")
    }

    func testEveryValidEnvelopeRoundTripsThroughBinaryAndXMLPropertyLists() throws {
        let handoff = try BigSyncReplicaJournalHandoff(
            installationIdentifier: "installation",
            retiringBindingGenerationIdentifiers: ["B", "A"],
            destinationBindingGenerationIdentifier: "C")
        for mode in modes {
            let events: [String?] = mode == .encryptedDataReset ? [nil, eventA] : [nil]
            let handoffs: [BigSyncReplicaJournalHandoff?] = mode.reuploadsRetainedLocalData ? [nil, handoff] : [nil]
            for phase in phases {
                for event in events {
                    for handoff in handoffs {
                        let original = state(mode, phase: phase, event: event, handoff: handoff)
                        for format in [PropertyListSerialization.PropertyListFormat.binary, .xml] {
                            let bytes = try PropertyListSerialization.data(fromPropertyList: original.propertyList,
                                                                           format: format, options: 0)
                            let fields = try XCTUnwrap(PropertyListSerialization.propertyList(from: bytes,
                                                                                              format: nil) as? [String: Any])
                            XCTAssertEqual(decode(fields), original)
                        }
                    }
                }
            }
        }
    }

    func testEnvelopeDoesNotTruncateVersionEpochOrOptionalRestoreIdentity() throws {
        let valid = state(.serverReconciliation).propertyList
        let malformedNumbers: [Any] = ["3", true, false, NSNull(), NSNumber(value: Double.nan),
                                      NSNumber(value: Double.infinity), NSNumber(value: UInt64.max)]
        for key in ["version", "epoch"] {
            for value in malformedNumbers {
                var fields = valid; fields[key] = value
                XCTAssertNil(decode(fields), "Invalid \(key): \(value)")
            }
            var fields = valid
            fields[key] = key == "version" ? NSNumber(value: 3.75) : NSNumber(value: Double(ChangeFeedMigrationState.initialEpoch) + 0.5)
            XCTAssertNil(decode(fields), "A fractional identity must not be rounded to an existing epoch")
        }
        var exactIntegral = valid
        exactIntegral["version"] = NSNumber(value: 3.0)
        exactIntegral["epoch"] = NSNumber(value: Double(ChangeFeedMigrationState.initialEpoch + 7))
        XCTAssertNotNil(decode(exactIntegral))
        for mode in modes {
            for value in [NSNull(), 7, "", "not-a-uuid"] as [Any] {
                var fields = state(mode).propertyList
                fields["backupRestoreEventIdentifier"] = value
                XCTAssertNil(decode(fields))
            }
            if mode != .backupRestore && mode != .encryptedDataReset {
                var fields = state(mode).propertyList; fields["backupRestoreEventIdentifier"] = eventA
                XCTAssertNil(decode(fields), "Only restore or its encrypted escalation may consume the event")
            }
        }
        for key in ["mode", "phase", "accountScopeIdentifier", "zoneName", "zoneOwnerName"] {
            var fields = valid; fields[key] = "unknown"
            XCTAssertNil(decode(fields))
        }
        var restore = state(.backupRestore).propertyList
        restore.removeValue(forKey: "backupRestoreEventIdentifier")
        XCTAssertNil(decode(restore))
        for raw in [NSNull(), "invalid", ["installationIdentifier": "installation"]] as [Any] {
            var fields = state(.encryptedDataReset).propertyList; fields["replicaJournalHandoff"] = raw
            XCTAssertNil(decode(fields))
        }
    }

    func testEncryptedEscalationRetainsRestoreEventAndSameEventRetryCannotDowngradeIt() throws {
        for phase in phases where phase != .requested && phase != .completed {
            let prior = state(.backupRestore, phase: phase)
            let encrypted = try request(.encryptedDataReset, over: prior)
            XCTAssertEqual(encrypted.mode, .encryptedDataReset)
            XCTAssertEqual(encrypted.backupRestoreEventIdentifier, eventA)
            XCTAssertEqual(encrypted.epoch, prior.epoch + 1)
            for resumedPhase in phases {
                var resumed = encrypted; resumed.phase = resumedPhase
                let reopened = try XCTUnwrap(decode(resumed.propertyList))
                XCTAssertEqual(try request(.backupRestore, over: reopened, event: eventA), resumed)
                if resumedPhase != .completed {
                    XCTAssertEqual(try request(.serverReconciliation, over: reopened), resumed)
                    XCTAssertEqual(try request(.initialImport, over: reopened), resumed)
                    XCTAssertEqual(try request(.encryptedDataReset, over: reopened), resumed)
                }
            }
        }
        XCTAssertThrowsError(try request(.encryptedDataReset, over: state(.backupRestore, phase: .requested))) {
            XCTAssertEqual($0 as? ChangeFeedMigrationPersistenceError, .restorePreparationRequired)
        }
    }

    func testNewRestoreEventSupersedesOldEncryptedRecoveryWithoutContinuingOldOwnerHandoff() throws {
        let handoff = try BigSyncReplicaJournalHandoff(installationIdentifier: "old-installation",
                                                       retiringBindingGenerationIdentifiers: ["A"],
                                                       destinationBindingGenerationIdentifier: "B")
        for phase in phases {
            let previous = state(.encryptedDataReset, phase: phase, event: eventA, handoff: handoff)
            let next = try request(.backupRestore, over: previous, event: eventB)
            XCTAssertEqual(next.mode, .backupRestore)
            XCTAssertEqual(next.phase, .requested)
            XCTAssertEqual(next.backupRestoreEventIdentifier, eventB)
            XCTAssertEqual(next.epoch, previous.epoch + 1)
            XCTAssertNil(next.replicaJournalHandoff)
        }
    }

    func testFirstAndSubsequentHandoffsSurviveEncryptedRecoveryAndRepeatedRequests() throws {
        let ab = try BigSyncReplicaJournalHandoff(installationIdentifier: "installation",
                                                 retiringBindingGenerationIdentifiers: ["A"],
                                                 destinationBindingGenerationIdentifier: "B")
        let bc = try BigSyncReplicaJournalHandoff(installationIdentifier: "installation",
                                                 retiringBindingGenerationIdentifiers: ["B"],
                                                 destinationBindingGenerationIdentifier: "C")
        for mode in [ChangeFeedResetMode.localDatasetRebootstrap, .encryptedDataReset] {
            let previous = state(.encryptedDataReset, event: eventA)
            let first = try request(mode, over: previous, handoff: ab)
            XCTAssertEqual(first.mode, .encryptedDataReset)
            XCTAssertEqual(first.backupRestoreEventIdentifier, eventA)
            XCTAssertEqual(first.replicaJournalHandoff, ab)
            XCTAssertEqual(first.epoch, previous.epoch + 1)
            XCTAssertEqual(try request(mode, over: first, handoff: ab), first)
            let next = try request(mode, over: first, handoff: bc)
            XCTAssertEqual(next.replicaJournalHandoff?.retiringBindingGenerationIdentifiers, ["A", "B"])
            XCTAssertEqual(next.replicaJournalHandoff?.destinationBindingGenerationIdentifier, "C")
            XCTAssertEqual(next.backupRestoreEventIdentifier, eventA)
            XCTAssertEqual(next.epoch, first.epoch + 1)
            XCTAssertEqual(try request(.serverReconciliation, over: next), next)
            XCTAssertEqual(try request(.backupRestore, over: next, event: eventA), next)
        }
    }

    func testModePriorityAndCompletedEpochsKeepTheirExistingContracts() throws {
        // Rows are existing unfinished mode; columns are the requested mode.
        let expected: [[ChangeFeedResetMode]] = [
            [.initialImport, .serverReconciliation, .backupRestore, .encryptedDataReset, .localDatasetRebootstrap],
            [.initialImport, .serverReconciliation, .backupRestore, .encryptedDataReset, .localDatasetRebootstrap],
            [.backupRestore, .backupRestore, .backupRestore, .encryptedDataReset, .backupRestore],
            [.encryptedDataReset, .encryptedDataReset, .backupRestore, .encryptedDataReset, .encryptedDataReset],
            [.localDatasetRebootstrap, .localDatasetRebootstrap, .backupRestore, .encryptedDataReset, .localDatasetRebootstrap],
        ]
        for (row, oldMode) in modes.enumerated() {
            for phase in phases {
                for (column, requestedMode) in modes.enumerated() {
                    let prior = state(oldMode, phase: phase)
                    let event = requestedMode == .backupRestore ? eventA : nil
                    if oldMode == .backupRestore && phase == .requested && requestedMode == .encryptedDataReset {
                        XCTAssertThrowsError(try request(requestedMode, over: prior, event: event))
                        continue
                    }
                    let next = try request(requestedMode, over: prior, event: event)
                    let wanted = phase == .completed ? requestedMode : expected[row][column]
                    XCTAssertEqual(next.mode, wanted)
                    XCTAssertNotNil(decode(next.propertyList))
                    if next == prior {
                        XCTAssertEqual(next.epoch, prior.epoch)
                    } else {
                        XCTAssertEqual(next.epoch, prior.epoch + 1)
                        XCTAssertEqual(next.phase, .requested)
                    }
                }
            }
        }
    }

    func testNamespaceInvalidRequestsAndEpochOverflowFailWithoutInventingAnEnvelope() throws {
        let prior = state(.serverReconciliation)
        for component in 0..<4 {
            XCTAssertThrowsError(try ChangeFeedMigrationState.requesting(
                current: prior, key: component == 0 ? "other" : prior.key,
                accountScopeIdentifier: component == 1 ? "other" : prior.accountScopeIdentifier,
                zoneName: component == 2 ? "other" : prior.zoneName,
                zoneOwnerName: component == 3 ? "other" : prior.zoneOwnerName,
                mode: .encryptedDataReset))
        }
        XCTAssertThrowsError(try request(.backupRestore, over: prior))
        XCTAssertThrowsError(try request(.serverReconciliation, over: prior, event: eventA))
        let exhausted = ChangeFeedMigrationState(key: prior.key, accountScopeIdentifier: prior.accountScopeIdentifier,
                                                zoneName: prior.zoneName, zoneOwnerName: prior.zoneOwnerName,
                                                epoch: Int.max, mode: .serverReconciliation, phase: .completed)
        XCTAssertThrowsError(try request(.encryptedDataReset, over: exhausted)) {
            XCTAssertEqual($0 as? ChangeFeedMigrationPersistenceError, .stateNotDurable)
        }
        let first = try request(.serverReconciliation, over: nil)
        XCTAssertEqual(first.epoch, ChangeFeedMigrationState.initialEpoch)
    }
}
