import Foundation
import XCTest
@testable import BigSyncKit

final class BigSyncRecordConflictPolicyTests: XCTestCase {
    func testMissingTargetHasNoCompetingConstructorClock() {
        let dates: [Date?] = [nil, .distantPast, Date(timeIntervalSince1970: 0),
                             Date(timeIntervalSince1970: 200), .distantFuture]
        for remoteExplicit in dates {
            for remoteModified in dates {
                for localExplicit in dates {
                    for localModified in dates {
                        XCTAssertTrue(BigSyncRecordConflictPolicy.acceptsRemote(
                            remoteExplicitlyModifiedAt: remoteExplicit,
                            remoteModifiedAt: remoteModified,
                            localExplicitlyModifiedAt: localExplicit,
                            localModifiedAt: localModified,
                            localObjectExists: false))
                    }
                }
            }
        }
    }

    func testExistingTargetsKeepTheOriginalLexicographicClockRule() {
        let dates: [Date?] = [nil, .distantPast, Date(timeIntervalSince1970: 0),
                             Date(timeIntervalSince1970: 200), .distantFuture]
        for remoteExplicit in dates {
            for remoteModified in dates {
                for localExplicit in dates {
                    for localModified in dates {
                        let remote = (remoteExplicit ?? .distantPast, remoteModified ?? .distantPast)
                        let local = (localExplicit ?? .distantPast, localModified ?? .distantPast)
                        let expected = remote >= local
                        XCTAssertEqual(BigSyncRecordConflictPolicy.acceptsRemote(
                            remoteExplicitlyModifiedAt: remoteExplicit,
                            remoteModifiedAt: remoteModified,
                            localExplicitlyModifiedAt: localExplicit,
                            localModifiedAt: localModified,
                            localObjectExists: true), expected)
                        XCTAssertEqual(BigSyncRecordConflictPolicy.acceptsRemote(
                            remoteExplicitlyModifiedAt: remoteExplicit,
                            remoteModifiedAt: remoteModified,
                            localExplicitlyModifiedAt: localExplicit,
                            localModifiedAt: localModified), expected)
                    }
                }
            }
        }
    }

    func testMissingExplicitDateIsNotTheSameAsMissingTarget() {
        let remote = Date(timeIntervalSince1970: 100)
        let local = Date(timeIntervalSince1970: 200)
        XCTAssertFalse(BigSyncRecordConflictPolicy.acceptsRemote(
            remoteExplicitlyModifiedAt: nil, remoteModifiedAt: remote,
            localExplicitlyModifiedAt: nil, localModifiedAt: local))
        XCTAssertTrue(BigSyncRecordConflictPolicy.acceptsRemote(
            remoteExplicitlyModifiedAt: nil, remoteModifiedAt: remote,
            localExplicitlyModifiedAt: nil, localModifiedAt: local, localObjectExists: false))
    }
}
