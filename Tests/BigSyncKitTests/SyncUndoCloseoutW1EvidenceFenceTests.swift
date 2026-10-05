import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@testable import BigSyncKit

extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testInvalidatedFenceCannotCertifyUnjournaledLiveObject() async throws {
        let (adapter, realm, object, incoming) = try await acceptedNote()
        let values = try BigSyncRecordFingerprint.fields(of: object)
        let baseline = try XCTUnwrap(realm.objects(BigSyncRecordBaseline.self).first)
        // Deliberate evidence corruption only: production disappearance keeps
        // the live recreation's journal in the same target transaction.
        try realm.write {
            BigSyncRecordBaseline.invalidate(recordName: incoming.recordID.recordName, in: realm)
        }
        let revision = baseline.revision
