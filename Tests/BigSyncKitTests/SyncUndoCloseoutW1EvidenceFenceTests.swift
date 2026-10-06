import CloudKit
import Foundation
import Logging
import RealmSwift
import XCTest
@_spi(CloudKitE2E) @testable import BigSyncKit


// These are committed-read tests, not permission to mutate another owner's
// transaction. The held writes use the exact operational W1 Realm handles;
// direct journal removal deliberately emulates an uncommitted acknowledgement.
extension SyncUndoCloseoutW1Tests {
    @BigSyncBackgroundActor
    func testPendingInventoryDoesNotExposeProvisionalJournalInsertion() async throws {
        let (adapter, realm, object, _) = try await acceptedNote()
        XCTAssertTrue(try adapter.pendingMutationInventory(
            entityTypes: [W1ContractNote.className()]).isEmpty)
        realm.beginWrite()
        defer { if realm.isInWriteTransaction { realm.cancelWrite() } }
        object.text = "provisional"
        object.refreshChangeMetadata(explicitlyModified: true,
            at: Date(timeIntervalSinceReferenceDate: 40))
