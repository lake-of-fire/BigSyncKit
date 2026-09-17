import RealmSwift
@testable import BigSyncKit

/// The red tier uses the existing journal-enabled adapter with its existing schema.
/// The green tier changes only this fixture's schema/policy opt-in to the real
/// record-rebasing feature. Assertions and operation order stay unchanged.
func configureReviewDisjointFieldFixture(_ configuration: inout Realm.Configuration) -> [String] {
    []
}
