import RealmSwift
@testable import BigSyncKit

/// Explicitly opt this fixture into the same real feature used by the composed
/// application. The regression operations and assertions are unchanged from red.
extension ReviewDisjointFieldRow: BigSyncRecordRebasePolicyProviding {
    static var bigSyncRecordRebasePolicy: BigSyncRecordRebasePolicy { .independentFields }
}

func configureReviewDisjointFieldFixture(_ configuration: inout Realm.Configuration) -> [String] {
    BigSyncMutationPolicy.enableRecordRebasing(in: &configuration)
    return [BigSyncRecordBaseline.className()]
}
