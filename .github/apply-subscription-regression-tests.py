#!/usr/bin/env python3
from pathlib import Path
import subprocess

BIG = Path("Tests/BigSyncKitTests/BigSyncKitTests.swift")
HOTFIX = Path("Tests/BigSyncKitTests/HotfixSubscriptionSafetyTests.swift")
EXPECTED_BIG_BLOB = "2b91673cc48f5a1bd488cb015eff69543e41b5f5"
EXPECTED_HOTFIX_BLOB = "f89bfd3b252592c0df270497abe082add3e05e8e"


def blob(path: Path) -> str:
    return subprocess.check_output(["git", "hash-object", str(path)], text=True).strip()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one preimage, found {count}")
    return text.replace(old, new, 1)


if blob(BIG) != EXPECTED_BIG_BLOB:
    raise SystemExit(f"unexpected BigSyncKitTests preimage: {blob(BIG)}")
if blob(HOTFIX) != EXPECTED_HOTFIX_BLOB:
    raise SystemExit(f"unexpected HotfixSubscriptionSafetyTests preimage: {blob(HOTFIX)}")

big = BIG.read_text()
big = replace_once(
    big,
    '''    @BigSyncBackgroundActor
    func testDatabaseSubscriptionIsSavedExactlyOnce() async {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)

        for _ in 0..<2 {
            await withCheckedContinuation { continuation in
                synchronizer.subscribeForChangesInDatabase { error in
                    XCTAssertNil(error)
                    continuation.resume()
                }
            }
        }

        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertEqual(database.modifySubscriptionOperationCount, 0)
    }
''',
    '''    @BigSyncBackgroundActor
    func testDatabaseSubscriptionIsSavedExactlyOnce() async {
        let database = FakeCloudKitDatabase()
        let synchronizer = makeSynchronizer(database: database)

        await withCheckedContinuation { continuation in
            synchronizer.subscribeForChangesInDatabase { error in
                XCTAssertNil(error)
                continuation.resume()
            }
        }
        // The broad fake keeps lookup results separate from save bookkeeping.
        // After cache revalidation became mandatory, make the successful save
        // visible to the next exact server lookup just as CloudKit would.
        database.fetchedSubscriptions = database.savedSubscriptions
        await withCheckedContinuation { continuation in
            synchronizer.subscribeForChangesInDatabase { error in
                XCTAssertNil(error)
                continuation.resume()
            }
        }

        XCTAssertEqual(database.savedSubscriptionCount, 1)
        XCTAssertEqual(database.modifySubscriptionOperationCount, 0)
    }
''',
    "database subscription repeat fixture",
)

big = replace_once(
    big,
    '''        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        database.subscriptionDeleteError =
            TestSynchronizationError.subscriptionMutationFailed
''',
    '''        let identifier = try XCTUnwrap(
            synchronizer.subscriptionIDForDatabaseSubscription()
        )
        // Cancellation now proves exact deterministic-ID ownership with a
        // server lookup before deleting. Model the subscription that the
        // preceding successful save actually created on CloudKit.
        database.fetchedSubscriptions = database.savedSubscriptions
        database.subscriptionDeleteError =
            TestSynchronizationError.subscriptionMutationFailed
''',
    "delete-failure server fixture",
)

big = replace_once(
    big,
    '''        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        database.accountIdentifierAfterNextSubscriptionDelete = "account-b"
''',
    '''        let identifier = try XCTUnwrap(
            synchronizer.subscriptionID(forRecordZoneID: zoneID)
        )
        // The cancellation path now resolves the deterministic server object
        // before delete. Keep this broad fake coherent with the successful
        // zone-subscription save performed immediately above.
        database.fetchedSubscriptions = database.savedSubscriptions
        database.accountIdentifierAfterNextSubscriptionDelete = "account-b"
''',
    "zone delete account-replacement fixture",
)

big = replace_once(
    big,
    '''        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        await Task.yield()
        synchronizer.beginSynchronization()

        for _ in 0..<1_000 where database.savedSubscriptionCount < 2 {
''',
    '''        database.accountIdentifier = "account-b"
        NotificationCenter.default.post(name: .CKAccountChanged, object: nil)
        // The account-change observer owns the recovery wakeup. Issuing a
        // second explicit begin here can race the observer and create a tail
        // drain, making an exact save-count assertion scheduler-dependent.
        await Task.yield()

        for _ in 0..<1_000 where database.savedSubscriptionCount < 2 {
''',
    "account-switch duplicate wakeup",
)
BIG.write_text(big)

hotfix = HOTFIX.read_text()
hotfix = replace_once(
    hotfix,
    '''        let deleted = await service.deletedIDs
        XCTAssertEqual(deleted, [identifier, identifier])
''',
    '''        let deleted = await service.deletedIDs
        // The first delete committed remotely before lifecycle retirement, so
        // the retry's exact ownership lookup observes absence and clears local
        // retry metadata without issuing a redundant second delete.
        XCTAssertEqual(deleted, [identifier])
''',
    "retired deletion retry expectation",
)
HOTFIX.write_text(hotfix)

print("patched", BIG, HOTFIX)
