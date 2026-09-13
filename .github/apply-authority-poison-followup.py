from pathlib import Path

I = "    "
II = I * 2
III = I * 3


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


sync_path = Path("Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer.swift")
sync = sync_path.read_text()
authorized = "\n".join([
    I + "var authorizedInvalidationGenerationSnapshot: UInt64? {",
    II + "lock.lock()",
    II + "defer { lock.unlock() }",
    II + "guard !isPoisoned else { return nil }",
    II + "return invalidationGeneration",
    I + "}",
]) + "\n"
if sync.count(authorized) != 1:
    raise SystemExit("authorized generation snapshot anchor changed")
admissible = "\n".join([
    "",
    I + "/// Permits generation-zero standalone metadata setup before the first",
    I + "/// account lease exists, but never lets a later poisoned generation",
    I + "/// become fresh authority while actor-isolated cancellation catches up.",
    I + "var admissibleOperationGenerationSnapshot: UInt64? {",
    II + "lock.lock()",
    II + "defer { lock.unlock() }",
    II + "guard !isPoisoned || invalidationGeneration == 0 else { return nil }",
    II + "return invalidationGeneration",
    I + "}",
    "",
    I + "/// Serializes non-suspending metadata publication against poison.",
    I + "func withAdmissibleOperationGeneration<T>(",
    II + "_ expected: UInt64,",
    II + "_ body: () throws -> T",
    I + ") rethrows -> T? {",
    II + "lock.lock()",
    II + "defer { lock.unlock() }",
    II + "guard invalidationGeneration == expected,",
    III + "!isPoisoned || invalidationGeneration == 0 else {",
    III + "return nil",
    II + "}",
    II + "return try body()",
    I + "}",
]) + "\n"
sync = sync.replace(authorized, authorized + admissible, 1)
old_run = "\n".join([
    II + "guard activeRunContext == context,",
    III + "synchronizationAttemptID == context.attemptID,",
    III + "synchronizationRunID == context.runID,",
    III + "!cancelSync else {",
])
new_run = "\n".join([
    II + "guard activeRunContext == context,",
    III + "synchronizationAttemptID == context.attemptID,",
    III + "synchronizationRunID == context.runID,",
    III + "!accountScopeAuthorityFence.rejectsAuthority,",
    III + "!cancelSync else {",
])
sync = replace_once(sync, old_run, new_run, "active run authority")
sync_path.write_text(sync)

subs_path = Path("Sources/BigSyncKit/QSSynchronizer/CloudKitSynchronizer+Subscriptions.swift")
subs = subs_path.read_text()
old_struct = "\n".join([
    "private struct CloudKitSubscriptionAccountFence: Sendable {",
    I + "let accountIdentifier: String",
    I + "let attemptID: UUID",
    I + "let runContext: CloudKitSynchronizer.RunContext?",
    "}",
])
new_struct = "\n".join([
    "private struct CloudKitSubscriptionAccountFence: Sendable {",
    I + "let accountIdentifier: String",
    I + "let attemptID: UUID",
    I + "let runContext: CloudKitSynchronizer.RunContext?",
    I + "let authorityGeneration: UInt64",
    "}",
])
subs = replace_once(subs, old_struct, new_struct, "subscription fence struct")
make_marker = "\n".join([I + "@BigSyncBackgroundActor", I + "private func makeSubscriptionAccountFence("])
helpers = "\n".join([
    I + "@BigSyncBackgroundActor",
    I + "private func checkSubscriptionOperationAuthority(",
    II + "attemptID: UUID,",
    II + "runContext: CloudKitSynchronizer.RunContext?,",
    II + "authorityGeneration: UInt64",
    I + ") throws {",
    II + "try Task.checkCancellation()",
    II + "guard synchronizationAttemptID == attemptID,",
    III + "accountScopeAuthorityFence.invalidationGenerationSnapshot",
    III + "== authorityGeneration else {",
    III + "throw CancellationError()",
    II + "}",
    II + "if let runContext {",
    III + "try checkRunContext(runContext)",
    II + "}",
    I + "}",
    "",
    I + "@BigSyncBackgroundActor",
    I + "private func commitSubscriptionMetadata(",
    II + "_ fence: CloudKitSubscriptionAccountFence,",
    II + "_ body: () throws -> Void",
    I + ") throws {",
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: fence.attemptID,",
    III + "runContext: fence.runContext,",
    III + "authorityGeneration: fence.authorityGeneration",
    II + ")",
    II + "guard try accountScopeAuthorityFence.withAdmissibleOperationGeneration(",
    III + "fence.authorityGeneration,",
    III + "{ try body(); return true }",
    II + ") == true else {",
    III + "throw CancellationError()",
    II + "}",
    I + "}",
    "",
])
subs = replace_once(subs, make_marker, helpers + make_marker, "subscription helper insertion")
make_open = "\n".join([
    I + "private func makeSubscriptionAccountFence(",
    II + "attemptID: UUID,",
    II + "runContext: CloudKitSynchronizer.RunContext?",
    I + ") async throws -> CloudKitSubscriptionAccountFence {",
])
make_prefix = "\n".join([
    make_open,
    II + "guard let authorityGeneration = accountScopeAuthorityFence",
    III + ".admissibleOperationGenerationSnapshot else {",
    III + "throw CancellationError()",
    II + "}",
])
subs = replace_once(subs, make_open, make_prefix, "subscription fence capture")
before_durability = II + "try keyValueStore.bigSyncValidateDurability()"
initial_check = "\n".join([
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: attemptID,",
    III + "runContext: runContext,",
    III + "authorityGeneration: authorityGeneration",
    II + ")",
])
subs = replace_once(subs, before_durability, initial_check + "\n" + before_durability, "pre-account authority")
post_account_anchor = "\n".join([
    II + "if let runContext {",
    III + "try checkRunContext(runContext)",
    III + "guard accountIdentifier == runContext.accountIdentifier else {",
    III + I + "throw OneOffRecordZoneResetError.cloudKitAccountChanged",
    III + "}",
    II + "}",
    II + "return CloudKitSubscriptionAccountFence(",
])
post_account_new = post_account_anchor.replace(
    II + "return CloudKitSubscriptionAccountFence(",
    "\n".join([
        II + "try checkSubscriptionOperationAuthority(",
        III + "attemptID: attemptID,",
        III + "runContext: runContext,",
        III + "authorityGeneration: authorityGeneration",
        II + ")",
        II + "return CloudKitSubscriptionAccountFence(",
    ]),
)
subs = replace_once(subs, post_account_anchor, post_account_new, "post-account authority")
return_anchor = "\n".join([
    III + "accountIdentifier: accountIdentifier,",
    III + "attemptID: attemptID,",
    III + "runContext: runContext",
])
return_new = return_anchor + ",\n" + III + "authorityGeneration: authorityGeneration"
subs = replace_once(subs, return_anchor, return_new, "subscription fence return")
revalidate_open = "\n".join([
    I + "private func revalidateSubscriptionAccountFence(",
    II + "_ fence: CloudKitSubscriptionAccountFence",
    I + ") async throws {",
])
revalidate_new = "\n".join([
    revalidate_open,
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: fence.attemptID,",
    III + "runContext: fence.runContext,",
    III + "authorityGeneration: fence.authorityGeneration",
    II + ")",
])
subs = replace_once(subs, revalidate_open, revalidate_new, "subscription revalidate prefix")
run_return = "\n".join([
    II + "if let runContext = fence.runContext {",
    III + "try await revalidateRunContext(runContext)",
    III + "return",
    II + "}",
])
run_return_new = "\n".join([
    II + "if let runContext = fence.runContext {",
    III + "try await revalidateRunContext(runContext)",
    III + "try checkSubscriptionOperationAuthority(",
    III + I + "attemptID: fence.attemptID,",
    III + I + "runContext: runContext,",
    III + I + "authorityGeneration: fence.authorityGeneration",
    III + ")",
    III + "return",
    II + "}",
])
subs = replace_once(subs, run_return, run_return_new, "run-context revalidate")
standalone_post = "\n".join([
    II + "guard synchronizationAttemptID == fence.attemptID else {",
    III + "throw CancellationError()",
    II + "}",
    II + "guard currentAccountIdentifier == fence.accountIdentifier else {",
])
standalone_new = "\n".join([
    II + "guard synchronizationAttemptID == fence.attemptID else {",
    III + "throw CancellationError()",
    II + "}",
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: fence.attemptID,",
    III + "runContext: nil,",
    III + "authorityGeneration: fence.authorityGeneration",
    II + ")",
    II + "guard currentAccountIdentifier == fence.accountIdentifier else {",
])
subs = replace_once(subs, standalone_post, standalone_new, "standalone revalidate")
lookup = "\n".join([II + "let existing = try await subscriptionStore.subscription(", III + "withID: expectedSubscriptionID", II + ")"])
fenced_lookup = "\n".join([
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: accountFence.attemptID,",
    III + "runContext: accountFence.runContext,",
    III + "authorityGeneration: accountFence.authorityGeneration",
    II + ")",
    lookup,
])
if subs.count(lookup) != 4:
    raise SystemExit(f"expected four lookup sites, found {subs.count(lookup)}")
subs = subs.replace(lookup, fenced_lookup)
save = II + "let saved = try await subscriptionStore.save(subscription: subscription)"
fenced_save = "\n".join([
    II + "try checkSubscriptionOperationAuthority(",
    III + "attemptID: accountFence.attemptID,",
    III + "runContext: accountFence.runContext,",
    III + "authorityGeneration: accountFence.authorityGeneration",
    II + ")",
    save,
])
if subs.count(save) != 2:
    raise SystemExit(f"expected two save sites, found {subs.count(save)}")
subs = subs.replace(save, fenced_save)

# Protect every local subscription-pointer publication against the same poison.
metadata_sites = [
    (II + "try persistDatabaseSubscriptionID(nil)", II + "try commitSubscriptionMetadata(accountFence) {\n" + III + "try persistDatabaseSubscriptionID(nil)\n" + II + "}"),
    (III + "try persistDatabaseSubscriptionID(existing.subscriptionID)", III + "try commitSubscriptionMetadata(accountFence) {\n" + III + I + "try persistDatabaseSubscriptionID(existing.subscriptionID)\n" + III + "}"),
    (II + "try persistDatabaseSubscriptionID(expectedSubscriptionID)", II + "try commitSubscriptionMetadata(accountFence) {\n" + III + "try persistDatabaseSubscriptionID(expectedSubscriptionID)\n" + II + "}"),
    (II + "try persistSubscriptionID(nil, for: zoneID)", II + "try commitSubscriptionMetadata(accountFence) {\n" + III + "try persistSubscriptionID(nil, for: zoneID)\n" + II + "}"),
    (III + "try persistSubscriptionID(zoneSubscription.subscriptionID, for: zoneID)", III + "try commitSubscriptionMetadata(accountFence) {\n" + III + I + "try persistSubscriptionID(zoneSubscription.subscriptionID, for: zoneID)\n" + III + "}"),
    (II + "try persistSubscriptionID(expectedSubscriptionID, for: zoneID)", II + "try commitSubscriptionMetadata(accountFence) {\n" + III + "try persistSubscriptionID(expectedSubscriptionID, for: zoneID)\n" + II + "}"),
    (II + "try persistRemovingSubscriptionID(identifier)", II + "try commitSubscriptionMetadata(accountFence) {\n" + III + "try persistRemovingSubscriptionID(identifier)\n" + II + "}"),
]
for old, new in metadata_sites:
    count = subs.count(old)
    if count == 0:
        raise SystemExit(f"missing metadata site: {old.strip()}")
    subs = subs.replace(old, new)
subs_path.write_text(subs)

tests_path = Path("Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift")
tests = tests_path.read_text()
if "testActiveRunRejectsSynchronousAuthorityPoisonBeforeActorCancellation" in tests:
    raise SystemExit("follow-up tests already exist")
tests += '''

extension CloudKitSynchronizerAccountFencingTests {
    @BigSyncBackgroundActor
    func testActiveRunRejectsSynchronousAuthorityPoisonBeforeActorCancellation()
    throws {
        let synchronizer = makeSynchronizer(transport: AccountFencingTransport())
        synchronizer.accountScopeAuthorityFence.clear()
        let context = CloudKitSynchronizer.RunContext(
            attemptID: synchronizer.synchronizationAttemptID,
            runID: synchronizer.synchronizationRunID,
            accountIdentifier: "account-a",
            accountScopeIdentifier: CloudKitSynchronizer.accountScopeIdentifier(for: "account-a")
        )
        synchronizer.activeRunContext = context
        synchronizer.accountScopeAuthorityFence.poison()
        XCTAssertThrowsError(try synchronizer.checkRunContext(context)) {
            XCTAssertTrue($0 is CancellationError)
        }
    }

    @BigSyncBackgroundActor
    func testStandaloneSubscriptionRejectsAuthorityPoisonBeforeServerLookup() async throws {
        let transport = AccountFencingTransport()
        let authority = AccountAuthorityFenceReference()
        let synchronizer = makeSynchronizer(
            transport: transport,
            accountIdentifierProvider: {
                await authority.poison()
                return "account-a"
            }
        )
        authority.synchronizer = synchronizer
        do {
            try await synchronizer.subscribeForChangesInDatabase()
            XCTFail("Expected stale subscription authority to be rejected")
        } catch is CancellationError {}
        XCTAssertEqual(transport.subscriptionFetchCount, 0)
        XCTAssertEqual(transport.subscriptionSaveCount, 0)
    }

    @BigSyncBackgroundActor
    func testStandaloneSubscriptionCannotAdoptAlreadyPoisonedGeneration() async throws {
        let transport = AccountFencingTransport()
        let synchronizer = makeSynchronizer(transport: transport)
        synchronizer.accountScopeAuthorityFence.poison()
        do {
            try await synchronizer.subscribeForChangesInDatabase()
            XCTFail("Expected poisoned standalone authority to be rejected")
        } catch is CancellationError {}
        XCTAssertEqual(transport.subscriptionFetchCount, 0)
        XCTAssertEqual(transport.subscriptionSaveCount, 0)
    }
}
'''
tests_path.write_text(tests)
