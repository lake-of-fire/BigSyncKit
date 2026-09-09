# BigSyncKit
**Available for hire:** *If you need help adding iCloud sync to your RealmSwift project, or you're using SyncKit/IceCream already and need to fix the issues you'll face with those libraries, I'm available for custom integrating this library into your existing projects. Please contact me via alex dot ehlke at gmail.*

## Working-tree status

This snapshot includes pre-release mutation-journal, recovery, and terminal-publication changes. Earlier production use of BigSyncKit does not qualify these changes. Native Realm/CloudKit integration, test discovery, signed lifecycle scenarios, and supported-device performance must be verified before release. The historical SyncKit documentation below is not the contract for the current adapter.

## Current mutation and recovery contract

`BigSyncPendingMutation` is the sole upload-work journal. A synchronized authoritative edit and its fresh journal generation belong in the same target Realm transaction. The throwing expected-identity metadata API returns a witness for verifying the final generation. An upload acknowledgement can retire only the exact generation it prepared, not newer work for that record. Ordinary authoritative writers can use `try refreshChangeMetadataRequiringJournal(at:)`, which samples the live identity at that mutation. Source commands admitted earlier must retain the expected-identity/witness overload instead. Let either error escape the enclosing Realm write; catching it per object permits partial commits. The legacy nonthrowing metadata API still exists for other callers, and its assertion paths must not be mistaken for the strict transaction boundary.

`RealmSwiftAdapter` defaults to `.custom`. Model semantic dispositions and pending-local protection run before ordinary field conflict resolution. Without a delegate, timestamps compare **existing** targets; a new object's constructor dates are not a competing local edit. Retransmission of an unchanged default local winner preserves its conflict timestamps. An explicit custom delegate may author a new value and retains its separate merge contract. The target's existence and preimage are resolved inside the actual write, not inferred from the earlier selection snapshot.

Inbound names for typed primary keys must round-trip through the exact representation used by outbound serialization. Integer aliases such as `01` or `+1`, and alternate UUID/ObjectId spelling, cannot address one target under a different journal key. The same rule applies before persisting relationship references. String keys remain opaque and byte-preserved; no identifier normalization or automatic remote rekeying is performed. An existing noncanonical remote name requires explicit diagnosis, not silent migration. The record-name separator is parsed as an ASCII byte so a leading combining mark in a string key is not lost during import, deletion, or cleanup.

Record, replacement, own-upload, and deletion validation share the same operational-error boundary. Cancellation, `BigSyncSemanticAdmissionUnavailable`, and `BigSyncInboundResourceUnavailable` abort without converting missing local prerequisites into semantic quarantine. Failure inside a target write rolls back that physical transaction, not previously committed transactions in another Realm. Other validator errors retain their existing quarantine treatment; this is not an automatic classifier for arbitrary transient errors.

Recovery may retire historical outbox entries only after resolving the registered identity for the exact recovery binding. An installed provider returning nil or malformed identity is an error, not evidence that a mutation came from a backup. A newly exposed binding requires retry/handoff rather than permission to discard preceding-binding work. Installation-owned generations must match both the recorded binding and their embedded identity/nonce. Only deliberately unbound configurations with no identity provider use the existing process-identity path.

Retained-data rebootstrap uses throwing generation creation. Identity loss or binding change while replacing a pending row aborts that physical target transaction. Recovery intent remains available for retry. This is not atomic across separate target/tracking files or several target Realms, and it does not resolve the domain meaning of acknowledged deletion, account return, or restored source data. Retiring an obsolete journal is not permission to delete retained user objects.

Direct imports, own-upload validation, and deletion input batches reject foreign zones and duplicate record names before any target group or tracking disposition commits. This preflight is distinct from later page/cursor validation. Names that collide under the existing name-keyed staging dictionaries are rejected, not silently coalesced or normalized.

Mutation responses are correlated with the exact requested record ID (including zone owner and name), and save results must also preserve record type. A mismatched success or conflict result fails that item without acknowledging or importing the returned record; valid independent successes can still be acknowledged. Prepared batches must stay within the adapter's zone and have unique names under the existing generation-map key semantics. Direct Realm adapter acknowledgement and missing-record/rebase callbacks also reject foreign-zone responses before writing. These checks protect against malformed adapter/store responses; they do not claim that Apple's service returns such responses in normal operation.

Cached system-field archives are accepted only for their tracking row's exact name, type, and adapter zone. A present unreadable or mismatched archive aborts upload composition instead of silently becoming a new record or sending another target's values under a cached foreign identity. The archive and pending generation remain intact. Valid redelivery may repair the system fields through the existing pending-preserving import path; the explicit, generation-matched unknown-item recovery still clears them when justified. This is not an automatic repair timer, a content merge, or permission to erase retained data. Missing configured target storage is distinct from an actually absent target.

Persisted database cursors are loaded through the throwing durable-store boundary. Only a missing value means first fetch: a wrong stored type or empty cursor is `corruptCursor`, and a storage read failure remains an operational storage error. The default CloudKit transport rejects present malformed database and zone token archives, including empty bytes. The existing corrupt-cursor path must durably request recovery before clearing cursors. Opaque nonempty scripted-feed bytes remain supported; cursor bytes do not order domain state or prove safe deletion retention. These checks do not themselves solve server-absent source admission after restore.

Record-zone checkpoint reads are now `try await adapter.serverChangeToken`. A missing provider throws; only absent checkpoint rows or one legacy explicitly cleared nil row mean first fetch. Empty bytes and multiple rows are corrupt, even if duplicate bytes agree. Reads, page compare-and-save, terminal boundary generation, and tagless quarantine lineage share this interpretation. Ordinary non-nil saves cannot select an arbitrary row to repair corruption. The existing explicit `saveToken(nil)` recovery path collapses all checkpoint rows to one cleared row and invalidates the current page proof in the same tracking transaction, retaining pending upload work and committed inbound-identity delivery. This changes an effect on the public property, not its name or the stored schema; concrete nonthrowing protocol witnesses remain valid, but callers through `ModelAdapter` must propagate errors.

A successfully returned database or zone page must carry nonempty opaque cursor bytes **before** its events are consumed. `invalidPageCursor` means the feed result cannot be checkpointed; it does not imply that stored history is corrupt and does not by itself request a reset. An earlier valid zone page remains committed if a later page is rejected; the database checkpoint does not advance past the failed zone drain. Direct page commits and token-save entry points also reject empty next checkpoints. The implementation does not compare token ordering, require that every successive token differs, decode scripted cursors as SDK tokens, or add an automatic repair timer.

Change-feed recovery reads and writes the existing version-3 envelope through the throwing durable-store API. A read failure or malformed envelope is not first-run absence. Version and epoch numbers must be exactly representable integers; a fractional value cannot select a rounded history. A failed write is not compensated by overwriting the store with an older envelope: the accepted state must be reloaded before retry. The synchronizer also rechecks the exact durable envelope before and after adapter reconciliation/completion. These checks do not add cross-process compare-and-swap or make adapter and key-value-store commits atomic.

A prepared backup restore can observe a later encrypted-data reset. The stronger recovery retains that restore event's identity, and retry of the same event resumes the encrypted reset rather than starting backup recovery again. A genuinely different restore event starts a new backup epoch and does not continue an old owner's handoff. New and accumulated binding handoffs survive encrypted recovery. An encrypted escalation before copied-journal retirement has reached the prepared boundary is rejected. If migration completion is durable but clearing the same restore event was interrupted, the existing completion acknowledges that event without another recovery epoch. This is transport recovery composition, not a rule admitting old restored manual support as current reading.

The exact restore envelope is persisted before clearing its cursors, subscriptions, or copied terminal marker. A fresh encrypted-reset fence is retained until journal-backed recovery finishes. The schema, namespace, and ordinary deletion policy are unchanged. `ChangeFeedRecoveryStateTests.swift` tests the actual Foundation-only envelope and selection logic; the callback/storage-fault and real-Realm restore tests still require native execution. A controlled store reporting failure after accepting a write is not a filesystem power-loss test.

Replica-binding and account-lease envelopes use their existing version-1 keys and wire fields. Present malformed optional owners are errors, not an unowned initial binding. Version/generation numbers must be exact non-Boolean integers; integral numeric encodings remain accepted. A lease validity field is a property-list Boolean, and invalidated leases omit the live account/date pair. Pending ports require distinct source/destination accounts and finite timestamps. Missing storage remains the supported initial state; no automatic corrupt-metadata reset or rekeying is performed.

Binding, lease, and account-marker publication require matching durable readback. Routine validation of an already matching marker rechecks its current stored value and does not rewrite it; the pre-await marker is not reused as proof. A failed call may have accepted its successor already: never compensate by restoring the old value, and reload before retrying. Every account-replacement policy now loads its prior account marker through the same throwing durable boundary before account lookup. Domain invalidation does not proceed after unverified lease invalidation. These checks do not make read/modify/write atomic between competing writers or across KVS and Realm, and do not decide whether acknowledged deletions or restored source records remain authoritative. The existing single coordinator and handoff/restore boundaries remain required. A pending generation accepted before a reported write failure may be visible to the existing identity reader; recovery must retain that generation rather than manufacture another one.

Explicit port activation sets its existing worker-restart fence before the first fallible binding/account publication. A write that committed but reported failure cannot leave that old worker eligible to resume normal synchronization. An unchanged pending requirement may still be retried; after accepted activation, reconstruct the worker from the persisted binding. This adds no durable phase or automatic restart mechanism, and does not make arbitrary callers' destination verification part of BigSync.

`AccountAuthorityPersistenceTests.swift` exercises the actual envelope/store policies with deterministic storage faults. The native account-fencing and strict-journal tests separately cover their composition. Host Foundation/CryptoKit-adapter execution does not qualify Objective-C protocol dispatch, Apple's CryptoKit, filesystem durability, Realm rollback, or native account callbacks.

The real adapter regressions live in `Tests/BigSyncKitTests/BigSyncKitTests.swift`; the isolated default comparator tests are in `BigSyncRecordConflictPolicyTests.swift`, and the key/error boundary tests are in `BigSyncRecordIdentityBoundaryTests.swift`. `CloudKitCursorPersistenceTests.swift` tests the Foundation-only stored-value distinction, not SDK token decoding. Passing portable comparator tests is not native recovery qualification.

Synchronize RealmSwift databases with CloudKit.

This project began with a fork of [mentrena's SyncKit](https://mentrena.github.io/SyncKit) and combines pieces of [caiyue1993's IceCream](https://github.com/caiyue1993/IceCream) to improve some gaps and scalability. This project builds on that foundation with further scalability-focused improvements.

- Scales to much larger Realm databases. The goal is to scale to millions of records and beyond.
- Supports latest RealmSwift versions with modern `@Persisted` annotation.
- Supports UUID primary keys.
- Supports List (Array) and MutableSet (Set) properties for primitives and custom object types.
- Moves most operations from the main thread into background threads (see BackgroundWorker).
- Adds schema version metadata for use in custom conflict resolution and migrating older incoming data.

The downsides that were traded off to achieve this:
- Removes CKReference / CKRecord.Reference usage. Record names are stored directly as a plain string field. This is because CloudKit limits references with deletion cascades to 750 references (leaving little upside to using references), and because references require precise sequencing in uploading/downloading records which leads to unreasonable memory requirements at scale (at least without a more performant rearchitecture). This trade-off is mitigated by moving data integrity checks or flexibility for eventually-consistent data into clients. It can be worth it at scale.
- Gives up ability to use fine-grained changed properties notifications to resolve merge conflicts. This was not a default behavior in SyncKit but could be enabled via the `client` merge policy or a PR to add this capability first-class.
- Custom conflict resolution functions must also account for list objects.
- Removes CoreData support in order to simplify maintenance needed for this fork, and because it can better focus on the primary use case of this fork.
- Uses `isDeleted` for the ordinary deletion lane; successful CloudKit deletion can be followed by target/tracking cleanup. Domains requiring negative state to survive later restore or account return need an explicit retention/admission contract; an acknowledged journal is not indefinite deletion evidence.

Future roadmap aspirations:
- [ ] Realtime sync with RxDB via WKWebView postMessage
- [ ] Realtime sync with Supabase
- [ ] Realtime sync with websocket
- [ ] OpenAPI-Generator custom templates for BigSyncKit boilerplate and configuration from specs

-------------
**Old readme copied below:**

![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/mentrena/synckit/Test/master)
[![Carthage compatible](https://img.shields.io/badge/Carthage-compatible-4BC51D.svg?style=flat)](https://github.com/Carthage/Carthage)
[![Version](https://img.shields.io/cocoapods/v/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)
[![License](https://img.shields.io/cocoapods/l/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)
[![Platform](https://img.shields.io/cocoapods/p/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)

SyncKit automates the process of synchronizing Core Data or Realm models using CloudKit.

SyncKit uses introspection to work with any model. It sits next to your Core Data or Realm stack, making it easy to add synchronization to existing apps.

For installation instructions and more information check the [Docs](https://mentrena.github.io/SyncKit)

## Author

Manuel Entrena, manuel@mentrena.com

## License

SyncKit is available under the MIT license. See the LICENSE file for more info.
