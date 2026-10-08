# Committed evidence and recovery integration

This second source integration starts from published PR148 commit
`71fc648578f1b6d06e0d37a83406a47ffc96baca` (tree
`99db2290e16aba42e8f10c18af1c7d52c66525c4`). It preserves that integration's
incoming target/tracking ownership checks, own-echo setup policy, actor changes,
12 registered regressions, reports and qualification workflows. Native build,
discovery and execution for the resulting selected tuple remain pending.

## Immutable donors

All four donor patches were read against canonical main
`ea7ccb1702015feab3479d7dc2e96d4e42010a1d`. The integration retains their actual
commit lineage when published, along with PR148.

| Donor | Immutable head | Change | Added native methods |
| --- | --- | --- | ---: |
| PR142 | `0d07d4938e389f55044fde5dc21ddc98bdce280e` | Read committed reset/rebuild phase, provenance and restored journal snapshots; recheck transport after suspensions and inside owned writes; preserve preparation/completion committed by a peer while a write waits. | 21 |
| PR143 | `2d9b072771e9e02c2bc2a13518460eec743d064f` | Reject CKAsset values for non-Data scalar properties before Realm KVC assignment, so malformed input throws through the rollback path. | 4 |
| PR146 | `cb83803b21a7128986f088bab03ff0a6716c4728` | Read semantic quarantine and server-membership evidence from committed tracking/target snapshots, with exact account, type, record and zone identity checks. | 7 |
| PR147 | `b65f5c2cceb6027332269cf8420337079ef9d7c6` | Revalidate the captured caller authority after conflict evidence replacement/retirement, while the owned transaction can still roll back. | 2 |

The migration changes distinguish a provisional marker on a shared Realm handle
from a completed durable phase. The backup-restore path carries detached journal
identities from the committed target cut and checks generation again when the
owned write admits them. A newer committed mutation is preserved. A reset which
deliberately advances cancellation retains the newly established generation;
later suspended continuations cannot publish into a replacement transport.

The evidence-query changes preserve the existing exact-record versus catalog
state policies. Their common helper checks committed target account membership,
server record identity and zone. An uncommitted acknowledgement, deletion,
quarantine insertion or account reassignment cannot grant or remove publication
evidence. The caller recovery checks supplement PR138's retained cleanup fix:
they guard evidence writes in `refreshRecordConflict` and
`discardResolvedRecordConflicts`, including synchronous account-fence poisoning
from an invoked callback.

No new queue, lease, clock, sync schema or account reset policy is introduced.
Target-first crash recovery and the mutation journal's generation ownership are
retained.

## Merge decisions

Source inputs are verified as exact Git blobs before three-way merging. The
only adapter overlap between PR142 and PR146 retains PR142's cancellation check
before their shared committed tracking snapshot. The verifier overlap retains
PR148's ownership classes and exact retained-cleanup case, then admits the new
cases by exact identity.

PR142's older Reader qualification workflow does not replace PR148's workflow.
The package dependencies remain RealmSwiftGaps
`1ffbedbb3d8dd90f44f651f618128e7806ce39dd` and SwiftUtilities
`f437c7d06fc631cd7a67731279411c417cdf8077`. Reader qualification keeps its existing
job identity, fresh package scratch, xcsift output, retained process statuses and
explicit default-false Release opt-in.

## Regression registration and execution boundary

There are 34 additional authored native methods, bringing this bounded Sync
integration's new roster to 46 when combined with PR148's 12. The seven PR146
methods belong to `SyncUndoCloseoutW1Tests`; the other new cases belong to
`ChangeFeedMigrationResumeTests`, `HotfixCollectionSafetyTests` and
`SyncRetainedRecordContractTests`. The W1 verifier requires all 46 and preserves
its full discovered-suite accounting. Exact method alternatives extend the
focused filter for migration, asset and retained recovery cases. The existing
opt-in mutation-journal performance benchmark exclusion is unchanged; no
additional skip exception is added.

The Python verifier tests retain PR146's independent seven-case contract and add
an independent 39-case contract for PR148 plus the other three donors. These
exercise missing discovery/execution, skip and duplicate rejection through
parsed XCTest packets and the verifier CLI. They do not inspect Swift source
strings and do not execute the native Swift methods.

PR143's package Debug asset runner is adjusted to use a fresh absolute evidence
directory and package scratch. Its Swift compilation/test pipeline passes
through xcsift, immediately retains all three Swift/tee/xcsift statuses and keeps
the XCTest XML check requiring the four asset regressions exactly once without
failure or skip. Source, dependency, toolchain, logs and formatted output remain
in the packet. Experimental prebuilt macros are disabled when the installed
Swift test command supports that option. The runner does not request Release,
UI, performance or signed CloudKit qualification.

The immutable PR143 donor did not contain its subsequently described C++ setup
correction. [PR143's setup report](https://github.com/lake-of-fire/BigSyncKit/pull/143)
records [run 37814850792](https://github.com/lake-of-fire/BigSyncKit/actions/runs/37814850792)
stopped in Realm Core 20.1.5
`geospatial.cpp` / s2geometry before executing any tests. This integration
explicitly adds `-Xcxx -fno-modules` to that package Debug runner so its C++
headers compile textually. The flag is limited to this build command; it does
not change Reader app build settings, and its effectiveness remains pending
native execution.

No native test, Apple build or workflow was manually dispatched in this
environment. The package Debug jobs may execute through ordinary PR automation.
The dedicated executor ran the reviewed Python verifier packet: 50 tests passed
in 2.832 seconds, process status 0. Exact verifier/test Git blob and SHA256 inputs,
logs and statuses were retained in the integration evidence. Python module,
workflow YAML and embedded Bash syntax checks also passed. These results verify
tooling contracts and syntax; all 34 additional Swift methods remain unrun here.
Mac UI, performance, signed macOS CloudKit and Release remain owner-deferred;
Release requires explicit workflow dispatch with its opt-in set true. Tooling
verification results are recorded separately in the integration evidence and
publication description, and must not be treated as native qualification.
