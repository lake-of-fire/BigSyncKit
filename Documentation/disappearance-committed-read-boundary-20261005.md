# Physical disappearance: committed evidence at each read boundary

## Scope and decision

Stacked on BigSync #96, `a7748d7a46f2268e4cfd0cb756003332253f98bb`.
Keep its shared owned-writer migration, target-first journal/baseline design and
actual main ancestry. No additional queue, schema, durable authority or transport
format is introduced. Reader #286 remains the application integration candidate.

Common `88f0f600` already contains the prior Unmark committed-snapshot correction,
including refresh reentry. This increment does not reapply its older source package.
It addresses the same concrete read-isolation category in the existing BigSync
physical-disappearance implementation, not an asserted new production incident.

## Four related boundaries

1. **Tracking publication:** after acquiring the tracking writer, inspect a new
   committed target snapshot. A different owner's provisional generation,
   resurrection or baseline revision cannot become durable tracking state or
   suppress publication of the already committed disappearance.
2. **Initial disappearance observation:** retain the committed baseline revision
   and submitted-candidate identity before awaiting the target writer. An aborted
   provisional revision must not manufacture a CAS mismatch. The live target
   transaction still rejects a genuinely committed successor.
3. **Pre-journal recovery:** while owning the target transaction, inspect committed
   tracking state before deciding whether old local work needs a journal. Neither
   provisional dirty tracking nor a provisional clear is evidence of that work.
4. **Deletion preparation:** only committed tombstone/journal/baseline state may
   authorize a server deletion or target-first acknowledgement recovery. The
   preparation returns a typed choice: no deletion, deletion evidence, or a
   tracking-only repair. Repair carries only its revision and resamples the live
   target after tracking admission; no frozen Realm escapes across that await.

The private synchronous snapshot helper refreshes only an unfrozen Realm which
is not currently writing, then freezes unconditionally. A refresh callback can
start an independent write before refresh returns, so returning the live Realm
from the apparent idle path is unsafe. The helper is local to this file.

Retain all current context/cancellation, account and binding checks, exact
baseline/candidate CAS, generation-matched acknowledgement, target-before-tracking
order, invalidated-baseline semantics and recovery errors. Actual target mutation
and journal writes remain inside their independently owned live transactions.
`validateRecordEvidenceCut` has an existing non-writing registry-validation branch;
the registry resolves identity using Realm configuration, not managed row mutation.
The native SDK/configuration behavior still needs native verification.

## Executed source-boundary evidence

Swift 6.2.1 on Linux x86_64, Swift 6 language mode, warnings as errors.
The runner compiles the **complete current production disappearance file** with
explicit MVCC Realm, CloudKit and adapter/domain collaborators. No second
reconciliation implementation is used. These are read-boundary tests, not actual
SDK, CloudKit, native actor, transaction admission or durability tests.

| Production source selection | Passing cases | Failing cases |
| --- | ---: | ---: |
| Complete unchanged predecessor | 5 | 10 |
| Final fixed source, Debug | 15 | 0 |
| Final fixed source, optimized with DEBUG hooks | 15 | 0 |
| Restore only old tracking-publication read | 12 | 3 |
| Restore only old deletion preparation | 11 | 4 |
| Restore only old initial evidence capture | 14 | 1 |
| Restore only old legacy-tracking read | 13 | 2 |
| Return live Realm after idle-path refresh | 14 | 1 |

The unchanged predecessor produced 20 assertion failures across its 10 failing
cases; assertion counts are not unique-test counts. Restoring the fixed source
passes again. Final checked-in runner bytes were exercised in fresh temporary
packages in both modes. Repeated runs are not additional unique tests.

One initial optimized setup omitted required DEBUG hooks, failed and aborted in
the collaborator; it is not a production or native crash and is not counted as a
completed 15-case run. The checked-in runner explicitly enables the existing hooks
in both modes. An earlier helper-name test compilation failure executed zero tests.
Original unsuccessful logs and subsequent completed runs are retained separately.

Run `bash Tools/DisappearanceReadBoundary/run.sh`; add `--configuration release`
only for optimized portable execution. The README lists collaborator limitations.

## Native acceptance and integration

Nine new methods are appended to the already registered
`SyncUndoCloseoutW1EvidenceFenceTests.swift`; its original test remains intact.
They use the actual file-backed W1 target/tracking fixtures, real adapter entries,
existing instance-scoped hooks, metadata journaling and held-owner abort/commit
histories. They cover both legacy directions, provisional generation/revision,
provisional tombstone/acknowledgement, committed deletion, captured evidence after
abort and rejection after a genuine committed successor.

**Native test source is syntax-parsed only: no native typecheck, discovery or
execution is claimed.** Add all nine `testDisappearance*`/`testDeletionPreparation*`
methods introduced by this diff to Reader's two exact method inventories when
selecting the resulting BigSync commit. Reconcile the actual gitlink, source hashes
and candidate manifest without replacing prior evidence. Run the existing W1
transport, deletion, receipt, evidence-fence and Realm owner suites alongside these
methods; preserve the composed Unmark/UI acceptance requirements. Existing file
membership is not runtime discovery.

No Reader pin, generated Xcode project, existing evidence archive, production
Realm/CloudKit data, deployment, target merge or release authorization is changed.
Keep the source PR draft pending deliberate composed native verification.
