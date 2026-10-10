# Own-upload quarantine keeps its original owner

This followup starts from convergence #144 after the reviewed inbound-live repair
at `9a51cdfb88213956d8eaa10f70674ad219a6e7cb`. The prior inbound-live report's
adjacent own-upload limitation is closed by this change.

The public `validateAuthoritativeOwnUploadRecords` method can validate a server
echo without applying target fields, but malformed echoes publish durable semantic
quarantine. A direct caller whose Task remains alive must not carry an old
selection into that tracking write after cancellation/reset, account, replica
binding or transport replacement. Normal ChangeRequestProcessor cancellation
already permanently cancels and joins its child Task; this is a direct package
API admission correction, not a claim of a newly observed Reader incident.

The method now captures scalar generation/account/binding/context/container/scope
authority before `ensureSetup`. Setup may legitimately create the provider. After
setup returns under the original scalar authority, the method captures the
existing full provider operation validator and keeps it across refresh, semantic
model callbacks, physical quarantine admission/exit, settlement and result return.
The semantic catch path checks owner first, so revocation cannot be converted into
a malformed-record quarantine belonging to a successor.

One DEBUG hook immediately before quarantine admission supports deterministic
direct-call replacement schedules without introducing production state or a new
model. `SyncSemanticIntentTests.testOwnUploadQuarantineRejectsRetiredOwnerAndStableOwnerRetries`
uses the existing authoritative snapshot and semantic-validation fixture. It
checks a normal valid echo, then all four Task-alive owner replacements before
invalid-echo publication. Rejection must leave quarantine absent, the target and
tracking encoding unchanged, and no manufactured journal. A fresh stable owner
must quarantine exactly one lineage with the original account/container identity.

Independent source review cleared this original-owner flow and the existing
fixture semantics. Native compilation, discovery and execution remain pending;
structural/byte checks are not runtime evidence. This followup preserves the
inbound-live, deletion, forwarding and retained-cleanup repairs already in #144.
