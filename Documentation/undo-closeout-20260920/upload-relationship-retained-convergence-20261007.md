# Upload, relationship, and retained tombstone convergence

This candidate composes the committed upload snapshot fix from PR #131 with the
retained tombstone and opaque legacy lifetime fixes from PR #132 and the committed
deferred relationship and collection audit fixes from PR #133. The three unique
legacy upload regression methods from PR #130 are also retained. PR #131's upload
implementation subsumes the narrower production change in PR #130.

## Reviewed source boundaries

- Upload preparation captures committed tracking and target snapshots, including
  the generation, serializer input, and comparison baseline. Missing-target
  serialization is read-only; an independently owned tracking disposition rechecks
  target absence and attempt/account/generation authority after its wait.
- Equal mutation generation does not authorize skipping a transport-lane change.
  A retained tombstone in the old physical-delete lane becomes upload work using
  its existing generation. An obsolete physical-delete receipt cannot acknowledge
  the newer upload lane.
- Deferred relationship groups contain immutable committed intent values. Target
  write admission resamples the committed tracking intent and parent together;
  final tracking cleanup rechecks exact intent contents under its own write.
- Opaque legacy lifetime arbitration uses UTF-8 order. Scalar map keys retain byte
  identity, UUID collection audits compare decoded UUID values and reject malformed
  members, and map fingerprints use a byte tie-break without changing established
  ordering for ordinary keys.

## Source identity

Parents included in the convergence commit:

- PR #131: `3fa8ff2b88cba60cf1e4e25aa1d9f1a8e804c4e8`
- PR #132: `a4edc0feafc8b3d60cbea4934ecc33aeb710021b`
- PR #133: `c8f4dad255d8db5827b2e9ecf023e178f9129054`

Additional retained test source: PR #130 at
`16bbe87e816708fc0822a9a5c17a8ee48c8482b4`.

The composed adapter blob is `63ce7d95c0df6eadd30fbdb2d862a6ac32bc0ceb`.
The composed baseline blob is `ae60a2ce740ac249c9e9e8343edbba3f37cbca66`.
The record identity blob is `63a4b9e2b6a75c7649d936bce476caff73b13e6b`.

## Qualification status

There are 22 newly retained native behavior methods across these four fixes:
four upload snapshot methods, five retained/lifetime methods, ten relationship and
collection methods, and three legacy upload methods. New private Realm models
have explicit unique Objective-C names, exclude themselves from the default
schema, and use explicit fixture objectTypes.

Two source reviewers inspected the combined production diff. No Swift compiler,
Xcode, or real-account CloudKit execution was available in this environment.
These native methods are authored coverage, not passing qualification evidence.
The earlier source-boundary CI success applies only to its recorded source head.
Final-tree native and assembled Reader qualification remain pending. Mac UI,
performance, signed macOS CloudKit, Release, and new media work remain owner-deferred.
