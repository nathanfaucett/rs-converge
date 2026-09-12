# Goal

Build a modular, async-first, local-first database engine with a transaction-first storage boundary.

The engine must make it possible to:

- use backend-agnostic kernels and row reconcilers;
- commit every catalog, row, primary-key mapping, index, tombstone, and replication change as one Engine Transaction;
- store each Logical Row through a Row Reconciler, including an Automerge-backed reconciler;
- use arbitrary immutable primary-key `Row` values, including composite keys, to address row generations;
- retain concurrent row and unique-index contenders, expose deterministic canonical visible state, and require explicit conflict resolution;
- preserve deletion facts as Tombstones and restore data only as new Generations; and
- exchange causally dependent Replication Envelopes and mergeable Checkpoints so independent replicas converge after offline work.

Backends provide ordered transactional storage. The engine owns catalogs, schema identities, reconciliation, derived indexes, and replication semantics. Transport, peer discovery, authorization, and scheduling remain outside the engine.
