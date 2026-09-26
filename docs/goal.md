# Goal

Build a modular, async-first, local-first database engine with a transaction-first storage boundary.

The engine must make it possible to:

- use backend-agnostic kernels and row reconcilers;
- commit every catalog, row, index, tombstone, and replication change as one Engine Transaction;
- store each Logical Row through a Row Reconciler, including an Automerge-backed reconciler;
- identify tables by table name, columns by `(table name, column name)`, and indexes by index name; require one immutable UUID primary key per table and use `[table name, UUID]` as the Logical Row identity;
- retain concurrent row and unique-index contenders and expose deterministic canonical visible state;
- preserve row deletions as Tombstones and restore a deleted row only with a new UUID; and
- exchange causally dependent Replication Envelopes and mergeable Checkpoints so independent replicas ofdb after offline work.

Backends provide ordered transactional storage. The engine owns catalogs, schema identities, UUID issuance, reconciliation, derived indexes, and replication semantics. Transport, peer discovery, authorization, and scheduling remain outside the engine.
