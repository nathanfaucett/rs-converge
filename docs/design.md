# Design

## Overview

The database is a modular, async-first engine built around one Engine Transaction. A transaction owns one complete read snapshot or write set for an operation batch. It commits all enlisted catalog, schema, UUID-keyed row, index, Automerge-row, tombstone, and replication-envelope changes together, or rolls them back together.

```text
query API / SQL translator
        -> Engine Transaction
        -> catalog, schema, UUID-keyed rows, and derived indexes
        -> Row Reconciler
        -> transactional kernel
        -> storage backend
```

Replication reuses the same transaction boundary:

```text
Replication Envelope or Checkpoint
        -> Engine Transaction
        -> reconcile schema and rows
        -> update canonical visible state and derived indexes
        -> persist envelope outcome and causal frontier
```

## Core Contracts

### Kernel

A Kernel creates a `KernelTransaction`. The transaction is the only path for catalog and record operations. It provides transactional reads, scans, writes, deletes, `commit`, and `rollback`.

Kernel implementations provide storage atomicity. The Engine composes all database facts that must change together within that boundary.

### Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler stores one Automerge document per Logical Row and uses stable column keys.

The Engine is the only path that applies local or replicated Automerge changes to an engine-managed row. Concurrent values for a column remain a Conflict. Automerge canonical ordering selects the visible value until an explicit Resolution settles the conflict.

### Generations and Tombstones

Tables, columns, and indexes have immutable Generation identities. A Logical Row is identified by its table generation and immutable UUID primary key. Names are reusable labels, not identities. When multiple active generations have the same label, deterministic canonical ordering selects the visible one.

A Tombstone permanently deletes a generation or UUID-keyed row from visible state, removes derived index records, and causes later changes to that identity to be retained as Superseded. A deleted UUID cannot be reused within a table generation; restore creates a row with a new UUID.

### Primary Keys and Indexes

Every table declares exactly one non-null `UUID PRIMARY KEY`. The engine may issue it through an Engine-scoped UUID provider; the default provider issues UUIDv7 values under `std`, while strict `no_std` callers must configure a provider before omitting a primary key.

The Engine derives Index Records from canonical visible Logical Rows within the same Engine Transaction. A local unique-index violation rejects its write. Concurrent replicated unique-index contenders are retained; scans return all rows and a unique-index lookup selects the contender with the lowest canonical UUID.

## Replication

### Envelopes

A Replication Envelope is an immutable, content-addressed record of one committed Engine Transaction. It contains causal parents and the ordered write set. Import is idempotent and atomic:

- an envelope with missing parents is retained as `Pending`;
- a ready envelope is applied as `Applied` or `Superseded`;
- malformed or unsupported envelope bytes are `Quarantined`; and
- stored envelopes are retained for multi-hop relay.

### Checkpoints

A Checkpoint contains schema facts and tombstones, complete codec row states, row tombstones, causal headers, and the causal frontier. Payload envelopes are omitted. Import merges it with local state; it never replaces a database file. Causal headers allow a delayed child of an omitted envelope to apply after bootstrap. The Automerge codec transfers complete documents so unresolved conflicts survive; the direct codec is a non-CRDT row codec and does not preserve concurrent scalar contenders.

### Public Boundary

The engine exposes frontier inspection, missing-envelope export, envelope and checkpoint import/export, outcome and conflict inspection, and explicit resolution submission. Transport, peer discovery, scheduling, signing, and authorization are outside the engine.

## Query Layer

The programmatic `Query` API and SQL translator produce statement batches for the Engine. The executor must reject unsupported query shapes rather than silently discarding clauses or semantics. Query features are added incrementally with public Engine tests, preserving the Engine Transaction boundary for every mutation.

## Current Implementation Scope

The repository has transactional in-memory and Redb kernels, an Automerge row codec, envelope logging/import, stable schema generation IDs, tombstones, explicit row-conflict resolution, compact checkpoints, and canonical unique-index contender handling. Broader query execution remains incremental; see `docs/uuid-primary-key-plan.md` for the UUID primary-key implementation checklist.
