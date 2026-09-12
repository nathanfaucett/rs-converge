# Design

## Overview

The database is a modular, async-first engine built around one Engine Transaction. A transaction owns one complete read snapshot or write set for an operation batch. It commits all enlisted catalog, schema, primary-key mapping, index, Automerge-row, tombstone, and replication-envelope changes together, or rolls them back together.

```text
query API / SQL translator
        -> Engine Transaction
        -> catalog, schema, primary-key mappings, and derived indexes
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

Tables, columns, indexes, and Logical Rows have immutable Generation identities. Names are reusable labels, not identities. When multiple active generations have the same label, deterministic canonical ordering selects the visible one.

A Tombstone permanently deletes a generation from visible state, removes active mappings and derived index records, and causes later changes to that generation to be retained as Superseded. Restore creates a new Generation rather than reactivating the tombstoned one.

### Primary Keys and Indexes

A Primary-Key Mapping maps an arbitrary immutable `Row`, including a composite key, to a Logical Row Generation's document ID.

The Engine derives Index Records from canonical visible Logical Rows within the same Engine Transaction. A unique-index conflict retains every contender. Primary-key scans return all rows; an index lookup selects the canonical contender until an explicit resolution changes it.

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

The repository has transactional in-memory and Redb kernels, an Automerge row codec, envelope logging/import, stable schema and row generation IDs, tombstones, explicit row-conflict resolution, compact checkpoints, and canonical unique-index contender handling. Broader query execution remains incremental; see `docs/replication-todo.md` for the implementation checklist.
