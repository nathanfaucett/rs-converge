# Domain Context

## Engine Transaction

One transaction owns one complete read snapshot or write set for an engine operation batch. A write transaction commits all enlisted catalog, schema, index, Automerge row, and tombstone changes together, or rolls them all back.

## Logical Row

A Logical Row is the row value the Engine reads, writes, and indexes. Its stored representation is selected by the Row Reconciler.

## Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler uses one Automerge document per Logical Row with stable column keys. The Engine is the only path for applying local or incoming Automerge changes to an engine-managed Logical Row. Concurrent values for one column are retained as a Conflict; Automerge canonical ordering selects the visible value.

## Row Identity

Each table has one immutable UUID primary key. A Logical Row is identified by its table generation and row UUID; the same pair identifies its Automerge document.

## Index Record

An Index Record maps an index key to a row UUID. The engine derives and updates Index Records from canonical visible Logical Rows in the same Engine Transaction. A unique-index conflict retains all rows; index lookup selects the canonical row.

## Conflict

A Conflict retains concurrent candidate values or objects that cannot all be active. A deterministic canonical ordering selects the visible candidate. An explicit Resolution is the only operation that settles a Conflict.

## Generation

A Generation is the immutable identity of a Table, Column, or Index. A Logical Row uses its table generation and immutable UUID. Names are labels and may be reused by a new Generation after the former Generation is Tombstoned.

## Sync State Unit

A Sync State Unit is the canonical transferable state for one Logical Row, Table, Column, Index, or Index Field. It contains its identity, state bytes, deletion metadata where applicable, and a digest. The Engine applies one unit atomically and maintains derived Index Records.

## Sync Manifest

A Sync Manifest maps each Sync State Unit identity to its digest. A Sync Session exchanges manifests, transfers mismatched units in batches, and retries by exchanging manifests again. The manifest is session state; the Engine stores no frontier, checkpoint, envelope log, or quarantine state.

## Tombstone

A Tombstone permanently marks a deleted Logical Row, Table, Column, or Index Generation. It removes active mappings and derived Index Records. Later changes to that Generation are Superseded; they are retained as replication facts but do not alter visible state. An explicit Restore creates a new Generation.
