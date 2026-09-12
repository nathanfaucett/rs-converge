# Domain Context

## Engine Transaction

One transaction owns one complete read snapshot or write set for an engine operation batch. A write transaction commits all enlisted catalog, schema, primary-key mapping, index, Automerge row, tombstone, and replication-envelope changes together, or rolls them all back.

## Logical Row

A Logical Row is the row value the Engine reads, writes, and indexes. Its stored representation is selected by the Row Reconciler.

## Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler uses one Automerge document per Logical Row with stable column keys. The Engine is the only path for applying local or incoming Automerge changes to an engine-managed Logical Row. Concurrent values for one column are retained as a Conflict; Automerge canonical ordering selects the visible value.

## Primary-Key Mapping

A Primary-Key Mapping associates an arbitrary, including composite, immutable primary-key `Row` with a Logical Row Generation's Automerge `DocumentId`.

## Index Record

An Index Record maps an index key to a primary-key `Row`. The engine derives and updates Index Records from canonical visible Logical Rows in the same Engine Transaction. A unique-index conflict retains all rows; index lookup selects the canonical row.

## Conflict

A Conflict retains concurrent candidate values or objects that cannot all be active. A deterministic canonical ordering selects the visible candidate. An explicit Resolution is the only operation that settles a Conflict.

## Generation

A Generation is the immutable identity of a Logical Row, Table, Column, or Index. Names are labels and may be reused by a new Generation after the former Generation is Tombstoned.

## Replication Envelope

A Replication Envelope is one immutable, causally dependent record of a committed Engine Transaction. Replicas apply its complete write set atomically, retain it for relay, and classify it as Applied, Pending, Superseded, or Quarantined.

## Checkpoint

A Checkpoint is a mergeable compact representation of replicated state and its causal frontier. It contains the facts required to reconcile with independent offline state; it never replaces a replica's database state.

## Tombstone

A Tombstone permanently marks a deleted Logical Row, Table, Column, or Index Generation. It removes active mappings and derived Index Records. Later changes to that Generation are Superseded; they are retained as replication facts but do not alter visible state. An explicit Restore creates a new Generation.
