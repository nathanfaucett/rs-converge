# Domain Context

## Engine Transaction

One transaction owns one complete read snapshot or write set for an engine operation batch. A write transaction commits all enlisted catalog, schema, primary-key mapping, index, Automerge row, and tombstone changes together, or rolls them all back.

## Logical Row

A Logical Row is the row value the Engine reads, writes, and indexes. Its stored representation is selected by the Row Reconciler.

## Row Reconciler

A Row Reconciler stores and resolves Logical Rows through an Engine Transaction. The Automerge Row Reconciler uses one Automerge document per Logical Row with stable column-index keys. The Engine is the only path for applying local or incoming Automerge changes to an engine-managed Logical Row. A same-column Automerge conflict rejects the full Engine Transaction before it updates any Index Record.

## Primary-Key Mapping

A Primary-Key Mapping associates an arbitrary, including composite, primary-key `Row` with the Logical Row's Automerge `DocumentId`.

## Index Record

An Index Record maps an index key to a primary-key `Row`. The engine derives and updates Index Records from the merged Logical Row in the same Engine Transaction.

## Tombstone

A Tombstone marks a deleted Logical Row. It removes its Primary-Key Mapping and Index Records. Incoming Automerge changes for a Tombstoned Logical Row are rejected unless a future explicit restore operation permits them.
