# Eventually Consistent File Replication Todo

## Definition of Done

- [x] Two independent files converge after every valid envelope/checkpoint exchange, including duplicate, reordered, delayed, and multi-hop delivery.
- [x] Canonical visible rows, schemas, indexes, tombstones, conflicts, and replication frontiers match after convergence.
- [x] `cargo hack test --feature-powerset --all-targets` passes.
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes.
- [x] `just crap` passes.

## 1. Envelopes

- [x] Define immutable content-addressed envelope and causal-parent types.
- [x] Log one envelope for each committed Engine Transaction.
- [x] Import ready envelopes atomically and idempotently.
- [x] Persist `Applied`, `Pending`, `Superseded`, and `Quarantined` outcomes.
- [x] Expose public outcome inspection.
- [x] Add tests for duplicate, corrupt, partial, and dependency-missing imports.

## 2. Remove legacy replication compatibility

- [x] Convert replication tests and examples to `Frontier`, `missing_envelopes`, and `import_envelope`.
- [x] Remove `ChangeReplication`, `changes_since`, and `apply_changes`.
- [x] Verify no deprecation warnings remain.

## 3. Stable identities

- [x] Add immutable IDs for tables, columns, indexes, and row generations.
- [x] Address replicated rows and schema changes by identity rather than name.
- [x] Make Automerge actors in-memory and unique per writable open; replace the interim per-write actor.
- [x] Test concurrent writes from copied database files.

## 4. Row reconciliation

- [x] Preserve same-column contenders and derive the canonical visible value.
- [x] Reject ordinary updates to conflicted scalars.
- [x] Add explicit replicated conflict-resolution operations.
- [x] Tombstone row generations permanently and classify late updates as superseded.
- [x] Add restore as a new row generation.
- [x] Replace rejection-based same-column and tombstoned-update tests with convergence tests.
- [x] Test partitions, shuffled delivery, delete/update, restore, and resolution.

## 5. Schema reconciliation

- [x] Model table, column, and index generations as add-only facts plus tombstones.
- [x] Resolve label collisions by canonical UUID ordering.
- [x] Support create, add, tombstone, restore, and recreate only.
- [x] Test concurrent schema creation/addition and table-drop races.

## 6. Indexes

- [x] Derive records only from canonical visible state in the import transaction.
- [x] Retain unique-index contenders and choose a canonical lookup winner.
- [x] Test index winner changes after tombstone and resolution.

## 7. Checkpoints and public API

- [x] Define a mergeable checkpoint and compacted causal frontier.
- [x] Keep tombstone facts through compaction.
- [x] Import checkpoints by merge, never file replacement.
- [x] Export missing envelopes from a frontier.
- [x] Expose conflict, pending, superseded, and quarantine inspection.
- [x] Add a public-API-only two-replica and three-replica sync test helper.

## 8. Finish

- [x] Remove or deprecate individual-change replication paths.
- [x] Update examples and project documentation.
- [x] Run the Definition of Done commands.
