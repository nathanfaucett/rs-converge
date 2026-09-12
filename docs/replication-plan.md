# Eventually Consistent File Replication Plan

## Goal

Independent database files converge to the same canonical visible state after they exchange all valid replication data, regardless of duplicate, delayed, reordered, or multi-hop delivery. A valid change is never discarded because of conflict, deletion, or missing dependencies.

## Scope

Keep transport outside the engine. The engine owns transaction envelopes, reconciliation, checkpoints, and inspection/resolution APIs. Reuse Redb, Automerge, and existing `Engine` transaction boundaries; add no dependency unless a later implementation proves one is needed.

## Delivery Order

### 1. Establish the replication model

Replace individual canonical `Change` records with immutable, content-addressed transaction envelopes. An envelope contains an ID, causal parents, and the ordered write set of one committed Engine Transaction. Define `Applied`, `Pending`, `Superseded`, and `Quarantined` outcomes.

**Exit check:** tests prove duplicate import is idempotent, a missing parent becomes pending, corrupt input is quarantined, and a whole ready envelope commits atomically.

### 2. Remove legacy replication compatibility

Remove the temporary `ChangeReplication`, `changes_since`, and `apply_changes` compatibility adapter. Convert every replication test and example to `Frontier`, `missing_envelopes`, and `import_envelope`. The adapter is only a transition aid; it must not remain in the final public API or test path.

**Exit check:** no replication test, example, or public export uses individual-change replication; clippy reports no deprecation warnings.

### 3. Make identity safe across copied files

Use immutable IDs for tables, columns, indexes, row generations, and envelopes. Replicated DML targets table and row generations; the engine maps relational primary keys to row generations, and a primary-key update moves that mapping without replacing the row document. Schema facts are keyed by generation ID and are add-only; tombstones are permanent, and equal labels choose the lowest UUID generation. Envelope bytes have an explicit version; unsupported and legacy versions are rejected rather than decoded under a changed schema. This is a breaking catalog storage change: existing label-keyed catalogs are not reinterpreted. Generate one fresh in-memory Automerge actor for each writable database open; do not persist or replicate it.

**Exit check:** two copied database files can both change the same row and later converge without shared-actor corruption.

### 4. Reconcile rows, deletes, and conflicts

Retain same-column Automerge conflicts and choose the canonical visible scalar by the specified Automerge ordering. Require an explicit replicated resolution operation to settle a conflict. Tombstone deleted row generations permanently; record late updates as `Superseded`. Restore creates a new generation. Replace the existing rejection-based tests (`concurrent_same_column_updates_reject_atomically` and `tombstoned_incoming_updates_reject`) because they prove permanent divergence rather than convergence.

**Exit check:** tests cover disjoint edits, same-column edits, delete/update races, explicit scalar resolution, restore, and shuffled delivery. Each pair of replicas reaches identical visible state.

### 5. Reconcile schema as identities and tombstones

Represent tables, columns, and indexes as immutable schema generations with tombstones. Resolve competing labels and unique-index contenders through the specified canonical UUID ordering. Initially support create, add, tombstone, restore, and recreate only.

**Exit check:** tests cover concurrent same-name table creation, concurrent column/index additions, table drop versus row update, and name reuse after tombstone.

### 6. Derive indexes from canonical state

Rebuild affected index records inside the importing Engine Transaction after reconciliation. Retain unique-index contenders, return all rows in primary-key scans, and select the canonical contender only for index lookup.

**Exit check:** tests cover concurrent unique-key inserts, tombstoning the canonical contender, and an explicit resolution changing the lookup winner.

### 7. Add checkpoint anti-entropy

Export and import mergeable checkpoints containing complete reconciliation state, causal frontier, conflicts, and permanent tombstones. A checkpoint merges with destination state; it never replaces a file. Retain pending envelopes and request/import a checkpoint when history is unavailable.

**Exit check:** a compacted source bootstraps an offline divergent destination; both converge and can resume envelope exchange.

### 8. Expose the minimal public replication API

Provide only checkpoint export, missing-envelope export by frontier, envelope/checkpoint import, state inspection, and explicit resolution submission. Keep peer discovery, scheduling, transport, signing, and authorization outside the engine.

**Exit check:** a test helper synchronizes replicas solely through this public API; it supports multi-hop relay and import/export bundles.

### 9. Verify and remove obsolete paths

Remove superseded individual-change replication APIs and update examples/docs. Keep the existing direct row codec only if explicitly documented as non-replicating.

**Exit check:** `cargo hack test --feature-powerset --all-targets`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `just crap` pass.
