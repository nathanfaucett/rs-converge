# Sync Codec Ownership Refactor

## Goal

Move synchronization protocol types and codec-specific synchronization behavior out of `engine`.

After this refactor:

```text
engine              owns database state, transactions, schemas, catalogs, and indexes
sync                owns sync messages, manifests, inventories, recovery, and sync traits
engine-automerge    owns the Automerge implementation of the sync trait
btree-automerge     owns Automerge document-change storage keys and encodings
```

`engine` must not define a type named `Sync*`, `DocumentChangeKey`, or an Automerge change-history API.

There is no migration and no compatibility layer. Use a destructive workflow: delete the superseded API first, allow the workspace to fail to compile, then repair every caller against the new design. Do not preserve aliases, adapters, wrappers, fallback branches, feature flags, or dual protocol paths.

## Non-negotiable boundary

`btree_automerge::DocumentChangeKey` remains the only document-change storage key. It stays internal to `btree-automerge` and `engine-automerge`.

A synchronization message identifies a realtime change by:

```text
(table generation ID, row UUID, opaque sync change ID, raw payload)
```

It must not carry an Automerge document ID. `AutomergeRowCodec` derives its document ID from the row UUID as it already does.

The sync protocol transfers opaque change IDs. Only the Automerge adapter interprets a change ID as an Automerge `ChangeHash`.

## Target dependency graph

```text
engine
  ↑
sync
  ↑
engine-automerge

btree-automerge
  ↑
engine-automerge
```

`sync` may depend on `engine`. `engine-automerge` may depend on both `engine` and `sync`. `engine` must not depend on either `sync` or `engine-automerge`.

Remove `sync`'s `engine-automerge` development dependency before adding the normal `engine-automerge -> sync` dependency. Move tests requiring Automerge to `engine-automerge`, the workspace root, or `crates/test`.

## Target contracts

### Engine contracts

Engine contracts must use database terms only. They must not mention manifests, inventories, snapshots, peers, transports, protocols, Automerge, change hashes, or sync.

Add the following public engine primitives. Put their implementations in a dedicated sibling module such as `crates/engine/src/state_transfer.rs`; keep `lib.rs` declaration/re-export-only.

1. `CatalogEntry`
   - Identifies one persisted catalog fact: table, table field, index, or index field.
   - Carries its engine-owned identity and encoded row value.
   - Does not contain a digest or a sync key.
2. `Engine::export_catalog_entries()`
   - Returns all current `CatalogEntry` values in deterministic engine order.
3. `Engine::apply_catalog_entry(entry)`
   - Persists one catalog entry and creates required physical data/index tables.
   - Commits or rolls back atomically.
4. `Engine::table_generations()`
   - Returns active `TableGenerationId` values.
5. `Engine::read_row_state(table, row, codec_operation)`
   - Runs a read-only codec operation in an engine transaction and rolls it back.
   - The codec operation receives `&K::Transaction` and `&R`.
6. `Engine::mutate_row_state(table, row, codec_operation)`
   - Ensures schema and physical row storage.
   - Loads the old logical row through `R::get_row`.
   - Runs one codec mutation inside the same transaction.
   - Receives the new logical row from the codec operation.
   - Calls `index::update_row` with the old and new values.
   - Commits only when every step succeeds; otherwise rolls back.

`read_row_state` and `mutate_row_state` must use a boxed `Future` callback type if required by Rust lifetimes. The callback API is an engine mutation primitive, not a sync API.

### Sync contracts

`crates/sync` owns all types used to represent or transfer synchronization state:

```rust
pub struct SyncChangeId(pub Vec<u8>);
pub enum SyncKey { /* catalog and row identities */ }
pub struct StateDigest(pub [u8; 32]);
pub struct SyncStateUnit { /* key, state, metadata, digest */ }
pub struct SyncManifest { /* sorted SyncKey/digest entries */ }
```

`SyncChangeId` is opaque. The only required behavior is clone, equality, ordering, serialization, and byte transport. It must not be named `DocumentChangeKey`.

Define a public `sync::SyncRowCodec<T>` extension trait:

```rust
pub trait SyncRowCodec<T>: engine::RowCodec<T>
where
    T: engine::KernelTransaction,
{
    fn row_ids(
        &self,
        transaction: &T,
        table: uuid::Uuid,
    ) -> impl Future<Output = engine::EngineResult<Vec<uuid::Uuid>>> + Send;

    fn export_state(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: uuid::Uuid,
    ) -> impl Future<Output = engine::EngineResult<Option<Vec<u8>>>> + Send;

    fn merge_state(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        state: &[u8],
    ) -> impl Future<Output = engine::EngineResult<Option<value::Row>>> + Send;

    fn export_metadata(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: uuid::Uuid,
    ) -> impl Future<Output = engine::EngineResult<Vec<u8>>> + Send;

    fn merge_metadata(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        metadata: &[u8],
    ) -> impl Future<Output = engine::EngineResult<()>> + Send;

    fn change_inventory(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: uuid::Uuid,
    ) -> impl Future<Output = engine::EngineResult<Vec<SyncChangeId>>> + Send;

    fn export_change(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        id: &SyncChangeId,
    ) -> impl Future<Output = engine::EngineResult<Option<Vec<u8>>>> + Send;

    fn apply_change(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> impl Future<Output = engine::EngineResult<Option<value::Row>>> + Send;
}
```

The trait owns row-state serialization, metadata serialization, and incremental-change semantics. `sync` builds manifests and performs protocol orchestration using this trait plus the neutral engine primitives.

`sync::synchronize` requires `R: SyncRowCodec<K::Transaction>`. A codec that does not implement the extension trait cannot use this sync protocol.

### Automerge adapter

`engine-automerge` implements `sync::SyncRowCodec` for `AutomergeRowCodec`.

For `change_inventory`, it returns one `SyncChangeId(change_hash.to_vec())` for every retained incremental Automerge change, in Automerge causal order.

For `export_change` and `apply_change`:

1. Validate that `SyncChangeId.0` is exactly 32 bytes; otherwise return `EngineError::custom`.
2. Convert the bytes to `[u8; 32]`.
3. Derive the Automerge document ID from `row`.
4. Construct `btree_automerge::DocumentChangeKey::new_incremental(document_id, hash)` internally.
5. Export or apply the stored raw incremental payload.

The adapter retains current behavior:

- duplicate incrementals are idempotent;
- change application is causally ordered;
- missing history/dependencies return `EngineError::SyncDependencyUnavailable`;
- snapshot state is used only for bootstrap and dependency recovery;
- raw `save_incremental()` payloads are treated as opaque and are not assumed to contain one framed Automerge change.

## Actionable TODOs

### 0. Establish a clean dependency graph

- [ ] Remove `engine-automerge` from `[dev-dependencies]` in `crates/sync/Cargo.toml`.
- [ ] Move every sync test that imports `engine_automerge` out of `crates/sync/tests/` to either `crates/engine-automerge/tests/`, `crates/test/`, or the root integration tests. Preserve each test case and assertion.
- [ ] Add `sync = { path = "../sync", default-features = false, features = [] }` to `crates/engine-automerge/Cargo.toml` under internal dependencies.
- [ ] Run `cargo check -p sync -p engine-automerge` and confirm Cargo reports no cyclic package dependency.

### 1. Delete the old boundary first

Perform this section before adding replacement APIs. The compile failures are the complete caller inventory for the old design.

- [ ] Delete `crates/engine/src/row_sync.rs` immediately; do not first replace its operations.
- [ ] Delete the `row_sync` module declaration and all `DocumentChangeKey`, `IncrementalChange`, `StateDigest`, `SyncKey`, `SyncManifest`, and `SyncStateUnit` re-exports from `crates/engine/src/lib.rs`.
- [ ] Delete `DocumentChangeKey` imports from `crates/engine/src/codec.rs`.
- [ ] Remove `sync_change_inventory`, `export_incremental_change`, and `apply_incremental_change` from `engine::RowCodec`.
- [ ] Remove `export_row_state`, `merge_row_state`, `export_row_metadata`, and `merge_row_metadata` from `engine::RowCodec` if they exist only for synchronization. They are replaced by `sync::SyncRowCodec`.
- [ ] Run `cargo check --workspace` and retain the resulting compile failures as the caller checklist; do not reintroduce any deleted type or method to make callers compile.
- [ ] Confirm `grep -R "SyncManifest\|SyncStateUnit\|SyncKey\|StateDigest\|DocumentChangeKey\|sync_change_inventory\|export_incremental_change\|apply_incremental_change" crates/engine` returns no sync or Automerge-specific engine API. `RowCodec` must retain only normal row-codec operations.

### 2. Add neutral engine state-transfer primitives

- [ ] Create `crates/engine/src/state_transfer.rs` containing `CatalogEntry`, `export_catalog_entries`, `apply_catalog_entry`, `table_generations`, `row_ids`, `read_row_state`, and `mutate_row_state`.
- [ ] Keep all catalog storage UUIDs and catalog row decoding inside engine code.
- [ ] Make catalog export deterministic: sort by catalog kind and its natural engine identity before returning.

- [ ] Make `mutate_row_state` update indexes exactly once after a successful codec mutation and before commit.
- [ ] Add engine unit tests proving `mutate_row_state` rolls back both row state and index updates when the codec operation returns an error.
- [ ] Add engine unit tests proving `apply_catalog_entry` creates required physical tables.

### 3. Define sync-owned data and codec capability

- [ ] Create `crates/sync/src/codec.rs` containing `SyncChangeId` and `SyncRowCodec`.
- [ ] Move `SyncKey`, `StateDigest`, `SyncStateUnit`, and `SyncManifest` from `engine` into `crates/sync/src/state.rs`.
- [ ] Preserve deterministic `SyncManifest::new` sorting and deduplication by `SyncKey`.
- [ ] Preserve digest input exactly as length-prefixed state bytes followed by length-prefixed metadata bytes.
- [ ] Re-export only the types needed by external codec implementations from `crates/sync/src/lib.rs`.
- [ ] Update `crates/sync/src/protocol.rs` so all sync message types import their state and change types from `crate`, never from `engine`.
- [ ] Replace `SyncIncrementalChange.key: DocumentChangeKey` with `id: SyncChangeId`.
- [ ] Replace `SyncRowInventory.changes: Vec<DocumentChangeKey>` with `Vec<SyncChangeId>`.
- [ ] Keep `table` and `row` in inventory and incremental messages; do not add a document ID field.

### 4. Implement the Automerge sync capability

- [ ] In `crates/engine-automerge/src/codec.rs`, implement `sync::SyncRowCodec<T>` for `AutomergeRowCodec`.
- [ ] Move every former `RowCodec` sync method implementation into the new trait implementation without changing persistence semantics.
- [ ] Convert between `SyncChangeId` and `[u8; 32]` only inside this implementation.
- [ ] Derive `DocumentId` from the supplied row UUID for every incremental export and apply operation.
- [ ] Remove the engine alias `SyncDocumentChangeKey` and all use of engine-owned change keys.
- [ ] Keep `btree_automerge::DocumentChangeKey` private to the Automerge codec implementation.
- [ ] Add tests for invalid change-ID byte lengths, wrong-row change IDs, duplicate changes, causal ordering, concurrent branches, and dependency recovery.

### 5. Rebuild session orchestration in the sync crate

- [ ] Move manifest construction, catalog-unit conversion, and row-state-unit conversion to `crates/sync/src/state.rs` or `crates/sync/src/session.rs`.
- [ ] Use `Engine::export_catalog_entries`, `Engine::table_generations`, and `SyncRowCodec::row_ids` to construct all state units.
- [ ] Require `SyncRowCodec::row_ids` to return every UUID known from either codec state or codec metadata, including tombstoned rows, in sorted and deduplicated order.
- [ ] Use `Engine::apply_catalog_entry` for catalog units.
- [ ] Use `Engine::mutate_row_state` for row snapshots and incremental batches so index updates remain atomic.
- [ ] Keep manifest comparison in `sync`; engine must not compare remote manifests.
- [ ] Keep change inventory comparison in `sync`; the codec only lists and exports its local change IDs.
- [ ] For a normal realtime update, send only missing incremental payloads.
- [ ] When `SyncDependencyUnavailable` is returned while applying an incremental batch, request exactly the affected `(table, row)` snapshots.
- [ ] When a recovery snapshot arrives, apply it before retrying or discarding queued incrementals for that row. Do not send a full snapshot during normal realtime updates.
- [ ] Retain protocol version 3 only if the serialized wire shape is byte-for-byte unchanged. Otherwise increment `PROTOCOL_VERSION` and update every protocol-version assertion.

### 6. Update consumers and delete obsolete code

- [ ] Update `src/api.rs`, `src/database.rs`, root tests, and `crates/test` runners to satisfy the new `R: sync::SyncRowCodec<K::Transaction>` bound wherever sync is invoked.
- [ ] Delete all obsolete engine sync helpers, imports, tests, aliases, adapters, wrappers, fallback branches, and feature flags.
- [ ] Do not retain type aliases from old engine sync types to new sync types or compatibility decoding for old messages.
- [ ] Update `CONTEXT.md` and `docs/sync-table-simplification.md` to state that sync owns protocol types and that Automerge implements `sync::SyncRowCodec`.

### 7. Required tests

- [ ] Add a compile-only test or test codec that implements `engine::RowCodec` but not `sync::SyncRowCodec`; it must compile for ordinary engine use and be unusable with `sync::synchronize`.
- [ ] Keep/add a test proving `AutomergeRowCodec` satisfies `sync::SyncRowCodec`.
- [ ] Test bootstrap convergence using full state units.
- [ ] Test a single realtime row edit transfers an incremental payload and does not transfer its complete snapshot.
- [ ] Test two independent realtime edits transfer only their distinct missing change IDs.
- [ ] Test duplicate incremental messages are idempotent.
- [ ] Test causal changes arrive and apply in dependency order.
- [ ] Test concurrent Automerge branches converge after exchanging their individual changes.
- [ ] Test unavailable history requests and applies a snapshot recovery.
- [ ] Test catalog entries and rows remain indexed correctly after bootstrap, incremental sync, and recovery.
- [ ] Test tombstone metadata propagates and remains represented in row enumeration.

### 8. Final verification

- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `cargo check --workspace`.
- [ ] Run `cargo test -p engine --lib`.
- [ ] Run `cargo test -p engine-automerge`.
- [ ] Run `cargo test -p sync --tests`.
- [ ] Run `cargo test --features='sync sql automerge redb in-memory' --test sync`.
- [ ] Run `cargo hack test --feature-powerset --workspace --all-targets`.
- [ ] Run `git diff --check`.
- [ ] Run `grep -R "DocumentChangeKey" crates/engine` and verify it has no matches.
- [ ] Run `grep -R "SyncManifest\|SyncStateUnit\|SyncKey\|StateDigest" crates/engine` and verify it has no matches.

## Completion criteria

The refactor is complete only when all of the following are true:

1. `engine` compiles and exposes no sync protocol types or Automerge change-key types.
2. `sync` owns all manifest, unit, inventory, incremental-message, and recovery types.
3. `AutomergeRowCodec` implements the `sync`-owned extension trait.
4. A non-sync `RowCodec` remains valid for normal engine use.
5. Realtime sync transfers incremental payloads keyed by opaque sync IDs, never full documents for normal edits.
6. Bootstrap and dependency recovery use full state units.
7. Applying any synced row mutation updates indexes and commits atomically.
8. Every required verification command passes.
