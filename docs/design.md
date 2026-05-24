# Design

### Overview

We are building a modular, async-first database foundation centered on a strict ACID transaction boundary. The backend contract is transaction-first: every read/write/index/catalog operation executes through a transaction object, and every transaction must support `commit` and `rollback`.

- Backends expose one entrypoint: `begin_transaction()`.
- The returned transaction object exposes row operations, index operations, schema operations, and lifecycle control.
- `commit()` and `rollback()` are required; there are no non-transactional mutation paths.

This approach guarantees that row and index changes can be coordinated atomically in one unit and removes ambiguity from optional or fallback write paths.

### Goals

- Enforce one strict ACID backend boundary.
- Make mutation semantics explicit: all writes must use a transaction object.
- Require lifecycle control (`commit`/`rollback`) for every backend implementation.
- Support pluggable async backends for both tables/indexes and durable storage.
- Keep adapter logic backend-agnostic and async-compatible.
- Enable batched multi-row updates via transaction objects in higher-level APIs.

### Key Concepts

- **Transactional Store**: A shared async map trait that can create a transaction object for mutations.
- **Transaction Object**: The only mutation path for inserts, removes, and commit/rollback.
- **TableBTree**: A table-oriented adapter that uses a transaction to update row data and supporting indexes together.
- **IndexBTree**: An index-oriented adapter that updates tuple-to-record mappings through transactions.
- **Backend swap**: Any semantic adapter can be backed by any compatible transactional B-tree store.

### Core Store Abstraction

The central contract is transaction-first. A core store implementation should provide one entrypoint to create a transaction object, and that transaction object is the only place where row/index/schema operations are defined.

Implementation note: internal storage engines may still use `BTree<K, V>`-style abstractions, but that is an internal detail rather than the public backend boundary.

Example internal API:

```rust
pub trait BTree<K, V>: Send + Sync {
    type Transaction<'a>: BTreeTransaction<K, V> + Send + 'a where Self: 'a;

    fn get<'a, Q>(&'a self, key: &'a Q) -> Self::GetFuture<'a, Q>
    where
        K: Borrow<Q> + Ord,
        Q: Ord + ?Sized + Sync;

    fn range<'a, T, R>(&'a self, range: R) -> Self::RangeStream<'a, T, R>
    where
        T: Ord + ?Sized + Send,
        K: Borrow<T> + Ord,
        R: RangeBounds<T> + Send;

    fn begin_transaction<'a>(&'a self) -> Self::BeginTransactionFuture<'a>;
}
```

### Transaction Abstraction

A transaction object is the required entrypoint for all backend operations. It should support:

- `get(key) -> Option<value>`
- `insert(key, value) -> Result<(), StoreError>`
- `remove(key) -> Option<value>`
- `range(range) -> Stream<(key, value)>`
- `commit() -> Result<(), StoreError>`
- `rollback() -> Result<(), StoreError>`

That enables higher-level adapters to:

- update a row and its indexes in the same transaction,
- batch multiple changes across rows before committing,
- coordinate table and index changes atomically.

### Adapter Specializations

Adapters are semantic layers built on top of the transactional store API.

#### TableBTree

`TableBTree` is a table/document adapter that uses a transactional `BTree` store.

Responsibilities:

- store records by a primary key or document identifier
- read records with `get` and `scan`
- perform all writes through a `BTreeTransaction`
- update supporting index state within the same transaction

Example semantics:

- key: document ID
- value: serialized row or document bytes

#### IndexBTree

`IndexBTree` is an index adapter that also uses transactions for write semantics.

Responsibilities:

- map index keys to record identifiers
- support index insert/delete semantics
- perform writes through a `BTreeTransaction`
- allow combined table/index changes to be grouped in one transaction

Example semantics:

- key: index tuple
- value: record ID or list of record IDs

### Persistent Stores

Persistent stores implement the transactional store API for raw bytes.

Responsibilities:

- handle raw byte key/value semantics
- expose the same async read contract as the core store
- support transaction creation, commit, and rollback
- serve as durable backing for higher-level adapters

### Adapter and Store Interactions

Adapters and stores share a single transactional interface with required lifecycle control.

- `BTree` is the base store trait.
- `BTreeTransaction` is the mutation object returned by the store.
- `TableBTree` and `IndexBTree` are semantic adapters that never mutate outside a transaction.
- Higher-level APIs can create a transaction, apply multiple row and index updates, and commit atomically.

### ACID Contract Requirements

- **Atomicity**: backend-visible changes become durable only on successful `commit`.
- **Consistency**: row and index updates must stay in sync across a transaction boundary.
- **Isolation**: uncommitted writes must not leak outside the transaction context.
- **Durability**: successful `commit` must survive backend durability guarantees.

Backends that cannot provide these guarantees are not valid implementations of the store boundary.

### Implementation Scope

This document is a blueprint for the core transactional store API and its first adapter model. Detailed storage formats, locking, and query planning remain out of scope.

Next step: implement `BTree` and `BTreeTransaction` in `crates/db-core`, then add semantic adapter traits and supporting store implementations.

### Engine Query Layer (db-engine)

The `db-engine` crate provides a small, programmatic relational query layer on top of the table/index adapters. It is intentionally narrow: a correctness-first executor that makes it easy to run multi-table queries without a full SQL parser or optimizer.

- Public API: an enum-based programmatic API (`EngineQuery`) with a unified `Select` variant that accepts qualified projections (`table` + `column_index`), join clauses, aggregates, `GROUP BY`, `ORDER BY`, and `LIMIT`/`OFFSET` options.
- Join support: inner, left, right, and full joins are supported. Multiple joins are executed left-deep in the order they are provided.
- Execution model: a left-deep, nested-loop executor implemented in `EngineKernel::read_extended`.
  - The executor collects rows for referenced tables (via transactional scans) and composes partial row maps during join processing.
  - Outer joins use null-extension semantics: when a row has no matching counterpart, the missing side's projected columns yield `NULL`.
- Aggregation: `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` are supported. `COUNT(None)` represents `COUNT(*)`. Aggregates are computed per-group by maintaining per-group aggregator state during execution.
- Ordering & limits: `ORDER BY` and `LIMIT`/`OFFSET` are supported. For grouped queries, `ORDER BY` currently supports ordering by group keys or aggregate outputs.

### Index Use and Performance

- Single-table selects continue to use index-assisted lookups when a matching index exists for the predicate.
- Join execution currently uses nested-loop semantics. Index-assisted join lookup (probe the right side using an index keyed by the join expression) is not implemented in the MVP but is planned as an optimization.
- The current executor prioritizes correctness and clear semantics over performance. For large datasets, nested-loop join behavior may be slow; future improvements include join reordering, index probes, and streaming execution.

### Semantics & Limitations

- Not a SQL engine: there is no SQL parser or planner in this crate. The API is programmatic and intentionally explicit.
- Not feature-complete: HAVING, DISTINCT, window functions, subqueries, and complex expression planning are out of scope for the initial implementation.
- Single-threaded executor: the current implementation performs execution in-process and is not parallelized.

### Tests

The engine layer includes unit tests that exercise the new features and verify semantics. See `crates/db-engine/src/engine.rs` for tests such as:

- `inner_join_simple`
- `left_join_simple`
- `right_join_simple`
- `full_join_simple`
- `multiple_joins_chain`
- `group_by_count_and_sum`
- `order_by_and_limit`

### Layout backend, format bridge, and facade

Crate **`db-named-bridge`** glues any named-tree layout backend to a format handler:

- **`TreeLayoutCatalog`** — tree registry and discovery on the layout backend.
- **`PerTreeFormatSync`** — format-specific merge for one logical tree.
- **`sync_cataloged_layouts`** — layout-level sync (catalog + per-tree format sync).
- **`LayoutFormatBridge`** — engine-facing `NamedBTreeMap` + `EngineNamedTreeBackend<EngineKey, Vec<u8>>` built
  from a layout backend.

Automerge is one format implementation (`db-named-bridge::automerge`):

- **Layout backend**: `InMemoryAutomergeLayoutBackend`, `RedbAutomergeLayoutBackend`.
- **Bridge**: `AutomergeFormatAdapter` (document encoding only; no sync on the bridge).
- **Layout sync**: `sync_automerge_layouts`, `automerge_layout_metrics`.

**`db-facade`** only opens `Database` via constructors and exposes `automerge_layout()` for
callers that need layout-level sync. SQL `Database` does not implement peer sync.

### Future Work (query layer)

- Add index-assisted join probes and simple cost heuristics for join ordering.
- Implement a lightweight logical plan representation to enable filter/aggregate pushdown and join reordering.
- Add streaming execution to avoid collecting entire tables into memory.
- Expand expression support and add a SQL front-end if desired.
