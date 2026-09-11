# Project Map and Review

## Executive Summary

This repository is intended to become a modular, async-first database foundation. Its design separates:

```text
SQL or programmatic query AST
        -> translator
        -> engine and catalog
        -> kernel transaction
        -> B-tree abstraction
        -> storage backend
```

The repository currently contains useful lower-level pieces: ordered key/value storage, an in-memory B-tree, a Redb adapter, an Automerge document adapter, database values, schemas, and a SQL parser/translator.

The in-memory path now supports a narrow end-to-end slice: create a table, insert a primary-keyed row, and perform a simple single-table projection. Redb and Automerge are B-tree adapters rather than engine backends. Joins, predicates, mutations beyond insert, indexes, transaction atomicity, and several SQL clauses remain unsupported.

## What It Is Trying To Do

The stated goal is a backend-independent database foundation with:

- backend-agnostic adapters;
- pluggable persistent implementations;
- async-compatible APIs;
- explicit transaction lifecycle and atomic operation sets;
- table and index abstractions;
- a PostgreSQL-like programmatic query API;
- SQL translation into that query API;
- a planner capable of joins and aggregations.

The design document makes a stronger claim than the current code: every read/write/catalog operation should run through one strict ACID transaction boundary, with atomicity, consistency, isolation, durability, and explicit commit/rollback behavior.

## Crate Map

| Crate                                | Intended role                                                      | What it currently does                                                                                                           | Assessment                                                                                                                                                                          |
| ------------------------------------ | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db`                                 | Top-level facade and feature selection                             | Re-exports the engine, SQL translator, and optional B-tree adapters.                                                             | Backend adapters are opt-in; the in-memory engine feature is available for examples.                                                                                                |
| `db-btree`                           | Backend-neutral ordered storage contract                           | Defines read, range, transaction, commit, rollback, and error traits.                                                            | The abstraction is real and usable. Its transaction semantics and conflict behavior are not specified.                                                                              |
| `db-engine` in-memory implementation | Test and example backend                                           | Provides a feature-gated named-table kernel backed by in-memory B-trees.                                                         | Supports the narrow create/insert/simple-select slice; its kernel transaction does not provide rollback or atomic multi-table changes.                                              |
| `db-btree-redb`                      | Persistent Redb implementation of the B-tree contract              | Implements Redb-backed reads, ranges, transactions, codecs, and table access.                                                    | A B-tree adapter only; it has no engine or catalog concern. Blocking Redb calls occur inside async methods, and unsafe `Send` implementations need tighter bounds or justification. |
| `db-btree-automerge`                 | Document-oriented B-tree adapter                                   | Reconstructs documents from snapshots and deltas, updates documents, and compacts history.                                       | A B-tree adapter only; removing a document deletes its history rather than recording a tombstone.                                                                                   |
| `db-value`                           | Database scalar, row, and JSON value model                         | Defines values, ordering, hashing, JSON conversion, and optional Redb/Automerge codecs.                                          | Core value support exists. Backend codec concerns are coupled into the value crate; JSON number edge cases need explicit tests, especially NaN behavior.                            |
| `db-schema`                          | Table, column, and index metadata                                  | Provides serializable schema structs.                                                                                            | Passive data structures only. There is no schema validation or invariant enforcement.                                                                                               |
| `db-query`                           | Programmatic query AST and result model                            | Defines SELECT/DML/DDL types, expressions, joins, aggregates, pagination, returning clauses, parameters, and a translator trait. | The AST models more functionality than the engine and SQL translator currently implement.                                                                                           |
| `db-sql-translator`                  | SQL parser and AST translator                                      | Parses a subset of SQL and creates `db-query` statements.                                                                        | Useful prototype, but several inputs translate successfully while losing semantics.                                                                                                 |
| `db-engine`                          | Generic catalog, kernel contract, schema operations, and execution | Defines `Engine`, catalog constants, generic `Kernel` traits, and the optional in-memory kernel.                                 | It does not depend on Redb or Automerge. The executor supports only create-table, insert, and simple select.                                                                        |
| `db-examples-util`                   | Shared CLI and scripted example runner                             | Translates and executes example SQL with parameters.                                                                             | Compiles against `Kernel`; its parameter and join examples exceed the current supported executor slice.                                                                             |
| `db-wasm`                            | WebAssembly-facing database API                                    | Rust source only contains a `wasm32` compile guard.                                                                              | The checked-in generated package and README describe an API that is not generated by the current Rust source.                                                                       |
| `db-proto`                           | Protobuf/gRPC definitions                                          | Builds generated types from `db.proto`.                                                                                          | Not a root workspace member or referenced by the engine. The protocol messages are skeletal.                                                                                        |

## Does It Do What It Says?

| Claim                          | Observed status             | Evidence                                                                                                                                                                                                               |
| ------------------------------ | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Async-first API                | **Partly**                  | Public traits are async, but Redb operations and commits are synchronous calls made inside async functions.                                                                                                            |
| Pluggable backends             | **Partly**                  | `db-engine` is backend-neutral. Redb and Automerge are independent B-tree adapters; only the in-memory kernel currently implements the engine contract.                                                                |
| Strict ACID engine transaction | **No**                      | The in-memory kernel transaction is a lightweight wrapper; child B-tree commits and table creation are not coordinated.                                                                                                |
| Atomic catalog changes         | **No**                      | `create_table` updates catalog tables through separate lower-level transactions. A later failure can leave partial metadata.                                                                                           |
| Query execution                | **Partly**                  | The executor supports CREATE TABLE, INSERT, and simple single-table SELECT. Other query and DDL forms return explicit unsupported errors.                                                                              |
| Joins and aggregations         | **No**                      | The AST has fields for them, but the executor is absent and SQL translation emits no aggregates or grouping.                                                                                                           |
| SQL parameters                 | **No**                      | Translation accepts `QueryParams`, but translation functions ignore them.                                                                                                                                              |
| SQL fidelity                   | **No**                      | SELECT pagination/order/grouping, UPDATE assignments, UPDATE/DELETE returning clauses, and ALTER TABLE operations are dropped. Multi-row INSERT reads only the first row. CREATE INDEX maps every column to index `0`. |
| Persistent Redb storage        | **Partly**                  | `db-btree-redb` provides persistent B-tree storage, but no engine kernel composes it yet.                                                                                                                              |
| In-memory examples             | **Partly**                  | Examples construct `InMemoryKernel`, but their parameterized/join SQL exceeds the supported executor slice.                                                                                                            |
| WASM API                       | **No**                      | Current Rust WASM source exports no database API, despite the checked-in generated package documenting one.                                                                                                            |
| `no_std` support               | **No, as a complete stack** | The root crate can set `no_std`, but it unconditionally enables `db-engine`'s `std` feature and the Redb/Automerge backend crates depend on standard-library functionality.                                            |

## Highest-Impact Gaps

### 1. The engine slice is deliberately narrow

`db-engine/src/executor.rs` supports CREATE TABLE, INSERT, and simple single-table SELECT. It rejects joins, predicates, aggregates, ordering, pagination, update, delete, index DDL, and other DDL explicitly.

The next executor milestone should add one semantic capability at a time, with public-engine tests before extending the SQL translator.

### 2. The transaction boundary is only an interface

The design requires one transaction to cover all catalog, table, and index changes. The current in-memory kernel creates tables immediately, and nested B-tree transactions commit independently. `KernelTransaction::commit` and `rollback` cannot coordinate those writes.

This is a correctness issue, not just missing polish. A failed multi-table operation can leave the catalog inconsistent. The kernel should own the backend transaction for its entire lifetime, or the abstraction should explicitly be redesigned around composable backend transactions.

### 3. Translator success can hide data loss

The SQL translator often returns an apparently valid AST after discarding requested behavior. That is more dangerous than rejecting unsupported SQL because callers cannot distinguish full execution from partial translation.

Until each clause is implemented, unsupported clauses should return translation errors. Then add focused tests for parameters, multi-row insert, update assignments, returning, pagination, ordering, grouping, indexes, and alter-table operations.

### 4. Backend and workspace wiring is incomplete

The facade now has opt-in `redb` and `automerge` B-tree-adapter features, while the in-memory engine remains feature-gated for tests and examples. `db-proto` remains outside the workspace; `db-schema` and `db-examples-util` are implicit workspace members. The remaining workflow gap is that example SQL uses unsupported parameters and joins.

### 5. Generated and documented surfaces have drifted

The root README contains only badges. The WASM README and generated package describe a `BrowserDatabase` API that is absent from the Rust source. The design document describes joins, aggregation, and strict ACID semantics that have no working implementation. Documentation should be brought into alignment with a staged implementation plan, or the missing APIs should be implemented.

## Test and Verification Gaps

The current tests do not cover the most important public behavior:

- no engine execution tests beyond CREATE TABLE, INSERT, and simple SELECT;
- no engine transaction tests for rollback, isolation, or multi-table atomicity;
- no catalog consistency tests for failed create/drop operations;
- no SQL translator tests for parameters and silently dropped clauses;
- no join or aggregation execution tests;
- no Automerge persistence or compaction tests; basic reconstruction, malformed-document isolation, and deletion are covered;
- no in-memory engine tests beyond the create/insert/simple-select slice;
- no WASM build/API synchronization test;
- no concurrency tests for backend transactions or async executor blocking.

`cargo check --workspace --all-targets` passes.

## Recommended Order of Work

1. Add public API tests for in-memory rollback, duplicate keys, schema validation, and unsupported query shapes.
2. Make unsupported SQL fail explicitly; then implement parameters and mutation/pagination semantics incrementally.
3. Redesign `KernelTransaction` to own table creation and child B-tree writes atomically.
4. Add index/catalog behavior and validate schema invariants.
5. Define a generic composition path for persistent B-tree adapters without coupling them to `db-engine`.
6. Either regenerate the WASM package from a real Rust API or mark it as stale and remove unsupported claims.
7. Add `db-proto` to the workspace, or document why it is standalone.

## Bottom Line

The repository now has a tested in-memory engine slice and clean B-tree-adapter layering, but it is not yet a general database engine. The highest-value next step is strengthening transaction ownership and extending execution semantics incrementally through public tests.
