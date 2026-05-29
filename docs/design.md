# Design

### Overview

We are building a modular, async-first database foundation centered on a strict ACID transaction boundary. The backend contract is transaction-first: every read/write/index/catalog operation executes through a transaction object.

### Goals

- Enforce one strict ACID backend boundary.
- Make mutation semantics explicit: all writes must use a transaction object.
- Require lifecycle control (`commit`/`rollback`) for every backend implementation.
- Support pluggable async backends.
- Keep adapter logic backend-agnostic and async-compatible.
- Enable batched multi-row updates via transaction objects in higher-level APIs.

### ACID Contract Requirements

- **Atomicity**: backend-visible changes become durable only on successful `commit`.
- **Consistency**: row and index updates must stay in sync across a transaction boundary.
- **Isolation**: uncommitted writes must not leak outside the transaction context.
- **Durability**: successful `commit` must survive backend durability guarantees.

### Core Store Abstraction

The central contract is transaction-first. A implementation should provide one entrypoint to create a transaction object, and that transaction object is the only place where operations are defined.

### Adapter Specializations

Adapters are semantic layers built on top of the transactional API.

#### Tables

Responsibilities:

- store records by a single primary key (UUIDs).
- read records with `get` and `scan`
- perform all writes through a transaction

#### Indexes

Responsibilities:

- map index keys to record keys (UUIDs)
- support index insert/delete semantics
- perform all writes through a transaction
- allow combined table/index changes to be grouped in one transaction

Backends that cannot provide these guarantees are not valid implementations of the store boundary.

### Query Layer

- an enum-based programmatic API (`Query`) syntax based on PostgreSQL semantics
- Intential simple like SQLite's `Statement` API.
- a query planner that translates `Query` objects into transaction operations.
- JOINs, Aggregations should be supported by the query planner.