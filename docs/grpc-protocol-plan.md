# Server/Client Protocol Plan

## Goal

Expose the existing `query::Statement` batch API through a small, transport-neutral
protocol. Ship gRPC with Tonic first, then adapt the same protocol for HTTP,
gRPC-Web, WebSockets, and Unix-domain sockets without changing query semantics.

Integrate the client into `Database::open_uri` so local and remote databases use
one application-facing entry point:

```text
:in_memory:                  local in-memory database
ofdb://./local.db            local file database
ofdb+grpc://db.example:50051 remote database over gRPC/TCP
ofdb+unix:///run/ofdb.sock   remote database over gRPC/Unix socket
```

`Database::open_uri` remains a synchronous constructor. Remote construction must
create a lazy Tonic channel; the first database operation establishes the
connection and returns connection errors through the existing async result.

## Scope

The first version has one operation:

```text
Execute(Statement batch) -> QueryResult batch
```

One request maps to one `Engine::execute` call. Therefore all statements in a
request share the Engine transaction boundary: the batch commits together or the
request fails. The protocol does not expose begin, commit, or rollback.

The remote `Database` initially supports `execute` and the two translation
methods by sending their resulting `query::Statement` batches. SQL translation
therefore remains client-side, using the caller-provided `Translator`.

Until corresponding RPCs exist, metadata, replication, conflict-resolution, and
checkpoint methods on `Database::Remote` return a clear unsupported-operation
error. Do not silently execute those methods against a local database or invent
client-side metadata.

Out of scope for v1:

- authentication, authorization, and tenant routing;
- server-side SQL text translation and query parameters;
- replication, checkpoints, conflict resolution, and subscriptions;
- pagination streams and cursors; and
- a WebSocket-specific RPC framing protocol.

## Transport Decision

The protocol is the protobuf messages and their request/response semantics.
gRPC is its first transport, not the protocol itself.

| Need                     | v1 approach                                            | Notes                                                                                                        |
| ------------------------ | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Native TCP client/server | Tonic gRPC over HTTP/2                                 | Primary supported path. `ofdb+grpc://host:port` selects it.                                                  |
| Unix socket              | Tonic server/client with a Unix listener and connector | gRPC remains HTTP/2; only the byte transport changes. `ofdb+unix:///path.sock` selects it.                   |
| Browser HTTP             | Add gRPC-Web adapter later                             | Standard browsers cannot use native Tonic gRPC directly.                                                     |
| Plain HTTP               | Add a JSON/protobuf HTTP adapter later                 | It calls the same transport-neutral service.                                                                 |
| WebSockets               | Add an explicit WebSocket adapter later                | Tonic does not turn a gRPC service into WebSocket RPC. Define framing only when a WebSocket consumer exists. |

Do not put HTTP, WebSocket, or Unix-socket logic in the query conversion layer.
Each adapter must invoke the same application service.

### Database URI rules

Extend the existing URI parser without changing the meaning of `:in_memory:` or
`ofdb://<path>`:

| URI                      | Parsed target                                                            | Required feature          |
| ------------------------ | ------------------------------------------------------------------------ | ------------------------- |
| `:in_memory:`            | `Database::InMemory`                                                     | `in-memory` + `automerge` |
| `ofdb://<path>`          | `Database::File`                                                         | `redb` + `automerge`      |
| `ofdb+grpc://host:port`  | `Database::Remote` with TCP endpoint                                     | `remote`                  |
| `ofdb+grpcs://host:port` | Reserved for TLS; reject in v1 with an explicit unsupported-scheme error | future `tls`              |
| `ofdb+unix:///path.sock` | `Database::Remote` with Unix endpoint                                    | `remote` + `unix`         |

The remote URI identifies the server endpoint, not a second database name. A
server process owns the selected database instance. Reject credentials, query
parameters, fragments, missing hosts, invalid ports, and non-empty Unix URI
hosts until they are explicitly specified.

## Wire Contract

Keep `package db;` for the initial version. Add the service and request envelope
to `crates/proto/proto/db.proto`:

```proto
service QueryService {
  rpc Execute(ExecuteRequest) returns (ExecuteResponse);
}

message ExecuteRequest {
  repeated Statement statements = 1;
}

message ExecuteResponse {
  repeated QueryResult results = 1;
}
```

The response has one result for each submitted statement, in statement order.
An empty batch is invalid. The server must reject a request that has no selected
`oneof` variant or that cannot be converted losslessly to `query` types.

### Required protobuf alignment

`db.proto` is currently a sketch. Before adding the service, make every message
losslessly represent its equivalent in `crates/query/src/query.rs` and its
`schema`/`value` dependencies.

| Rust API                                         | Required protobuf rule                                                                                                                                                        |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `value::Value`                                   | Model every variant: `Null`, `Type`, `Uuid`, `Bool`, `Integer`, `Float`, `Text`, `Json`, and `Blob`. `Value` must use a `oneof`; protobuf message presence represents `Null`. |
| `schema::ColumnSchema`                           | Include `name`, `ValueType`, `default`, and `primary_key`.                                                                                                                    |
| `schema::IndexSchema`                            | Include `name`, `table_name`, ordered `column_indices`, and `unique`.                                                                                                         |
| `schema::TableSchema`                            | Include `name` and ordered columns.                                                                                                                                           |
| `QueryInsert.returning`                          | Preserve `Option<Vec<String>>`; an absent field differs from a present empty list. Wrap the list in a message, rather than using `repeated string` directly.                  |
| `QueryUpdate.returning`, `QueryDelete.returning` | Preserve `Option<Vec<QueryColumn>>` with the same wrapper pattern.                                                                                                            |
| `QuerySelect.limit` and `offset`                 | Keep optional scalar presence, which matches `Option<usize>` after checked `u64` conversion.                                                                                  |
| `QueryExpr` and statement enums                  | Reject an absent or unknown `oneof`; never map it to a default query shape.                                                                                                   |
| `QueryResult`                                    | Preserve ordered rows and complete result-column metadata.                                                                                                                    |

Use `optional` only where scalar presence is meaningful. Use wrapper messages
for optional repeated fields because protobuf repeated fields have no presence.
Do not retain placeholder comments or unused enums such as
`QueryInsertValueKind` after the replacement schema is complete.

### Errors

Use gRPC status codes at the transport boundary:

| Condition                                                  | Status                                                                       |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Malformed protobuf shape, invalid conversions, empty batch | `INVALID_ARGUMENT`                                                           |
| Unsupported executor feature/query shape                   | `UNIMPLEMENTED`                                                              |
| Missing table/index or invalid database request            | `FAILED_PRECONDITION` or `NOT_FOUND`, according to the concrete engine error |
| Unexpected engine/storage failure                          | `INTERNAL`                                                                   |
| Request deadline expires                                   | `DEADLINE_EXCEEDED`                                                          |

Initially return concise status messages only. Introduce typed protobuf error
details only when a client needs stable machine-readable error codes.

## Crate Boundaries

```text
crates/proto/       Canonical .proto files and generated Prost/Tonic bindings
crates/protocol/    Proto <-> query/schema/value conversions and transport-neutral service trait
crates/server/      Tonic QueryService implementation and listener bootstrap
crates/client/      Tonic client wrapper and URI endpoint constructors
src/database.rs     Local/remote Database enum and open_uri dispatch
src/uri.rs          Local and remote URI parsing
```

`crates/protocol` depends on `proto`, `query`, `schema`, and `value`; it does not
depend on Tonic server transport types. It owns all checked conversions and
conversion tests.

`crates/client` owns Tonic client transport details and exposes a small typed
client with `execute`, `connect_tcp_lazy`, and `connect_unix_lazy` operations.
It must not expose generated protobuf types from the `Database` API.

Add a `Remote` variant to `Database` in `src/database.rs`, containing the typed
client. Extend `database_call!` or replace it with a dispatch helper so local
Engine variants and the remote client share the supported async methods without
duplicating URI or conversion logic. Keep the existing local variants and URI
behavior unchanged.

Define a small async application boundary in `crates/protocol`, conceptually:

```rust
trait QueryExecutor {
    async fn execute(&self, statements: Vec<query::Statement>)
        -> Result<Vec<query::QueryResult>, QueryServiceError>;
}
```

Implement it for the selected `Engine<K, R>` configuration in the server crate
or in a dedicated engine adapter module. The Tonic handler only:

1. validates and converts `ExecuteRequest`;
2. calls `QueryExecutor::execute`; and
3. converts results or errors to the wire response/status.

Keep `crates/proto` generated-code-only. Its `lib.rs` remains thin.

## Implementation Checklist

### 1. Establish the workspace and feature model

- [ ] Add `crates/proto` to the root workspace; it currently has Tonic-related
      dependencies but is not a workspace member.
- [ ] Define root `[workspace.dependencies]` or replace the existing `workspace`
      dependency references in `crates/proto/Cargo.toml`; choose one consistent
      workspace convention.
- [ ] Create `crates/protocol`, `crates/server`, and `crates/client` as separate
      workspace crates.
- [ ] Add the client crates/features to the root `ofdb` package. Keep `remote`
      disabled by default unless the project decides that network support belongs in
      the default feature set.
- [ ] Keep protocol/query conversion `no_std + alloc` where possible. Gate
      Tonic, Tokio, network listeners, and Unix sockets behind `std` features.
- [ ] Add `grpc`, `tcp`, and `unix` server/client features. Make Unix unavailable
      on unsupported targets at compile time; do not simulate it with TCP.

### 2. Replace the protobuf sketch with the canonical query schema

- [ ] Rewrite `crates/proto/proto/db.proto` to include every query, schema, and
      value field listed above.
- [ ] Add `QueryService`, `ExecuteRequest`, and `ExecuteResponse`.
- [ ] Reserve removed field numbers and message/enum names before future schema
      revisions. Never reuse a released field number.
- [ ] Regenerate bindings with the existing `tonic-prost-build` build script.
- [ ] Add protobuf round-trip tests for representative nested expressions,
      every `Value` variant, optional `returning`, DDL, and results.

### 3. Implement lossless conversions

- [ ] Add dedicated modules in `crates/protocol` for `value`, `schema`, `query`,
      `result`, and `error` conversion.
- [ ] Implement `TryFrom<proto::...>` for wire-to-domain conversion and `From`
      for domain-to-wire conversion where conversion cannot fail.
- [ ] Check `u64` to `usize` conversions for `limit` and `offset` and return a
      conversion error on overflow.
- [ ] Validate row/value shapes at the protocol boundary where they can be
      validated without duplicating Engine semantic checks.
- [ ] Ensure an absent `oneof` and unsupported generated enum value produce an
      error, never a default domain value.

### 4. Add the transport-neutral service

- [ ] Define `QueryExecutor` and `QueryServiceError` in `crates/protocol`.
- [ ] Make the executor adapter call `Engine::execute` exactly once per request.
- [ ] Map known `EngineError` variants to protocol errors without exposing
      backend implementation strings as a stable client contract.
- [ ] Test that a multi-statement request invokes one batch execution and
      preserves result ordering.

### 5. Ship the Tonic server and client

- [ ] Implement generated `proto::query_service_server::QueryService` in
      `crates/server`.
- [ ] Convert request, delegate, and map the response/error using
      `tonic::{Request, Response, Status}`.
- [ ] Provide TCP serving with explicit bind address, graceful shutdown input,
      request-size limits, and deadline propagation.
- [ ] Provide Unix-domain-socket serving with `tokio::net::UnixListener` and
      Tonic's incoming-stream server API. Define socket-file cleanup and ownership
      rules in the server configuration API.
- [ ] Add `crates/client` as a thin typed wrapper over generated Tonic client
      bindings, including `connect_tcp_lazy` and a Unix connector constructor behind
      the `unix` feature.
- [ ] Make the client wrapper implement the remote `execute` operation and map
      Tonic status failures to `EngineError::Custom` or a dedicated public remote
      error variant.
- [ ] Add `UriScheme::Grpc` and `UriScheme::Unix` in `src/uri.rs`, preserving the
      existing local variants and rejecting invalid endpoint components.
- [ ] Add `Database::Remote(Client)` in `src/database.rs` and dispatch
      `Database::open_uri` to lazy TCP/Unix client constructors when the `remote`
      feature is enabled. Return a feature-disabled error otherwise.
- [ ] Route `Database::execute`, `translate_and_execute`, and
      `translate_and_execute_with_params` through the remote client. Keep translation
      local, then send the resulting statement batch.
- [ ] Return a deliberate unsupported-operation error for remote metadata,
      replication, checkpoint, conflict, and resolution methods until their RPCs
      are added; add tests for this behavior.
- [ ] Do not add retries by default: writes are not safe to retry without a
      request idempotency contract.

### 6. Test and document the contract

- [ ] Unit-test each conversion module, including all absent `oneof` cases and
      every optional-field distinction.
- [ ] Unit-test URI parsing for local, TCP, Unix, missing host, invalid port,
      invalid Unix authority, reserved TLS, credentials, query, and fragment cases.
- [ ] Test `Database::open_uri` constructs local variants as before, constructs a
      lazy `Database::Remote` for valid TCP/Unix URIs, and returns a feature error
      when remote support is disabled.
- [ ] Add server integration tests over loopback TCP for successful execution,
      invalid request mapping, unsupported query mapping, ordered batch results,
      and atomic failure of a write batch.
- [ ] Add Unix integration tests behind `cfg(unix)` using the same
      `Database::open_uri("ofdb+unix:///...")` path.
- [ ] Add a generated-client integration test against the Tonic server.
- [ ] Add a compatibility test that encodes fixed v1 protobuf fixtures and
      decodes them with the current bindings.
- [ ] Run `just fmt-check`, `just clippy`, `just test`, and `just crap`.

## Acceptance Criteria

- `Database::open_uri("ofdb+grpc://host:port")` returns a remote database that
  lazily connects and executes through the generated Tonic client.
- `Database::open_uri("ofdb+unix:///path.sock")` returns a remote database that
  uses the Unix-domain-socket connector with the same RPC behavior.
- Existing local URI behavior is unchanged, and remote schemes fail clearly
  when the required feature is disabled.
- A generated Tonic client can submit a valid `ExecuteRequest` over TCP and
  receive ordered `QueryResult` values identical to direct `Engine::execute`.
- A malformed or lossy wire request is rejected before execution.
- One request executes exactly one Engine statement batch, preserving its
  transaction boundary.
- Remote `Database::execute` and local translation methods behave the same for
  supported operations; unsupported remote methods return explicit errors.
- Unix-domain-socket clients use the same generated RPC and get the same
  behavior as TCP clients.
- The protocol schema represents all currently public `query`, `schema`, and
  `value` data without placeholders or silent semantic loss.
- Adding HTTP, gRPC-Web, or WebSocket support requires only a new adapter over
  `QueryExecutor`; it does not change protobuf/domain conversions or Engine
  execution behavior.
