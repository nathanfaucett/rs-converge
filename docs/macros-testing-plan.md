# Test Framework Plan: Macro-Generated Runner Matrix

Status: plan, not implemented.

## Problem

Every suite-runner combination is a hand-written `#[test]` fn with the same body:

```rust
#[test]
fn test_cluster_offline_suite() {
    run(async {
        let runner = ClusterOfflineRunner::default();
        runner.run_suite(&standard_suite()).await.unwrap();
    });
}
```

This boilerplate grows linearly with suites × runners, invites drift (e.g. `local_sql.rs` runs two suites inside one test fn, so neither can be filtered or failed independently), and gives no single place that declares which scenarios run on which execution model.

## Goal

A scenario is defined once and is only trusted once it passes on every relevant runner. Adding a case to `standard_suite()` must run it on all four runners with zero new test code. Default suites cover the 90% of SQL/database usage — CRUD lifecycle, filtering and projection, constraints and indexes, schema evolution, and concurrent multi-node writes with convergence; contract tests for unsupported SQL are isolated in a separate non-default suite.

Success criteria:

1. One macro invocation per suite-runner pair; no hand-written `run(async { ... })` in `tests/`.
2. Each pair is a real `#[test]` fn: libtest parallelism, `cargo test <name>` filtering, IDE discovery.
3. Same scenario data runs unchanged on `SingleNodeRunner`, `ClusterOfflineRunner`, `ClusterRealtimeRunner`, `ChaosRunner`.
4. Chaos tests stay `#[ignore]`d (chaos CI workflow only).
5. `lib.rs` / `mod.rs` stay thin per `AGENTS.md`.
6. Default suites (`standard_suite`, `sql_suite`) contain only 90%-usage scenarios; unsupported-SQL assertions exist only in `sql_limits_suite`.

## Non-Goals

- No new crates. Declarative (`macro_rules!`) macros need none.
- No changes to runner behavior, `Step`/`Expectation` vocabulary, or `VerificationPolicy`.
- No const/static scenario data (see "Deferred" at the end).
- `tests/database.rs`, `tests/sync.rs`, `tests/sql_select.rs` are out of scope (not runner-matrix tests).

## Crate Impact

| Crate                           | Change                                                                                                 |
| ------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `crates/test` (dep `ofdb-test`) | Add `src/macros.rs` (`test_suite!`, `test_case!`), add `src/runtime.rs` (move `run()` out of `lib.rs`) |
| `ofdb` (root)                   | Refactor `tests/cluster_sync.rs`, `tests/cluster_chaos.rs`, `tests/local_sql.rs` to macros             |
| New crates / new dependencies   | None                                                                                                   |

Note: `crates/macros` is the proc-macro derive crate for the `ofdb` facade (`FromRow`, ...). It is unrelated; the test macros live in `crates/test` to keep that boundary obvious.

## Current State

- `crates/test`: `TestCase` / `TestSuite` (runtime data, `Vec` fields, builder API), four runners behind `TestRunner` (`SingleNodeRunner`, `ClusterOfflineRunner`, `ClusterRealtimeRunner`, `ChaosRunner`), `VerificationPolicy`, chaos network, in-memory transport, cluster helpers, `run()` block-on helper.
- `tests/common/`: scenario constructors — `standard_suite()` (4 cases), `sql_suite()` (12 cases), individual case fns.
- `tests/cluster_sync.rs`: 4 hand-written fns (suite + one case × offline/realtime).
- `tests/cluster_chaos.rs`: 2 hand-written fns, both `#[ignore]`.
- `tests/local_sql.rs`: 2 hand-written fns; one fn runs `standard_suite()` **and** `sql_suite()` back to back.
- `lib.rs` contains the free fn `run()` — violates the thin-lib rule.

## Design

### D1 — Scenarios stay runtime data

`TestSuite` / `TestCase` keep their `Vec` fields and builder API. Reuse across runners comes from macros, not from const data. Scenario code in `tests/common/` is unchanged.

### D2 — Step model unchanged

Steps keep `node: NodeId`. Distributed scenarios need per-node targeting (node 0 inserts Ada, node 1 inserts Lin); an `all_nodes: bool` flag cannot express that. Documented contract (already implemented, no code change): `case.required_nodes()` is the minimum node count derived from step/expectation targets; runners enforce their own floor (cluster runners use `max(2)`), and `SingleNodeRunner` collapses all node targets onto node 0.

### D3 — Two generation macros

- `test_suite!(name, suite_expr, runner_expr)` → one `#[test] fn name` running the whole suite.
- `test_case!(name, case_expr, runner_expr)` → one `#[test] fn name` running one case.

Both accept leading attributes (for `#[ignore = "..."]`) and any runner expression, so configured runners work (`ChaosRunner::default().with_drop_rate(0.5)`).

### D4 — Hygiene

Expansions reference framework items via `$crate` (`$crate::run`, `$crate::TestRunner`), so the macros work from any consumer crate. Suite/case/runner expressions resolve at the call site; test files import them as usual.

### D5 — Granularity policy

Suite-level fns are the default. Use `test_case!` when a case needs isolated filtering or parallel execution (today: `concurrent_user_inserts`, the slow cluster scenario). A failing case aborts its suite fn — that is the signal to promote it to a `test_case!` invocation.

### D6 — Scenario home

`tests/common/` stays the single home of scenario constructors. Test files contain only imports and macro invocations.

### D7 — Thin lib

Move `run()` from `lib.rs` to `crates/test/src/runtime.rs`; `lib.rs` keeps declarations and re-exports only.

### D8 — Scenario portfolio: the 90% first

Default suites contain only scenarios representing the 90% of SQL/database usage. Contract tests for unsupported SQL are isolated in a separate, non-default suite so the default signal stays about real usage and stays stable as SQL support grows.

| Suite                                         | Cases                                                                                                                        | Proves                                                                                                                  |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `standard_suite` (all runners)                | `concurrent_user_inserts`, `basic_crud`, `unique_constraints`                                                                | Offline-first core: concurrent multi-node writes converge, full CRUD lifecycle converges, constraints hold and converge |
| `sql_suite` (single node)                     | `crud_lifecycle`, `filtering_and_projection`, `constraints_and_indexes`, `schema_evolution`, `atomic_batch`, `common_errors` | The 90% single-node SQL surface                                                                                         |
| `sql_limits_suite` (single node, non-default) | `unsupported_sql_is_rejected`                                                                                                | Unsupported SQL is rejected without mutating state                                                                      |

Case mapping from today's suites (mechanical):

- `basic_crud` → extend with `DELETE` (today it only inserts and updates — a 90% gap)
- `negative_syntax_and_schema_errors` → folded into `common_errors`; removed from `standard_suite` (error taxonomy is local engine semantics, not replication semantics)
- `wildcard_projection` + `projection_and_comparison` + `null_predicate` + `boolean_predicate` + `boolean_composition` → merged into `filtering_and_projection` (one `users` table with `id`/`name`/`active`, rows covering NULL and booleans)
- `insert_columns_defaults_and_returning` + `generated_uuid_crud_and_predicates` → merged into `crud_lifecycle`
- `indexes_after_rows_can_be_recreated` + `inline_unique_constraint` → merged into `constraints_and_indexes`
- `alter_table_default_materializes_existing_rows` → `schema_evolution`
- `failed_statement_batch_is_atomic` → `atomic_batch`
- `rejected_sql_does_not_mutate_state` → split: syntax/table/column/type/constraint steps plus the final state-unchanged proof → `common_errors`; ORDER BY / LIMIT / DISTINCT / GROUP BY / JOIN / aggregate / multi-table steps → `unsupported_sql_is_rejected`

### D9 — Suite × runner matrix

| Suite (`tests/common/`)          | SingleNode  | ClusterOffline | ClusterRealtime | Chaos                     |
| -------------------------------- | ----------- | -------------- | --------------- | ------------------------- |
| `standard_suite`                 | `local_sql` | `cluster_sync` | `cluster_sync`  | `cluster_chaos` (ignored) |
| `sql_suite`                      | `local_sql` | —              | —               | —                         |
| `sql_limits_suite` (non-default) | `local_sql` | —              | —               | —                         |

Rationale: `standard_suite` asserts replication semantics (multi-node steps, convergence) so it must run on every runner. `sql_suite` and `sql_limits_suite` assert single-node engine semantics; cluster runs would re-execute the same paths at much higher cost. Adding a suite to another runner is one macro line.

Per-case fns: `concurrent_user_inserts` × all four runners (preserves today's coverage).

## Macro API Contract

Definition (`crates/test/src/macros.rs`):

```rust
#[macro_export]
macro_rules! test_suite {
    ($(#[ $attr:meta ])* $name:ident, $suite:expr, $runner:expr) => {
        #[test]
        $(#[$attr])*
        fn $name() {
            $crate::run(async {
                let suite = $suite;
                let runner = $runner;
                $crate::TestRunner::run_suite(&runner, &suite).await.unwrap();
            });
        }
    };
}

#[macro_export]
macro_rules! test_case {
    ($(#[ $attr:meta ])* $name:ident, $case:expr, $runner:expr) => {
        #[test]
        $(#[$attr])*
        fn $name() {
            $crate::run(async {
                let case = $case;
                let runner = $runner;
                $crate::TestRunner::run_case(&runner, &case).await.unwrap();
            });
        }
    };
}
```

Usage (`tests/cluster_sync.rs` after refactor):

```rust
mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{
    ClusterOfflineRunner, ClusterRealtimeRunner, test_case, test_suite,
};

test_suite!(standard_offline, standard_suite(), ClusterOfflineRunner::default());
test_suite!(standard_realtime, standard_suite(), ClusterRealtimeRunner::default());
test_case!(concurrent_inserts_offline, case_concurrent_user_inserts(), ClusterOfflineRunner::default());
test_case!(concurrent_inserts_realtime, case_concurrent_user_inserts(), ClusterRealtimeRunner::default());
```

Chaos (`tests/cluster_chaos.rs`):

```rust
test_suite!(
    #[ignore = "chaos testing is intended for chaos CI workflow"]
    standard_chaos, standard_suite(), ChaosRunner::default()
);
```

Generated fn naming: `{suite}_{runner}` and `{case}_{runner}` (`standard_offline`, `sql_surface_local`, `concurrent_inserts_chaos`, ...), so prefix filtering selects a whole suite-runner slice: `cargo test --test local_sql sql_surface_local`.

## File Layout (after)

```
crates/test/src/
  lib.rs        // declarations + re-exports only
  macros.rs     // test_suite!, test_case!
  runtime.rs    // run()
  case.rs, chaos.rs, cluster.rs, transport.rs, runner/  // unchanged

tests/
  common/           // scenario constructors
    mod.rs          // standard_suite + standard cases
    sql.rs          // sql_suite + sql_limits_suite + sql cases
  cluster_sync.rs   // 2 test_suite! + 2 test_case!
  cluster_chaos.rs  // 1 test_suite! + 1 test_case!, both #[ignore]
  local_sql.rs      // 3 test_suite! + 1 test_case!
  database.rs, sync.rs, sql_select.rs  // unchanged
```

Root `Cargo.toml` needs no changes: test targets and `required-features` are already declared, and `ofdb-test` is already a dev-dependency.

## Implementation Checklist

### Phase 1 — Macros in `crates/test`

- [ ] Create `crates/test/src/macros.rs` with `test_suite!` and `test_case!` exactly as in the API Contract
- [ ] Create `crates/test/src/runtime.rs` with `pub fn run`; remove `run` from `lib.rs`; re-export `pub use runtime::run;`
- [ ] `lib.rs`: add `pub mod macros; pub mod runtime;` — declarations and re-exports only
- [ ] Add `crates/test/tests/macros.rs` smoke test: a no-op `TestRunner` plus one `test_suite!`, one `test_case!`, and one `#[ignore]` invocation
- [ ] Verify: `cargo test -p test --test macros` and `cargo clippy -p test --all-targets -- -D warnings`

### Phase 2 — Refactor root integration tests

- [ ] `tests/common/mod.rs`: extend `basic_crud` with `DELETE`; remove `negative_syntax_and_schema_errors` from `standard_suite`
- [ ] `tests/common/sql.rs`: build the D8 portfolio — merge predicate cases into `filtering_and_projection`, insert/default/returning cases into `crud_lifecycle`, index/unique cases into `constraints_and_indexes`; rename to `schema_evolution` and `atomic_batch`; split `rejected_sql_does_not_mutate_state` into `common_errors` (joins `sql_suite`) and `unsupported_sql_is_rejected` (new `sql_limits_suite`)
- [ ] `tests/cluster_sync.rs`: replace 4 fns with `standard_offline`, `standard_realtime` (`test_suite!`) + `concurrent_inserts_offline`, `concurrent_inserts_realtime` (`test_case!`)
- [ ] `tests/cluster_chaos.rs`: replace 2 fns with `#[ignore]`d `standard_chaos` (`test_suite!`) + `concurrent_inserts_chaos` (`test_case!`)
- [ ] `tests/local_sql.rs`: replace 2 fns with `standard_local`, `sql_surface_local`, `sql_limits_local` (`test_suite!`) + `concurrent_inserts_local` (`test_case!`); splitting the suites into separate fns is deliberate — independent filtering and failure isolation
- [ ] Confirm no boilerplate remains: `grep -rn "run(async" tests/` returns nothing
- [ ] Verify: `cargo test --test cluster_sync --test local_sql --features "sync sql automerge redb in-memory"` and `cargo test --test cluster_chaos --features "sync sql automerge redb in-memory" -- --list` (fns exist and are ignored)

### Phase 3 — Full verification

- [ ] `cargo test --test local_sql sql_surface_local --features "sql automerge redb in-memory"` — filtering works
- [ ] `just clippy` and `just fmt-check`
- [ ] `just test` (feature-powerset, all targets) before merge
- [ ] Optional: `just crap-summary` to confirm no complexity regression

## Definition of Done

- Adding a case to `standard_suite()` in `tests/common/mod.rs` runs it on all four runners with zero new test code.
- Default suites contain only 90%-usage scenarios; unsupported-SQL assertions exist only in `sql_limits_suite`.
- `cargo test --test cluster_sync -- --list` shows exactly: `standard_offline`, `standard_realtime`, `concurrent_inserts_offline`, `concurrent_inserts_realtime`.
- Chaos fns remain `#[ignore]`d.
- `lib.rs` contains no free functions.
- All previously passing tests still pass (`just test`).

## Deferred: Const Scenario Data

Static (`const`) suites were considered and dropped — they are a hope, not a requirement, and the blockers are structural: `Vec` fields are not const-constructible (would need `&'static [T]` everywhere), `impl Into` bounds are unusable in `const fn`, and `Row`/`Value` contain `String`/`Vec` so expected rows need a const-friendly mirror type. Revisit only if zero-allocation static suites become a real requirement; the macro API above would not change.
