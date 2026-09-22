# Macro Plan: Static Suites + Generated Test Functions

## Goal

Define test suites once as static constants and generate individual `#[test]` functions per suite-runner combination via macros. Each suite gets its own test function for full libtest discoverability, parallel execution, and filtering.

## Current State

- `crates/test/` provides `TestCase`, `TestSuite`, and runner implementations
- Integration tests in `tests/` manually write `#[test]` fn per suite-runner pair (boilerplate)
- `lib.rs` is already thin (module declarations only) — must stay that way
- Suite definitions are runtime functions (`fn standard_suite() -> TestSuite`) — must become static

## Design Decisions

### 1. Static Suite Definitions (not functions)

Replace `fn standard_suite() -> TestSuite` with `const STANDARD_SUITE: TestSuite = ...;`.

**Problem**: `TestSuite` uses `Vec<TestCase>`, `TestCase` uses `Vec<Step>`, `Vec` is not `const`-constructible.

**Solution**: Change internal representation to `&'static [T]` slices:

```rust
pub struct TestSuite {
    pub name: &'static str,
    pub cases: &'static [TestCase],
}

pub struct TestCase {
    pub name: &'static str,
    pub setup: &'static [&'static str],
    pub steps: &'static [Step],
    pub expectations: &'static [Expectation],
}

pub struct Step {
    pub sql: Cow<'static, str>,
    pub expected_error: Option<ExpectedError>,
    /// True = run on all nodes, False = run on a single node (runtime picks which)
    pub all_nodes: bool,
}

impl Step {
    pub const fn sql(sql: impl Into<Cow<'static, str>>) -> Self {
        Self { sql: sql.into(), expected_error: None, all_nodes: true }
    }
    pub const fn failing(sql: impl Into<Cow<'static, str>>, expected_error: ExpectedError) -> Self {
        Self { sql: sql.into(), expected_error: Some(expected_error), all_nodes: true }
    }
}

impl TestCaseBuilder {
    pub fn step_all(mut self, sql: impl Into<Cow<'static, str>>) -> Self {
        self.steps.push(Step::sql(sql));
        self
    }
    pub fn step_one(mut self, sql: impl Into<Cow<'static, str>>) -> Self {
        let mut step = Step::sql(sql);
        step.all_nodes = false;
        self.steps.push(step);
        self
    }
}
```

This makes `TestSuite` and `TestCase` fully `const`-constructible with zero runtime allocation.

**Key design change**: `Step` no longer carries a `NodeId`. Steps only specify whether they run on all nodes or a single node; the runtime decides which node(s) to execute them on.

### 2. Macros in Dedicated Module (not lib.rs)

`lib.rs` stays thin — only `mod macros;` and re-exports. Implementation lives in `crates/test/src/macros.rs`.

```rust
// lib.rs
pub mod macros;  // test_suite!, test_case!
```

### 3. Macro API

```rust
// Static suite definition
const STANDARD_SUITE: TestSuite = TestSuite::new("standard")
    .with_case(CONCURRENT_INSERTS)
    .with_case(NEGATIVE_SYNTAX_CASE)
    .with_case(BASIC_CRUD_CASE);

// Static case definition
const CONCURRENT_INSERTS: TestCase = TestCase::builder("concurrent_user_inserts")
    .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
    .step_all("INSERT INTO users VALUES (...)")
    .step_one("INSERT INTO users VALUES (...)")
    .expect_query("SELECT name FROM users WHERE name = 'Ada'", rows![Row::from(["Ada"])])
    .build();

// Generate test functions — each becomes a separate #[test] fn
test_suite!(standard_single, STANDARD_SUITE, SingleNodeRunner);
test_suite!(standard_cluster_offline, STANDARD_SUITE, ClusterOfflineRunner);
test_suite!(standard_cluster_realtime, STANDARD_SUITE, ClusterRealtimeRunner);
test_suite!(standard_chaos, STANDARD_SUITE, ChaosRunner);

test_case!(concurrent_inserts_chaos, CONCURRENT_INSERTS, ChaosRunner);
```

### 4. Test File Organization

Each integration test file owns its suite definitions and generated tests:

```
tests/
  cluster_sync.rs      // STANDARD_SUITE × [Offline, Realtime]
  cluster_chaos.rs     // STANDARD_SUITE × [Chaos]
  local_sql.rs         // STANDARD_SUITE + SQL_SUITE × [SingleNode]
  sql_select.rs        // SQL_SUITE × [SingleNode] (case-level)
```

No shared `common/` module needed — suites are static constants defined inline or in a `tests/suites.rs` module.

## Implementation Todos

### Phase 1: Make types const-compatible

- [ ] Change `TestSuite.cases` from `Vec<TestCase>` to `&'static [TestCase]`
- [ ] Change `TestCase.setup` from `Vec<Cow<'static, str>>` to `&'static [&'static str]`
- [ ] Change `TestCase.steps` from `Vec<Step>` to `&'static [Step]` (Step has no NodeId, only `all_nodes: bool`)
- [ ] Change `TestCase.expectations` from `Vec<Expectation>` to `&'static [Expectation]`
- [ ] Change `Expectation.expected_rows` from `Vec<Row>` to `&'static [Row]`
- [ ] Add `const fn` constructors: `TestSuite::new()`, `TestCase::builder()`, `Step::sql()`, `Step::failing()`, `Expectation::all()`, `Expectation::node()`
- [ ] Remove `Step::node()` constructor — steps no longer carry NodeId
- [ ] Update `TestCase::required_nodes()` — runtime determines node count from runner, not from steps
- [ ] Add builder methods that work in const context (or use macro-based construction)
- [ ] Update all existing tests to use new static format

### Phase 2: Create macros module

- [ ] Create `crates/test/src/macros.rs` with `test_suite!` and `test_case!` macros
- [ ] Export macros from `lib.rs` via `pub mod macros`
- [ ] `test_suite!(name, suite, runner)` expands to `#[test] fn name() { run(async { <runner>::default().run_suite(&suite).await.unwrap() }) }`
- [ ] `test_case!(name, case, runner)` expands to `#[test] fn name() { run(async { <runner>::default().run_case(&case).await.unwrap() }) }`
- [ ] Verify `cargo check -p ofdb-test` passes

### Phase 3: Refactor integration tests

- [ ] Replace `tests/common/` with static suite constants in `tests/suites.rs` or inline
- [ ] Refactor `tests/cluster_sync.rs` to use `test_suite!` macro for offline + realtime
- [ ] Refactor `tests/cluster_chaos.rs` to use `test_suite!` macro for chaos
- [ ] Refactor `tests/local_sql.rs` to use `test_suite!` for single-node + SQL suite
- [ ] Refactor `tests/sql_select.rs` to use `test_case!` for individual SQL cases
- [ ] Remove `tests/common/` directory entirely

### Phase 4: Verify

- [ ] `cargo test -p ofdb-test` — all generated tests run
- [ ] `cargo test -p ofdb-test --test cluster_sync` — filtering works
- [ ] `cargo test -p ofdb-test --test cluster_chaos` — chaos tests run
- [ ] `cargo hack test --feature-powerset --all-targets` — all feature combos pass

## API Contract

```rust
// Suite construction (const)
const STANDARD_SUITE: TestSuite = TestSuite::new("standard")
    .with_case(CONCURRENT_INSERTS)
    .with_case(NEGATIVE_SYNTAX_CASE);

// Case construction (const)
const CONCURRENT_INSERTS: TestCase = TestCase::builder("concurrent_user_inserts")
    .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
    .step_all("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')")
    .step_one("INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')")
    .expect_query("SELECT name FROM users WHERE name = 'Ada'", rows![Row::from(["Ada"])])
    .expect_query("SELECT name FROM users WHERE name = 'Lin'", rows![Row::from(["Lin"])])
    .build();

// Test generation (macro)
test_suite!(standard_single, STANDARD_SUITE, SingleNodeRunner);
test_suite!(standard_offline, STANDARD_SUITE, ClusterOfflineRunner);
test_suite!(standard_realtime, STANDARD_SUITE, ClusterRealtimeRunner);
test_suite!(standard_chaos, STANDARD_SUITE, ChaosRunner);
```

## Constraints

- `lib.rs` and `mod.rs` must stay thin — only declarations and re-exports
- No implementation logic in `lib.rs` or `mod.rs`
- Macros live in `crates/test/src/macros.rs`
- All suite/case data is `const` (static, no runtime allocation)
- Each macro expansion produces exactly one `#[test] fn`
- Generated tests use existing `run()` helper for async execution
