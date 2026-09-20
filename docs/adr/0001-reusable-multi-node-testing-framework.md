# ADR 0001: Reusable Multi-Node Testing Framework

## Status

Accepted

## Context

The Converge database engine is designed for local-first, distributed environments where independent nodes execute transactional changes offline or online and later converge via replication envelopes and checkpoints.

Integration tests in [`tests/`](file:///home/nathan/code/rust/aicacia/converge/tests/) and [`crates/test/`](file:///home/nathan/code/rust/aicacia/converge/crates/test/) have previously been bifurcated:

1. **Single-node query checks ([`Case`](file:///home/nathan/code/rust/aicacia/converge/crates/test/src/case.rs)):** Tested single queries sequentially on isolated instances, lacking multi-step workflows, multi-node replication, and network fault tolerance.
2. **Ad-hoc cluster scripts ([`tests/sync.rs`](file:///home/nathan/code/rust/aicacia/converge/tests/sync.rs) & [`crates/test/src/chaos.rs`](file:///home/nathan/code/rust/aicacia/converge/crates/test/src/chaos.rs)):** Imperatively instantiated clusters, performed manual sync routines, and asserted state. These could not be reused across single-node query validations or alternate network topologies without rewriting.

---

## Architectural Principles & Decisions

### 1. Tests as Pure Data

- **Zero Runner Logic in Tests**: Test specifications contain no runners, no sync loops, and no network or environment assumptions.
- **Pure Intent**: Tests specify:
  1. Setup statements (DDL / seeds).
  2. Sequential steps/mutations attributed to logical nodes (`NodeId(0)`, `NodeId(1)`, ...), supporting both expected successes and expected errors.
  3. Expectations evaluated **strictly against the final converged state**.
- **Portability**: The exact same `TestCase` or `TestSuite` data can be handed to any runner.

### 2. Strict Determinism & Expectations

- **Final Converged State Only**: Test expectations evaluate the state after all mutations and runner-driven synchronization have completed.
- **Explicit UUIDs for Entity Verification**: Tests checking UUID-keyed data must use static UUIDs. If an outcome does not depend on specific UUID values, verifying the result becomes part of the engine's canonical ordering validation.
- **Strict Row Equality**: Queries enforce strict row equality (`assert_eq!(actual, expected)`). Test authors must be explicit about ordering (e.g. using `ORDER BY`) to ensure deterministic verification.
- **SQL-Centric Integration Boundary**: All mutations and queries in integration test cases execute via SQL. Low-level internal engine primitives (e.g. manual conflict inspection or envelope byte hacking) remain dedicated unit tests.

### 3. Target Backend: Automerge / Redb Exclusively

- These integration test suites target **`automerge/redb` exclusively** (`RedbKernel` + `AutomergeRowCodec`).
- Neither `in-memory` nor `direct/redb` are used for these integration suites:
  - Eliminating `in-memory` ensures real disk I/O, transaction locks, and true persistence boundaries are always exercised.
  - Eliminating `direct/redb` focuses integration testing strictly on the primary CRDT-reconciled, Automerge-backed production architecture.

### 4. CI/CD & Local Execution Tiers

To ensure developer velocity while maintaining distributed verification:

- **Default / Local (`cargo test`)**: Runs `SingleNodeRunner` against `automerge/redb` for rapid turnaround on SQL syntax, planner logic, and basic execution.
- **Cluster CI/CD**: Runs `ClusterOfflineRunner` and `ClusterRealtimeRunner` against `automerge/redb` to verify multi-node partition reconciliation and realtime replication.
- **Chaos CI/CD**: Runs `ChaosRunner` against `automerge/redb` as an independent job to stress eventual consistency under dropped frames, message delays, and dynamic partitions.

---

## Design & API Specification

```
 ┌─────────────────────────────────────────────────────────────┐
 │                Test Specification (Pure Data)               │
 │                                                             │
 │   TestCase {                                                │
 │       name: "concurrent_unique_inserts",                    │
 │       setup: ["CREATE TABLE users ..."],                    │
 │       steps: [                                              │
 │           Step::sql(0, "INSERT INTO users ... 'Ada'"),      │
 │           Step::sql(1, "INSERT INTO users ... 'Lin'"),      │
 │           Step::failing(0, "INSERT ... DUPLICATE", Error),  │
 │       ],                                                    │
 │       expectations: [                                       │
 │           Expectation::all("SELECT * ...", [Ada, Lin]),     │
 │       ]                                                     │
 │   }                                                         │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                 Evaluated as pure input by Runners
                     (Automerge/Redb Backend)
                                │
 ┌──────────────────────────────┴──────────────────────────────┐
 │                     Runner Implementations                  │
 ├────────────────────┬──────────────────┬─────────────────────┤
 │ SingleNodeRunner   │ OfflineRunner    │ RealtimeRunner      │ ChaosRunner
 │ (Local Default)    │ (Cluster CI)     │ (Cluster CI)        │ (Chaos CI)
 │ - Collapses nodes  │ - Nodes mutate   │ - Writes broadcast  │ - Partitions
 │   into 1 engine      disconnected       immediately via       - Dropped frames
 │ - Fast SQL check   │ - Sync to end      mesh bus            │ - Eventual sync
 └────────────────────┴──────────────────┴─────────────────────┴──────────────────
```

### 1. Test IR (`crates/test/src/case.rs`)

```rust
use std::borrow::Cow;
use std::collections::BTreeSet;
use value::Row;

/// Logical node identifier within a test scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub usize);

impl From<usize> for NodeId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

/// Expected error category for negative test steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedError {
    ConstraintViolation,
    SyntaxError,
    TableNotFound,
    ColumnNotFound,
    TypeMismatch,
}

/// A pure data representation of a mutation or step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub node: NodeId,
    pub sql: Cow<'static, str>,
    pub expected_error: Option<ExpectedError>,
}

impl Step {
    pub fn sql(node: impl Into<NodeId>, sql: impl Into<Cow<'static, str>>) -> Self {
        Self {
            node: node.into(),
            sql: sql.into(),
            expected_error: None,
        }
    }

    pub fn failing(
        node: impl Into<NodeId>,
        sql: impl Into<Cow<'static, str>>,
        expected_error: ExpectedError,
    ) -> Self {
        Self {
            node: node.into(),
            sql: sql.into(),
            expected_error: Some(expected_error),
        }
    }
}

/// An expected result evaluated against the final converged state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expectation {
    pub query: Cow<'static, str>,
    pub expected_rows: Vec<Row>,
    /// When None, verified against all active nodes in the cluster.
    pub target_node: Option<NodeId>,
}

impl Expectation {
    pub fn all(query: impl Into<Cow<'static, str>>, expected_rows: Vec<Row>) -> Self {
        Self {
            query: query.into(),
            expected_rows,
            target_node: None,
        }
    }

    pub fn node(
        node: impl Into<NodeId>,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        Self {
            query: query.into(),
            expected_rows,
            target_node: Some(node.into()),
        }
    }
}

/// A complete, self-contained, runner-agnostic test scenario.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestCase {
    pub name: &'static str,
    pub setup: Vec<Cow<'static, str>>,
    pub steps: Vec<Step>,
    pub expectations: Vec<Expectation>,
}

impl TestCase {
    pub fn builder(name: &'static str) -> TestCaseBuilder {
        TestCaseBuilder::new(name)
    }

    pub fn required_nodes(&self) -> usize {
        let mut nodes = BTreeSet::new();
        nodes.insert(NodeId(0));
        for step in &self.steps {
            nodes.insert(step.node);
        }
        for exp in &self.expectations {
            if let Some(node) = exp.target_node {
                nodes.insert(node);
            }
        }
        nodes.len()
    }
}

#[derive(Clone, Debug, Default)]
pub struct TestSuite {
    pub name: &'static str,
    pub cases: Vec<TestCase>,
}

impl TestSuite {
    pub fn new(name: &'static str) -> Self {
        Self { name, cases: Vec::new() }
    }

    pub fn add(&mut self, case: TestCase) -> &mut Self {
        self.cases.push(case);
        self
    }
}
```

### 2. Fluent Builder API

```rust
pub struct TestCaseBuilder {
    name: &'static str,
    setup: Vec<Cow<'static, str>>,
    steps: Vec<Step>,
    expectations: Vec<Expectation>,
}

impl TestCaseBuilder {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            setup: Vec::new(),
            steps: Vec::new(),
            expectations: Vec::new(),
        }
    }

    pub fn setup<I, S>(mut self, statements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Cow<'static, str>>,
    {
        self.setup.extend(statements.into_iter().map(Into::into));
        self
    }

    pub fn step(mut self, node: impl Into<NodeId>, sql: impl Into<Cow<'static, str>>) -> Self {
        self.steps.push(Step::sql(node, sql));
        self
    }

    pub fn step_failing(
        mut self,
        node: impl Into<NodeId>,
        sql: impl Into<Cow<'static, str>>,
        expected_error: ExpectedError,
    ) -> Self {
        self.steps.push(Step::failing(node, sql, expected_error));
        self
    }

    pub fn step_root(self, sql: impl Into<Cow<'static, str>>) -> Self {
        self.step(0, sql)
    }

    pub fn expect_query(
        mut self,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        self.expectations.push(Expectation::all(query, expected_rows));
        self
    }

    pub fn expect_node_query(
        mut self,
        node: impl Into<NodeId>,
        query: impl Into<Cow<'static, str>>,
        expected_rows: Vec<Row>,
    ) -> Self {
        self.expectations.push(Expectation::node(node, query, expected_rows));
        self
    }

    pub fn build(self) -> TestCase {
        TestCase {
            name: self.name,
            setup: self.setup,
            steps: self.steps,
            expectations: self.expectations,
        }
    }
}
```

---

### 3. Runner Architecture & Verification Policies (`crates/test/src/runner/`)

Runners own execution and verification configurations:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerificationPolicy {
    pub io_correctness: bool,
    pub convergence: bool,
    pub state_consistency: bool,
}

impl Default for VerificationPolicy {
    fn default() -> Self {
        Self {
            io_correctness: true,
            convergence: true,
            state_consistency: true,
        }
    }
}

pub trait TestRunner {
    type Error: std::error::Error + Send + Sync + 'static;

    fn run_case(
        &self,
        case: &TestCase,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    async fn run_suite(&self, suite: &TestSuite) -> Result<(), Self::Error> {
        for case in &suite.cases {
            self.run_case(case).await?;
        }
        Ok(())
    }
}
```

#### Runner Implementations (Automerge/Redb Backend)

1. **`SingleNodeRunner` (Local Default)**
   - Serializes all steps on a single persistent `automerge/redb` engine instance.
   - Cleans up database directory on drop.
   - Verifies expected outcomes and expected errors locally.

2. **`ClusterOfflineRunner` (Cluster CI)**
   - Spins up $N$ distinct persistent `automerge/redb` nodes.
   - Distributes steps across isolated physical nodes.
   - Syncs all nodes to convergence (`sync_all`).
   - Asserts strict query equality (`assert_eq!`) across all converged nodes.
   - Asserts state convergence (`export_checkpoint`, causal frontier, envelope outcomes).

3. **`ClusterRealtimeRunner` (Cluster CI)**
   - Connects $N$ persistent `automerge/redb` nodes via active transport channels.
   - Propagates replication envelopes immediately after each commit.
   - Asserts final convergence and strict equality.

4. **`ChaosRunner` (Chaos CI)**
   - Connects $N$ persistent `automerge/redb` nodes with stochastic drop rates and dynamic partitions.
   - Executes steps under intermittent network failures.
   - Heals partitions and synchronizes until eventual consistency is verified.

---

## Test Authoring Example

A clean, declarative, SQL-only test case with deterministic UUIDs:

```rust
pub fn case_concurrent_user_inserts() -> TestCase {
    TestCase::builder("concurrent_user_inserts")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        // Logical Node 0 inserts Ada with explicit UUID
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        // Logical Node 1 inserts Lin with explicit UUID
        .step(
            1,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        )
        // Negative step check: duplicate primary key rejects
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada Duplicate')",
            ExpectedError::ConstraintViolation,
        )
        // Final expectation: strict row ordering on all converged nodes
        .expect_query(
            "SELECT name FROM users ORDER BY name",
            vec![
                Row::from(["Ada"]),
                Row::from(["Lin"]),
            ],
        )
        .build()
}
```

---

## Test Organization & Cargo Configuration

Tests are organized into suites executed across CI workflows:

```
tests/
  ├── local_sql.rs      # Runs SingleNodeRunner with automerge/redb (default `cargo test`)
  ├── cluster_sync.rs   # Runs ClusterOffline & Realtime with automerge/redb (Cluster CI job)
  └── cluster_chaos.rs  # Runs ChaosRunner with automerge/redb (Chaos CI job, marked #[ignore])
```

- **Local Development**:
  ```bash
  cargo test
  ```
- **Cluster CI**:
  ```bash
  cargo test --test cluster_sync
  ```
- **Chaos CI**:
  ```bash
  cargo test --test cluster_chaos -- --ignored
  ```
