use std::borrow::Cow;

use value::Row;

use crate::case::{ExpectedError, NodeId, TestCase, TestSuite};

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
            convergence: false,
            state_consistency: false,
        }
    }
}

impl VerificationPolicy {
    pub const fn new(io_correctness: bool, convergence: bool, state_consistency: bool) -> Self {
        Self {
            io_correctness,
            convergence,
            state_consistency,
        }
    }

    pub const fn strict() -> Self {
        Self {
            io_correctness: true,
            convergence: true,
            state_consistency: true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("Engine error: {0}")]
    Engine(#[from] engine::EngineError),
    #[error("Sync error: {0}")]
    Sync(String),
    #[error("Step on node {node:?} failed: expected {expected:?}, but execution succeeded")]
    ExpectedErrorDidNotOccur {
        node: NodeId,
        expected: ExpectedError,
    },
    #[error(
        "Step on node {node:?} failed with unexpected error: expected {expected:?}, got {actual}"
    )]
    UnexpectedError {
        node: NodeId,
        expected: Option<ExpectedError>,
        actual: String,
    },
    #[error(
        "Expectation failed for query '{query}' on node {node:?}:\n  expected: {expected:?}\n  actual:   {actual:?}"
    )]
    ExpectationFailed {
        node: Option<NodeId>,
        query: Cow<'static, str>,
        expected: Vec<Row>,
        actual: Vec<Row>,
    },
    #[error(
        "Convergence failure on query '{query}' between node 0 and node {diverged_node}:\n  node 0: {rows_node_0:?}\n  node {diverged_node}: {rows_other:?}"
    )]
    ConvergenceFailure {
        query: Cow<'static, str>,
        diverged_node: usize,
        rows_node_0: Vec<Row>,
        rows_other: Vec<Row>,
    },
    #[error("State convergence failure: {0}")]
    StateConvergenceFailure(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Chaos failure: {0}")]
    Chaos(String),
}

#[async_trait::async_trait]
pub trait TestRunner {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn run_case(&self, case: &TestCase) -> Result<(), Self::Error>;
    async fn run_suite(&self, suite: &TestSuite) -> Result<(), Self::Error> {
        for case in &suite.cases {
            self.run_case(case).await?;
        }
        Ok(())
    }
}

pub mod chaos;
pub mod cluster_offline;
pub mod cluster_realtime;
pub mod single;

pub use chaos::ChaosRunner;
pub use cluster_offline::ClusterOfflineRunner;
pub use cluster_realtime::ClusterRealtimeRunner;
pub use single::SingleNodeRunner;
