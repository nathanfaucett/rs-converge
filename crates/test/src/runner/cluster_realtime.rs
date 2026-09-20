use sync::SessionConfig;

use crate::{
    case::{NodeId, TestCase},
    cluster::automerge_redb_cluster,
    runner::{RunnerError, TestRunner, VerificationPolicy},
};

/// Orchestrates multi-node cluster integration tests in realtime mesh replication mode:
/// replication envelopes are broadcast across nodes immediately after each mutation.
#[derive(Clone, Debug)]
pub struct ClusterRealtimeRunner {
    pub policy: VerificationPolicy,
    pub session_config: SessionConfig,
    pub max_sync_rounds: usize,
}

impl Default for ClusterRealtimeRunner {
    fn default() -> Self {
        Self {
            policy: VerificationPolicy::new(true, true, false),
            session_config: SessionConfig::default(),
            max_sync_rounds: 10,
        }
    }
}

impl ClusterRealtimeRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: VerificationPolicy) -> Self {
        Self {
            policy,
            ..Default::default()
        }
    }

    pub fn with_session_config(mut self, config: SessionConfig) -> Self {
        self.session_config = config;
        self
    }
}

#[async_trait::async_trait]
impl TestRunner for ClusterRealtimeRunner {
    type Error = RunnerError;

    async fn run_case(&self, case: &TestCase) -> Result<(), Self::Error> {
        let n = case.required_nodes().max(2);
        let (cleanup, cluster) = automerge_redb_cluster(n);

        // Setup on node 0
        for stmt in &case.setup {
            cluster
                .try_exec(0, stmt.as_ref())
                .await
                .map_err(RunnerError::Engine)?;
        }
        if !case.setup.is_empty() {
            cluster
                .sync_all(&self.session_config)
                .await
                .map_err(|e| RunnerError::Sync(e.to_string()))?;
        }

        // Realtime steps: each successful mutation immediately propagates to peers
        for step in &case.steps {
            let result = cluster.try_exec(step.node.0, step.sql.as_ref()).await;
            match (&step.expected_error, result) {
                (None, Ok(_)) => {
                    for other in 0..n {
                        if other != step.node.0 {
                            cluster
                                .sync(step.node.0, other, &self.session_config)
                                .await
                                .map_err(|e| RunnerError::Sync(e.to_string()))?;
                        }
                    }
                }
                (None, Err(err)) => {
                    return Err(RunnerError::UnexpectedError {
                        node: step.node,
                        expected: None,
                        actual: err.to_string(),
                    });
                }
                (Some(expected), Ok(_)) => {
                    return Err(RunnerError::ExpectedErrorDidNotOccur {
                        node: step.node,
                        expected: *expected,
                    });
                }
                (Some(expected), Err(err)) => {
                    if !expected.matches(&err) {
                        return Err(RunnerError::UnexpectedError {
                            node: step.node,
                            expected: Some(*expected),
                            actual: err.to_string(),
                        });
                    }
                }
            }
        }

        // Ensure complete convergence across the cluster
        cluster
            .sync_until_converged(&self.session_config, self.max_sync_rounds)
            .await
            .map_err(|e| RunnerError::Sync(e.to_string()))?;

        // Convergence verification across all nodes
        if self.policy.convergence {
            for exp in &case.expectations {
                if exp.target_node.is_none() {
                    let first_rows = cluster
                        .try_exec(0, exp.query.as_ref())
                        .await
                        .map_err(RunnerError::Engine)?;
                    for node in 1..n {
                        let node_rows = cluster
                            .try_exec(node, exp.query.as_ref())
                            .await
                            .map_err(RunnerError::Engine)?;
                        if node_rows != first_rows {
                            return Err(RunnerError::ConvergenceFailure {
                                query: exp.query.clone(),
                                diverged_node: node,
                                rows_node_0: first_rows,
                                rows_other: node_rows,
                            });
                        }
                    }
                }
            }
        }

        // State consistency verification
        if self.policy.state_consistency {
            let first_frontier = cluster
                .engine(0)
                .frontier()
                .await
                .map_err(RunnerError::Engine)?;
            let first_checkpoint = cluster
                .engine(0)
                .export_checkpoint()
                .await
                .map_err(RunnerError::Engine)?;
            let first_outcomes = cluster
                .engine(0)
                .envelope_outcomes()
                .await
                .map_err(RunnerError::Engine)?;

            for node in 1..n {
                let node_frontier = cluster
                    .engine(node)
                    .frontier()
                    .await
                    .map_err(RunnerError::Engine)?;
                if node_frontier != first_frontier {
                    return Err(RunnerError::StateConvergenceFailure(format!(
                        "nodes 0 and {node} diverged in causal frontier"
                    )));
                }

                let node_checkpoint = cluster
                    .engine(node)
                    .export_checkpoint()
                    .await
                    .map_err(RunnerError::Engine)?;
                if node_checkpoint != first_checkpoint {
                    return Err(RunnerError::StateConvergenceFailure(format!(
                        "nodes 0 and {node} diverged in checkpoint state"
                    )));
                }

                let node_outcomes = cluster
                    .engine(node)
                    .envelope_outcomes()
                    .await
                    .map_err(RunnerError::Engine)?;
                if node_outcomes != first_outcomes {
                    return Err(RunnerError::StateConvergenceFailure(format!(
                        "nodes 0 and {node} diverged in envelope outcomes"
                    )));
                }
            }
        }

        // IO correctness verification
        if self.policy.io_correctness {
            for exp in &case.expectations {
                let target_nodes: Vec<usize> = match exp.target_node {
                    Some(node) => vec![node.0],
                    None => (0..n).collect(),
                };
                for node in target_nodes {
                    let actual_rows = cluster
                        .try_exec(node, exp.query.as_ref())
                        .await
                        .map_err(RunnerError::Engine)?;
                    if actual_rows != exp.expected_rows {
                        return Err(RunnerError::ExpectationFailed {
                            node: Some(NodeId(node)),
                            query: exp.query.clone(),
                            expected: exp.expected_rows.clone(),
                            actual: actual_rows,
                        });
                    }
                }
            }
        }

        drop(cluster);
        drop(cleanup);
        Ok(())
    }
}
