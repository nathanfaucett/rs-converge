use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine::Engine;
use engine_automerge::AutomergeRowCodec;
use engine_redb::RedbKernel;
use sql_translator::SqlTranslator;

use crate::{
    case::TestCase,
    runner::{RunnerError, TestRunner, VerificationPolicy},
};

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

fn database_directory() -> PathBuf {
    loop {
        let id = DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("test-single-{}-{id}", std::process::id()));
        if std::fs::create_dir(&path).is_ok() {
            return path;
        }
    }
}

struct CleanupDir(PathBuf);

impl Drop for CleanupDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Executes test cases on a single persistent `automerge/redb` node, collapsing
/// multi-node step assignments into a sequential local execution stream.
#[derive(Clone, Copy, Debug, Default)]
pub struct SingleNodeRunner {
    pub policy: VerificationPolicy,
}

impl SingleNodeRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: VerificationPolicy) -> Self {
        Self { policy }
    }
}

impl TestRunner for SingleNodeRunner {
    type Error = RunnerError;

    async fn run_case(&self, case: &TestCase) -> Result<(), Self::Error> {
        let dir = database_directory();
        let cleanup = CleanupDir(dir.clone());
        let db_path = dir.join("0.redb");
        let kernel = RedbKernel::new(Arc::new(
            redb::Database::create(&db_path).map_err(engine::EngineError::custom)?,
        ));
        let engine = Engine::new(kernel, AutomergeRowCodec::new());

        // Setup
        for stmt in &case.setup {
            engine
                .translate_and_execute(stmt.as_ref(), &SqlTranslator)
                .await
                .map_err(RunnerError::Engine)?;
        }

        // Steps
        for step in &case.steps {
            let result = engine
                .translate_and_execute(step.sql.as_ref(), &SqlTranslator)
                .await;
            match (&step.expected_error, result) {
                (None, Ok(_)) => {}
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

        // Expectations
        if self.policy.io_correctness {
            for exp in &case.expectations {
                let mut results = engine
                    .translate_and_execute(exp.query.as_ref(), &SqlTranslator)
                    .await
                    .map_err(RunnerError::Engine)?;
                let actual_rows = results.pop().map(|r| r.rows).unwrap_or_default();
                if actual_rows != exp.expected_rows {
                    return Err(RunnerError::ExpectationFailed {
                        node: exp.target_node,
                        query: exp.query.clone(),
                        expected: exp.expected_rows.clone(),
                        actual: actual_rows,
                    });
                }
            }
        }

        drop(engine);
        drop(cleanup);
        Ok(())
    }
}
