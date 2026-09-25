//! Smoke test for the `test_suite!` / `test_case!` generation macros.
//! Lives in the root crate because the `test` package's lib name shadows
//! the libtest harness crate, which breaks `#[test]` inside that package.

use ofdb_test::{RunnerError, TestCase, TestRunner, TestSuite, test_case, test_suite};

#[derive(Debug)]
struct NoopRunner;

impl TestRunner for NoopRunner {
    type Error = RunnerError;

    async fn run_case(&self, _case: &TestCase) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn noop_case() -> TestCase {
    TestCase::builder("noop").build()
}

fn noop_suite() -> TestSuite {
    TestSuite::new("noop_suite").with_case(noop_case())
}

test_suite!(noop_suite_runs, noop_suite(), NoopRunner);

test_case!(noop_case_runs, noop_case(), NoopRunner);

test_suite!(
    #[ignore = "attribute passthrough"]
    noop_suite_ignored,
    noop_suite(),
    NoopRunner
);
