mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb_test::{ChaosRunner, TestRunner, run};

#[test]
#[ignore = "chaos testing is intended for chaos CI workflow"]
fn test_cluster_chaos_suite() {
    run(async {
        let runner = ChaosRunner::default();
        let suite = standard_suite();
        runner.run_suite(&suite).await.unwrap();
    });
}

#[test]
#[ignore = "chaos testing is intended for chaos CI workflow"]
fn test_cluster_chaos_concurrent_inserts_case() {
    run(async {
        let runner = ChaosRunner::default();
        let case = case_concurrent_user_inserts();
        runner.run_case(&case).await.unwrap();
    });
}
