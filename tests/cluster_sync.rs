mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use converge_test::{ClusterOfflineRunner, ClusterRealtimeRunner, TestRunner, run};

#[test]
fn test_cluster_offline_suite() {
    run(async {
        let runner = ClusterOfflineRunner::default();
        let suite = standard_suite();
        runner.run_suite(&suite).await.unwrap();
    });
}

#[test]
fn test_cluster_offline_concurrent_inserts_case() {
    run(async {
        let runner = ClusterOfflineRunner::default();
        let case = case_concurrent_user_inserts();
        runner.run_case(&case).await.unwrap();
    });
}

#[test]
fn test_cluster_realtime_suite() {
    run(async {
        let runner = ClusterRealtimeRunner::default();
        let suite = standard_suite();
        runner.run_suite(&suite).await.unwrap();
    });
}

#[test]
fn test_cluster_realtime_concurrent_inserts_case() {
    run(async {
        let runner = ClusterRealtimeRunner::default();
        let case = case_concurrent_user_inserts();
        runner.run_case(&case).await.unwrap();
    });
}
