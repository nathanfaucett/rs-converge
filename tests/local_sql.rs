mod common;
#[path = "common/sql.rs"]
mod sql;

use common::{case_concurrent_user_inserts, standard_suite};
use converge_test::{SingleNodeRunner, TestRunner, run};
use sql::sql_suite;

#[test]
fn test_local_sql_suite() {
    run(async {
        let runner = SingleNodeRunner::default();
        runner.run_suite(&standard_suite()).await.unwrap();
        runner.run_suite(&sql_suite()).await.unwrap();
    });
}

#[test]
fn test_local_sql_concurrent_inserts_case() {
    run(async {
        let runner = SingleNodeRunner::default();
        let case = case_concurrent_user_inserts();
        runner.run_case(&case).await.unwrap();
    });
}
