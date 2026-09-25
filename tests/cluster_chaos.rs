mod common;

use common::{case_concurrent_user_inserts, standard_suite};
use ofdb::SessionConfig;
use ofdb_test::{
    ChaosRunner, ChaosScenario, ChaosStep, automerge_in_memory_cluster, run, run_chaos, test_case,
    test_suite,
};

test_suite!(
    #[ignore = "chaos testing is intended for chaos CI workflow"]
    standard_chaos,
    standard_suite(),
    ChaosRunner::default()
);

test_case!(
    #[ignore = "chaos testing is intended for chaos CI workflow"]
    concurrent_inserts_chaos,
    case_concurrent_user_inserts(),
    ChaosRunner::default()
);

#[test]
#[ignore = "chaos testing is intended for chaos CI workflow"]
fn seeded_partition_history_reproduces_from_its_seed() {
    run(async {
        let scenario = ChaosScenario {
            name: "two_replica_partition_repair",
            nodes: 2,
            seed: 42,
            steps: vec![
                ChaosStep::Exec {
                    node: 0,
                    sql: "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                },
                ChaosStep::Sync,
                ChaosStep::Partition {
                    groups: &[&[0], &[1]],
                },
                ChaosStep::Exec {
                    node: 0,
                    sql: "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
                },
                ChaosStep::Exec {
                    node: 1,
                    sql: "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
                },
                ChaosStep::CurrentlyDiverged {
                    a: 0,
                    b: 1,
                    query: "SELECT * FROM users",
                },
                ChaosStep::Heal,
                ChaosStep::DropRate(0.5),
                ChaosStep::EventuallyConsistent {
                    query: "SELECT * FROM users",
                    max_rounds: 16,
                },
            ],
        };
        run_chaos(
            automerge_in_memory_cluster(2),
            scenario,
            &SessionConfig::default(),
        )
        .await;
    });
}
