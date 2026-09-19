use db_engine::{DirectRowCodec, InMemoryKernel};
use db_sync::SessionConfig;
use db_test::{ChaosScenario, ChaosStep, Cluster, run_chaos};

const CREATE_USERS: &str = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)";
const SELECT_USERS: &str = "SELECT * FROM users";
const INSERT_ADA: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')";
const INSERT_LIN: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')";

fn cluster(nodes: usize) -> Cluster<InMemoryKernel, DirectRowCodec> {
    Cluster::new(nodes, InMemoryKernel::new, || DirectRowCodec)
}

fn config() -> SessionConfig {
    SessionConfig::new()
}

#[tokio::test]
async fn empty_replica_bootstraps() {
    let cluster = cluster(2);
    let _ = cluster.exec(0, CREATE_USERS).await;
    let _ = cluster.exec(0, INSERT_ADA).await;

    cluster.sync(0, 1, &config()).await.unwrap();

    cluster.assert_converged(SELECT_USERS).await;
}

#[tokio::test]
async fn concurrent_offline_writes_converge() {
    let cluster = cluster(2);
    let _ = cluster.exec(0, CREATE_USERS).await;
    cluster.sync(0, 1, &config()).await.unwrap();
    let _ = cluster.exec(0, INSERT_ADA).await;
    let _ = cluster.exec(1, INSERT_LIN).await;

    cluster.sync_all(&config()).await.unwrap();

    cluster.assert_converged(SELECT_USERS).await;
}

#[tokio::test]
async fn interrupted_session_retries() {
    let cluster = cluster(2);
    let _ = cluster.exec(0, CREATE_USERS).await;

    assert!(cluster.sync_interrupted(0, 1, &config(), 0).await.is_err());
    cluster.sync(0, 1, &config()).await.unwrap();

    cluster.assert_converged(SELECT_USERS).await;
}

#[tokio::test]
async fn partition_keeps_replicas_diverged() {
    let cluster = cluster(2);
    let config = config();

    run_chaos(
        cluster,
        ChaosScenario {
            name: "partition diverges",
            nodes: 2,
            seed: 1,
            steps: vec![
                ChaosStep::Exec {
                    node: 0,
                    sql: CREATE_USERS,
                },
                ChaosStep::Sync,
                ChaosStep::Partition {
                    groups: &[&[0], &[1]],
                },
                ChaosStep::Exec {
                    node: 0,
                    sql: INSERT_ADA,
                },
                ChaosStep::Sync,
                ChaosStep::CurrentlyDiverged {
                    a: 0,
                    b: 1,
                    query: SELECT_USERS,
                },
            ],
        },
        &config,
    )
    .await;
}

#[tokio::test]
async fn healing_retries_and_converges() {
    let cluster = cluster(2);
    let config = config();

    run_chaos(
        cluster,
        ChaosScenario {
            name: "heal converges",
            nodes: 2,
            seed: 1,
            steps: vec![
                ChaosStep::Exec {
                    node: 0,
                    sql: CREATE_USERS,
                },
                ChaosStep::Sync,
                ChaosStep::Partition {
                    groups: &[&[0], &[1]],
                },
                ChaosStep::Exec {
                    node: 0,
                    sql: INSERT_ADA,
                },
                ChaosStep::Sync,
                ChaosStep::Heal,
                ChaosStep::EventuallyConsistent {
                    query: SELECT_USERS,
                    max_rounds: 1,
                },
            ],
        },
        &config,
    )
    .await;
}
