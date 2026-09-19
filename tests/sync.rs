use db::{EnvelopeOutcome, SessionConfig};
use db_test::{
    ChaosScenario, ChaosStep, TransportDirection, direct_in_memory_cluster, direct_redb_cluster,
    run, run_chaos,
};

const CREATE_USERS: &str = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)";
const SELECT_USERS: &str = "SELECT * FROM users";
const INSERT_ADA: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')";
const INSERT_LIN: &str =
    "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')";

#[test]
fn bootstrap_and_noop_repair_converge() {
    run(async {
        for checkpoint_threshold in [None, Some(0)] {
            let cluster = direct_in_memory_cluster(2);
            let mut config = SessionConfig::new();
            config.checkpoint_threshold = checkpoint_threshold;
            cluster.exec(0, CREATE_USERS).await;
            cluster.exec(0, INSERT_ADA).await;
            cluster.sync(0, 1, &config).await.unwrap();
            cluster.sync(0, 1, &config).await.unwrap();
            cluster.assert_converged(SELECT_USERS).await;
            cluster.assert_state_converged().await;
        }
    });
}

#[test]
fn offline_writes_and_ordered_three_replica_repair_converge() {
    run(async {
        let cluster = direct_in_memory_cluster(3);
        let config = SessionConfig::new();
        cluster.exec(0, CREATE_USERS).await;
        cluster
            .sync_pairs(&[(0, 1), (1, 2)], &config)
            .await
            .unwrap();
        cluster.exec(0, INSERT_ADA).await;
        cluster.exec(1, INSERT_LIN).await;
        cluster.exec(2, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Mia')").await;
        cluster
            .sync_pairs(&[(2, 0), (1, 2), (0, 1), (2, 1)], &config)
            .await
            .unwrap();
        cluster.assert_converged(SELECT_USERS).await;
        cluster.assert_state_converged().await;
    });
}

#[test]
fn persistent_direct_replicas_sync() {
    run(async {
        let (_cleanup, cluster) = direct_redb_cluster(2);
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &SessionConfig::new()).await.unwrap();
        cluster.assert_converged(SELECT_USERS).await;
    });
}

#[test]
fn relay_survives_an_unavailable_source() {
    run(async {
        let cluster = direct_in_memory_cluster(3);
        let config = SessionConfig::new();
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.sync(1, 2, &config).await.unwrap();
        cluster.assert_state_converged().await;
    });
}

#[test]
fn reversed_duplicate_and_malformed_envelopes_are_safe() {
    run(async {
        let cluster = direct_in_memory_cluster(2);
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        let mut envelopes = cluster.export_envelopes(0).await.unwrap();
        let child_index = envelopes
            .iter()
            .position(|envelope| {
                envelopes
                    .iter()
                    .any(|candidate| envelope.parents.contains(&candidate.id))
            })
            .unwrap();
        let child = envelopes.remove(child_index);
        let parent = envelopes.remove(0);
        assert_eq!(
            cluster.import_envelope(1, child.clone()).await.unwrap(),
            EnvelopeOutcome::Pending
        );
        assert_eq!(
            cluster.import_envelope(1, parent).await.unwrap(),
            EnvelopeOutcome::Applied
        );
        assert_eq!(
            cluster.import_envelope(1, child).await.unwrap(),
            EnvelopeOutcome::Applied
        );
        cluster.assert_state_converged().await;
        assert!(matches!(
            cluster.import_envelope_bytes(1, vec![0]).await.unwrap(),
            EnvelopeOutcome::Quarantined { .. }
        ));
    });
}

#[test]
fn every_directional_frame_failure_retries() {
    run(async {
        for (direction, frames) in [
            (TransportDirection::LeftToRight, 0..6),
            (TransportDirection::RightToLeft, 0..5),
        ] {
            for frame in frames {
                let cluster = direct_in_memory_cluster(2);
                let config = SessionConfig::new();
                cluster.exec(0, CREATE_USERS).await;
                cluster.exec(0, INSERT_ADA).await;
                assert!(
                    cluster
                        .sync_interrupted_in(0, 1, &config, direction, frame)
                        .await
                        .is_err()
                );
                cluster.sync(0, 1, &config).await.unwrap();
                cluster.assert_state_converged().await;
            }
        }
    });
}

#[test]
fn seeded_partition_history_reproduces_from_its_seed() {
    run(async {
        let scenario = ChaosScenario {
            name: "two_replica_partition_repair",
            nodes: 2,
            seed: 42,
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
                ChaosStep::Exec {
                    node: 1,
                    sql: INSERT_LIN,
                },
                ChaosStep::CurrentlyDiverged {
                    a: 0,
                    b: 1,
                    query: SELECT_USERS,
                },
                ChaosStep::Heal,
                ChaosStep::DropRate(0.5),
                ChaosStep::EventuallyConsistent {
                    query: SELECT_USERS,
                    max_rounds: 16,
                },
            ],
        };
        run_chaos(direct_in_memory_cluster(2), scenario, &SessionConfig::new()).await;
    });
}

#[test]
fn checkpoint_merges_destination_offline_writes() {
    run(async {
        let cluster = direct_in_memory_cluster(2);
        let mut config = SessionConfig::new();
        config.checkpoint_threshold = Some(0);
        cluster.exec(0, CREATE_USERS).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster
            .exec(1, "CREATE TABLE notes (id UUID PRIMARY KEY, text TEXT)")
            .await;
        cluster.exec(1, "INSERT INTO notes VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'offline')").await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.sync(1, 0, &config).await.unwrap();
        cluster.assert_state_converged().await;
        cluster.assert_converged(SELECT_USERS).await;
        cluster.assert_converged("SELECT * FROM notes").await;
    });
}
