#![cfg(all(feature = "automerge", feature = "redb"))]

use db::{Row, SessionConfig, Uuid, Value};
use db_test::{automerge_redb_cluster, run};

const CREATE_PEOPLE: &str = "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, city TEXT)";
const INSERT_ADA: &str =
    "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', NULL)";
const SELECT_PEOPLE: &str = "SELECT id, name, city FROM people";

fn key() -> Row {
    Row::new(vec![Value::Uuid(
        Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap(),
    )])
}

#[test]
fn persistent_automerge_replicas_merge_different_columns() {
    run(async {
        let (_cleanup, cluster) = automerge_redb_cluster(2);
        let config = SessionConfig::new();
        cluster.exec(0, CREATE_PEOPLE).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.exec(0, "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        cluster.exec(1, "UPDATE people SET city = 'Paris' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        cluster.sync_all(&config).await.unwrap();
        cluster.assert_converged(SELECT_PEOPLE).await;
    });
}

#[test]
fn same_column_conflicts_resolve_and_replicate() {
    run(async {
        let (_cleanup, cluster) = automerge_redb_cluster(2);
        let config = SessionConfig::new();
        cluster.exec(0, CREATE_PEOPLE).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.exec(0, "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        cluster.exec(1, "UPDATE people SET name = 'Linus' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        cluster.sync_all(&config).await.unwrap();
        assert_eq!(
            cluster.row_conflicts(0, "people", &key()).await.unwrap(),
            vec!["name"]
        );
        cluster
            .resolve_row(
                0,
                "people",
                &key(),
                vec![("name".into(), Value::from("Margaret"))],
            )
            .await
            .unwrap();
        cluster.sync_all(&config).await.unwrap();
        assert!(
            cluster
                .row_conflicts(0, "people", &key())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            cluster
                .row_conflicts(1, "people", &key())
                .await
                .unwrap()
                .is_empty()
        );
        cluster.assert_converged(SELECT_PEOPLE).await;
    });
}

#[test]
fn row_tombstone_supersedes_delayed_update() {
    run(async {
        let (_cleanup, cluster) = automerge_redb_cluster(2);
        let config = SessionConfig::new();
        cluster.exec(0, CREATE_PEOPLE).await;
        cluster.exec(0, INSERT_ADA).await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.exec(0, "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        let update = cluster
            .export_missing_envelopes(0, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        cluster.exec(1, "DELETE FROM people WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)").await;
        assert!(matches!(
            cluster.import_envelope(1, update).await.unwrap(),
            db::EnvelopeOutcome::Superseded
        ));
        cluster.sync_all(&config).await.unwrap();
        cluster.assert_converged(SELECT_PEOPLE).await;
    });
}
