#![cfg(all(feature = "automerge", feature = "redb", feature = "sync"))]

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use converge::{
    AutomergeRowCodec, Engine, RedbKernel, SessionConfig, SqlTranslator, SyncRole, Value, redb,
    synchronize,
};
use test::{in_memory_transport_pair, run};

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

fn database_path() -> PathBuf {
    let id = DATABASE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("db-root-sync-{}-{id}.redb", std::process::id()))
}

fn replica(path: &Path) -> Engine<RedbKernel, AutomergeRowCodec> {
    Engine::new(
        RedbKernel::new(Arc::new(reconverge::Database::create(path).unwrap())),
        AutomergeRowCodec::new(),
    )
}

async fn execute(engine: &Engine<RedbKernel, AutomergeRowCodec>, sql: &str) -> Vec<converge::Row> {
    engine
        .translate_and_execute(sql, &SqlTranslator)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .rows
}

async fn sync(
    left: &Engine<RedbKernel, AutomergeRowCodec>,
    right: &Engine<RedbKernel, AutomergeRowCodec>,
    config: &SessionConfig,
) {
    let (mut left_transport, mut right_transport) = in_memory_transport_pair();
    let (left_result, right_result) = futures::join!(
        synchronize(left, &mut left_transport, config, SyncRole::Initiator),
        synchronize(right, &mut right_transport, config, SyncRole::Responder),
    );
    left_result.unwrap();
    right_result.unwrap();
}

#[test]
fn durable_automerge_engines_bootstrap_an_empty_peer_from_a_checkpoint() {
    run(async {
        let source_path = database_path();
        let destination_path = database_path();
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        let mut config = SessionConfig::new();
        config.checkpoint_threshold = Some(0);

        execute(
            &source,
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, city TEXT)",
        )
        .await;
        execute(
            &source,
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', NULL)",
        )
        .await;
        sync(&source, &destination, &config).await;

        assert_eq!(
            execute(&destination, "SELECT id, name, city FROM people").await,
            execute(&source, "SELECT id, name, city FROM people").await
        );
        assert_eq!(
            source.frontier().await.unwrap(),
            destination.frontier().await.unwrap()
        );
        drop((source, destination));
        std::fs::remove_file(source_path).unwrap();
        std::fs::remove_file(destination_path).unwrap();
    });
}

#[test]
fn durable_automerge_engines_converge_offline_writes_after_sync() {
    run(async {
        let left_path = database_path();
        let right_path = database_path();
        let left = replica(&left_path);
        let right = replica(&right_path);
        let config = SessionConfig::new();

        execute(
            &left,
            "CREATE TABLE people (id UUID PRIMARY KEY, name TEXT, city TEXT)",
        )
        .await;
        execute(
            &left,
            "INSERT INTO people VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', NULL)",
        )
        .await;
        sync(&left, &right, &config).await;
        execute(
            &left,
            "UPDATE people SET name = 'Grace' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
        )
        .await;
        execute(
            &right,
            "UPDATE people SET city = 'Paris' WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
        )
        .await;
        sync(&left, &right, &config).await;

        let expected = vec![converge::Row::new(vec![
            Value::Uuid(converge::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap()),
            Value::from("Grace"),
            Value::from("Paris"),
        ])];
        assert_eq!(
            execute(&left, "SELECT id, name, city FROM people").await,
            expected
        );
        assert_eq!(
            execute(&right, "SELECT id, name, city FROM people").await,
            expected
        );
        assert_eq!(
            left.frontier().await.unwrap(),
            right.frontier().await.unwrap()
        );
        drop((left, right));
        std::fs::remove_file(left_path).unwrap();
        std::fs::remove_file(right_path).unwrap();
    });
}
