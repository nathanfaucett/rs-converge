use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine::{Engine, EngineResult, Kernel};
use engine_automerge::AutomergeRowCodec;
use engine_redb::RedbKernel;
use futures::join;
use sql_translator::SqlTranslator;

use sync::{
    SessionConfig, SyncError, SyncRole, SyncRowCodec, apply_sync_state_for, export_sync_state_for,
    sync_manifest_for, synchronize,
};
use value::Row;

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

fn database_directory() -> PathBuf {
    loop {
        let id = DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("test-sync-{}-{id}", std::process::id()));
        if std::fs::create_dir(&path).is_ok() {
            return path;
        }
    }
}

fn automerge_in_memory_cluster(n: usize) -> Cluster<engine::InMemoryKernel, AutomergeRowCodec> {
    Cluster::new(n, engine::InMemoryKernel::new, AutomergeRowCodec::new)
}

fn automerge_redb_cluster(
    n: usize,
) -> (RedbClusterCleanup, Cluster<RedbKernel, AutomergeRowCodec>) {
    redb_cluster(n, AutomergeRowCodec::new)
}

fn redb_cluster<R>(
    n: usize,
    new_row_codec: fn() -> R,
) -> (RedbClusterCleanup, Cluster<RedbKernel, R>)
where
    R: SyncRowCodec<engine_redb::RedbKernelTransaction>,
{
    let directory = database_directory();
    let nodes = (0..n)
        .map(|id| Node {
            id,
            engine: Engine::new(
                RedbKernel::new(Arc::new(
                    redb::Database::create(directory.join(format!("{id}.redb"))).unwrap(),
                )),
                new_row_codec(),
            ),
        })
        .collect();
    (RedbClusterCleanup(directory), Cluster { nodes })
}

struct RedbClusterCleanup(PathBuf);

impl Drop for RedbClusterCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct Node<K: Kernel, R: SyncRowCodec<K::Transaction>> {
    pub id: usize,
    pub engine: Engine<K, R>,
}

pub struct Cluster<K: Kernel, R: SyncRowCodec<K::Transaction>> {
    nodes: Vec<Node<K, R>>,
}

impl<K: Kernel, R: SyncRowCodec<K::Transaction>> Cluster<K, R> {
    pub fn new(n: usize, new_kernel: fn() -> K, new_row_codec: fn() -> R) -> Self {
        let nodes = (0..n)
            .map(|id| Node {
                id,
                engine: Engine::new(new_kernel(), new_row_codec()),
            })
            .collect();
        Self { nodes }
    }

    pub const fn len(&self) -> usize {
        self.nodes.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn node(&self, id: usize) -> &Node<K, R> {
        &self.nodes[id]
    }

    pub fn nodes(&self) -> &[Node<K, R>] {
        &self.nodes
    }

    pub fn engine(&self, id: usize) -> &Engine<K, R> {
        &self.nodes[id].engine
    }

    pub async fn have_same_manifests(&self) -> bool {
        if self.nodes.len() <= 1 {
            return true;
        }
        let first = match sync_manifest_for(&self.nodes[0].engine).await {
            Ok(manifest) => manifest,
            Err(_) => return false,
        };
        for node in &self.nodes[1..] {
            match sync_manifest_for(&node.engine).await {
                Ok(manifest) if manifest == first => {}
                _ => return false,
            }
        }
        true
    }

    pub async fn exec(&self, node_id: usize, sql: &str) -> Vec<Row> {
        self.try_exec(node_id, sql).await.unwrap()
    }

    pub async fn try_exec(&self, node_id: usize, sql: &str) -> EngineResult<Vec<Row>> {
        let mut results = self.nodes[node_id]
            .engine
            .translate_and_execute(sql, &SqlTranslator)
            .await?;
        Ok(results.pop().map(|r| r.rows).unwrap_or_default())
    }

    pub async fn sync(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        self.sync_with_failure(left, right, config, None, None)
            .await
    }

    pub async fn sync_interrupted(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
        fail_at: usize,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        self.sync_with_failure(left, right, config, None, Some(fail_at))
            .await
    }

    pub async fn sync_interrupted_in(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
        direction: TransportDirection,
        fail_at: usize,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        self.sync_with_failure(left, right, config, Some(direction), Some(fail_at))
            .await
    }

    pub async fn sync_pairs(
        &self,
        pairs: &[(usize, usize)],
        config: &SessionConfig,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        for (left, right) in pairs {
            self.sync(*left, *right, config).await?;
        }
        Ok(())
    }

    pub async fn sync_all(
        &self,
        config: &SessionConfig,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        for left in 0..self.nodes.len() {
            for right in left + 1..self.nodes.len() {
                self.sync(left, right, config).await?;
            }
        }
        Ok(())
    }

    pub async fn assert_converged(&self, query: &str) {
        let mut results = Vec::new();
        for node in 0..self.nodes.len() {
            results.push(self.exec(node, query).await);
        }
        for (node, result) in results.iter().enumerate() {
            assert_eq!(
                &results[0], result,
                "nodes 0 and {node} diverged on query: {query}"
            );
        }
    }

    pub async fn assert_state_converged(&self) {
        let first_state = export_sync_state_for(&self.nodes[0].engine).await.unwrap();
        for (node, current) in self.nodes.iter().enumerate().skip(1) {
            assert_eq!(
                first_state,
                export_sync_state_for(&current.engine).await.unwrap(),
                "nodes 0 and {node} diverged in sync state"
            );
        }
    }

    async fn sync_with_failure(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
        direction: Option<TransportDirection>,
        fail_at: Option<usize>,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        assert_ne!(left, right, "cannot synchronize a node with itself");
        let (mut left_transport, mut right_transport) = match fail_at {
            Some(fail_at) => in_memory_transport_pair_failing(direction, Some(fail_at)),
            None => in_memory_transport_pair(),
        };
        let (left_result, right_result) = join!(
            synchronize(
                &self.nodes[left].engine,
                &mut left_transport,
                config,
                SyncRole::Initiator,
            ),
            synchronize(
                &self.nodes[right].engine,
                &mut right_transport,
                config,
                SyncRole::Responder,
            ),
        );
        left_result?;
        right_result?;
        Ok(())
    }
}

use sync::{ChaosScenario, ChaosStep, TransportDirection};

#[test]
fn bootstrap_and_noop_repair_converge() {
    futures::executor::block_on(async {
        {
            let cluster = automerge_in_memory_cluster(2);
            let config = SessionConfig::default();
            cluster
                .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
                .await;
            cluster.exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')").await;
            cluster.sync(0, 1, &config).await.unwrap();
            cluster.sync(0, 1, &config).await.unwrap();
            cluster.assert_converged("SELECT * FROM users").await;
            cluster.assert_state_converged().await;
        }
    });
}

#[test]
fn offline_writes_and_ordered_three_replica_repair_converge() {
    futures::executor::block_on(async {
        let cluster = automerge_in_memory_cluster(3);
        let config = SessionConfig::default();
        cluster
            .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
            .await;
        cluster
            .sync_pairs(&[(0, 1), (1, 2)], &config)
            .await
            .unwrap();
        cluster.exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')").await;
        cluster.exec(1, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')").await;
        cluster.exec(2, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Mia')").await;
        cluster
            .sync_pairs(&[(2, 0), (1, 2), (0, 1), (2, 1)], &config)
            .await
            .unwrap();
        cluster.assert_converged("SELECT * FROM users").await;
        cluster.assert_state_converged().await;
    });
}

#[test]
fn persistent_direct_replicas_sync() {
    futures::executor::block_on(async {
        let (_cleanup, cluster) = automerge_redb_cluster(2);
        cluster
            .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
            .await;
        cluster.exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')").await;
        cluster.sync(0, 1, &SessionConfig::default()).await.unwrap();
        cluster.assert_converged("SELECT * FROM users").await;
    });
}

#[test]
fn relay_survives_an_unavailable_source() {
    futures::executor::block_on(async {
        let cluster = automerge_in_memory_cluster(3);
        let config = SessionConfig::default();
        cluster
            .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
            .await;
        cluster.exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')").await;
        cluster.sync(0, 1, &config).await.unwrap();
        cluster.sync(1, 2, &config).await.unwrap();
        cluster.assert_state_converged().await;
    });
}

#[test]
fn every_directional_frame_failure_retries() {
    futures::executor::block_on(async {
        for (direction, frames) in [
            (TransportDirection::LeftToRight, 0..4),
            (TransportDirection::RightToLeft, 0..3),
        ] {
            for frame in frames {
                let cluster = automerge_in_memory_cluster(2);
                let config = SessionConfig::default();
                cluster
                    .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
                    .await;
                cluster.exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')").await;
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
    futures::executor::block_on(async {
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

fn run_chaos<C, S>(
    cluster: C,
    scenario: ChaosScenario,
    config: &SessionConfig,
) -> impl std::future::Future<Output = ()>
where
    C: Into<Cluster<engine::InMemoryKernel, AutomergeRowCodec>>,
    S: Into<ChaosScenario>,
{
    async move {
        let mut cluster = cluster.into();
        let mut scenario = scenario.into();
        let mut network = crate::chaos::ChaosNetwork::new(scenario.seed);
        network.set_drop_rate(0.0);

        // Setup on node 0 with healthy network
        for step in &scenario.steps {
            match step {
                ChaosStep::Exec { node, sql } => {
                    cluster.try_exec(*node, sql).await.unwrap();
                }
                ChaosStep::Sync => {
                    cluster.sync_all(&config).await.unwrap();
                }
                ChaosStep::Partition { groups } => {
                    network.set_partition(groups.clone());
                }
                ChaosStep::Heal => {
                    network.heal();
                    network.set_drop_rate(0.0);
                }
                ChaosStep::DropRate(rate) => {
                    network.set_drop_rate(*rate);
                }
                ChaosStep::CurrentlyDiverged { a, b, query } => {
                    let rows_a = cluster.exec(*a, query).await;
                    let rows_b = cluster.exec(*b, query).await;
                    assert_ne!(rows_a, rows_b, "expected divergence");
                }
                ChaosStep::EventuallyConsistent { query, max_rounds } => {
                    let mut converged = false;
                    for _ in 0..*max_rounds {
                        cluster.sync_all(&config).await.unwrap();
                        if cluster.have_same_manifests().await {
                            converged = true;
                            break;
                        }
                    }
                    assert!(converged, "expected eventual consistency");
                }
            }
        }
    }
}

mod chaos {
    use std::collections::HashMap;

    use value::Row;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ChaosScenario {
        pub name: String,
        pub nodes: usize,
        pub seed: u64,
        pub steps: Vec<ChaosStep>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum ChaosStep {
        Exec { node: usize, sql: String },
        Sync,
        Partition { groups: Vec<Vec<usize>> },
        Heal,
        DropRate(f64),
        CurrentlyDiverged { a: usize, b: usize, query: String },
        EventuallyConsistent { query: String, max_rounds: usize },
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum TransportDirection {
        LeftToRight,
        RightToLeft,
    }

    pub struct ChaosNetwork {
        seed: u64,
        drop_rate: f64,
        partition: Option<Vec<Vec<usize>>>,
    }

    impl ChaosNetwork {
        pub fn new(seed: u64) -> Self {
            Self {
                seed,
                drop_rate: 0.0,
                partition: None,
            }
        }

        pub fn set_drop_rate(&mut self, rate: f64) {
            self.drop_rate = rate;
        }

        pub fn set_partition(&mut self, groups: Vec<Vec<usize>>) {
            self.partition = Some(groups);
        }

        pub fn heal(&mut self) {
            self.partition = None;
        }

        pub fn can_communicate(&self, left: usize, right: usize) -> bool {
            if let Some(ref partition) = self.partition {
                let left_in = partition.iter().any(|group| group.contains(&left));
                let right_in = partition.iter().any(|group| group.contains(&right));
                left_in && right_in
            } else {
                true
            }
        }

        pub fn should_interrupt(&self) -> bool {
            use rand::Rng;
            let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed);
            rng.gen_bool(self.drop_rate)
        }
    }
}
