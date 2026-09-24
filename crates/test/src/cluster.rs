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
    SessionConfig, SyncError, SyncRole, SyncRowCodec, export_sync_state_for, sync_manifest_for,
    synchronize,
};
use value::Row;

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

use crate::transport::{
    InMemoryTransportError, TransportDirection, in_memory_transport_pair,
    in_memory_transport_pair_failing,
};

pub struct Node<K: Kernel, R: SyncRowCodec<K::Transaction>> {
    pub id: usize,
    pub engine: Engine<K, R>,
}

pub struct Cluster<K: Kernel, R: SyncRowCodec<K::Transaction>> {
    nodes: Vec<Node<K, R>>,
}

pub struct RedbClusterCleanup(PathBuf);

pub fn automerge_in_memory_cluster(n: usize) -> Cluster<engine::InMemoryKernel, AutomergeRowCodec> {
    Cluster::new(n, engine::InMemoryKernel::new, AutomergeRowCodec::new)
}

impl Drop for RedbClusterCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn automerge_redb_cluster(
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

fn database_directory() -> PathBuf {
    loop {
        let id = DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("test-{}-{id}", std::process::id()));
        if std::fs::create_dir(&path).is_ok() {
            return path;
        }
    }
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

    pub async fn sync_until_converged(
        &self,
        config: &SessionConfig,
        max_rounds: usize,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        if self.nodes.len() <= 1 {
            return Ok(());
        }
        for _ in 0..max_rounds {
            self.sync_all(config).await?;
            if self.have_same_manifests().await {
                return Ok(());
            }
        }
        Ok(())
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

    pub async fn row_conflicts(
        &self,
        node_id: usize,
        table: &str,
        key: &Row,
    ) -> EngineResult<Vec<String>> {
        self.nodes[node_id].engine.row_conflicts(table, key).await
    }

    pub async fn resolve_row(
        &self,
        node_id: usize,
        table: &str,
        key: &Row,
        values: Vec<(String, value::Value)>,
    ) -> EngineResult<()> {
        self.nodes[node_id]
            .engine
            .resolve_row(table, key, values)
            .await
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

    pub async fn is_converged(&self, query: &str) -> bool {
        let first = self.exec(0, query).await;
        for node in 1..self.nodes.len() {
            if self.exec(node, query).await != first {
                return false;
            }
        }
        true
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
