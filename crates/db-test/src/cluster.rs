use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use db_engine::{
    Checkpoint, DirectRowCodec, Engine, EngineResult, EnvelopeOutcome, Frontier, Kernel, RowCodec,
    TransactionEnvelope,
};
use db_engine_automerge::AutomergeRowCodec;
use db_engine_redb::RedbKernel;
use db_sql_translator::SqlTranslator;
use db_sync::{SessionConfig, SyncError, SyncRole, synchronize};
use db_value::Row;
use futures::join;

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

use crate::transport::{
    InMemoryTransportError, TransportDirection, in_memory_transport_pair,
    in_memory_transport_pair_failing,
};

pub struct Node<K: Kernel, R: RowCodec<K::Transaction>> {
    pub id: usize,
    pub engine: Engine<K, R>,
}

pub struct Cluster<K: Kernel, R: RowCodec<K::Transaction>> {
    nodes: Vec<Node<K, R>>,
}

pub struct RedbClusterCleanup(PathBuf);

pub fn direct_in_memory_cluster(n: usize) -> Cluster<db_engine::InMemoryKernel, DirectRowCodec> {
    Cluster::new(n, db_engine::InMemoryKernel::new, || DirectRowCodec)
}

impl Drop for RedbClusterCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn direct_redb_cluster(n: usize) -> (RedbClusterCleanup, Cluster<RedbKernel, DirectRowCodec>) {
    redb_cluster(n, || DirectRowCodec)
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
    R: RowCodec<db_engine_redb::RedbKernelTransaction>,
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
        let path = std::env::temp_dir().join(format!("db-test-{}-{id}", std::process::id()));
        if std::fs::create_dir(&path).is_ok() {
            return path;
        }
    }
}

impl<K: Kernel, R: RowCodec<K::Transaction>> Cluster<K, R> {
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

    pub async fn exec(&self, node_id: usize, sql: &str) -> Vec<Row> {
        self.try_exec(node_id, sql).await.unwrap()
    }

    pub async fn try_exec(&self, node_id: usize, sql: &str) -> EngineResult<Vec<Row>> {
        Ok(self.nodes[node_id]
            .engine
            .translate_and_execute(sql, &SqlTranslator)
            .await?
            .pop()
            .unwrap()
            .rows)
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
        for &(left, right) in pairs {
            self.sync(left, right, config).await?;
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

    pub async fn export_envelopes(&self, node_id: usize) -> EngineResult<Vec<TransactionEnvelope>> {
        self.nodes[node_id]
            .engine
            .missing_envelopes(&Frontier::default())
            .await
    }

    pub async fn export_missing_envelopes(
        &self,
        source: usize,
        destination: usize,
    ) -> EngineResult<Vec<TransactionEnvelope>> {
        let frontier = self.nodes[destination].engine.frontier().await?;
        self.nodes[source].engine.missing_envelopes(&frontier).await
    }

    pub async fn import_envelope(
        &self,
        node_id: usize,
        envelope: TransactionEnvelope,
    ) -> EngineResult<EnvelopeOutcome> {
        self.nodes[node_id].engine.import_envelope(envelope).await
    }

    pub async fn import_envelope_bytes(
        &self,
        node_id: usize,
        bytes: Vec<u8>,
    ) -> EngineResult<EnvelopeOutcome> {
        self.nodes[node_id]
            .engine
            .import_envelope_bytes(bytes)
            .await
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
        values: Vec<(String, db_value::Value)>,
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
        let first_checkpoint: Checkpoint = self.nodes[0].engine.export_checkpoint().await.unwrap();
        let first_outcomes = self.nodes[0].engine.envelope_outcomes().await.unwrap();
        for (node, current) in self.nodes.iter().enumerate().skip(1) {
            assert_eq!(
                first_checkpoint,
                current.engine.export_checkpoint().await.unwrap(),
                "nodes 0 and {node} diverged in checkpoint state"
            );
            assert_eq!(
                first_outcomes,
                current.engine.envelope_outcomes().await.unwrap(),
                "nodes 0 and {node} diverged in envelope outcomes"
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
