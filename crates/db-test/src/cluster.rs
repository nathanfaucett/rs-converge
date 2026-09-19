use db_engine::{Engine, Kernel, RowCodec};
use db_sql_translator::SqlTranslator;
use db_sync::{SessionConfig, SyncError, SyncRole, synchronize};
use db_value::Row;
use futures::join;

use crate::transport::{
    InMemoryTransportError, in_memory_transport_pair, in_memory_transport_pair_failing_at,
};

pub struct Node<K: Kernel, R: RowCodec<K::Transaction>> {
    pub id: usize,
    pub engine: Engine<K, R>,
}

pub struct Cluster<K: Kernel, R: RowCodec<K::Transaction>> {
    nodes: Vec<Node<K, R>>,
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

    pub fn node(&mut self, id: usize) -> &mut Node<K, R> {
        &mut self.nodes[id]
    }

    pub fn nodes(&mut self) -> &mut [Node<K, R>] {
        &mut self.nodes
    }

    pub async fn exec(&self, node_id: usize, sql: &str) -> Vec<Row> {
        self.nodes[node_id]
            .engine
            .translate_and_execute(sql, &SqlTranslator)
            .await
            .unwrap()
            .pop()
            .unwrap()
            .rows
    }

    pub async fn sync(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        self.sync_with_failure(left, right, config, None).await
    }

    pub async fn sync_interrupted(
        &self,
        left: usize,
        right: usize,
        config: &SessionConfig,
        fail_at: usize,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        self.sync_with_failure(left, right, config, Some(fail_at))
            .await
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
        fail_at: Option<usize>,
    ) -> Result<(), SyncError<InMemoryTransportError>> {
        assert_ne!(left, right, "cannot synchronize a node with itself");
        let (mut left_transport, mut right_transport) = match fail_at {
            Some(fail_at) => in_memory_transport_pair_failing_at(Some(fail_at)),
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
