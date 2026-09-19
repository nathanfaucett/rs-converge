mod case;
mod chaos;
mod cluster;
mod transport;

pub use case::{Case, assert_case};
pub use chaos::{ChaosNetwork, ChaosScenario, ChaosStep, run_chaos};
pub use cluster::{
    Cluster, Node, RedbClusterCleanup, automerge_redb_cluster, direct_in_memory_cluster,
    direct_redb_cluster,
};
pub use value::{Row, Value};
pub fn run<T>(future: impl core::future::Future<Output = T>) -> T {
    futures::executor::block_on(future)
}

pub use transport::{
    InMemoryTransport, InMemoryTransportError, TransportDirection, in_memory_transport_pair,
    in_memory_transport_pair_failing, in_memory_transport_pair_failing_at,
};
