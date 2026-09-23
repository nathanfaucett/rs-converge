pub mod case;
pub mod chaos;
pub mod cluster;
pub mod runner;
pub mod transport;

pub use case::{Expectation, ExpectedError, NodeId, Step, TestCase, TestCaseBuilder, TestSuite};
pub use chaos::{ChaosNetwork, ChaosScenario, ChaosStep, run_chaos};
pub use cluster::{
    Cluster, Node, RedbClusterCleanup, automerge_in_memory_cluster, automerge_redb_cluster,
};
pub use runner::{
    ChaosRunner, ClusterOfflineRunner, ClusterRealtimeRunner, RunnerError, SingleNodeRunner,
    TestRunner, VerificationPolicy,
};
pub use transport::{
    InMemoryTransport, InMemoryTransportError, TransportDirection, in_memory_transport_pair,
    in_memory_transport_pair_failing, in_memory_transport_pair_failing_at,
};
pub use value::{Row, Value};

pub fn run<T>(future: impl core::future::Future<Output = T>) -> T {
    futures::executor::block_on(future)
}
