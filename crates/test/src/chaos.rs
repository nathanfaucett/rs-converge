use std::collections::HashSet;

use engine::Kernel;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sync::SessionConfig;
use sync::SyncRowCodec;

use crate::cluster::Cluster;

pub struct ChaosNetwork {
    partitions: Vec<HashSet<usize>>,
    drop_rate: f64,
    rng: StdRng,
}

impl ChaosNetwork {
    pub fn new(seed: u64) -> Self {
        Self {
            partitions: vec![],
            drop_rate: 0.0,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn partition(&mut self, groups: &[&[usize]]) {
        self.partitions = groups
            .iter()
            .map(|group| group.iter().copied().collect())
            .collect();
    }

    pub fn heal(&mut self) {
        self.partitions.clear();
    }

    pub fn set_drop_rate(&mut self, rate: f64) {
        self.drop_rate = rate.clamp(0.0, 1.0);
    }

    pub fn can_communicate(&self, from: usize, to: usize) -> bool {
        self.partitions.is_empty()
            || self
                .partitions
                .iter()
                .any(|partition| partition.contains(&from) && partition.contains(&to))
    }

    pub fn should_interrupt(&mut self) -> bool {
        self.rng.random::<f64>() < self.drop_rate
    }
}

pub struct ChaosScenario {
    pub name: &'static str,
    pub nodes: usize,
    pub seed: u64,
    pub steps: Vec<ChaosStep>,
}

pub enum ChaosStep {
    Exec {
        node: usize,
        sql: &'static str,
    },
    Sync,
    Partition {
        groups: &'static [&'static [usize]],
    },
    Heal,
    DropRate(f64),
    EventuallyConsistent {
        query: &'static str,
        max_rounds: usize,
    },
    CurrentlyDiverged {
        a: usize,
        b: usize,
        query: &'static str,
    },
}

pub async fn run_chaos<K: Kernel, R: SyncRowCodec<K::Transaction>>(
    cluster: Cluster<K, R>,
    scenario: ChaosScenario,
    config: &SessionConfig,
) {
    let ChaosScenario {
        name,
        nodes,
        seed,
        steps,
    } = scenario;
    assert_eq!(
        cluster.len(),
        nodes,
        "{name} (seed {seed}): node count mismatch"
    );
    let mut network = ChaosNetwork::new(seed);

    for step in steps {
        match step {
            ChaosStep::Exec { node, sql } => {
                assert!(
                    cluster.try_exec(node, sql).await.is_ok(),
                    "{name} (seed {seed}): execution failed on node {node}: {sql}"
                );
            }
            ChaosStep::Sync => sync_available_pairs(&cluster, &mut network, config).await,
            ChaosStep::Partition { groups } => network.partition(groups),
            ChaosStep::Heal => network.heal(),
            ChaosStep::DropRate(rate) => network.set_drop_rate(rate),
            ChaosStep::EventuallyConsistent { query, max_rounds } => {
                for _ in 0..max_rounds {
                    sync_available_pairs(&cluster, &mut network, config).await;
                    if cluster.is_converged(query).await {
                        break;
                    }
                }
                assert!(
                    cluster.is_converged(query).await,
                    "{name} (seed {seed}): replicas did not ofdb on query: {query}"
                );
            }
            ChaosStep::CurrentlyDiverged { a, b, query } => {
                assert_ne!(
                    cluster.exec(a, query).await,
                    cluster.exec(b, query).await,
                    "{} (seed {}): expected divergence between {a} and {b}",
                    name,
                    seed
                );
            }
        }
    }
}

async fn sync_available_pairs<K: Kernel, R: SyncRowCodec<K::Transaction>>(
    cluster: &Cluster<K, R>,
    network: &mut ChaosNetwork,
    config: &SessionConfig,
) {
    for left in 0..cluster.len() {
        for right in left + 1..cluster.len() {
            if !network.can_communicate(left, right) {
                continue;
            }
            let result = if network.should_interrupt() {
                cluster.sync_interrupted(left, right, config, 0).await
            } else {
                cluster.sync(left, right, config).await
            };
            let _ = result;
        }
    }
}
