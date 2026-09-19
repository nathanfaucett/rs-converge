use std::collections::HashSet;

use db_engine::{Kernel, RowCodec};
use db_sync::SessionConfig;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

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

pub async fn run_chaos<K: Kernel, R: RowCodec<K::Transaction>>(
    cluster: Cluster<K, R>,
    scenario: ChaosScenario,
    config: &SessionConfig,
) {
    assert_eq!(
        cluster.len(),
        scenario.nodes,
        "{}: node count mismatch",
        scenario.name
    );
    let mut network = ChaosNetwork::new(scenario.seed);

    for step in scenario.steps {
        match step {
            ChaosStep::Exec { node, sql } => {
                let _ = cluster.exec(node, sql).await;
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
                cluster.assert_converged(query).await;
            }
            ChaosStep::CurrentlyDiverged { a, b, query } => {
                assert_ne!(
                    cluster.exec(a, query).await,
                    cluster.exec(b, query).await,
                    "expected divergence between {a} and {b}"
                );
            }
        }
    }
}

async fn sync_available_pairs<K: Kernel, R: RowCodec<K::Transaction>>(
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
