mod settings;
mod shard_cutover;
mod strategy;

pub use settings::Settings;
pub use shard_cutover::ShardCutover;

use derive_more::Deref;
use itertools::Itertools;

use crate::node::{Node, WeightedNode};

#[derive(Deref)]
pub struct Balancer(Settings);

impl Balancer {
    pub fn new(settings: Settings) -> Self {
        Self(settings)
    }

    pub fn balance_shards(&self, nodes: &[Node]) -> Vec<ShardCutover> {
        let shard_cutovers = Vec::default();

        loop {
            // sorts nodes by interest depending of the balance strategy
            let mut nodes = nodes
                .iter()
                .map(|node| WeightedNode::new(node, self.strategy.weights_node(node)))
                .sorted_by_cached_key(|node| node.weight)
                .collect::<Vec<_>>();

            let Some(source_node) = nodes.pop().filter(|_| nodes.len() > 1) else {
                break;
            };

            let destination_node = &nodes[0];

            if self
                .tolerance
                .diff(source_node.weight as f32, destination_node.weight as f32)
                .is_above()
            {

            }
        }

        shard_cutovers
    }
}
