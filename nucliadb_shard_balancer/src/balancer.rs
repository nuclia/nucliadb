// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

use std::collections::VecDeque;
use std::num::NonZeroUsize;

use clap::{Args, ValueEnum};
use itertools::Itertools;

use crate::node::{Node, WeightedNode};
use crate::shard::{Shard, ShardCutover, ShardIndex};
use crate::threshold::Threshold;

/// A structure containing all shard balancing settings.
#[derive(Args)]
pub struct BalanceSettings {
    /// The shard balancing strategy.
    #[arg(short, long, value_enum)]
    strategy: BalanceStrategy,
    /// The value that indicates the load tolerance between nodes.
    ///
    /// To put it simply, if the difference (calculated depending of the balance strategy) between
    /// nodes is below that load tolerance, none shard balancing takes place.
    #[arg(short, long)]
    load_tolerance: Threshold,
    /// The maximum number of shards per node.
    #[arg(long)]
    shard_limit: NonZeroUsize,
    /// The port to use when transferring shard(s) between nodes
    #[arg(short, long)]
    port: u16,
}

/// All available strategies for distributing shards on the nodes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum BalanceStrategy {
    /// Prioritize the distribution of active shards.
    ///
    /// Note that this strategy will try to evenly distribute active shards on the nodes
    /// by picking any shard that decrease the more the load difference between nodes.
    ActiveShard,

    /// Prioritize the distribution of workload.
    ///
    /// Note that this strategy will always try to evenly distribute workload on the nodes
    /// then the number of active shards on nodes may vary a lot.
    Workload,
}

/// A high-level balancer that, given a list of nodes, performs shard cutover calculation to
/// distribute evenly shards over nodes.
///
/// The balancer algorithm is barely the following one:
/// 1. Weight all nodes depending of the balance strategy
///     - The [`BalanceStrategy::ActiveShard`] strategy prioritizes the number of active shards on
///       the nodes.
///     - The [`BalanceStrategy::Workload`] strategy prioritizes the workload of the nodes.
/// 2. Select all the nodes with enough differences with the weighier one
/// 3. Find a shard that match the following criterias:
///     - The given node does not contain the shard replica.
///     - The shard has not been moved during previous shard cutover.
/// 4. *OPTIONAL*: If the node candidate is full, swap shard candidate with an idle shard.
pub struct Balancer {
    settings: BalanceSettings,
}

impl Balancer {
    /// Creates a new balancer with the given balance settings.
    pub fn new(settings: BalanceSettings) -> Self {
        Self { settings }
    }

    /// Creates the list of shard cutovers to evenly distribute the shards in the given list of
    /// nodes.
    ///
    /// Note that this method will do not perform the real shard balancing but just create the list
    /// of effective shard cutovers.
    pub fn balance_shards<'a>(
        &'a self,
        mut nodes: Vec<Node>,
        shard_index: &'a ShardIndex,
    ) -> impl Iterator<Item = ShardCutover> + '_ {
        // store moved shards in order to avoid cyclic shard balancing
        let mut moved_shards = Vec::default();

        std::iter::from_fn(move || {
            let (shard_cutover, shard, source_position, destination_position) = {
                // sorts nodes by weight depending of the balance strategy
                let mut nodes = nodes
                    .iter()
                    .enumerate()
                    .map(|(i, node)| WeightedNode::new(node, self.weight_node(node), i))
                    .sorted_by_cached_key(WeightedNode::weight)
                    .rev()
                    // skips weightier nodes with only one active shard because
                    // moving shard from them will unbalance the load even more
                    .skip_while(|node| node.active_shards().count() <= 1)
                    .collect::<VecDeque<_>>();

                let weightier_node = nodes.pop_front()?;

                let (source_node, destination_node, shard) = nodes
                    .iter()
                    .rev()
                    // only consider nodes with enough difference
                    .filter(|node_candidate| {
                        self.settings
                            .load_tolerance
                            .diff(weightier_node.weight(), node_candidate.weight())
                            .is_above()
                    })
                    .find_map(|node_candidate| {
                        self.select_shard(
                            weightier_node,
                            *node_candidate,
                            &moved_shards,
                            shard_index,
                        )
                    })?;

                let shard_cutover = ShardCutover::new(
                    shard.id().to_string(),
                    source_node.listen_address(),
                    destination_node.listen_address(),
                    self.settings.port,
                );

                // we use position trick here to avoid conflict with the borrow checker.
                (
                    shard_cutover,
                    shard.clone(),
                    source_node.position(),
                    destination_node.position(),
                )
            };

            // move shard in order to propagate node weight changes in next iteration
            nodes[source_position].remove_shard(shard.id());
            moved_shards.push(shard.id().to_string());
            nodes[destination_position].add_shard(shard);

            Some(shard_cutover)
        })
    }

    /// Get the node weight depending of the shard balancing strategy.
    fn weight_node(&self, node: &Node) -> u64 {
        match self.settings.strategy {
            BalanceStrategy::ActiveShard => node.active_shards().count() as u64,
            BalanceStrategy::Workload => node.load_score(),
        }
    }

    /// Select the appropriate shard to move between the two nodes.
    ///
    /// Note that if the node candidate is full, the method will try to swap any idle shard
    /// from it to the weightier node.
    fn select_shard<'a, 'b>(
        &'a self,
        weightier_node: WeightedNode<'b>,
        node_candidate: WeightedNode<'b>,
        moved_shards: &[String],
        shard_index: &ShardIndex,
    ) -> Option<(WeightedNode<'b>, WeightedNode<'b>, &'b Shard)> {
        let weight_difference = weightier_node.weight() - node_candidate.weight();

        weightier_node
            .active_shards()
            // removes all shards that can not be used for the current shard balancing
            .filter(|shard| {
                let shard_handle = shard_index.get(shard.id());

                !node_candidate
                    .shards()
                    .any(|shard| shard_handle.contains(shard.id()))
                    && !moved_shards
                        .iter()
                        .any(|moved_shard| moved_shard == shard.id())
            })
            .min_by_key(|shard| {
                // we multiply by two in order to represent both the substraction from the weightier
                // node and the addition to the node candidate.
                let shard_difference = shard.load_score() * 2;
                let (min, max) = (
                    weight_difference.min(shard_difference),
                    weight_difference.max(shard_difference),
                );

                // selects the shard that give the lower difference between the two nodes
                max - min
            })
            .and_then(|shard| {
                // checks if node candidate can accept any new shard.
                // if not, checks if we can move an idle shard from the node candidate to
                // the weightier one.
                if node_candidate.shards().count() == self.settings.shard_limit.get()
                    && !weightier_node.shards().count() != self.settings.shard_limit.get()
                {
                    node_candidate
                        .idle_shards()
                        // select first idle shard
                        .next()
                        .map(|shard| (node_candidate, weightier_node, shard))
                } else {
                    Some((weightier_node, node_candidate, shard))
                }
            })
    }
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests {
    use super::*;

    #[test]
    fn it_distributes_active_shards() {
        let shard_index = ShardIndex::new(Vec::default());
        let nodes = vec![
            Node::new(
                "n1".to_string(),
                "192.168.0.1:4444".parse().unwrap(),
                vec![
                    Shard::new("s1".to_string(), 0, "kb1".to_string()),
                    Shard::new("s2".to_string(), 42, "kb1".to_string()),
                    Shard::new("s3".to_string(), 21, "kb1".to_string()),
                    Shard::new("s4".to_string(), 21, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n2".to_string(),
                "192.168.0.2:4444".parse().unwrap(),
                vec![
                    Shard::new("s5".to_string(), 0, "kb1".to_string()),
                    Shard::new("s6".to_string(), 0, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n3".to_string(),
                "192.168.0.3:4444".parse().unwrap(),
                vec![
                    Shard::new("s7".to_string(), 1, "kb1".to_string()),
                    Shard::new("s8".to_string(), 2, "kb1".to_string()),
                    Shard::new("s9".to_string(), 3, "kb1".to_string()),
                    Shard::new("s10".to_string(), 4, "kb1".to_string()),
                ],
            ),
        ];

        let tests = vec![
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::ActiveShard,
                    load_tolerance: Threshold::PlainValue(1),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![
                    ShardCutover::new(
                        "s8".to_string(),
                        "192.168.0.3:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                    ShardCutover::new(
                        "s7".to_string(),
                        "192.168.0.3:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                ],
            ),
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::ActiveShard,
                    load_tolerance: Threshold::PlainValue(0),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![
                    ShardCutover::new(
                        "s8".to_string(),
                        "192.168.0.3:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                    ShardCutover::new(
                        "s7".to_string(),
                        "192.168.0.3:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                    ShardCutover::new(
                        "s3".to_string(),
                        "192.168.0.1:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                ],
            ),
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::ActiveShard,
                    load_tolerance: Threshold::PlainValue(2),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![ShardCutover::new(
                    "s8".to_string(),
                    "192.168.0.3:4444".parse().unwrap(),
                    "192.168.0.2:4444".parse().unwrap(),
                    42,
                )],
            ),
        ];

        for (balancer, expected_shard_cutovers) in tests {
            let shard_cutovers = balancer
                .balance_shards(nodes.clone(), &shard_index)
                .collect::<Vec<_>>();

            assert_eq!(shard_cutovers, expected_shard_cutovers);
        }
    }

    #[test]
    fn it_distributes_workload() {
        let shard_index = ShardIndex::new(Vec::default());
        let nodes = vec![
            Node::new(
                "n1".to_string(),
                "192.168.0.1:4444".parse().unwrap(),
                vec![
                    Shard::new("s1".to_string(), 0, "kb1".to_string()),
                    Shard::new("s2".to_string(), 100, "kb1".to_string()),
                    Shard::new("s3".to_string(), 20, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n2".to_string(),
                "192.168.0.2:4444".parse().unwrap(),
                vec![
                    Shard::new("s4".to_string(), 0, "kb1".to_string()),
                    Shard::new("s5".to_string(), 0, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n3".to_string(),
                "192.168.0.3:4444".parse().unwrap(),
                vec![
                    Shard::new("s6".to_string(), 50, "kb1".to_string()),
                    Shard::new("s7".to_string(), 25, "kb1".to_string()),
                    Shard::new("s8".to_string(), 25, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n4".to_string(),
                "192.168.0.4:4444".parse().unwrap(),
                vec![
                    Shard::new("s8".to_string(), 0, "kb1".to_string()),
                    Shard::new("s9".to_string(), 80, "kb1".to_string()),
                ],
            ),
        ];

        let tests = vec![
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::Workload,
                    load_tolerance: Threshold::PlainValue(120),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![],
            ),
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::Workload,
                    load_tolerance: Threshold::PlainValue(100),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![ShardCutover::new(
                    "s2".to_string(),
                    "192.168.0.1:4444".parse().unwrap(),
                    "192.168.0.2:4444".parse().unwrap(),
                    42,
                )],
            ),
            (
                Balancer::new(BalanceSettings {
                    strategy: BalanceStrategy::Workload,
                    load_tolerance: Threshold::PlainValue(50),
                    shard_limit: NonZeroUsize::new(10).unwrap(),
                    port: 42,
                }),
                vec![
                    ShardCutover::new(
                        "s2".to_string(),
                        "192.168.0.1:4444".parse().unwrap(),
                        "192.168.0.2:4444".parse().unwrap(),
                        42,
                    ),
                    ShardCutover::new(
                        "s6".to_string(),
                        "192.168.0.3:4444".parse().unwrap(),
                        "192.168.0.1:4444".parse().unwrap(),
                        42,
                    ),
                ],
            ),
        ];

        for (balancer, expected_shard_cutovers) in tests {
            let shard_cutovers = balancer
                .balance_shards(nodes.clone(), &shard_index)
                .collect::<Vec<_>>();

            assert_eq!(shard_cutovers, expected_shard_cutovers);
        }
    }

    #[test]
    fn it_moves_out_idle_shard_on_full_node() {
        let shard_index = ShardIndex::new(Vec::default());
        let nodes = vec![
            Node::new(
                "n1".to_string(),
                "192.168.0.1:4444".parse().unwrap(),
                vec![
                    Shard::new("s1".to_string(), 1, "kb1".to_string()),
                    Shard::new("s2".to_string(), 2, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n2".to_string(),
                "192.168.0.2:4444".parse().unwrap(),
                vec![
                    Shard::new("s3".to_string(), 0, "kb1".to_string()),
                    Shard::new("s4".to_string(), 0, "kb1".to_string()),
                    Shard::new("s4".to_string(), 0, "kb1".to_string()),
                ],
            ),
        ];

        let balancer = Balancer::new(BalanceSettings {
            strategy: BalanceStrategy::ActiveShard,
            load_tolerance: Threshold::PlainValue(1),
            shard_limit: NonZeroUsize::new(3).unwrap(),
            port: 42,
        });

        let expected_shard_cutovers = vec![
            ShardCutover::new(
                "s3".to_string(),
                "192.168.0.2:4444".parse().unwrap(),
                "192.168.0.1:4444".parse().unwrap(),
                42,
            ),
            ShardCutover::new(
                "s1".to_string(),
                "192.168.0.1:4444".parse().unwrap(),
                "192.168.0.2:4444".parse().unwrap(),
                42,
            ),
        ];

        let shard_cutovers = balancer
            .balance_shards(nodes, &shard_index)
            .collect::<Vec<_>>();

        assert_eq!(shard_cutovers, expected_shard_cutovers);
    }

    #[test]
    fn it_ignores_loaded_nodes_with_only_one_active_shard() {
        let shard_index = ShardIndex::new(Vec::default());
        let nodes = vec![
            Node::new(
                "n1".to_string(),
                "192.168.0.1:4444".parse().unwrap(),
                vec![Shard::new("s1".to_string(), 100, "kb1".to_string())],
            ),
            Node::new(
                "n2".to_string(),
                "192.168.0.2:4444".parse().unwrap(),
                vec![
                    Shard::new("s2".to_string(), 50, "kb1".to_string()),
                    Shard::new("s3".to_string(), 25, "kb1".to_string()),
                ],
            ),
            Node::new(
                "n3".to_string(),
                "192.168.0.3:4444".parse().unwrap(),
                vec![Shard::new("4".to_string(), 0, "kb1".to_string())],
            ),
        ];

        let balancer = Balancer::new(BalanceSettings {
            strategy: BalanceStrategy::Workload,
            load_tolerance: Threshold::PlainValue(50),
            shard_limit: NonZeroUsize::new(10).unwrap(),
            port: 42,
        });

        let expected_shard_cutovers = vec![ShardCutover::new(
            "s2".to_string(),
            "192.168.0.2:4444".parse().unwrap(),
            "192.168.0.3:4444".parse().unwrap(),
            42,
        )];

        let shard_cutovers = balancer
            .balance_shards(nodes, &shard_index)
            .collect::<Vec<_>>();

        assert_eq!(shard_cutovers, expected_shard_cutovers);
    }
}
