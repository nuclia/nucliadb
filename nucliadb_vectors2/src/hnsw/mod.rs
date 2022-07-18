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

use crate::segment::SegmentSlice;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod params {
    pub fn level_factor() -> f64 {
        1.0 / (m() as f64).ln()
    }
    pub const fn m_max() -> usize {
        30
    }
    pub const fn m() -> usize {
        30
    }
    pub const fn ef_construction() -> usize {
        100
    }
    pub const fn k_neighbours() -> usize {
        10
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntryPoint {
    pub node: Node,
    pub layer: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub segment: u64,
    pub vector: SegmentSlice,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Edge {
    pub dist: f32,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    pub cnx: HashMap<Node, HashMap<Node, Edge>>,
}

impl Default for GraphLayer {
    fn default() -> Self {
        GraphLayer::new()
    }
}

impl GraphLayer {
    // const INITIAL_CAPACITY: usize = 1000;
    // const OCCUPANCY_MIN: usize = 70;
    // const OCCUPANCY_MAX: usize = 90;
    // const OCCUPANCY_MID: usize = 80;
    // const fn should_increase(len: usize, cap: usize) -> bool {
    //     let occupancy = (len / cap) * 100;
    //     occupancy > GraphLayer::OCCUPANCY_MAX
    // }
    // const fn should_decrease(len: usize, cap: usize) -> bool {
    //     let occupancy = (len / cap) * 100;
    //     cap > GraphLayer::INITIAL_CAPACITY && occupancy < GraphLayer::OCCUPANCY_MIN
    // }
    // const fn resize_by(len: usize, cap: usize) -> i64 {
    //     let len = len as i64;
    //     let cap = cap as i64;
    //     let ocup = GraphLayer::OCCUPANCY_MID as i64;
    //     ((100 * len) - (ocup * cap)) / ocup
    // }
    // fn increase_policy(&mut self) {
    //     if let Some(factor) = self.check_policy(GraphLayer::should_increase) {
    //         self.cnx.reserve(factor as usize);
    //     }
    // }
    // fn decrease_policy(&mut self) {
    //     if let Some(factor) = self.check_policy(GraphLayer::should_decrease) {
    //         let cap = ((self.cnx.capacity() as i64) + factor) as usize;
    //         self.cnx.shrink_to(cap);
    //     }
    // }
    // fn check_policy(&self, policy: fn(_: usize, _: usize) -> bool) -> Option<i64> {
    //     let len = self.cnx.len();
    //     let cap = self.cnx.capacity();
    //     if policy(self.cnx.len(), self.cnx.capacity()) {
    //         Some(GraphLayer::resize_by(len, cap))
    //     } else {
    //         None
    //     }
    // }
    pub fn new() -> GraphLayer {
        GraphLayer {
            cnx: HashMap::new(),
        }
    }
    pub fn add_node(&mut self, node: Node) {
        self.cnx.entry(node).or_insert_with(HashMap::new);
    }
    pub fn add_edge(&mut self, from: Node, edge: Edge, to: Node) {
        self.cnx
            .get_mut(&from)
            .and_then(|edges| edges.insert(to, edge));
    }
    pub fn remove_node(&mut self, node: Node) {
        self.cnx.remove(&node);
    }
    pub fn remove_edge(&mut self, from: Node, to: Node) {
        self.cnx.get_mut(&from).and_then(|edges| edges.remove(&to));
    }
    pub fn has_node(&self, node: Node) -> bool {
        self.cnx.contains_key(&node)
    }
    pub fn get_edges(&self, node: Node) -> impl Iterator<Item = (&Node, &Edge)> {
        self.cnx[&node].iter()
    }
    pub fn get_edge(&self, from: Node, to: Node) -> Option<Edge> {
        self.cnx
            .get(&from)
            .and_then(|edges| edges.get(&to))
            .copied()
    }
    pub fn no_edges(&self, node: Node) -> usize {
        self.cnx.get(&node).map_or(0, |v| v.len())
    }
    pub fn no_nodes(&self) -> usize {
        self.cnx.len()
    }
    pub fn first(&self) -> Option<Node> {
        self.cnx.keys().next().cloned()
    }
    pub fn is_empty(&self) -> bool {
        self.cnx.len() == 0
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Hnsw {
    pub max_layer: usize,
    pub entry_point: Option<EntryPoint>,
    pub layers_out: Vec<GraphLayer>,
    pub layers_in: Vec<GraphLayer>,
}
