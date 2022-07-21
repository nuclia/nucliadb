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

pub mod ops;
use std::collections::HashMap;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::segment::SegmentSlice;

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
    pub layer: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub segment: usize,
    pub vector: SegmentSlice,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Edge {
    pub dist: f32,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    pub lout: HashMap<Node, HashMap<Node, Edge>>,
    pub lin: HashMap<Node, HashMap<Node, Edge>>,
}

impl GraphLayer {
    pub fn new() -> GraphLayer {
        GraphLayer::default()
    }
    pub fn add_node(&mut self, node: Node) {
        self.lout.entry(node).or_insert_with(HashMap::new);
        self.lin.entry(node).or_insert_with(HashMap::new);
    }
    pub fn add_edge(&mut self, from: Node, edge: Edge, to: Node) {
        self.lout
            .get_mut(&from)
            .and_then(|edges| edges.insert(to, edge));
        self.lin
            .get_mut(&to)
            .and_then(|edges| edges.insert(from, edge));
    }
    pub fn remove_edge(&mut self, from: Node, to: Node) {
        self.lout.get_mut(&from).and_then(|edges| edges.remove(&to));
        self.lin.get_mut(&to).and_then(|edges| edges.remove(&from));
    }
    pub fn has_node(&self, node: Node) -> bool {
        debug_assert_eq!(self.lout.contains_key(&node), self.lin.contains_key(&node));
        self.lout.contains_key(&node)
    }
    pub fn get_out_edges(&self, node: Node) -> impl Iterator<Item = (&Node, &Edge)> {
        self.lout[&node].iter()
    }
    pub fn get_in_edges(&self, node: Node) -> impl Iterator<Item = (&Node, &Edge)> {
        self.lin[&node].iter()
    }
    pub fn get_edge(&self, from: Node, to: Node) -> Option<Edge> {
        self.lout
            .get(&from)
            .and_then(|edges| edges.get(&to))
            .copied()
    }
    pub fn no_out_edges(&self, node: Node) -> usize {
        self.lout.get(&node).map_or(0, |v| v.len())
    }
    pub fn no_nodes(&self) -> usize {
        self.lout.len()
    }
    pub fn first(&self) -> Option<Node> {
        self.lout.keys().next().cloned()
    }
    pub fn is_empty(&self) -> bool {
        self.lout.len() == 0
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Hnsw {
    pub entry_point: Option<EntryPoint>,
    pub layers: Vec<GraphLayer>,
}
