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

use crate::index::Location;

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
    pub node: Location,
    pub layer: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Edge {
    pub dist: f32,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct GraphLayer {
    pub lout: HashMap<Location, HashMap<Location, Edge>>,
    pub lin: HashMap<Location, HashMap<Location, Edge>>,
}

impl GraphLayer {
    fn new() -> GraphLayer {
        GraphLayer::default()
    }
    fn add_node(&mut self, node: Location) {
        self.lout.entry(node).or_insert_with(HashMap::new);
        self.lin.entry(node).or_insert_with(HashMap::new);
    }
    fn add_edge(&mut self, from: Location, edge: Edge, to: Location) {
        self.lout
            .get_mut(&from)
            .and_then(|edges| edges.insert(to, edge));
        self.lin
            .get_mut(&to)
            .and_then(|edges| edges.insert(from, edge));
    }
    fn take_out_edges(&mut self, x: Location) -> Vec<(Location, f32)> {
        self.lout
            .get_mut(&x)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .fold(vec![], |mut collector, (y, edge)| {
                self.lin.get_mut(&y).and_then(|edges| edges.remove(&x));
                collector.push((y, edge.dist));
                collector
            })
    }
    fn take_in_edges(&mut self, x: Location) -> Vec<(Location, f32)> {
        self.lin
            .get_mut(&x)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .fold(vec![], |mut collector, (y, edge)| {
                self.lout.get_mut(&y).and_then(|edges| edges.remove(&x));
                collector.push((y, edge.dist));
                collector
            })
    }
    fn has_node(&self, node: Location) -> bool {
        debug_assert_eq!(self.lout.contains_key(&node), self.lin.contains_key(&node));
        self.lout.contains_key(&node)
    }
    fn get_out_edges(&self, node: Location) -> impl Iterator<Item = (&Location, &Edge)> {
        self.lout[&node].iter()
    }
    fn no_out_edges(&self, node: Location) -> usize {
        self.lout.get(&node).map_or(0, |v| v.len())
    }
    fn no_nodes(&self) -> usize {
        self.lout.len()
    }
    fn first(&self) -> Option<Location> {
        self.lout.keys().next().cloned()
    }
    fn is_empty(&self) -> bool {
        self.lout.len() == 0
    }
    #[cfg(test)]
    fn remove_edge(&mut self, from: Location, to: Location) {
        self.lout.get_mut(&from).and_then(|edges| edges.remove(&to));
        self.lin.get_mut(&to).and_then(|edges| edges.remove(&from));
    }
    #[cfg(test)]
    fn get_in_edges(&self, node: Location) -> impl Iterator<Item = (&Location, &Edge)> {
        self.lin[&node].iter()
    }
    #[cfg(test)]
    fn get_edge(&self, from: Location, to: Location) -> Option<Edge> {
        self.lout
            .get(&from)
            .and_then(|edges| edges.get(&to))
            .copied()
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Hnsw {
    pub entry_point: Option<EntryPoint>,
    pub layers: Vec<GraphLayer>,
}

impl Hnsw {
    pub fn new() -> Hnsw {
        Hnsw::default()
    }
    fn increase_layers_with(&mut self, x: Location, level: usize) -> &mut Self {
        while self.layers.len() < level {
            let mut new_layer = GraphLayer::new();
            new_layer.add_node(x);
            self.layers.push(new_layer);
        }
        self
    }
    fn remove_empty_layers(&mut self) -> &mut Self {
        while self.layers.last().map(|l| l.is_empty()).unwrap_or_default() {
            self.layers.pop();
        }
        self
    }
    fn update_entry_point(&mut self) -> &mut Self {
        self.entry_point = self
            .layers
            .last()
            .and_then(|l| l.first().map(|node| (node, self.layers.len())))
            .map(|(node, layer)| EntryPoint { node, layer });
        self
    }
}
