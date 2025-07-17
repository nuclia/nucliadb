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

use fxhash::FxHashMap;
use ops_hnsw::{Hnsw, Layer};

use super::*;

const NO_EDGES: [(Address, Edge); 0] = [];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryPoint {
    pub node: Address,
    pub layer: usize,
}

pub type Edge = f32;

#[derive(Default, Clone)]
pub struct RAMLayer {
    pub out: FxHashMap<Address, Vec<(Address, Edge)>>,
}

impl RAMLayer {
    fn out_edges(&self, node: Address) -> std::iter::Copied<std::slice::Iter<'_, (Address, Edge)>> {
        self.out
            .get(&node)
            .map_or_else(|| NO_EDGES.iter().copied(), |out| out.iter().copied())
    }
    pub fn new() -> RAMLayer {
        RAMLayer::default()
    }
    pub fn add_node(&mut self, node: Address) {
        self.out.entry(node).or_default();
    }
    pub fn add_edge(&mut self, from: Address, edge: Edge, to: Address) {
        if let Some(edges) = self.out.get_mut(&from) {
            edges.push((to, edge))
        }
    }
    pub fn take_out_edges(&mut self, x: Address) -> Vec<(Address, Edge)> {
        self.out.get_mut(&x).map(std::mem::take).unwrap_or_default()
    }
    pub fn no_out_edges(&self, node: Address) -> usize {
        self.out.get(&node).map_or(0, |v| v.len())
    }
    pub fn first(&self) -> Option<Address> {
        self.out.keys().next().cloned()
    }
    pub fn is_empty(&self) -> bool {
        self.out.len() == 0
    }
}

#[derive(Default, Clone)]
pub struct RAMHnsw {
    pub entry_point: Option<EntryPoint>,
    pub layers: Vec<RAMLayer>,
}
impl RAMHnsw {
    pub fn new() -> RAMHnsw {
        Self::default()
    }
    pub fn increase_layers_with(&mut self, x: Address, level: usize) -> &mut Self {
        while self.layers.len() <= level {
            let mut new_layer = RAMLayer::new();
            new_layer.add_node(x);
            self.layers.push(new_layer);
        }
        self
    }
    pub fn remove_empty_layers(&mut self) -> &mut Self {
        while self.layers.last().map(|l| l.is_empty()).unwrap_or_default() {
            self.layers.pop();
        }
        self
    }
    pub fn update_entry_point(&mut self) -> &mut Self {
        self.remove_empty_layers();
        self.entry_point = self
            .layers
            .iter()
            .enumerate()
            .last()
            .and_then(|(index, l)| l.first().map(|node| (node, index)))
            .map(|(node, layer)| EntryPoint { node, layer });
        self
    }
    pub fn no_layers(&self) -> usize {
        self.layers.len()
    }
}

impl<'a> Layer for &'a RAMLayer {
    type EdgeIt = std::iter::Copied<std::slice::Iter<'a, (Address, Edge)>>;
    fn get_out_edges(&self, node: Address) -> Self::EdgeIt {
        self.out_edges(node)
    }
}

impl<'a> Hnsw for &'a RAMHnsw {
    type L = &'a RAMLayer;
    fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_point
    }
    fn get_layer(&self, i: usize) -> Self::L {
        &self.layers[i]
    }
}
