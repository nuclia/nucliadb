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

use rustc_hash::FxHashMap;
use search::{SearchableHnsw, SearchableLayer};

use crate::VectorAddr;

use super::*;

const NO_EDGES: [(VectorAddr, Edge); 0] = [];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryPoint {
    pub node: VectorAddr,
    pub layer: usize,
}

pub type Edge = f32;

#[derive(Default, Clone)]
pub struct RAMLayer {
    pub out: FxHashMap<VectorAddr, Vec<(VectorAddr, Edge)>>,
}

impl RAMLayer {
    fn out_edges(&self, node: VectorAddr) -> std::iter::Copied<std::slice::Iter<'_, (VectorAddr, Edge)>> {
        self.out
            .get(&node)
            .map_or_else(|| NO_EDGES.iter().copied(), |out| out.iter().copied())
    }
    pub fn new() -> RAMLayer {
        RAMLayer::default()
    }
    pub fn add_node(&mut self, node: VectorAddr) {
        self.out.entry(node).or_default();
    }
    pub fn add_edge(&mut self, from: VectorAddr, edge: Edge, to: VectorAddr) {
        if let Some(edges) = self.out.get_mut(&from) {
            edges.push((to, edge))
        }
    }
    pub fn take_out_edges(&mut self, x: VectorAddr) -> Vec<(VectorAddr, Edge)> {
        self.out.get_mut(&x).map(std::mem::take).unwrap_or_default()
    }
    pub fn no_out_edges(&self, node: VectorAddr) -> usize {
        self.out.get(&node).map_or(0, |v| v.len())
    }
    pub fn first(&self) -> Option<VectorAddr> {
        self.out.keys().next().cloned()
    }
    pub fn contains(&self, node: &VectorAddr) -> bool {
        self.out.contains_key(node)
    }
}

pub struct RAMHnsw {
    pub entry_point: EntryPoint,
    pub layers: Vec<RAMLayer>,
}
impl RAMHnsw {
    pub fn new() -> RAMHnsw {
        Self {
            entry_point: EntryPoint {
                node: VectorAddr(0),
                layer: 0,
            },
            layers: vec![],
        }
    }

    /// Adds a node to the graph at all layers below the selected top layer
    pub fn add_node(&mut self, node: VectorAddr, top_layer: usize) {
        for _ in self.layers.len()..=top_layer {
            self.layers.push(RAMLayer::new());
        }

        for layer in 0..=top_layer {
            self.layers[layer].add_node(node)
        }
    }

    /// Updates the entrypoint to point to the first node of the top layer
    pub fn update_entry_point(&mut self) {
        // Only update if the entrypoint is not already at the top layer
        if self.layers.len() > self.entry_point.layer + 1 {
            self.entry_point = EntryPoint {
                node: self.layers.last().unwrap().first().unwrap(),
                layer: self.layers.len() - 1,
            }
        }
    }

    pub fn no_layers(&self) -> usize {
        self.layers.len()
    }
}

impl<'a> SearchableLayer for &'a RAMLayer {
    type EdgeIt = std::iter::Copied<std::slice::Iter<'a, (VectorAddr, Edge)>>;
    fn get_out_edges(&self, node: VectorAddr) -> Self::EdgeIt {
        self.out_edges(node)
    }
}

impl<'a> SearchableHnsw for &'a RAMHnsw {
    type L = &'a RAMLayer;
    fn get_entry_point(&self) -> EntryPoint {
        self.entry_point
    }
    fn get_layer(&self, i: usize) -> Self::L {
        &self.layers[i]
    }
}
