// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::sync::{RwLock, RwLockReadGuard};

use rustc_hash::FxHashMap;
use search::{SearchableHnsw, SearchableLayer};

use crate::VectorAddr;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryPoint {
    pub node: VectorAddr,
    pub layer: usize,
}

pub type Edge = f32;

#[derive(Default)]
pub struct RAMLayer {
    pub(super) out: FxHashMap<VectorAddr, RwLock<Vec<(VectorAddr, Edge)>>>,
}

impl RAMLayer {
    pub fn new() -> RAMLayer {
        RAMLayer::default()
    }

    pub fn add_node(&mut self, node: VectorAddr) {
        self.out.entry(node).or_default();
    }

    pub fn contains(&self, node: &VectorAddr) -> bool {
        self.out.contains_key(node)
    }

    pub fn num_out_edges(&self, node: &VectorAddr) -> usize {
        // This is called by the serialization code for all nodes and each layer, so we need to handle non-existing nodes
        self.out.get(node).map(|n| n.read().unwrap().len()).unwrap_or(0)
    }

    /// Remove any links that point to a node which is not in this layer
    /// See RAMHnsw.fix_broken_links for details
    fn fix_broken_links(&self) {
        for edges in self.out.values() {
            let mut edges = edges.write().unwrap();
            edges.retain(|e| self.out.contains_key(&e.0));
        }
    }
}

pub struct RAMHnsw {
    pub entry_point: EntryPoint,
    pub layers: Vec<RAMLayer>,
}
impl Default for RAMHnsw {
    fn default() -> Self {
        Self::new()
    }
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
                node: *self.layers.last().unwrap().out.keys().next().unwrap(),
                layer: self.layers.len() - 1,
            }
        }
    }

    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }

    /// Remove any links that point to a node which is not in this layer
    /// A bug in a previous version of this program could cause a node in layer N
    /// to link to a node in layer N-1. This breaks navigation accross layer N.
    /// This function will delete any such link from the graph.
    /// Also delete empty layers and entrypoints pointing to unexisting layers
    pub fn fix_broken_graph(&mut self) {
        // Fix links to a node not in this lauer
        for l in &self.layers[1..] {
            l.fix_broken_links();
        }

        // Delete empty layers
        while let Some(layer) = self.layers.last() {
            if layer.out.is_empty() {
                self.layers.pop();
            } else {
                break;
            }
        }

        // If entrypoint point to non-existing layer, point it to the top-most layer
        if self.entry_point.layer >= self.layers.len() {
            self.entry_point.layer = self.layers.len() - 1;
            let last_layer = self.layers.last().unwrap();
            if !last_layer.contains(&self.entry_point.node) {
                // If the current entrypoint node is not in the last layer, point to another node that is here
                self.entry_point.node = *self.layers.last().unwrap().out.keys().next().unwrap();
            }
        }
    }
}

pub struct EdgesIterator<'a>(RwLockReadGuard<'a, Vec<(VectorAddr, Edge)>>, usize);

impl<'a> Iterator for EdgesIterator<'a> {
    type Item = VectorAddr;

    fn next(&mut self) -> Option<Self::Item> {
        let it = self.0.get(self.1);
        self.1 += 1;
        it.map(|(addr, _score)| *addr)
    }
}

impl SearchableLayer for &RAMLayer {
    fn get_out_edges(&self, node: VectorAddr) -> impl Iterator<Item = VectorAddr> {
        EdgesIterator(self.out[&node].read().unwrap(), 0)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_broken_links() {
        // Create a minimal broken graph
        let layer0 = RAMLayer {
            out: [
                (VectorAddr(0), RwLock::new(vec![(VectorAddr(1), 0.5)])),
                (VectorAddr(1), RwLock::new(vec![(VectorAddr(0), 0.5)])),
            ]
            .into_iter()
            .collect(),
        };
        let layer1 = RAMLayer {
            out: [(VectorAddr(0), RwLock::new(vec![(VectorAddr(1), 0.5)]))]
                .into_iter()
                .collect(),
        };
        let mut graph = RAMHnsw::new();
        graph.layers.push(layer0);
        graph.layers.push(layer1);
        graph.fix_broken_graph();
        assert!(graph.layers[1].out[&VectorAddr(0)].read().unwrap().is_empty());
    }
}
