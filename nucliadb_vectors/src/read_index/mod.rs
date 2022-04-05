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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::RwLock;

use crate::graph_disk::{Disk, LockDisk};
use crate::graph_elems::{EdgeId, NodeId};
use crate::graph_index::Index;

#[derive(Default, Clone, Debug)]
pub struct ReadNodeKnowledge {
    out_edges: Vec<(EdgeId, NodeId)>,
}

#[derive(Default, Clone, Debug)]
pub struct ReadIndexLayer {
    node_record: HashMap<NodeId, ReadNodeKnowledge>,
}

impl ReadIndexLayer {
    fn disk_add(&mut self, node: NodeId) {
        self.node_record.insert(node, ReadNodeKnowledge::default());
    }
    fn disk_connect(&mut self, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.node_record
            .get_mut(&source)
            .unwrap()
            .out_edges
            .push((edge, destination));
    }
    fn new() -> ReadIndexLayer {
        ReadIndexLayer::default()
    }
}

pub struct ReadIndex {
    entry_point: Option<(NodeId, usize)>,
    layers: Vec<ReadIndexLayer>,
}

impl Debug for ReadIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_data = f.debug_struct("ReadIndex");
        debug_data.field("entry_point", &self.entry_point);
        debug_data.field("no_layers", &self.layers.len());
        for (i, layer) in self.layers.iter().enumerate() {
            let layer_id = format!("layer_{}", i);
            debug_data.field(layer_id.as_str(), &layer.node_record.len());
        }
        debug_data.finish()
    }
}

impl ReadIndex {
    pub fn new(disk: &Disk) -> ReadIndex {
        ReadIndex {
            entry_point: disk.get_entry_point(),
            layers: vec![ReadIndexLayer::new(); 70],
        }
    }
    pub fn reload(&mut self, disk: &LockDisk) {
        self.entry_point = disk.get_entry_point();
        self.layers = vec![ReadIndexLayer::new(); 70];
    }
    pub fn has_node(&self, layer: usize, node: NodeId) -> bool {
        self.layers[layer].node_record.contains_key(&node)
    }
    pub fn get_edges(&self, layer: usize, node: NodeId) -> Vec<(EdgeId, NodeId)> {
        self.layers[layer]
            .node_record
            .get(&node)
            .unwrap()
            .out_edges
            .clone()
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.entry_point
    }
    pub fn is_cached(&self, node: NodeId) -> bool {
        self.has_node(0, node)
    }

    pub fn add_node(&mut self, node: NodeId, top_layer: usize) {
        for i in 0..=top_layer {
            self.layers[i].disk_add(node);
        }
    }

    pub fn add_connexion(
        &mut self,
        layer: usize,
        source: NodeId,
        destination: NodeId,
        edge: EdgeId,
    ) {
        self.layers[layer].disk_connect(source, destination, edge);
    }
}

#[derive(Debug)]
pub struct LockReader {
    index: RwLock<ReadIndex>,
}
impl From<ReadIndex> for LockReader {
    fn from(index: ReadIndex) -> Self {
        LockReader {
            index: RwLock::new(index),
        }
    }
}
impl Index for LockReader {
    fn is_cached(&self, node: NodeId) -> bool {
        self.index.read().unwrap().is_cached(node)
    }

    fn add_node_from_disk(&self, node: NodeId, top_layer: usize) {
        self.index.write().unwrap().add_node(node, top_layer)
    }

    fn add_connexion(&self, layer: usize, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.index
            .write()
            .unwrap()
            .add_connexion(layer, source, destination, edge)
    }
}

impl LockReader {
    pub fn reload(&self, disk: &LockDisk) {
        self.index.write().unwrap().reload(disk)
    }
    pub fn get_edges(&self, layer: usize, node: NodeId) -> Vec<(EdgeId, NodeId)> {
        self.index.read().unwrap().get_edges(layer, node)
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.index.read().unwrap().get_entry_point()
    }
}
