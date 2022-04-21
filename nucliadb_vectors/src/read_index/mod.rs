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

use crate::graph_disk::{Disk, DiskEdge, DiskNode, LockDisk};
use crate::graph_elems::{Distance, GraphVector, LabelId, NodeId};

pub struct ReadIndex {
    version_number: usize,
    entry_point: Option<(NodeId, usize)>,
    nodes: HashMap<NodeId, DiskNode>,
}

impl Debug for ReadIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_data = f.debug_struct("ReadIndex");
        debug_data.field("entry_point", &self.entry_point);
        debug_data.finish()
    }
}

impl ReadIndex {
    pub fn new(disk: &Disk) -> ReadIndex {
        ReadIndex {
            version_number: disk.get_version_number(),
            entry_point: disk.get_entry_point(),
            nodes: HashMap::new(),
        }
    }
    pub fn get_version_number(&self) -> usize {
        self.version_number
    }
    pub fn distance_to(&self, i: &GraphVector, j: NodeId) -> f32 {
        let j = &self.nodes.get(&j).unwrap().node.vector;
        Distance::cosine(i, j)
    }
    pub fn has_labels(&self, node: NodeId, labels: &[LabelId]) -> bool {
        let node = &self.nodes.get(&node).unwrap().node;
        labels.iter().all(|l| node.labels.contains(l))
    }
    pub fn get_node_key(&self, node: NodeId) -> String {
        self.nodes.get(&node).unwrap().node.key.clone()
    }
    pub fn reload(&mut self, disk: &LockDisk) {
        self.version_number = disk.get_version_number();
        self.entry_point = disk.get_entry_point();
        self.nodes.clear();
    }
    pub fn get_edge(&self, layer: usize, node: NodeId, edge: usize) -> DiskEdge {
        self.nodes.get(&node).unwrap().get_layer_out(layer)[edge].clone()
    }
    pub fn no_edges(&self, layer: usize, node: NodeId) -> usize {
        self.nodes.get(&node).unwrap().get_layer_out(layer).len()
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.entry_point
    }
    pub fn is_cached(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node)
    }
    pub fn add_node(&mut self, id: NodeId, node: DiskNode) {
        self.nodes.insert(id, node);
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

impl LockReader {
    pub fn get_version_number(&self) -> usize {
        self.index.read().unwrap().get_version_number()
    }
    pub fn has_labels(&self, node: NodeId, labels: &[LabelId]) -> bool {
        self.index.read().unwrap().has_labels(node, labels)
    }
    pub fn get_node_key(&self, node: NodeId) -> String {
        self.index.read().unwrap().get_node_key(node)
    }
    pub fn distance_to(&self, i: &GraphVector, j: NodeId) -> f32 {
        self.index.read().unwrap().distance_to(i, j)
    }
    pub fn is_cached(&self, node: NodeId) -> bool {
        self.index.read().unwrap().is_cached(node)
    }
    pub fn add_node_from_disk(&self, id: NodeId, node: DiskNode) {
        self.index.write().unwrap().add_node(id, node);
    }
    pub fn reload(&self, disk: &LockDisk) {
        self.index.write().unwrap().reload(disk)
    }
    pub fn get_edge(&self, layer: usize, node: NodeId, edge: usize) -> DiskEdge {
        self.index.read().unwrap().get_edge(layer, node, edge)
    }
    pub fn no_edges(&self, layer: usize, node: NodeId) -> usize {
        self.index.read().unwrap().no_edges(layer, node)
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.index.read().unwrap().get_entry_point()
    }
}
