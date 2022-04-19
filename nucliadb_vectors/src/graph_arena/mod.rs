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
#![allow(unused)]
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::graph_disk::{Disk, LockDisk};
use crate::graph_elems::*;
#[derive(Clone, Debug, Default)]
pub struct Arena {
    version_number: usize,
    fresh_node_id: NodeId,
    fresh_edge_id: EdgeId,
    fresh_label_id: LabelId,
    nodes: HashMap<NodeId, Node>,
    edges: HashMap<EdgeId, Edge>,
    deleted_edges: Vec<EdgeId>,
    deleted_labels: Vec<LabelId>,
}

impl Arena {
    pub fn new() -> Arena {
        Arena::default()
    }
    pub fn with_capacity(no_nodes: usize, no_edges: usize) -> Arena {
        Arena {
            nodes: HashMap::with_capacity(no_nodes),
            edges: HashMap::with_capacity(no_edges * no_nodes),
            ..Arena::default()
        }
    }
    pub fn from_disk(disk: &Disk) -> Arena {
        Arena {
            version_number: disk.get_version_number(),
            fresh_label_id: disk.get_fresh_label(),
            fresh_node_id: disk.get_fresh_node(),
            fresh_edge_id: disk.get_fresh_edge(),
            deleted_edges: disk.get_deleted_edges(),
            ..Arena::default()
        }
    }
    pub fn reload(&mut self, disk: &LockDisk) {
        self.version_number = disk.get_version_number();
        self.fresh_label_id = disk.get_fresh_label();
        self.fresh_node_id = disk.get_fresh_node();
        self.fresh_edge_id = disk.get_fresh_edge();
        self.deleted_edges = disk.get_deleted_edges();
        self.nodes = HashMap::with_capacity(self.nodes.capacity());
        self.edges = HashMap::with_capacity(self.edges.capacity());
    }
    pub fn dump_into_disk(&mut self, disk: &LockDisk) {
        disk.log_fresh_label(self.fresh_label_id);
        disk.log_fresh_node(self.fresh_node_id);
        disk.log_fresh_edge(self.fresh_edge_id);
        disk.log_deleted_labels(&self.deleted_labels);
        disk.log_deleted_edges(&self.deleted_edges);
    }

    pub fn get_version_number(&self) -> usize {
        self.version_number
    }
    pub fn insert_node(&mut self, node: Node) -> NodeId {
        let index = self.fresh_node_id.fresh();
        self.nodes.insert(index, node);
        index
    }
    pub fn insert_edge(&mut self, edge: Edge) -> EdgeId {
        match self.deleted_edges.pop() {
            Some(id) => {
                self.edges.insert(id, edge);
                id
            }
            None => {
                let index = self.fresh_edge_id.fresh();
                self.edges.insert(index, edge);
                index
            }
        }
    }
    pub fn free_label(&mut self) -> LabelId {
        match self.deleted_labels.pop() {
            Some(id) => id,
            None => self.fresh_label_id.fresh(),
        }
    }
    pub fn load_node_from_disk(&mut self, id: NodeId, node: Node) -> bool {
        if let Entry::Vacant(e) = self.nodes.entry(id) {
            e.insert(node);
            true
        } else {
            false
        }
    }
    pub fn load_edge_from_disk(&mut self, id: EdgeId, edge: Edge) -> bool {
        if let Entry::Vacant(e) = self.edges.entry(id) {
            e.insert(edge);
            true
        } else {
            false
        }
    }
    pub fn delete_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
    }
    pub fn delete_edge(&mut self, edge_id: EdgeId) {
        self.deleted_edges.push(edge_id);
        self.edges.remove(&edge_id);
    }
    pub fn delete_label(&mut self, label_id: LabelId) {
        self.deleted_labels.push(label_id)
    }
    pub fn has_node(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node)
    }
    pub fn has_edge(&self, edge: EdgeId) -> bool {
        self.edges.contains_key(&edge)
    }
    pub fn is_empty(&self) -> bool {
        self.no_nodes() == 0
    }
    pub fn no_nodes(&self) -> usize {
        self.nodes.len()
    }
    pub fn get_edge(&self, id: EdgeId) -> &Edge {
        self.edges.get(&id).unwrap()
    }
    pub fn get_node(&self, id: NodeId) -> &Node {
        self.nodes.get(&id).unwrap()
    }
}

#[derive(Debug, Default)]
pub struct LockArena {
    arena: RwLock<Arena>,
}

impl From<Arena> for LockArena {
    fn from(arena: Arena) -> Self {
        LockArena {
            arena: RwLock::new(arena),
        }
    }
}

impl LockArena {
    pub fn reload(&self, disk: &LockDisk) {
        self.arena.write().unwrap().reload(disk)
    }
    pub fn dump_into_disk(&self, disk: &LockDisk) {
        self.arena.write().unwrap().dump_into_disk(disk)
    }

    pub fn get_version_number(&self) -> usize {
        self.arena.read().unwrap().get_version_number()
    }
    pub fn insert_node(&self, node: Node) -> NodeId {
        self.arena.write().unwrap().insert_node(node)
    }
    pub fn insert_edge(&self, edge: Edge) -> EdgeId {
        self.arena.write().unwrap().insert_edge(edge)
    }
    pub fn free_label(&self) -> LabelId {
        self.arena.write().unwrap().free_label()
    }
    pub fn load_node_from_disk(&self, id: NodeId, node: Node) -> bool {
        self.arena.write().unwrap().load_node_from_disk(id, node)
    }
    pub fn load_edge_from_disk(&self, id: EdgeId, edge: Edge) -> bool {
        self.arena.write().unwrap().load_edge_from_disk(id, edge)
    }
    pub fn delete_node(&self, node_id: NodeId) {
        self.arena.write().unwrap().delete_node(node_id)
    }
    pub fn delete_edge(&self, edge_id: EdgeId) {
        self.arena.write().unwrap().delete_edge(edge_id)
    }
    pub fn delete_label(&self, label_id: LabelId) {
        self.arena.write().unwrap().delete_label(label_id)
    }
    pub fn has_node(&self, node: NodeId) -> bool {
        self.arena.read().unwrap().has_node(node)
    }
    pub fn has_edge(&self, edge: EdgeId) -> bool {
        self.arena.read().unwrap().has_edge(edge)
    }
    pub fn is_empty(&self) -> bool {
        self.arena.read().unwrap().is_empty()
    }
    pub fn no_nodes(&self) -> usize {
        self.arena.read().unwrap().no_nodes()
    }
    pub fn get_edge(&self, id: EdgeId) -> Edge {
        self.arena.read().unwrap().get_edge(id).clone()
    }
    pub fn get_node(&self, id: NodeId) -> Node {
        self.arena.read().unwrap().get_node(id).clone()
    }
}
