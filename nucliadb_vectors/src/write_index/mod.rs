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
#[cfg(test)]
#[allow(unused)]
pub mod test_utils;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::RwLock;

use crate::graph_disk::{Disk, LockDisk};
use crate::graph_elems::{EdgeId, NodeId};
use crate::graph_index::Index;

#[derive(Clone, Default, Debug)]
pub struct WriteNodeKnowledge {
    is_partial: bool,
    stack_position: usize,
    pub out_edges: HashMap<NodeId, EdgeId>,
    pub in_edges: HashMap<NodeId, EdgeId>,
}
impl WriteNodeKnowledge {
    fn in_position(stack_position: usize) -> WriteNodeKnowledge {
        WriteNodeKnowledge {
            stack_position,
            ..WriteNodeKnowledge::default()
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct WriteIndexLayer {
    stack_gaps: Vec<usize>,
    node_stack: Vec<Option<NodeId>>,
    node_record: HashMap<NodeId, WriteNodeKnowledge>,
}

impl WriteIndexLayer {
    fn disk_connect(&mut self, source: NodeId, destination: NodeId, edge: EdgeId) {
        if !self.node_record.contains_key(&destination) {
            self.add_node(destination);
            self.node_record.get_mut(&destination).unwrap().is_partial = true;
        }
        if !self.node_record.contains_key(&source) {
            self.add_node(source);
            self.node_record.get_mut(&source).unwrap().is_partial = true;
        }
        self.connect(source, destination, edge);
    }
    fn disk_add(&mut self, node_id: NodeId) {
        match self.node_record.get_mut(&node_id) {
            Some(n) => {
                n.is_partial = false;
            }
            None => {
                self.add_node(node_id);
            }
        }
    }
    fn has_total_node(&self, node_id: NodeId) -> bool {
        match self.node_record.get(&node_id) {
            Some(n) => !n.is_partial,
            None => false,
        }
    }
    pub fn new() -> WriteIndexLayer {
        WriteIndexLayer::default()
    }
    pub fn with_ep(ep: NodeId) -> WriteIndexLayer {
        WriteIndexLayer {
            node_stack: vec![Some(ep)],
            node_record: HashMap::from([(ep, WriteNodeKnowledge::in_position(0))]),
            ..WriteIndexLayer::new()
        }
    }
    fn add_node(&mut self, node: NodeId) {
        let mut knowledge = WriteNodeKnowledge::default();
        if let Some(free) = self.stack_gaps.pop() {
            self.node_stack[free] = Some(node);
            knowledge.stack_position = free
        } else {
            knowledge.stack_position = self.node_stack.len();
            self.node_stack.push(Some(node));
        }
        self.node_record.insert(node, knowledge);
    }
    fn connect(&mut self, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.node_record
            .get_mut(&source)
            .unwrap()
            .out_edges
            .insert(destination, edge);
        self.node_record
            .get_mut(&destination)
            .unwrap()
            .in_edges
            .insert(source, edge);
    }
    fn disconnect(&mut self, source: NodeId, destination: NodeId) {
        self.node_record
            .get_mut(&source)
            .unwrap()
            .out_edges
            .remove(&destination);
        self.node_record
            .get_mut(&destination)
            .unwrap()
            .in_edges
            .remove(&source);
    }
    fn erase(&mut self, x: NodeId) {
        let knowledge = self.node_record.remove(&x).unwrap();
        self.node_stack[knowledge.stack_position] = None;
        self.stack_gaps.push(knowledge.stack_position);
    }
    fn first_node(&self) -> Option<NodeId> {
        let mut index = 0;
        loop {
            if index == self.node_stack.len() {
                break None;
            } else if self.node_stack[index].is_some() {
                break self.node_stack[index];
            } else {
                index += 1;
            }
        }
    }
    fn out_edges(&self, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.node_record.get(&node).unwrap().out_edges.clone()
    }
    fn in_edges(&self, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.node_record.get(&node).unwrap().in_edges.clone()
    }
}

pub struct WriteIndex {
    max_layers: usize,
    entry_point: Option<(NodeId, usize)>,
    top_layer: HashMap<NodeId, usize>,
    layers: Vec<WriteIndexLayer>,
}

impl Debug for WriteIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_data = f.debug_struct("WriteIndex");
        debug_data.field("entry_point", &self.entry_point);
        debug_data.field("no_layers", &self.layers.len());
        for (i, layer) in self.layers.iter().enumerate() {
            let layer_id = format!("layer_{}", i);
            debug_data.field(layer_id.as_str(), &layer.node_record.len());
        }
        debug_data.finish()
    }
}

impl WriteIndex {
    pub fn new(disk: &Disk) -> WriteIndex {
        WriteIndex {
            entry_point: disk.get_entry_point(),
            max_layers: 4,
            top_layer: HashMap::new(),
            layers: vec![WriteIndexLayer::new(); 70],
        }
    }
    pub fn with_params(disk: &Disk, max_layers: usize) -> WriteIndex {
        WriteIndex {
            max_layers,
            ..WriteIndex::new(disk)
        }
    }
    pub fn replace_layer(&mut self, at: usize, layer: WriteIndexLayer) {
        self.layers[at] = layer;
    }
    pub fn set_entry_point(&mut self, node: NodeId, layer: usize) {
        self.entry_point = Some((node, layer));
    }
    pub fn set_top_layer(&mut self, node: NodeId, layer: usize) {
        self.top_layer.insert(node, layer);
    }
    pub fn connect(&mut self, layer: usize, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.layers[layer].connect(source, destination, edge);
    }
    pub fn disconnect(&mut self, layer: usize, source: NodeId, destination: NodeId) {
        self.layers[layer].disconnect(source, destination);
    }
    pub fn erase(&mut self, x: NodeId) {
        // remove x from all layers
        let x_layer = *self.top_layer.get(&x).unwrap();
        self.layers[0..=x_layer].iter_mut().for_each(|l| l.erase(x));
        // remove x from top layer record
        let mut top_layer = self.top_layer.remove(&x).unwrap();
        // update entry point
        if self.entry_point.map_or(false, |(v, _)| v == x) {
            self.entry_point = None;
            loop {
                self.entry_point = self.layers[top_layer]
                    .first_node()
                    .map(|node| (node, top_layer));
                if let Some((n, _)) = self.entry_point {
                    assert!(n != x);
                }
                if top_layer == 0 || self.entry_point.is_some() {
                    break;
                } else {
                    top_layer -= 1;
                }
            }
        }
    }
    pub fn reload(&mut self, disk: &LockDisk) {
        self.entry_point = disk.get_entry_point();
        self.layers = vec![WriteIndexLayer::new(); 70];
        self.top_layer.clear();
    }
    pub fn out_edges(&self, layer: usize, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.layers[layer].out_edges(node)
    }
    pub fn in_edges(&self, layer: usize, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.layers[layer].in_edges(node)
    }
    pub fn top_layers(&self) -> HashMap<NodeId, usize> {
        self.top_layer.clone()
    }
    pub fn get_top_layer(&self, node: NodeId) -> usize {
        *self.top_layer.get(&node).unwrap()
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.entry_point
    }
    pub fn max_layers(&self) -> usize {
        self.max_layers
    }
    pub fn is_cached(&self, node: NodeId) -> bool {
        self.layers[0].has_total_node(node)
    }
    pub fn add_node(&mut self, layer: usize, node: NodeId) {
        self.layers[layer].add_node(node);
    }
    pub fn disk_add_node(&mut self, node: NodeId, top_layer: usize) {
        self.top_layer.insert(node, top_layer);
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
pub struct LockWriter {
    index: RwLock<WriteIndex>,
}
impl Index for LockWriter {
    fn is_cached(&self, node: NodeId) -> bool {
        self.index.read().unwrap().is_cached(node)
    }

    fn add_node_from_disk(&self, node: NodeId, top_layer: usize) {
        self.index.write().unwrap().disk_add_node(node, top_layer)
    }

    fn add_connexion(&self, layer: usize, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.index
            .write()
            .unwrap()
            .add_connexion(layer, source, destination, edge)
    }
}

impl From<WriteIndex> for LockWriter {
    fn from(index: WriteIndex) -> Self {
        LockWriter {
            index: RwLock::new(index),
        }
    }
}

impl LockWriter {
    pub fn add_node(&self, node: NodeId, top_layer: usize) {
        self.index.write().unwrap().add_node(top_layer, node)
    }
    pub fn replace_layer(&self, at: usize, layer: WriteIndexLayer) {
        self.index.write().unwrap().replace_layer(at, layer)
    }
    pub fn set_entry_point(&self, node: NodeId, layer: usize) {
        self.index.write().unwrap().set_entry_point(node, layer)
    }
    pub fn set_top_layer(&self, node: NodeId, layer: usize) {
        self.index.write().unwrap().set_top_layer(node, layer)
    }
    pub fn connect(&self, layer: usize, source: NodeId, destination: NodeId, edge: EdgeId) {
        self.index
            .write()
            .unwrap()
            .connect(layer, source, destination, edge)
    }
    pub fn disconnect(&self, layer: usize, source: NodeId, destination: NodeId) {
        self.index
            .write()
            .unwrap()
            .disconnect(layer, source, destination)
    }
    pub fn erase(&self, x: NodeId) {
        self.index.write().unwrap().erase(x)
    }
    pub fn reload(&self, disk: &LockDisk) {
        self.index.write().unwrap().reload(disk)
    }
    pub fn out_edges(&self, layer: usize, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.index.read().unwrap().out_edges(layer, node)
    }
    pub fn in_edges(&self, layer: usize, node: NodeId) -> HashMap<NodeId, EdgeId> {
        self.index.read().unwrap().in_edges(layer, node)
    }
    pub fn top_layers(&self) -> HashMap<NodeId, usize> {
        self.index.read().unwrap().top_layers()
    }
    pub fn get_top_layer(&self, node: NodeId) -> usize {
        self.index.read().unwrap().get_top_layer(node)
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.index.read().unwrap().get_entry_point()
    }
    pub fn max_layers(&self) -> usize {
        self.index.read().unwrap().max_layers()
    }
}
