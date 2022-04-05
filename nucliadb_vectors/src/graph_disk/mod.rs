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

mod database;
mod db_elems;

use std::path::Path;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::graph_disk::database::InternalDB;
use crate::graph_elems::*;

const DB_NODES: &str = "NODES";
const DB_LABELS: &str = "LABELS";
const DB_LOG: &str = "LOG";
const DB_NODE_INVERSE: &str = "NODE_INVERSE";
const DB_LABEL_INVERSE: &str = "LABEL_INVERSE";

#[derive(Serialize, Deserialize)]
pub struct DiskNode {
    pub node: Node,
    // neighbours[i].0 = out edges for node in layer i
    // neighbours[i].1 = in edges for node in layer i
    pub neighbours: Vec<(Vec<DiskEdge>, Vec<DiskEdge>)>,
}

#[derive(Serialize, Deserialize)]
pub struct DiskEdge {
    pub my_id: EdgeId,
    pub edge: Edge,
    pub goes_to: NodeId,
    pub from: NodeId,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum LogField {
    VersionNumber,
    FreshNode,
    FreshEdge,
    FreshLabel,
    DeletedNodes,
    DeletedEdges,
    DeletedLabels,
    EntryPoint,
}

pub struct Disk {
    node_db: InternalDB<NodeId, DiskNode>,
    label_db: InternalDB<LabelId, Label>,
    node_inverse: InternalDB<String, NodeId>,
    label_inverse: InternalDB<String, LabelId>,
    log: InternalDB<LogField, Vec<u8>>,
}

impl Disk {
    pub fn start(path: &Path) -> Disk {
        let mut disk = Disk {
            node_db: InternalDB::new(path, DB_NODES),
            label_db: InternalDB::new(path, DB_LABELS),
            node_inverse: InternalDB::new(path, DB_NODE_INVERSE),
            label_inverse: InternalDB::new(path, DB_LABEL_INVERSE),
            log: InternalDB::new(path, DB_LOG),
        };
        if disk.log.is_empty() {
            let default_fresh_node = bincode::serialize(&NodeId::new()).unwrap();
            disk.log
                .atomic_insert(&LogField::FreshNode, &default_fresh_node);
            let default_fresh_edge = bincode::serialize(&EdgeId::new()).unwrap();
            disk.log
                .atomic_insert(&LogField::FreshEdge, &default_fresh_edge);
            let default_fresh_label = bincode::serialize(&LabelId::new()).unwrap();
            disk.log
                .atomic_insert(&LogField::FreshLabel, &default_fresh_label);
            let no_nodes_deleted = bincode::serialize(&(Vec::new() as Vec<NodeId>)).unwrap();
            disk.log
                .atomic_insert(&LogField::DeletedNodes, &no_nodes_deleted);
            let no_edges_deleted = bincode::serialize(&(Vec::new() as Vec<EdgeId>)).unwrap();
            disk.log
                .atomic_insert(&LogField::DeletedEdges, &no_edges_deleted);
            let no_labels_deleted = bincode::serialize(&(Vec::new() as Vec<LabelId>)).unwrap();
            disk.log
                .atomic_insert(&LogField::DeletedLabels, &no_labels_deleted);
            let no_entry_point = bincode::serialize(&(None as Option<(NodeId, usize)>)).unwrap();
            disk.log
                .atomic_insert(&LogField::EntryPoint, &no_entry_point);
            let version_number = bincode::serialize(&0_usize).unwrap();
            disk.log
                .atomic_insert(&LogField::VersionNumber, &version_number);
        }
        disk
    }
    #[allow(clippy::ptr_arg)]
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<String> {
        self.node_inverse.get_prefixed(prefix)
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        let bytes = self.log.get(&LogField::EntryPoint).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_node_id(&self, vector: &String) -> Option<NodeId> {
        self.node_inverse.get(vector)
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_label_id(&self, label: &String) -> Option<LabelId> {
        self.label_inverse.get(label)
    }
    pub fn get_node(&self, node: NodeId) -> DiskNode {
        self.node_db.get(&node).unwrap()
    }
    pub fn get_label(&self, label: LabelId) -> Label {
        self.label_db.get(&label).unwrap()
    }
    pub fn get_version_number(&self) -> usize {
        let bytes = self.log.get(&LogField::VersionNumber).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn get_fresh_node(&self) -> NodeId {
        let bytes = self.log.get(&LogField::FreshNode).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn get_fresh_edge(&self) -> EdgeId {
        let bytes = self.log.get(&LogField::FreshEdge).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn get_fresh_label(&self) -> LabelId {
        let bytes = self.log.get(&LogField::FreshLabel).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn get_deleted_nodes(&self) -> Vec<NodeId> {
        let bytes = self.log.get(&LogField::DeletedNodes).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn get_deleted_edges(&self) -> Vec<EdgeId> {
        let bytes = self.log.get(&LogField::DeletedEdges).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
    pub fn add_node(&mut self, node_id: &NodeId, node: &DiskNode) {
        self.node_db.atomic_insert(node_id, node);
    }
    pub fn add_label(&mut self, label: &Label) {
        self.label_inverse.atomic_insert(&label.value, &label.my_id);
        self.label_db.atomic_insert(&label.my_id, label);
    }
    pub fn grow_label(&mut self, label_id: LabelId) {
        let mut label = self.get_label(label_id);
        label.reached_by += 1;
        self.add_label(&label);
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_node_id(&mut self, vector: &String, node_id: NodeId) {
        self.node_inverse.atomic_insert(vector, &node_id);
    }
    #[allow(clippy::ptr_arg)]
    pub fn remove_vector(&mut self, vector: &String) {
        let id = self.node_inverse.get(vector).unwrap();
        self.node_inverse.atomic_delete(vector);
        self.node_db.atomic_delete(&id);
    }
    pub fn remove_label(&mut self, id: LabelId) -> usize {
        let mut label = self.label_db.get(&id).unwrap();
        label.reached_by -= 1;
        self.label_db.atomic_insert(&id, &label);
        label.reached_by
    }
    pub fn update_version_number(&mut self) {
        let version = self.get_version_number();
        let bytes = bincode::serialize(&(version + 1)).unwrap();
        self.log.atomic_insert(&LogField::VersionNumber, &bytes);
    }
    pub fn log_fresh_node(&mut self, node: NodeId) {
        let bytes = bincode::serialize(&node).unwrap();
        self.log.atomic_insert(&LogField::FreshNode, &bytes);
    }
    pub fn log_fresh_edge(&mut self, edge: EdgeId) {
        let bytes = bincode::serialize(&edge).unwrap();
        self.log.atomic_insert(&LogField::FreshEdge, &bytes);
    }
    pub fn log_fresh_label(&mut self, label: LabelId) {
        let bytes = bincode::serialize(&label).unwrap();
        self.log.atomic_insert(&LogField::FreshLabel, &bytes);
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_nodes(&mut self, nodes: &Vec<NodeId>) {
        let bytes = bincode::serialize(nodes).unwrap();
        self.log.atomic_insert(&LogField::DeletedNodes, &bytes);
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_edges(&mut self, edges: &Vec<EdgeId>) {
        let bytes = bincode::serialize(edges).unwrap();
        self.log.atomic_insert(&LogField::DeletedEdges, &bytes);
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_labels(&mut self, labels: &Vec<LabelId>) {
        let bytes = bincode::serialize(labels).unwrap();
        self.log.atomic_insert(&LogField::DeletedLabels, &bytes);
    }
    pub fn log_entry_point(&mut self, ep: &Option<(NodeId, usize)>) {
        let bytes = bincode::serialize(ep).unwrap();
        self.log.atomic_insert(&LogField::EntryPoint, &bytes);
    }
    pub fn no_nodes(&self) -> usize {
        self.node_inverse.len()
    }
    pub fn no_labels(&self) -> usize {
        self.label_inverse.len()
    }
}

pub struct LockDisk {
    disk: RwLock<Disk>,
}
impl From<Disk> for LockDisk {
    fn from(disk: Disk) -> Self {
        LockDisk {
            disk: RwLock::new(disk),
        }
    }
}

impl LockDisk {
    #[allow(clippy::ptr_arg)]
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<String> {
        self.disk.read().unwrap().all_nodes_in(prefix)
    }
    pub fn get_entry_point(&self) -> Option<(NodeId, usize)> {
        self.disk.read().unwrap().get_entry_point()
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_node_id(&self, vector: &String) -> Option<NodeId> {
        self.disk.read().unwrap().get_node_id(vector)
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_label_id(&self, label: &String) -> Option<LabelId> {
        self.disk.read().unwrap().get_label_id(label)
    }
    pub fn get_node(&self, node: NodeId) -> DiskNode {
        self.disk.read().unwrap().get_node(node)
    }
    pub fn get_label(&self, label: LabelId) -> Label {
        self.disk.read().unwrap().get_label(label)
    }
    pub fn get_version_number(&self) -> usize {
        self.disk.read().unwrap().get_version_number()
    }
    pub fn get_fresh_node(&self) -> NodeId {
        self.disk.read().unwrap().get_fresh_node()
    }
    pub fn get_fresh_edge(&self) -> EdgeId {
        self.disk.read().unwrap().get_fresh_edge()
    }
    pub fn get_fresh_label(&self) -> LabelId {
        self.disk.read().unwrap().get_fresh_label()
    }
    pub fn get_deleted_nodes(&self) -> Vec<NodeId> {
        self.disk.read().unwrap().get_deleted_nodes()
    }
    pub fn get_deleted_edges(&self) -> Vec<EdgeId> {
        self.disk.read().unwrap().get_deleted_edges()
    }
    pub fn add_node(&self, node_id: &NodeId, node: &DiskNode) {
        self.disk.write().unwrap().add_node(node_id, node)
    }
    pub fn add_label(&self, label: &Label) {
        self.disk.write().unwrap().add_label(label)
    }
    pub fn grow_label(&self, label_id: LabelId) {
        self.disk.write().unwrap().grow_label(label_id)
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_node_id(&self, vector: &String, node_id: NodeId) {
        self.disk.write().unwrap().log_node_id(vector, node_id)
    }
    #[allow(clippy::ptr_arg)]
    pub fn remove_vector(&self, vector: &String) {
        self.disk.write().unwrap().remove_vector(vector)
    }
    pub fn remove_label(&self, id: LabelId) -> usize {
        self.disk.write().unwrap().remove_label(id)
    }
    pub fn update_version_number(&self) {
        self.disk.write().unwrap().update_version_number()
    }
    pub fn log_fresh_node(&self, node: NodeId) {
        self.disk.write().unwrap().log_fresh_node(node)
    }
    pub fn log_fresh_edge(&self, edge: EdgeId) {
        self.disk.write().unwrap().log_fresh_edge(edge)
    }
    pub fn log_fresh_label(&self, label: LabelId) {
        self.disk.write().unwrap().log_fresh_label(label)
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_nodes(&self, nodes: &Vec<NodeId>) {
        self.disk.write().unwrap().log_deleted_nodes(nodes)
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_edges(&self, edges: &Vec<EdgeId>) {
        self.disk.write().unwrap().log_deleted_edges(edges)
    }
    #[allow(clippy::ptr_arg)]
    pub fn log_deleted_labels(&self, labels: &Vec<LabelId>) {
        self.disk.write().unwrap().log_deleted_labels(labels)
    }
    pub fn log_entry_point(&self, ep: &Option<(NodeId, usize)>) {
        self.disk.write().unwrap().log_entry_point(ep)
    }
    pub fn no_nodes(&self) -> usize {
        self.disk.read().unwrap().no_nodes()
    }
    pub fn no_labels(&self) -> usize {
        self.disk.read().unwrap().no_labels()
    }
}
