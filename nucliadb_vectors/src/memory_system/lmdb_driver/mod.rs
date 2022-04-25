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

use std::path::Path;
use std::sync::RwLock;

use crate::memory_system::elements::*;
use crate::memory_system::lmdb_driver::database::InternalDB;
const DB_NODES: &str = "NODES";
const DB_LABELS: &str = "LABELS";
const DB_LOG: &str = "LOG";
const DB_NODE_INVERSE: &str = "NODE_INVERSE";
const DB_LAYER_IN: &str = "LAYER_IN";
const DB_LAYER_OUT: &str = "LAYER_OUT";

#[derive(Copy, Clone)]
pub enum LogField {
    VersionNumber = 0,
    FreshNode,
    FreshEdge,
    EntryPoint,
}

impl ByteRpr for LogField {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
    fn deserialize(bytes: &[u8]) -> Self {
        use LogField::*;
        match bytes[0] {
            0 => VersionNumber,
            1 => FreshNode,
            2 => FreshEdge,
            3 => EntryPoint,
            _ => panic!("Unknown LogField: {bytes:?}"),
        }
    }
}

impl FixedByteLen for LogField {
    fn segment_len() -> usize {
        1
    }
}

pub struct LMBDStorage {
    label_db: InternalDB<String, ()>,
    node_db: InternalDB<NodeID, Node>,
    node_inverse: InternalDB<String, NodeID>,
    layer_db_out: InternalDB<u64, GraphLayer>,
    layer_db_in: InternalDB<u64, GraphLayer>,
    log: InternalDB<LogField, Vec<u8>>,
}

impl LMBDStorage {
    pub fn start(path: &Path) -> LMBDStorage {
        LMBDStorage {
            node_db: InternalDB::new(path, DB_NODES),
            label_db: InternalDB::new(path, DB_LABELS),
            node_inverse: InternalDB::new(path, DB_NODE_INVERSE),
            layer_db_out: InternalDB::new(path, DB_LAYER_OUT),
            layer_db_in: InternalDB::new(path, DB_LAYER_IN),
            log: InternalDB::new(path, DB_LOG),
        }
    }
    pub fn get_layer_out(&self, layer: u64) -> Option<GraphLayer> {
        self.layer_db_out.get(&layer)
    }
    pub fn get_layer_in(&self, layer: u64) -> Option<GraphLayer> {
        self.layer_db_in.get(&layer)
    }
    #[allow(clippy::ptr_arg)]
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<(String, NodeID)> {
        self.node_inverse.get_prefixed(prefix)
    }
    pub fn get_entry_point(&self) -> Option<EntryPoint> {
        self.log
            .get(&LogField::EntryPoint)
            .map(|b| EntryPoint::deserialize(&b))
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_node_id(&self, vector: &String) -> Option<NodeID> {
        self.node_inverse.get(vector)
    }
    pub fn get_node(&self, node: NodeID) -> Option<Node> {
        self.node_db.get(&node)
    }
    pub fn has_label(&self, node: NodeID, label: &str) -> bool {
        let path = format!("{}/{}", node, label);
        self.label_db.get(&path).is_some()
    }
    pub fn get_version_number(&self) -> Option<u128> {
        self.log
            .get(&LogField::VersionNumber)
            .map(|b| u128::deserialize(&b))
    }
    pub fn get_node_generator(&self) -> NodeIDGen {
        self.log
            .get(&LogField::FreshNode)
            .map(|b| NodeID::deserialize(&b))
            .map(NodeIDGen::from_seed)
            .unwrap_or_default()
    }
    pub fn add_node(&mut self, key: String, node_id: NodeID, node: Node) {
        self.node_db.atomic_insert(&node_id, &node);
        self.node_inverse.atomic_insert(&key, &node_id);
    }
    pub fn add_label(&mut self, node: NodeID, label: String) {
        let path = format!("{}/{}", node, label);
        self.label_db.atomic_insert(&path, &());
    }
    pub fn remove_vector(&mut self, vector: String) {
        let id = self.node_inverse.get(&vector).unwrap();
        self.node_inverse.atomic_delete(&vector);
        self.node_db.atomic_delete(&id);
        self.label_db.rmv_with_prefix(&id.to_string());
    }
    pub fn update_version_number(&mut self) {
        let version = self.get_version_number().unwrap_or_default();
        let bytes = u128::serialize(&(version + 1));
        self.log.atomic_insert(&LogField::VersionNumber, &bytes);
    }
    pub fn log_fresh_node(&mut self, node: NodeID) {
        self.log
            .atomic_insert(&LogField::FreshNode, &node.serialize());
    }
    pub fn log_entry_point(&mut self, ep: EntryPoint) {
        self.log
            .atomic_insert(&LogField::EntryPoint, &ep.serialize());
    }
    pub fn no_nodes(&self) -> usize {
        self.node_inverse.len()
    }
    pub fn no_labels(&self) -> usize {
        self.label_db.len()
    }
}

pub struct LockLMDB {
    disk: RwLock<LMBDStorage>,
}
impl From<LMBDStorage> for LockLMDB {
    fn from(disk: LMBDStorage) -> Self {
        LockLMDB {
            disk: RwLock::new(disk),
        }
    }
}

impl LockLMDB {
    #[allow(clippy::ptr_arg)]
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<(String, NodeID)> {
        self.disk.read().unwrap().all_nodes_in(prefix)
    }
    pub fn get_entry_point(&self) -> Option<EntryPoint> {
        self.disk.read().unwrap().get_entry_point()
    }
    pub fn get_node_generator(&self) -> NodeIDGen {
        self.disk.read().unwrap().get_node_generator()
    }
    pub fn get_layer_out(&self, layer: u64) -> Option<GraphLayer> {
        self.disk.read().unwrap().get_layer_out(layer)
    }
    pub fn get_layer_in(&self, layer: u64) -> Option<GraphLayer> {
        self.disk.read().unwrap().get_layer_in(layer)
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_node_id(&self, vector: &String) -> Option<NodeID> {
        self.disk.read().unwrap().get_node_id(vector)
    }
    pub fn get_node(&self, node: NodeID) -> Option<Node> {
        self.disk.read().unwrap().get_node(node)
    }
    pub fn has_label(&self, node: NodeID, label: &str) -> bool {
        self.disk.read().unwrap().has_label(node, label)
    }
    pub fn get_version_number(&self) -> Option<u128> {
        self.disk.read().unwrap().get_version_number()
    }
    pub fn add_node(&self, key: String, node_id: NodeID, node: Node) {
        self.disk.write().unwrap().add_node(key, node_id, node)
    }
    pub fn add_label(&mut self, node: NodeID, label: String) {
        self.disk.write().unwrap().add_label(node, label)
    }
    pub fn remove_vector(&self, vector: String) {
        self.disk.write().unwrap().remove_vector(vector)
    }
    pub fn update_version_number(&self) {
        self.disk.write().unwrap().update_version_number()
    }
    pub fn log_fresh_node(&self, node: NodeID) {
        self.disk.write().unwrap().log_fresh_node(node)
    }
    pub fn log_entry_point(&self, ep: EntryPoint) {
        self.disk.write().unwrap().log_entry_point(ep)
    }
    pub fn no_nodes(&self) -> usize {
        self.disk.read().unwrap().no_nodes()
    }
    pub fn no_labels(&self) -> usize {
        self.disk.read().unwrap().no_labels()
    }
}
