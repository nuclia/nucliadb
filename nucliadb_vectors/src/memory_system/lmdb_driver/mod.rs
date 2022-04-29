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
    node_db: InternalDB<String, Node>,
}

impl LMBDStorage {
    pub fn start(path: &Path) -> LMBDStorage {
        LMBDStorage {
            node_db: InternalDB::new(path, DB_NODES),
            label_db: InternalDB::new(path, DB_LABELS),
        }
    }

    #[allow(clippy::ptr_arg)]
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<(String, Node)> {
        self.node_db.get_prefixed(prefix)
    }
    #[allow(clippy::ptr_arg)]
    pub fn get_node(&self, vector: &String) -> Option<Node> {
        self.node_db.get(vector)
    }
    pub fn has_label(&self, key: &str, label: &str) -> bool {
        let path = format!("{}/{}", key, label);
        self.label_db.get(&path).is_some()
    }
    pub fn add_node(&mut self, key: String, node: Node) {
        self.node_db.atomic_insert(&key, &node);
    }
    pub fn add_label(&mut self, key: String, label: String) {
        let path = format!("{}/{}", key, label);
        self.label_db.atomic_insert(&path, &());
    }
    pub fn remove_vector(&mut self, vector: &String) {
        self.node_db.atomic_delete(vector);
        self.label_db.rmv_with_prefix(vector);
    }
    pub fn no_nodes(&self) -> usize {
        self.node_db.len()
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
    pub fn all_nodes_in(&self, prefix: &String) -> Vec<(String, Node)> {
        self.disk.read().unwrap().all_nodes_in(prefix)
    }
    pub fn get_node(&self, node: &String) -> Option<Node> {
        self.disk.read().unwrap().get_node(node)
    }
    pub fn has_label(&self, key: &str, label: &str) -> bool {
        self.disk.read().unwrap().has_label(key, label)
    }
    pub fn add_node(&self, key: String, node: Node) {
        self.disk.write().unwrap().add_node(key, node)
    }
    pub fn add_label(&mut self, key: String, label: String) {
        self.disk.write().unwrap().add_label(key, label)
    }
    pub fn remove_vector(&self, vector: &String) {
        self.disk.write().unwrap().remove_vector(vector)
    }
    pub fn no_nodes(&self) -> usize {
        self.disk.read().unwrap().no_nodes()
    }
    pub fn no_labels(&self) -> usize {
        self.disk.read().unwrap().no_labels()
    }
}
