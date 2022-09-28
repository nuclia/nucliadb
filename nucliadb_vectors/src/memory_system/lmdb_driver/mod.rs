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

use std::path::Path;

use heed::flags::Flags;
use heed::types::{ByteSlice, Str, Unit};
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::memory_system::elements::*;

const LMDB_ENV: &str = "ENV_lmdb";
const DB_NODES: &str = "NODES_lmdb";
const DB_NODES_INV: &str = "NODES_INV_lmdb";
const DB_LABELS: &str = "LABELS_lmdb";
const DB_LAYERS_OUT: &str = "LAYERS_OUT_ldmd";
const DB_LAYERS_IN: &str = "LAYERS_IN_lmdb";
const DB_LOG: &str = "LOG_lmdb";
const DB_DELETED: &str = "DELETED_lmdb";
const STAMP: &str = "stamp.nuclia";
const MAX_DBS: u32 = 3000;
#[cfg(target_pointer_width = "64")]
const MAP_SIZE: usize = 1048576 * 100000;
#[cfg(target_pointer_width = "32")]
const MAP_SIZE: usize = 1048576;

pub struct LMBDStorage {
    env: Env,
    // (String, ())
    label_db: Database<Str, Unit>,
    // (String, Node)
    node_db: Database<Str, ByteSlice>,
    // (Node, String)
    node_inv_db: Database<ByteSlice, Str>,
    // (u64, GraphLayer)
    layer_out_db: Database<ByteSlice, ByteSlice>,
    // (u64, GraphLayer)
    layer_in_db: Database<ByteSlice, ByteSlice>,
    // (LogField, serialized data)
    log: Database<ByteSlice, ByteSlice>,
    // (u128, Vec<Node>)
    deleted_log: Database<ByteSlice, ByteSlice>,
}

impl LMBDStorage {
    pub fn create(path: &Path) -> LMBDStorage {
        if !path.join(STAMP).exists() {
            let env_path = path.join(LMDB_ENV);
            std::fs::create_dir_all(&env_path).unwrap();
            let mut env_builder = EnvOpenOptions::new();
            env_builder.max_dbs(MAX_DBS);
            env_builder.map_size(MAP_SIZE);
            unsafe {
                env_builder.flag(Flags::MdbNoLock);
            }
            let env = env_builder.open(&env_path).unwrap();
            let label_db = env.create_database(Some(DB_LABELS)).unwrap();
            let node_db = env.create_database(Some(DB_NODES)).unwrap();
            let node_inv_db = env.create_database(Some(DB_NODES_INV)).unwrap();
            let layer_out_db = env.create_database(Some(DB_LAYERS_OUT)).unwrap();
            let layer_in_db = env.create_database(Some(DB_LAYERS_IN)).unwrap();
            let log = env.create_database(Some(DB_LOG)).unwrap();
            let deleted_log = env.create_database(Some(DB_DELETED)).unwrap();
            let lmdb = LMBDStorage {
                env,
                label_db,
                node_db,
                node_inv_db,
                layer_out_db,
                layer_in_db,
                deleted_log,
                log,
            };
            let mut w_txn = lmdb.rw_txn();
            let log = GraphLog {
                version_number: 0,
                max_layer: 0,
                entry_point: None,
            };
            lmdb.insert_log(&mut w_txn, log);
            w_txn.commit().unwrap();
            std::fs::File::create(&path.join(STAMP)).unwrap();
            lmdb
        } else {
            LMBDStorage::open(path)
        }
    }
    pub fn open(path: &Path) -> LMBDStorage {
        let mut env_builder = EnvOpenOptions::new();
        unsafe {
            env_builder.flag(Flags::MdbNoLock);
        }
        env_builder.max_dbs(MAX_DBS);
        env_builder.map_size(MAP_SIZE);
        let env_path = path.join(LMDB_ENV);
        let env = env_builder.open(&env_path).unwrap();
        let label_db = env.open_database(Some(DB_LABELS)).unwrap().unwrap();
        let node_db = env.open_database(Some(DB_NODES)).unwrap().unwrap();
        let node_inv_db = env.open_database(Some(DB_NODES_INV)).unwrap().unwrap();
        let layer_out_db = env.open_database(Some(DB_LAYERS_OUT)).unwrap().unwrap();
        let layer_in_db = env.open_database(Some(DB_LAYERS_IN)).unwrap().unwrap();
        let log = env.open_database(Some(DB_LOG)).unwrap().unwrap();
        let deleted_log = env.open_database(Some(DB_DELETED)).unwrap().unwrap();
        LMBDStorage {
            env,
            label_db,
            node_db,
            node_inv_db,
            layer_out_db,
            layer_in_db,
            deleted_log,
            log,
        }
    }
    pub fn ro_txn(&self) -> RoTxn<'_> {
        self.env.read_txn().unwrap()
    }
    pub fn rw_txn(&self) -> RwTxn<'_, '_> {
        self.env.write_txn().unwrap()
    }
    pub fn get_node(&self, txn: &RoTxn<'_>, vector: &str) -> Option<Node> {
        let v = self.node_db.get(txn, vector).unwrap();
        v.map(Node::from_byte_rpr)
    }
    pub fn get_node_key<'a>(&self, txn: &'a RoTxn, node: Node) -> Option<&'a str> {
        self.node_inv_db.get(txn, &node.alloc_byte_rpr()).unwrap()
    }
    pub fn get_keys(&self, txn: &RoTxn) -> Vec<String> {
        let mut result = vec![];
        let mut it = self.node_db.iter(txn).unwrap();
        while let Some((key, _)) = it.next().transpose().unwrap() {
            result.push(key.to_string());
        }
        result
    }
    pub fn has_label(&self, txn: &RoTxn<'_>, key: &str, label: &str) -> bool {
        let path = format!("{}/{}", key, label);
        let exist = self.label_db.get(txn, path.as_str()).unwrap();
        exist.is_some()
    }
    pub fn add_node(&self, txn: &mut RwTxn<'_, '_>, key: String, node: Node) {
        let node = node.alloc_byte_rpr();
        self.node_db.put(txn, key.as_str(), &node).unwrap();
        self.node_inv_db.put(txn, &node, key.as_str()).unwrap();
    }
    pub fn add_label(&self, txn: &mut RwTxn<'_, '_>, key: String, label: String) {
        let path = format!("{}/{}", key, label);
        self.label_db.put(txn, path.as_str(), &()).unwrap();
    }
    pub fn get_prefixed(&self, txn: &RoTxn, prefix: &str) -> Vec<String> {
        let mut result = vec![];
        let mut iter = self.node_db.prefix_iter(txn, prefix).unwrap();
        while let Some((k, _)) = iter.next().transpose().unwrap() {
            result.push(k.to_string());
        }
        result
    }
    pub fn insert_layer_out(&self, txn: &mut RwTxn<'_, '_>, id: u64, layer: &GraphLayer) {
        self.layer_out_db
            .put(txn, &id.alloc_byte_rpr(), &layer.alloc_byte_rpr())
            .unwrap();
    }
    pub fn insert_layer_in(&self, txn: &mut RwTxn<'_, '_>, id: u64, layer: &GraphLayer) {
        self.layer_in_db
            .put(txn, &id.alloc_byte_rpr(), &layer.alloc_byte_rpr())
            .unwrap();
    }
    pub fn get_layer_out(&self, txn: &RoTxn<'_>, layer: u64) -> Option<GraphLayer> {
        self.layer_out_db
            .get(txn, &layer.alloc_byte_rpr())
            .unwrap()
            .map(GraphLayer::from_byte_rpr)
    }
    pub fn get_layer_in(&self, txn: &RoTxn<'_>, layer: u64) -> Option<GraphLayer> {
        self.layer_in_db
            .get(txn, &layer.alloc_byte_rpr())
            .unwrap()
            .map(GraphLayer::from_byte_rpr)
    }
    pub fn insert_log(&self, txn: &mut RwTxn<'_, '_>, log: GraphLog) {
        self.log
            .put(
                txn,
                &LogField::EntryPoint.alloc_byte_rpr(),
                &log.entry_point.alloc_byte_rpr(),
            )
            .unwrap();
        self.log
            .put(
                txn,
                &LogField::NoLayers.alloc_byte_rpr(),
                &log.max_layer.alloc_byte_rpr(),
            )
            .unwrap();
        self.log
            .put(
                txn,
                &LogField::VersionNumber.alloc_byte_rpr(),
                &log.version_number.alloc_byte_rpr(),
            )
            .unwrap();
    }
    #[allow(clippy::ptr_arg)]
    pub fn mark_deleted(&self, txn: &mut RwTxn<'_, '_>, time_stamp: u128, rmv: &Vec<Node>) {
        self.deleted_log
            .put(txn, &time_stamp.alloc_byte_rpr(), &rmv.alloc_byte_rpr())
            .unwrap();
    }
    pub fn clear_deleted(&self, txn: &mut RwTxn<'_, '_>) -> Vec<Node> {
        let delete = self
            .deleted_log
            .get_greater_than(txn, &0u128.alloc_byte_rpr())
            .unwrap()
            .map(|(node, v)| (u128::from_byte_rpr(node), Vec::from_byte_rpr(v)));
        match delete {
            Some((stamp, deleted)) => {
                self.deleted_log
                    .delete(txn, &stamp.alloc_byte_rpr())
                    .unwrap();
                deleted.iter().for_each(|n| self.remove_vector(txn, *n));
                deleted
            }
            None => vec![],
        }
    }
    pub fn remove_vector(&self, txn: &mut RwTxn<'_, '_>, node: Node) {
        if let Some(key) = self.get_node_key(txn, node).map(|s| s.to_string()) {
            self.node_db.delete(txn, &key).unwrap();
            self.node_inv_db
                .delete(txn, &node.alloc_byte_rpr())
                .unwrap();
            let label_query = format!("{}/", key);
            let mut iter = self.label_db.prefix_iter_mut(txn, &label_query).unwrap();
            while iter.next().transpose().unwrap().is_some() {
                iter.del_current().unwrap();
            }
        }
    }
    pub fn get_log(&self, txn: &RoTxn<'_>) -> GraphLog {
        let version_number = self
            .log
            .get(txn, &LogField::VersionNumber.alloc_byte_rpr())
            .unwrap()
            .map(u128::from_byte_rpr)
            .unwrap();
        let max_layer = self
            .log
            .get(txn, &LogField::NoLayers.alloc_byte_rpr())
            .unwrap()
            .map(u64::from_byte_rpr)
            .unwrap();
        let entry_point = self
            .log
            .get(txn, &LogField::EntryPoint.alloc_byte_rpr())
            .unwrap()
            .map(Option::from_byte_rpr)
            .unwrap();
        GraphLog {
            version_number,
            max_layer,
            entry_point,
        }
    }
}
